package mqttv5

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/internal/testpki"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

func peerSerial(t *testing.T, c *testbroker.Conn) *big.Int {
	t.Helper()
	state, ok := c.TLS()
	if !ok || len(state.PeerCertificates) == 0 {
		t.Fatalf("connection %d has no client certificate", c.Index)
	}
	return state.PeerCertificates[0].SerialNumber
}

// GetClientCertificate runs on every handshake, so a reconnect presents
// the certificate it returns at that moment: rotating a client
// certificate needs nothing from the client.
func TestTLSClientCertificateRotatesOnReconnect(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	deviceCA := testpki.NewRoot(t, "device CA")
	b := testbroker.NewTLS(t, &tls.Config{
		Certificates: []tls.Certificate{brokerCA.Server(t)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    deviceCA.Pool(),
		Time:         testpki.Clock,
	})
	first, second := deviceCA.Client(t, "device"), deviceCA.Client(t, "device")
	var current atomic.Pointer[tls.Certificate]
	current.Store(&first)
	var calls atomic.Int32
	tbClient(t, b, WithTLSConfig(&tls.Config{
		RootCAs: brokerCA.Pool(),
		Time:    testpki.Clock,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			calls.Add(1)
			return current.Load(), nil
		},
	}))

	if got := peerSerial(t, b.Conn(0, 0)); got.Cmp(first.Leaf.SerialNumber) != 0 {
		t.Fatalf("first connection presented serial %v, want %v", got, first.Leaf.SerialNumber)
	}

	current.Store(&second)
	b.DropAll()
	reconnected := b.Conn(1, 3*time.Second)
	if reconnected == nil {
		t.Fatal("client did not reconnect")
	}
	waitLog(t, reconnected, wire.CONNECT, 1)
	if got := peerSerial(t, reconnected); got.Cmp(second.Leaf.SerialNumber) != 0 {
		t.Fatalf("reconnection presented serial %v, want the rotated %v", got, second.Leaf.SerialNumber)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("GetClientCertificate called %d times, want once per connection (2)", n)
	}
}

// A broker whose certificate does not verify never receives CONNECT,
// so credentials never reach it, and the caller can tell why.
func TestTLSUntrustedBrokerReceivesNoConnect(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	otherCA := testpki.NewRoot(t, "other CA")
	b := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{brokerCA.Server(t)}})

	reported := make(chan error, 1)
	c, err := New(
		WithBroker(b.URL()),
		WithClientID("tb-"+t.Name()),
		WithCredentials("user", []byte("secret")),
		WithLogger(quietLogger()),
		WithTLSConfig(&tls.Config{RootCAs: otherCA.Pool(), Time: testpki.Clock}),
		WithOnConnectError(func(err error) {
			select {
			case reported <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = c.Connect(ctx)

	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Fatalf("Connect error = %v, want x509.UnknownAuthorityError", err)
	}
	select {
	case got := <-reported:
		if !errors.As(got, &unknown) {
			t.Fatalf("OnConnectError got %v, want x509.UnknownAuthorityError", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnConnectError did not fire")
	}
	conn := b.Conn(0, time.Second)
	if conn == nil {
		t.Fatal("broker accepted no connection")
	}
	<-conn.Gone()
	if log := conn.Log(); len(log) != 0 {
		t.Fatalf("broker received %d packets from a client that does not trust it", len(log))
	}
}

// ServerInfo and OnConnectionUp report the TLS handshake of a TLS
// connection: the broker's chain and what was negotiated.
func TestConnackInfoReportsTLSState(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	serverCert := brokerCA.Server(t)
	b := testbroker.NewTLS(t, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		NextProtos:   []string{"mqtt"},
	})
	ups := make(chan ConnackInfo, 1)
	cli := tbClient(t, b,
		WithTLSConfig(&tls.Config{RootCAs: brokerCA.Pool(), NextProtos: []string{"mqtt"}, Time: testpki.Clock}),
		WithOnConnectionUp(func(info ConnackInfo) { ups <- info }),
	)

	info, ok := cli.ServerInfo()
	if !ok {
		t.Fatal("ServerInfo: not connected")
	}
	assertTLSState(t, "ServerInfo", info.TLS, serverCert)
	select {
	case up := <-ups:
		assertTLSState(t, "OnConnectionUp", up.TLS, serverCert)
	case <-time.After(3 * time.Second):
		t.Fatal("OnConnectionUp did not fire")
	}

	// The caller gets a copy.
	info.TLS.PeerCertificates[0] = nil
	again, _ := cli.ServerInfo()
	if again.TLS.PeerCertificates[0] == nil {
		t.Fatal("modifying ServerInfo().TLS changed the client's copy")
	}
}

func assertTLSState(t *testing.T, from string, state *tls.ConnectionState, serverCert tls.Certificate) {
	t.Helper()
	if state == nil {
		t.Fatalf("%s: TLS is nil on a TLS connection", from)
	}
	if !state.HandshakeComplete {
		t.Fatalf("%s: HandshakeComplete is false", from)
	}
	if len(state.PeerCertificates) == 0 || state.PeerCertificates[0].SerialNumber.Cmp(serverCert.Leaf.SerialNumber) != 0 {
		t.Fatalf("%s: PeerCertificates does not start with the broker's leaf", from)
	}
	if len(state.VerifiedChains) == 0 {
		t.Fatalf("%s: no verified chain", from)
	}
	if state.NegotiatedProtocol != "mqtt" {
		t.Fatalf("%s: NegotiatedProtocol = %q, want mqtt", from, state.NegotiatedProtocol)
	}
}

func TestConnackInfoTLSNilOnPlainTCP(t *testing.T) {
	cli := tbClient(t, testbroker.New(t))
	info, ok := cli.ServerInfo()
	if !ok {
		t.Fatal("ServerInfo: not connected")
	}
	if info.TLS != nil {
		t.Fatalf("TLS = %+v on a plain TCP connection, want nil", info.TLS)
	}
}

// statefulConn is a custom transport's connection that wraps a TLS
// connection and reports its state.
type statefulConn struct {
	transport.Conn
	tls *tls.Conn
}

func (c statefulConn) ConnectionState() tls.ConnectionState { return c.tls.ConnectionState() }

// opaqueConn wraps a TLS connection without reporting its state.
type opaqueConn struct{ transport.Conn }

// A custom WithDialFunc transport reports TLS state by implementing
// transport.ConnectionStater; one that does not reports none.
func TestConnackInfoTLSFromCustomDialFunc(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	b := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{brokerCA.Server(t)}})
	cfg := &tls.Config{RootCAs: brokerCA.Pool(), Time: testpki.Clock}
	dialWrapped := func(wrap func(*tls.Conn) transport.Conn) transport.DialFunc {
		return func(ctx context.Context, u *url.URL) (transport.Conn, error) {
			c, err := transport.DialTLS(ctx, u.Host, u.Hostname(), transport.DialOpts{TLSConfig: cfg})
			if err != nil {
				return nil, err
			}
			return wrap(c.(*tls.Conn)), nil
		}
	}

	t.Run("implements ConnectionStater", func(t *testing.T) {
		cli := tbClient(t, b, WithDialFunc(dialWrapped(func(c *tls.Conn) transport.Conn { return statefulConn{c, c} })))
		if info, _ := cli.ServerInfo(); info.TLS == nil || !info.TLS.HandshakeComplete {
			t.Fatalf("TLS = %+v, want the wrapped connection's state", info.TLS)
		}
	})
	t.Run("does not", func(t *testing.T) {
		cli := tbClient(t, b, WithDialFunc(dialWrapped(func(c *tls.Conn) transport.Conn { return opaqueConn{c} })))
		if info, _ := cli.ServerInfo(); info.TLS != nil {
			t.Fatalf("TLS = %+v, want nil for a connection that reports no state", info.TLS)
		}
	})
}

// The TLS verifier runs after the handshake and before CONNECT, under
// the attempt's deadline, with the broker's URL and TLS state; an error
// fails the attempt before the broker sees a packet.
func TestTLSVerifierRunsBeforeConnect(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	serverCert := brokerCA.Server(t)
	b := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{serverCert}})
	refused := errors.New("refused by the verifier")
	var (
		gotURL      *url.URL
		gotState    tls.ConnectionState
		hadDeadline bool
	)
	reported := make(chan error, 1)
	c, err := New(
		WithBroker(b.URL()),
		WithClientID("tb-"+t.Name()),
		WithCredentials("user", []byte("secret")),
		WithLogger(quietLogger()),
		WithTLSConfig(&tls.Config{RootCAs: brokerCA.Pool(), Time: testpki.Clock}),
		WithTLSVerifier(func(ctx context.Context, u *url.URL, state tls.ConnectionState) error {
			gotURL, gotState = u, state
			_, hadDeadline = ctx.Deadline()
			return refused
		}),
		WithOnConnectError(func(err error) {
			select {
			case reported <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Connect(ctx); !errors.Is(err, refused) {
		t.Fatalf("Connect error = %v, want the verifier's error", err)
	}
	if got := <-reported; !errors.Is(got, refused) {
		t.Fatalf("OnConnectError got %v, want the verifier's error", got)
	}
	if gotURL == nil || gotURL.String() != b.URL() {
		t.Fatalf("verifier got URL %v, want %s", gotURL, b.URL())
	}
	if len(gotState.PeerCertificates) == 0 || !gotState.PeerCertificates[0].Equal(serverCert.Leaf) {
		t.Fatal("verifier did not get the broker's certificate")
	}
	if !hadDeadline {
		t.Fatal("verifier's context has no deadline; want the connect timeout")
	}
	conn := b.Conn(0, time.Second)
	if conn == nil {
		t.Fatal("broker accepted no connection")
	}
	<-conn.Gone()
	if log := conn.Log(); len(log) != 0 {
		t.Fatalf("broker received %d packets from a connection the verifier refused", len(log))
	}
}

// A verifier that accepts lets the connection proceed, and runs again on
// every reconnect.
func TestTLSVerifierRunsPerConnection(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	b := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{brokerCA.Server(t)}})
	var calls atomic.Int32
	tbClient(t, b,
		WithTLSConfig(&tls.Config{RootCAs: brokerCA.Pool(), Time: testpki.Clock}),
		WithTLSVerifier(func(context.Context, *url.URL, tls.ConnectionState) error {
			calls.Add(1)
			return nil
		}),
	)
	b.DropAll()
	reconnected := b.Conn(1, 3*time.Second)
	if reconnected == nil {
		t.Fatal("client did not reconnect")
	}
	waitLog(t, reconnected, wire.CONNECT, 1)
	if n := calls.Load(); n != 2 {
		t.Fatalf("verifier ran %d times, want once per connection (2)", n)
	}
}

// A verifier on a connection without TLS fails closed.
func TestTLSVerifierRequiresTLS(t *testing.T) {
	b := testbroker.New(t)
	c, err := New(
		WithBroker(b.URL()),
		WithClientID("tb-"+t.Name()),
		WithLogger(quietLogger()),
		WithTLSVerifier(func(context.Context, *url.URL, tls.ConnectionState) error { return nil }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Connect(ctx); !errors.Is(err, ErrNotTLS) {
		t.Fatalf("Connect error = %v, want ErrNotTLS", err)
	}
	conn := b.Conn(0, time.Second)
	if conn == nil {
		t.Fatal("broker accepted no connection")
	}
	<-conn.Gone()
	if log := conn.Log(); len(log) != 0 {
		t.Fatalf("broker received %d packets over a connection the verifier could not check", len(log))
	}
}

func TestWithTLSVerifierRejectsNil(t *testing.T) {
	if _, err := New(WithBroker("mqtts://broker.example"), WithTLSVerifier(nil)); err == nil {
		t.Fatal("New accepted WithTLSVerifier(nil)")
	}
}
