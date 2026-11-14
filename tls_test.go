package mqttv5

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"net"
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

// tlsConfigTrusting returns a client TLS config trusting only ca.
func tlsConfigTrusting(ca *testpki.Authority) *tls.Config {
	return &tls.Config{RootCAs: ca.Pool(), Time: testpki.Clock}
}

// The provider runs on every TLS dial, so rotating the root it returns
// lets the client trust a broker that re-issued its certificate under a
// new CA, without a new client.
func TestTLSConfigFuncRotatesRoots(t *testing.T) {
	oldCA := testpki.NewRoot(t, "old CA")
	newCA := testpki.NewRoot(t, "new CA")
	oldLeaf, newLeaf := oldCA.Server(t), newCA.Server(t)
	var serving atomic.Pointer[tls.Certificate]
	serving.Store(&oldLeaf)
	b := testbroker.NewTLS(t, &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return serving.Load(), nil },
		// Every reconnect is a full handshake, in which the broker
		// presents its current certificate; a resumed one would carry
		// the session's. TestTLSResumptionReverifiesRoots covers that.
		SessionTicketsDisabled: true,
	})

	var trusted atomic.Pointer[testpki.Authority]
	trusted.Store(oldCA)
	var calls atomic.Int32
	refused := make(chan error, 16)
	up := make(chan ConnackInfo, 4)
	tbClient(t, b,
		WithTLSConfigFunc(func(ctx context.Context, u *url.URL) (*tls.Config, error) {
			calls.Add(1)
			return tlsConfigTrusting(trusted.Load()), nil
		}),
		WithOnConnectError(func(err error) {
			select {
			case refused <- err:
			default:
			}
		}),
		WithOnConnectionUp(func(info ConnackInfo) { up <- info }),
	)
	<-up

	serving.Store(&newLeaf)
	b.DropAll()
	select {
	case err := <-refused:
		var unknown x509.UnknownAuthorityError
		if !errors.As(err, &unknown) {
			t.Fatalf("reconnect to the re-issued certificate failed with %v, want x509.UnknownAuthorityError", err)
		}
	case <-up:
		t.Fatal("reconnected to a certificate under a root the provider does not trust yet")
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect attempt")
	}

	trusted.Store(newCA)
	select {
	case <-up:
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect after the provider rotated its root")
	}
	if n := calls.Load(); n < 3 {
		t.Fatalf("provider called %d times, want once per attempt (at least 3)", n)
	}
}

// Each broker of WithBrokers gets the config the provider returns for
// its URL: failover reaches a broker under a different CA.
func TestTLSConfigFuncPerBroker(t *testing.T) {
	caA, caB := testpki.NewRoot(t, "CA A"), testpki.NewRoot(t, "CA B")
	a := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{caA.Server(t)}})
	b := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{caB.Server(t)}})
	trust := map[string]*testpki.Authority{a.Addr(): caA, b.Addr(): caB}

	up := make(chan ConnackInfo, 4)
	c, err := New(
		WithBrokers(a.URL(), b.URL()),
		WithFailoverOrder(FailoverPriority),
		WithClientID("tb-"+t.Name()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)),
		WithLogger(quietLogger()),
		WithTLSConfigFunc(func(ctx context.Context, u *url.URL) (*tls.Config, error) {
			ca, ok := trust[u.Host]
			if !ok {
				return nil, fmt.Errorf("no CA for %s", u.Host)
			}
			return tlsConfigTrusting(ca), nil
		}),
		WithOnConnectionUp(func(info ConnackInfo) { up <- info }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	if info := <-up; info.Broker != a.URL() {
		t.Fatalf("connected to %s, want the first broker %s", info.Broker, a.URL())
	}

	a.Close()
	select {
	case info := <-up:
		if info.Broker != b.URL() {
			t.Fatalf("failed over to %s, want %s", info.Broker, b.URL())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no failover to the second broker")
	}
}

var errNoCredentials = errors.New("no credentials yet")

// A provider error ends the attempt before any TCP dial and reaches the
// caller wrapped; so does a nil config with no error. The provider runs
// under the attempt's deadline, and plain TCP brokers never consult it.
func TestTLSConfigFuncFailures(t *testing.T) {
	tests := []struct {
		name    string
		config  *tls.Config
		err     error
		wantErr error
	}{
		{name: "provider error", err: errNoCredentials, wantErr: errNoCredentials},
		{name: "nil config and no error", wantErr: transport.ErrNilTLSConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testbroker.NewTLS(t, &tls.Config{})
			var hadDeadline atomic.Bool
			reported := make(chan error, 1)
			c, err := New(
				WithBroker(b.URL()),
				WithClientID("tb-"+t.Name()),
				WithLogger(quietLogger()),
				WithTLSConfigFunc(func(ctx context.Context, u *url.URL) (*tls.Config, error) {
					_, ok := ctx.Deadline()
					hadDeadline.Store(ok)
					return tt.config, tt.err
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
			if err := c.Connect(ctx); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Connect error = %v, want %v", err, tt.wantErr)
			}
			if got := <-reported; !errors.Is(got, tt.wantErr) {
				t.Fatalf("OnConnectError got %v, want %v", got, tt.wantErr)
			}
			if !hadDeadline.Load() {
				t.Fatal("provider ran without the attempt's deadline")
			}
			if n := b.Accepted(); n != 0 {
				t.Fatalf("broker accepted %d connections; the provider failed before the dial", n)
			}
		})
	}

	t.Run("plain TCP broker", func(t *testing.T) {
		b := testbroker.New(t)
		var calls atomic.Int32
		tbClient(t, b, WithTLSConfigFunc(func(context.Context, *url.URL) (*tls.Config, error) {
			calls.Add(1)
			return nil, errNoCredentials
		}))
		if n := calls.Load(); n != 0 {
			t.Fatalf("provider called %d times for an mqtt:// broker", n)
		}
	})
}

// Options that would be silently ignored are rejected at New: one TLS
// config source at a time, and none beside WithDialFunc, which owns the
// whole dial.
func TestTLSOptionConflictsRejected(t *testing.T) {
	cfg := &tls.Config{}
	provider := func(context.Context, *url.URL) (*tls.Config, error) { return cfg, nil }
	dial := func(context.Context, *url.URL) (transport.Conn, error) { return nil, errors.New("unused") }
	tests := []struct {
		name string
		opts []Option
	}{
		{"WithTLSConfig and WithTLSConfigFunc", []Option{WithTLSConfig(cfg), WithTLSConfigFunc(provider)}},
		{"WithTLSConfig and WithDialFunc", []Option{WithTLSConfig(cfg), WithDialFunc(dial)}},
		{"WithTLSConfigFunc and WithDialFunc", []Option{WithTLSConfigFunc(provider), WithDialFunc(dial)}},
		{"WithDialer and WithDialFunc", []Option{WithDialer(&net.Dialer{}), WithDialFunc(dial)}},
		{"WithTLSConfigFunc(nil)", []Option{WithTLSConfigFunc(nil)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := append([]Option{WithBroker("mqtts://broker.example")}, tt.opts...)
			if _, err := New(opts...); err == nil {
				t.Fatal("New accepted the combination")
			}
		})
	}
	for _, opts := range [][]Option{{WithTLSConfig(cfg)}, {WithTLSConfigFunc(provider)}, {WithDialFunc(dial)}} {
		if _, err := New(append([]Option{WithBroker("mqtts://broker.example")}, opts...)...); err != nil {
			t.Fatalf("New rejected a single option: %v", err)
		}
	}
}

// countingCache is a session cache that counts its use.
type countingCache struct {
	tls.ClientSessionCache
	gets, puts atomic.Int32
}

func (c *countingCache) Get(key string) (*tls.ClientSessionState, bool) {
	c.gets.Add(1)
	return c.ClientSessionCache.Get(key)
}

func (c *countingCache) Put(key string, cs *tls.ClientSessionState) {
	c.puts.Add(1)
	c.ClientSessionCache.Put(key, cs)
}

func didResume(t *testing.T, c *testbroker.Conn) bool {
	t.Helper()
	waitLog(t, c, wire.CONNECT, 1)
	state, ok := c.TLS()
	if !ok {
		t.Fatalf("connection %d is not TLS", c.Index)
	}
	return state.DidResume
}

// A reconnect resumes the TLS session through the client's own cache,
// whether the config is fixed or built per dial, without modifying the
// caller's config. A config presenting a client certificate resumes only
// through a cache of its own, so a rotated certificate is presented on
// the next connection; SessionTicketsDisabled turns resumption off.
func TestTLSReconnectResumesSession(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	deviceCA := testpki.NewRoot(t, "device CA")
	device := deviceCA.Client(t, "device")
	tests := []struct {
		name string
		// perDial supplies the config through WithTLSConfigFunc, a new
		// one per dial, instead of WithTLSConfig.
		perDial         bool
		clientCert      bool
		getClientCert   bool
		ownCache        bool
		ticketsDisabled bool
		wantResume      bool
	}{
		{name: "WithTLSConfig", wantResume: true},
		{name: "WithTLSConfigFunc", perDial: true, wantResume: true},
		{name: "own session cache", ownCache: true, wantResume: true},
		{name: "client certificate", clientCert: true},
		{name: "GetClientCertificate", getClientCert: true},
		{name: "GetClientCertificate per dial", perDial: true, getClientCert: true},
		{name: "client certificate with its own session cache", clientCert: true, ownCache: true, wantResume: true},
		{name: "SessionTicketsDisabled", ticketsDisabled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &tls.Config{Certificates: []tls.Certificate{brokerCA.Server(t)}}
			if tt.clientCert || tt.getClientCert {
				server.ClientAuth = tls.RequireAndVerifyClientCert
				server.ClientCAs = deviceCA.Pool()
				server.Time = testpki.Clock
			}
			b := testbroker.NewTLS(t, server)

			var own *countingCache
			if tt.ownCache {
				own = &countingCache{ClientSessionCache: tls.NewLRUClientSessionCache(0)}
			}
			config := func() *tls.Config {
				cfg := tlsConfigTrusting(brokerCA)
				if tt.clientCert {
					cfg.Certificates = []tls.Certificate{device}
				}
				if tt.getClientCert {
					cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &device, nil }
				}
				if own != nil {
					cfg.ClientSessionCache = own
				}
				cfg.SessionTicketsDisabled = tt.ticketsDisabled
				return cfg
			}
			given := config()
			opt := WithTLSConfig(given)
			if tt.perDial {
				opt = WithTLSConfigFunc(func(context.Context, *url.URL) (*tls.Config, error) { return config(), nil })
			}
			tbClient(t, b, opt)

			if didResume(t, b.Conn(0, 0)) {
				t.Fatal("the first connection resumed a session")
			}
			b.DropAll()
			reconnected := b.Conn(1, 3*time.Second)
			if reconnected == nil {
				t.Fatal("client did not reconnect")
			}
			if got := didResume(t, reconnected); got != tt.wantResume {
				t.Fatalf("reconnection resumed = %v, want %v", got, tt.wantResume)
			}
			if own != nil && (own.puts.Load() == 0 || own.gets.Load() < 2) {
				t.Fatalf("own session cache used %d gets, %d puts; want a get per connection and a put", own.gets.Load(), own.puts.Load())
			}
			if given.ClientSessionCache != config().ClientSessionCache || given.ServerName != "" {
				t.Fatal("the client modified the caller's TLS config")
			}
		})
	}
}

// A session is resumed only while its certificate chain still verifies
// against the roots of the config at hand: once the client stops
// trusting the root, the reconnect is a full handshake and is refused,
// though the client holds a ticket the broker would accept.
func TestTLSResumptionReverifiesRoots(t *testing.T) {
	oldCA, newCA := testpki.NewRoot(t, "old CA"), testpki.NewRoot(t, "new CA")
	b := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{oldCA.Server(t)}})

	var trusted atomic.Pointer[testpki.Authority]
	trusted.Store(oldCA)
	refused := make(chan error, 16)
	up := make(chan ConnackInfo, 4)
	tbClient(t, b,
		WithTLSConfigFunc(func(context.Context, *url.URL) (*tls.Config, error) {
			return tlsConfigTrusting(trusted.Load()), nil
		}),
		WithOnConnectError(func(err error) {
			select {
			case refused <- err:
			default:
			}
		}),
		WithOnConnectionUp(func(info ConnackInfo) { up <- info }),
	)
	<-up
	b.DropAll()
	if reconnected := b.Conn(1, 3*time.Second); reconnected == nil || !didResume(t, reconnected) {
		t.Fatal("the reconnect under the same roots did not resume")
	}
	<-up

	trusted.Store(newCA)
	b.DropAll()
	select {
	case err := <-refused:
		var unknown x509.UnknownAuthorityError
		if !errors.As(err, &unknown) {
			t.Fatalf("reconnect after distrusting the root failed with %v, want x509.UnknownAuthorityError", err)
		}
	case <-up:
		t.Fatal("resumed a session whose certificate chains to a root the client no longer trusts")
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect attempt")
	}
}
