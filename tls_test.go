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
