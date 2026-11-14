package ws

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/ashtonian/mqttv5/internal/testpki"
	"github.com/ashtonian/mqttv5/transport"
)

// startEchoWS spins up an HTTP server that upgrades each connection
// to a WebSocket and echoes every binary frame it receives. The
// returned base URL has a ws:// scheme — callers append a path.
func startEchoWS(t *testing.T) (base *url.URL, cleanup func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := gws.UpgradeHTTP(r, w)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		for {
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				return
			}
			if op != gws.OpBinary {
				continue
			}
			if err := wsutil.WriteServerBinary(conn, msg); err != nil {
				return
			}
		}
	}))
	u, _ := url.Parse(strings.Replace(srv.URL, "http://", "ws://", 1))
	return u, srv.Close
}

func TestDialAndRoundTrip(t *testing.T) {
	base, cleanup := startEchoWS(t)
	defer cleanup()

	conn, err := Dial(context.Background(), base, DialOpts{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	want := []byte("hello mqtt over ws")
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// The echo server replies with a single binary frame containing
	// the same bytes. Read until we have them all.
	got := make([]byte, len(want))
	n := 0
	deadline := time.Now().Add(2 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	for n < len(want) {
		k, err := conn.Read(got[n:])
		if err != nil {
			t.Fatalf("Read after %d/%d bytes: %v", n, len(want), err)
		}
		n += k
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("roundtrip mismatch: got %q want %q", got, want)
	}
}

// Without a TLSConfig, wss:// verifies the broker against the system
// roots, as mqtts:// does: a certificate they do not trust is refused,
// and one trusted through TLSConfig is accepted.
func TestWssVerifiesByDefault(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := gws.UpgradeHTTP(r, w)
		if err == nil {
			conn.Close()
		}
	}))
	defer srv.Close()
	u, _ := url.Parse(strings.Replace(srv.URL, "https://", "wss://", 1))
	ctx := context.Background()
	if conn, err := Dial(ctx, u, DialOpts{}); err == nil {
		conn.Close()
		t.Fatal("wss:// to an untrusted certificate succeeded")
	}
	conn, err := Dial(ctx, u, DialOpts{TLSConfig: &tls.Config{RootCAs: certPool(srv.Certificate())}})
	if err != nil {
		t.Fatalf("wss:// with the certificate trusted: %v", err)
	}
	conn.Close()
}

// wss:// verifies the broker's chain, name and validity window against
// the caller's roots, the same matrix mqtts:// runs.
func TestWssVerification(t *testing.T) {
	root, cases := testpki.VerificationCases(t)
	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, _, _, err := gws.UpgradeHTTP(r, w)
				if err == nil {
					conn.Close()
				}
			}))
			srv.TLS = &tls.Config{Certificates: []tls.Certificate{tt.Cert}}
			srv.Config.ErrorLog = log.New(io.Discard, "", 0) // refused handshakes are the point
			srv.StartTLS()
			defer srv.Close()
			u, _ := url.Parse(strings.Replace(srv.URL, "https://", "wss://", 1))

			conn, err := Dial(context.Background(), u, DialOpts{TLSConfig: &tls.Config{
				RootCAs:    root.Pool(),
				ServerName: tt.ServerName,
				Time:       testpki.Clock,
			}})
			if tt.WantErr != nil {
				if err == nil {
					conn.Close()
					t.Fatal("dial succeeded, want a verification error")
				}
				if !tt.WantErr(err) {
					t.Fatalf("dial error = %v, not the expected verification error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			conn.Close()
		})
	}
}

// A wss:// connection reports its TLS handshake; a ws:// one reports
// none.
func TestConnectionState(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := gws.UpgradeHTTP(r, w)
		if err == nil {
			conn.Close()
		}
	}))
	defer srv.Close()
	u, _ := url.Parse(strings.Replace(srv.URL, "https://", "wss://", 1))
	conn, err := Dial(context.Background(), u, DialOpts{TLSConfig: &tls.Config{RootCAs: certPool(srv.Certificate())}})
	if err != nil {
		t.Fatalf("wss:// dial: %v", err)
	}
	defer conn.Close()
	state, ok := transport.TLSState(conn)
	if !ok || len(state.PeerCertificates) == 0 || !state.PeerCertificates[0].Equal(srv.Certificate()) {
		t.Fatalf("wss:// TLSState = %v; want the server's certificate", ok)
	}

	base, cleanup := startEchoWS(t)
	defer cleanup()
	plain, err := Dial(context.Background(), base, DialOpts{})
	if err != nil {
		t.Fatalf("ws:// dial: %v", err)
	}
	defer plain.Close()
	if _, ok := transport.TLSState(plain); ok {
		t.Fatal("ws:// reports TLS state")
	}
}

// startWSS serves WebSocket upgrades over TLS with cert and returns
// its wss:// URL.
func startWSS(t *testing.T, cert tls.Certificate) *url.URL {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := gws.UpgradeHTTP(r, w)
		if err == nil {
			conn.Close()
		}
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	u, _ := url.Parse(strings.Replace(srv.URL, "https://", "wss://", 1))
	return u
}

var errNoCredentials = errors.New("no credentials yet")

// TLSConfigFunc supplies the wss:// config per dial, with the dial's
// context and URL; its failures end the dial before the upgrade.
func TestTLSConfigFunc(t *testing.T) {
	root := testpki.NewRoot(t, "root")
	u := startWSS(t, root.Server(t))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	t.Run("config per dial", func(t *testing.T) {
		var gotURL string
		var hadDeadline bool
		conn, err := Dial(ctx, u, DialOpts{TLSConfigFunc: func(ctx context.Context, u *url.URL) (*tls.Config, error) {
			gotURL = u.String()
			_, hadDeadline = ctx.Deadline()
			return &tls.Config{RootCAs: root.Pool(), Time: testpki.Clock}, nil
		}})
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		conn.Close()
		if gotURL != u.String() {
			t.Fatalf("provider got URL %q, want %q", gotURL, u.String())
		}
		if !hadDeadline {
			t.Fatal("provider ran without the dial's deadline")
		}
	})

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
			_, err := Dial(ctx, u, DialOpts{TLSConfigFunc: func(context.Context, *url.URL) (*tls.Config, error) {
				return tt.config, tt.err
			}})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Dial error = %v, want %v", err, tt.wantErr)
			}
		})
	}

	t.Run("with TLSConfig", func(t *testing.T) {
		_, err := Dial(ctx, u, DialOpts{
			TLSConfig:     &tls.Config{},
			TLSConfigFunc: func(context.Context, *url.URL) (*tls.Config, error) { return &tls.Config{}, nil },
		})
		if !errors.Is(err, ErrConflictingTLSConfig) {
			t.Fatalf("Dial error = %v, want ErrConflictingTLSConfig", err)
		}
	})

	t.Run("not consulted for ws://", func(t *testing.T) {
		base, cleanup := startEchoWS(t)
		defer cleanup()
		called := false
		conn, err := Dial(ctx, base, DialOpts{TLSConfigFunc: func(context.Context, *url.URL) (*tls.Config, error) {
			called = true
			return nil, errNoCredentials
		}})
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		conn.Close()
		if called {
			t.Fatal("provider called for ws://")
		}
	})
}

func certPool(c *x509.Certificate) *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(c)
	return p
}

func TestRejectUnsupportedScheme(t *testing.T) {
	u, _ := url.Parse("mqtt://example.com:1883")
	_, err := Dial(context.Background(), u, DialOpts{})
	if err == nil {
		t.Fatal("expected error for mqtt:// scheme")
	}
}

// TestMultipleSmallReads verifies that the transport.Conn.Read API
// works when the consumer reads field-at-a-time (one byte each
// call) — this mirrors the MQTT decoder's behaviour.
func TestMultipleSmallReads(t *testing.T) {
	base, cleanup := startEchoWS(t)
	defer cleanup()

	conn, err := Dial(context.Background(), base, DialOpts{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	payload := bytes.Repeat([]byte("AB"), 50) // 100 bytes
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	one := make([]byte, 1)
	got := make([]byte, 0, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < len(payload); i++ {
		n, err := conn.Read(one)
		if err != nil {
			t.Fatalf("Read at byte %d: %v", i, err)
		}
		if n != 1 {
			t.Fatalf("Read got %d bytes at i=%d, want 1", n, i)
		}
		got = append(got, one[0])
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("byte-by-byte read mismatch")
	}
}
