package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testpki"
)

// TestDialDispatch covers the URL-scheme switch in Dial. We don't need
// a server up for the negative cases — they fail in URL parsing or
// connect-refused before any bytes flow.
func TestDialDispatchUnknownScheme(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := Dial(ctx, "stomp://example.com:1883", DialOpts{})
	if !errors.Is(err, ErrUnknownScheme) {
		t.Fatalf("got %v, want ErrUnknownScheme", err)
	}
}

func TestDialDispatchMissingHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := Dial(ctx, "mqtt://", DialOpts{})
	if !errors.Is(err, ErrMissingHost) {
		t.Fatalf("got %v, want ErrMissingHost", err)
	}
}

func TestDialTCPSuccess(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = c.Write([]byte("hi"))
		_ = c.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, "mqtt://"+ln.Addr().String(), DialOpts{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "hi" {
		t.Fatalf("read = %q, want %q", buf, "hi")
	}
}

func TestDialTCPCtxCancel(t *testing.T) {
	// Dialing an unroutable address must respect ctx cancellation.
	// 192.0.2.0/24 is RFC 5737 TEST-NET-1 — no host should answer.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Dial(ctx, "mqtt://192.0.2.1:1883", DialOpts{})
	if err == nil {
		t.Fatal("expected dial to fail")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("dial ignored ctx deadline (took %v)", time.Since(start))
	}
}

// handshake is what the server side of one TLS connection saw.
type handshake struct {
	err   error
	state tls.ConnectionState
}

// serveTLSOnce accepts one connection on a TLS listener with cfg,
// completes the server handshake, writes "ok" when it succeeds, and
// reports the result.
func serveTLSOnce(t *testing.T, cfg *tls.Config) (addr string, result <-chan handshake) {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	ch := make(chan handshake, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			ch <- handshake{err: err}
			return
		}
		defer c.Close()
		tc := c.(*tls.Conn)
		if err := tc.Handshake(); err != nil {
			ch <- handshake{err: err}
			return
		}
		_, _ = tc.Write([]byte("ok"))
		ch <- handshake{state: tc.ConnectionState()}
	}()
	return ln.Addr().String(), ch
}

func dialTLS(t *testing.T, addr string, cfg *tls.Config) (Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return Dial(ctx, "mqtts://"+addr, DialOpts{TLSConfig: cfg})
}

func readOK(t *testing.T, c Conn) {
	t.Helper()
	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "ok" {
		t.Fatalf("read = %q, want %q", buf, "ok")
	}
}

// mqtts:// verifies the broker's chain, name and validity window
// against the caller's roots.
func TestDialTLSVerification(t *testing.T) {
	root, cases := testpki.VerificationCases(t)
	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			addr, _ := serveTLSOnce(t, &tls.Config{Certificates: []tls.Certificate{tt.Cert}})
			c, err := dialTLS(t, addr, &tls.Config{
				RootCAs:    root.Pool(),
				ServerName: tt.ServerName,
				Time:       testpki.Clock,
			})
			if tt.WantErr != nil {
				if err == nil {
					c.Close()
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
			defer c.Close()
			readOK(t, c)
		})
	}
}

// With no ServerName, the URL host is verified, on a copy: the
// caller's config is left as it was. crypto/tls refuses to verify with
// an empty ServerName, so a successful dial to a certificate valid only
// for 127.0.0.1 shows the URL host was used.
func TestDialTLSServerNameFromURLHost(t *testing.T) {
	root := testpki.NewRoot(t, "root")
	cert := root.Server(t, testpki.WithDNSNames(), testpki.WithIPs(net.IPv4(127, 0, 0, 1)))
	addr, _ := serveTLSOnce(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	cfg := &tls.Config{RootCAs: root.Pool(), Time: testpki.Clock}
	c, err := dialTLS(t, addr, cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	readOK(t, c)
	if cfg.ServerName != "" {
		t.Fatalf("caller's config was modified: ServerName = %q", cfg.ServerName)
	}
}

// A broker that requires a client certificate gets the one in the
// config, and refuses a client without one.
func TestDialTLSClientCertificate(t *testing.T) {
	root := testpki.NewRoot(t, "root")
	clientCA := testpki.NewRoot(t, "client CA")
	server := &tls.Config{
		Certificates: []tls.Certificate{root.Server(t)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA.Pool(),
		Time:         testpki.Clock,
	}

	t.Run("certificate supplied", func(t *testing.T) {
		cert := clientCA.Client(t, "device-1")
		addr, result := serveTLSOnce(t, server)
		c, err := dialTLS(t, addr, &tls.Config{
			RootCAs:      root.Pool(),
			Certificates: []tls.Certificate{cert},
			Time:         testpki.Clock,
		})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer c.Close()
		readOK(t, c)
		hs := <-result
		if hs.err != nil {
			t.Fatalf("server handshake: %v", hs.err)
		}
		if got := hs.state.PeerCertificates[0].SerialNumber; got.Cmp(cert.Leaf.SerialNumber) != 0 {
			t.Fatalf("server saw client serial %v, want %v", got, cert.Leaf.SerialNumber)
		}
	})

	t.Run("no certificate", func(t *testing.T) {
		addr, result := serveTLSOnce(t, server)
		c, err := dialTLS(t, addr, &tls.Config{RootCAs: root.Pool(), Time: testpki.Clock})
		if err == nil {
			// TLS 1.3 completes the client's handshake before the
			// server checks its certificate: the refusal arrives as
			// an alert on the first read.
			defer c.Close()
			if _, rerr := io.ReadFull(c, make([]byte, 2)); rerr == nil {
				t.Fatal("read succeeded without a client certificate")
			}
		}
		if hs := <-result; hs.err == nil {
			t.Fatal("server accepted a client without a certificate")
		}
	})
}

func TestSchemes(t *testing.T) {
	tests := []struct {
		scheme   string
		tls, tcp bool
	}{
		{"mqtts", true, false},
		{"tls", true, false},
		{"ssl", true, false},
		{"mqtt", false, true},
		{"tcp", false, true},
		{"wss", false, false},
		{"ws", false, false},
		{"MQTTS", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			if got := IsTLSScheme(tt.scheme); got != tt.tls {
				t.Errorf("IsTLSScheme(%q) = %v, want %v", tt.scheme, got, tt.tls)
			}
			if got := IsTCPScheme(tt.scheme); got != tt.tcp {
				t.Errorf("IsTCPScheme(%q) = %v, want %v", tt.scheme, got, tt.tcp)
			}
		})
	}
}

// TLSClientConfig fills in a server name and a session cache only where
// the caller's config leaves them to the dial, never on the caller's
// config itself.
func TestTLSClientConfig(t *testing.T) {
	cache := tls.NewLRUClientSessionCache(1)
	own := tls.NewLRUClientSessionCache(1)
	cert := tls.Certificate{Certificate: [][]byte{{1}}}
	getCert := func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil }
	tests := []struct {
		name       string
		cfg        *tls.Config
		serverName string
		cache      tls.ClientSessionCache
		wantName   string
		wantCache  tls.ClientSessionCache
		// wantSame expects cfg itself back: nothing to fill in.
		wantSame bool
	}{
		{name: "nil config", serverName: "broker", cache: cache, wantName: "broker", wantCache: cache},
		{name: "nil config without a cache", serverName: "broker", wantName: "broker"},
		{name: "server name kept", cfg: &tls.Config{ServerName: "lb"}, serverName: "broker", cache: cache, wantName: "lb", wantCache: cache},
		{name: "nothing to fill in", cfg: &tls.Config{ServerName: "lb"}, serverName: "broker", wantName: "lb", wantSame: true},
		{name: "own cache kept", cfg: &tls.Config{ServerName: "lb", ClientSessionCache: own}, cache: cache, wantName: "lb", wantCache: own, wantSame: true},
		{name: "client certificate", cfg: &tls.Config{Certificates: []tls.Certificate{cert}}, serverName: "broker", cache: cache, wantName: "broker"},
		{name: "GetClientCertificate", cfg: &tls.Config{GetClientCertificate: getCert}, serverName: "broker", cache: cache, wantName: "broker"},
		{name: "client certificate with its own cache", cfg: &tls.Config{Certificates: []tls.Certificate{cert}, ClientSessionCache: own}, cache: cache, wantCache: own, wantSame: true},
		{name: "session tickets disabled", cfg: &tls.Config{SessionTicketsDisabled: true}, serverName: "broker", cache: cache, wantName: "broker"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var before tls.Config
			if tt.cfg != nil {
				before = *tt.cfg.Clone()
			}
			got := TLSClientConfig(tt.cfg, tt.serverName, tt.cache)
			if got.ServerName != tt.wantName {
				t.Errorf("ServerName = %q, want %q", got.ServerName, tt.wantName)
			}
			if got.ClientSessionCache != tt.wantCache {
				t.Errorf("ClientSessionCache = %v, want %v", got.ClientSessionCache, tt.wantCache)
			}
			if (got == tt.cfg) != tt.wantSame {
				t.Errorf("returned the caller's config = %v, want %v", got == tt.cfg, tt.wantSame)
			}
			if tt.cfg != nil && (tt.cfg.ServerName != before.ServerName || tt.cfg.ClientSessionCache != before.ClientSessionCache) {
				t.Error("modified the caller's config")
			}
		})
	}
}
