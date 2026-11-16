package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/ashtonian/mqttv5/internal/testpki"
	"github.com/ashtonian/mqttv5/transport"
)

func TestDialRoundTrip(t *testing.T) {
	srv := startServer(t)
	srv.echo()

	conn := dial(t, srv.url, srv.opts())
	defer conn.Close()

	want := connectPacket(64)
	write(t, conn, want)
	readFull(t, conn, want)
	if got := conn.RemoteAddr().String(); got != srv.ln.Addr().String() {
		t.Fatalf("RemoteAddr = %s, want %s", got, srv.ln.Addr())
	}
	// A dual-stack socket can share its port with an IPv4 socket bound
	// to 127.0.0.1, which then takes the broker's replies.
	if la, ok := conn.LocalAddr().(*net.UDPAddr); !ok || la.IP.To4() == nil {
		t.Fatalf("LocalAddr = %v, want an IPv4 socket for an IPv4 broker", conn.LocalAddr())
	}
}

// Dials on one PacketConn, from any number of Dialers and Dial, share
// it while their connections are open, then hand it back to the caller
// open and unread.
func TestPacketConn(t *testing.T) {
	srv := startServer(t)
	srv.echo()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	opts := srv.opts()
	opts.PacketConn = pc
	ctx := testContext(t)

	oneShot := func(ctx context.Context, u *url.URL) (transport.Conn, error) { return Dial(ctx, u, opts) }
	var conns []transport.Conn
	for _, dial := range []transport.DialFunc{NewDialer(opts).Dial, NewDialer(opts).Dial, oneShot} {
		conn, err := dial(ctx, srv.url)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if conn.LocalAddr().String() != pc.LocalAddr().String() {
			t.Fatalf("LocalAddr = %v, want the PacketConn's %v", conn.LocalAddr(), pc.LocalAddr())
		}
		conns = append(conns, conn)
	}
	for i, conn := range conns {
		p := connectPacket(i + 1)
		write(t, conn, p)
		readFull(t, conn, p)
	}
	for _, conn := range conns {
		conn.Close()
	}
	waitReleased(t, pc)

	sender, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.WriteTo([]byte("mine"), pc.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	_ = pc.SetReadDeadline(time.Now().Add(testTimeout))
	buf := make([]byte, 16)
	n, _, err := pc.ReadFrom(buf)
	if err != nil || string(buf[:n]) != "mine" {
		t.Fatalf("PacketConn ReadFrom after the connections closed = %q, %v; want the caller's datagram", buf[:n], err)
	}
}

// waitReleased waits for every connection over pc to give up its share.
func waitReleased(t *testing.T, pc net.PacketConn) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		sockets.mu.Lock()
		_, inUse := sockets.inUse[pc]
		sockets.mu.Unlock()
		if !inUse {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the PacketConn is still in use after its connections closed")
		}
		time.Sleep(time.Millisecond)
	}
}

// listedPacketConn is a PacketConn a map cannot key: a slice field
// makes it incomparable.
type listedPacketConn struct {
	net.PacketConn
	_ []string
}

func TestPacketConnNotComparable(t *testing.T) {
	srv := startServer(t)
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	opts := srv.opts()
	opts.PacketConn = listedPacketConn{PacketConn: pc}
	if _, err := Dial(testContext(t), srv.url, opts); err == nil || !strings.Contains(err.Error(), "not comparable") {
		t.Fatalf("dial error = %v, want the PacketConn refused", err)
	}
}

func TestParseTarget(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    target
		wantErr bool
	}{
		{name: "default port", url: "mqtt-quic://broker.example", want: target{host: "broker.example", port: 14567}},
		{name: "explicit port", url: "mqtt-quic://broker.example:4433", want: target{host: "broker.example", port: 4433}},
		{name: "IPv6 literal", url: "mqtt-quic://[::1]:4433", want: target{host: "::1", port: 4433}},
		{name: "other scheme", url: "mqtts://broker.example:8883", wantErr: true},
		{name: "no host", url: "mqtt-quic://:4433", wantErr: true},
		{name: "port zero", url: "mqtt-quic://broker.example:0", wantErr: true},
		{name: "port out of range", url: "mqtt-quic://broker.example:70000", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			if err != nil {
				t.Fatal(err)
			}
			got, err := parseTarget(u)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseTarget = %+v, want an error", got)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("parseTarget = %+v, %v; want %+v", got, err, tt.want)
			}
		})
	}
	if _, err := parseTarget(nil); err == nil {
		t.Fatal("parseTarget(nil) succeeded")
	}
}

// Without a TLSConfig the broker is verified against the system roots;
// a certificate trusted through TLSConfig is accepted.
func TestVerifiesByDefault(t *testing.T) {
	srv := startServer(t)
	ctx := testContext(t)
	if conn, err := Dial(ctx, srv.url, DialOpts{}); err == nil {
		conn.Close()
		t.Fatal("dial to an untrusted certificate succeeded")
	} else if errors.Is(err, ErrALPN) {
		t.Fatalf("dial error = %v, want a verification error", err)
	}
	dial(t, srv.url, srv.opts()).Close()
}

// The broker's chain, name and validity window are verified against the
// caller's roots, the matrix every TLS transport runs.
func TestVerification(t *testing.T) {
	root, cases := testpki.VerificationCases(t)
	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			srv := startServer(t, func(c *serverConfig) { c.cert = &tt.Cert })
			cfg := trusting(root)
			cfg.ServerName = tt.ServerName
			conn, err := Dial(testContext(t), srv.url, DialOpts{TLSConfig: cfg})
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

// A broker that does not select ALPN "mqtt" fails the dial with ErrALPN.
func TestALPNMismatch(t *testing.T) {
	srv := startServer(t, func(c *serverConfig) { c.nextProtos = []string{"h3"} })
	if _, err := Dial(testContext(t), srv.url, srv.opts()); !errors.Is(err, ErrALPN) {
		t.Fatalf("dial error = %v, want ErrALPN", err)
	}
}

// The connection reports its TLS handshake, so the client fills in
// ConnackInfo.TLS and runs WithTLSVerifier.
func TestConnectionState(t *testing.T) {
	srv := startServer(t)
	conn := dial(t, srv.url, srv.opts())
	defer conn.Close()
	state, ok := transport.TLSState(conn)
	if !ok || state.NegotiatedProtocol != alpn || state.Version != tls.VersionTLS13 || len(state.PeerCertificates) == 0 {
		t.Fatalf("TLSState = %v, ALPN %q, version %#x, %d certificates; want TLS 1.3 with ALPN mqtt and the broker's chain",
			ok, state.NegotiatedProtocol, state.Version, len(state.PeerCertificates))
	}
}

var errNoCredentials = errors.New("no credentials yet")

// TLSConfigFunc supplies the config per dial, with the dial's context
// and URL; its failures end the dial before any packet is sent.
func TestTLSConfigFunc(t *testing.T) {
	srv := startServer(t)
	ctx := testContext(t)

	t.Run("config per dial", func(t *testing.T) {
		var gotURL string
		var hadDeadline bool
		dial(t, srv.url, DialOpts{TLSConfigFunc: func(ctx context.Context, u *url.URL) (*tls.Config, error) {
			gotURL = u.String()
			_, hadDeadline = ctx.Deadline()
			return trusting(srv.root), nil
		}}).Close()
		if gotURL != srv.url.String() {
			t.Fatalf("provider got URL %q, want %q", gotURL, srv.url)
		}
		if !hadDeadline {
			t.Fatal("provider ran without the dial's deadline")
		}
	})

	tests := []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "provider error", err: errNoCredentials, wantErr: errNoCredentials},
		{name: "nil config and no error", wantErr: transport.ErrNilTLSConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Dial(ctx, srv.url, DialOpts{TLSConfigFunc: func(context.Context, *url.URL) (*tls.Config, error) {
				return nil, tt.err
			}})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("dial error = %v, want %v", err, tt.wantErr)
			}
		})
	}

	t.Run("with TLSConfig", func(t *testing.T) {
		_, err := Dial(ctx, srv.url, DialOpts{
			TLSConfig:     &tls.Config{},
			TLSConfigFunc: func(context.Context, *url.URL) (*tls.Config, error) { return &tls.Config{}, nil },
		})
		if !errors.Is(err, ErrConflictingTLSConfig) {
			t.Fatalf("dial error = %v, want ErrConflictingTLSConfig", err)
		}
	})
}

// A Dialer's dials resume the TLS session, so the client's reconnects
// do; Dial alone resumes only through the config's own cache. The
// caller's config is never modified.
func TestResumesSession(t *testing.T) {
	srv := startServer(t)
	srv.echo()
	opts := srv.opts()
	ctx := testContext(t)

	dials := func(dial transport.DialFunc) []bool {
		t.Helper()
		var resumed []bool
		for range 2 {
			conn, err := dial(ctx, srv.url)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			// A round trip lets the client take in the session ticket,
			// which the server sends after the handshake.
			write(t, conn, connectPacket(1))
			readFull(t, conn, connectPacket(1))
			resumed = append(resumed, conn.(*Conn).qc.ConnectionState().TLS.DidResume)
			conn.Close()
		}
		return resumed
	}
	if got := dials(DialFunc(opts)); got[0] || !got[1] {
		t.Fatalf("DialFunc resumed = %v, want [false true]", got)
	}
	oneShot := func(ctx context.Context, u *url.URL) (transport.Conn, error) { return Dial(ctx, u, opts) }
	if got := dials(oneShot); got[0] || got[1] {
		t.Fatalf("Dial resumed = %v, want [false false]", got)
	}
	if opts.TLSConfig.ClientSessionCache != nil || opts.TLSConfig.ServerName != "" || opts.TLSConfig.NextProtos != nil {
		t.Fatal("dialing modified the caller's TLS config")
	}
}

func TestQUICConfig(t *testing.T) {
	cfg := newDialer(DialOpts{}).quicConfig(DefaultHandshakeTimeout)
	if cfg.HandshakeIdleTimeout != DefaultHandshakeTimeout || cfg.MaxIdleTimeout != DefaultMaxIdleTimeout ||
		cfg.KeepAlivePeriod != DefaultMaxIdleTimeout/4 {
		t.Fatalf("defaults: handshake %v, idle %v, keep-alive %v", cfg.HandshakeIdleTimeout, cfg.MaxIdleTimeout, cfg.KeepAlivePeriod)
	}
	if !cfg.EnableStreamResetPartialDelivery {
		t.Fatal("RESET_STREAM_AT not enabled")
	}

	cfg = newDialer(DialOpts{MaxIdleTimeout: time.Minute, KeepAlivePeriod: 10 * time.Second}).quicConfig(time.Second)
	if cfg.MaxIdleTimeout != time.Minute || cfg.KeepAlivePeriod != 10*time.Second || cfg.HandshakeIdleTimeout != time.Second {
		t.Fatalf("overrides: handshake %v, idle %v, keep-alive %v", cfg.HandshakeIdleTimeout, cfg.MaxIdleTimeout, cfg.KeepAlivePeriod)
	}
}

// The broker cannot open a stream of its own: the client's transport
// parameters allow none.
func TestBrokerCannotOpenStreams(t *testing.T) {
	srv := startServer(t)
	conn := dial(t, srv.url, srv.opts())
	defer conn.Close()
	write(t, conn, connectPacket(1))
	sc := srv.next(t)
	if _, err := sc.qc.OpenStream(); !errors.As(err, new(*quicgo.StreamLimitReachedError)) {
		t.Fatalf("broker OpenStream error = %v, want the stream limit", err)
	}
	if _, err := sc.qc.OpenUniStream(); !errors.As(err, new(*quicgo.StreamLimitReachedError)) {
		t.Fatalf("broker OpenUniStream error = %v, want the stream limit", err)
	}
}
