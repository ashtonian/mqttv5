package quic

import (
	"context"
	"crypto/x509"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testpki"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// failingHandshake is short: the blackhole never answers, and the test
// waits it out in real time once per QUIC attempt.
const failingHandshake = 50 * time.Millisecond

func wantFallback(t *testing.T, conn transport.Conn) {
	t.Helper()
	if _, ok := conn.(*fallbackConn); !ok {
		t.Fatalf("dial returned %T, want the TLS fallback", conn)
	}
	write(t, conn, connectPacket(4))
	readFull(t, conn, connectPacket(4))
	if _, ok := transport.TLSState(conn); !ok {
		t.Fatal("the fallback reports no TLS state")
	}
}

// A reachable endpoint that does not speak MQTT over QUIC falls back at
// once, without a retry or a backoff (draft §6.6.1).
func TestFallbackOnALPNMismatch(t *testing.T) {
	srv := startServer(t, func(c *serverConfig) { c.nextProtos = []string{"h3"} })
	opts := srv.opts()
	opts.Fallback = &Fallback{URL: startTLSEcho(t, srv.root)}
	opts, fake := withClock(opts)

	conn, err := Dial(testContext(t), srv.url, opts)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	wantFallback(t, conn)
	if fake.Pending() != 0 {
		t.Fatal("an ALPN mismatch waited before falling back")
	}
}

// A network failure is retried after 1, 2 and 4 s before falling back;
// once fallen back, the next dial tries QUIC once (draft §6.6.1, §6.6.3).
func TestFallbackAfterNetworkFailures(t *testing.T) {
	hole := startBlackhole(t)
	root := testpki.NewRoot(t, "root")
	opts, fake := withClock(DialOpts{
		TLSConfig:        trusting(root),
		HandshakeTimeout: failingHandshake,
		Fallback:         &Fallback{URL: startTLSEcho(t, root)},
	})
	d := NewDialer(opts)
	// Long enough that the QUIC attempts get their full 10 s: half of
	// what is left.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type result struct {
		conn transport.Conn
		err  error
	}
	dialed := make(chan result, 1)
	go func() {
		conn, err := d.Dial(ctx, hole.url())
		dialed <- result{conn, err}
	}()
	for _, wait := range networkRetryBackoff {
		advanceWhenPending(t, fake, wait)
	}
	r := <-dialed
	if r.err != nil {
		t.Fatalf("dial: %v", r.err)
	}
	wantFallback(t, r.conn)
	r.conn.Close()
	if got, want := hole.attempts(), 1+len(networkRetryBackoff); got != want {
		t.Fatalf("QUIC attempts = %d, want %d", got, want)
	}

	conn, err := d.Dial(ctx, hole.url())
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	defer conn.Close()
	wantFallback(t, conn)
	if got, want := hole.attempts(), 2+len(networkRetryBackoff); got != want {
		t.Fatalf("QUIC attempts after the second dial = %d, want %d", got, want)
	}
}

// The QUIC attempts take at most half of what is left of the dial's
// context, so the fallback has time to connect.
func TestFallbackLeavesTheFallbackHalf(t *testing.T) {
	hole := startBlackhole(t)
	root := testpki.NewRoot(t, "root")
	opts, fake := withClock(DialOpts{
		TLSConfig:        trusting(root),
		HandshakeTimeout: failingHandshake,
		Fallback:         &Fallback{URL: startTLSEcho(t, root)},
	})
	// Half of 1 s leaves no room for the first 1 s backoff.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := Dial(ctx, hole.url(), opts)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	wantFallback(t, conn)
	if hole.attempts() != 1 || fake.Pending() != 0 {
		t.Fatalf("QUIC attempts = %d, %d timers pending; want one attempt and no wait", hole.attempts(), fake.Pending())
	}
}

// Without a Fallback a network failure ends the dial after one attempt:
// the client's reconnect loop retries.
func TestNoFallbackOneAttempt(t *testing.T) {
	hole := startBlackhole(t)
	opts, fake := withClock(DialOpts{HandshakeTimeout: failingHandshake})
	_, err := Dial(testContext(t), hole.url(), opts)
	if err == nil || !isNetworkFailure(err) {
		t.Fatalf("dial error = %v, want a network failure", err)
	}
	if hole.attempts() != 1 || fake.Pending() != 0 {
		t.Fatalf("QUIC attempts = %d, %d timers pending; want one attempt", hole.attempts(), fake.Pending())
	}
}

// The fallback is TLS only, never plain TCP (draft §6.6.2).
func TestFallbackNeverPlaintext(t *testing.T) {
	for _, raw := range []string{"mqtt://127.0.0.1:1883", "tcp://127.0.0.1:1883", "ws://127.0.0.1/mqtt"} {
		t.Run(raw, func(t *testing.T) {
			u := &url.URL{Scheme: scheme, Host: "127.0.0.1:14567"}
			_, err := Dial(testContext(t), u, DialOpts{Fallback: &Fallback{URL: raw}})
			if !errors.Is(err, ErrFallbackNotTLS) {
				t.Fatalf("dial error = %v, want ErrFallbackNotTLS", err)
			}
		})
	}
}

// A certificate QUIC cannot verify is an error, not a reason to fall
// back.
func TestNoFallbackOnVerificationError(t *testing.T) {
	srv := startServer(t)
	other := testpki.NewRoot(t, "other root")
	opts := DialOpts{TLSConfig: trusting(other), Fallback: &Fallback{URL: startTLSEcho(t, other)}}
	_, err := Dial(testContext(t), srv.url, opts)
	if !errors.As(err, new(x509.UnknownAuthorityError)) {
		t.Fatalf("dial error = %v, want the QUIC verification error", err)
	}
}

func TestFallbackDefaultURL(t *testing.T) {
	tests := []struct {
		host string
		want fallbackTarget
	}{
		{host: "broker.example", want: fallbackTarget{url: "mqtts://broker.example:8883", addr: "broker.example:8883", serverName: "broker.example"}},
		{host: "::1", want: fallbackTarget{url: "mqtts://[::1]:8883", addr: "[::1]:8883", serverName: "::1"}},
	}
	for _, tt := range tests {
		got, err := parseFallback(&Fallback{}, tt.host)
		if err != nil || *got != tt.want {
			t.Fatalf("parseFallback(%q) = %+v, %v; want %+v", tt.host, got, err, tt.want)
		}
	}
	got, err := parseFallback(&Fallback{URL: "tls://other.example"}, "broker.example")
	if err != nil || got.addr != "other.example:8883" || got.serverName != "other.example" {
		t.Fatalf("parseFallback without a port = %+v, %v", got, err)
	}
}

// The fallback's connection reports Session Taken Over to its Dialer, as
// a QUIC connection does.
func TestFallbackReportsTakeover(t *testing.T) {
	srv := startServer(t, func(c *serverConfig) { c.nextProtos = []string{"h3"} })
	opts := srv.opts()
	opts.Fallback = &Fallback{URL: startTLSEcho(t, srv.root)}
	opts, fake := withClock(opts)
	d := NewDialer(opts)
	ctx := testContext(t)

	conn, err := d.Dial(ctx, srv.url)
	if err != nil {
		t.Fatal(err)
	}
	transport.ObserveDisconnect(conn, byte(wire.ReasonSessionTakenOver))
	conn.Close()

	dialed := make(chan error, 1)
	go func() {
		c, err := d.Dial(ctx, srv.url)
		if err == nil {
			c.Close()
		}
		dialed <- err
	}()
	advanceWhenPending(t, fake, DefaultTakeoverBackoff)
	if err := <-dialed; err != nil {
		t.Fatalf("dial after the takeover backoff: %v", err)
	}
}
