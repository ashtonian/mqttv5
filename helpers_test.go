// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// tbClient connects a client to a testbroker with fast reconnects and a
// silent logger.
func tbClient(t *testing.T, b *testbroker.Broker, opts ...Option) *Client {
	t.Helper()
	base := []Option{
		WithBroker(b.URL()),
		WithClientID("tb-" + t.Name()),
		WithReconnectBackoff(ConstantBackoff(10 * time.Millisecond)),
		WithLogger(quietLogger()),
		WithKeepAlive(60),
	}
	c, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Disconnect(ctx)
	})
	return c
}

func recvMsg(t *testing.T, ch <-chan *Message, d time.Duration) *Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("channel closed")
		}
		return m
	case <-time.After(d):
		t.Fatalf("no message within %v", d)
		return nil
	}
}

// withClock injects a time source (normally a *clock.Fake) for tests.
func withClock(c clock.Clock) Option {
	return func(cfg *Config) error {
		cfg.clock = c
		return nil
	}
}

// waitLog waits until conn has recorded n packets of type pt.
func waitLog(t *testing.T, conn *testbroker.Conn, pt wire.PacketType, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for _, p := range conn.Log() {
			if p.Type == pt {
				count++
			}
		}
		if count >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("fewer than %d %s recorded", n, pt)
}

// noLeaks records how many goroutines run now. The returned check
// fails t if more are running once exiting ones have had time to
// finish, and prints every goroutine's stack. Take the baseline before
// starting anything the test owns, and check after tearing it down.
func noLeaks(t *testing.T) func() {
	t.Helper()
	before := runtime.NumGoroutine()
	return func() {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for runtime.NumGoroutine() > before {
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				t.Fatalf("%d goroutines before, %d after teardown:\n%s", before, runtime.NumGoroutine(), buf[:n])
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// acceptConnect reads CONNECT and writes a Success CONNACK. Test
// authors call this as the standard handshake at the start of each
// per-connection handler.
func acceptConnect(t *testing.T, c net.Conn, dec *wire.Decoder) {
	t.Helper()
	acceptConnectWith(t, c, dec, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
}

// acceptConnectWith reads CONNECT and replies with the given CONNACK.
func acceptConnectWith(t *testing.T, c net.Conn, dec *wire.Decoder, opts wire.ConnackOpts) {
	t.Helper()
	pkt, err := dec.ReadPacket()
	if err != nil {
		t.Errorf("CONNECT read: %v", err)
		return
	}
	if pkt.Type() != wire.CONNECT {
		t.Errorf("got %s, want CONNECT", pkt.Type())
	}
	pkt.Release()
	if _, err := wire.WriteConnack(c, opts); err != nil {
		t.Errorf("CONNACK write: %v", err)
	}
}

func expectClosed(t *testing.T, ch <-chan *Message) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("received a message on a subscription that should be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed")
	}
}
