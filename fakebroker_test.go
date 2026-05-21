// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"net"
	"sync"
	"testing"

	"github.com/ashtonian/mqttv5/wire"
)

// fakeBroker is a minimal TCP listener used by tests. Each accepted
// connection is handed to a per-test handler so the test author scripts
// the broker behaviour (read N packets, write specific replies, etc.).
//
// The broker is deliberately not a generic MQTT server — wiring up full
// broker semantics would couple the client tests to a second
// implementation. Per-test scripts keep the assertion surface tight.
type fakeBroker struct {
	ln      net.Listener
	addr    string
	handler func(*fakeBroker, net.Conn)

	mu      sync.Mutex
	closed  bool
	closeCh chan struct{}
}

// newFakeBroker accepts a handler that receives the broker pointer as
// its first arg — handlers commonly want to read <-fb.Done() to know
// when the test is tearing down, and a self-reference avoids the
// "fb captured by closure before assignment" footgun.
func newFakeBroker(t *testing.T, handler func(*fakeBroker, net.Conn)) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fb := &fakeBroker{
		ln:      ln,
		addr:    ln.Addr().String(),
		handler: handler,
		closeCh: make(chan struct{}),
	}
	go fb.serve()
	t.Cleanup(fb.Close)
	return fb
}

func (b *fakeBroker) serve() {
	for {
		c, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.handler(b, c)
	}
}

// URL returns the mqtt:// URL the client should dial.
func (b *fakeBroker) URL() string { return "mqtt://" + b.addr }

// Close shuts the listener. Safe to call multiple times.
func (b *fakeBroker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	close(b.closeCh)
	_ = b.ln.Close()
}

// Done returns a channel closed when Close is called. Per-connection
// handlers select on this to know when to bail.
func (b *fakeBroker) Done() <-chan struct{} { return b.closeCh }

// acceptConnect reads CONNECT and writes a Success CONNACK. Test
// authors call this as the standard handshake at the start of each
// per-connection handler.
func acceptConnect(t *testing.T, c net.Conn, dec *wire.Decoder) {
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
	if _, err := wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}); err != nil {
		t.Errorf("CONNACK write: %v", err)
	}
}
