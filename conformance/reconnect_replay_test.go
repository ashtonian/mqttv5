// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// connCapture wraps the built-in dial path and records the most recent
// net.Conn handed to the client, so a test can Close() it directly to
// simulate an ungraceful network drop (no DISCONNECT). Each reconnect
// dial overwrites the captured conn — Latest always points at the live
// connection.
type connCapture struct {
	mu    sync.Mutex
	conn  net.Conn
	dials int
}

// DialFunc returns a transport.DialFunc that defers to the real TCP/TLS
// dial (transport.Dial, via the *url.URL's string form, so the
// default-port logic for mqtt:// is reused) and captures the resulting
// connection.
func (cc *connCapture) DialFunc() transport.DialFunc {
	return func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		c, err := transport.Dial(ctx, u.String(), transport.DialOpts{})
		if err != nil {
			return nil, err
		}
		cc.mu.Lock()
		cc.conn = c
		cc.dials++
		cc.mu.Unlock()
		return c, nil
	}
}

// dropLatest closes the most recently captured connection, forcing an
// ungraceful drop under the client's reader/writer. Returns false if no
// connection has been captured yet.
func (cc *connCapture) dropLatest() bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.conn == nil {
		return false
	}
	_ = cc.conn.Close()
	return true
}

func (cc *connCapture) dialCount() int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.dials
}

// TestReconnect_QoS1SurvivesConnectionDrop drives the headline QoS 1
// durability guarantee against a real broker: a QoS 1 message published
// across an ungraceful connection drop is replayed by the supervisor
// after it reconnects, delivered exactly once to a separate subscriber
// (payload byte-equal), and the publisher's Publish call ultimately
// returns success (PUBACK received).
//
// Mechanism: the publisher P keeps a non-expiring session
// (WithSessionExpiry(300) + WithCleanStartOnReconnect(false)) so the
// broker resumes its in-flight QoS 1 state across the reconnect, and a
// WithDialFunc wrapper captures P's live net.Conn so the test can
// Close() it to look like a network drop. P publishes a QoS 1 message
// and the test forces the drop around the publish; the supervisor
// reconnects on a short backoff and replays the unacked PUBLISH.
//
// DUP=1 on the replayed copy is the strict MQTT v5 §4.4 expectation, but
// whether the subscriber observes DUP depends on broker timing (a broker
// that PUBACKs P before the drop, or that delivers to S before P's
// replay lands, can surface the message with DUP=0). The strong,
// broker-independent invariants asserted here are: the message is
// delivered, exactly once, byte-equal, and P.Publish returns nil. See
// broker_caveats for the DUP nuance.
func TestReconnect_QoS1SurvivesConnectionDrop(t *testing.T) {
	requireBroker(t, brokerURL())

	topic := "conformance/reconnect-replay/" + randSuffix()
	want := []byte("qos1-survives-drop")

	// Subscriber S: durable enough to keep its subscription across the
	// whole test. It stays connected throughout; the QoS 1 publish must
	// reach it once after P's reconnect+replay.
	sub, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID("conformance-replay-sub-"+randSuffix()),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithConnectTimeout(5*time.Second),
		mqttv5.WithCleanStart(false),
		mqttv5.WithSessionExpiry(300),
	)
	if err != nil {
		t.Fatalf("New sub: %v", err)
	}
	if err := sub.Connect(context.Background()); err != nil {
		t.Fatalf("Connect sub: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = sub.Disconnect(ctx)
	})

	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(8))
	if err != nil {
		t.Fatalf("subscribe %s: %v", topic, err)
	}
	// Let the broker install the subscription before any publish races it.
	time.Sleep(100 * time.Millisecond)

	// Publisher P: stable ClientID + non-expiring session so the broker
	// resumes in-flight QoS 1 state after the drop. Short reconnect
	// backoff so the replay happens inside the test's timeout. The
	// connCapture DialFunc lets the test sever P's socket directly.
	cc := &connCapture{}
	pub, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID("conformance-replay-pub-"+randSuffix()),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithConnectTimeout(5*time.Second),
		mqttv5.WithCleanStart(false),
		mqttv5.WithCleanStartOnReconnect(false),
		mqttv5.WithSessionExpiry(300),
		mqttv5.WithReconnectBackoff(mqttv5.ConstantBackoff(100*time.Millisecond)),
		mqttv5.WithDialFunc(cc.DialFunc()),
	)
	if err != nil {
		t.Fatalf("New pub: %v", err)
	}
	if err := pub.Connect(context.Background()); err != nil {
		t.Fatalf("Connect pub: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pub.Disconnect(ctx)
	})
	if cc.dialCount() == 0 {
		t.Fatal("DialFunc was not invoked on Connect — capture wiring is broken")
	}

	// Publish QoS 1 in the background; for QoS 1, Publish blocks until
	// PUBACK, which (given the forced drop) only arrives after the
	// supervisor reconnects and replays. A generous timeout absorbs the
	// reconnect blip.
	pubDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		pubDone <- pub.Publish(ctx, wire.PublishOpts{
			Topic:   topic,
			Payload: want,
			QoS:     1,
		})
	}()

	// Force an ungraceful drop of P's connection around the publish.
	// Closing the captured socket under the reader/writer looks like a
	// network drop (not a graceful DISCONNECT), so the broker keeps the
	// QoS 1 message un-acked and the supervisor must replay it. A short
	// sleep first gives the PUBLISH a chance to be written before the
	// socket dies, exercising the replay path either way (un-acked
	// in-flight publish is re-sent on reconnect).
	time.Sleep(20 * time.Millisecond)
	if !cc.dropLatest() {
		t.Fatal("no captured connection to drop")
	}

	// The message must arrive exactly once at S, byte-equal. Collect for
	// a generous window, then assert no duplicate follows.
	m := expectMessage(t, ch, 15*time.Second)
	if string(m.Payload) != string(want) {
		t.Errorf("payload = %q, want %q", m.Payload, want)
	}
	if m.QoS != 1 {
		t.Errorf("QoS = %d, want 1", m.QoS)
	}
	// DUP is informational only — record it, don't gate on it (broker
	// timing dependent; see broker_caveats / the doc comment).
	t.Logf("delivered copy: DUP=%v QoS=%d (DUP is broker-timing dependent)", m.Dup, m.QoS)
	_ = m.Ack()

	// Exactly-once: no duplicate of the same payload should follow. A
	// replay that the broker forwarded twice (or a broken dedup) would
	// surface a second copy here.
	expectNoMessage(t, ch, 1*time.Second)

	// The supervisor must actually have reconnected to deliver the
	// PUBACK — i.e. it dialled more than once.
	if got := cc.dialCount(); got < 2 {
		t.Errorf("dialCount = %d, want >= 2 (supervisor should have reconnected)", got)
	}

	// P's Publish must ultimately succeed: nil return == PUBACK received
	// after the replay.
	select {
	case err := <-pubDone:
		if err != nil {
			t.Errorf("Publish returned error (expected PUBACK after replay): %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Publish did not complete (no PUBACK after reconnect+replay)")
	}
}
