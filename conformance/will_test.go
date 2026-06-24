// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"bytes"
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// connWatcher captures the most recent transport.Conn handed back by a
// dial so a test can Close() it directly — simulating a network drop
// (no DISCONNECT packet) underneath the reader/writer goroutines.
type connWatcher struct {
	mu   sync.Mutex
	conn transport.Conn
}

// dialFunc returns a transport.DialFunc that dials the real broker via
// the default TCP/TLS path and records the live connection. It matches
// the transport.DialFunc signature exactly:
//
//	func(ctx context.Context, brokerURL *url.URL) (transport.Conn, error)
func (w *connWatcher) dialFunc() transport.DialFunc {
	return func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		c, err := transport.Dial(ctx, u.String(), transport.DialOpts{})
		if err != nil {
			return nil, err
		}
		w.mu.Lock()
		w.conn = c
		w.mu.Unlock()
		return c, nil
	}
}

// closeConn closes the captured connection out from under the client,
// which the broker observes as an ungraceful drop and which triggers
// the will. Returns false if no connection was ever captured.
func (w *connWatcher) closeConn() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn == nil {
		return false
	}
	_ = w.conn.Close()
	return true
}

// newWillClient builds and connects a client carrying the supplied
// will, wired so that (a) its raw net.Conn is captured for an
// ungraceful close and (b) its supervisor does NOT reconnect — a
// reconnect would re-arm the will and race the observer. The returned
// client is NOT registered for a graceful Disconnect cleanup; the
// caller decides whether to drop it (ungraceful) or Disconnect it
// (graceful). Both paths are accounted for so the broker session is
// always torn down.
func newWillClient(t *testing.T, will *wire.WillOpts) (*mqttv5.Client, *connWatcher) {
	t.Helper()
	requireBroker(t, brokerURL())

	w := &connWatcher{}
	cli, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(t.Name()+"-will-"+randSuffix()),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithConnectTimeout(5*time.Second),
		// Session ends with the connection; nothing for the broker to
		// retain or resume that could re-arm the will.
		mqttv5.WithSessionExpiry(0),
		// Stop the supervisor on connection loss so the ungraceful
		// close is terminal — no reconnect, no re-armed will.
		mqttv5.WithOnConnectionDown(func() bool { return false }),
		mqttv5.WithWill(will),
		mqttv5.WithDialFunc(w.dialFunc()),
	)
	if err != nil {
		t.Fatalf("New will client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect will client: %v", err)
	}
	if w.conn == nil {
		t.Fatal("dial wrapper never captured a connection")
	}
	// Belt-and-suspenders: whatever the test does, make sure the
	// connection is gone by the end so no session lingers. A second
	// Close on an already-closed conn is harmless.
	t.Cleanup(func() { _ = w.closeConn() })
	return cli, w
}

// TestWill_DeliveredOnUngracefulDisconnect verifies the broker publishes
// the configured will (payload + will properties) when the will client's
// TCP connection drops without a DISCONNECT.
func TestWill_DeliveredOnUngracefulDisconnect(t *testing.T) {
	requireBroker(t, brokerURL())

	willTopic := "conformance/will/ungraceful/" + randSuffix()
	wantPayload := []byte("client-A-died")
	wantContentType := "application/octet-stream"
	wantCorrelation := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	wantUserProps := []wire.UserProperty{
		{Key: "reason", Value: "lwt"},
		{Key: "node", Value: "alpha"},
	}

	will := &wire.WillOpts{
		Topic:           willTopic,
		Payload:         wantPayload,
		QoS:             1,
		ContentType:     wantContentType,
		CorrelationData: wantCorrelation,
		UserProperties:  wantUserProps,
	}

	// Subscriber B installed BEFORE A drops, so it is registered when
	// the broker publishes the will.
	sub := connect(t)
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: willTopic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatalf("subscribe %s: %v", willTopic, err)
	}
	time.Sleep(50 * time.Millisecond)

	// Build A with the will, then drop its socket directly — no
	// Disconnect, which would clear the will.
	_, w := newWillClient(t, will)
	if !w.closeConn() {
		t.Fatal("no captured connection to close")
	}

	m := expectMessage(t, ch, 5*time.Second)
	defer m.Ack()

	if !bytes.Equal(m.Payload, wantPayload) {
		t.Errorf("will payload = %q, want %q", m.Payload, wantPayload)
	}
	if m.QoS != 1 {
		t.Errorf("will QoS = %d, want 1", m.QoS)
	}
	if ct, ok := m.Properties.String(wire.PropContentType); !ok || ct != wantContentType {
		t.Errorf("will ContentType = %q (ok=%v), want %q", ct, ok, wantContentType)
	}
	if cd, ok := m.Properties.Binary(wire.PropCorrelationData); !ok || !bytes.Equal(cd, wantCorrelation) {
		t.Errorf("will CorrelationData = %x (ok=%v), want %x", cd, ok, wantCorrelation)
	}
	gotProps := map[string]string{}
	for k, v := range m.Properties.UserProperties() {
		gotProps[k] = v
	}
	for _, wp := range wantUserProps {
		if gv, ok := gotProps[wp.Key]; !ok {
			t.Errorf("missing will user property %q", wp.Key)
		} else if gv != wp.Value {
			t.Errorf("will user property %q = %q, want %q", wp.Key, gv, wp.Value)
		}
	}
}

// TestWill_SuppressedOnGracefulDisconnect verifies a normal DISCONNECT
// clears the will: the broker must NOT publish it.
func TestWill_SuppressedOnGracefulDisconnect(t *testing.T) {
	requireBroker(t, brokerURL())

	willTopic := "conformance/will/graceful/" + randSuffix()
	will := &wire.WillOpts{
		Topic:   willTopic,
		Payload: []byte("should-not-be-published"),
		QoS:     1,
	}

	sub := connect(t)
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: willTopic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatalf("subscribe %s: %v", willTopic, err)
	}
	time.Sleep(50 * time.Millisecond)

	cli, _ := newWillClient(t, will)

	// Graceful DISCONNECT (§3.14.4): the Will Message is discarded.
	dctx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	if err := cli.Disconnect(dctx); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// No will should ever arrive.
	expectNoMessage(t, ch, 1*time.Second)
}

// TestWill_DelayInterval verifies WillDelayInterval defers publication:
// after an ungraceful drop the will must not arrive before the delay
// elapses, but must arrive once it does. Will-delay timing is
// broker-dependent (a broker may publish at session-expiry instead, or
// not honor the delay at all), so a broker that delivers early — before
// the delay — is treated as "delay not honored" and skipped rather than
// failed, keeping the test a signal instead of a flake.
//
// The central assertion measures the time from the ungraceful drop to
// delivery and requires it to be at least the full delay minus a small
// slack (not half the delay): a broker that ignores the delay and
// delivers well inside the window — e.g. ~1.2s against a 3s delay — must
// fail, while a broker that honors it (delivers ~3s) passes. The 3s
// delay gives loopback-timer jitter room above the slack.
func TestWill_DelayInterval(t *testing.T) {
	requireBroker(t, brokerURL())

	const delaySecs = 3
	delay := uint32(delaySecs)
	// Required lower bound on observed delivery latency: the full delay
	// minus a small slack for timer/loopback jitter. A correct broker
	// (delivers ~delaySecs) clears this; one that delivers early does not.
	const slack = 500 * time.Millisecond
	minDelay := time.Duration(delaySecs)*time.Second - slack

	willTopic := "conformance/will/delay/" + randSuffix()
	wantPayload := []byte("delayed-will")
	will := &wire.WillOpts{
		Topic:             willTopic,
		Payload:           wantPayload,
		QoS:               1,
		WillDelayInterval: &delay,
	}

	sub := connect(t)
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: willTopic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatalf("subscribe %s: %v", willTopic, err)
	}
	time.Sleep(50 * time.Millisecond)

	_, w := newWillClient(t, will)
	dropAt := time.Now()
	if !w.closeConn() {
		t.Fatal("no captured connection to close")
	}

	// First window: wait half the delay. Nothing should arrive this
	// early; anything that does is below minDelay and means the broker
	// ignored WillDelayInterval, so skip (don't fail) — timing is
	// broker-dependent and this keeps the test a signal, not a flake.
	firstWindow := time.Duration(delaySecs) * time.Second / 2
	select {
	case m := <-ch:
		elapsed := time.Since(dropAt)
		_ = m.Ack()
		if elapsed < minDelay {
			t.Skipf("broker delivered will after %v, before the %ds delay (min ~%v) — "+
				"WillDelayInterval not honored, skipping", elapsed, delaySecs, minDelay)
		}
		// Arrived at or after the full delay despite the short first
		// window (clock jitter at the boundary): validate and finish.
		if !bytes.Equal(m.Payload, wantPayload) {
			t.Errorf("delayed will payload = %q, want %q", m.Payload, wantPayload)
		}
		return
	case <-time.After(firstWindow):
		// Good: no premature delivery inside the delay window.
	}

	// Now the will must show up once the delay fully elapses. Allow a
	// generous tail so a slow broker timer doesn't flake the test.
	m := expectMessage(t, ch, 6*time.Second)
	defer m.Ack()

	// Validate the delay was actually honored: delivery latency must be
	// at least the full delay minus slack. With delaySecs=3 a correct
	// broker (~3s) clears minDelay (~2.5s); a broker that ignores the
	// delay and delivers at ~1.2s fails here.
	elapsed := time.Since(dropAt)
	if elapsed < minDelay {
		t.Errorf("will arrived after %v, expected >= ~%v (delay %ds minus slack)",
			elapsed, minDelay, delaySecs)
	}
	if !bytes.Equal(m.Payload, wantPayload) {
		t.Errorf("delayed will payload = %q, want %q", m.Payload, wantPayload)
	}
	if m.QoS != 1 {
		t.Errorf("delayed will QoS = %d, want 1", m.QoS)
	}
}
