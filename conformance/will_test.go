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
// newWillClient connects a client carrying will whose session lasts
// sessionExpiry seconds after its connection ends.
func newWillClient(t *testing.T, will *mqttv5.WillOptions, sessionExpiry uint32) (*mqttv5.Client, *connWatcher) {
	t.Helper()
	requireBroker(t, brokerURL())

	w := &connWatcher{}
	cli, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(t.Name()+"-will-"+randSuffix()),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithConnectTimeout(5*time.Second),
		mqttv5.WithSessionExpiry(sessionExpiry),
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
	wantUserProps := []mqttv5.UserProperty{
		{Key: "reason", Value: "lwt"},
		{Key: "node", Value: "alpha"},
	}

	will := &mqttv5.WillOptions{
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
	_, w := newWillClient(t, will, 0)
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
	if ct := m.Properties.ContentType(); ct != wantContentType {
		t.Errorf("will ContentType = %q, want %q", ct, wantContentType)
	}
	if cd := m.Properties.CorrelationData(); !bytes.Equal(cd, wantCorrelation) {
		t.Errorf("will CorrelationData = %x, want %x", cd, wantCorrelation)
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
	will := &mqttv5.WillOptions{
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

	cli, _ := newWillClient(t, will, 0)

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
// elapses, but must arrive once it does. The will client's session
// outlives the delay, since a session that ends publishes the will at
// once. Delivery must come no earlier than the full delay minus a small
// slack for timer and loopback jitter.
func TestWill_DelayInterval(t *testing.T) {
	requireBroker(t, brokerURL())

	const delaySecs = 3
	delay := uint32(delaySecs)
	// Brokers count the delay in whole seconds (mosquitto fires up to a
	// second early), so the floor is one second under the delay: still
	// far from the immediate publish of a broker that ignores it.
	minDelay := time.Duration(delaySecs-1) * time.Second

	willTopic := "conformance/will/delay/" + randSuffix()
	wantPayload := []byte("delayed-will")
	will := &mqttv5.WillOptions{
		Topic:         willTopic,
		Payload:       wantPayload,
		QoS:           1,
		DelayInterval: &delay,
	}

	sub := connect(t)
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: willTopic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatalf("subscribe %s: %v", willTopic, err)
	}
	time.Sleep(50 * time.Millisecond)

	// The broker publishes the will when the delay passes or the
	// session ends, whichever comes first (§3.1.3.2.2): the session
	// must outlive the delay for the delay to show.
	_, w := newWillClient(t, will, 60)
	dropAt := time.Now()
	if !w.closeConn() {
		t.Fatal("no captured connection to close")
	}

	// First window: half the delay, in which nothing may arrive.
	firstWindow := time.Duration(delaySecs) * time.Second / 2
	select {
	case m := <-ch:
		elapsed := time.Since(dropAt)
		_ = m.Ack()
		if elapsed < minDelay {
			t.Fatalf("will delivered %v after the drop, before the %ds delay", elapsed, delaySecs)
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

	elapsed := time.Since(dropAt)
	if elapsed < minDelay {
		t.Errorf("will arrived after %v, expected >= %v (delay %ds, counted in whole seconds)",
			elapsed, minDelay, delaySecs)
	}
	if !bytes.Equal(m.Payload, wantPayload) {
		t.Errorf("delayed will payload = %q, want %q", m.Payload, wantPayload)
	}
	if m.QoS != 1 {
		t.Errorf("delayed will QoS = %d, want 1", m.QoS)
	}
}
