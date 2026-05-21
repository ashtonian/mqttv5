// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// publishRecorder is a fake-broker handler shared across N concurrent
// per-connection handlers. Each handler logs (clientID, topic) for
// every inbound PUBLISH so tests can assert routing distribution.
type publishRecorder struct {
	mu        sync.Mutex
	publishes []recordedPublish
}

type recordedPublish struct {
	clientID string
	topic    string
}

func (r *publishRecorder) record(clientID, topic string) {
	r.mu.Lock()
	r.publishes = append(r.publishes, recordedPublish{clientID, topic})
	r.mu.Unlock()
}

func (r *publishRecorder) snapshot() []recordedPublish {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedPublish, len(r.publishes))
	copy(out, r.publishes)
	return out
}

// recordingHandler reads CONNECT (capturing the ClientID), replies
// CONNACK, then loops recording PUBLISHes and auto-acking QoS 1.
func recordingHandler(t *testing.T, rec *publishRecorder) func(*fakeBroker, net.Conn) {
	t.Helper()
	return func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)

		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			t.Errorf("got %s, want CONNECT", pkt.Type())
			pkt.Release()
			return
		}
		// conn.ClientID aliases the frame buffer; clone it so the
		// captured value survives pkt.Release and subsequent frame
		// pool reuse.
		clientID := strings.Clone(conn.ClientID)
		pkt.Release()
		if _, err := wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}); err != nil {
			return
		}

		for {
			select {
			case <-fb.Done():
				return
			default:
			}
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			switch pkt.Type() {
			case wire.PUBLISH:
				pub := pkt.(*wire.Publish)
				// pub.Topic aliases the frame buffer; clone before
				// retaining or it dangles after pkt.Release.
				rec.record(clientID, strings.Clone(pub.Topic))
				if pub.QoS == 1 {
					_, _ = wire.WritePuback(c, wire.PubRespOpts{
						PacketID:   pub.PacketID,
						ReasonCode: wire.ReasonSuccess,
					})
				}
			case wire.PINGREQ:
				_, _ = wire.WritePingresp(c)
			case wire.DISCONNECT:
				pkt.Release()
				return
			}
			pkt.Release()
		}
	}
}

// waitForPoolReady blocks until pool members have all reached
// Connected = true, or the timeout expires.
func waitForPoolReady(t *testing.T, cli *Client, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cli.pubPool == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		ready := 0
		for _, m := range cli.pubPool.members {
			if m.Connected() {
				ready++
			}
		}
		if ready >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pool not ready within %v", timeout)
}

// TestPoolRouting_HashByTopic_SameTopicSameMember verifies that with
// PoolRoutingHashByTopic, repeated publishes to the same topic all
// land on the same pool member's ClientID.
func TestPoolRouting_HashByTopic_SameTopicSameMember(t *testing.T) {
	t.Parallel()
	rec := &publishRecorder{}
	fb := newFakeBroker(t, recordingHandler(t, rec))

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("hashtest"),
		WithPublisherPool(3),
		WithPublisherPoolRouting(PoolRoutingHashByTopic),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	waitForPoolReady(t, cli, 3, 3*time.Second)

	// Publish 5× to topic A, 5× to topic B. With hash-by-topic each
	// topic must collapse to a single member.
	for range 5 {
		if err := cli.Publish(context.Background(), wire.PublishOpts{
			Topic: "alpha", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatalf("Publish alpha: %v", err)
		}
		if err := cli.Publish(context.Background(), wire.PublishOpts{
			Topic: "beta", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatalf("Publish beta: %v", err)
		}
	}

	// Give the broker side time to record all publishes.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) >= 10 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	pubs := rec.snapshot()
	if len(pubs) != 10 {
		t.Fatalf("recorded %d publishes, want 10: %+v", len(pubs), pubs)
	}

	alphaClients := map[string]int{}
	betaClients := map[string]int{}
	for _, p := range pubs {
		switch p.topic {
		case "alpha":
			alphaClients[p.clientID]++
		case "beta":
			betaClients[p.clientID]++
		}
	}
	if len(alphaClients) != 1 {
		t.Fatalf("topic alpha hit %d distinct ClientIDs (want 1): %v", len(alphaClients), alphaClients)
	}
	if len(betaClients) != 1 {
		t.Fatalf("topic beta hit %d distinct ClientIDs (want 1): %v", len(betaClients), betaClients)
	}
}

// TestPoolRouting_RoundRobin_SpreadsAcrossMembers verifies that the
// default policy distributes consecutive publishes (to the same topic)
// across multiple pool members.
func TestPoolRouting_RoundRobin_SpreadsAcrossMembers(t *testing.T) {
	t.Parallel()
	rec := &publishRecorder{}
	fb := newFakeBroker(t, recordingHandler(t, rec))

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("rrtest"),
		WithPublisherPool(3),
		// PoolRoutingRoundRobin is the default — no WithPublisherPoolRouting needed.
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	waitForPoolReady(t, cli, 3, 3*time.Second)

	// Publish 9× to a single topic. With round-robin and 3 members
	// every member should receive at least one publish.
	for range 9 {
		if err := cli.Publish(context.Background(), wire.PublishOpts{
			Topic: "same/topic", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) >= 9 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	pubs := rec.snapshot()
	if len(pubs) != 9 {
		t.Fatalf("recorded %d publishes, want 9", len(pubs))
	}

	clients := map[string]int{}
	for _, p := range pubs {
		clients[p.clientID]++
	}
	// Round-robin across 3 members of 9 publishes: every pool
	// member must have received at least one. (We exclude the
	// parent ClientID because the parent only takes the fallback
	// path when all members are down, which isn't this test.)
	pubMembers := 0
	for id := range clients {
		if id != "rrtest" {
			pubMembers++
		}
	}
	if pubMembers < 3 {
		t.Fatalf("round-robin reached %d of 3 pool members (want all 3): %v", pubMembers, clients)
	}
}

// TestPoolMemberInheritsConnectProperties verifies that the v5 CONNECT
// property options on the parent Config propagate to every pool
// member's CONNECT. The test snapshots the per-member CONNECT and
// asserts each property is present.
func TestPoolMemberInheritsConnectProperties(t *testing.T) {
	const memberCount = 2

	// snapshot extracts the v5 fields synchronously while the frame
	// is still alive — Properties is a view over the frame buffer
	// and becomes invalid after Release.
	type connectSnapshot struct {
		clientID         string
		maxPacketSize    uint32
		hasMaxPacketSize bool
		topicAliasMax    uint16
		hasTopicAliasMax bool
		rri              byte
		hasRRI           bool
		rpi              byte
		hasRPI           bool
		userProperty     map[string]string
	}
	var snapsMu sync.Mutex
	snaps := []connectSnapshot{}
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			pkt.Release()
			return
		}
		// ClientID and user-property strings alias the frame buffer;
		// clone before Release or they dangle once the frame returns
		// to the pool and a subsequent CONNECT overwrites it.
		snap := connectSnapshot{clientID: strings.Clone(conn.ClientID), userProperty: map[string]string{}}
		snap.maxPacketSize, snap.hasMaxPacketSize = conn.Properties.Uint32(wire.PropMaximumPacketSize)
		snap.topicAliasMax, snap.hasTopicAliasMax = conn.Properties.Uint16(wire.PropTopicAliasMaximum)
		snap.rri, snap.hasRRI = conn.Properties.Byte(wire.PropRequestResponseInfo)
		snap.rpi, snap.hasRPI = conn.Properties.Byte(wire.PropRequestProblemInfo)
		for k, v := range conn.Properties.UserProperties() {
			snap.userProperty[strings.Clone(k)] = strings.Clone(v)
		}
		conn.Release()
		snapsMu.Lock()
		snaps = append(snaps, snap)
		snapsMu.Unlock()
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		<-fb.Done()
	})

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("inherit-test"),
		WithPublisherPool(memberCount),
		WithMaximumPacketSize(1024*1024),
		WithInboundTopicAliasMaximum(16),
		WithRequestResponseInformation(true),
		WithRequestProblemInformation(true),
		WithConnectUserProperty("env", "test"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	want := memberCount + 1
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapsMu.Lock()
		got := len(snaps)
		snapsMu.Unlock()
		if got >= want {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	snapsMu.Lock()
	defer snapsMu.Unlock()
	if len(snaps) < want {
		t.Fatalf("captured %d CONNECTs, want %d", len(snaps), want)
	}

	for i, s := range snaps {
		if !s.hasMaxPacketSize || s.maxPacketSize != 1024*1024 {
			t.Errorf("CONNECT[%d] (%q) MaximumPacketSize = (%d, %v), want (1048576, true)",
				i, s.clientID, s.maxPacketSize, s.hasMaxPacketSize)
		}
		if !s.hasTopicAliasMax || s.topicAliasMax != 16 {
			t.Errorf("CONNECT[%d] (%q) TopicAliasMaximum = (%d, %v), want (16, true)",
				i, s.clientID, s.topicAliasMax, s.hasTopicAliasMax)
		}
		if !s.hasRRI || s.rri != 1 {
			t.Errorf("CONNECT[%d] (%q) RequestResponseInformation = (%d, %v), want (1, true)",
				i, s.clientID, s.rri, s.hasRRI)
		}
		if !s.hasRPI || s.rpi != 1 {
			t.Errorf("CONNECT[%d] (%q) RequestProblemInformation = (%d, %v), want (1, true)",
				i, s.clientID, s.rpi, s.hasRPI)
		}
		if s.userProperty["env"] != "test" {
			t.Errorf("CONNECT[%d] (%q) missing env=test user property", i, s.clientID)
		}
	}
}

// TestPoolFallbackIncrementsStats verifies that a publish falling back
// from the pool (all members unhealthy) to the main connection
// increments Stats.PoolFallbacks when WithStats is enabled.
//
// The broker rejects every pool-member CONNECT with a non-success
// CONNACK so the pool members fail to connect and stay disconnected.
// The parent connects normally and handles the fallback publish.
func TestPoolFallbackIncrementsStats(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			pkt.Release()
			return
		}
		isPool := strings.Contains(conn.ClientID, "-pub-")
		conn.Release()
		if isPool {
			// Refuse the pool member with NotAuthorized — the client
			// reports ErrConnectRefused, m.started stays false, the
			// member never enters the supervisor.
			_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonNotAuthorized})
			return
		}
		// Parent: accept normally and PUBACK every QoS 1.
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			pub, ok := pkt.(*wire.Publish)
			if !ok {
				pkt.Release()
				continue
			}
			id := pub.PacketID
			pkt.Release()
			if id != 0 {
				_, _ = wire.WritePuback(c, wire.PubRespOpts{PacketID: id, ReasonCode: wire.ReasonSuccess})
			}
		}
	})

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("fallback-stats"),
		WithStats(),
		WithPublisherPool(2),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	// Pool members were refused, so they're unconnected. The publish
	// path must fall back to the main connection.
	if err := cli.Publish(context.Background(), wire.PublishOpts{
		Topic: "fallback/test", QoS: 1, Payload: []byte("x"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cli.Stats().PoolFallbacks >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("PoolFallbacks = %d, want >= 1", cli.Stats().PoolFallbacks)
}

// TestPoolMemberInheritsDialFunc verifies that a parent-configured
// DialFunc is propagated to pool members. Without this, a user on
// the WebSocket transport (or any non-built-in scheme) sees pool
// members fail to dial with "unsupported scheme".
func TestPoolMemberInheritsDialFunc(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		<-fb.Done()
	})

	var dialCount atomic.Int32
	dialFn := func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		dialCount.Add(1)
		// Defer to the built-in transport so the rest of the handshake
		// proceeds normally.
		return transport.Dial(ctx, u.String(), transport.DialOpts{})
	}

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("dialfunc-prop-test"),
		WithDialFunc(dialFn),
		WithPublisherPool(2),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if dialCount.Load() >= 3 { // 1 parent + 2 members
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("DialFunc invoked %d times, want >= 3 (parent + 2 members)", dialCount.Load())
}
