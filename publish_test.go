// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

func TestPublishQoS0(t *testing.T) {
	received := make(chan *wire.Publish, 1)
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		if pub, ok := pkt.(*wire.Publish); ok {
			// Copy the bits we want to inspect before Release.
			received <- &wire.Publish{
				Topic:   string([]byte(pub.Topic)),
				Payload: append([]byte(nil), pub.Payload...),
				QoS:     pub.QoS,
			}
		}
		pkt.Release()
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Publish(context.Background(), PublishOptions{
		Topic:   "sensor/temp",
		Payload: []byte("42.7"),
		QoS:     0,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-received:
		if got.Topic != "sensor/temp" || string(got.Payload) != "42.7" || got.QoS != 0 {
			t.Errorf("got %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broker to see publish")
	}
}

func TestPublishQoS1(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		pub, ok := pkt.(*wire.Publish)
		if !ok {
			pkt.Release()
			return
		}
		id := pub.PacketID
		pkt.Release()
		_, _ = wire.WritePuback(c, wire.PubRespOpts{PacketID: id})
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, PublishOptions{
		Topic:   "events",
		Payload: []byte("hi"),
		QoS:     1,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestPublishQoS2(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		// PUBLISH → PUBREC
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		pub, ok := pkt.(*wire.Publish)
		if !ok {
			pkt.Release()
			return
		}
		id := pub.PacketID
		pkt.Release()
		_, _ = wire.WritePubrec(c, wire.PubRespOpts{PacketID: id})

		// PUBREL → PUBCOMP
		pkt, err = dec.ReadPacket()
		if err != nil {
			return
		}
		if rel, ok := pkt.(*wire.PubResp); !ok || rel.Type() != wire.PUBREL {
			t.Errorf("expected PUBREL, got %T %s", pkt, pkt.Type())
		}
		pkt.Release()
		_, _ = wire.WritePubcomp(c, wire.PubRespOpts{PacketID: id})
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, PublishOptions{
		Topic:   "events/important",
		Payload: []byte("once-and-only-once"),
		QoS:     2,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestPublishWithoutConnect(t *testing.T) {
	cli, _ := New(WithBroker("mqtt://127.0.0.1:1"))
	err := cli.Publish(context.Background(), PublishOptions{Topic: "x", QoS: 0})
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("got %v, want ErrNotConnected", err)
	}
}

func aliasOf(p testbroker.Packet) uint16 {
	v, _ := p.Properties().Uint16(wire.PropTopicAlias)
	return v
}

// Outbound aliases are opt-in.
func TestOutboundAliasesOffByDefault(t *testing.T) {
	max := uint16(10)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{TopicAliasMaximum: max})
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	for i := 0; i < 3; i++ {
		if err := cli.Publish(context.Background(), PublishOptions{Topic: "long/topic/name", QoS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.PUBLISH && (p.Topic == "" || aliasOf(p) != 0) {
			t.Fatalf("aliased PUBLISH without WithOutboundTopicAliases: %+v", p)
		}
	}
}

// With concurrent publishers and connections dropping mid-stream,
// a PUBLISH that uses an alias never reaches the broker before the
// PUBLISH that registered the alias on the same connection.
func TestOutboundAliasNeverPrecedesRegistration(t *testing.T) {
	max := uint16(4)
	accept := func(c *testbroker.Conn) { c.AcceptConnect(wire.ConnackOpts{TopicAliasMaximum: max}) }
	dropAfter := func(n int) testbroker.Script {
		return func(c *testbroker.Conn) {
			accept(c)
			for got := 0; got < n; {
				if _, ok := c.Await(wire.PUBLISH, 3*time.Second); !ok {
					return
				}
				got++
			}
		}
	}
	b := testbroker.New(t, dropAfter(150), dropAfter(150), dropAfter(150))
	b.SetFallback(func(c *testbroker.Conn) { accept(c); c.ServeAuto() })
	cli := tbClient(t, b, WithOutboundTopicAliases())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Go(func() {
			// Keep publishing across the three dropped connections.
			for i := 0; b.Accepted() < 4 || i < 100; i++ {
				err := cli.Publish(ctx, PublishOptions{Topic: fmt.Sprintf("alias/%d", (w+i)%6), Payload: []byte{byte(i)}})
				if errors.Is(err, ErrNotConnected) {
					time.Sleep(time.Millisecond)
				}
				if ctx.Err() != nil {
					t.Error("publishers did not see four connections")
					return
				}
			}
		})
	}
	wg.Wait()

	for i := 0; i < b.Accepted(); i++ {
		known := map[uint16]string{}
		for _, p := range b.Conn(i, 0).Log() {
			if p.Type != wire.PUBLISH {
				continue
			}
			alias := aliasOf(p)
			switch {
			case alias == 0:
			case alias > max:
				t.Fatalf("conn %d: alias %d above the broker's maximum %d", i, alias, max)
			case p.Topic != "":
				known[alias] = p.Topic
			case known[alias] == "":
				t.Fatalf("conn %d: alias %d used before it was registered on this connection", i, alias)
			}
		}
	}
	if b.Accepted() < 2 {
		t.Fatalf("only %d connections; the reconnect path was not exercised", b.Accepted())
	}
}

// Inbound alias violations close the connection with the spec's
// reason codes.
func TestInboundAliasViolations(t *testing.T) {
	tests := []struct {
		name   string
		send   func(c *testbroker.Conn)
		reason wire.ReasonCode
	}{
		{"above advertised maximum", func(c *testbroker.Conn) {
			c.Publish(wire.PublishOpts{Topic: "a", TopicAlias: 3})
		}, wire.ReasonTopicAliasInvalid},
		{"alias zero", func(c *testbroker.Conn) {
			c.Raw([]byte{0x30, 7, 0, 1, 'a', 3, 0x23, 0, 0})
		}, wire.ReasonTopicAliasInvalid},
		{"never registered", func(c *testbroker.Conn) {
			c.Publish(wire.PublishOpts{TopicAlias: 1})
		}, wire.ReasonProtocolError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.AcceptConnect(wire.ConnackOpts{})
				tt.send(c)
				if p := c.Expect(wire.DISCONNECT, 0); p.Reason != tt.reason {
					c.T.Errorf("DISCONNECT reason %#x, want %#x", byte(p.Reason), byte(tt.reason))
				}
			})
			tbClient(t, b, WithInboundTopicAliasMaximum(2))
			waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
		})
	}
}

func TestTopicAlias_OutboundAutoAllocation(t *testing.T) {
	// Broker advertises TopicAliasMaximum=5 in CONNACK. Then it
	// records two PUBLISHes on the same topic and asserts the
	// second uses the alias.
	pubs := make(chan seenPub, 4)

	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)

		// Read CONNECT.
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		pkt.Release()

		// Reply CONNACK advertising TopicAliasMaximum=5.
		max := uint16(5)
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode:        wire.ReasonSuccess,
			TopicAliasMaximum: max,
		})

		// Read two publishes from the client.
		for range 2 {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			if pub, ok := pkt.(*wire.Publish); ok {
				alias, _ := pub.Properties.Uint16(wire.PropTopicAlias)
				pubs <- seenPub{topic: string([]byte(pub.Topic)), alias: alias}
			}
			pkt.Release()
		}
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()), WithOutboundTopicAliases())
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	// Publish the same topic twice at QoS 0.
	for i := range 2 {
		if err := cli.Publish(context.Background(), PublishOptions{
			Topic:   "sensor/temp",
			Payload: fmt.Appendf(nil, "msg-%d", i),
			QoS:     0,
		}); err != nil {
			t.Fatal(err)
		}
	}

	first := mustRecvPub(t, pubs)
	second := mustRecvPub(t, pubs)

	// First: topic registered + alias assigned.
	if first.topic != "sensor/temp" || first.alias == 0 {
		t.Errorf("first publish: topic=%q alias=%d (want sensor/temp + non-zero alias)",
			first.topic, first.alias)
	}
	// Second: empty topic + same alias re-used.
	if second.topic != "" || second.alias != first.alias {
		t.Errorf("second publish: topic=%q alias=%d (want \"\" + alias %d)",
			second.topic, second.alias, first.alias)
	}
}

// seenPub is what the alias test's broker captures from each PUBLISH.
type seenPub struct {
	topic string
	alias uint16
}

func mustRecvPub(t *testing.T, ch chan seenPub) seenPub {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for publish")
		return seenPub{}
	}
}

func TestTopicAlias_InboundSubstitution(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		// SUBSCRIBE → SUBACK
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		sub, ok := pkt.(*wire.Subscribe)
		if !ok {
			pkt.Release()
			return
		}
		_, _ = wire.WriteSuback(c, wire.SubackOpts{
			PacketID:    sub.PacketID,
			ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
		})
		pkt.Release()

		// First PUBLISH: registers alias 7 → "sensor/temp".
		alias := uint16(7)
		_, _ = wire.WritePublish(c, wire.PublishOpts{
			Topic: "sensor/temp", Payload: []byte("first"),
			QoS: 1, PacketID: 100, TopicAlias: alias,
		})

		// Read PUBACK.
		if pkt, err := dec.ReadPacket(); err == nil {
			pkt.Release()
		}

		// Second PUBLISH: empty Topic, alias 7 — client must
		// substitute "sensor/temp" from the cache.
		_, _ = wire.WritePublish(c, wire.PublishOpts{
			Topic: "", Payload: []byte("second"),
			QoS: 1, PacketID: 101, TopicAlias: alias,
		})

		if pkt, err := dec.ReadPacket(); err == nil {
			pkt.Release()
		}
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()), WithInboundTopicAliasMaximum(10))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	ch, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sensor/+", QoS: 1}}, SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}

	got := []string{}
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case m := <-ch:
			got = append(got, m.Topic+":"+string(m.Payload))
			_ = m.Ack()
		case <-deadline:
			t.Fatalf("only got %d messages: %v", len(got), got)
		}
	}
	// Both messages should have been routed to the same handler under
	// "sensor/temp" — proving the alias was resolved before trie
	// matching.
	for _, m := range got {
		if !strings.HasPrefix(m, "sensor/temp:") {
			t.Errorf("topic not resolved via alias: %q", m)
		}
	}
}

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
func recordingHandler(t *testing.T, rec *publishRecorder) func(*testbroker.Broker, net.Conn) {
	t.Helper()
	return func(fb *testbroker.Broker, c net.Conn) {
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
		ready := 0
		for _, m := range cli.pool.members {
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
	fb := testbroker.NewRaw(t, recordingHandler(t, rec))

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
		if err := cli.Publish(context.Background(), PublishOptions{
			Topic: "alpha", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatalf("Publish alpha: %v", err)
		}
		if err := cli.Publish(context.Background(), PublishOptions{
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
	fb := testbroker.NewRaw(t, recordingHandler(t, rec))

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
		if err := cli.Publish(context.Background(), PublishOptions{
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
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
		if s.hasRPI && s.rpi != 1 {
			t.Errorf("CONNECT[%d] (%q) RequestProblemInformation = %d, want absent or 1",
				i, s.clientID, s.rpi)
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
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
			// Refuse the pool member: it retries in the background
			// and stays unconnected.
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
	if err := cli.Publish(context.Background(), PublishOptions{
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
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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

// countingBroker answers every connection, refusing PUBLISHes with
// refuse (0 accepts) and counting them; a client ID in dead never gets
// connected: its CONNECT is refused with 0x88 Server unavailable.
func countingBroker(t *testing.T, refuse wire.ReasonCode, dead func(clientID string) bool) (*testbroker.Broker, *atomic.Int32) {
	var publishes atomic.Int32
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		connect, ok := c.Await(wire.CONNECT, 0)
		if !ok {
			return
		}
		if dead != nil && dead(connect.Connect.ClientID) {
			_ = c.Write(func(w io.Writer) (int64, error) {
				return wire.WriteConnack(w, wire.ConnackOpts{ReasonCode: wire.ReasonServerUnavailable})
			})
			c.Close()
			return
		}
		if err := c.Write(func(w io.Writer) (int64, error) { return wire.WriteConnack(w, wire.ConnackOpts{}) }); err != nil {
			return
		}
		for {
			p, ok, err := c.Next(time.Minute)
			if !ok {
				if errors.Is(err, testbroker.ErrTimeout) {
					continue
				}
				return
			}
			switch p.Type {
			case wire.PUBLISH:
				publishes.Add(1)
				if p.QoS == 1 {
					c.Puback(p.PacketID, refuse)
				}
			case wire.PINGREQ:
				c.Write(wire.WritePingresp)
			case wire.DISCONNECT:
				return
			}
		}
	})
	return b, &publishes
}

// A broker refusal is the caller's answer; the pool does not try
// the message on its other members or the main connection.
func TestPoolPublishesOnceOnRefusal(t *testing.T) {
	b, publishes := countingBroker(t, wire.ReasonQuotaExceeded, nil)
	cli := tbClient(t, b, WithPublisherPool(3), WithStats())
	waitForPoolReady(t, cli, 3, 3*time.Second)
	err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1, Payload: []byte("x")})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("Publish = %v, want the broker's refusal", err)
	}
	if n := publishes.Load(); n != 1 {
		t.Fatalf("one Publish put %d PUBLISHes on the wire", n)
	}
	if cli.Stats().PoolFallbacks != 0 {
		t.Fatal("refusal fell back to the main connection")
	}
}

// A member that cannot send passes the message on, once.
func TestPoolFailsOverWhenAMemberIsDown(t *testing.T) {
	b, publishes := countingBroker(t, wire.ReasonSuccess, func(id string) bool { return strings.HasSuffix(id, "-pub-1") })
	cli := tbClient(t, b, WithPublisherPool(2))
	waitForPoolReady(t, cli, 1, 3*time.Second)
	const n = 6
	for range n {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 1})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := publishes.Load(); got != n {
		t.Fatalf("%d publishes put %d PUBLISHes on the wire", n, got)
	}
}

// A pool member refused at first keeps trying and joins the pool.
func TestPoolMemberRetriesFirstConnect(t *testing.T) {
	var refused atomic.Bool
	b, _ := countingBroker(t, wire.ReasonSuccess, func(id string) bool {
		return strings.HasSuffix(id, "-pub-1") && refused.CompareAndSwap(false, true)
	})
	cli := tbClient(t, b, WithPublisherPool(2))
	waitForPoolReady(t, cli, 2, 3*time.Second)
	if !refused.Load() {
		t.Fatal("the broker never refused the pool member")
	}
}

// Publish may run while the client connects and disconnects.
func TestPoolLifecycleRace(t *testing.T) {
	b, _ := countingBroker(t, wire.ReasonSuccess, nil)
	cli, err := New(WithBroker(b.URL()), WithClientID("race"), WithPublisherPool(2), WithLogger(quietLogger()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_ = cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 1})
				cancel()
			}
		})
	}
	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := cli.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		if err := cli.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	close(stop)
	wg.Wait()
}

// When OnConnectionDown stops the reconnects, the client is torn down
// as by Disconnect: subscriptions close, and Connect starts again.
func TestStopReconnectingClosesSubscriptions(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-c.Gone()
	})
	b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithOnConnectionDown(func() bool { return false }))
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "s"}})
	if err != nil {
		t.Fatal(err)
	}
	b.Conn(0, 0).Close()
	expectClosed(t, ch)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect after the supervisor stopped: %v", err)
	}
}

// A pooled publish follows the parent's publish options: with
// WithQoSDowngrade a QoS 2 publish to a Maximum QoS 1 broker goes out
// from a member at QoS 1 instead of failing there.
func TestPoolMemberDowngradesQoS(t *testing.T) {
	maxQoS := byte(1)
	type sent struct {
		clientID string
		qos      byte
	}
	got := make(chan sent, 1)
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		ci := c.AcceptConnect(wire.ConnackOpts{MaximumQoS: &maxQoS})
		if ci == nil {
			return
		}
		for {
			p, ok, err := c.Next(time.Minute)
			if !ok {
				if errors.Is(err, testbroker.ErrTimeout) {
					continue
				}
				return
			}
			switch p.Type {
			case wire.PUBLISH:
				got <- sent{ci.ClientID, p.QoS}
				if p.QoS == 1 {
					c.Puback(p.PacketID, wire.ReasonSuccess)
				}
			case wire.DISCONNECT:
				return
			}
		}
	})
	cli := tbClient(t, b, WithPublisherPool(2), WithQoSDowngrade())
	waitForPoolReady(t, cli, 2, 3*time.Second)
	if err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 2}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	s := <-got
	if !strings.Contains(s.clientID, "-pub-") || s.qos != 1 {
		t.Fatalf("PUBLISH from %q at QoS %d, want a pool member at QoS 1", s.clientID, s.qos)
	}
}

func TestPoolMemberInheritsPublishBehaviour(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_000, 0))
	cli, err := New(
		WithBroker("tcp://127.0.0.1:1"),
		WithClientID("inherit"),
		WithPublisherPool(2),
		WithQoSDowngrade(),
		WithSessionLossPolicy(SessionLossFail),
		WithOutboundTopicAliases(),
		WithLenientDecoding(),
		WithFollowServerRedirects(),
		WithReadBufferSize(4096),
		withClock(fake),
	)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range cli.pool.members {
		c := m.cfg
		if !c.QoSDowngrade || c.SessionLossPolicy != SessionLossFail || !c.OutboundTopicAliases ||
			!c.LenientDecoding || !c.FollowServerRedirects || c.ReadBufferSize != 4096 || c.clock != fake {
			t.Errorf("member %d config = %+v, want the parent's publish behaviour", i+1, *c)
		}
	}
}

// WithPublisherPool(1) moves publishing onto one publish-only
// connection beside the main one.
func TestPoolOfOne(t *testing.T) {
	rec := &publishRecorder{}
	fb := testbroker.NewRaw(t, recordingHandler(t, rec))
	cli, err := New(WithBroker(fb.URL()), WithClientID("one"), WithPublisherPool(1), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())
	if cli.pool == nil {
		t.Fatal("WithPublisherPool(1) built no pool")
	}
	waitForPoolReady(t, cli, 1, 3*time.Second)

	if err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if pubs := rec.snapshot(); len(pubs) != 1 || pubs[0].clientID != "one-pub-1" {
		t.Fatalf("publishes = %+v, want one from one-pub-1", pubs)
	}
}

// A client whose broker assigns its ClientID leaves its pool members'
// to the broker as well: a derived "-pub-N" would be the same in every
// process, and each would take the others' connections over.
func TestPoolOfAnUnnamedClient(t *testing.T) {
	b := testbroker.New(t)
	cli, err := New(WithBroker(b.URL()), WithPublisherPool(2), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())
	waitForPoolReady(t, cli, 2, 3*time.Second)

	for i := range 3 {
		c := b.Conn(i, time.Second)
		if c == nil {
			t.Fatalf("broker accepted %d connections, want 3", b.Accepted())
		}
		if id := c.Log()[0].Connect.ClientID; id != "" {
			t.Errorf("connection %d: CONNECT ClientID %q, want empty", i, id)
		}
	}
}

// Stats covers the pool's connections as well as the main one: the
// publishes a caller made through the pool count as the client's.
func TestStatsCoverThePool(t *testing.T) {
	b, _ := countingBroker(t, wire.ReasonSuccess, nil)
	cli := tbClient(t, b, WithPublisherPool(2), WithStats())
	waitForPoolReady(t, cli, 2, 3*time.Second)

	const n = 6
	for range n {
		if err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	var s Stats
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s = cli.Stats()
		if s.PublishesSent == n && s.PublishesAcked == n && s.Connects == 3 && s.PoolConnected == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s.PublishesSent != n || s.PublishesAcked != n {
		t.Errorf("PublishesSent = %d, PublishesAcked = %d; want %d each", s.PublishesSent, s.PublishesAcked, n)
	}
	if s.Connects != 3 || s.PoolConnected != 2 {
		t.Errorf("Connects = %d, PoolConnected = %d; want 3 and 2", s.Connects, s.PoolConnected)
	}
	if s.PoolFallbacks != 0 {
		t.Errorf("PoolFallbacks = %d with every member connected", s.PoolFallbacks)
	}
}

// A pool member reads its own acknowledgements, so a SubscribeCallback
// handler holding the main connection's read loop does not hold up a
// pooled publish.
func TestPooledPublishDoesNotWaitOnCallbackHandler(t *testing.T) {
	b := testbroker.New(t)
	cli := tbClient(t, b, WithPublisherPool(1))
	waitForPoolReady(t, cli, 1, 3*time.Second)

	entered := make(chan struct{}, 1)
	gate := make(chan struct{})
	defer close(gate)
	_, err := cli.SubscribeCallback(context.Background(), []TopicFilter{{Topic: "in"}}, func(*Message) {
		entered <- struct{}{}
		<-gate
	})
	if err != nil {
		t.Fatal(err)
	}
	var main *testbroker.Conn
	for i := range b.Accepted() {
		if c := b.Conn(i, time.Second); c != nil && c.Log()[0].Connect.ClientID == cli.ClientID() {
			main = c
		}
	}
	if main == nil {
		t.Fatal("no main connection at the broker")
	}
	if err := main.Publish(wire.PublishOpts{Topic: "in", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, PublishOptions{Topic: "out", QoS: 1}); err != nil {
		t.Fatalf("pooled publish while the main read loop is held: %v", err)
	}
}

// heldPubackBroker accepts every connection; the pool member's first
// PUBLISH is reported on got and acknowledged only once release closes.
func heldPubackBroker(t *testing.T, got chan<- uint16, release <-chan struct{}) *testbroker.Broker {
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		info := c.AcceptConnect(wire.ConnackOpts{})
		if info == nil {
			return
		}
		if !strings.HasSuffix(info.ClientID, "-pub-1") {
			c.ServeAuto()
			return
		}
		p, ok := c.Await(wire.PUBLISH, 0)
		if !ok {
			return
		}
		got <- p.PacketID
		select {
		case <-release:
		case <-c.Gone():
			return
		}
		_ = c.Puback(p.PacketID, wire.ReasonSuccess)
		c.ServeAuto()
	})
	return b
}

// Disconnect waits for publishes in flight on pool members: a member has
// no session to finish them later.
func TestDisconnectDrainsPooledPublishes(t *testing.T) {
	got := make(chan uint16, 1)
	release := make(chan struct{})
	cli := tbClient(t, heldPubackBroker(t, got, release), WithPublisherPool(1))
	waitForPoolReady(t, cli, 1, 3*time.Second)

	pubErr := make(chan error, 1)
	go func() { pubErr <- cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1}) }()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("the pooled publish never reached the broker")
	}
	discErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		discErr <- cli.Disconnect(ctx)
	}()
	select {
	case err := <-discErr:
		t.Fatalf("Disconnect returned (%v) while a pooled publish was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-pubErr; err != nil {
		t.Errorf("in-flight pooled publish = %v, want its PUBACK", err)
	}
	if err := <-discErr; err != nil {
		t.Errorf("Disconnect = %v", err)
	}
	if err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1}); !errors.Is(err, ErrClosed) {
		t.Errorf("Publish after Disconnect = %v, want ErrClosed", err)
	}
}

// Disconnect's ctx bounds the wait; a publish it cuts short returns
// ErrClosed.
func TestDisconnectDrainIsBoundedByCtx(t *testing.T) {
	got := make(chan uint16, 1)
	release := make(chan struct{})
	defer close(release)
	cli := tbClient(t, heldPubackBroker(t, got, release), WithPublisherPool(1))
	waitForPoolReady(t, cli, 1, 3*time.Second)

	pubErr := make(chan error, 1)
	go func() { pubErr <- cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1}) }()
	<-got
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cli.Disconnect(ctx) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Disconnect outlived its ctx")
	}
	select {
	case err := <-pubErr:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("cut-short publish = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the in-flight publish never returned")
	}
}

// A client connected again after Disconnect publishes through its pool.
func TestPoolPublishesAfterReconnect(t *testing.T) {
	b, publishes := countingBroker(t, wire.ReasonSuccess, nil)
	cli := tbClient(t, b, WithPublisherPool(1))
	waitForPoolReady(t, cli, 1, 3*time.Second)
	if err := cli.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForPoolReady(t, cli, 1, 3*time.Second)
	if err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1}); err != nil {
		t.Fatalf("Publish after reconnect = %v", err)
	}
	if n := publishes.Load(); n != 1 {
		t.Errorf("broker saw %d PUBLISHes, want 1", n)
	}
}

// A drain racing with publishers that back out of the closing pool
// neither hangs nor closes its channel twice, and admits nothing after.
func TestPoolDrainRacesWithPublishers(t *testing.T) {
	for range 500 {
		p := &publisherPool{}
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 50 {
					if p.enter() {
						p.exit()
					}
				}
			})
		}
		if err := p.drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if p.enter() {
			t.Fatal("a closed pool admitted a call")
		}
	}
}

// The members are connections of their own: while the main connection
// is down and reconnecting, publishing goes on through them.
func TestPoolPublishesWhileMainConnectionIsDown(t *testing.T) {
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		info := c.AcceptConnect(wire.ConnackOpts{})
		if info == nil {
			return
		}
		if strings.HasSuffix(info.ClientID, "-pub-1") {
			c.ServeAuto()
		}
		// The main connection closes at once, every time.
	})
	cli := tbClient(t, b, WithPublisherPool(1), WithReconnectBackoff(ConstantBackoff(time.Minute)))
	waitForPoolReady(t, cli, 1, 3*time.Second)
	deadline := time.Now().Add(3 * time.Second)
	for cli.Connected() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if cli.Connected() {
		t.Fatal("the main connection never dropped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 1}); err != nil {
		t.Fatalf("Publish with the main connection down = %v", err)
	}
}
