// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"bytes"
	"context"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// TestReceiveMaximumHonoured publishes far more QoS 1 messages at once
// than the broker's Receive Maximum (mosquitto advertises 20, EMQX 32,
// HiveMQ 10, the strict HiveMQ 4) and checks the client never had more
// in flight than the broker allows and that every publish completes.
func TestReceiveMaximumHonoured(t *testing.T) {
	for _, url := range []string{brokerURL(), secondaryBrokerURL(), strictBrokerURL()} {
		t.Run(url, func(t *testing.T) {
			requireBroker(t, url)
			cli := connectTo(t, url, mqttv5.WithStats())
			info, ok := cli.ServerInfo()
			if !ok {
				t.Fatal("not connected")
			}
			t.Logf("broker Receive Maximum %d", info.ReceiveMaximum)
			limit := int(info.ReceiveMaximum)

			var maxInFlight atomic.Int64
			stop := make(chan struct{})
			var sampler sync.WaitGroup
			sampler.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					if n := int64(limit) - int64(cli.Stats().SendQuota); n > maxInFlight.Load() {
						maxInFlight.Store(n)
					}
					time.Sleep(50 * time.Microsecond)
				}
			})

			const n = 400
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Go(func() {
					if err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: "conformance/rm/" + randSuffix(), Payload: []byte{byte(i)}, QoS: 1}); err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()
			close(stop)
			sampler.Wait()
			if got := maxInFlight.Load(); got > int64(limit) || got < 1 {
				t.Fatalf("max in flight %d, Receive Maximum %d", got, limit)
			}
			if got := cli.Stats().PublishesAcked; got != n {
				t.Fatalf("%d of %d publishes acknowledged", got, n)
			}
		})
	}
}

// connectTo is connect against a specific broker.
func connectTo(t *testing.T, url string, opts ...mqttv5.Option) *mqttv5.Client {
	t.Helper()
	all := append([]mqttv5.Option{
		mqttv5.WithBroker(url),
		mqttv5.WithClientID(t.Name() + "-" + randSuffix()),
		mqttv5.WithConnectTimeout(5 * time.Second),
	}, opts...)
	cli, err := mqttv5.New(all...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = cli.Disconnect(ctx)
	})
	return cli
}

// TestSubscribe_NoLocal_SuppressesOwnPublishes verifies the MQTT v5
// §3.8.3.1 No-Local subscription option: when a client subscribes with
// NoLocal=true, the broker must NOT deliver that client's own matching
// PUBLISHes back to it. A second client subscribing to the same topic
// without No-Local still receives them.
//
// No-Local is a per-connection behavior — it only suppresses delivery
// when the same connection both publishes and subscribes. This test
// proves the broker honors the option byte we encode (the wire round
// trip is covered separately in wire/subscribe.go).
func TestSubscribe_NoLocal_SuppressesOwnPublishes(t *testing.T) {
	requireBroker(t, brokerURL())

	topic := "conformance/nolocal/" + randSuffix()

	// Client C subscribes WITH No-Local and is also the publisher: it
	// must not get its own messages echoed back. Use Subscribe directly
	// so we can set NoLocal on the filter (withSubscriber doesn't).
	c := connect(t)
	chC, _, err := c.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, NoLocal: true}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatalf("C subscribe %s (NoLocal): %v", topic, err)
	}

	// Client D subscribes to the SAME topic WITHOUT No-Local — it is a
	// separate connection, so it must still receive C's publish.
	d := connect(t)
	chD, _, err := d.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, NoLocal: false}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatalf("D subscribe %s: %v", topic, err)
	}

	// Brief pause so both subscriptions are installed broker-side
	// before the publish races them.
	time.Sleep(50 * time.Millisecond)

	want := []byte("nolocal-payload")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Publish(ctx, mqttv5.PublishOptions{
		Topic: topic, Payload: want, QoS: 1,
	}); err != nil {
		t.Fatalf("C publish: %v", err)
	}

	// D (separate connection, normal subscription) must receive it,
	// byte-equal and at the negotiated QoS.
	m := expectMessage(t, chD, 3*time.Second)
	if !bytes.Equal(m.Payload, want) || m.QoS != 1 {
		t.Errorf("D got QoS=%d payload=%q, want QoS=1 payload=%q", m.QoS, m.Payload, want)
	}
	if m.Topic != topic {
		t.Errorf("D topic = %q, want %q", m.Topic, topic)
	}
	_ = m.Ack()

	// C subscribed with No-Local, so its own publish must NOT come back.
	// Wait well past D's delivery to be sure the broker had time to
	// (wrongly) echo it if No-Local were broken.
	expectNoMessage(t, chC, 1500*time.Millisecond)
}

// TestOverlappingSubscriptionsOneCopyEach checks against real brokers
// that, with two overlapping subscriptions on one client, each receives
// the message exactly once. The broker sends one copy per
// subscription, each carrying that subscription's identifier.
func TestOverlappingSubscriptionsOneCopyEach(t *testing.T) {
	for _, url := range []string{brokerURL(), secondaryBrokerURL()} {
		t.Run(url, func(t *testing.T) {
			requireBroker(t, url)
			cli := connectTo(t, url)
			if info, _ := cli.ServerInfo(); !info.SubscriptionIdentifiersAvailable {
				t.Skip("broker does not support subscription identifiers")
			}
			topic := "conformance/overlap/" + randSuffix()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			wide, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "conformance/overlap/#", QoS: 1}}, mqttv5.SubBuffer(8))
			if err != nil {
				t.Fatal(err)
			}
			narrow, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(8))
			if err != nil {
				t.Fatal(err)
			}
			pub := connectTo(t, url)
			if err := pub.Publish(ctx, mqttv5.PublishOptions{Topic: topic, Payload: []byte("once"), QoS: 1}); err != nil {
				t.Fatal(err)
			}
			for name, ch := range map[string]<-chan *mqttv5.Message{"wide": wide, "narrow": narrow} {
				got := 0
				for done := false; !done; {
					select {
					case m := <-ch:
						_ = m.Ack()
						got++
					case <-time.After(500 * time.Millisecond):
						done = true
					}
				}
				if got != 1 {
					t.Errorf("%s subscription received %d copies, want 1", name, got)
				}
			}
		})
	}
}

// Retain As Published decides whether a live forward keeps the
// publisher's RETAIN flag. The option only shows on a live forward of a
// PUBLISH whose own RETAIN bit is set:
//
//   - RetainAsPublished=true: the broker forwards the publisher's RETAIN
//     flag verbatim. A live publish with RETAIN=1 is delivered RETAIN=1.
//   - RetainAsPublished=false (the spec default): the broker clears the
//     RETAIN flag on forwarded PUBLISHes. The same live RETAIN=1 publish
//     is delivered RETAIN=0.
//
// Both legs install the subscription FIRST on a fresh topic (so the
// later publish is a live forward, not a subscribe-time retained
// replay) and then publish an identical RETAIN=1 message. The legs
// differ ONLY in the RetainAsPublished option and produce OPPOSITE
// delivered RETAIN flags — that is what makes the option load-bearing.
//
// Per §3.3.1.3 a retained replay sent because a subscription was just
// established always carries RETAIN=1 regardless of RetainAsPublished,
// so the replay path cannot discriminate the option and is left to
// TestSubscribe_RetainHandling. Each leg clears its retained message
// afterward so it can't leak across the shared broker.
func TestSubscribe_RetainAsPublished(t *testing.T) {
	requireBroker(t, brokerURL())

	pub := connect(t)

	// --- RAP=true: live RETAIN=1 publish forwarded with RETAIN=1. ---
	trueTopic := "conformance/rap-true/" + randSuffix()
	subTrue := connect(t)
	chTrue, _, err := subTrue.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: trueTopic, QoS: 1, RetainAsPublished: true}},
		mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	// Let the broker install the subscription so the publish below is a
	// live forward against an existing subscription, not a replay.
	time.Sleep(50 * time.Millisecond)

	want := []byte("rap-live-true")
	if err := pub.Publish(context.Background(), mqttv5.PublishOptions{
		Topic: trueTopic, Payload: want, QoS: 1, Retain: true,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = pub.Publish(context.Background(), mqttv5.PublishOptions{
			Topic: trueTopic, QoS: 1, Retain: true, Payload: nil,
		})
	})

	mTrue := expectMessage(t, chTrue, 3*time.Second)
	if !mTrue.Retain {
		t.Error("RetainAsPublished=true: live RETAIN=1 publish delivered with RETAIN=0, want RETAIN=1")
	}
	if !bytes.Equal(mTrue.Payload, want) {
		t.Errorf("RetainAsPublished=true: payload = %q, want %q", mTrue.Payload, want)
	}
	_ = mTrue.Ack()

	// --- RAP=false: same live RETAIN=1 publish forwarded RETAIN cleared. ---
	falseTopic := "conformance/rap-false/" + randSuffix()
	subFalse := connect(t)
	chFalse, _, err := subFalse.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: falseTopic, QoS: 1, RetainAsPublished: false}},
		mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	live := []byte("rap-live-false")
	if err := pub.Publish(context.Background(), mqttv5.PublishOptions{
		Topic: falseTopic, Payload: live, QoS: 1, Retain: true,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = pub.Publish(context.Background(), mqttv5.PublishOptions{
			Topic: falseTopic, QoS: 1, Retain: true, Payload: nil,
		})
	})

	mFalse := expectMessage(t, chFalse, 3*time.Second)
	if mFalse.Retain {
		t.Error("RetainAsPublished=false: live RETAIN=1 publish delivered with RETAIN=1, want RETAIN=0")
	}
	if !bytes.Equal(mFalse.Payload, live) {
		t.Errorf("RetainAsPublished=false: payload = %q, want %q", mFalse.Payload, live)
	}
	_ = mFalse.Ack()
}

// Retain Handling decides whether a new subscription receives the
// stored retained message. Against a stored retained message:
//
//   - mode 0 (send retained at subscribe time): a brand-new subscription
//     receives the retained message immediately. Asserted on RETAIN=1
//     replay + payload.
//   - mode 2 (do not send retained at subscribe time): no retained
//     replay at all. Asserted via expectNoMessage.
//
// mode 1 (send retained only if the subscription did not already exist)
// depends on whether the subscription already existed; it is exercised
// here only as the "new subscription -> retained delivered" case, which
// for a fresh distinct filter behaves like mode 0.
//
// Each leg uses its own topic + freshly connected client so a prior
// leg's installed subscription can't satisfy the next leg's "is this
// subscription new?" decision.
func TestSubscribe_RetainHandling(t *testing.T) {
	requireBroker(t, brokerURL())

	pub := connect(t)
	want := []byte("rh-retained")

	// storeRetained publishes a retained message on a fresh topic and
	// returns it; the caller subscribes afterward. A cleanup clears the
	// retained message so it can't leak across the shared broker.
	storeRetained := func(t *testing.T) string {
		t.Helper()
		topic := "conformance/rh/" + randSuffix()
		if err := pub.Publish(context.Background(), mqttv5.PublishOptions{
			Topic: topic, Payload: want, QoS: 1, Retain: true,
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = pub.Publish(context.Background(), mqttv5.PublishOptions{
				Topic: topic, QoS: 1, Retain: true, Payload: nil,
			})
		})
		// Let the broker store the retained message before any
		// subscribe races it.
		time.Sleep(100 * time.Millisecond)
		return topic
	}

	t.Run("mode0_sendsRetainedAtSubscribe", func(t *testing.T) {
		topic := storeRetained(t)
		sub := connect(t)
		ch, _, err := sub.Subscribe(context.Background(),
			[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, RetainHandling: 0}},
			mqttv5.SubBuffer(4))
		if err != nil {
			t.Fatal(err)
		}
		m := expectMessage(t, ch, 3*time.Second)
		if !m.Retain {
			t.Error("RetainHandling=0: replay delivered with RETAIN=0, want RETAIN=1")
		}
		if !bytes.Equal(m.Payload, want) {
			t.Errorf("RetainHandling=0: payload = %q, want %q", m.Payload, want)
		}
		_ = m.Ack()
	})

	t.Run("mode1_sendsRetainedOnNewSubscription", func(t *testing.T) {
		topic := storeRetained(t)
		sub := connect(t)
		// Fresh client + fresh filter => the subscription is new, so
		// mode 1 must deliver the retained message (same observable
		// behavior as mode 0 for a first-time subscription).
		ch, _, err := sub.Subscribe(context.Background(),
			[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, RetainHandling: 1}},
			mqttv5.SubBuffer(4))
		if err != nil {
			t.Fatal(err)
		}
		m := expectMessage(t, ch, 3*time.Second)
		if !m.Retain {
			t.Error("RetainHandling=1 (new sub): replay delivered with RETAIN=0, want RETAIN=1")
		}
		if !bytes.Equal(m.Payload, want) {
			t.Errorf("RetainHandling=1 (new sub): payload = %q, want %q", m.Payload, want)
		}
		_ = m.Ack()
	})

	t.Run("mode2_suppressesRetainedAtSubscribe", func(t *testing.T) {
		topic := storeRetained(t)
		sub := connect(t)
		ch, _, err := sub.Subscribe(context.Background(),
			[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, RetainHandling: 2}},
			mqttv5.SubBuffer(4))
		if err != nil {
			t.Fatal(err)
		}
		// Mode 2 must NOT replay the retained message at subscribe time.
		expectNoMessage(t, ch, 1*time.Second)

		// Sanity: the subscription itself is live — a subsequent live
		// publish still flows. This guards against a false pass where
		// the subscribe silently failed rather than suppressing the
		// retained replay.
		live := []byte("rh-live-after-mode2")
		if err := pub.Publish(context.Background(), mqttv5.PublishOptions{
			Topic: topic, Payload: live, QoS: 1, Retain: false,
		}); err != nil {
			t.Fatal(err)
		}
		m := expectMessage(t, ch, 3*time.Second)
		if !bytes.Equal(m.Payload, live) {
			t.Errorf("RetainHandling=2: live payload = %q, want %q", m.Payload, live)
		}
		if m.Retain {
			t.Error("RetainHandling=2: live forward delivered with RETAIN=1, want RETAIN=0")
		}
		_ = m.Ack()
	})
}

// rawSub is a minimal MQTT v5 connection driven through the wire codec
// directly (wire.WriteConnect / WriteSubscribe + wire.NewDecoder). The
// client allocates Subscription Identifiers itself; speaking the wire
// format here lets the test choose an exact identifier and assert the
// broker mirrors that value (§3.3.4).
//
// This mirrors the existing fake-broker tests, which already drive raw
// connections via wire.NewDecoder(conn).ReadPacket() (see auth_test.go).
type rawSub struct {
	conn net.Conn
	dec  *wire.Decoder
	// subIDsAvail is the broker's CONNACK SubscriptionIdentifiersAvailable
	// (§3.2.2.3.12): true unless the broker explicitly advertised 0.
	subIDsAvail bool
}

// dialRawSub opens a raw TCP MQTT v5 connection, sends CONNECT, and
// reads CONNACK — capturing the SubscriptionIdentifiersAvailable
// capability. The caller drives SUBSCRIBE / acks directly.
func dialRawSub(t *testing.T) *rawSub {
	t.Helper()

	conn, err := net.DialTimeout("tcp", stripScheme(brokerURL()), 5*time.Second)
	if err != nil {
		t.Fatalf("dial broker %s: %v", brokerURL(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	if _, err := wire.WriteConnect(conn, wire.ConnectOpts{
		ClientID:   t.Name() + "-raw-" + randSuffix(),
		CleanStart: true,
		KeepAlive:  30,
	}); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}

	dec := wire.NewDecoder(conn)
	pkt, err := dec.ReadPacket()
	if err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}
	ca, ok := pkt.(*wire.Connack)
	if !ok {
		pkt.Release()
		t.Fatalf("expected CONNACK, got %s", pkt.Type())
	}
	if ca.ReasonCode.IsError() {
		pkt.Release()
		t.Fatalf("CONNACK refused: reason=%#02x", byte(ca.ReasonCode))
	}
	// Absent property means "supported" per §3.2.2.3.12; a present 0
	// means the broker disabled subscription identifiers.
	v, present := ca.Properties.Byte(wire.PropSubscriptionIDAvailable)
	subIDsAvail := !present || v != 0
	pkt.Release()

	return &rawSub{conn: conn, dec: dec, subIDsAvail: subIDsAvail}
}

// subscribe sends one SUBSCRIBE carrying subID (§3.8.2.1.2) for filters
// and waits for the SUBACK, asserting every filter was granted. subID
// must be >= 1 per §3.8.2.1.2.
func (r *rawSub) subscribe(t *testing.T, packetID uint16, subID uint32, filters ...wire.SubscribeFilter) {
	t.Helper()

	if _, err := wire.WriteSubscribe(r.conn, wire.SubscribeOpts{
		PacketID:               packetID,
		Filters:                filters,
		SubscriptionIdentifier: &subID,
	}); err != nil {
		t.Fatalf("write SUBSCRIBE (id=%d): %v", subID, err)
	}

	pkt, err := r.dec.ReadPacket()
	if err != nil {
		t.Fatalf("read SUBACK: %v", err)
	}
	defer pkt.Release()
	sa, ok := pkt.(*wire.Suback)
	if !ok {
		t.Fatalf("expected SUBACK, got %s", pkt.Type())
	}
	if sa.PacketID != packetID {
		t.Fatalf("SUBACK packet id = %d, want %d", sa.PacketID, packetID)
	}
	if len(sa.ReasonCodes) != len(filters) {
		t.Fatalf("SUBACK has %d reason codes, want %d", len(sa.ReasonCodes), len(filters))
	}
	for i, rc := range sa.ReasonCodes {
		if rc.IsError() {
			t.Fatalf("SUBACK filter[%d] (%q) refused: reason=%#02x", i, filters[i].Topic, byte(rc))
		}
	}
}

// expectPublish reads the next inbound PUBLISH within the deadline set
// on the connection, asserting topic + payload, and acks QoS 1 with a
// PUBACK so the broker considers it delivered. Non-PUBLISH control
// packets in between (none are expected on this idle connection) fail
// the test. Returns the decoded PUBLISH so the caller can inspect its
// Subscription Identifier; the caller MUST call Release.
func (r *rawSub) expectPublish(t *testing.T, wantTopic string, wantPayload []byte) *wire.Publish {
	t.Helper()

	pkt, err := r.dec.ReadPacket()
	if err != nil {
		t.Fatalf("read PUBLISH (%s): %v", wantTopic, err)
	}
	pub, ok := pkt.(*wire.Publish)
	if !ok {
		pkt.Release()
		t.Fatalf("expected PUBLISH, got %s", pkt.Type())
	}
	if pub.Topic != wantTopic {
		topic, payload := pub.Topic, append([]byte(nil), pub.Payload...)
		pub.Release()
		t.Fatalf("PUBLISH topic = %q payload = %q, want topic %q", topic, payload, wantTopic)
	}
	if !bytes.Equal(pub.Payload, wantPayload) {
		got := append([]byte(nil), pub.Payload...)
		pub.Release()
		t.Fatalf("PUBLISH payload = %q, want %q", got, wantPayload)
	}
	if pub.QoS == 1 {
		if _, err := wire.WritePuback(r.conn, wire.PubRespOpts{PacketID: pub.PacketID}); err != nil {
			pub.Release()
			t.Fatalf("write PUBACK: %v", err)
		}
	}
	return pub
}

// TestSubscribe_SubscriptionIdentifier_EchoedOnDelivery verifies the
// MQTT v5 Subscription Identifier echo (§3.3.4): when a SUBSCRIBE
// carries a Subscription Identifier (§3.8.2.1.2), every matching
// PUBLISH the broker dispatches to that subscription is tagged with the
// same identifier in PropSubscriptionIdentifier (§3.3.2.3.8).
//
// The test sets an EXPLICIT identifier on the wire (the public Subscribe
// path does not — subscribe.go leaves wire.SubscribeOpts.
// SubscriptionIdentifier nil), so it asserts the strongest invariant:
// the delivered identifier equals the exact value requested, on every
// matching PUBLISH.
//
// Gating: the broker's CONNACK SubscriptionIdentifiersAvailable flag
// (§3.2.2.3.12) is read on connect. If the broker advertises sub-ids
// disabled (a present 0), the test skips — the broker would SUBACK-
// reject the identifier and there is nothing to echo. Otherwise sub-ids
// are available and a MISSING echo is a hard failure (the broker MUST
// mirror a requested identifier per §3.3.4), never a skip. mosquitto
// 2.x and emqx both support subscription identifiers.
func TestSubscribe_SubscriptionIdentifier_EchoedOnDelivery(t *testing.T) {
	requireBroker(t, brokerURL())

	sub := dialRawSub(t)
	if !sub.subIDsAvail {
		t.Skip("conformance: broker advertised SubscriptionIdentifiersAvailable=0")
	}
	pub := connect(t)

	const wantID uint32 = 4242
	topic := "conformance/subid/" + randSuffix()
	sub.subscribe(t, 1, wantID, wire.SubscribeFilter{Topic: topic, QoS: 1})

	// First delivery: the requested identifier must come back verbatim.
	want1 := []byte("subid-first")
	if err := pub.Publish(t.Context(), mqttv5.PublishOptions{
		Topic: topic, Payload: want1, QoS: 1,
	}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	m1 := sub.expectPublish(t, topic, want1)
	gotID, ok := m1.Properties.Varint(wire.PropSubscriptionIdentifier)
	m1.Release()
	if !ok {
		t.Errorf("first delivery dropped the SubscriptionIdentifier; broker advertised sub-ids available so it MUST echo the requested id=%d (§3.3.4)", wantID)
	} else if gotID != wantID {
		t.Errorf("first delivery SubscriptionIdentifier = %d, want %d (broker must echo the requested id)", gotID, wantID)
	}

	// Stability: a second matching PUBLISH carries the identical id.
	want2 := []byte("subid-second")
	if err := pub.Publish(t.Context(), mqttv5.PublishOptions{
		Topic: topic, Payload: want2, QoS: 1,
	}); err != nil {
		t.Fatalf("publish second: %v", err)
	}
	m2 := sub.expectPublish(t, topic, want2)
	gotID2, ok := m2.Properties.Varint(wire.PropSubscriptionIdentifier)
	m2.Release()
	if !ok {
		t.Errorf("second delivery dropped the SubscriptionIdentifier present on the first (id=%d)", wantID)
	} else if gotID2 != wantID {
		t.Errorf("second delivery SubscriptionIdentifier = %d, want %d (must be stable per subscription)", gotID2, wantID)
	}
}

// TestSubscribe_SubscriptionIdentifier_RoutesByID verifies the §3.8.4
// routing invariant: when one session holds several subscriptions each
// carrying a distinct Subscription Identifier, a delivered PUBLISH is
// tagged with the identifier of the subscription it matched — so the
// receiver can route by id.
//
// Two filters with DISTINCT identifiers are installed on ONE connection
// (idA on topicA, idB on topicB) — one filter per SUBSCRIBE, since the
// Subscription Identifier is a packet-level property. The topics are
// non-overlapping: a publish to topicA matches only the idA
// subscription and a publish to topicB only the idB subscription, so
// each delivery carries exactly one Subscription Identifier and the
// first-occurrence Properties.Varint lookup is unambiguous. (The
// overlapping case — one PUBLISH matching two subscriptions — yields
// MULTIPLE Subscription Identifier properties per §3.8.4, which the
// public Properties.Varint only reports the first of; the
// non-overlapping form pins the routing invariant without depending on
// a repeated-property accessor.)
func TestSubscribe_SubscriptionIdentifier_RoutesByID(t *testing.T) {
	requireBroker(t, brokerURL())

	sub := dialRawSub(t)
	if !sub.subIDsAvail {
		t.Skip("conformance: broker advertised SubscriptionIdentifiersAvailable=0")
	}
	pub := connect(t)

	const (
		idA uint32 = 7
		idB uint32 = 9
	)
	suffix := randSuffix()
	topicA := "conformance/subid-route/a/" + suffix
	topicB := "conformance/subid-route/b/" + suffix

	sub.subscribe(t, 1, idA, wire.SubscribeFilter{Topic: topicA, QoS: 1})
	sub.subscribe(t, 2, idB, wire.SubscribeFilter{Topic: topicB, QoS: 1})

	// A publish to topicA must be tagged with idA only.
	wantA := []byte("route-a")
	if err := pub.Publish(t.Context(), mqttv5.PublishOptions{
		Topic: topicA, Payload: wantA, QoS: 1,
	}); err != nil {
		t.Fatalf("publish A: %v", err)
	}
	mA := sub.expectPublish(t, topicA, wantA)
	gotA, ok := mA.Properties.Varint(wire.PropSubscriptionIdentifier)
	mA.Release()
	if !ok {
		t.Errorf("topicA delivery dropped the SubscriptionIdentifier; want id=%d (§3.8.4)", idA)
	} else if gotA != idA {
		t.Errorf("topicA delivery SubscriptionIdentifier = %d, want %d (must carry the matched subscription's id)", gotA, idA)
	}

	// A publish to topicB must be tagged with idB only.
	wantB := []byte("route-b")
	if err := pub.Publish(t.Context(), mqttv5.PublishOptions{
		Topic: topicB, Payload: wantB, QoS: 1,
	}); err != nil {
		t.Fatalf("publish B: %v", err)
	}
	mB := sub.expectPublish(t, topicB, wantB)
	gotB, ok := mB.Properties.Varint(wire.PropSubscriptionIdentifier)
	mB.Release()
	if !ok {
		t.Errorf("topicB delivery dropped the SubscriptionIdentifier; want id=%d (§3.8.4)", idB)
	} else if gotB != idB {
		t.Errorf("topicB delivery SubscriptionIdentifier = %d, want %d (must carry the matched subscription's id, not topicA's)", gotB, idB)
	}
}

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
