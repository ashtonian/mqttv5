// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// rawSub is a minimal MQTT v5 connection driven through the wire codec
// directly (wire.WriteConnect / WriteSubscribe + wire.NewDecoder). It
// exists because the public Subscribe path never puts a Subscription
// Identifier on the SUBSCRIBE packet (the client-side nextSubID is an
// internal activeSubs map key, not an on-wire property — subscribe.go
// builds wire.SubscribeOpts{PacketID, Filters} and leaves
// SubscriptionIdentifier nil), so it cannot exercise the §3.4.2.3
// identifier echo. Speaking the wire format here lets the test set an
// explicit identifier and assert the broker mirrors that exact value.
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
// MQTT v5 §3.4.2.3 Subscription Identifier echo: when a SUBSCRIBE
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
// mirror a requested identifier per §3.4.2.3), never a skip. mosquitto
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
	if err := pub.Publish(t.Context(), wire.PublishOpts{
		Topic: topic, Payload: want1, QoS: 1,
	}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	m1 := sub.expectPublish(t, topic, want1)
	gotID, ok := m1.Properties.Varint(wire.PropSubscriptionIdentifier)
	m1.Release()
	if !ok {
		t.Errorf("first delivery dropped the SubscriptionIdentifier; broker advertised sub-ids available so it MUST echo the requested id=%d (§3.4.2.3)", wantID)
	} else if gotID != wantID {
		t.Errorf("first delivery SubscriptionIdentifier = %d, want %d (broker must echo the requested id)", gotID, wantID)
	}

	// Stability: a second matching PUBLISH carries the identical id.
	want2 := []byte("subid-second")
	if err := pub.Publish(t.Context(), wire.PublishOpts{
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
	if err := pub.Publish(t.Context(), wire.PublishOpts{
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
	if err := pub.Publish(t.Context(), wire.PublishOpts{
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
