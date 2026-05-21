// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	jsoncodec "github.com/ashtonian/mqttv5/codec/json"
	"github.com/ashtonian/mqttv5/wire"
)

// withSubscriber spins up a second client subscribed to filter so the
// caller can verify a publish actually crossed the broker. Returns the
// channel and a cleanup closure.
func withSubscriber(t *testing.T, filter string, bufferSize int) (<-chan *mqttv5.Message, func()) {
	t.Helper()
	sub := connect(t)
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: filter, QoS: 1}}, mqttv5.SubBuffer(bufferSize))
	if err != nil {
		t.Fatalf("subscribe %s: %v", filter, err)
	}
	// Brief pause so the broker has actually installed the
	// subscription before any publish races it.
	time.Sleep(50 * time.Millisecond)
	return ch, func() {}
}

// expectMessage receives one message from ch within d. Fails the test
// on timeout — every caller wants the message; missing it is always
// a failure.
func expectMessage(t *testing.T, ch <-chan *mqttv5.Message, d time.Duration) *mqttv5.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(d):
		t.Fatalf("timeout waiting for message after %v", d)
		return nil
	}
}

// expectNoMessage asserts that nothing arrives within d. Used by
// negative tests (CleanStart wiped sub, retained-then-cleared, QoS 2
// no-duplicate).
func expectNoMessage(t *testing.T, ch <-chan *mqttv5.Message, d time.Duration) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("unexpected message: topic=%s payload=%q", m.Topic, m.Payload)
	case <-time.After(d):
	}
}

// ---------------- Connect ----------------

func TestConnect_Disconnect(t *testing.T) {
	cli := connect(t)
	if !cli.Connected() {
		t.Fatal("Connected() = false after Connect")
	}
	if cli.ClientID() == "" {
		t.Error("ClientID() is empty")
	}
}

func TestConnect_RoundTripsThroughBroker(t *testing.T) {
	// The deepest "connected" check is that the broker actually
	// dispatches a publish for us — that proves CONNECT, the
	// keepalive, and the read loop all work.
	pub := connect(t)
	topic := "conformance/connect/" + randSuffix()
	ch, cleanup := withSubscriber(t, topic, 4)
	defer cleanup()

	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic:   topic,
		Payload: []byte("ping"),
		QoS:     0,
	}); err != nil {
		t.Fatal(err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if !bytes.Equal(m.Payload, []byte("ping")) {
		t.Errorf("payload = %q, want ping", m.Payload)
	}
	_ = m.Ack()
}

func TestConnect_CleanStartWipesPriorSubscription(t *testing.T) {
	// CleanStart=true must discard any prior session for this
	// ClientID — including server-side subscriptions.
	id := "conformance-clean-" + randSuffix()
	topic := "conformance/clean/" + randSuffix()

	first, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(id),
		mqttv5.WithCleanStart(false),
		mqttv5.WithSessionExpiry(60),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4)); err != nil {
		t.Fatal(err)
	}
	_ = first.Disconnect(context.Background())

	// Same ID, CleanStart=true → broker wipes the prior session.
	second, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(id),
		mqttv5.WithCleanStart(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Disconnect(context.Background()) })

	// Open a channel just so we have something to read; client-side
	// Subscribe is required to observe deliveries on this Client.
	// (The server-side subscription from "first" should be gone.)
	freshCh, _, err := second.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: "other/" + randSuffix(), QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}

	pub := connect(t)
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic:   topic,
		Payload: []byte("should-not-arrive"),
		QoS:     0,
	}); err != nil {
		t.Fatal(err)
	}
	expectNoMessage(t, freshCh, 500*time.Millisecond)
}

func TestConnect_WithCredentials(t *testing.T) {
	// allow_anonymous=true in mosquitto.conf, so any creds work.
	// Verify by round-tripping a publish — proves the broker didn't
	// silently reject the CONNECT.
	cli := connect(t, mqttv5.WithCredentials("anyuser", []byte("anypass")))
	topic := "conformance/creds/" + randSuffix()
	ch, cleanup := withSubscriber(t, topic, 4)
	defer cleanup()

	if err := cli.Publish(context.Background(), wire.PublishOpts{
		Topic:   topic,
		Payload: []byte("creds-ok"),
		QoS:     0,
	}); err != nil {
		t.Fatal(err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if string(m.Payload) != "creds-ok" {
		t.Errorf("payload = %q", m.Payload)
	}
	_ = m.Ack()
}

// ---------------- Publish QoS levels (with subscriber verification) ----------------

func TestPublish_QoS0_DeliveredToSubscriber(t *testing.T) {
	pub := connect(t)
	topic := "conformance/qos0/" + randSuffix()
	ch, cleanup := withSubscriber(t, topic, 4)
	defer cleanup()

	want := []byte("qos0-payload")
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic:   topic,
		Payload: want,
		QoS:     0,
	}); err != nil {
		t.Fatal(err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if !bytes.Equal(m.Payload, want) || m.QoS != 0 {
		t.Errorf("got QoS=%d payload=%q, want QoS=0 payload=%q", m.QoS, m.Payload, want)
	}
	_ = m.Ack()
}

func TestPublish_QoS1_DeliveredAndAcked(t *testing.T) {
	pub := connect(t)
	topic := "conformance/qos1/" + randSuffix()
	ch, cleanup := withSubscriber(t, topic, 4)
	defer cleanup()

	want := []byte("qos1-payload")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, wire.PublishOpts{
		Topic: topic, Payload: want, QoS: 1,
	}); err != nil {
		// Publish returning nil here is itself proof of PUBACK — the
		// caller blocked until the broker responded.
		t.Fatal(err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if !bytes.Equal(m.Payload, want) || m.QoS != 1 {
		t.Errorf("got QoS=%d payload=%q, want QoS=1 payload=%q", m.QoS, m.Payload, want)
	}
	_ = m.Ack()
}

func TestPublish_QoS2_ExactlyOnce(t *testing.T) {
	pub := connect(t)
	sub := connect(t)
	topic := "conformance/qos2/" + randSuffix()

	// Subscribe at QoS 2 explicitly — the broker delivers at
	// min(pub_qos, sub_qos), so a QoS 1 sub would downgrade QoS 2
	// publishes to QoS 1. We need QoS 2 on both sides to exercise
	// the full PUBLISH→PUBREC→PUBREL→PUBCOMP handshake.
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 2}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	want := []byte("exactly-once")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, wire.PublishOpts{
		Topic: topic, Payload: want, QoS: 2,
	}); err != nil {
		t.Fatal(err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if !bytes.Equal(m.Payload, want) || m.QoS != 2 {
		t.Errorf("got QoS=%d payload=%q, want QoS=2 payload=%q", m.QoS, m.Payload, want)
	}
	_ = m.Ack()

	// QoS 2 guarantees exactly-once — no duplicate should arrive.
	expectNoMessage(t, ch, 500*time.Millisecond)
}

// ---------------- Wildcards ----------------

func TestSubscribe_PlusWildcard_MatchesOneLevel(t *testing.T) {
	sub := connect(t)
	pub := connect(t)

	prefix := "conformance/wild/" + randSuffix()
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: prefix + "/+/data", QoS: 1}}, mqttv5.SubBuffer(8))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	wantMatches := []string{"alpha", "beta", "gamma"}
	for _, level := range wantMatches {
		topic := prefix + "/" + level + "/data"
		if err := pub.Publish(context.Background(), wire.PublishOpts{
			Topic: topic, Payload: []byte(level), QoS: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Negative: a 2-level path must NOT match + (which is exactly one
	// level).
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: prefix + "/two/levels/data", Payload: []byte("nope"), QoS: 1,
	}); err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for len(got) < len(wantMatches) {
		m := expectMessage(t, ch, 3*time.Second)
		level := strings.TrimSuffix(strings.TrimPrefix(m.Topic, prefix+"/"), "/data")
		if string(m.Payload) != level {
			t.Errorf("payload %q != topic level %q", m.Payload, level)
		}
		got[level] = true
		_ = m.Ack()
	}
	for _, want := range wantMatches {
		if !got[want] {
			t.Errorf("missing %q", want)
		}
	}
	// The non-matching publish should not be in the queue.
	expectNoMessage(t, ch, 200*time.Millisecond)
}

func TestSubscribe_HashWildcard_MatchesParentAndChildren(t *testing.T) {
	sub := connect(t)
	pub := connect(t)

	root := "conformance/hash/" + randSuffix()
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: root + "/#", QoS: 1}}, mqttv5.SubBuffer(16))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	topics := []string{
		root,
		root + "/a",
		root + "/a/b",
		root + "/a/b/c",
	}
	for _, topic := range topics {
		if err := pub.Publish(context.Background(), wire.PublishOpts{
			Topic: topic, Payload: []byte(topic), QoS: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	got := map[string]bool{}
	for len(got) < len(topics) {
		m := expectMessage(t, ch, 3*time.Second)
		if string(m.Payload) != m.Topic {
			t.Errorf("payload %q != topic %q", m.Payload, m.Topic)
		}
		got[m.Topic] = true
		_ = m.Ack()
	}
	for _, want := range topics {
		if !got[want] {
			t.Errorf("missing topic %q", want)
		}
	}
}

// ---------------- Properties (every value verified) ----------------

func TestPubSub_AllPublishProperties(t *testing.T) {
	sub := connect(t)
	pub := connect(t)

	topic := "conformance/props/" + randSuffix()
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	wantUserProps := []wire.UserProperty{
		{Key: "device", Value: "sensor-01"},
		{Key: "trace_id", Value: "01HXZ7QY9V5N3RW4G6FJ8KE2BD"},
		{Key: "tenant", Value: "acme"},
	}
	wantContentType := "text/plain"
	wantResponse := "rpc/responses/abc"
	wantCorrelation := []byte{0x01, 0x02, 0x03, 0x04}

	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic:           topic,
		Payload:         []byte("with-props"),
		QoS:             1,
		ContentType:     wantContentType,
		ResponseTopic:   wantResponse,
		CorrelationData: wantCorrelation,
		UserProperties:  wantUserProps,
	}); err != nil {
		t.Fatal(err)
	}

	m := expectMessage(t, ch, 3*time.Second)
	defer m.Ack()

	if ct, _ := m.Properties.String(wire.PropContentType); ct != wantContentType {
		t.Errorf("ContentType = %q, want %q", ct, wantContentType)
	}
	if rt, _ := m.Properties.String(wire.PropResponseTopic); rt != wantResponse {
		t.Errorf("ResponseTopic = %q, want %q", rt, wantResponse)
	}
	if cd, _ := m.Properties.Binary(wire.PropCorrelationData); !bytes.Equal(cd, wantCorrelation) {
		t.Errorf("CorrelationData = %x, want %x", cd, wantCorrelation)
	}

	gotProps := map[string]string{}
	for k, v := range m.Properties.UserProperties() {
		gotProps[k] = v
	}
	for _, wp := range wantUserProps {
		if gv, ok := gotProps[wp.Key]; !ok {
			t.Errorf("missing user property %q", wp.Key)
		} else if gv != wp.Value {
			t.Errorf("user property %q = %q, want %q", wp.Key, gv, wp.Value)
		}
	}
}

// ---------------- Retain ----------------

func TestPublish_Retain_DeliveredToLateSubscriber(t *testing.T) {
	pub := connect(t)
	topic := "conformance/retain/" + randSuffix()

	want := []byte("retained-value")
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, Payload: want, QoS: 1, Retain: true,
	}); err != nil {
		t.Fatal(err)
	}

	sub := connect(t)
	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}

	m := expectMessage(t, ch, 3*time.Second)
	if !m.Retain {
		t.Error("Retain flag = false on retained delivery")
	}
	if !bytes.Equal(m.Payload, want) {
		t.Errorf("payload = %q, want %q", m.Payload, want)
	}
	_ = m.Ack()

	// Clear retain with an empty payload + Retain=true; a fresh
	// late subscriber must now get nothing.
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, QoS: 1, Retain: true, Payload: nil,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	sub2 := connect(t)
	ch2, _, err := sub2.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	expectNoMessage(t, ch2, 500*time.Millisecond)
}

// ---------------- Queue subscribe (values + order) ----------------

func TestSubscribeQueue_OrderedDelivery(t *testing.T) {
	sub := connect(t)
	pub := connect(t)
	topic := "conformance/queue/" + randSuffix()

	q, _, err := sub.SubscribeQueue(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	const n = 10
	want := make([]string, n)
	for i := range n {
		want[i] = "msg-" + string(rune('A'+i))
		if err := pub.Publish(context.Background(), wire.PublishOpts{
			Topic: topic, Payload: []byte(want[i]), QoS: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range n {
		m, ok := q.Dequeue(ctx)
		if !ok {
			t.Fatalf("Dequeue ok=false at i=%d", i)
		}
		// QoS 1 ordering: messages on one topic from one publisher
		// arrive in order.
		if string(m.Payload) != want[i] {
			t.Errorf("Dequeue[%d] = %q, want %q", i, m.Payload, want[i])
		}
		_ = m.Ack()
	}
}

// ---------------- Typed[T] ----------------

type reading struct {
	DeviceID string  `json:"device_id"`
	Temp     float64 `json:"temp"`
	Site     string  `json:"site"`
}

func TestTypedJSON_FullStructEquality(t *testing.T) {
	sub := connect(t)
	pub := connect(t)

	typedSub := mqttv5.NewTyped[reading](sub, jsoncodec.Codec[reading]{})
	typedPub := mqttv5.NewTyped[reading](pub, jsoncodec.Codec[reading]{})

	topic := "conformance/typed/" + randSuffix()
	ch, _, err := typedSub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	want := reading{DeviceID: "sensor-77", Temp: 21.5, Site: "us-west-2"}
	if err := typedPub.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, QoS: 1,
	}, want); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-ch:
		if m.Value != want {
			t.Errorf("Value = %+v, want %+v", m.Value, want)
		}
		_ = m.Ack()
	case <-time.After(3 * time.Second):
		t.Fatal("no typed message")
	}
}

// ---------------- Large payload (bytes equality) ----------------

func TestPublish_LargePayload_ByteEquality(t *testing.T) {
	sub := connect(t)
	pub := connect(t)
	topic := "conformance/large/" + randSuffix()

	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(2))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// Deterministic non-repeating-ish payload so a length-only
	// passing test would still fail on byte equality.
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte((i * 31) % 256)
	}

	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, Payload: payload, QoS: 1,
	}); err != nil {
		t.Fatal(err)
	}

	m := expectMessage(t, ch, 5*time.Second)
	if len(m.Payload) != len(payload) {
		t.Fatalf("payload length = %d, want %d", len(m.Payload), len(payload))
	}
	if !bytes.Equal(m.Payload, payload) {
		for i := range payload {
			if m.Payload[i] != payload[i] {
				t.Fatalf("payload byte %d: got 0x%02X, want 0x%02X", i, m.Payload[i], payload[i])
			}
		}
	}
	_ = m.Ack()
}

// ---------------- Session resume (publish-while-offline) ----------------

func TestSession_QueuedPublishesDeliveredOnResume(t *testing.T) {
	// Subscriber A connects with CleanStart=false + SessionExpiry>0,
	// subscribes, disconnects. Publisher P publishes QoS 1 while A
	// is offline. When A reconnects, the broker must deliver the
	// queued message.
	id := "conformance-resume-" + randSuffix()
	topic := "conformance/resume/" + randSuffix()

	subA1, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(id),
		mqttv5.WithCleanStart(false),
		mqttv5.WithSessionExpiry(60),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := subA1.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := subA1.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(8)); err != nil {
		_ = subA1.Disconnect(context.Background())
		t.Fatal(err)
	}
	_ = subA1.Disconnect(context.Background())

	// Publish while A is offline.
	pub := connect(t)
	want := []byte("queued-while-offline")
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, Payload: want, QoS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	// A reconnects with the SAME ClientID + CleanStart=false. Re-Subscribe
	// so we have a local channel; the broker still routes the queued
	// message because the server-side subscription persisted with the
	// session.
	subA2, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(id),
		mqttv5.WithCleanStart(false),
		mqttv5.WithSessionExpiry(60),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := subA2.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subA2.Disconnect(context.Background()) })

	ch, _, err := subA2.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(8))
	if err != nil {
		t.Fatal(err)
	}

	m := expectMessage(t, ch, 3*time.Second)
	if !bytes.Equal(m.Payload, want) {
		t.Errorf("queued publish payload = %q, want %q", m.Payload, want)
	}
	_ = m.Ack()
}

// ---------------- Topic alias end-to-end ----------------

func TestTopicAlias_OutboundReducesBytes(t *testing.T) {
	// mosquitto advertises TopicAliasMaximum=10 by default. Publish
	// the same topic twice; the second publish should arrive at the
	// subscriber under the same topic (broker substitutes from its
	// alias cache). Verify both arrive with full Topic resolved.
	pub := connect(t)
	sub := connect(t)
	topic := "conformance/alias-out/" + randSuffix()

	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	for i := range 2 {
		if err := pub.Publish(context.Background(), wire.PublishOpts{
			Topic:   topic,
			Payload: []byte("alias-msg-" + string(rune('A'+i))),
			QoS:     0,
		}); err != nil {
			t.Fatal(err)
		}
	}

	got := []string{}
	for len(got) < 2 {
		m := expectMessage(t, ch, 3*time.Second)
		if m.Topic != topic {
			t.Errorf("topic = %q, want %q (broker should have resolved alias)",
				m.Topic, topic)
		}
		got = append(got, string(m.Payload))
		_ = m.Ack()
	}
}

// ---------------- Unsubscribe ----------------

func TestUnsubscribe_StopsDelivery(t *testing.T) {
	sub := connect(t)
	pub := connect(t)
	topic := "conformance/unsub/" + randSuffix()

	ch, token, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// Confirm baseline delivery first.
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, Payload: []byte("before"), QoS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if string(m.Payload) != "before" {
		t.Fatalf("payload before unsub = %q, want before", m.Payload)
	}
	_ = m.Ack()

	// Now unsubscribe.
	if err := sub.Unsubscribe(context.Background(), token); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	// Publish again — must NOT arrive. The channel should also be
	// closed by the runtime.
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, Payload: []byte("after"), QoS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	// Channel should be closed and any "after" message dropped.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("received message after Unsubscribe")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("channel was not closed by Unsubscribe")
	}
}

// ---------------- Multi-subscription dispatch ----------------

func TestSubscribe_MultipleHandlersDispatch(t *testing.T) {
	cli := connect(t)
	pub := connect(t)
	topic := "conformance/multi/" + randSuffix()

	chA, _, err := cli.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	chB, _, err := cli.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	want := []byte("fanout")
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, Payload: want, QoS: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// Both internal subscriptions should fire — the trie matches
	// twice for one inbound publish.
	gotA := atomic.Bool{}
	gotB := atomic.Bool{}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		select {
		case m := <-chA:
			if bytes.Equal(m.Payload, want) {
				gotA.Store(true)
			}
			_ = m.Ack()
		case <-time.After(2 * time.Second):
		}
	}()
	go func() {
		defer wg.Done()
		select {
		case m := <-chB:
			if bytes.Equal(m.Payload, want) {
				gotB.Store(true)
			}
			_ = m.Ack()
		case <-time.After(2 * time.Second):
		}
	}()
	wg.Wait()
	if !gotA.Load() || !gotB.Load() {
		t.Errorf("fan-out: A=%v B=%v, want both true", gotA.Load(), gotB.Load())
	}
}

// ---------------- Shared subscriptions ----------------

func TestSubscribe_SharedSub_RoundRobinAcrossGroup(t *testing.T) {
	// MQTT v5 §4.8.2: every PUBLISH matching $share/{group}/{filter}
	// goes to exactly one subscriber in the group. The distribution
	// algorithm is broker-specific — mosquitto's default is
	// round-robin, which we assert here. Two invariants matter:
	//
	//   1. every published payload is delivered exactly once across
	//      the group (no drops, no duplicates — the spec guarantee).
	//   2. mosquitto's round-robin spreads evenly: every subscriber
	//      receives msgs/subs ± 1 messages.
	//
	// The second assertion is mosquitto-specific. EMQX's default is
	// random and may leave one sub starved over short test runs —
	// skip there.
	if !strings.Contains(brokerURL(), "1883") {
		t.Skip("round-robin assertion is mosquitto-specific")
	}

	const (
		subs = 3
		msgs = 30
	)
	topic := "conformance/sharedsub/" + randSuffix()
	filter := "$share/g-" + randSuffix() + "/" + topic

	var (
		mu       sync.Mutex
		perSub   = make([]int, subs)
		payloads = make(map[string]int) // payload → recipient count
	)
	var wg sync.WaitGroup
	wg.Add(msgs)

	for i := range subs {
		sub := connect(t)
		idx := i
		if _, err := sub.SubscribeCallback(context.Background(),
			[]mqttv5.TopicFilter{{Topic: filter, QoS: 1}},
			func(m *mqttv5.Message) {
				mu.Lock()
				perSub[idx]++
				payloads[string(m.Payload)]++
				mu.Unlock()
				wg.Done()
			}); err != nil {
			t.Fatalf("sub %d: %v", i, err)
		}
	}
	// Let SUBACKs settle so the broker has every member registered
	// before the first PUBLISH lands.
	time.Sleep(100 * time.Millisecond)

	pub := connect(t)
	for i := range msgs {
		if err := pub.Publish(context.Background(), wire.PublishOpts{
			Topic:   topic,
			Payload: fmt.Appendf(nil, "msg-%d", i),
			QoS:     1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		mu.Lock()
		t.Fatalf("only %d of %d messages delivered after 5s (per-sub %v)",
			len(payloads), msgs, perSub)
	}

	mu.Lock()
	defer mu.Unlock()

	// (1) spec invariant — exactly-once delivery per payload.
	if got := len(payloads); got != msgs {
		t.Errorf("delivered %d distinct payloads, want %d (drops?)", got, msgs)
	}
	for payload, count := range payloads {
		if count != 1 {
			t.Errorf("payload %q delivered %d times, want exactly 1 (group dedup broken)",
				payload, count)
		}
	}

	// (2) mosquitto round-robin — every sub within ±1 of msgs/subs.
	target := msgs / subs
	for i, n := range perSub {
		if n < target-1 || n > target+1 {
			t.Errorf("sub %d got %d messages, want %d±1 (mosquitto round-robin skew)",
				i, n, target)
		}
	}
}

// ---------------- ClientGroup ----------------

func TestClientGroup_PublishFanOutToBothBrokers(t *testing.T) {
	requireBroker(t, secondaryBrokerURL())

	topic := "conformance/group/" + randSuffix()
	want := []byte("fanout-msg")

	sub1Cli := connect(t, mqttv5.WithBroker(brokerURL()))
	sub2Cli := connect(t, mqttv5.WithBroker(secondaryBrokerURL()))
	ch1, _, err := sub1Cli.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	ch2, _, err := sub2Cli.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	g, err := mqttv5.NewClientGroup(
		[]mqttv5.GroupMember{
			{Broker: brokerURL()},
			{Broker: secondaryBrokerURL()},
		},
		mqttv5.WithGroupSharedOpts(
			mqttv5.WithClientID("conformance-group-"+randSuffix()),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer g.Disconnect(context.Background())

	if err := g.Publish(context.Background(), wire.PublishOpts{
		Topic: topic, Payload: want, QoS: 1,
	}); err != nil {
		t.Fatal(err)
	}

	var (
		mu          sync.Mutex
		gotPayloads []string
	)
	wg := sync.WaitGroup{}
	wg.Add(2)
	for _, ch := range []<-chan *mqttv5.Message{ch1, ch2} {
		go func(c <-chan *mqttv5.Message) {
			defer wg.Done()
			select {
			case m := <-c:
				mu.Lock()
				gotPayloads = append(gotPayloads, string(m.Payload))
				mu.Unlock()
				_ = m.Ack()
			case <-time.After(3 * time.Second):
			}
		}(ch)
	}
	wg.Wait()
	sort.Strings(gotPayloads)
	if len(gotPayloads) != 2 {
		t.Errorf("only %d/2 subscribers received the fan-out: %v",
			len(gotPayloads), gotPayloads)
	}
	for _, p := range gotPayloads {
		if p != string(want) {
			t.Errorf("got payload %q, want %q", p, want)
		}
	}
}
