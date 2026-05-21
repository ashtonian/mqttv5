// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// testJSONCodec is a tiny inline json Codec[T] used only by api_test.go.
// The shipping json codec lives in its own submodule
// (codec/json/json.go) so importing it would pull the codec submodule
// into the main module's test dependency graph — the inline version
// keeps main stdlib-only.
type testJSONCodec[T any] struct{}

func (testJSONCodec[T]) Encode(v T) ([]byte, error) { return json.Marshal(v) }
func (testJSONCodec[T]) Decode(b []byte) (T, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}

// pushPublishBroker is a fakeBroker handler factory that pushes one
// PUBLISH on the given topic shortly after CONNACK + SUBSCRIBE.
func pushPublishBroker(t *testing.T, payload []byte) func(*fakeBroker, net.Conn) {
	return func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

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
		topic := sub.Filters[0].Topic
		// Resolve the topic for the PUBLISH — drop any wildcards.
		pubTopic := topic
		if strings.ContainsAny(topic, "+#") {
			pubTopic = strings.ReplaceAll(topic, "+", "x")
			pubTopic = strings.ReplaceAll(pubTopic, "/#", "/x")
		}
		pkt.Release()

		_, _ = wire.WritePublish(c, wire.PublishOpts{
			Topic:    pubTopic,
			Payload:  payload,
			QoS:      1,
			PacketID: 100,
		})

		// Read PUBACK and hold the connection open.
		pkt, err = dec.ReadPacket()
		if err == nil {
			pkt.Release()
		}
		<-fb.Done()
	}
}

// ---------------- Subscribe (channel) ----------------

func TestSubscribeChannel(t *testing.T) {
	fb := newFakeBroker(t, pushPublishBroker(t, []byte("hello")))

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	ch, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sensor/+", QoS: 1}}, SubBuffer(4))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-ch:
		if string(msg.Payload) != "hello" {
			t.Errorf("payload = %q, want hello", msg.Payload)
		}
		// Manual ack required for channel subs.
		if err := msg.Ack(); err != nil {
			t.Errorf("Ack: %v", err)
		}
		// Ack is idempotent.
		if err := msg.Ack(); err != nil {
			t.Errorf("Ack (2nd call): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message arrived on channel")
	}
}

func TestSubscribeChannelClosedOnUnsubscribe(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			switch p := pkt.(type) {
			case *wire.Subscribe:
				_, _ = wire.WriteSuback(c, wire.SubackOpts{
					PacketID:    p.PacketID,
					ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
				})
			case *wire.Unsubscribe:
				_, _ = wire.WriteUnsuback(c, wire.UnsubackOpts{
					PacketID:    p.PacketID,
					ReasonCodes: []wire.ReasonCode{wire.ReasonSuccess},
				})
			}
			pkt.Release()
		}
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	ch, tok, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "x", QoS: 1}}, SubBuffer(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Unsubscribe(context.Background(), tok); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel closed, got a message")
		}
	case <-time.After(time.Second):
		t.Fatal("channel was not closed after Unsubscribe")
	}
}

// ---------------- SubscribeQueue ----------------

func TestSubscribeQueue(t *testing.T) {
	fb := newFakeBroker(t, pushPublishBroker(t, []byte("queued")))

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	q, _, err := cli.SubscribeQueue(context.Background(),
		[]TopicFilter{{Topic: "sensor/+", QoS: 1}})
	if err != nil {
		t.Fatalf("SubscribeQueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, ok := q.Dequeue(ctx)
	if !ok {
		t.Fatal("Dequeue returned ok=false")
	}
	if string(msg.Payload) != "queued" {
		t.Errorf("payload = %q, want queued", msg.Payload)
	}
	if err := msg.Ack(); err != nil {
		t.Errorf("Ack: %v", err)
	}
}

// ---------------- Typed[T] ----------------

type sensorReading struct {
	DeviceID string  `json:"device_id"`
	Temp     float64 `json:"temp"`
}

func TestTypedPublishSubscribe(t *testing.T) {
	// JSON-encode the expected payload so the broker pushes a payload
	// the Typed decoder can read.
	expected := sensorReading{DeviceID: "sensor-001", Temp: 42.7}
	jsonBytes, _ := testJSONCodec[sensorReading]{}.Encode(expected)

	fb := newFakeBroker(t, pushPublishBroker(t, jsonBytes))

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	typed := NewTyped(cli, testJSONCodec[sensorReading]{})
	ch, _, err := typed.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sensor/+", QoS: 1}}, SubBuffer(4))
	if err != nil {
		t.Fatalf("typed.Subscribe: %v", err)
	}

	select {
	case msg := <-ch:
		if msg.Value != expected {
			t.Errorf("Value = %+v, want %+v", msg.Value, expected)
		}
		_ = msg.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("no typed message arrived")
	}
}

// ---------------- ClientGroup ----------------

func TestClientGroupPublishFanOut(t *testing.T) {
	publishesSeen := atomic.Int32{}

	makeBroker := func() *fakeBroker {
		return newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
			defer c.Close()
			dec := wire.NewDecoder(c)
			acceptConnect(t, c, dec)
			for {
				pkt, err := dec.ReadPacket()
				if err != nil {
					return
				}
				if _, ok := pkt.(*wire.Publish); ok {
					publishesSeen.Add(1)
				}
				pkt.Release()
			}
		})
	}
	fb1 := makeBroker()
	fb2 := makeBroker()

	g, err := NewClientGroup([]GroupMember{
		{Broker: fb1.URL()},
		{Broker: fb2.URL()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer g.Disconnect(context.Background())

	if err := g.Publish(context.Background(), wire.PublishOpts{
		Topic:   "alpha",
		Payload: []byte("ping"),
		QoS:     0,
	}); err != nil {
		t.Fatalf("group Publish: %v", err)
	}

	// Allow both brokers' read goroutines to dispatch.
	deadline := time.Now().Add(2 * time.Second)
	for publishesSeen.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := publishesSeen.Load(); got != 2 {
		t.Errorf("publishesSeen = %d, want 2 (both brokers should receive)", got)
	}
}

// ---------------- Topic alias ----------------

func TestTopicAlias_OutboundAutoAllocation(t *testing.T) {
	// Broker advertises TopicAliasMaximum=5 in CONNACK. Then it
	// records two PUBLISHes on the same topic and asserts the
	// second uses the alias.
	pubs := make(chan seenPub, 4)

	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
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

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	// Publish the same topic twice at QoS 0.
	for i := range 2 {
		if err := cli.Publish(context.Background(), wire.PublishOpts{
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
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
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

	cli, _ := New(WithBroker(fb.URL()))
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

func TestClientGroupSubscribeFanIn(t *testing.T) {
	makeBroker := func(payload []byte) *fakeBroker {
		return newFakeBroker(t, pushPublishBroker(t, payload))
	}
	fb1 := makeBroker([]byte("from-broker-1"))
	fb2 := makeBroker([]byte("from-broker-2"))

	g, err := NewClientGroup([]GroupMember{
		{Broker: fb1.URL()},
		{Broker: fb2.URL()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer g.Disconnect(context.Background())

	ch, _, err := g.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sensor/+", QoS: 1}}, SubBuffer(8))
	if err != nil {
		t.Fatalf("group Subscribe: %v", err)
	}

	got := map[string]bool{}
	deadline := time.NewTimer(3 * time.Second)
	for len(got) < 2 {
		select {
		case msg := <-ch:
			got[string(msg.Payload)] = true
			_ = msg.Ack()
		case <-deadline.C:
			t.Fatalf("only got %d messages: %v", len(got), got)
		}
	}
	if !got["from-broker-1"] || !got["from-broker-2"] {
		t.Errorf("missing payloads: %v", got)
	}
}
