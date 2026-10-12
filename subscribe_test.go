// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func TestSubscribeAndReceive(t *testing.T) {
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

		// Push a PUBLISH the client should route to its handler.
		_, _ = wire.WritePublish(c, wire.PublishOpts{
			Topic:    "sensor/temp",
			Payload:  []byte("42.7"),
			QoS:      1,
			PacketID: 100,
		})

		// Receive PUBACK from client.
		pkt, err = dec.ReadPacket()
		if err == nil {
			pkt.Release()
		}
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	got := make(chan string, 1)
	_, err := cli.SubscribeCallback(context.Background(),
		[]TopicFilter{{Topic: "sensor/+", QoS: 1}},
		func(m *Message) {
			got <- m.Topic + ":" + string(m.Payload)
		})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-got:
		if msg != "sensor/temp:42.7" {
			t.Errorf("got %q, want sensor/temp:42.7", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestUnsubscribe(t *testing.T) {
	subSeen := atomic.Int32{}
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
		subSeen.Add(1)

		// UNSUBSCRIBE → UNSUBACK
		pkt, err = dec.ReadPacket()
		if err != nil {
			return
		}
		unsub, ok := pkt.(*wire.Unsubscribe)
		if !ok {
			pkt.Release()
			return
		}
		_, _ = wire.WriteUnsuback(c, wire.UnsubackOpts{
			PacketID:    unsub.PacketID,
			ReasonCodes: []wire.ReasonCode{wire.ReasonSuccess},
		})
		pkt.Release()
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	token, err := cli.SubscribeCallback(context.Background(),
		[]TopicFilter{{Topic: "topic/x", QoS: 1}}, func(*Message) {})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if subSeen.Load() != 1 {
		t.Errorf("subSeen = %d, want 1", subSeen.Load())
	}

	if err := cli.Unsubscribe(context.Background(), token); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
}

// brokerCapabilityHandler returns a raw testbroker handler that completes
// the CONNECT with a CONNACK carrying the supplied capability flags
// (nil means "absent", which per spec defaults to supported). It then
// holds the connection open so Subscribe-time validations can run.
func brokerCapabilityHandler(t *testing.T, shared, wildcard, subID *byte) func(*testbroker.Broker, net.Conn) {
	t.Helper()
	return func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			t.Errorf("CONNECT read: %v", err)
			return
		}
		pkt.Release()
		if _, err := wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode:                      wire.ReasonSuccess,
			SharedSubscriptionAvailable:     shared,
			WildcardSubscriptionAvailable:   wildcard,
			SubscriptionIdentifierAvailable: subID,
		}); err != nil {
			t.Errorf("CONNACK write: %v", err)
			return
		}
		<-fb.Done()
	}
}

func TestSubscribeBlockedBySharedSubsUnsupported(t *testing.T) {
	disabled := byte(0)
	fb := testbroker.NewRaw(t, brokerCapabilityHandler(t, &disabled, nil, nil))

	cli, _ := New(WithBroker(fb.URL()), WithClientID("shared-sub-test"))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	_, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "$share/g/sport/+", QoS: 1}}, SubBuffer(16))
	if !errors.Is(err, ErrSharedSubsUnsupported) {
		t.Fatalf("Subscribe shared: got %v, want ErrSharedSubsUnsupported", err)
	}
}

func TestSubscribeBlockedByWildcardSubsUnsupported(t *testing.T) {
	disabled := byte(0)
	fb := testbroker.NewRaw(t, brokerCapabilityHandler(t, nil, &disabled, nil))

	cli, _ := New(WithBroker(fb.URL()), WithClientID("wildcard-test"))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	_, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sport/+", QoS: 1}}, SubBuffer(16))
	if !errors.Is(err, ErrWildcardSubsUnsupported) {
		t.Fatalf("Subscribe wildcard: got %v, want ErrWildcardSubsUnsupported", err)
	}
}

func TestSubscribeAllowedWhenBrokerSupports(t *testing.T) {
	// Default behaviour: broker omits the properties => supported.
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		// Accept the SUBSCRIBE so the test path completes.
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		sub, ok := pkt.(*wire.Subscribe)
		if !ok {
			t.Errorf("expected SUBSCRIBE, got %s", pkt.Type())
			pkt.Release()
			return
		}
		id := sub.PacketID
		pkt.Release()
		_, _ = wire.WriteSuback(c, wire.SubackOpts{
			PacketID:    id,
			ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
		})
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()), WithClientID("default-caps-test"))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	_, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sport/+", QoS: 1}}, SubBuffer(16))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
}

// testJSONCodec is a tiny inline json Codec[T] for the typed tests.
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

// pushPublishBroker is a raw testbroker handler factory that pushes one
// PUBLISH on the given topic shortly after CONNACK + SUBSCRIBE.
func pushPublishBroker(t *testing.T, payload []byte) func(*testbroker.Broker, net.Conn) {
	return func(fb *testbroker.Broker, c net.Conn) {
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

func TestSubscribeChannel(t *testing.T) {
	fb := testbroker.NewRaw(t, pushPublishBroker(t, []byte("hello")))

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
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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

func TestSubscribeQueue(t *testing.T) {
	fb := testbroker.NewRaw(t, pushPublishBroker(t, []byte("queued")))

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

type sensorReading struct {
	DeviceID string  `json:"device_id"`
	Temp     float64 `json:"temp"`
}

func TestTypedPublishSubscribe(t *testing.T) {
	// JSON-encode the expected payload so the broker pushes a payload
	// the Typed decoder can read.
	expected := sensorReading{DeviceID: "sensor-001", Temp: 42.7}
	jsonBytes, _ := testJSONCodec[sensorReading]{}.Encode(expected)

	fb := testbroker.NewRaw(t, pushPublishBroker(t, jsonBytes))

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

// TestSubscribeChanDropOldestRejected verifies that an explicit
// SubDropPolicy(DropOldest) on the channel-based Subscribe returns
// ErrChanDropOldestUnsupported. DropOldest requires head-eviction,
// which would race a consumer-owned channel.
func TestSubscribeChanDropOldestRejected(t *testing.T) {
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

	cli, err := New(WithBroker(fb.URL()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	_, _, err = cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "x/y", QoS: 1}},
		SubDropPolicy(DropOldest),
	)
	if !errors.Is(err, ErrChanDropOldestUnsupported) {
		t.Fatalf("Subscribe err = %v, want ErrChanDropOldestUnsupported", err)
	}
}

// TestSubscribeChanIgnoresClientLevelDropOldestDefault verifies the
// asymmetric handling: a client-level WithDropPolicy(DropOldest)
// propagates to chan Subscribe but is silently downgraded to
// DropNewest (chan can't peek-and-pop). Only an explicit per-call
// SubDropPolicy(DropOldest) errors.
func TestSubscribeChanIgnoresClientLevelDropOldestDefault(t *testing.T) {
	subAcked := make(chan struct{}, 1)
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			sub, ok := pkt.(*wire.Subscribe)
			if !ok {
				pkt.Release()
				continue
			}
			id := sub.PacketID
			pkt.Release()
			_, _ = wire.WriteSuback(c, wire.SubackOpts{
				PacketID:    id,
				ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
			})
			select {
			case subAcked <- struct{}{}:
			default:
			}
		}
	})

	cli, err := New(
		WithBroker(fb.URL()),
		WithDropPolicy(DropOldest), // client-level default
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	// Channel Subscribe with no explicit SubDropPolicy must succeed.
	_, _, err = cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "x/y", QoS: 1}},
	)
	if err != nil {
		t.Fatalf("Subscribe (chan) with client-level DropOldest: %v", err)
	}
}

// A consumer that falls behind loses QoS 0 messages but no QoS 1 or 2
// one: those have room beyond the buffer or cap for as many as the
// broker may send before the consumer acks one. Auto-ack gives up the
// broker's flow control, and with it that room.
func TestSlowConsumerKeepsQoS1(t *testing.T) {
	const recvMax = 8
	// Two QoS 0 messages fill the buffer; recvMax QoS 1 messages and two
	// more QoS 0 ones follow.
	sent := []string{"s/q0/a", "s/q0/b"}
	for i := 1; i <= recvMax; i++ {
		sent = append(sent, fmt.Sprintf("s/q1/%d", i))
	}
	sent = append(sent, "s/q0/c", "s/q0/d")
	qos1 := sent[2 : 2+recvMax]

	filters := []TopicFilter{{Topic: "s/#", QoS: 1}}
	viaChannel := func(t *testing.T, cli *Client, opts []SubscribeOption) func() []string {
		ch, _, err := cli.Subscribe(context.Background(), filters, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return func() []string {
			var got []string
			for {
				select {
				case m := <-ch:
					got = append(got, m.Topic)
					_ = m.Ack()
				default:
					return got
				}
			}
		}
	}
	viaQueue := func(t *testing.T, cli *Client, opts []SubscribeOption) func() []string {
		q, _, err := cli.SubscribeQueue(context.Background(), filters, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return func() []string {
			var got []string
			for {
				m, ok := q.TryDequeue()
				if !ok {
					return got
				}
				got = append(got, m.Topic)
				_ = m.Ack()
			}
		}
	}

	tests := []struct {
		name      string
		subscribe func(*testing.T, *Client, []SubscribeOption) func() []string
		opts      []SubscribeOption
		delivered []string
		dropped   []string
	}{
		{"channel", viaChannel, nil, sent[:2+recvMax], sent[2+recvMax:]},
		{"queue", viaQueue, nil, sent[:2+recvMax], sent[2+recvMax:]},
		{"channel with auto-ack", viaChannel, []SubscribeOption{SubAutoAck()}, sent[:2], append(slices.Clone(qos1), sent[2+recvMax:]...)},
		{"queue with auto-ack", viaQueue, []SubscribeOption{SubAutoAck()}, sent[:2], append(slices.Clone(qos1), sent[2+recvMax:]...)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ready := make(chan struct{})
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.AcceptConnect(wire.ConnackOpts{})
				c.ServeSubscribe(-1)
				<-ready
				var id uint16
				for _, topic := range sent {
					opts := wire.PublishOpts{Topic: topic}
					if strings.HasPrefix(topic, "s/q1/") {
						id++
						opts.QoS, opts.PacketID = 1, id
					}
					c.Publish(opts)
				}
				c.Hold(0)
			})
			cli := tbClient(t, b, WithReceiveMaximum(recvMax))
			var mu sync.Mutex
			var dropped []string
			opts := append([]SubscribeOption{SubBuffer(2), SubMaxQueueSize(2), SubOnDrop(func(m *Message) {
				mu.Lock()
				dropped = append(dropped, m.Topic)
				mu.Unlock()
			})}, tt.opts...)
			drain := tt.subscribe(t, cli, opts)
			close(ready)
			// The read loop places messages in order, so once the last
			// drop is seen every message has been placed.
			deadline := time.Now().Add(3 * time.Second)
			for {
				mu.Lock()
				n := len(dropped)
				mu.Unlock()
				if n >= len(tt.dropped) || time.Now().After(deadline) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			got := drain()
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(got, tt.delivered) {
				t.Errorf("delivered %v, want %v", got, tt.delivered)
			}
			if !slices.Equal(dropped, tt.dropped) {
				t.Errorf("dropped %v, want %v", dropped, tt.dropped)
			}
		})
	}
}

func TestParseShareFilter(t *testing.T) {
	for _, tc := range []struct {
		filter     string
		wantGroup  string
		wantUnder  string
		wantShared bool
	}{
		{"$share/g1/foo/bar", "g1", "foo/bar", true},
		{"$share/group/+/+", "group", "+/+", true},
		{"$share/g/#", "g", "#", true},
		{"foo/bar", "", "foo/bar", false},                 // plain filter
		{"$share/", "", "$share/", false},                 // missing group + filter
		{"$share/onlyName", "", "$share/onlyName", false}, // missing filter
	} {
		t.Run(tc.filter, func(t *testing.T) {
			g, u, s := parseShareFilter(tc.filter)
			if g != tc.wantGroup || u != tc.wantUnder || s != tc.wantShared {
				t.Errorf("parseShareFilter(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tc.filter, g, u, s, tc.wantGroup, tc.wantUnder, tc.wantShared)
			}
		})
	}
}

func TestSubscribe_SharedFilter_TrieMatchesUnderlying(t *testing.T) {
	// The broker delivers inbound PUBLISHes with the original
	// (non-share) topic. The trie must match against the underlying
	// filter — otherwise nothing dispatches.
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
		// Validate the broker received the FULL $share/... filter.
		if len(sub.Filters) != 1 || sub.Filters[0].Topic != "$share/workers/sensor/+" {
			t.Errorf("broker saw filter %v, want [$share/workers/sensor/+]", sub.Filters)
		}
		_, _ = wire.WriteSuback(c, wire.SubackOpts{
			PacketID:    sub.PacketID,
			ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
		})
		pkt.Release()

		// Push a PUBLISH using the UNDERLYING topic (no $share prefix).
		_, _ = wire.WritePublish(c, wire.PublishOpts{
			Topic:    "sensor/temp",
			Payload:  []byte("shared-msg"),
			QoS:      1,
			PacketID: 100,
		})
		// Drain PUBACK.
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
		[]TopicFilter{{Topic: "$share/workers/sensor/+", QoS: 1}}, SubBuffer(4))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case m := <-ch:
		if m.Topic != "sensor/temp" || string(m.Payload) != "shared-msg" {
			t.Errorf("got topic=%q payload=%q", m.Topic, m.Payload)
		}
		_ = m.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("shared subscription did not dispatch the inbound publish")
	}
}

// A message's PublishOptions publish it again as it arrived.
func TestMessagePublishOptions(t *testing.T) {
	format, expiry := byte(1), uint32(90)
	sent := wire.PublishOpts{
		Topic: "plant/line-1", Payload: []byte(`{"rpm":1200}`), QoS: 1, PacketID: 1, Retain: true,
		PayloadFormatIndicator: &format, MessageExpiryInterval: &expiry,
		ContentType: "application/json", ResponseTopic: "plant/replies", CorrelationData: []byte{7},
		UserProperties: []UserProperty{{Key: "site", Value: "3"}, {Key: "site", Value: "4"}},
	}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(sent)
		c.Hold(0)
	})
	cli := tbClient(t, b)
	msgs, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "plant/#", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	m := recvMsg(t, msgs, 2*time.Second)
	defer m.Ack()

	got := m.PublishOptions()
	want := PublishOptions{
		Topic: sent.Topic, Payload: sent.Payload, QoS: 1, Retain: true,
		PayloadFormatIndicator: &format, MessageExpiryInterval: &expiry,
		ContentType: sent.ContentType, ResponseTopic: sent.ResponseTopic, CorrelationData: sent.CorrelationData,
		UserProperties: sent.UserProperties,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PublishOptions =\n%+v\nwant\n%+v", got, want)
	}
}

// The drop hook must receive a readable message (it used to receive a
// Message whose frame had already been released, crashing on m.Topic).
func TestSubOnDropReceivesReadableMessage(t *testing.T) {
	ready := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-ready
		for i := 0; i < 5; i++ {
			c.Publish(wire.PublishOpts{Topic: fmt.Sprintf("d/%d", i), Payload: []byte(fmt.Sprintf("p%d", i))})
		}
		c.Hold(0)
	})
	cli := tbClient(t, b)
	var mu sync.Mutex
	var dropped []string
	_, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "d/#"}}, SubBuffer(1),
		SubOnDrop(func(m *Message) {
			mu.Lock()
			dropped = append(dropped, m.Topic+"="+string(m.Payload))
			mu.Unlock()
		}))
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(dropped)
		mu.Unlock()
		if n == 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dropped) != 4 {
		t.Fatalf("dropped = %v, want 4 drops", dropped)
	}
	for i, s := range dropped {
		if want := fmt.Sprintf("d/%d=p%d", i+1, i+1); s != want {
			t.Errorf("drop %d = %q, want %q", i, s, want)
		}
	}
}

// A second Ack on an already-acked message is a no-op; it must never
// acknowledge or invalidate a later delivery.
func TestDoubleAckNeverTouchesAnotherMessage(t *testing.T) {
	step := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "a/1", Payload: []byte("A"), QoS: 1, PacketID: 1})
		<-step
		c.Publish(wire.PublishOpts{Topic: "a/2", Payload: []byte("B"), QoS: 1, PacketID: 2})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "a/#", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	mA := recvMsg(t, ch, 2*time.Second)
	if err := mA.Ack(); err != nil {
		t.Fatal(err)
	}
	close(step)
	mB := recvMsg(t, ch, 2*time.Second)
	_ = mA.Ack() // second Ack: must be a no-op
	_ = mA.Ack()

	conn := b.Conn(0, time.Second)
	if _, ok := conn.Await(wire.PUBACK, 300*time.Millisecond); !ok {
		t.Fatal("no PUBACK for A")
	}
	if p, ok := conn.Await(wire.PUBACK, 300*time.Millisecond); ok {
		t.Fatalf("PUBACK id=%d sent before the application acked B", p.PacketID)
	}
	if mB.Topic != "a/2" || string(mB.Payload) != "B" {
		t.Fatalf("B corrupted: topic=%q payload=%q", mB.Topic, mB.Payload)
	}
	_ = mB.Ack()
	if p, ok := conn.Await(wire.PUBACK, time.Second); !ok || p.PacketID != 2 {
		t.Fatalf("PUBACK for B: ok=%v id=%d", ok, p.PacketID)
	}
}

// With overlapping subscriptions each handler owns its own handle; one
// handler acking twice must not release or acknowledge for the other.
func TestDoubleAckOnSharedDeliveryKeepsOtherHandlerValid(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "m/x", Payload: []byte("PAYLOAD"), QoS: 1, PacketID: 1})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	chA, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "m/#", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	chB, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "m/x", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	mA := recvMsg(t, chA, 2*time.Second)
	mB := recvMsg(t, chB, 2*time.Second)
	_ = mA.Ack()
	_ = mA.Ack()
	conn := b.Conn(0, time.Second)
	if p, ok := conn.Await(wire.PUBACK, 300*time.Millisecond); ok {
		t.Fatalf("PUBACK id=%d sent while handler B still holds the message", p.PacketID)
	}
	if mB.Topic != "m/x" || string(mB.Payload) != "PAYLOAD" {
		t.Fatalf("B invalidated: topic=%q payload=%q", mB.Topic, mB.Payload)
	}
	_ = mB.Ack()
	if _, ok := conn.Await(wire.PUBACK, time.Second); !ok {
		t.Fatal("no PUBACK after both handlers acked")
	}
}

// Zero-copy subscriptions keep the same per-handle guarantees.
func TestZeroCopyDoubleAckIsNoOp(t *testing.T) {
	step := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "z/1", Payload: []byte("A"), QoS: 1, PacketID: 1})
		<-step
		c.Publish(wire.PublishOpts{Topic: "z/2", Payload: []byte("B"), QoS: 1, PacketID: 2})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "z/#", QoS: 1}}, SubZeroCopy())
	if err != nil {
		t.Fatal(err)
	}
	mA := recvMsg(t, ch, 2*time.Second)
	_ = mA.Ack()
	close(step)
	mB := recvMsg(t, ch, 2*time.Second)
	_ = mA.Ack()
	if mB.Topic != "z/2" || string(mB.Payload) != "B" {
		t.Fatalf("B corrupted: topic=%q payload=%q", mB.Topic, mB.Payload)
	}
	conn := b.Conn(0, time.Second)
	conn.Expect(wire.PUBACK, time.Second)
	conn.ExpectNone(wire.PUBACK, 300*time.Millisecond)
	_ = mB.Ack()
	conn.Expect(wire.PUBACK, time.Second)
}

// Disconnect must unregister handlers; reconnecting and subscribing to
// the same filter must not dispatch into the closed channel.
func TestDisconnectConnectResubscribeSameFilter(t *testing.T) {
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		if _, ok := c.ServeSubscribe(-1); !ok {
			return
		}
		if c.Index == 1 {
			c.Publish(wire.PublishOpts{Topic: "r/x", Payload: []byte("after-reconnect")})
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ctx := context.Background()
	old, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "r/x"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-old; ok {
		t.Fatal("old channel should be closed by Disconnect")
	}
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "r/x"}})
	if err != nil {
		t.Fatal(err)
	}
	m := recvMsg(t, ch, 2*time.Second)
	if string(m.Payload) != "after-reconnect" {
		t.Fatalf("payload = %q", m.Payload)
	}
}

// The reader may dispatch through a trie snapshot taken before
// Unsubscribe removed the route. A closed route must ack the message
// instead of sending on its closed channel.
func TestRouteDispatchAfterCloseAcks(t *testing.T) {
	ch := make(chan *Message, 1)
	r := &route{deliver: func(m *Message) { ch <- m }}
	r.close(func() { close(ch) })

	d := &delivery{}
	d.refs.Store(1)
	m := &d.first
	m.d = d
	r.dispatch(m)
	if !m.acked.Load() || d.refs.Load() != 0 {
		t.Fatalf("message on a closed route not acked: acked=%v refs=%d", m.acked.Load(), d.refs.Load())
	}
}

func TestRouteCloseRacingDispatch(t *testing.T) {
	for i := 0; i < 10000; i++ {
		ch := make(chan *Message, 1)
		r := &route{deliver: func(m *Message) {
			select {
			case ch <- m:
			default:
				_ = m.Ack()
			}
		}}
		var wg sync.WaitGroup
		wg.Go(func() {
			for j := 0; j < 4; j++ {
				d := &delivery{}
				d.refs.Store(1)
				d.first.d = d
				r.dispatch(&d.first)
			}
		})
		wg.Go(func() { r.close(func() { close(ch) }) })
		wg.Wait()
	}
}

// End to end: Unsubscribe racing a QoS 0 stream on an overlapping filter.
// The broker follows every SUBACK and UNSUBACK with a burst on the
// filter, so each Unsubscribe races the dispatch of messages for its
// subscription, and the stream never outruns the client however fast
// the machine is. Run with -race.
func TestUnsubscribeRacingDispatch(t *testing.T) {
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAutoThen(func(p testbroker.Packet) error {
			if p.Type != wire.SUBSCRIBE && p.Type != wire.UNSUBSCRIBE {
				return nil
			}
			for range 8 {
				if err := c.Publish(wire.PublishOpts{Topic: "flood/x", Payload: []byte("f")}); err != nil {
					return err
				}
			}
			return nil
		})
	})
	cli := tbClient(t, b)
	ctx := context.Background()
	if _, err := cli.SubscribeCallback(ctx, []TopicFilter{{Topic: "flood/#"}}, func(*Message) {}); err != nil {
		t.Fatal(err)
	}
	const workers = 4
	iterations := 1000
	if testing.Short() {
		iterations = 200
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for i := 0; i < iterations/workers; i++ {
				ch, tok, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "flood/x"}}, SubBuffer(1))
				if err != nil {
					t.Error(err)
					return
				}
				go func() {
					for range ch {
					}
				}()
				if err := cli.Unsubscribe(ctx, tok); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// A queued small message costs roughly its own size, not a pinned
// 4 KiB frame.
func TestQueuedSmallMessagesStaySmall(t *testing.T) {
	const n = 20000
	payload := make([]byte, 64)
	ready := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-ready
		for i := 0; i < n; i++ {
			c.Publish(wire.PublishOpts{Topic: "q/small", Payload: payload})
		}
		c.Hold(0)
	})
	cli := tbClient(t, b)
	q, _, err := cli.SubscribeQueue(context.Background(), []TopicFilter{{Topic: "q/#"}})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	close(ready)
	deadline := time.Now().Add(10 * time.Second)
	for q.Len() < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if q.Len() != n {
		t.Fatalf("queued %d of %d", q.Len(), n)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	per := (int64(after.HeapInuse) - int64(before.HeapInuse)) / n
	t.Logf("heap per queued 64 B message: %d B", per)
	if per > 400 {
		t.Errorf("heap per queued 64 B message = %d B, want ≤ 400", per)
	}
}

// SubAutoAck acks on the read goroutine before the consumer receives the
// message, and the consumer still gets an owned, readable copy.
func TestSubAutoAckDeliversOwnedMessage(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "aa/1", Payload: []byte("v"), QoS: 1, PacketID: 9})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "aa/#", QoS: 1}}, SubAutoAck())
	if err != nil {
		t.Fatal(err)
	}
	conn := b.Conn(0, time.Second)
	if p := conn.Expect(wire.PUBACK, time.Second); p.PacketID != 9 {
		t.Fatalf("PUBACK id = %d before the consumer read the message", p.PacketID)
	}
	m := recvMsg(t, ch, 2*time.Second)
	_ = m.Ack()
	_ = m.Ack()
	conn.ExpectNone(wire.PUBACK, 200*time.Millisecond)
	if m.Topic != "aa/1" || string(m.Payload) != "v" {
		t.Fatalf("message = %q %q", m.Topic, m.Payload)
	}
}

func TestSubscribeQueueCapDefaults(t *testing.T) {
	tests := []struct {
		name   string
		client []Option
		sub    []SubscribeOption
		want   int
	}{
		{"default", nil, nil, DefaultMaxSubscribeQueueSize},
		{"client cap", []Option{WithMaxSubscribeQueueSize(10)}, nil, 10},
		{"client unbounded", []Option{WithMaxSubscribeQueueSize(UnboundedQueue)}, nil, UnboundedQueue},
		{"per-sub cap", nil, []SubscribeOption{SubMaxQueueSize(5)}, 5},
		{"per-sub zero keeps client value", []Option{WithMaxSubscribeQueueSize(10)}, []SubscribeOption{SubMaxQueueSize(0)}, 10},
		{"per-sub unbounded", nil, []SubscribeOption{SubMaxQueueSize(UnboundedQueue)}, UnboundedQueue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(append([]Option{WithBroker("mqtt://127.0.0.1:1")}, tt.client...)...)
			if err != nil {
				t.Fatal(err)
			}
			cfg := newSubscribeConfig(c)
			for _, o := range tt.sub {
				o(&cfg)
			}
			if cfg.maxQueueSize != tt.want {
				t.Fatalf("maxQueueSize = %d, want %d", cfg.maxQueueSize, tt.want)
			}
		})
	}
}

// A callback that retains m.Topic / m.Payload must keep seeing its own
// bytes after later frames reuse the pooled buffers.
func TestCallbackRetainedFieldsStayValid(t *testing.T) {
	const n = 200
	ready := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-ready
		for i := 0; i < n; i++ {
			c.Publish(wire.PublishOpts{Topic: fmt.Sprintf("cb/%d", i), Payload: []byte(fmt.Sprintf("payload-%d", i))})
		}
		c.Hold(0)
	})
	cli := tbClient(t, b)
	var (
		mu       sync.Mutex
		topics   []string
		payloads [][]byte
		done     = make(chan struct{})
	)
	_, err := cli.SubscribeCallback(context.Background(), []TopicFilter{{Topic: "cb/#"}}, func(m *Message) {
		mu.Lock()
		defer mu.Unlock()
		topics = append(topics, m.Topic)
		payloads = append(payloads, m.Payload)
		if len(topics) == n {
			close(done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("not all messages delivered")
	}
	mu.Lock()
	defer mu.Unlock()
	for i := range topics {
		if want := fmt.Sprintf("cb/%d", i); topics[i] != want {
			t.Fatalf("retained topic %d = %q, want %q", i, topics[i], want)
		}
		if want := fmt.Sprintf("payload-%d", i); string(payloads[i]) != want {
			t.Fatalf("retained payload %d = %q, want %q", i, payloads[i], want)
		}
	}
}

// A large message is delivered in the frame it was read into, which is
// never recycled: retained messages stay intact, and receiving one costs
// about its size rather than one copy per stage.
func TestLargeMessagesKeepTheirFrame(t *testing.T) {
	const n, size = 8, 1 << 20
	payloads := make([][]byte, n)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte(i)}, size)
	}
	ready := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-ready
		for i := 0; i < n; i++ {
			c.Publish(wire.PublishOpts{Topic: fmt.Sprintf("big/%d", i), Payload: payloads[i]})
		}
		c.Hold(0)
	})
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "big/#"}}, SubBuffer(n))
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	close(ready)
	msgs := make([]*Message, n)
	for i := range msgs {
		msgs[i] = recvMsg(t, ch, 5*time.Second)
	}
	runtime.ReadMemStats(&after)
	for i, m := range msgs {
		if m.Topic != fmt.Sprintf("big/%d", i) || !bytes.Equal(m.Payload, payloads[i]) {
			t.Fatalf("message %d changed after later deliveries", i)
		}
		_ = m.Ack()
	}
	if per := (after.TotalAlloc - before.TotalAlloc) / n; per > 3*size/2 {
		t.Fatalf("receiving a %d-byte message allocated %d bytes", size, per)
	}
}
