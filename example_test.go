// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

// Example shows the minimal connect → subscribe → publish loop. The
// supervisor (reconnect, replay, resubscribe) is implicit — once
// Connect returns, every drop is recovered automatically until
// Disconnect.
func Example() {
	cli, err := mqttv5.New(
		mqttv5.WithBroker("mqtt://localhost:1883"),
		mqttv5.WithClientID("example"),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := cli.Connect(ctx); err != nil {
		panic(err)
	}
	defer cli.Disconnect(ctx)

	msgs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "demo/#", QoS: 1}})
	if err != nil {
		panic(err)
	}
	go func() {
		for m := range msgs {
			fmt.Printf("recv %s %s\n", m.Topic, m.Payload)
			_ = m.Ack()
		}
	}()

	_ = cli.Publish(ctx, wire.PublishOpts{
		Topic:   "demo/hello",
		Payload: []byte("world"),
		QoS:     1,
	})
}

// ExampleClient_Subscribe receives matched messages on a buffered
// channel. The caller MUST call [mqttv5.Message.Ack] on each one — for
// QoS 1/2 the broker-side ack is held until Ack is called, in §4.6
// arrival order across overlapping filters.
func ExampleClient_Subscribe() {
	cli, _ := mqttv5.New(mqttv5.WithBroker("mqtt://localhost:1883"))
	ctx := context.Background()
	_ = cli.Connect(ctx)
	defer cli.Disconnect(ctx)

	msgs, token, err := cli.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
		mqttv5.SubBuffer(256),
		mqttv5.SubOnDrop(func(m *mqttv5.Message) {
			// Dropped messages have already been ack'd; this is for
			// observability only. Must not call Ack and must not retain
			// Topic/Payload past return.
			fmt.Println("dropped", m.Topic)
		}),
	)
	if err != nil {
		return
	}
	for m := range msgs {
		fmt.Println(m.Topic, string(m.Payload))
		_ = m.Ack()
	}
	_ = cli.Unsubscribe(ctx, token)
}

// ExampleClient_SubscribeQueue stores matched messages in an
// unbounded queue. [mqttv5.DropOldest] evicts the queue head when
// SubMaxQueueSize is reached so the freshest N messages survive.
func ExampleClient_SubscribeQueue() {
	cli, _ := mqttv5.New(mqttv5.WithBroker("mqtt://localhost:1883"))
	ctx := context.Background()
	_ = cli.Connect(ctx)
	defer cli.Disconnect(ctx)

	q, _, err := cli.SubscribeQueue(ctx,
		[]mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
		mqttv5.SubMaxQueueSize(10_000),
		mqttv5.SubDropPolicy(mqttv5.DropOldest),
	)
	if err != nil {
		return
	}
	for {
		m, ok := q.Dequeue(ctx)
		if !ok {
			break
		}
		fmt.Println(m.Topic, string(m.Payload))
		_ = m.Ack()
	}
}

// ExampleClient_SubscribeCallback registers a synchronous handler
// invoked on the read goroutine. The handler must not block — a slow
// callback stalls PINGRESP processing and drops the connection. Ack
// fires automatically after the callback returns.
func ExampleClient_SubscribeCallback() {
	cli, _ := mqttv5.New(mqttv5.WithBroker("mqtt://localhost:1883"))
	ctx := context.Background()
	_ = cli.Connect(ctx)
	defer cli.Disconnect(ctx)

	_, err := cli.SubscribeCallback(ctx,
		[]mqttv5.TopicFilter{{Topic: "ctrl/+", QoS: 0}},
		func(m *mqttv5.Message) {
			fmt.Println(m.Topic, string(m.Payload))
		},
	)
	if err != nil {
		return
	}
}

// ExampleClient_Publish sends a QoS 1 publish. Publish returns after
// PUBACK arrives. If the connection drops mid-flight the session
// entry persists and the supervisor replays the packet with DUP=1 on
// reconnect; the caller stays blocked across the drop.
func ExampleClient_Publish() {
	cli, _ := mqttv5.New(mqttv5.WithBroker("mqtt://localhost:1883"))
	ctx := context.Background()
	_ = cli.Connect(ctx)
	defer cli.Disconnect(ctx)

	err := cli.Publish(ctx, wire.PublishOpts{
		Topic:   "events/example",
		Payload: []byte(`{"value":42}`),
		QoS:     1,
	})
	if err != nil {
		fmt.Println("publish:", err)
	}
}

// ExampleClient_SetBrokers shows multi-broker failover. The
// supervisor rotates through [mqttv5.Config.BrokerURLs] on every
// reconnect attempt; [mqttv5.Client.SetBrokers] replaces the list at
// runtime — the canonical hook is a ServerMoved DISCONNECT.
func ExampleClient_SetBrokers() {
	var cli *mqttv5.Client
	cli, _ = mqttv5.New(
		mqttv5.WithBrokers(
			"mqtts://broker-a.example.com:8883",
			"mqtts://broker-b.example.com:8883",
		),
		mqttv5.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}),
		mqttv5.WithOnServerDisconnect(func(d *wire.Disconnect) {
			if ref, ok := d.Properties.String(wire.PropServerReference); ok {
				_ = cli.SetBrokers(ref)
			}
		}),
	)
	_ = cli
}

// ExampleNewClientGroup creates one [mqttv5.Client] per broker and
// dispatches via the configured GroupPublishPolicy. Subscribe merges
// inbound messages from every member into one channel.
//
// For failover within one logical client (interchangeable brokers,
// same data), use [mqttv5.WithBrokers] on a single Client instead —
// see [Example_ClientSetBrokers].
func ExampleNewClientGroup() {
	group, err := mqttv5.NewClientGroup(
		[]mqttv5.GroupMember{
			{
				Broker: "mqtts://emea.example.com:8883",
				Name:   "emea",
				Opts:   []mqttv5.Option{mqttv5.WithCredentials("emea-svc", []byte("token-1"))},
			},
			{
				Broker: "mqtts://apac.example.com:8883",
				Name:   "apac",
				Opts:   []mqttv5.Option{mqttv5.WithCredentials("apac-svc", []byte("token-2"))},
			},
		},
		mqttv5.WithGroupSharedOpts(
			mqttv5.WithClientID("fanout"),
			mqttv5.WithKeepAlive(30),
		),
		mqttv5.WithGroupPublishPolicy(mqttv5.GroupPublishBroadcast),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := group.Connect(ctx); err != nil {
		panic(err)
	}
	defer group.Disconnect(ctx)

	msgs, _, err := group.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}})
	if err != nil {
		return
	}
	for m := range msgs {
		fmt.Println(m.Topic)
		_ = m.Ack()
	}
}

// readingCodec is a tiny inline [mqttv5.Codec] used by
// [ExampleNewTyped]. In real code prefer the codec/json submodule or
// any other implementation of mqttv5.Codec[T].
type readingCodec struct{}

type reading struct {
	Device string  `json:"device"`
	Temp   float64 `json:"temp"`
}

func (readingCodec) Encode(v reading) ([]byte, error) { return json.Marshal(v) }
func (readingCodec) Decode(b []byte) (reading, error) {
	var v reading
	err := json.Unmarshal(b, &v)
	return v, err
}

// ExampleNewTyped wraps a [mqttv5.Client] with a [mqttv5.Codec] so
// Publish and Subscribe carry decoded values. Decode failures on the
// receive path are logged + acked + dropped — wrap the codec for
// stricter behaviour.
func ExampleNewTyped() {
	cli, _ := mqttv5.New(mqttv5.WithBroker("mqtt://localhost:1883"))
	ctx := context.Background()
	_ = cli.Connect(ctx)
	defer cli.Disconnect(ctx)

	typed := mqttv5.NewTyped[reading](cli, readingCodec{})

	_ = typed.Publish(ctx,
		wire.PublishOpts{Topic: "sensors/a1", QoS: 1},
		reading{Device: "a1", Temp: 22.5},
	)

	ch, _, err := typed.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "sensors/+", QoS: 1}})
	if err != nil {
		return
	}
	for m := range ch {
		fmt.Println(m.Topic, m.Value.Temp)
		_ = m.Ack()
	}
}

// ExampleNewQueuePublisher durably enqueues publishes and drains them
// to the broker in order from a background goroutine. Publish returns
// once the queue has stored the entry, before the broker round-trip.
//
// For at-least-once across process restart, swap
// [mqttv5.NewMemoryPublisherQueue] for the queue/file submodule.
func ExampleNewQueuePublisher() {
	cli, _ := mqttv5.New(mqttv5.WithBroker("mqtt://localhost:1883"))
	ctx := context.Background()
	_ = cli.Connect(ctx)
	defer cli.Disconnect(ctx)

	pub, err := mqttv5.NewQueuePublisher(cli, mqttv5.NewMemoryPublisherQueue(),
		mqttv5.WithQueueBatchSize(32),
		mqttv5.WithQueueTTL(24*time.Hour),
		mqttv5.WithDeadLetter(func(e mqttv5.QueueEntry, err error) {
			fmt.Println("dead letter", e.Publish.Topic, err)
		}),
	)
	if err != nil {
		panic(err)
	}
	defer pub.Close(context.Background())

	_ = pub.Publish(ctx, wire.PublishOpts{
		Topic:   "events/durable",
		Payload: []byte("at-least-once"),
		QoS:     1,
	})
}

// ExampleWithPublisherPool dedicates N publish-only connections to
// the same broker, lifting publish throughput above what one
// keep-alive socket can absorb. Pair with
// [mqttv5.PoolRoutingHashByTopic] to preserve per-topic ordering.
func ExampleWithPublisherPool() {
	cli, err := mqttv5.New(
		mqttv5.WithBroker("mqtt://localhost:1883"),
		mqttv5.WithPublisherPool(4),
		mqttv5.WithPublisherPoolRouting(mqttv5.PoolRoutingHashByTopic),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	_ = cli.Connect(ctx)
	defer cli.Disconnect(ctx)

	// Round-robin / hashed across the 4 pool members under the hood.
	for i := range 100 {
		_ = cli.Publish(ctx, wire.PublishOpts{
			Topic:   fmt.Sprintf("events/tick/%d", i%4),
			Payload: fmt.Appendf(nil, `{"n":%d}`, i),
			QoS:     1,
		})
	}
}

// ExampleNewQueue shows the lock-protected unbounded MPSC queue
// returned by [mqttv5.Client.SubscribeQueue]. It is exported so
// callers can build their own internal pipelines on top of the same
// type.
func ExampleNewQueue() {
	q := mqttv5.NewQueue[string]()

	go func() {
		defer q.Close()
		q.Enqueue("one")
		q.Enqueue("two")
	}()

	ctx := context.Background()
	for {
		v, ok := q.Dequeue(ctx)
		if !ok {
			break
		}
		fmt.Println(v)
	}
	// Output:
	// one
	// two
}
