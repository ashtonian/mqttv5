// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package mqttv5 is an MQTT v5 client for Go that allocates little per
// message, with an ergonomic API. The supervisor (reconnect, replay-in-flight,
// auto-resubscribe) is baked into every [Client]; there is no separate
// "auto-reconnect" wrapper.
//
// # Why this package
//
// One package, one [Client]. No paho / autopaho split — reconnect,
// replay, and resubscribe are always on. Channel-native subscribe:
// <-chan *[Message] ([Client.Subscribe]), [Queue] of *[Message]
// ([Client.SubscribeQueue]), or a sync callback ([Client.SubscribeCallback]).
// Backpressure is a first-class concept — per-subscription
// [DropNewest] / [DropOldest] auto-ack the dropped message so the
// broker stops retransmitting.
//
// Three multi-broker patterns are kept distinct:
//
//   - Failover within one logical client (interchangeable brokers,
//     one session identity): [WithBrokers].
//   - N parallel sessions to N independent brokers, each treated as
//     itself: [ClientGroup]. Configurable per-broker auth / TLS /
//     ClientID via [GroupMember.Opts]; pluggable publish policy
//     ([GroupPublishBroadcast], [GroupPublishRoundRobin],
//     [GroupPublishHashByTopic]).
//   - Publisher pool against one broker: [WithPublisherPool].
//
// Typed publish / subscribe goes through the generic [Codec] interface;
// JSON and msgpack ship in separate submodules so the core stays
// stdlib-only. The durable outbound [QueuePublisher] lets you enqueue
// publishes while disconnected, keeps a window of them in flight once
// connected, and, with the queue/file and store/file submodules,
// continues the exchanges in flight after a process crash instead of
// publishing those messages again (a lost broker session aside).
//
// With [WithStore], every packet that depends on a session record is
// sent only once the record is written, and a failed write stops the
// client as a crash would: the next [Client.Connect] reloads the
// session from the store (see [WithOnStoreFailure]).
//
// Operator surface:
//
//   - [Client.Stats] returns a snapshot of in-memory counters
//     (connects, publishes sent/acked, inbound dropped, pool
//     fallbacks, ping timeouts, ...). Opt in via [WithStats]; when
//     off, each counter update is a nil check.
//   - [Client.DisconnectWith] sends a custom DISCONNECT (reason code,
//     ReasonString, SessionExpiry override).
//   - [WithConnectPacketBuilder] mutates the CONNECT immediately
//     before each attempt — the canonical OAuth-token-rotation hook.
//   - [WithOnConnectError] / [WithOnReconnectAttempt] feed metrics
//     and alerting per attempt; [WithOnConnectionDown] returns false
//     to terminate the supervisor.
//
// The MQTT v5 client feature set: shared subscriptions, Subscription
// Identifiers, topic aliases (in and out), session expiry and
// resumption, retained messages, the Will and its properties, server
// redirects, enhanced authentication (CONNECT and mid-session, §4.12),
// and the broker's CONNACK limits enforced before a packet is sent.
// Every packet from the broker is validated before it is acted on.
//
// # Quick start
//
//	cli, err := mqttv5.New(
//	    mqttv5.WithBroker("mqtt://localhost:1883"),
//	    mqttv5.WithClientID("ingest-svc-1"),
//	)
//	if err != nil { panic(err) }
//
//	ctx := context.Background()
//	if err := cli.Connect(ctx); err != nil { panic(err) }
//	defer cli.Disconnect(ctx)
//
//	// Channel subscribe — manual ack, §4.6-ordered PUBACK flush.
//	msgs, _, err := cli.Subscribe(ctx,
//	    []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}})
//	if err != nil { panic(err) }
//	go func() {
//	    for m := range msgs {
//	        fmt.Printf("%s: %s\n", m.Topic, m.Payload)
//	        _ = m.Ack()
//	    }
//	}()
//
//	// QoS 1 publish — supervisor replays with DUP=1 across drops.
//	_ = cli.Publish(ctx, PublishOptions{
//	    Topic:   "events/example",
//	    Payload: []byte("hello"),
//	    QoS:     1,
//	})
//
// See the Example functions below for end-to-end snippets covering
// each subscribe shape, typed payloads, multi-broker fan-out, and the
// durable queue publisher.
//
// # Options naming
//
// Option helpers split by scope: [Option] (passed to [New]) configures
// the [Client] and is named With…; [SubscribeOption] (passed inline
// to Subscribe / SubscribeQueue) configures one subscription and is
// named Sub…; [ClientGroupOption] (passed to [NewClientGroup])
// configures the group and is named WithGroup….
//
// # Submodules
//
// Each opt-in submodule has its own go.mod so importing it does not
// add a runtime dependency to the core:
//
//   - codec/json     — JSON [Codec] implementation, wired into [Typed].
//   - codec/msgpack  — MessagePack [Codec] via vmihailenco/msgpack/v5.
//   - queue/file     — Crash-safe outbound queue for [QueuePublisher].
//   - store/file     — Crash-safe session store for in-flight QoS 1/2.
//   - transport/ws   — WebSocket transport via gobwas/ws; wire it in
//     with [WithDialFunc].
//
// # Architecture
//
// One goroutine per connection drives the read path
// (read -> decode -> trie match -> handler) with handlers running
// synchronously on the reader. On a TCP or Unix connection, a publish
// that waits for its write writes on its own goroutine when the
// connection is idle; if its ctx ends mid-write, a deadline stops the
// write and the writer goroutine finishes the packet. When publishers
// overlap, and on TLS and WebSocket connections, a second goroutine
// drains a many-producer-single-consumer write channel and coalesces
// the queued packets, so they do not take turns on a write lock around
// [net.Conn.Write]. [WithPublisherPool] runs N such connections, each
// with its own writer goroutine, so publishing is not limited to one
// connection's writer.
//
// Packets and frame buffers up to 64 KiB come from sync.Pools. An
// inbound [Message] owns a copy of its topic, payload and properties,
// so a pooled frame goes back to its pool before any handler runs; a
// larger frame is never pooled and becomes the message's own.
// [SubZeroCopy] subscriptions alias the frame until every handle is
// acked.
//
// A supervisor goroutine reconnects with configurable backoff, resumes
// the session, replays unacknowledged QoS 1/2 publishes with DUP=1 in
// their original order, and re-issues every tracked subscription.
//
// # Benchmarks
//
// See benchmarks/ for end-to-end and codec benchmarks against
// eclipse/paho.golang and eclipse/paho.mqtt.golang.
package mqttv5
