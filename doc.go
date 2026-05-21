// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package mqttv5 is a fast, ergonomic MQTT v5 client for Go. The
// supervisor (reconnect, replay-in-flight, auto-resubscribe) is baked
// into every [Client]; there is no separate "auto-reconnect" wrapper.
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
// publishes while disconnected, drain on reconnect, and survive
// process restart (when combined with the queue/file submodule).
//
// Operator surface:
//
//   - [Client.Stats] returns a snapshot of in-memory counters
//     (connects, publishes sent/acked, inbound dropped, pool
//     fallbacks, ping timeouts, ...). Opt in via [WithStats] — when
//     off the hot path compiles to a single predicted nil-check.
//   - [Client.DisconnectWith] sends a custom DISCONNECT (reason code,
//     ReasonString, SessionExpiry override).
//   - [WithConnectPacketBuilder] mutates the CONNECT immediately
//     before each attempt — the canonical OAuth-token-rotation hook.
//   - [WithOnConnectError] / [WithOnReconnectAttempt] feed metrics
//     and alerting per attempt; [WithOnConnectionDown] returns false
//     to terminate the supervisor.
//
// Full MQTT v5 conformance: shared subscriptions, topic aliases
// (in + out), session expiry, retained messages, will + will
// properties, enhanced authentication (CONNECT and mid-session
// §4.12), and CONNACK capability flags honoured before any matching
// SUBSCRIBE traffic goes on the wire.
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
//	_ = cli.Publish(ctx, wire.PublishOpts{
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
//   - queue/file     — Durable outbound publish queue (filesystem WAL).
//   - store/file     — Crash-safe session store for in-flight QoS 1/2.
//   - transport/ws   — WebSocket transport via gobwas/ws; wire it in
//     with [WithDialFunc].
//
// # Architecture
//
// One goroutine per connection drives the read path
// (read -> decode -> trie match -> handler) with handlers running
// synchronously on the reader. A second goroutine drains a
// many-producer-single-consumer write channel — no mutex around
// [net.Conn.Write], so concurrent publishers scale across cores.
//
// Packets and frame buffers come from per-type sync.Pools. Inbound
// [Message] aliases the pooled frame for zero-copy Topic and
// Payload; the frame returns to the pool only when refcounted
// [Message.Ack] reaches the last handler. A supervisor goroutine
// reconnects with configurable backoff, replays in-flight QoS 1/2
// publishes with DUP=1, and re-issues every tracked subscription.
//
// # Benchmarks
//
// See benchmarks/ for end-to-end and codec micro-benchmarks against
// eclipse/paho.golang.
package mqttv5
