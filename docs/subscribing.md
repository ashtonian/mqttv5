# Subscribing

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

- [Channel — manual ack, ordered flush](#channel--manual-ack-ordered-flush)
- [Queue — bounded, optional `DropOldest`](#queue--bounded-optional-dropoldest)
- [Callback — sync, auto-ack](#callback--sync-auto-ack)
- [Durable consumers](#durable-consumers)

Three delivery shapes. Each takes `[]TopicFilter`, so several filters
go out in one SUBSCRIBE.

## Channel — manual ack, ordered flush

```go
msgs, token, err := cli.Subscribe(ctx,
    []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
    mqttv5.SubBuffer(256),
)
for m := range msgs {
    handle(m)
    _ = m.Ack() // PUBACK released in §4.6 arrival order
}
_ = cli.Unsubscribe(ctx, token) // closes msgs
```

A consumer that falls behind loses QoS 0 messages, never QoS 1 or 2.
Once the channel holds `SubBuffer` messages, an incoming QoS 0 message
is **acked and dropped**; observe drops via `SubOnDrop(...)`. QoS 1
and 2 messages have room for `WithReceiveMaximum` more (default 256):
as many as the broker sends before the consumer acks one, so the
broker holds the rest back. Two exceptions: `SubAutoAck()` acks on
receipt, which gives up that flow control, so QoS 1 and 2 messages are
dropped as QoS 0 ones are; and messages still unread from a session
the broker discarded take room of their own.

## Queue — bounded, optional `DropOldest`

```go
q, _, _ := cli.SubscribeQueue(ctx,
    []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
    mqttv5.SubMaxQueueSize(10_000),
    mqttv5.SubDropPolicy(mqttv5.DropOldest), // keeps freshest 10k
)
for {
    m, ok := q.Dequeue(ctx)
    if !ok {
        break
    }
    handle(m)
    _ = m.Ack()
}
```

Queues hold at most `DefaultMaxSubscribeQueueSize` (65,536) messages
unless `WithMaxSubscribeQueueSize` / `SubMaxQueueSize` say otherwise;
`mqttv5.UnboundedQueue` removes the cap. When full, `DropNewest` (the
default) acks and drops an incoming QoS 0 message, while QoS 1 and 2
messages have the same room beyond the cap as on a channel;
`DropOldest` evicts the queue head, whatever its QoS, and acks it
before enqueueing.

Only the queue variant supports `DropOldest` — channels can't
peek-and-pop without racing the consumer.

## Callback — sync, auto-ack

```go
cli.SubscribeCallback(ctx,
    []mqttv5.TopicFilter{{Topic: "ctrl/+", QoS: 0}},
    func(m *mqttv5.Message) {
        // Runs on the read goroutine — MUST be non-blocking: the
        // acks of this client's own publishes wait behind it.
        process(m)
        // Ack auto-fires after return.
    },
)
```

Pass `mqttv5.SubZeroCopy()` as a trailing option to skip the copy; the
message's fields are then valid only until the callback returns. The
handler must not call `Disconnect`, which waits for the read goroutine;
start it on another goroutine. While a handler runs, everything else on
the connection waits behind it: other subscriptions' messages and the
acknowledgements of this client's own publishes (a
[publish pool](publisher-pool.md#publish-pool-withpublisherpool) takes publishing off
the connection). The keep-alive does not take that wait for a dead
broker. Work that may take long belongs on a channel or queue
subscription.

## Durable consumers

A consumer with a session the broker keeps (a fixed ClientID,
`WithCleanStart(false)`, and a `WithSessionExpiry` longer than its
downtime) gets the messages that arrived while it was down. The broker
sends them right after the CONNACK, before `Connect` has returned, so
subscribe before `Connect`: `Subscribe` then puts the subscription in
place at once and its SUBSCRIBE goes out first on the connection. A
message no subscription matches is acknowledged and dropped, so a
`Subscribe` after `Connect` races the broker and loses whatever arrives
first; such messages are counted in `Stats().InboundUnrouted`, and the
first on each connection is logged at Warn.

```go
cli, _ := mqttv5.New(
    mqttv5.WithBroker(url),
    mqttv5.WithClientID("billing-worker-1"),
    mqttv5.WithCleanStart(false),        // resume the session after a restart
    mqttv5.WithSessionExpiry(24*60*60),  // the broker keeps it a day
)
msgs, _, _ := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "invoices/#", QoS: 1}})
if err := cli.Connect(ctx); err != nil {
    log.Fatal(err)
}
for m := range msgs { // first what arrived while the worker was down
    handle(m)
    _ = m.Ack()
}
```
