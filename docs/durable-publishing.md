# Durable publishing with `QueuePublisher`

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

`QueuePublisher` decouples the caller from broker availability:

- `Publish` returns as soon as the message is stored in the queue.
- A drain goroutine keeps up to `WithQueueWindow` messages in flight
  (default 32), in queue order, whenever the client is connected, and a
  recorder goroutine removes them from the queue once the broker has
  accepted them. The acknowledgements that arrive together are recorded
  together, in one `Ack` — one transaction, and with `queue/file` one
  sync — so removal keeps pace with the broker. Throughput
  approaches window ÷ round-trip time, the window capped by the broker's
  Receive Maximum (20 on mosquitto): against a broker that allows the
  whole window, at 25 ms, about 1,200 msg/s with the default window and
  600 with a window of 16, against 38 msg/s one at a time
  (`BenchmarkQueuePublisherRTT`).
- With a durable session store (`WithStore`), the records of the
  messages in flight share the store's writes: about half the window
  goes in each `Apply`, with `store/file` one transaction and one fsync.
  Draining a backlog with `store/file`'s group commit (macOS, in-process
  broker without a Receive Maximum) took 73 µs per message with the
  default window, 16 messages to each fsync, and 15 µs with a window of
  256, with `queue/file` on `SyncNone`; with `queue/file` also on group
  commit, whose Acks take fsyncs of their own, 0.42 ms and 85 µs. Raise
  the window when the disk's sync rate, rather than the broker, limits
  the drain.

```go
import (
    qfile "github.com/ashtonian/mqttv5/queue/file"
    sfile "github.com/ashtonian/mqttv5/store/file"
)

st, _ := sfile.Open("/var/lib/myapp/session")
cli, _ := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithClientID("dev-1"), mqttv5.WithStore(st))
q, _ := qfile.Open("/var/lib/myapp/outbound")
pub, _ := mqttv5.NewQueuePublisher(cli, q,
    mqttv5.WithQueueMaxSize(1_000_000),
    mqttv5.WithQueueTTL(24*time.Hour),
    mqttv5.WithDeadLetter(func(e mqttv5.QueueEntry, err error) {
        log.Printf("dropped %s: %v", e.Publish.Topic, err)
    }),
)
defer pub.Close(ctx)

_ = pub.Publish(ctx, mqttv5.PublishOptions{Topic: "logs", Payload: data, QoS: 1})
```

**Failures.** A refusal that cannot pass — PUBACK/PUBREC 0x87 Not
authorized, 0x90 Topic Name invalid, 0x95 Packet too large, 0x99 Payload
format invalid, 0x9A Retain not supported, 0x9B QoS not supported, or the
same verdict from the broker's CONNACK limits — goes to the dead-letter
callback and the message is removed (`WithQueueClassifier` replaces the
rule). Anything else, such as 0x97 Quota exceeded, is retried after
`WithQueueRetryBackoff`, behind the messages that follow it, so one bad
message never blocks the queue. An expired message is dead-lettered with
`ErrMessageExpired` instead of sent; one that is sent carries the
lifetime it has left as its Message Expiry Interval.

**One exchange per message.** A slow broker never causes a second
exchange for the same message. Each message's ID is stored with its
exchange in the client's session (`WithStore`), and the message leaves
the queue before the session forgets the exchange. With `queue/file` and
`store/file`, a restarted process continues the exchanges its
predecessor had in flight — QoS 1 resent with DUP=1, QoS 2 from where it
stopped — rather than publishing those messages again. If removing a
finished message from the queue fails, the session keeps the exchange's
record and the removal is retried with `WithQueueRetryBackoff` (or by
the next process) without publishing the message again. `Close` waits,
until its ctx ends, for the exchanges in flight and their outcomes; one
that finishes later keeps its outcome in the session, and the next
`QueuePublisher` on the queue records it. A message is
published again as new only if the broker lost the session; enable
`WithQueueIdempotencyKey` to add the message ID as the `mqttv5-msg-id`
user property so consumers can drop that copy.

**Bounds.** `WithQueueMaxSize` is enforced atomically by the queue, so
concurrent producers cannot exceed it. At the bound `DropNewest`
(default) returns `ErrQueueFull`; `DropOldest` evicts the oldest
messages not yet being published and dead-letters them with
`ErrQueueFull`. Constructing a `QueuePublisher` never removes anything.
Only `DropOldest` with a bound can evict a message the drain is about
to take, so only then does the drain wait for each `Publish` whose
`Enqueue` is still writing, which with a syncing `queue/file` is a disk
sync; otherwise the drain sends what is stored while producers write.

QoS 0 is rejected (`ErrQoS0NotQueueable`), as is a message that could
never be sent (invalid topic, Topic Alias): the error comes from
`Publish`, not a dead letter later. The publisher copies the message, so
the caller may reuse its buffers.

A `QueuePublisher` publishes on the client's main connection, never
through a [publish pool](publisher-pool.md#publish-pool-withpublisherpool): its exchanges
are recorded in that connection's session, which is what lets them
continue after a reconnect or restart.

Use `mqttv5.NewMemoryPublisherQueue()` for in-process buffering without
crash safety. A custom `PublisherQueue` can be checked against the
contract with `github.com/ashtonian/mqttv5/queue/queuetest`. Its `Ack`
takes the sequence numbers of several entries and removes all of them
or, with an error, none. Entries must become visible to `Peek` in
sequence order however many `Enqueue` calls run at once: assign the
sequence number and store the entry in one atomic step. The drain peeks
after the last sequence number it took while producers enqueue, and
would never see an entry that appeared behind it.
