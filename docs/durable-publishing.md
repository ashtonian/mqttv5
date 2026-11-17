# Durable publishing with `QueuePublisher`

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

`QueuePublisher` decouples the caller from broker availability:

- `Publish` returns as soon as the message is stored in the queue.
- A drain goroutine publishes the queue's messages in order whenever the
  client is connected, holding up to `WithQueueWindow` of them (default
  4,096) and `WithQueueWindowBytes` of their bytes (default 8 MiB) at
  once, and a recorder goroutine removes them from the queue
  once the broker has accepted them. The acknowledgements that arrive
  together are recorded together, in one `Ack` — one transaction, and
  with `queue/file` one sync — so removal keeps pace with the broker.
- With a durable session store (`WithStore`), the records of the
  messages taken together are stored together, in one `Apply`: with
  `store/file`, one transaction and one sync.
- Throughput approaches the window ÷ the time each message holds its
  place in it; see [Sizing the window](#sizing-the-window).

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
published again as new only if the broker lost the session, or, without
a durable session store, if the process stopped while it was in flight
or accepted and not yet removed: at most the window. Enable
`WithQueueIdempotencyKey` to add the message ID as the `mqttv5-msg-id`
user property so consumers can drop that copy.

**Bounds.** `WithQueueMaxSize` is enforced atomically by the queue, so
concurrent producers cannot exceed it. At the bound `DropNewest`
(default) returns `ErrQueueFull`; `DropOldest` evicts the oldest
messages not yet being published and dead-letters them with
`ErrQueueFull`. The window's messages are never evicted, so under
`DropOldest` the default window is at most half the bound. Constructing
a `QueuePublisher` never removes anything.
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
contract with `github.com/ashtonian/mqttv5/queue/queuetest`. Its `Peek`
takes a `PeekLimit`: at most `Entries` entries, ending with the one
whose `Size` brings their total to `Bytes` (`PeekLimit.Reached` is the
rule), and the first entry whatever its size. Its `Ack`
takes the sequence numbers of several entries and removes all of them
or, with an error, none. Entries must become visible to `Peek` in
sequence order however many `Enqueue` calls run at once: assign the
sequence number and store the entry in one atomic step. The drain peeks
after the last sequence number it took while producers enqueue, and
would never see an entry that appeared behind it.

## Sizing the window

A message holds its place in the window from when the drain takes it
until it has left the queue:

- the broker's round trip, PUBLISH to PUBACK (to PUBCOMP for QoS 2);
- with a syncing `queue/file` (`SyncGroupCommit`, the default), the sync
  that removes it. A group commit removes the messages accepted while
  the one before it ran, so a message waits about two syncs and each
  sync removes about half the window;
- with a syncing session store such as `store/file`, the sync that
  stores its record, which comes before the PUBLISH is sent.

The drain approaches window ÷ that time, so size the window as the rate
you need × that time. The broker's Receive Maximum (20 on mosquitto) and
`WithMaxInflight` bound only the messages sent and not yet
acknowledged; the rest of the window waits to be stored, sent or
removed, so a window larger than the Receive Maximum still pays.

The window has two bounds: `WithQueueWindow` messages (default 4,096)
and `WithQueueWindowBytes` bytes of `QueueEntry.Size` — ID, topic,
payload and properties (default 8 MiB). Small messages reach the count
first, large ones the bytes. The drain always takes the next message
when it holds none, so a message larger than the byte bound goes alone.

Draining a backlog of 64-byte QoS 1 messages, `queue/file` and
`store/file` both on group commit, against an in-process broker with a
Receive Maximum of 20 (macOS M2 Max, medians of 6 interleaved runs), per
message, with the messages each sync removed or stored:

| Window | `queue/file`, no session store | `queue/file` and `store/file` |
|---:|---:|---:|
| 32 | 72 µs (16 per sync) | 311 µs (10–13) |
| 256 | 11 µs (121) | 27 µs (76) |
| 1,024 | 7.4 µs (216) | 10.6 µs (222–274) |
| 4,096 (default) | 7.3 µs (208) | 8.4 µs (282–396) |

Up to 1,024 the syncs bound the drain: the committers were busy
87–99% of the time, and each sync took more messages as the window
grew. By 4,096 they no longer do, and the client and broker set the
pace. This disk's syncs (1.1–3.4 ms) were up to three times slower in
some runs; a disk whose syncs take longer needs a proportionally larger
window for the same rate. Against a broker 25 ms away, with a memory
queue, the drain approaches window ÷ round trip: 39 msg/s with a window
of 1, 1,250 with 32, 9,900 with 256 and 138,000 with 4,096
(`BenchmarkQueuePublisherRTT`).

For larger messages the byte bound decides. With `store/file` (same
setup, medians of 3 runs):

| Payload | 2 MiB | 8 MiB (default) | 32 MiB |
|---:|---:|---:|---:|
| 4 KiB | 33 µs | 21.5 µs | 20.5 µs |
| 64 KiB | 434 µs | 184 µs (356 MB/s) | 182 µs |

A larger window costs:

- **Memory:** the window's messages are held in memory, and while one
  is published and stored, its encoded packet and write buffers as
  well. The window's live memory measured two to three times the byte
  bound: about 16 MiB at the default with 64 KiB messages.
- **Duplicates without a durable session store:** a restart publishes
  again the messages that were in flight or accepted and not yet
  removed, at most the window. QoS 1 allows the copy;
  `WithQueueIdempotencyKey` lets consumers drop it.
- **Eviction:** under `DropOldest` the window's messages are never
  evicted, so the default window is at most half of `WithQueueMaxSize`.

Lower the byte bound to cap memory; raise either bound for a long round
trip or a slow disk.
