# Publishing

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

- [Pipelining: `PublishAsync`](#pipelining-publishasync)
- [Write path](#write-path)
- [Broker limits](#broker-limits)

```go
err := cli.Publish(ctx, mqttv5.PublishOptions{
    Topic:   "sensors/a1/temp",
    QoS:     1,
    Payload: []byte(`{"temp":22.5}`),
})
```

When `Publish` returns depends on the QoS:

| QoS | `Publish` returns |
|---|---|
| 0 | once the packet is queued for the connection's writer; with `WithPublishMode(PublishWaitForFlush)`, once it is written |
| 1 | when the broker's PUBACK arrives |
| 2 | when the broker's PUBCOMP arrives |

`ctx` bounds the call. While the client is disconnected `Publish`
returns `ErrNotConnected`, or with `WithPublishWaitForConnection()`
waits for the next connection and sends the message on it; with a
[publish pool](publisher-pool.md#publish-pool-withpublisherpool) that means no member is
connected either, since publishing goes on through the members while
the main connection reconnects. A
[`QueuePublisher`](durable-publishing.md#durable-publishing-with-queuepublisher) queues
through outages instead. A QoS 1/2 message accepted before a
drop belongs to the session: it is resent when the session resumes, and
the caller stays blocked until the broker acknowledges it. A refusal
(reason code 0x80 or above) is returned as a `*ReasonCodeError`.

## Pipelining: `PublishAsync`

A QoS 1/2 `Publish` holds its goroutine until the acknowledgement
arrives, so n messages in flight take n goroutines, each parked on its
own message. `PublishAsync` returns once the session holds the message
and reports the broker's answer through a `*PublishResult`, so one
goroutine keeps as many in flight as it chooses:

```go
const window = 64
outstanding := make([]*mqttv5.PublishResult, window)
for i, reading := range readings {
    if r := outstanding[i%window]; r != nil {
        if err := r.Wait(ctx); err != nil {
            return err // refused, or Disconnect ended the run
        }
    }
    r, err := cli.PublishAsync(ctx, mqttv5.PublishOptions{Topic: reading.Topic, QoS: 1, Payload: reading.JSON})
    if err != nil {
        return err // not accepted: the session does not hold it
    }
    outstanding[i%window] = r
}
for _, r := range outstanding {
    if r != nil {
        if err := r.Wait(ctx); err != nil {
            return err
        }
    }
}
```

- **Accepted or not.** An error from `PublishAsync` means the session
  does not hold the message, and nothing more happens to it. Once
  accepted the message belongs to the session as with `Publish` — a
  dropped connection resends it — and every outcome comes through the
  result. `Done()` closes when the PUBACK (QoS 1) or PUBCOMP (QoS 2)
  arrives; `Err()` is nil until then, and afterwards nil or what
  `Publish` would have returned, a refusal as a `*ReasonCodeError`. With
  `WithStore`, a message whose record the store refuses is never sent,
  and its result completes with the `*StoreError` (see
  [Store failures](operations.md#store-failures)).
  `Wait(ctx)` waits for `Done()` and returns `Err()`.
- **ctx.** The ctx of `PublishAsync` bounds the start only: the wait for
  a connection (with `WithPublishWaitForConnection`), for a packet
  identifier and for room in the write queue. It does not wait for the
  session store: the message's record is written in the background,
  together with the others due, and the PUBLISH goes out once it is. If it ends before the message is queued, the message is taken
  back and the call returns ctx's error. Nothing withdraws an accepted
  message; the ctx of `Wait` bounds that wait alone.
- **Order and writes.** Messages go out in the order they are accepted,
  behind anything the same goroutine published before, on one
  connection (a [publish pool](publisher-pool.md#publish-pool-withpublisherpool)
  spreads them over its members). A QoS 1/2 message is never written on
  the calling goroutine: the writer goroutine sends those accepted
  meanwhile together, in one write.
- **Bounds.** At most the send quota is in flight — the broker's
  Receive Maximum, or `WithMaxInflight` — and the rest wait in the
  session for acknowledgements to make room, each holding its encoded
  packet and a packet identifier. With all 65,535 identifiers in use,
  `PublishAsync` waits for one, bounded by its ctx
  (`ErrPacketIDsExhausted`). Bound what you keep outstanding, as the
  window above does, to bound memory.
- **QoS 0**, including a message `WithQoSDowngrade` lowers to QoS 0,
  has no acknowledgement: it is sent as `Publish` sends it, per
  `WithPublishMode`, and its result is complete at once.
- **Disconnect** completes the outstanding results with `ErrClosed`, or
  with what ended the run; their messages stay in the session for the
  next `Connect`. With a publish pool, `Disconnect` first waits, bounded
  by its ctx, for the results outstanding on members, as it waits for
  running `Publish` calls.
- **Stats.** `Stats().PublishesSent` counts an accepted QoS 1/2 message
  once the broker acknowledges it.

Throughput then follows the window rather than the goroutine count: one
goroutine keeping W messages outstanding approaches W ÷ round-trip time
until the send quota caps it. At 25 ms that is 38 msg/s one at a time,
609 with a window of 16 and 2,408 with 64 (`BenchmarkPublishAsyncRTT`).

Which call to use:

| Need | Use |
|---|---|
| The broker's answer before going on | `Publish` |
| A stream from one goroutine, outcomes checked as they complete | `PublishAsync` |
| More in flight than one connection's Receive Maximum allows | a [publish pool](publisher-pool.md#publish-pool-withpublisherpool), with either call |
| Messages that survive outages and restarts | [`QueuePublisher`](durable-publishing.md#durable-publishing-with-queuepublisher) |

## Write path

- On a TCP or Unix connection with nothing queued, a publish that waits
  for its write (a QoS 1/2 `Publish`, or QoS 0 with
  `PublishWaitForFlush`) writes on its own goroutine: no hand-off, and a
  QoS 0 header and payload go out in one `writev` without the payload
  being copied. A QoS 1/2 `PublishAsync` leaves the write to the writer
  goroutine, so the messages it accepts in a row share one write.
- When publishers overlap, their packets queue for the connection's
  writer goroutine. The QoS 1/2 publishes in the queue go out together,
  up to 64 in one write and as many as the send quota allows; queued
  QoS 0 packets do with `WithWriteBatch(n)`, up to n in one write.
  Fire-and-forget QoS 0 always goes through the writer.
- TLS, WebSocket and QUIC connections have no `writev`: each write is
  a TLS record or WebSocket frame and a syscall of its own. The writer
  copies a packet's header and body, or a batch of packets, into one
  write of up to 16 KiB; a larger payload goes out on its own,
  uncopied.
- If ctx ends during a write, the writer goroutine finishes the packet,
  so it may still reach the broker after `Publish` returned ctx's error.
- `QueuePublisher`'s drain releases the messages it starts in one pass
  together: on an idle TCP or Unix connection it writes them itself, in
  one `writev`, and otherwise the writer goroutine sends them in one
  write.

## Broker limits

Every publish, subscribe and unsubscribe is checked against the CONNACK
before anything is written:

- QoS above Maximum QoS → `ErrQoSNotSupported`, or the QoS is lowered
  with `WithQoSDowngrade()`;
- retain with Retain Available = 0 → `ErrRetainNotSupported`;
- a packet above Maximum Packet Size → `ErrPacketTooLarge`.

At most Receive Maximum QoS 1/2 publishes are in flight, or fewer with
`WithMaxInflight(n)`; further `Publish` calls wait, bounded by ctx, and
further `PublishAsync` messages wait in the session
(`Stats().SendQuota` shows the headroom). The client may send fewer
than the broker's Receive Maximum, never more ([MQTT-4.9.0-1]). Stored
messages that a reconnect to a stricter broker rules out complete with
the same errors. `Client.ServerInfo()` returns everything the broker
granted.

[MQTT-4.9.0-1]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901251 "4.9 Flow Control"
