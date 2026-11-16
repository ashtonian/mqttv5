# Publishing

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

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

## Write path

- On a TCP or Unix connection with nothing queued, a publish that waits
  for its write (QoS 1/2, or QoS 0 with `PublishWaitForFlush`) writes on
  its own goroutine: no hand-off, and a QoS 0 header and payload go out
  in one `writev` without the payload being copied.
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
- `QueuePublisher` hands its window's messages to the writer goroutine,
  so they share writes.

## Broker limits

Every publish, subscribe and unsubscribe is checked against the CONNACK
before anything is written:

- QoS above Maximum QoS → `ErrQoSNotSupported`, or the QoS is lowered
  with `WithQoSDowngrade()`;
- retain with Retain Available = 0 → `ErrRetainNotSupported`;
- a packet above Maximum Packet Size → `ErrPacketTooLarge`.

At most Receive Maximum QoS 1/2 publishes are in flight, or fewer with
`WithMaxInflight(n)`; further ones wait, bounded by ctx
(`Stats().SendQuota` shows the headroom). The client may send fewer
than the broker's Receive Maximum, never more ([MQTT-4.9.0-1]). Stored
messages that a reconnect to a stricter broker rules out complete with
the same errors. `Client.ServerInfo()` returns everything the broker
granted.

[MQTT-4.9.0-1]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901251 "4.9 Flow Control"
