# Typed publish / subscribe

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

The client gets one `Encoding`; each call names its payload type, so
one client handles any number of them.

```go
import jsoncodec "github.com/ashtonian/mqttv5/codec/json"

type Reading struct {
    Device string
    Temp   float64
}

type Command struct {
    Op string
}

cli, _ := mqttv5.New(
    mqttv5.WithBroker(url),
    mqttv5.WithEncoding(jsoncodec.Encoding{}), // one Encoding, any payload type
)

// Subscribed before Connect, so nothing the broker sends is missed.
readings, _, _ := cli.SubscribeAs[Reading](ctx,
    []mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}})
commands, _, _ := cli.SubscribeAs[Command](ctx,
    []mqttv5.TopicFilter{{Topic: "devices/+/cmd", QoS: 1}})
if err := cli.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer cli.Disconnect(ctx)

// Declares Content Type application/json.
_ = cli.PublishValue(ctx, mqttv5.PublishOptions{Topic: "sensors/a1", QoS: 1},
    Reading{Device: "a1", Temp: 22.5})

for {
    select {
    case m, ok := <-readings:
        if !ok {
            return
        }
        fmt.Println(m.Topic, m.Value.Temp)
        _ = m.Ack()
    case m, ok := <-commands:
        if !ok {
            return
        }
        fmt.Println(m.Topic, m.Value.Op)
        _ = m.Ack()
    }
}
```

- **Subscribing.** `SubscribeAs[T]`, `SubscribeQueueAs[T]` and
  `SubscribeCallbackAs` are `Subscribe`, `SubscribeQueue` and
  `SubscribeCallback` with each payload decoded into a `T`, with the
  same buffering, bounds, drop policy, refusals and closing. A
  `*TypedMessage[T]` carries the decoded `Value` beside the `*Message`,
  which is acked as usual. Payloads are decoded on the connection's read
  goroutine as they arrive, straight into the delivered message.
- **Publishing.** `PublishValue` encodes any value and publishes it as
  `Publish` does, declaring the Encoding's Content Type unless
  `PublishOptions.ContentType` is set; `PublishOptions.Payload` must be
  empty.
- **A payload that does not decode** is not delivered: it is acked, so
  the broker does not send it again, logged at Warn, counted in
  `Stats().DecodeFailures`, and handed with the error to
  `SubOnDecodeError` if the subscription set one.
- **Several encodings.** `SubEncoding` decodes one subscription with
  another `Encoding` than the client's. A `ClientGroup` member uses its
  own, so a group can join brokers whose payloads differ.
- **Groups and queues.** `ClientGroup.SubscribeAs`,
  `ClientGroup.SubscribeQueueAs` and `ClientGroup.PublishValue` decode
  and encode with each member's `Encoding`, and need one on every
  member; `QueuePublisher.PublishValue` encodes before the message is
  queued.
- **Without an Encoding** every typed call returns `ErrNoEncoding`
  before anything is sent.

`codec/json` and `codec/msgpack` provide an `Encoding` each. For
protobuf, Cap'n Proto, FlatBuffers or a format of your own, implement
the three methods (`ContentType`, `Marshal`, `Unmarshal`); `Unmarshal`
must not keep its input, which on a `SubZeroCopy` subscription is the
network frame. The typed calls are generic methods (Go 1.27), and an
interface cannot list a generic method: code that hides `*Client` behind
an interface lists `Subscribe` and decodes itself.
