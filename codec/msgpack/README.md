# codec/msgpack

A MessagePack `Encoding` for mqttv5's
[typed calls](../../README.md#typed-publish--subscribe)
(`Client.PublishValue`, `Client.SubscribeAs` and their variants), built
on [`github.com/vmihailenco/msgpack/v5`](https://github.com/vmihailenco/msgpack).

```go
import msgpackcodec "github.com/ashtonian/mqttv5/codec/msgpack"

cli, _ := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithEncoding(msgpackcodec.Encoding{}))
readings, _, _ := cli.SubscribeAs[Reading](ctx, []mqttv5.TopicFilter{{Topic: "t/+", QoS: 1}})
if err := cli.Connect(ctx); err != nil {
    return err
}
```

| | |
|---|---|
| Module | `github.com/ashtonian/mqttv5/codec/msgpack` |
| Depends on | `github.com/vmihailenco/msgpack/v5` v5.4.1 (not on `mqttv5`: `Encoding` satisfies `mqttv5.Encoding` structurally) |

## Overview

`Encoding` marshals any value with `msgpack.Marshal` and unmarshals a
payload with `msgpack.Unmarshal` — a smaller payload than JSON for the
same value. One `Encoding` serves every payload type; the type is named
per call. A publish made with it declares the MQTT 5 Content Type
`application/vnd.msgpack`, MessagePack's IANA registration, unless
`PublishOptions.ContentType` says otherwise.

A payload that does not decode is not delivered: the client acks it so
the broker does not resend it, logs it at Warn (`mqttv5: payload decode
failed`, with the topic), counts it in `Stats().DecodeFailures` and
hands it to the subscription's `SubOnDecodeError`, if any.

To receive msgpack on one subscription of a client whose `Encoding` is
JSON, pass `mqttv5.SubEncoding(msgpackcodec.Encoding{})` to it.

## Configuration

None: the zero value `Encoding{}` is the encoding. Field names follow
the `msgpack` struct tags.

## External dependencies

| Module | Version | Used for |
|---|---|---|
| `github.com/vmihailenco/msgpack/v5` | v5.4.1 | encoding and decoding |

## API

| Symbol | Description |
|---|---|
| `Encoding` | MessagePack encoding; zero value usable, safe for concurrent use. |
| `(Encoding) ContentType() string` | `MediaType`. |
| `(Encoding) Marshal(v any) ([]byte, error)` | `msgpack.Marshal(v)`. |
| `(Encoding) Unmarshal(data []byte, v any) error` | `msgpack.Unmarshal(data, v)`; keeps nothing of `data`. |
| `MediaType` | `"application/vnd.msgpack"`. |

Other formats (Protobuf, CBOR, …) need only a type with these three
methods; nothing in `mqttv5` depends on this package.

## Build, run, test

```bash
go -C codec/msgpack test -race ./...
```

## Operations

| Symptom | Check | Then |
|---|---|---|
| `mqttv5: payload decode failed` in the logs; `Stats().DecodeFailures` rising | the topic in the log entry; what publishes there | The publisher sends another encoding or shape; such messages are acknowledged and not delivered. |

## Security

Decoding untrusted payloads allocates in proportion to their size; bound
it with `mqttv5.WithMaximumPacketSize`.

## Code map

| File | What |
|---|---|
| `msgpack.go` | `Encoding`, `MediaType` |
| `msgpack_test.go` | round trips, decode errors, and that a decoded value keeps nothing of its input |
