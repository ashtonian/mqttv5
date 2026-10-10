# codec/msgpack

A MessagePack `Codec[T]` for
[`mqttv5.Typed[T]`](../../README.md#typed-publish--subscribe), built on
[`github.com/vmihailenco/msgpack/v5`](https://github.com/vmihailenco/msgpack).

```go
import msgpackcodec "github.com/ashtonian/mqttv5/codec/msgpack"

readings := mqttv5.NewTyped(cli, msgpackcodec.Codec[Reading]{})
```

| | |
|---|---|
| Module | `github.com/ashtonian/mqttv5/codec/msgpack` |
| Depends on | `github.com/vmihailenco/msgpack/v5` v5.4.1 (not on `mqttv5`: `Codec[T]` satisfies `mqttv5.Codec[T]` structurally) |

## Overview

`Codec[T]` encodes a value with `msgpack.Marshal` and decodes a payload
into a new `T` with `msgpack.Unmarshal` — a smaller payload than JSON
for the same value. `Typed[T].Publish` sends the encoded bytes;
`Typed[T].Subscribe` and `SubscribeQueue` decode each message before
delivering it. A payload that does not decode is logged at Warn
(`mqttv5/typed: decode failed`, with the topic), acknowledged so the
broker does not resend it, and not delivered.

The codec sets no MQTT properties: set `ContentType` in
`PublishOptions` (for example `application/msgpack`) if receivers need
it.

## Configuration

None: the zero value `Codec[T]{}` is the codec. Field names follow the
`msgpack` struct tags on `T`.

## External dependencies

| Module | Version | Used for |
|---|---|---|
| `github.com/vmihailenco/msgpack/v5` | v5.4.1 | encoding and decoding |

## API

| Symbol | Description |
|---|---|
| `Codec[T any]` | MessagePack codec for `T`; zero value usable. |
| `(Codec[T]) Encode(v T) ([]byte, error)` | `msgpack.Marshal(v)`. |
| `(Codec[T]) Decode(b []byte) (T, error)` | `msgpack.Unmarshal` into a fresh `T`. |

Other encodings (Protobuf, CBOR, …) need only a type with these two
methods; nothing in `mqttv5` depends on this package.

## Build, run, test

```bash
go -C codec/msgpack test -race ./...
```

## Operations

| Symptom | Check | Then |
|---|---|---|
| `mqttv5/typed: decode failed` in the logs | the topic in the log entry; what publishes there | The publisher sends another encoding or shape; such messages are acknowledged and dropped. |

## Security

Decoding untrusted payloads allocates in proportion to their size; bound
it with `mqttv5.WithMaximumPacketSize`.

## Code map

| File | What |
|---|---|
| `msgpack.go` | `Codec[T]` |
| `msgpack_test.go` | round trips and decode errors |
