# codec/json

A JSON `Codec[T]` for [`mqttv5.Typed[T]`](../../README.md#typed-publish--subscribe),
built on the standard library's `encoding/json`.

```go
import jsoncodec "github.com/ashtonian/mqttv5/codec/json"

type Reading struct {
    Device string  `json:"device"`
    Temp   float64 `json:"temp"`
}

readings := mqttv5.NewTyped(cli, jsoncodec.Codec[Reading]{})
_ = readings.Publish(ctx, mqttv5.PublishOptions{Topic: "t/a1", QoS: 1}, Reading{Device: "a1", Temp: 21.5})
```

| | |
|---|---|
| Module | `github.com/ashtonian/mqttv5/codec/json` |
| Depends on | the Go standard library only (not even on `mqttv5`: `Codec[T]` satisfies `mqttv5.Codec[T]` structurally) |

## Overview

`Codec[T]` encodes a value with `json.Marshal` and decodes a payload into
a new `T` with `json.Unmarshal`. `Typed[T].Publish` sends the encoded
bytes as the payload; `Typed[T].Subscribe` and `SubscribeQueue` decode
each message before delivering it. A payload that does not decode is
logged at Warn (`mqttv5/typed: decode failed`, with the topic),
acknowledged so the broker does not resend it, and not delivered.

The codec sets no MQTT properties: set `ContentType: "application/json"`
or `PayloadFormatIndicator` in `PublishOptions` if receivers need them.

## Configuration

None: the zero value `Codec[T]{}` is the codec. Field names and
omission follow `encoding/json` struct tags on `T`.

## External dependencies

None beyond the standard library.

## API

| Symbol | Description |
|---|---|
| `Codec[T any]` | JSON codec for `T`; zero value usable. |
| `(Codec[T]) Encode(v T) ([]byte, error)` | `json.Marshal(v)`. |
| `(Codec[T]) Decode(b []byte) (T, error)` | `json.Unmarshal` into a fresh `T`. |

## Build, run, test

```bash
go -C codec/json test -race ./...
```

[`examples/typed`](../../examples/typed) runs it against a broker.

## Operations

| Symptom | Check | Then |
|---|---|---|
| `mqttv5/typed: decode failed` in the logs | the topic in the log entry; what publishes there | The publisher sends something other than JSON for `T`, or a different shape; such messages are acknowledged and dropped. |
| Fields arrive empty | JSON field names | Unexported fields and mismatched tags are ignored by `encoding/json`. |

## Security

Decoding untrusted payloads allocates in proportion to their size; bound
it with `mqttv5.WithMaximumPacketSize`. `encoding/json` does not execute
code.

## Code map

| File | What |
|---|---|
| `json.go` | `Codec[T]` |
| `json_test.go` | round trips and decode errors |
