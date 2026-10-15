# codec/json

A JSON `Encoding` for mqttv5's [typed calls](../../docs/typed-payloads.md)
(`Client.PublishValue`, `Client.SubscribeAs` and their variants), built
on the standard library's `encoding/json`.

```go
import jsoncodec "github.com/ashtonian/mqttv5/codec/json"

type Reading struct {
    Device string  `json:"device"`
    Temp   float64 `json:"temp"`
}

cli, _ := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithEncoding(jsoncodec.Encoding{}))
readings, _, _ := cli.SubscribeAs[Reading](ctx, []mqttv5.TopicFilter{{Topic: "t/+", QoS: 1}})
if err := cli.Connect(ctx); err != nil {
    return err
}
_ = cli.PublishValue(ctx, mqttv5.PublishOptions{Topic: "t/a1", QoS: 1}, Reading{Device: "a1", Temp: 21.5})
```

| | |
|---|---|
| Module | `github.com/ashtonian/mqttv5/codec/json` |
| Depends on | the Go standard library only (not even on `mqttv5`: `Encoding` satisfies `mqttv5.Encoding` structurally) |

## Overview

`Encoding` marshals any value with `json.Marshal` and unmarshals a
payload with `json.Unmarshal`. One `Encoding` serves every payload type
the client publishes or subscribes to; the type is named per call. A
publish made with it declares the MQTT 5 Content Type
`application/json` unless `PublishOptions.ContentType` says otherwise.

A payload that does not decode is not delivered: the client acks it so
the broker does not resend it, logs it at Warn (`mqttv5: payload decode
failed`, with the topic), counts it in `Stats().DecodeFailures` and
hands it to the subscription's `SubOnDecodeError`, if any.

## Configuration

None: the zero value `Encoding{}` is the encoding. Field names and
omission follow `encoding/json` struct tags.

## External dependencies

None beyond the standard library.

## API

| Symbol | Description |
|---|---|
| `Encoding` | JSON encoding; zero value usable, safe for concurrent use. |
| `(Encoding) ContentType() string` | `MediaType`. |
| `(Encoding) Marshal(v any) ([]byte, error)` | `json.Marshal(v)`. |
| `(Encoding) Unmarshal(data []byte, v any) error` | `json.Unmarshal(data, v)`; keeps nothing of `data`. |
| `MediaType` | `"application/json"`. |

## Build, run, test

```bash
go -C codec/json test -race ./...
```

[`examples/typed`](../../examples/typed) runs it against a broker.

## Operations

| Symptom | Check | Then |
|---|---|---|
| `mqttv5: payload decode failed` in the logs; `Stats().DecodeFailures` rising | the topic in the log entry; what publishes there | The publisher sends something other than JSON, or another shape than the subscription's type; such messages are acknowledged and not delivered. |
| Fields arrive empty | JSON field names | Unexported fields and mismatched tags are ignored by `encoding/json`. |

## Security

Decoding untrusted payloads allocates in proportion to their size; bound
it with `mqttv5.WithMaximumPacketSize`. `encoding/json` does not execute
code.

## Code map

| File | What |
|---|---|
| `json.go` | `Encoding`, `MediaType` |
| `json_test.go` | round trips, decode errors, and that a decoded value keeps nothing of its input |
