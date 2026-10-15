# mqttv5

[![Go Reference](https://pkg.go.dev/badge/github.com/ashtonian/mqttv5.svg)](https://pkg.go.dev/github.com/ashtonian/mqttv5)
[![CI](https://github.com/ashtonian/mqttv5/actions/workflows/ci.yml/badge.svg)](https://github.com/ashtonian/mqttv5/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

An idiomatic MQTT v5 client for Go with built-in reconnects, typed
payloads, durable sessions, and support for multiple brokers.

Requires **Go 1.27 or later**. **Pre-1.0:** minor releases may change
the API; see [stability](docs/reference.md#stability).

## Why mqttv5

- **Standard-library-only core.** Persistence, WebSocket transport and
  payload codecs are optional submodules.
- **Built-in supervisor.** Reconnects, session resumption, in-flight
  replay and re-subscription are part of every `Client`.
- **Delivery you control.** Channels, bounded queues or callbacks,
  with manual acknowledgements and per-subscription backpressure.
- **Typed payloads and enhanced authentication.** JSON or msgpack
  encoding, custom codecs, rotating credentials and re-authentication.
- **Durable publishing.** Optional file-backed queues and session
  stores continue publishing through outages and process restarts.
- **Multiple brokers.** Failover, parallel sessions and publish pools
  cover different connection patterns.

See the [comparison with Paho](docs/comparison.md),
[reliability guarantees](docs/reliability.md), and
[broker conformance suite](conformance/).

## Install

```bash
go get github.com/ashtonian/mqttv5
```

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ashtonian/mqttv5"
)

func main() {
    ctx := context.Background()

    cli, err := mqttv5.New(
        mqttv5.WithBroker("mqtt://localhost:1883"),
        mqttv5.WithClientID("quickstart"),
    )
    if err != nil {
        log.Fatal(err)
    }
    if err := cli.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer cli.Disconnect(ctx)

    // Reconnects, session resumption and re-subscription happen underneath.
    msgs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}})
    if err != nil {
        log.Fatal(err)
    }

    err = cli.Publish(ctx, mqttv5.PublishOptions{Topic: "sensors/a1", QoS: 1, Payload: []byte("22.5")})
    if err != nil {
        log.Fatal(err)
    }

    m := <-msgs
    fmt.Printf("%s: %s\n", m.Topic, m.Payload)
    _ = m.Ack() // the broker gets its PUBACK once the message is acked
}
```

Run it against a local broker:

```bash
docker run -d -p 1883:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf
go -C examples run ./readme/quickstart
```

The example uses channel delivery: call `Ack` after processing each
message. For a consumer resuming a durable session, [subscribe before
connecting](docs/subscribing.md#durable-consumers) so queued messages
have a handler as soon as they arrive.

## Multiple brokers

| Need | API | Connections |
|---|---|---|
| Fail over among interchangeable brokers or cluster nodes | `WithBrokers` | One at a time |
| Reach independent brokers with separate data | `ClientGroup` | One per member, all live |
| Increase concurrent publishing to the client's broker(s) | `WithPublisherPool` | Additional publish-only connections |

These patterns compose. See the [multi-broker guide](docs/multi-broker.md),
[publish pools](docs/publisher-pool.md), and [recipes](docs/recipes.md)
for bridging, regional HA, quorum writes and sharding.

## Performance

Allocations per `Publish` call with a **1 KiB payload**, measured against
the same pinned mosquitto broker. QoS 0 waits for the packet to be
written (`PublishWaitForFlush`); QoS 1/2 wait for the broker's
acknowledgement. `autopaho` is MQTT 5; `paho3` is MQTT 3.1.1.

<!-- benchtab file=benchmarks/results/2026-10-10-e2e/e2e.txt filter=".name:E2E_Publish /size:1KiB" rows=qos cols=lib unit=allocs/op -->

| qos | mqttv5 | autopaho | paho3 |
|---|---:|---:|---:|
| 0 | 0 | 15 | 23 |
| 1 | 5 | 53 | 42 |
| 2 | 6 | 92 | 62 |

<sub>allocs/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 80beb93780a2.</sub>
<!-- /benchtab -->

See the [full benchmarks](benchmarks/README.md) for decoding and delivery
allocations, throughput, latency, methodology and reproduction commands.

## Documentation

| Task | Read |
|---|---|
| Publish and receive messages | [Publishing](docs/publishing.md), [subscriptions](docs/subscribing.md), [typed payloads](docs/typed-payloads.md) |
| Keep messages through outages | [Durable publishing](docs/durable-publishing.md), [reliability](docs/reliability.md) |
| Add authentication or another transport | [Authentication](docs/authentication.md), [WebSocket](docs/websocket.md) |
| Connect multiple brokers | [Multi-broker guide](docs/multi-broker.md), [recipes](docs/recipes.md) |
| Find options, errors and defaults | [Configuration reference](docs/reference.md), [Go API](https://pkg.go.dev/github.com/ashtonian/mqttv5) |
| Operate a client | [Metrics and troubleshooting](docs/operations.md), [security](docs/security.md) |
| Try runnable code | [Examples](examples/README.md) |
| Contribute | [Build and test](CONTRIBUTING.md), [architecture](docs/architecture.md) |

The [documentation index](docs/README.md) also covers request/response,
disconnecting, optional modules and dependencies.

## License

Apache 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE). Independent,
clean-room implementation of the [MQTT v5.0 specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
