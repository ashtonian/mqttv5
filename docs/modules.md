# Modules

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

The core uses the standard library only. Each optional submodule has
its own `go.mod`, so the core never pulls in its dependencies.

| Submodule | Import | Purpose |
|---|---|---|
| core | `github.com/ashtonian/mqttv5` | Client, supervisor, options, in-memory queue |
| JSON codec | `github.com/ashtonian/mqttv5/codec/json` | `Encoding` for the typed calls (stdlib only) |
| msgpack codec | `github.com/ashtonian/mqttv5/codec/msgpack` | `Encoding` via `vmihailenco/msgpack/v5` |
| File session store | `github.com/ashtonian/mqttv5/store/file` | Crash-safe in-flight QoS 1/2 state |
| File publish queue | `github.com/ashtonian/mqttv5/queue/file` | Crash-safe outbound publish queue for `QueuePublisher` |
| WebSocket transport | `github.com/ashtonian/mqttv5/transport/ws` | ws:// and wss:// — `WithDialFunc(ws.DialFunc(ws.DialOpts{...}))` |

Every module is released with the same version tag (`store/file/vX.Y.Z`
alongside `vX.Y.Z`); install a submodule at the version of the core you
use:

```bash
go get github.com/ashtonian/mqttv5@latest github.com/ashtonian/mqttv5/store/file@latest
```

`store/file` and `queue/file` bring in `internal/filedb`, the bbolt layer
they share, at the same version.

## External dependencies

| Module | Depends on |
|---|---|
| `github.com/ashtonian/mqttv5` (core) | Go standard library only, tests included |
| `store/file`, `queue/file` | [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt) through `internal/filedb` |
| `transport/ws` | [`github.com/gobwas/ws`](https://github.com/gobwas/ws) |
| `codec/json` | standard library |
| `codec/msgpack` | [`github.com/vmihailenco/msgpack/v5`](https://github.com/vmihailenco/msgpack) |
| `benchmarks` | eclipse/paho.golang, eclipse/paho.mqtt.golang, golang.org/x/perf (not a dependency of anything else) |

At runtime the client needs only an MQTT v5 broker reachable over TCP,
TLS or (with `transport/ws`) WebSocket.
