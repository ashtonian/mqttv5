# mqttv5

[![Go Reference](https://pkg.go.dev/badge/github.com/ashtonian/mqttv5.svg)](https://pkg.go.dev/github.com/ashtonian/mqttv5)
[![Go Report Card](https://goreportcard.com/badge/github.com/ashtonian/mqttv5)](https://goreportcard.com/report/github.com/ashtonian/mqttv5)
[![CI](https://github.com/ashtonian/mqttv5/actions/workflows/ci.yml/badge.svg)](https://github.com/ashtonian/mqttv5/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A fast, ergonomic MQTT v5 client for Go. Single package, stdlib-only
core, zero-allocation packet decode, Go-native subscribe
surface (`<-chan *Message` / `Queue[*Message]` / callback), and the
supervisor (reconnect + replay + resubscribe) baked into every
`Client`.

```bash
go get github.com/ashtonian/mqttv5
```

- Module: `github.com/ashtonian/mqttv5`
- License: [Apache 2.0](LICENSE) (with [NOTICE](NOTICE))
- Go: 1.26+
- Benchmarks: [benchmarks/README.md](benchmarks/README.md)

## Example

```go
package main

import (
    "context"
    "fmt"

    "github.com/ashtonian/mqttv5"
    jsoncodec "github.com/ashtonian/mqttv5/codec/json"
    "github.com/ashtonian/mqttv5/wire"
)

type Event struct {
    Device string  `json:"device"`
    Temp   float64 `json:"temp"`
}

func main() {
    ctx := context.Background()

    client, _ := mqttv5.New(mqttv5.WithBroker("mqtt://localhost:1883"))
    _ = client.Connect(ctx)
    defer client.Disconnect(ctx)

    // Generic typed pub/sub via Codec[T] (JSON ships in a sibling
    // submodule). Supervisor handles reconnect + auto-resubscribe +
    // QoS 1/2 replay underneath — you just write the consumer loop.
    events := mqttv5.NewTyped(client, jsoncodec.Codec[Event]{})

    msgs, _, _ := events.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}})
    go func() {
        for m := range msgs {
            fmt.Printf("%s: %+v\n", m.Topic, m.Value) // m.Value already decoded
            _ = m.Ack() // PUBACK held for QoS 1 until you ack
        }
    }()

    _ = events.Publish(ctx, wire.PublishOpts{Topic: "events/hello", QoS: 1}, Event{Device: "a1", Temp: 22.5})
}
```

See [`examples/`](examples/) for full demos — TLS, multi-broker failover,
publisher pool, durable queue, raw-bytes subscribe, WebSocket, OAuth
rotation, and lifecycle observability.

## Why this over [eclipse/paho.golang](https://github.com/eclipse/paho.golang) + [autopaho](https://github.com/eclipse/paho.golang/tree/master/autopaho)

- **Go-idiomatic top to bottom.** Channels (`<-chan *Message`) and
  queues for delivery, not just global `OnPublishReceived` callbacks.
  `context.Context` on every operation. Sentinel errors with
  `errors.Is`. Functional options instead of a 40-field
  `ClientOptions` struct.
- **One client. Supervisor baked in.** No `paho` / `autopaho` split —
  reconnect, replay-in-flight, and auto-resubscribe are always on.
- **27–106× faster decode, zero allocations.** Topic and payload are
  zero-copy slices into a pooled frame; properties decode lazily.
  **~30× less GC pressure** at sustained
  load (~29 MB/s of garbage vs ~880 MB/s for autopaho at 100k msg/s)
  — smaller pauses, less p99 jitter.
- **Multi-broker, kept distinct and composable.** Failover
  (`WithBrokers`), fan-out across N independent brokers (`ClientGroup`),
  publish-only pool against one broker (`WithPublisherPool`) — three
  real patterns, each its own API. Compose them: `WithBrokers` inside
  a `GroupMember` for HA-per-region, then `WithPublisherPool` on top
  for throughput.
- **Publisher writes don't serialise on a mutex.** paho holds one
  mutex across every connection `Write`, so concurrent publishers
  contend on each other's syscalls. mqttv5 has each producer hand its
  packet to a per-connection MPSC channel that a single writer
  goroutine drains to the socket — no write-lock contention on the hot
  path. **~1.6× faster** under 8-goroutine QoS 1 fan-in onto one
  connection. `WithPublisherPool(N)` then runs N such connections, each
  with its own writer goroutine, to spread write load across cores.
- **Backpressure as a first-class concept.** Per-subscription
  `DropNewest` / `DropOldest`, with the dropped message auto-ack'd so
  the broker stops retransmitting.
- **Generic typed payloads.** `Codec[T]` boundary; JSON and msgpack
  codecs ship in separate submodules so the core stays stdlib-only.
- **Durable outbound queue** (`QueuePublisher` + file-backed
  `queue/file/`) — enqueue while disconnected, drain on reconnect,
  survive process restart.
- **Conformance.** Full MQTT v5: shared subscriptions, topic aliases
  (in + out), session expiry, retained messages, will + will
  properties, enhanced authentication (CONNECT + mid-session §4.12),
  CONNACK capability flags honoured — `Subscribe*` errors before the
  wire when the broker has disabled the feature.
- **WebSocket as a sibling module** — `transport/ws` brings ws/wss via
  `WithDialFunc(ws.DialFunc(opts))` (see [`examples/ws`](examples/ws)).
  Zero impact on the core's stdlib-only promise.

## Runnable examples

In [`examples/`](examples/) — one go.mod, run any of them with
`MQTT_BROKER=mqtt://127.0.0.1:1883`:

| Path | Shows |
|---|---|
| [`examples/basic`](examples/basic) | Connect, channel subscribe, publish |
| [`examples/typed`](examples/typed) | `Typed[T]` + JSON codec |
| [`examples/reconnect`](examples/reconnect) | Full lifecycle callback set (Up / Down / ConnectError / ReconnectAttempt) surviving a broker restart |
| [`examples/group`](examples/group) | `ClientGroup` multi-broker fan-out / fan-in |
| [`examples/ws`](examples/ws) | WebSocket transport — `WithDialFunc(ws.DialFunc(opts))` |
| [`examples/stats`](examples/stats) | `Client.Stats()` snapshot — bridge into Prometheus / OTel / expvar |
| [`examples/oauth`](examples/oauth) | `WithConnectPacketBuilder` rotating an OAuth bearer per CONNECT |
| [`examples/disconnect`](examples/disconnect) | `DisconnectWith` carrying ReasonCode + ReasonString + SessionExpiry override |

```bash
docker run -d -p 1883:1883 eclipse-mosquitto
go -C examples run ./basic
```

## Install / submodules

The core is stdlib-only. Opt-in submodules each have their own
`go.mod` so importing them doesn't add a runtime dep to the core.

| Submodule | Import | Purpose |
|---|---|---|
| core | `github.com/ashtonian/mqttv5` | Client, supervisor, options, in-memory queue |
| JSON codec | `github.com/ashtonian/mqttv5/codec/json` | `Codec[T]` for `Typed[T]` (stdlib only) |
| msgpack codec | `github.com/ashtonian/mqttv5/codec/msgpack` | `Codec[T]` via `vmihailenco/msgpack/v5` |
| File session store | `github.com/ashtonian/mqttv5/store/file` | Crash-safe in-flight QoS 1/2 state |
| File publish queue | `github.com/ashtonian/mqttv5/queue/file` | Durable outbound publish queue (WAL) |
| WebSocket transport | `github.com/ashtonian/mqttv5/transport/ws` | ws:// and wss:// — `WithDialFunc(ws.DialFunc(ws.DialOpts{...}))` |

## Three multi-broker patterns

Three distinct shapes, each its own API:

| Goal | API | Connections |
|---|---|---|
| **Failover** — one logical client across interchangeable brokers (same data) | `WithBrokers(urls...)` | 1 at a time, supervisor rotates on drop |
| **Parallel sessions to N independent brokers** | `NewClientGroup(members, opts...)` | N (one per broker), all live |
| **Publish throughput** — saturate one broker | `WithPublisherPool(N)` | N publish-only to the *same* broker |

These compose. Use `WithBrokers` inside a `GroupMember.Opts` to get
HA-per-region fan-out; use `WithPublisherPool` alongside `WithBrokers`
to get throughput against an HA pair.

`Client.SetBrokers(urls...)` swaps the failover list at runtime —
typical use is inside `WithOnServerDisconnect` when the broker sends
`ReasonServerMoved` with a `ServerReference`:

```go
mqttv5.WithOnServerDisconnect(func(d *wire.Disconnect) {
    if ref, ok := d.Properties.String(wire.PropServerReference); ok {
        _ = cli.SetBrokers(ref)
    }
})
```

### `ClientGroup` policies

`ClientGroup` is for **N parallel sessions to N brokers**, each
treated as itself — bridges between independent brokers,
multi-tenant SaaS with per-broker credentials, or a clustered broker
fleet you want N parallel sessions into. If your brokers are
interchangeable for the same data, use `WithBrokers` on a single
Client; `ClientGroup` does not failover between members.

Construction takes a `GroupMember` list plus group-level options:

```go
g, _ := mqttv5.NewClientGroup(
    []mqttv5.GroupMember{
        {
            Broker: "mqtts://emea.example.com:8883",
            Name:   "emea",
            Opts:   []mqttv5.Option{mqttv5.WithCredentials("emea-svc", []byte(token1))},
        },
        {
            Broker: "mqtts://apac.example.com:8883",
            Name:   "apac",
            Opts:   []mqttv5.Option{mqttv5.WithCredentials("apac-svc", []byte(token2))},
        },
    },
    mqttv5.WithGroupSharedOpts(
        mqttv5.WithClientID("fleet"),
        mqttv5.WithKeepAlive(30),
    ),
    mqttv5.WithGroupPublishPolicy(mqttv5.GroupPublishBroadcast),
)
```

`GroupMember.Opts` applies **after** `WithGroupSharedOpts`, so
per-member auth / TLS / ClientID / callbacks win. Member names
default to `member-N` (1-based) when unset.

| Publish policy | Behaviour | Use case |
|---|---|---|
| `GroupPublishBroadcast` (default) | Every member receives. Succeeds if any did. | Bridge / mirror — members carry different data |
| `GroupPublishRoundRobin` | Next healthy member per call. | Distribute publishes across a clustered broker fleet |
| `GroupPublishHashByTopic` | FNV-1a(topic) → member. Per-topic ordering. | Fleet throughput with per-topic affinity |

Subscribe is always "all members + merge into one channel/queue".
The returned token map is keyed by member name — pass to
`UnsubscribeAll` or selectively to `Unsubscribe(name, token)`.

```go
ch, tokens, _ := g.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}})
// tokens["emea"], tokens["apac"]
defer g.UnsubscribeAll(ctx, tokens)
```

Connect / Disconnect / Subscribe run in parallel across members by
default — pass `WithGroupSequentialLifecycle` if you need
deterministic ordering. Use `g.Members()` or `g.Member(name)` for
direct per-member access (per-member `Stats()`, etc.).

## Subscribe shapes

All take `[]TopicFilter` so multi-filter SUBSCRIBE is a single packet.

### Channel — manual ack, ordered flush

```go
msgs, token, err := cli.Subscribe(ctx,
    []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
    mqttv5.SubBuffer(256),
)
for m := range msgs {
    handle(m)
    _ = m.Ack() // PUBACK released in §4.6 arrival order
}
_ = cli.Unsubscribe(ctx, token) // closes msgs
```

If the buffer fills, the incoming message is **auto-ack'd and
dropped** so the broker stops retrying. Observe drops via
`SubOnDrop(...)`.

### Queue — unbounded, optional `DropOldest`

```go
q, _, _ := cli.SubscribeQueue(ctx,
    []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
    mqttv5.SubMaxQueueSize(10_000),
    mqttv5.SubDropPolicy(mqttv5.DropOldest), // keeps freshest 10k
)
for {
    m, ok := q.Dequeue(ctx)
    if !ok { break }
    handle(m)
    _ = m.Ack()
}
```

`DropOldest` evicts the queue head and acks it before enqueueing —
only the queue variant supports it (channels can't peek-and-pop
without racing the consumer).

### Callback — sync, auto-ack

```go
cli.SubscribeCallback(ctx,
    []mqttv5.TopicFilter{{Topic: "ctrl/+", QoS: 0}},
    func(m *mqttv5.Message) {
        // Runs on the read goroutine — MUST be non-blocking.
        process(m)
        // Ack auto-fires after return.
    },
)
```

## Typed publish / subscribe

```go
import jsoncodec "github.com/ashtonian/mqttv5/codec/json"

type Reading struct { Device string; Temp float64 }

typed := mqttv5.NewTyped[Reading](cli, jsoncodec.Codec[Reading]{})

_ = typed.Publish(ctx, wire.PublishOpts{Topic: "sensors/a1", QoS: 1},
    Reading{Device: "a1", Temp: 22.5})

ch, _, _ := typed.Subscribe(ctx,
    []mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}})
for m := range ch {
    fmt.Println(m.Topic, m.Value.Temp)
    _ = m.Ack()
}
```

Implement `mqttv5.Codec[T]` for protobuf, Cap'n Proto, FlatBuffers,
custom binary — the core has no codec dependency.

## Durable `QueuePublisher`

`QueuePublisher` decouples the caller from broker availability:
`Publish` returns as soon as the entry is durably stored. A drain
goroutine handles the broker round-trip whenever the client is
connected.

```go
import qfile "github.com/ashtonian/mqttv5/queue/file"

q, _ := qfile.Open("/var/lib/myapp/outbound")
pub := mqttv5.NewQueuePublisher(cli, q,
    mqttv5.WithQueueBatchSize(32),
    mqttv5.WithQueueMaxSize(1_000_000),
    mqttv5.WithQueueTTL(24*time.Hour),
    mqttv5.WithDeadLetter(func(e mqttv5.QueueEntry, err error) {
        log.Printf("dropped %s: %v", e.Publish.Topic, err)
    }),
)
defer pub.Close(ctx)

_ = pub.Publish(ctx, wire.PublishOpts{Topic: "logs", Payload: data, QoS: 1})
```

QoS 0 is rejected (`ErrQoS0NotQueueable`) — durable enqueue is
meaningless when the broker has no obligation to deliver. Use
`mqttv5.NewMemoryPublisherQueue()` for in-process buffering without
crash safety.

## WebSocket

```go
import (
    "github.com/ashtonian/mqttv5"
    "github.com/ashtonian/mqttv5/transport/ws"
)

cli, _ := mqttv5.New(
    mqttv5.WithBroker("wss://broker.example.com/mqtt"),
    mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg})),
)
```

`wss://` requires a non-nil `TLSConfig` — `ws.DialFunc` returns
`ErrMissingTLSConfig` at the first Connect attempt if you pass `nil`.
No silent downgrade.

## Options reference

### Client construction

| Option | Default | Effect |
|---|---|---|
| `WithBroker(url)` / `WithBrokers(urls...)` | (required) | Broker URL(s). `mqtt`/`tcp`/`mqtts`/`tls`/`ssl` schemes; default ports filled in. `ws`/`wss` via `WithDialFunc(ws.DialFunc(opts))`. |
| `WithDialFunc(fn)` | — | Replaces the built-in TCP/TLS dial. Takes precedence over `WithDialer`/`WithTLSConfig`. Nil rejected. |
| `WithClientID(s)` | broker-assigned | MQTT ClientID. Empty = ask broker to assign one via `AssignedClientIdentifier`; `cli.ClientID()` then returns that assigned value after CONNACK. |
| `WithCredentials(user, pass)` | — | CONNECT username + password. Static. For per-attempt rotation use `WithConnectPacketBuilder`. |
| `WithKeepAlive(seconds)` | 30 | Keepalive interval. 0 rejected — use `WithoutKeepAlive`. |
| `WithoutKeepAlive()` | — | Disable PINGREQ entirely. Rarely correct in production. |
| `WithCleanStart(b)` | true | `CleanStart` on the initial CONNECT. |
| `WithCleanStartOnReconnect(b)` | false | `CleanStart` on every reconnect. False preserves QoS 1/2 session for resume. |
| `WithSessionExpiry(seconds)` | 300 (5 min) | Session Expiry Interval (§3.1.2.11.2). Default holds the broker session long enough for QoS 1/2 resume across a typical reconnect blip. Pass 0 to end the session with the connection. |
| `WithReceiveMaximum(n)` | unset (broker default 65535) | Cap on concurrent inbound QoS 1/2. |
| `WithMaximumPacketSize(n)` | 0 (no advertised limit) | CONNECT property §3.1.2.11.4 — caps the largest packet the broker may send. **Note:** with the default, a buggy / hostile broker can send arbitrarily large PUBLISHes; set explicitly when broker trust is limited. |
| `WithInboundTopicAliasMaximum(n)` | 0 (no inbound aliases) | CONNECT property §3.1.2.11.5 — opt into wire compression on inbound PUBLISHes. |
| `WithRequestResponseInformation(b)` | false | CONNECT property §3.1.2.11.6 — broker returns `ResponseInformation` in CONNACK. |
| `WithRequestProblemInformation(b)` | true | CONNECT property §3.1.2.11.7 — broker returns `ReasonString` / `UserProperties` on errors. On by default; pass `false` to opt out. |
| `WithConnectUserProperty(k, v)` / `WithConnectUserProperties(p)` | — | CONNECT user properties; append-style or bulk replace. |
| `WithConnectTimeout(d)` | 10 s | Dial + CONNECT/CONNACK budget. |
| `WithPingTimeout(d)` | 10 s | PINGRESP budget. Total dead-connection detection = `KeepAlive + PingTimeout` (40 s with defaults), inside the broker's 1.5×KeepAlive cutoff so the client drives reconnect. |
| `WithDisconnectFlushTimeout(d)` | 500 ms | Flush budget for the DISCONNECT write on graceful shutdown. |
| `WithWriteQueueSize(n)` | 256 | Internal MPSC write buffer. |
| `WithWriteBatch(n)` | 0 (off) | Coalesce up to n pre-encoded packets per writev syscall. Wins under sustained concurrent publishers; measure before enabling. |
| `WithWriteOverflowPolicy(p)` | `WriteBlock` | QoS 0 only. `WriteBlock` waits for queue room / ctx; `WriteDropNewest` returns `ErrWriteQueueFull` immediately when the writer queue is full. Use for telemetry where head-of-line latency on the producer is worse than occasional loss. QoS 1/2 always block on the broker ack regardless. |
| `WithWill(opts)` | — | Will message + properties. |
| `WithReconnectBackoff(b)` | `ExponentialBackoff(1s, 30s, 200ms)` | `ConstantBackoff(d)` also shipped. |
| `WithTLSConfig(*tls.Config)` | — | TLS for `mqtts://`. |
| `WithDialer(*net.Dialer)` | default | Override transport `net.Dialer`. |
| `WithStore(s)` | in-memory | `session.Store` impl (use `store/file` for crash safety). |
| `WithLogger(*slog.Logger)` | `slog.Default()` | Structured logging. |
| `WithStats()` | — | Enable in-memory counters for `Client.Stats()` (off by default to keep the hot path branch-predictor friendly). |
| `WithPublishMode(mode)` | `PublishFireAndForget` | `PublishWaitForFlush` makes QoS 0 wait for `conn.Write`. |
| `WithPublisherPool(N)` | 0 (off) | N dedicated publish-only conns. |
| `WithPublisherPoolRouting(p)` | `PoolRoutingRoundRobin` | `PoolRoutingHashByTopic` preserves per-topic ordering. |
| `WithPublisherPoolClientIDFn(fn)` | `"%s-pub-%d"` | Customise per-member ClientIDs. Required when the parent ClientID is empty (broker-assigned). |
| `WithMaxSubscribeQueueSize(n)` | 0 (unbounded) | Default per-sub queue cap. |
| `WithDropPolicy(p)` | `DropNewest` | Default drop policy for full sub buffers. |
| `WithOnConnectionUp(fn)` | — | `func(*wire.Connack)`. Fires after every successful CONNACK; receives a detached clone safe to retain. Must not block. |
| `WithOnConnectionDown(fn)` | — | `func() bool`. Fires on unexpected disconnect (not user Disconnect). Return false to terminate the supervisor. Must not block. |
| `WithOnConnectError(fn)` | — | Fires per failed CONNECT attempt (dial err, CONNACK refusal, AUTH-loop err). Observability only. |
| `WithOnReconnectAttempt(fn)` | — | Fires immediately before each reconnect dial. Receives `(attempt, brokerURL)`. |
| `WithOnServerDisconnect(fn)` | — | Fires on broker-initiated DISCONNECT with detached `*wire.Disconnect`. May call `Client.SetBrokers(...)` to redirect. |
| `WithConnectPacketBuilder(fn)` | — | `func(ctx, *wire.ConnectOpts) error`. Mutate CONNECT immediately before serialisation; canonical OAuth-token-rotation hook. |
| `WithAuthenticator(a)` | — | MQTT v5 enhanced auth (CONNECT + mid-session §4.12). |

### Per-subscribe

`SubscribeOption` applies to `Subscribe`, `SubscribeQueue`,
`SubscribeCallback`, plus the `Typed[T]` and `ClientGroup` variants.

| Option | Effect |
|---|---|
| `SubBuffer(n)` | Channel buffer size (Subscribe only). Default `DefaultSubscribeBuffer` (64). |
| `SubMaxQueueSize(n)` | Queue cap (SubscribeQueue only). 0 = unbounded. |
| `SubDropPolicy(p)` | `DropNewest` / `DropOldest`. SubscribeQueue honours both; chan-based `Subscribe` returns `ErrChanDropOldestUnsupported` when DropOldest is set explicitly. |
| `SubOnDrop(fn)` | Metrics hook fired when a message is dropped + acked. |
| `SubAutoAck()` | Opt-in: dispatcher acks each delivery before handing it to the consumer; the received `*Message` is a detached copy (Topic/Payload/Properties cloned, safe to retain) and `m.Ack()` is a no-op. Trade: 2 allocs/msg + breaks at-least-once semantics (consumer crash between delivery and processing has nothing to replay). Reach for it on QoS 0 / observational consumers. Ignored by `SubscribeCallback`. |

### Per-`QueuePublisher`

| Option | Effect |
|---|---|
| `WithQueueBatchSize(n)` | Drain batch ceiling. Default 16. |
| `WithQueueMaxSize(n)` | Bound the queue; `Publish` returns `ErrQueueFull` when at cap (DropNewest) or evicts the head (DropOldest). |
| `WithQueueDropPolicy(p)` | `DropNewest` (default) or `DropOldest`. DropOldest calls `PublisherQueue.EvictHead`; backends that can't evict return `ErrEvictionNotSupported` at construct. |
| `WithQueueIdleInterval(d)` | Drain-loop wakeup tick when no Enqueue signal arrives. Default 500 ms. |
| `WithQueuePublishTimeout(d)` | Per-message broker handshake cap inside the drain loop. Default 30 s. |
| `WithQueueTTL(d)` | Drop entries older than `d` at drain time; mirrors into `MessageExpiryInterval` so the broker also enforces. |
| `WithDeadLetter(fn)` | Terminal-failure callback (TTL expiry, DropOldest eviction). |

## Observability — `Client.Stats()`

`Client.Stats()` returns a snapshot of in-memory counters. Opt in
via `WithStats()` — when off, the hot path skips every atomic
increment and `Stats()` returns the zero value.

```go
cli, _ := mqttv5.New(
    mqttv5.WithBroker(broker),
    mqttv5.WithStats(),
)
// ...
s := cli.Stats()
fmt.Printf("sent=%d acked=%d inflight=%d connects=%d failures=%d\n",
    s.PublishesSent, s.PublishesAcked, s.PublishesInflight,
    s.Connects, s.ConnectFailures)
```

Counters cover connect/disconnect/publish/subscribe lifecycle, inbound
drops, pool fallbacks, and ping timeouts. Bridge each field into your
own metrics surface (Prometheus / OpenTelemetry / expvar) — the lib
intentionally has no metrics-library dependency. Full field list in
the [`Stats`](https://pkg.go.dev/github.com/ashtonian/mqttv5#Stats)
godoc. See [`examples/stats`](examples/stats).

## Graceful disconnect

`Disconnect(ctx)` sends `ReasonNormalDisconnection` with no
properties. Use `DisconnectWith(ctx, opts)` to override:

```go
expiry := uint32(0)
_ = cli.DisconnectWith(ctx, wire.DisconnectOpts{
    ReasonCode:            wire.ReasonAdministrativeAction,
    ReasonString:          "planned shutdown",
    SessionExpiryInterval: &expiry, // override to drop the session immediately
})
```

The `OnConnectionDown` callback is *not* invoked on a user-initiated
disconnect — the call site itself is the "going down" signal.
See [`examples/disconnect`](examples/disconnect).

## Per-attempt credential rotation

`WithConnectPacketBuilder(fn)` runs immediately before each CONNECT is
serialised. Use it to refresh an OAuth token, fetch a SigV4-signed
CONNECT credential, or rotate any other per-attempt secret. The
context is bounded by `ConnectTimeout`.

```go
mqttv5.WithConnectPacketBuilder(func(ctx context.Context, opts *wire.ConnectOpts) error {
    tok, err := oauth.FetchToken(ctx)
    if err != nil {
        return err // fails this attempt; supervisor retries after backoff
    }
    opts.Username = "service-account"
    opts.Password = []byte(tok)
    return nil
}),
```

Pair with `WithOnConnectError` for observability — every refusal /
network failure fires the callback with the per-attempt error.
See [`examples/oauth`](examples/oauth).

## Sentinel errors

Branch with `errors.Is(err, ...)`; stable across versions.

| Error | Source | Meaning |
|---|---|---|
| `ErrNotConnected` | `Publish`, `Subscribe*`, `Unsubscribe` | No live connection. Retry / wait for reconnect. |
| `ErrAlreadyConnected` | `Connect` | Connect called twice. |
| `ErrClosed` | any after `Disconnect` | Client torn down. |
| `ErrConnectRefused` | `Connect` | Broker non-success CONNACK reason. |
| `ErrUnexpectedPacket` | various | Broker sent an unexpected packet for the current state. Treat as protocol bug. |
| `ErrMissingBroker` | `New` | No URLs supplied. |
| `ErrInvalidBrokerURL` | `New`, `SetBrokers` | URL failed to parse or has unsupported scheme. `WithDialFunc` relaxes scheme validation. |
| `ErrChanDropOldestUnsupported` | `Subscribe` (chan) | Explicit `SubDropPolicy(DropOldest)` on the channel-based Subscribe. Use `SubscribeQueue` for DropOldest. |
| `ErrWriteQueueFull` | `Publish` (QoS 0) | Writer queue at capacity AND client configured with `WithWriteOverflowPolicy(WriteDropNewest)`. The publish never reached the wire. |
| `ErrNilHandler` | `SubscribeCallback` | Handler argument was nil. |
| `ErrSharedSubsUnsupported` | `Subscribe*` | Broker disabled `$share/...` in CONNACK. |
| `ErrWildcardSubsUnsupported` | `Subscribe*` | Broker disabled `+` / `#` in CONNACK. |
| `ErrSubscriptionIDsUnsupported` | `Subscribe*` | Broker disabled SubscriptionIdentifier property. |
| `ErrNoHealthyPublishers` | publisher pool internal | All pool members down — falls back to main conn. |
| `ErrQueueClosed` | `QueuePublisher.Publish` | After `Close`. |
| `ErrQueueFull` | `QueuePublisher.Publish` | `WithQueueMaxSize` cap reached (DropNewest). |
| `ErrQoS0NotQueueable` | `QueuePublisher.Publish` | QoS 0 + durable enqueue is meaningless. |
| `ErrEvictionNotSupported` | `NewQueuePublisher` | DropOldest requested on a backend that can't evict. |
| `ws.ErrMissingTLSConfig` | `transport/ws.Dial` / `DialFunc` | `wss://` URL without a TLS config. No silent downgrade. |

## Performance vs autopaho

Apple M2 Pro, Go 1.26, eclipse-mosquitto on loopback. Full output:
[`benchmarks/e2e_results.txt`](benchmarks/e2e_results.txt). `WithStats`
is off in the published numbers; the in-memory counters compile to a
nil-check on the hot path when disabled, so enabling them is
negligible — re-run the suite if you want exact numbers under your
load.

### Codec micro (`wire` vs `paho.golang/packets`)

| Decode 256 B, no props | autopaho | mqttv5 | speedup |
|---|---:|---:|---:|
| ns/op | 1,326 | **50** | **27×** |
| allocs/op | 22 | **0** | |
| B/op | 5,187 | **0** | |

| Decode 256 B + 5 user properties (lazy) | autopaho | mqttv5 | speedup |
|---|---:|---:|---:|
| ns/op | 5,732 | **54** | **106×** |
| allocs/op | 93 | **0** | |

Decode allocation is **constant in payload size** — `Topic` and
`Payload` are zero-copy slices into a pooled frame.

### End-to-end vs real broker

| Workload, 256 B payload | autopaho | mqttv5 | Result |
|---|---:|---:|---|
| Publish QoS 0 single goroutine | ~5–6 µs | ~5–6 µs | Within run-to-run noise (winner flips per run); mqttv5 is **zero-alloc** (0 vs 15) |
| Publish QoS 1 (waits for PUBACK) | ~154 µs | **~142 µs** | mqttv5 ~8 % faster, 4.8× fewer allocs (11 vs 53) |
| Publish QoS 1 × 8 goroutines, 1 KiB | ~33 µs | **~20 µs** | mqttv5 **~1.6× faster** under fan-in, 5× fewer allocs (11 vs 56) |
| RoundTrip (pub → broker → sub) | ~223 µs | **~184 µs** | mqttv5 ~17 % faster, 4.5× fewer allocs (21 vs 95) |

Numbers are means of the per-run figures in
[`e2e_results.txt`](benchmarks/e2e_results.txt) (`-count=2`) — loopback
to mosquitto, so they are dominated by the broker round-trip and are
**noisy run to run** (±20–30 %). Treat the ratios and alloc counts as
the stable signal, not the absolute µs. Single-producer QoS 0 is a
**wash within that noise** — the winner flips between runs, because
autopaho writes inline under an uncontended mutex while mqttv5 hands the
packet to its writer goroutine; that handoff is a draw at one producer
and turns into the win once producers contend (fan-in). mqttv5 stays
zero-alloc on that path either way. Got a *single* hot publisher and
want more single-connection throughput? `WithWriteBatch(32)` coalesces
queued packets into one `writev` (~25 % faster than autopaho in this
loopback test); it's off by default to keep one-packet-per-segment
framing.

## Reliability semantics

| Behaviour | Detail |
|---|---|
| `Connect` | Blocks until CONNACK (or ctx). Supervisor handles all subsequent reconnects in the background. |
| Reconnect | `ExponentialBackoff(1s, 30s, 200ms)` default. With `WithBrokers`, URLs rotate per attempt; successful connect sticks. |
| Publish QoS 1/2 across drop | Serialised once at register time; replayed on every reconnect with `DUP=1` (§3.3.1.1). Caller stays blocked on session's `Done` and resumes on the eventual ack. |
| Subscribe across drop | Every active subscription re-issued on every reconnect. Subs the broker refused (SUBACK reason ≥ 0x80) drop from the resubscribe set. |
| Mid-session re-auth (§4.12) | Broker AUTH post-CONNACK routes to `Authenticator.Continue`. No Authenticator configured = client emits DISCONNECT 0x87 and reconnects via fresh CONNECT. |
| CONNACK capability flags | `Shared` / `Wildcard` / `SubscriptionIdentifier` availability honoured — `Subscribe*` errors before the wire if the broker disabled the feature. |
| Server-initiated DISCONNECT | `WithOnServerDisconnect(fn)` fires with detached `*wire.Disconnect` before the generic `OnConnectionDown`. Callback may call `Client.SetBrokers(...)` to redirect; new list takes effect on next reconnect. |
| PINGRESP liveness | No PINGRESP within `PingTimeout` → connection treated as dead → supervisor redials. |
| Manual ack ordering | QoS 1 PUBACK held until `m.Ack()`, flushed in §4.6 arrival order. QoS 2 PUBREC held until `m.Ack()`; PUBCOMP fires automatically when PUBREL arrives. |
| Multi-handler dispatch | A PUBLISH matching multiple overlapping filters delivers the **same** `*Message` to every handler. `m.Ack()` is refcounted — only the final call releases the frame. |
| Topic / payload lifetime | Zero-copy slices into a pooled frame; valid until `Ack()` returns. Use `m.CloneTopic()` / `m.ClonePayload()` to retain past `Ack()`. |
| Topic alias outbound | Auto-allocated on QoS 0 publishes when broker advertises `TopicAliasMaximum > 0`. Skipped for QoS 1/2 so replay carries the full topic. |
| Disconnect | Best-effort graceful DISCONNECT (bounded by ctx + `cs.dying`), tears down per-conn goroutines, closes consumer channels/queues. Idempotent. |

## Architecture

One goroutine per connection drives `read → decode → trie match →
handlers (sync)`. A dedicated writer goroutine drains an MPSC channel
of outbound frames — no mutex-around-Write contention. Packets and
frame buffers come from per-type pools; topic and payload are
zero-copy slices into the frame, valid until refcounted
`Message.Ack()`. Properties decode lazily. A supervisor handles
reconnect with configurable backoff, replays in-flight QoS 1/2
publishes with `DUP=1`, and re-issues every tracked subscription.

## Build / test / bench

```bash
# Core — no broker required.
go test ./...
go test -race ./...

# Codec micro benchmarks (no broker).
go -C benchmarks test -bench=. -run=^$ -benchmem -count=3 -benchtime=2s

# End-to-end vs autopaho (needs mosquitto).
docker compose -f conformance/docker-compose.yml up -d mosquitto
go -C benchmarks test -tags e2e -bench='^BenchmarkE2E_' -benchmem -benchtime=2s -count=2

# Conformance suite (mosquitto + emqx).
docker compose -f conformance/docker-compose.yml up -d
go -C conformance test -tags conformance -race -v
```

## Stability

- Wire protocol: MQTT v5 OASIS, stable.
- Public `Client` / `Config` / option / `Stats` surface is stable;
  any breaking change is called out in release notes with a mapping.
- Sentinel errors above are stable; branch on them with `errors.Is`.
- Submodules version independently — each has its own `go.mod`.
- `wire/` codec internals are mutable — treat as private.

## Independence

Independent, clean-room implementation written from the
[MQTT v5.0 OASIS specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
Not a fork of any existing Go MQTT client. The `benchmarks/`
submodule imports `eclipse/paho.golang` for head-to-head comparison
only — it is not redistributed.

## License

Apache 2.0 — see [LICENSE](LICENSE) for the full text and [NOTICE](NOTICE)
for the attribution notice. Per-file headers carry
`SPDX-License-Identifier: Apache-2.0`.
