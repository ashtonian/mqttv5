# mqttv5

[![Go Reference](https://pkg.go.dev/badge/github.com/ashtonian/mqttv5.svg)](https://pkg.go.dev/github.com/ashtonian/mqttv5)
[![CI](https://github.com/ashtonian/mqttv5/actions/workflows/ci.yml/badge.svg)](https://github.com/ashtonian/mqttv5/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

An MQTT v5 client for Go: a low cost per message, an API that reads
like Go, and QoS 1/2 sessions that survive reconnects and, with a file
store, process restarts.

- One package, standard library only. Persistence, WebSocket and codecs
  are optional submodules.
- Reconnect, session resumption and re-subscription are built into every
  `Client`.
- Messages arrive on a channel, a bounded queue or a callback, and are
  acknowledged when you say so.
- Decoding a PUBLISH of up to 64 KiB allocates nothing once pools are
  warm.

```bash
go get github.com/ashtonian/mqttv5
```

Requires Go 1.26 or later. Licensed under [Apache 2.0](LICENSE).

## Contents

- **Start:** [Quick start](#quick-start) · [Why mqttv5](#why-mqttv5) ·
  [Examples](#examples) · [Modules](#modules)
- **Guide:** [Publishing](#publishing) · [Subscribing](#subscribing) ·
  [Typed publish / subscribe](#typed-publish--subscribe) ·
  [Durable publishing](#durable-publishing-with-queuepublisher) ·
  [Authentication](#authentication) · [WebSocket](#websocket) ·
  [Multiple brokers](#multiple-brokers) · [Disconnecting](#disconnecting)
- **Reference:** [Options](#options) · [Errors](#errors) ·
  [Reliability semantics](#reliability-semantics) ·
  [Performance](#performance)
- **Operating:** [Operations](#operations) · [Security](#security)
- **Contributing:** [Architecture](#architecture) · [Code map](#code-map) ·
  [External dependencies](#external-dependencies) ·
  [Build and test](#build-and-test)
- **Project:** [Stability](#stability) · [Independence](#independence) ·
  [License](#license)

---

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

---

## Why mqttv5

Compared with [eclipse/paho.golang](https://github.com/eclipse/paho.golang)
and its [autopaho](https://github.com/eclipse/paho.golang/tree/master/autopaho)
supervisor:

- **One client, supervisor built in.** Reconnect, in-flight replay and
  re-subscription are always on; there is no second package to wire up.
- **A Go-shaped API.** Channels and queues for delivery as well as
  callbacks, `context.Context` on every call, sentinel errors for
  `errors.Is`, and functional options instead of a large options struct.
- **A low cost per message.** Decoding a PUBLISH of up to 64 KiB
  allocates nothing once pools are warm; a delivered message costs two
  allocations at QoS 0 ([Performance](#performance)).
- **Messages you can keep.** A delivered `*Message` owns its topic,
  payload and properties, so it is safe to retain, and `Ack` may be
  called more than once. `SubZeroCopy()` skips the copy on hot paths.
- **Backpressure per subscription.** A consumer that falls behind loses
  QoS 0 messages, never QoS 1 or 2: the broker's flow control holds
  those back. A dropped message is acknowledged so the broker stops
  resending it.
- **Several brokers, three distinct patterns.** Failover
  (`WithBrokers`), parallel sessions (`ClientGroup`) and a publish pool
  (`WithPublisherPool`), each its own API, and they compose
  ([Multiple brokers](#multiple-brokers)).
- **Durable publishing.** `QueuePublisher` with `queue/file` and
  `store/file` keeps publishing through outages and survives a process
  crash without publishing an in-flight message twice.
- **Typed payloads.** `Typed[T]` over a `Codec[T]`; JSON and msgpack
  codecs ship as submodules, so the core stays standard library only.
- **The MQTT v5 feature set, tested against real brokers.** Shared
  subscriptions, Subscription Identifiers, topic aliases in both
  directions, session expiry and resumption, retained messages, the Will
  and its properties, server redirects, enhanced authentication (CONNECT
  and mid-session, [§4.12]), and the broker's CONNACK limits enforced
  before a packet is sent. The [conformance suite](conformance/) runs
  against mosquitto and EMQX on every change, and nightly against HiveMQ
  CE, including one that announces tight CONNACK limits.
- **WebSocket without a core dependency.** `transport/ws` plugs in
  through `WithDialFunc` ([WebSocket](#websocket)).

---

## Examples

[`examples/`](examples/) is one module of runnable programs. Each reads
the broker from `MQTT_BROKER` (default `mqtt://127.0.0.1:1883`):

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
docker run -d -p 1883:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf
go -C examples run ./basic
```

---

## Modules

The core uses the standard library only. Each optional submodule has
its own `go.mod`, so the core never pulls in its dependencies.

| Submodule | Import | Purpose |
|---|---|---|
| core | `github.com/ashtonian/mqttv5` | Client, supervisor, options, in-memory queue |
| JSON codec | `github.com/ashtonian/mqttv5/codec/json` | `Codec[T]` for `Typed[T]` (stdlib only) |
| msgpack codec | `github.com/ashtonian/mqttv5/codec/msgpack` | `Codec[T]` via `vmihailenco/msgpack/v5` |
| File session store | `github.com/ashtonian/mqttv5/store/file` | Crash-safe in-flight QoS 1/2 state |
| File publish queue | `github.com/ashtonian/mqttv5/queue/file` | Crash-safe outbound publish queue for `QueuePublisher` |
| WebSocket transport | `github.com/ashtonian/mqttv5/transport/ws` | ws:// and wss:// — `WithDialFunc(ws.DialFunc(ws.DialOpts{...}))` |

Every module is released with the same version tag (`store/file/v0.11.0`
alongside `v0.11.0`); install a submodule at the version of the core you
use:

```bash
go get github.com/ashtonian/mqttv5@v0.11.0 github.com/ashtonian/mqttv5/store/file@v0.11.0
```

`store/file` and `queue/file` bring in `internal/filedb`, the bbolt layer
they share, at the same version.

---

## Publishing

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
returns `ErrNotConnected`; a [`QueuePublisher`](#durable-publishing-with-queuepublisher)
queues through outages instead. A QoS 1/2 message accepted before a
drop belongs to the session: it is resent when the session resumes, and
the caller stays blocked until the broker acknowledges it. A refusal
(reason code 0x80 or above) is returned as a `*ReasonCodeError`.

### Write path

- On a TCP or Unix connection with nothing queued, a publish that waits
  for its write (QoS 1/2, or QoS 0 with `PublishWaitForFlush`) writes on
  its own goroutine: no hand-off, and a QoS 0 header and payload go out
  in one `writev` without the payload being copied.
- When publishers overlap, their packets queue for the connection's
  writer goroutine, which writes them together; `WithWriteBatch(n)`
  coalesces them into one `writev`. Fire-and-forget QoS 0 always goes
  through the writer.
- If ctx ends during a write, the writer goroutine finishes the packet,
  so it may still reach the broker after `Publish` returned ctx's error.
- `WithPublisherPool(N)` spreads publishing over N connections.

### Broker limits

Every publish, subscribe and unsubscribe is checked against the CONNACK
before anything is written:

- QoS above Maximum QoS → `ErrQoSNotSupported`, or the QoS is lowered
  with `WithQoSDowngrade()`;
- retain with Retain Available = 0 → `ErrRetainNotSupported`;
- a packet above Maximum Packet Size → `ErrPacketTooLarge`.

At most Receive Maximum QoS 1/2 publishes are in flight; further ones
wait, bounded by ctx (`Stats().SendQuota` shows the headroom). Stored
messages that a reconnect to a stricter broker rules out complete with
the same errors. `Client.ServerInfo()` returns everything the broker
granted.

---

## Subscribing

Three delivery shapes. Each takes `[]TopicFilter`, so several filters
go out in one SUBSCRIBE.

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

A consumer that falls behind loses QoS 0 messages, never QoS 1 or 2.
Once the channel holds `SubBuffer` messages, an incoming QoS 0 message
is **acked and dropped**; observe drops via `SubOnDrop(...)`. QoS 1
and 2 messages have room for `WithReceiveMaximum` more (default 256):
as many as the broker sends before the consumer acks one, so the
broker holds the rest back. Two exceptions: `SubAutoAck()` acks on
receipt, which gives up that flow control, so QoS 1 and 2 messages are
dropped as QoS 0 ones are; and messages still unread from a session
the broker discarded take room of their own.

### Queue — bounded, optional `DropOldest`

```go
q, _, _ := cli.SubscribeQueue(ctx,
    []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}},
    mqttv5.SubMaxQueueSize(10_000),
    mqttv5.SubDropPolicy(mqttv5.DropOldest), // keeps freshest 10k
)
for {
    m, ok := q.Dequeue(ctx)
    if !ok {
        break
    }
    handle(m)
    _ = m.Ack()
}
```

Queues hold at most `DefaultMaxSubscribeQueueSize` (65,536) messages
unless `WithMaxSubscribeQueueSize` / `SubMaxQueueSize` say otherwise;
`mqttv5.UnboundedQueue` removes the cap. When full, `DropNewest` (the
default) acks and drops an incoming QoS 0 message, while QoS 1 and 2
messages have the same room beyond the cap as on a channel;
`DropOldest` evicts the queue head, whatever its QoS, and acks it
before enqueueing.

Only the queue variant supports `DropOldest` — channels can't
peek-and-pop without racing the consumer.

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

Pass `mqttv5.SubZeroCopy()` as a trailing option to skip the copy; the
message's fields are then valid only until the callback returns. The
handler must not call `Disconnect`, which waits for the read goroutine;
start it on another goroutine.

---

## Typed publish / subscribe

```go
import jsoncodec "github.com/ashtonian/mqttv5/codec/json"

type Reading struct {
    Device string
    Temp   float64
}

typed := mqttv5.NewTyped[Reading](cli, jsoncodec.Codec[Reading]{})

_ = typed.Publish(ctx, mqttv5.PublishOptions{Topic: "sensors/a1", QoS: 1},
    Reading{Device: "a1", Temp: 22.5})

ch, _, _ := typed.Subscribe(ctx,
    []mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}})
for m := range ch {
    fmt.Println(m.Topic, m.Value.Temp)
    _ = m.Ack()
}
```

Implement `mqttv5.Codec[T]` for protobuf, Cap'n Proto, FlatBuffers, or
custom binary — the core has no codec dependency.

Payloads are decoded on the connection's read goroutine as they arrive
and delivered with the buffer, bound and drop policy of
`Client.Subscribe` / `SubscribeQueue`; a payload the codec rejects is
logged, acked and dropped. The channel or queue closes when the
subscription ends, whether or not anyone is still reading it.

---

## Durable publishing with `QueuePublisher`

`QueuePublisher` decouples the caller from broker availability:

- `Publish` returns as soon as the message is stored in the queue.
- A drain goroutine keeps up to `WithQueueWindow` messages in flight
  (default 32), in queue order, whenever the client is connected, and
  removes each from the queue once the broker has accepted it. Throughput
  approaches window ÷ round-trip time: at 25 ms, about 1,200 msg/s with
  the default window and 600 with a window of 16, against 38 msg/s one at
  a time (`BenchmarkQueuePublisherRTT`).

```go
import (
    qfile "github.com/ashtonian/mqttv5/queue/file"
    sfile "github.com/ashtonian/mqttv5/store/file"
)

st, _ := sfile.Open("/var/lib/myapp/session")
cli, _ := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithClientID("dev-1"), mqttv5.WithStore(st))
q, _ := qfile.Open("/var/lib/myapp/outbound")
pub, _ := mqttv5.NewQueuePublisher(cli, q,
    mqttv5.WithQueueMaxSize(1_000_000),
    mqttv5.WithQueueTTL(24*time.Hour),
    mqttv5.WithDeadLetter(func(e mqttv5.QueueEntry, err error) {
        log.Printf("dropped %s: %v", e.Publish.Topic, err)
    }),
)
defer pub.Close(ctx)

_ = pub.Publish(ctx, mqttv5.PublishOptions{Topic: "logs", Payload: data, QoS: 1})
```

**Failures.** A refusal that cannot pass — PUBACK/PUBREC 0x87 Not
authorized, 0x90 Topic Name invalid, 0x95 Packet too large, 0x99 Payload
format invalid, 0x9A Retain not supported, 0x9B QoS not supported, or the
same verdict from the broker's CONNACK limits — goes to the dead-letter
callback and the message is removed (`WithQueueClassifier` replaces the
rule). Anything else, such as 0x97 Quota exceeded, is retried after
`WithQueueRetryBackoff`, behind the messages that follow it, so one bad
message never blocks the queue. An expired message is dead-lettered with
`ErrMessageExpired` instead of sent; one that is sent carries the
lifetime it has left as its Message Expiry Interval.

**One exchange per message.** A slow broker never causes a second
exchange for the same message. Each message's ID is stored with its
exchange in the client's session (`WithStore`), and the message leaves
the queue before the session forgets the exchange. With `queue/file` and
`store/file`, a restarted process continues the exchanges its
predecessor had in flight — QoS 1 resent with DUP=1, QoS 2 from where it
stopped — rather than publishing those messages again. If removing a
finished message from the queue fails, the session keeps the exchange's
record and the removal is retried with `WithQueueRetryBackoff` (or by
the next process) without publishing the message again. A message is
published again as new only if the broker lost the session; enable
`WithQueueIdempotencyKey` to add the message ID as the `mqttv5-msg-id`
user property so consumers can drop that copy.

**Bounds.** `WithQueueMaxSize` is enforced atomically by the queue, so
concurrent producers cannot exceed it. At the bound `DropNewest`
(default) returns `ErrQueueFull`; `DropOldest` evicts the oldest
messages not yet being published and dead-letters them with
`ErrQueueFull`. Constructing a `QueuePublisher` never removes anything.

QoS 0 is rejected (`ErrQoS0NotQueueable`), as is a message that could
never be sent (invalid topic, Topic Alias): the error comes from
`Publish`, not a dead letter later. The publisher copies the message, so
the caller may reuse its buffers.

Use `mqttv5.NewMemoryPublisherQueue()` for in-process buffering without
crash safety. A custom `PublisherQueue` can be checked against the
contract with `github.com/ashtonian/mqttv5/queuetest`.

---

## Authentication

`WithCredentials(user, pass)` sends a fixed username and password.
For credentials that change, rotate them per connection attempt or
re-authenticate the live connection; `WithAuthenticator` adds MQTT v5
enhanced authentication (challenge/response such as SCRAM).

### Credentials per connection attempt

`WithConnectPacketBuilder(fn)` runs immediately before each CONNECT is
serialised. Use it to refresh an OAuth token, fetch a SigV4-signed
CONNECT credential, or rotate any other per-attempt secret. The context
is bounded by `ConnectTimeout`.

```go
mqttv5.WithConnectPacketBuilder(func(ctx context.Context, opts *mqttv5.ConnectOptions) error {
    tok, err := oauth.FetchToken(ctx)
    if err != nil {
        return err // fails this attempt; supervisor retries after backoff
    }
    opts.Username = "service-account"
    opts.Password = []byte(tok)
    return nil
}),
```

`WithOnConnectError` reports each failed attempt with its error. See
[`examples/oauth`](examples/oauth).

### Re-authentication without reconnecting

`Client.Reauthenticate(ctx)` drives MQTT v5 re-authentication ([§4.12]) on
the *live* connection — no reconnect, no QoS-state churn:

1. Sends an AUTH `0x19` carrying a fresh `Authenticator.Begin(ctx)` payload.
2. Services any broker challenges via `Continue`.
3. Returns when the broker concludes with `0x00` Success.

Use it on a long-lived connection whose bearer token (OAuth, JWT)
expires: start a timer from the token's lifetime and call
`Reauthenticate` ahead of expiry.

```go
// e.g. 30s before the JWT `exp`:
if err := cli.Reauthenticate(ctx); err != nil {
    // ErrReauthRejected → broker refused the new credential;
    // the supervisor is already reconnecting with a fresh CONNECT.
}
```

Behaviour:

- `ctx` bounds the whole operation, including the token fetch in `Begin`.
- Calls are single-flighted per connection.
- A broker rejection returns `ErrReauthRejected` and tears the
  connection down, so the supervisor reconnects through the normal
  CONNECT path.
- `WithOnReauthenticated` observes every successful refresh in one
  place.

### Mutual authentication

For mechanisms with mutual authentication (e.g. SCRAM), an
`Authenticator` may also implement the optional `ServerFinalVerifier`
interface (`VerifyServerFinal([]byte) error`).

The client hands it the server's concluding `AuthenticationData` — the
CONNACK on connect, the AUTH `0x00` on re-auth — so it can verify the
server proved knowledge of the credential.

A verification failure aborts the connect, or fails `Reauthenticate`
and tears the connection down.

---

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

Without `TLSConfig`, `wss://` verifies the broker against the system
roots with the URL's host as server name, the same default as
`mqtts://`. `ws://` and `wss://` URLs need `WithDialFunc`; `New` rejects
them otherwise.

---

## Multiple brokers

Three patterns, each its own API:

| Goal | API | Connections |
|---|---|---|
| **Failover** — one logical client across interchangeable brokers (same data) | `WithBrokers(urls...)` | 1 at a time, supervisor rotates on drop |
| **Parallel sessions to N independent brokers** | `NewClientGroup(members, opts...)` | N (one per broker), all live |
| **Publish across connections** — more than one connection's writer | `WithPublisherPool(N)` | N publish-only to the *same* broker |

These compose:

- `WithBrokers` inside a `GroupMember.Opts` gives HA-per-region fan-out.
- `WithPublisherPool` alongside `WithBrokers` spreads publishing across connections to an HA pair.

Publisher pool members are configured from the parent: brokers, dialing,
TLS, credentials, authenticator, CONNECT properties, timeouts, buffer
sizes, write path, backoff, `WithRetryInitialConnect`, and the publish
behaviour (`WithQoSDowngrade`, `WithSessionLossPolicy`,
`WithOutboundTopicAliases`, `WithLenientDecoding`,
`WithFollowServerRedirects`). They do not take the Will, the `Store`,
the lifecycle callbacks, or a session: every member connects with
CleanStart=1 and Session Expiry 0, so a QoS 1/2 publish interrupted by
a member's reconnect is handled by the session-loss policy (republished
by default, possibly duplicated). Use the main connection for publishes
that must be resumed exactly.

### Redirecting at runtime

A broker can send the client elsewhere ([§4.11]): a CONNACK or DISCONNECT
with reason `0x9D` Server moved or `0x9C` Use another server and a
Server Reference. `WithOnServerRedirect` reports each one;
`WithFollowServerRedirects()` acts on it — a move replaces the broker
list, a temporary redirect applies to the next connection attempt only.
A reference that is a bare `host` or `host:port` keeps the current URL's
scheme and path.

```go
cli, _ := mqttv5.New(
    mqttv5.WithBroker("mqtts://broker-a.example.com:8883"),
    mqttv5.WithFollowServerRedirects(),
    mqttv5.WithOnServerRedirect(func(r mqttv5.ServerRedirect) {
        log.Printf("redirected to %s (permanent=%v)", r.Reference, r.Permanent())
    }),
)
```

`Client.SetBrokers(urls...)` swaps the failover list at runtime by hand.

### `ClientGroup` policies

`ClientGroup` is for **N parallel sessions to N brokers**, each treated
as itself:

- bridges between independent brokers,
- multi-tenant SaaS with per-broker credentials,
- a clustered broker fleet you want N parallel sessions into.

If your brokers are interchangeable for the same data, use
`WithBrokers` on a single Client — `ClientGroup` does not failover
between members.

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
per-member auth / TLS / ClientID / callbacks win. Member names default
to `member-N` (1-based) when unset.

`Publish` returns every member's outcome (`[]GroupResult`) and an
error decided by the policy below:

| Publish policy | Behaviour | Use case |
|---|---|---|
| `GroupPublishBroadcast` (default) | Publishes to every member at once. Fails with a `*GroupError` when fewer than `WithGroupSuccess` members succeed. | Bridge / mirror — members carry different data |
| `GroupPublishRoundRobin` | One member per call, the next each time. | Distribute publishes across a clustered broker fleet |
| `GroupPublishHashByTopic` | FNV-1a(topic) → member. Per-topic ordering. | Fleet throughput with per-topic affinity |

RoundRobin and HashByTopic send each message once. They move to the
next member only when the chosen one could not send it at all (not
connected); a broker refusal or the ctx ending is returned as is, so a
message never goes out on two members.

| Success policy (`WithGroupSuccess`) | Broadcast Publish and Subscribe succeed when |
|---|---|
| `GroupSuccessAll` (default) | every member succeeded |
| `GroupSuccessAny` | at least one member succeeded |
| `GroupSuccessQuorum(n)` | at least `n` members succeeded |

```go
results, err := g.Publish(ctx, mqttv5.PublishOptions{Topic: "events/x", QoS: 1, Payload: p})
for _, r := range results {
    if r.Err != nil {
        log.Printf("%s: %v", r.Member, r.Err) // errors.Is(err, mqttv5.ErrNotAuthorized) works on the GroupError too
    }
}
```

Subscribe subscribes every member and merges their messages into one
channel or queue; `Ack` goes back to the member that delivered the
message. Delivery is direct from each member's read loop — no
forwarding goroutines — with the buffer, bound and drop policy of
`Client.Subscribe` / `SubscribeQueue` applied to the merged output.
When fewer members than the success policy requires subscribe, or ctx
ends, the members that did are unsubscribed again and the error is
returned. Otherwise the token map, keyed by member name, holds the
members that subscribed — pass it to `UnsubscribeAll`, or one entry to
`Unsubscribe(name, token)`. The merged output closes once every member's
subscription has ended (unsubscribed, refused, or `Disconnect`).

```go
ch, tokens, err := g.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}})
// tokens["emea"], tokens["apac"]
defer g.UnsubscribeAll(ctx, tokens)
```

Connect / Disconnect / Subscribe run in parallel across members by
default. Pass `WithGroupSequentialLifecycle` if you need deterministic
ordering. A group is used once: after `Disconnect` its methods return
`ErrClosed`.

Use `g.Members()` or `g.Member(name)` for direct per-member access
(per-member `Stats()`, etc.).

---

## Disconnecting

`Disconnect(ctx)` ends the client gracefully:

1. it sends DISCONNECT (Normal disconnection) after any acknowledgements
   already made;
2. it waits for the connection's goroutines (the read loop runs
   `SubscribeCallback` handlers) and closes every subscription's channel
   or queue;
3. it waits, bounded by ctx, for session store writes.

Unfinished QoS 1/2 exchanges stay in the session for the next `Connect`.
`Disconnect` is idempotent and does not fire `OnConnectionDown`: the
call itself is the "going down" signal. A lifecycle callback may call
it; a `SubscribeCallback` handler must not (see
[Lifecycle callbacks](#lifecycle-callbacks)).

`DisconnectWith(ctx, opts)` sets the reason code and properties:

```go
expiry := uint32(0)
_ = cli.DisconnectWith(ctx, mqttv5.DisconnectOptions{
    ReasonCode:            mqttv5.ReasonAdministrativeAction,
    ReasonString:          "planned shutdown",
    SessionExpiryInterval: &expiry, // override to drop the session immediately
})
```

See [`examples/disconnect`](examples/disconnect).

---

## Options

### Client

#### Broker and transport

| Option | Default | Effect |
|---|---|---|
| `WithBroker(url)` / `WithBrokers(urls...)` | (required) | Broker URL(s). `mqtt`/`tcp`/`mqtts`/`tls`/`ssl` schemes; default ports filled in. `ws`/`wss` via `WithDialFunc(ws.DialFunc(opts))`. |
| `WithDialFunc(fn)` | — | Replaces the built-in TCP/TLS dial. Takes precedence over `WithDialer`/`WithTLSConfig`. Nil rejected. |
| `WithTLSConfig(*tls.Config)` | — | TLS for `mqtts://`. |
| `WithDialer(*net.Dialer)` | default | Override transport `net.Dialer`. |

#### Identity and authentication

| Option | Default | Effect |
|---|---|---|
| `WithClientID(s)` | broker-assigned | MQTT ClientID. Empty = ask the broker to assign one via `AssignedClientIdentifier`; `cli.ClientID()` then returns the assigned value after CONNACK. |
| `WithCredentials(user, pass)` | — | CONNECT username + password. Static. For per-attempt rotation use `WithConnectPacketBuilder`. |
| `WithConnectPacketBuilder(fn)` | — | `func(ctx, *ConnectOptions) error`. Change the CONNECT's username, password and user properties before each attempt; canonical OAuth-token-rotation hook. |
| `WithAuthenticator(a)` | — | MQTT v5 enhanced auth (CONNECT + re-auth [§4.12]). `Begin(ctx)` resolves the credential; client-initiated refresh via `Client.Reauthenticate`. |

#### Session and CONNECT properties

| Option | Default | Effect |
|---|---|---|
| `WithCleanStart(b)` | true, or false when there is session state to resume | `CleanStart` on the initial CONNECT. Unset, the client resumes (CleanStart=0) when it holds unfinished QoS 1/2 flows from an earlier `Connect` or from `WithStore`; `true` always starts fresh and discards that state. |
| `WithCleanStartOnReconnect(b)` | false | `CleanStart` on every reconnect. False preserves QoS 1/2 session for resume. |
| `WithSessionExpiry(seconds)` | 300 (5 min) | Session Expiry Interval ([§3.1.2.11.2]). Pass 0 to end the session with the connection. A broker override in CONNACK is honoured. |
| `WithSessionLossPolicy(p)` | `SessionLossRepublish` | What happens to unacknowledged QoS 1/2 publishes when a connection starts without the session (Session Present = 0): `SessionLossRepublish` sends them again as new messages in their original order; `SessionLossFail` completes them with `ErrSessionLost`. |
| `WithStore(s)` | none (in memory) | `session.Store` that persists session state across restarts (use `store/file`). Without it, state survives reconnects only. A failed write of a new message's record fails that `Publish`; any other failed write stops the client (see [Store failures](#store-failures)). |
| `WithReceiveMaximum(n)` | 256 (`DefaultReceiveMaximum`) | QoS 1/2 messages the broker may send unacknowledged, and so how many a subscription whose consumer falls behind holds beyond its buffer. A broker that exceeds it is disconnected with reason 0x93. |
| `WithMaximumPacketSize(n)` | 0 (no advertised limit) | CONNECT property [§3.1.2.11.4] — caps the largest packet the broker may send. See note below. |
| `WithInboundTopicAliasMaximum(n)` | 0 (no inbound aliases) | CONNECT property [§3.1.2.11.5] — opt into wire compression on inbound PUBLISHes. An alias the broker sends outside 1..n closes the connection (`0x94`). |
| `WithOutboundTopicAliases()` | off | Replace repeated QoS 0 topics with topic aliases within the broker's Topic Alias Maximum. |
| `WithLenientDecoding()` | off | Tolerate a non-minimal Remaining Length and reserved PINGRESP flags from the broker (logged) instead of disconnecting. |
| `WithRequestResponseInformation(b)` | false | CONNECT property [§3.1.2.11.6] — broker returns `ResponseInformation` in CONNACK. |
| `WithRequestProblemInformation(b)` | true | CONNECT property [§3.1.2.11.7] — broker returns `ReasonString` / `UserProperties` on errors. Pass `false` to opt out. |
| `WithConnectUserProperty(k, v)` / `WithConnectUserProperties(p)` | — | CONNECT user properties; append-style or bulk replace. |
| `WithWill(*WillOptions)` | — | Will message + properties; validated by `New`. |

> **Session expiry default.** 300 s holds the broker session long enough
> for QoS 1/2 resume across a typical reconnect blip.

> **Maximum packet size.** With the default (no advertised limit), a
> buggy or hostile broker can send arbitrarily large PUBLISHes. Set it
> explicitly when broker trust is limited.

#### Keepalive and timeouts

| Option | Default | Effect |
|---|---|---|
| `WithKeepAlive(seconds)` | 30 | Requested keep-alive. 0 rejected — use `WithoutKeepAlive`. A Server Keep Alive in CONNACK replaces it ([§3.2.2.3.14]); `ServerInfo().KeepAlive` reports the one in effect. |
| `WithoutKeepAlive()` | — | Request no keep-alive (no PINGREQ unless the broker sets a Server Keep Alive). Rarely correct in production. |
| `WithConnectTimeout(d)` | 10 s | Dial + CONNECT/CONNACK budget. |
| `WithPingTimeout(d)` | min(10 s, KeepAlive/2) | How long to wait for any packet after a PINGREQ before declaring the connection dead. Must be shorter than the keep-alive (`New` rejects it otherwise). |
| `WithDisconnectFlushTimeout(d)` | 500 ms | Flush budget for the DISCONNECT write on graceful shutdown. |
| `WithReadBufferSize(n)` | 16 KiB | Per-connection read window. Larger windows cut read syscalls for floods of small messages (`BenchmarkReceiveWindow`). |

> **Keep-alive.** A PINGREQ goes out once either direction has been
> quiet for the keep-alive, so the gap between packets the client sends
> never exceeds it. After a PINGREQ, any inbound packet counts as the
> answer (a PINGRESP can sit behind a long inbound backlog). With
> nothing inbound for `PingTimeout` the connection is dead: detection
> takes at most `KeepAlive + PingTimeout` (40 s with defaults), inside
> the broker's 1.5×KeepAlive cutoff, so the client drives the reconnect.

#### Reconnect

| Option | Default | Effect |
|---|---|---|
| `WithReconnectBackoff(b)` | `ExponentialBackoff(1s, 30s, 200ms)` | Delay before reconnect attempt n. `ConstantBackoff(d)` also shipped. The attempt count carries over connections that drop sooner than the delay before them. |
| `WithRetryInitialConnect()` | off | A failed first CONNECT is retried in the background like a reconnect and `Connect` returns nil; `Client.AwaitConnection(ctx)` waits for the connection. Without it `Connect` returns the failure. |
| `WithFollowServerRedirects()` | off | Connect where a `0x9C`/`0x9D` redirect points (see [Redirecting at runtime](#redirecting-at-runtime)). |

#### Write path and publishing

| Option | Default | Effect |
|---|---|---|
| `WithWriteQueueSize(n)` | 256 | Internal MPSC write buffer. |
| `WithWriteBatch(n)` | 0 (off) | Coalesce up to n queued packets per writev syscall. Saves syscalls when concurrent publishers queue packets behind the writer; with one publisher or large payloads there is little to coalesce. The benchmarks do not measure it: measure with your workload before enabling. |
| `WithWriteOverflowPolicy(p)` | `WriteBlock` | QoS 0 only. See note below. |
| `WithPublishMode(mode)` | `PublishFireAndForget` | `PublishWaitForFlush` makes QoS 0 wait until the packet is written; on an idle TCP or Unix connection the caller writes it itself, without copying the payload. ctx bounds the wait; a write it interrupts is finished by the writer goroutine. |
| `WithPublisherPool(N)` | 0 (off) | N dedicated publish-only conns. |
| `WithPublisherPoolRouting(p)` | `PoolRoutingRoundRobin` | `PoolRoutingHashByTopic` preserves per-topic ordering. |
| `WithPublisherPoolClientIDFn(fn)` | `"%s-pub-%d"` | Customise per-member ClientIDs. Required when the parent ClientID is empty (broker-assigned). |
| `WithQoSDowngrade()` | off | Send a publish whose QoS exceeds the broker's Maximum QoS at that maximum instead of failing it with `ErrQoSNotSupported`. Weakens the guarantee (QoS 2 becomes at-least-once). |

> **Broker limits.** Checked before a packet is written; see
> [Broker limits](#broker-limits).

> **Write overflow policy.**
>
> - `WriteBlock` waits for queue room or ctx.
> - `WriteDropNewest` returns `ErrWriteQueueFull` immediately when the
>   writer queue is full. Use it for telemetry, where head-of-line
>   latency on the producer is worse than occasional loss.
> - QoS 1/2 always block on the broker ack regardless.

#### Subscription defaults

| Option | Default | Effect |
|---|---|---|
| `WithMaxSubscribeQueueSize(n)` | `DefaultMaxSubscribeQueueSize` (65,536) | Default `SubscribeQueue` cap. `UnboundedQueue` removes it. |
| `WithDropPolicy(p)` | `DropNewest` | Default drop policy for full sub buffers. |

#### Lifecycle callbacks

These callbacks run one at a time, in event order, on a goroutine the
client owns:

- They may call `Disconnect`, `Connect` and `SetBrokers`.
- Before each reconnect attempt the client waits for the callbacks so
  far, so a `SetBrokers` call from one applies to that attempt, and a
  slow callback delays it.
- They run after their event, not inside the call that caused it:
  `OnConnectionUp` may run after `Connect` returns, and a callback for an
  event before `Disconnect` may run after `Disconnect` returns.

Message handlers are different: `SubscribeCallback` handlers and
`SubOnDrop` hooks run on the connection's read loop, which `Disconnect`
waits for. Calling `Disconnect` from one would wait for itself; start it
on another goroutine (`go cli.Disconnect(ctx)`). A teardown held up by a
handler logs `disconnect is waiting for a SubscribeCallback handler`
after five seconds.

| Option | Signature / when | Effect |
|---|---|---|
| `WithOnConnectionUp(fn)` | `func(ConnackInfo)` — after every successful CONNACK | What the broker granted: session present, limits, assigned ClientID, effective keep-alive, server reference, ... |
| `WithOnConnectionDown(fn)` | `func() bool` — on unexpected disconnect (not user Disconnect) | Return false to terminate the supervisor. |
| `WithOnConnectError(fn)` | per failed CONNECT attempt (dial err, CONNACK refusal, AUTH-loop err) | Observability only. |
| `WithOnReconnectAttempt(fn)` | immediately before each reconnect dial | Receives `(attempt, brokerURL)`. |
| `WithOnServerDisconnect(fn)` | `func(DisconnectInfo)` — on broker-initiated DISCONNECT | Reason code, reason string, server reference, user properties. |
| `WithOnServerRedirect(fn)` | `func(ServerRedirect)` — CONNACK or DISCONNECT with `0x9C`/`0x9D` and a Server Reference | Observability, or custom handling instead of `WithFollowServerRedirects`. |
| `WithOnResubscribeError(fn)` | `func(SubscriptionToken, error)` — the broker refused filters when the client re-subscribed after a session loss | The token's `Err()` is set when no filter is left. |
| `WithOnReauthenticated(fn)` | when a re-authentication started with `Reauthenticate` concludes with broker AUTH `0x00` | Observability only. |
| `WithOnStoreFailure(fn)` | `func(error)` — once, after a `WithStore` write failed and the client stopped because of it | The error is a `*StoreError` (`errors.Is(err, ErrStoreFailed)`). Call `Connect` to start again from what the store holds. |

#### Observability

| Option | Default | Effect |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | Structured logging. |
| `WithStats()` | — | Enable in-memory counters for `Client.Stats()` (off by default to keep the hot path branch-predictor friendly). |

### Per-subscribe

`SubscribeOption` applies to `Subscribe`, `SubscribeQueue`,
`SubscribeCallback`, plus the `Typed[T]` and `ClientGroup` variants.

| Option | Effect |
|---|---|
| `SubBuffer(n)` | Channel buffer size (Subscribe only): the QoS 0 messages held before one is dropped; QoS 1/2 have room for Receive Maximum more. Default `DefaultSubscribeBuffer` (64). |
| `SubMaxQueueSize(n)` | Queue cap (SubscribeQueue only); under `DropNewest`, QoS 1/2 have room for Receive Maximum more. 0 keeps the client default; `UnboundedQueue` removes the cap. |
| `SubDropPolicy(p)` | `DropNewest` / `DropOldest`. SubscribeQueue honours both, and `DropOldest` evicts whatever the QoS; chan-based `Subscribe` returns `ErrChanDropOldestUnsupported` when DropOldest is set explicitly. |
| `SubOnDrop(fn)` | Hook run on the read goroutine before a dropped message is acked; every field is readable. Must not block or call `Disconnect`. |
| `SubAutoAck()` | Opt-in: the dispatcher acks each delivery before handing it to the consumer, so a full buffer or queue drops QoS 1/2 messages too. See note below. Ignored by `SubscribeCallback`. |
| `SubZeroCopy()` | Deliver messages whose Topic / Payload / Properties alias the network frame (valid until every receiving subscription has acked; for callbacks, until return). Saves the copy: one allocation per message at QoS 0. Also accepted by `SubscribeCallback`. |

> **`SubAutoAck` trade-offs.**
>
> - `m.Ack()` is a no-op; the message is an owned copy, safe to retain.
> - Breaks at-least-once semantics: a consumer crash between delivery
>   and processing has nothing to replay.
> - Reach for it on QoS 0 / observational consumers.

### Per-`QueuePublisher`

| Option | Type | Default | Effect |
|---|---|---|---|
| `WithQueueWindow(n)` | `int` | 32 (`DefaultQueueWindow`) | Messages in flight at once; the broker's Receive Maximum still applies. Must be ≥ 1. |
| `WithQueueMaxSize(n)` | `int` | 0 (unbounded) | Bound on queued messages, enforced atomically by the queue. |
| `WithQueueDropPolicy(p)` | `DropPolicy` | `DropNewest` | At the bound: `DropNewest` returns `ErrQueueFull`; `DropOldest` evicts the oldest messages not in flight and dead-letters them. |
| `WithQueueTTL(d)` | `time.Duration` | 0 (none) | Longest a message may wait, from `Publish`; the remaining lifetime is sent as Message Expiry Interval. |
| `WithDeadLetter(fn)` | `func(QueueEntry, error)` | none | Messages removed without the broker accepting them, with the reason. Called from internal goroutines, possibly concurrently; must not block. |
| `WithQueueClassifier(fn)` | `func(error) bool` | `PermanentPublishError` | Which failures are dead-lettered rather than retried. |
| `WithQueueRetryBackoff(b)` | `Backoff` | `DefaultQueueRetryBackoff` (1 s doubling to 1 min, ±0.5 s) | Delay before retrying a message whose publish failed for a reason that may pass. |
| `WithQueueIdempotencyKey()` | — | off | Adds the `mqttv5-msg-id` user property with the message's ID. |

### Per-`ClientGroup`

| Option | Type | Default | Effect |
|---|---|---|---|
| `WithGroupSharedOpts(opts...)` | `...Option` | none | Client options applied to every member before its `GroupMember.Opts`. |
| `WithGroupPublishPolicy(p)` | `GroupPublishPolicy` | `GroupPublishBroadcast` | Broadcast, RoundRobin or HashByTopic (see [`ClientGroup` policies](#clientgroup-policies)). |
| `WithGroupSuccess(s)` | `GroupSuccess` | `GroupSuccessAll` | How many members must succeed for a broadcast Publish or a Subscribe; `GroupSuccessAny`, `GroupSuccessQuorum(n)`. |
| `WithGroupSequentialLifecycle()` | — | parallel | Connect, Disconnect and Subscribe members one after another. |

---

## Errors

Branch on sentinel errors with `errors.Is`. Typed errors
(`*ReasonCodeError`, `*SubscribeError`, …) carry the details; reach
them with `errors.As`.

### Connection lifecycle

| Error | Source | Meaning |
|---|---|---|
| `ErrNotConnected` | `Publish`, `Subscribe*` | No live connection; from `Publish` it also guarantees the message was not sent. Retry / wait for reconnect. |
| `ErrAlreadyConnected` | `Connect` | Connect called twice. |
| `ErrClosed` | any after `Disconnect` | Client torn down. |
| `ErrConnectRefused` | `Connect` | Broker non-success CONNACK reason; the error is a `*ReasonCodeError` carrying the code, reason string and server reference. |
| `*ProtocolError` | `Connect`, connection drops | The broker broke MQTT v5 (e.g. Session Present for CleanStart=1, a packet above the client's Maximum Packet Size, more in-flight messages than the client's Receive Maximum). The client sent DISCONNECT with `Reason` and reconnects; counted in `Stats().ProtocolErrors`. |
| `ErrInvalidSessionExpiry` | `DisconnectWith` | Non-zero Session Expiry when CONNECT sent 0 [MQTT-3.14.2-2]; nothing is sent. |
| `ErrUnexpectedPacket` | various | Broker sent an unexpected packet for the current state. Treat as protocol bug. |
| `ErrMissingBroker` | `New` | No URLs supplied. |
| `ErrInvalidBrokerURL` | `New`, `SetBrokers` | URL failed to parse or has unsupported scheme. `WithDialFunc` relaxes scheme validation. |

### Re-authentication

| Error | Source | Meaning |
|---|---|---|
| `ErrNoAuthenticator` | `Reauthenticate` | Called without `WithAuthenticator`. |
| `ErrReauthInProgress` | `Reauthenticate` | A re-auth is already in flight on this connection (calls are single-flighted). |
| `ErrReauthRejected` | `Reauthenticate` | Broker rejected re-auth via DISCONNECT; wrapped with the reason code. Connection torn down; supervisor reconnects. |

### Subscribe

| Error | Source | Meaning |
|---|---|---|
| `*UnsubscribeError` | `Unsubscribe` | The broker refused to remove some filters (UNSUBACK ≥ 0x80); the local subscription is closed anyway. |
| `ErrChanDropOldestUnsupported` | `Subscribe` (chan) | Explicit `SubDropPolicy(DropOldest)` on the channel-based Subscribe. Use `SubscribeQueue` for DropOldest. |
| `ErrNilHandler` | `SubscribeCallback` | Handler argument was nil. |
| `ErrSharedSubsUnsupported` | `Subscribe*` | Broker disabled `$share/...` in CONNACK. |
| `ErrWildcardSubsUnsupported` | `Subscribe*` | Broker disabled `+` / `#` in CONNACK. |
| `*SubscribeError` | `Subscribe*` | The broker refused some filters; `Results` has each filter's outcome. With some granted the subscription is active for those; with none its channel/queue is closed. `errors.Is` matches the refusal's sentinel (e.g. `ErrNotAuthorized`). |
| `*RejectedByDisconnectError` | `Subscribe*`, `Unsubscribe` | Instead of answering, the broker closed the connection with a DISCONNECT blaming the packet (0x81 Malformed Packet, 0x82 Protocol Error, …) each of the 3 times it was sent — e.g. mosquitto for a filter deeper than 200 levels. The client gives up on it and stays connected; the subscription is closed. |

### Publish

| Error | Source | Meaning |
|---|---|---|
| `ErrWriteQueueFull` | `Publish` (QoS 0) | Writer queue at capacity AND client configured with `WithWriteOverflowPolicy(WriteDropNewest)`. The publish never reached the wire. |
| `*ReasonCodeError` | `Publish` (QoS 1/2) | The broker refused the message (PUBACK/PUBREC reason ≥ 0x80). `errors.Is` matches `ErrNotAuthorized`, `ErrQuotaExceeded`, `ErrTopicNameInvalid`, `ErrPayloadFormatInvalid`, `ErrPacketTooLarge`; `errors.As` gives the code and reason string. |
| `ErrQoSNotSupported` | `Publish` | QoS above the broker's Maximum QoS (see `WithQoSDowngrade`). |
| `ErrInvalidTopic` | `Publish`, `Subscribe*`, `Unsubscribe` | Topic name with a wildcard, NUL or invalid UTF-8; malformed filter or shared subscription. |
| `ErrInvalidField` | `Publish`, `Subscribe*`, `Connect` | Another option MQTT v5 forbids (DUP on QoS 0, No Local on a shared subscription, Retain Handling 3, ...). |
| `ErrFieldTooLong` | any | A string or binary field over 65,535 bytes. |
| `ErrTopicAliasInvalid` | `Publish` | Topic Alias above the broker's maximum, or a QoS 1/2 publish with an alias but no topic. |
| `ErrRetainNotSupported` | `Publish` | Retained message on a broker with Retain Available = 0. |
| `ErrPacketTooLarge` | `Publish`, `Subscribe*`, `Unsubscribe` | Packet above the broker's Maximum Packet Size; nothing was sent. |
| `ErrSessionLost` | `Publish` (QoS 1/2) | Session lost with `WithSessionLossPolicy(SessionLossFail)`. |
| `ErrMessageExpired` | `Publish` (QoS 1/2) | Message Expiry ran out before a republish after session loss. |
| `ErrPacketIDsExhausted` | `Publish`, `Subscribe*` | All 65,535 packet identifiers in use until ctx ended. |
| `ErrNoHealthyPublishers` | publisher pool internal | No pool member could send the message — falls back to the main connection. A member's broker refusal or ctx error is returned instead, never retried elsewhere. |

### `QueuePublisher`

| Error | Source | Meaning |
|---|---|---|
| `ErrQueueClosed` | `QueuePublisher.Publish`, `PublisherQueue` methods | After `Close`. |
| `ErrQueueFull` | `QueuePublisher.Publish`; dead letters | At `WithQueueMaxSize` under `DropNewest`, or under `DropOldest` when every queued message is in flight. Dead letters carry it for evicted messages. |
| `ErrQoS0NotQueueable` | `QueuePublisher.Publish` | QoS 0 has no acknowledgement to queue for. |
| `ErrMessageExpired` | dead letters | The message's TTL or Message Expiry ran out before it was sent. |
| `*ReasonCodeError` | dead letters | The broker refused the message with a code `WithQueueClassifier` calls permanent. |

### `ClientGroup`

| Error | Source | Meaning |
|---|---|---|
| `*GroupError` | `ClientGroup.Publish`, `ClientGroup.Subscribe` | Fewer members succeeded than `WithGroupSuccess` requires; `Results` has each member's outcome and `errors.Is` sees their errors. |

### Transport

| Error | Source | Meaning |
|---|---|---|
| `transport.ErrUnknownScheme` | `transport.Dial` | The URL's scheme is not one the built-in dialer knows. `New` rejects such a broker URL up front with `ErrInvalidBrokerURL`. |
| `transport.ErrMissingHost` | `transport.Dial` | The URL has no host. |

---

## Reliability semantics

### Connection

**Connect.** Blocks until the CONNACK, or until ctx ends. Without
`WithRetryInitialConnect()` it returns the first failure. With it,
`Connect` returns nil, the supervisor retries with the reconnect
backoff, and `Client.AwaitConnection(ctx)` waits for the connection;
until then `Publish` and `Subscribe` return `ErrNotConnected` and a
`QueuePublisher` keeps queuing.

**Reconnect.** `ExponentialBackoff(1s, 30s, 200ms)` by default. With
`WithBrokers`, URLs rotate per attempt and a successful connect sticks.
The backoff starts over only after a connection outlives the delay that
preceded it, so a broker that accepts the CONNECT and drops the
connection at once is retried at a growing interval, not in a tight
loop.

**Keep-alive.** After a PINGREQ, any packet from the broker counts as
the answer. With none within `PingTimeout` the connection is treated as
dead and the supervisor redials.

**Server-initiated DISCONNECT.** `WithOnServerDisconnect(fn)` receives
a `DisconnectInfo` (reason, reason string, server reference, user
properties) before `OnConnectionDown`. Redirects (`0x9C`/`0x9D`) are
also reported to `WithOnServerRedirect` and followed with
`WithFollowServerRedirects`.

### Sessions

**Session resume (Session Present = 1).** Unacknowledged QoS 1/2
PUBLISHes are resent in their original send order with `DUP=1` ([§4.4],
[§4.6]); a QoS 2 message that already got its PUBREC resends PUBREL,
never the PUBLISH; PUBRELs follow PUBREC order. Acks the application
made while disconnected are sent on resume. The caller stays blocked on
`Publish` across the drop.

**Session loss (Session Present = 0, or CleanStart = 1).** Inbound
state is discarded, so the new session's messages are never mistaken
for duplicates, and an `Ack` on a message from the old session is a
no-op. Unacknowledged outbound QoS 1/2 messages follow
`WithSessionLossPolicy`: republished as new messages (the default; a
duplicate is possible, and QoS 2 cannot stay exactly-once across the
loss) or failed with `ErrSessionLost`. A broker that reports a present
session for a CleanStart=1 CONNECT is a protocol error (DISCONNECT
`0x82`). With `WithBrokers`, failover to another broker is a session
loss.

**Send quota.** At most the broker's Receive Maximum QoS 1/2 PUBLISHes
are in flight ([§4.9]); further publishes wait in order.

**Packet identifiers.** Owned per flow (publish, subscribe,
unsubscribe) and released only when that flow completes, so a stray ack
cannot free an identifier another flow is using.

**Duplicates from the broker.** A resent PUBLISH (DUP) for a message
the application still holds is neither delivered again nor acknowledged
on the application's behalf; a QoS 2 duplicate after PUBREC gets PUBREC
again; PUBREL for an unknown identifier gets PUBCOMP `0x92`. Acks for
unknown or mismatched packet identifiers are ignored and counted in
`Stats().AcksIgnored`.

### Delivery

**Acknowledgement order.** A QoS 1 PUBACK is held until `m.Ack()` and
flushed in [§4.6] arrival order. A QoS 2 PUBREC is held until `m.Ack()`;
PUBCOMP goes out automatically when PUBREL arrives.

**A consumer that falls behind.** Holding the acknowledgement until
`m.Ack()` is also the flow control: the broker sends at most Receive
Maximum QoS 1/2 messages the client has not acknowledged, so a channel
or queue subscription keeps room for that many beyond its buffer and
drops none of them. QoS 0 messages beyond the buffer are dropped and
acked (see [Subscribing](#subscribing)).

**One message, several subscriptions.** A PUBLISH matching several
subscriptions gives each its own `*Message` handle over the same bytes.
The broker's ack (PUBACK or PUBREC) goes out once every handle has been
acked. `m.Ack()` is idempotent per handle: a second call is a no-op and
never affects another message or handler.

**Topic and payload lifetime.** Owned by the message and valid for as
long as you hold it. With `SubZeroCopy()` they alias the network frame
and are valid only until every receiving subscription has acked
(callbacks: until return); use `m.CloneTopic()` / `m.ClonePayload()` to
keep them longer.

### Subscriptions

**Across a drop.** A SUBSCRIBE or UNSUBSCRIBE still waiting for its
answer when the connection drops is sent again on the next one, in the
order the calls were made; `Subscribe` and `Unsubscribe` keep waiting
until the answer arrives or ctx ends. When the broker has no session,
pending UNSUBSCRIBEs complete without being sent and every subscription
is subscribed again, oldest first. Filters the broker refuses then end
the subscription's share of them: `WithOnResubscribeError` is called,
and a subscription left with no filters closes with `token.Err()` set.
A SUBSCRIBE or UNSUBSCRIBE the broker answers by disconnecting
(DISCONNECT blaming a packet it received) is sent at most three times,
then fails with `*RejectedByDisconnectError`. A filter the broker never
granted is not unsubscribed.

**Shared filters.** A broker keeps one subscription per exact filter,
and a SUBSCRIBE for a filter it already holds replaces it ([§3.8.4]).
Subscriptions of one client with the same filter therefore share the
broker's: each receives every message for it; the options of the most
recent `Subscribe` (QoS, No Local, Retain As Published, Retain
Handling) apply to all of them until the last one unsubscribes; retained
messages sent for a later `Subscribe` reach the earlier ones as well.
`Unsubscribe` sends an UNSUBSCRIBE only for filters no other
subscription of the client uses.

**Subscription Identifiers.** When the broker supports them (CONNACK),
every SUBSCRIBE carries a Subscription Identifier the client allocates,
and a PUBLISH tagged with identifiers reaches only the subscriptions it
was sent for. With overlapping filters (`a/#` and `a/b`) a broker may
send one copy per matching subscription; each subscription then
receives exactly one. Without identifiers every matching subscription
receives every copy.

**Broker capabilities.** When the CONNACK disables shared subscriptions,
wildcards or Subscription Identifiers, `Subscribe*` fails before
anything is sent.

**Teardown.** `Unsubscribe` closes the subscription's channel or queue
before it returns, then reports the broker's answer; `Disconnect` closes
all of them. Each closes exactly once; a PUBLISH already being
dispatched to a closed subscription is acked instead of delivered.
`Disconnect` clears all routes, so `Connect` followed by `Subscribe`
starts clean.

### Protocol

**Validation.** Every packet from the broker is checked against MQTT v5
before it is acted on (fixed-header flags, minimal Remaining Length,
which properties each packet may carry and their values, UTF-8 strings,
topic names, packet identifiers, reason codes, topic aliases). A
violation closes the connection with DISCONNECT and the spec's reason
code (`0x81` Malformed Packet, `0x82` Protocol Error, or a specific one
such as `0x93`, `0x94`, `0x95`), then the supervisor reconnects; each
is logged at Error and counted in `Stats().ProtocolErrors`.
`WithLenientDecoding()` tolerates the two violations real brokers
produce harmlessly (a non-minimal Remaining Length, reserved PINGRESP
flag bits). Outbound packets are validated the same way: an invalid
topic, filter or option fails the call with `ErrInvalidTopic`,
`ErrInvalidField` or `ErrFieldTooLong` before anything is sent.

**Topic aliases.** Outbound aliases are opt-in
(`WithOutboundTopicAliases()`): QoS 0 publishes then replace a repeated
topic with an alias while the broker's Topic Alias Maximum lasts; the
alias is allocated, encoded and queued under one lock per connection,
so a use never overtakes its registration. QoS 1/2 always carry the full
topic, since they may be resent on a connection that knows no aliases.
Inbound aliases need `WithInboundTopicAliasMaximum(n)`; an alias of 0
or above `n` is a DISCONNECT `0x94`, an alias used before it was
registered a `0x82`.

**Re-authentication ([§4.12]).** Client-initiated with
`Reauthenticate(ctx)`: AUTH `0x19`, then `Begin`/`Continue`, until the
broker's `0x00` Success. The client always replies `0x18` Continue;
only the server concludes, and an inbound `0x00` is terminal. A
rejection returns `ErrReauthRejected` and the client reconnects with a
fresh CONNECT.

The exchange is validated: during CONNECT the broker may only send AUTH
`0x18` under the CONNECT's Authentication Method, and a CONNACK may not
name another method; after CONNACK a broker AUTH is only valid inside a
re-authentication the client started (a broker cannot initiate one),
again under the same method. Anything else is a protocol error
(DISCONNECT `0x82`). Authenticators that do I/O to answer a challenge
can implement `ContextAuthenticator` to be cancelled at the connect
timeout.

---

## Performance

Measured with the [benchmark suite](benchmarks/): mqttv5 against
eclipse/paho.golang (`autopaho` and its `packets` codec) and
eclipse/paho.mqtt.golang (`paho3`, MQTT 3.1.1), every client running the
same scenario code through the same pinned mosquitto broker. Each table
is generated by `benchmarks/cmd/benchtab` from a recording made with
`benchmarks/scripts/run.sh`, which keeps the commit, toolchain, host,
broker and load average it ran under; the recordings stay out of the
repository, and each table names the date and commit of its own.

This page shows allocations, which vary far less between hosts and runs
than timings do; they count every goroutine in the benchmark process,
and pools, batching and timers still move them a little. Timings —
call latency, throughput, CPU per message, open-loop latency
percentiles, reconnect time — with the conditions they were measured
under and how to read them, are in
[benchmarks/README.md](benchmarks/README.md).

### Codec

Allocations to decode one PUBLISH from a stream (`props=five`: a
content type and five user properties), then read its properties:

<!-- benchtab file=benchmarks/results/2026-10-10-codec/codec.txt filter=".name:DecodePublishRead" rows=props,size cols=lib unit=allocs/op -->

| props | size | eclipse | mqttv5 |
|---|---|---:|---:|
| none | 64B | 22 | 0 |
| none | 256B | 22 | 0 |
| none | 1KiB | 24 | 0 |
| none | 16KiB | 32 | 0 |
| five | 64B | 93 | 0 |
| five | 256B | 93 | 0 |
| five | 1KiB | 95 | 0 |
| five | 16KiB | 103 | 0 |

<sub>allocs/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 80beb93780a2.</sub>
<!-- /benchtab -->

Allocations to encode one PUBLISH and write it, with each of mqttv5's
encoders: `mqttv5-header` writes the caller's payload as it is (a QoS 0
publish its caller writes on a TCP connection), `mqttv5-pooled` copies
the packet into a pooled buffer (a QoS 0 publish queued for the writer
goroutine), and `mqttv5-owned` copies it into a buffer of its own that
the session keeps for retransmission (every QoS 1/2 publish). See the
[codec section](benchmarks/README.md#codec):

<!-- benchtab file=benchmarks/results/2026-10-10-codec/codec.txt filter=".name:EncodePublish" rows=props,size cols=lib unit=allocs/op -->

| props | size | eclipse | mqttv5-header | mqttv5-pooled | mqttv5-owned |
|---|---|---:|---:|---:|---:|
| none | 64B | 9 | 0 | 0 | 1 |
| none | 256B | 9 | 0 | 0 | 1 |
| none | 1KiB | 9 | 0 | 0 | 1 |
| none | 16KiB | 9 | 0 | 0 | 1 |
| five | 64B | 12 | 0 | 0 | 1 |
| five | 256B | 12 | 0 | 0 | 1 |
| five | 1KiB | 12 | 0 | 0 | 1 |
| five | 16KiB | 12 | 0 | 0 | 1 |

<sub>allocs/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 80beb93780a2.</sub>
<!-- /benchtab -->

### Publishing

Allocations per `Publish` call in the publishing client. QoS 0 returns
once the packet is written (mqttv5 with `PublishWaitForFlush`, which on
this TCP connection writes it on the caller's goroutine without copying
the payload); QoS 1 and 2 return when the broker has acknowledged. The
packet a QoS 1/2 publish keeps for retransmission is one exact-size
copy.

<!-- benchtab file=benchmarks/results/2026-10-10-e2e/e2e.txt filter=".name:E2E_Publish" rows=qos,size cols=lib unit=allocs/op -->

| qos | size | mqttv5 | autopaho | paho3 |
|---|---|---:|---:|---:|
| 0 | 64B | 0 | 15 | 23 |
| 0 | 1KiB | 0 | 15 | 23 |
| 0 | 1MiB | 0 | 15 | 23 |
| 1 | 64B | 5 | 53 | 42 |
| 1 | 1KiB | 5 | 53 | 42 |
| 1 | 1MiB | 7 | 55 ±2% | 43 |
| 2 | 64B | 6 | 92 | 62 |
| 2 | 1KiB | 6 | 92 | 62 |
| 2 | 1MiB | 8 | 93 | 63 |

<sub>allocs/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 80beb93780a2.</sub>
<!-- /benchtab -->

<!-- benchtab file=benchmarks/results/2026-10-10-e2e/e2e.txt filter=".name:E2E_Publish" rows=qos,size cols=lib unit=B/op -->

| qos | size | mqttv5 | autopaho | paho3 |
|---|---|---:|---:|---:|
| 0 | 64B | 0B | 600B | 1.305KiB |
| 0 | 1KiB | 0B | 600B | 2.305KiB |
| 0 | 1MiB | 0B | 600B | 1.009MiB |
| 1 | 64B | 424B | 4.309KiB | 1.641KiB |
| 1 | 1KiB | 1.447KiB | 5.309KiB | 2.642KiB |
| 1 | 1MiB | 1.009MiB | 1.012MiB | 1.010MiB |
| 2 | 64B | 440B | 7.191KiB | 2.145KiB |
| 2 | 1KiB | 1.462KiB | 8.191KiB | 3.146KiB |
| 2 | 1MiB | 1.009MiB | 1.015MiB | 1.010MiB |

<sub>B/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 80beb93780a2.</sub>
<!-- /benchtab -->

### Receiving

Allocations per delivered message, counted across the benchmark
process; the raw publisher in it allocates nothing per message, so they
are the subscriber's. `callback` is a handler the library calls; `chan`
is mqttv5's channel and, for autopaho, the channel an application
forwards into from its handler. A QoS 1 message is acknowledged when
the callback returns, or, in `chan` and `queue` mode, when a consumer
takes it.

<!-- benchtab file=benchmarks/results/2026-10-10-e2e/e2e.txt filter=".name:E2E_Receive /consumers:1" rows=mode,qos,size cols=lib unit=allocs/op -->

| mode | qos | size | mqttv5 | autopaho | paho3 |
|---|---|---|---:|---:|---:|
| callback | 0 | 64B | 2 | 27 | 16 |
| callback | 0 | 1KiB | 2 | 29 | 16 |
| callback | 1 | 64B | 5 | 40 | 33 |
| callback | 1 | 1KiB | 5 | 42 | 34 |
| chan | 1 | 64B | 5 | 40 | — |
| chan | 1 | 1KiB | 5 | 42 | — |
| queue | 1 | 64B | 6 | — | — |
| queue | 1 | 1KiB | 6 | — | — |

<sub>allocs/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 80beb93780a2.</sub>
<!-- /benchtab -->

Publish-to-delivery round trip, both clients in one process:

<!-- benchtab file=benchmarks/results/2026-10-10-e2e/e2e.txt filter=".name:E2E_RoundTrip" rows=qos,size cols=lib unit=allocs/op -->

| qos | size | mqttv5 | autopaho | paho3 |
|---|---|---:|---:|---:|
| 0 | 64B | 5 | 45 | 42 |
| 0 | 1KiB | 5 | 47 | 42 |
| 0 | 1MiB | 13 | 66 | 42 |
| 1 | 64B | 13 | 95 | 78 |
| 1 | 1KiB | 13 | 97 | 78 |
| 1 | 1MiB | 22 ±5% | 117 ±1% | 80 |
| 2 | 64B | 15 | 173 | 120 |
| 2 | 1KiB | 15 | 175 | 120 |
| 2 | 1MiB | 25 ±4% | 194 | 121 ±1% |

<sub>allocs/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 80beb93780a2.</sub>
<!-- /benchtab -->

mqttv5's receive path by delivery mode, measured in-process without a
broker (`BenchmarkReceive`, 64-byte payloads):

<!-- benchtab file=benchmarks/results/2026-10-10-receive/receive.txt filter=".name:Receive /size:64B" rows=qos,consumer cols=delivery unit=allocs/op -->

| qos | consumer | owned | zerocopy |
|---|---|---:|---:|
| 1 | callback | 4 | 3 |
| 1 | chan | 3 | 2 |
| 1 | queue | 4 | 3 |
| 0 | callback | 2 | 1 |

<sub>allocs/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 80beb93780a2.</sub>
<!-- /benchtab -->

`WithStats` is off in these runs; its counters are atomic adds behind a
nil check.

---

## Operations

### Health and metrics

`Client.Connected()` reports a live connection, and
`Client.ServerInfo()` what the broker granted. The
[lifecycle callbacks](#lifecycle-callbacks) report connection events as
they happen.

`Client.Stats()` returns a snapshot of in-memory counters. Opt in via
`WithStats()` — when off, the hot path skips every atomic increment and
`Stats()` returns the zero value.

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

The counters cover connects and disconnects, publishes and
subscriptions, inbound drops, pool fallbacks, ping timeouts, ignored
acks, session store errors and broker protocol violations; the
[`Stats`](https://pkg.go.dev/github.com/ashtonian/mqttv5#Stats) godoc
lists every field. The library has no metrics dependency: export the
fields to Prometheus, OpenTelemetry or expvar on a timer (see
[`examples/stats`](examples/stats)).

### Logs

Structured `log/slog` through `WithLogger` (default `slog.Default()`).
Normal message traffic logs nothing.

- **Info:** connects and disconnects.
- **Warn:** recoverable problems: reconnects, refused filters, stray
  acknowledgements, typed decode failures, a teardown waiting on a
  handler.
- **Error:** protocol violations and store failures.

### Troubleshooting

| Symptom | Check | Then |
|---|---|---|
| `Connect` returns `ErrConnectRefused` | `errors.As(err, &rce)` for the CONNACK reason code | 0x86/0x87: credentials or authorization; 0x9C/0x9D: the broker redirects, see `WithFollowServerRedirects` |
| Repeated reconnects | `Stats().Connects`, `ProtocolErrors`; Error logs | Protocol errors name the violation; a broker that sends harmless non-minimal lengths needs `WithLenientDecoding()` |
| `Publish` blocks | `Stats().PublishesInflight` at the broker's Receive Maximum | The broker is not acknowledging; the call returns when it does or ctx ends |
| `ErrWriteQueueFull` | `WithWriteOverflowPolicy(WriteDropNewest)` is set and the writer is behind | Raise `WithWriteQueueSize`, or use the default blocking policy |
| Messages missing on a subscription | `Stats().InboundDropped`; `SubOnDrop` | The consumer is slower than the stream. Only QoS 0 messages are dropped, unless `SubAutoAck` or `DropOldest` is set: raise `SubBuffer` / `SubMaxQueueSize`, subscribe at QoS 1, consume faster, or use a callback (backpressure) |
| Duplicate messages after a restart | Session Present in `OnConnectionUp`; the store | Without a durable `WithStore`, a restart loses QoS 1/2 state; `WithSessionLossPolicy` decides what happens to unacknowledged publishes |
| `ErrLocked` from `store/file` / `queue/file` | another process holds the file | One process per store directory |
| Client stopped, `WithOnStoreFailure` fired, `Publish` returns `ErrStoreFailed` | the `*StoreError`'s `Op` and `Err`; the Error log; free space and permissions on the store directory | Fix the store, then `Connect` again: it reloads the session from the store (see [Store failures](#store-failures)) |
| Memory grows with large messages | the broker's message sizes | Set `WithMaximumPacketSize` to the largest message you expect |
| `Disconnect` does not return; Warn log `disconnect is waiting for a SubscribeCallback handler` | goroutine dump (`SIGQUIT`) for the handler's stack | A handler or `SubOnDrop` hook is blocked, or called `Disconnect` itself: make it return, and run `Disconnect` from another goroutine |
| Reconnects start late | a lifecycle callback that takes long to return | The client waits for the callbacks before each attempt: keep them short, hand slow work to another goroutine |

### Store failures

Every packet that depends on a `WithStore` record waits until the
record is written: a PUBLISH for its record, a PUBREL for the
AwaitPubcomp phase, a PUBREC for the inbound AwaitPubrel record and a
PUBCOMP for that record's deletion. One flow's writes reach the store in
the order the client decided them, also across a reconnect.

A failed write of a new message's record fails that `Publish` with a
`*StoreError`. Any other failed write means the store no longer holds
what the session has done, so the client stops instead of carrying on
without the guarantee the store is there for, much as a crash would
stop it:

1. it drops the session state it holds in memory and sends nothing more
   that depends on it;
2. it sends DISCONNECT 0x80 (the broker publishes the Will) and ends as
   `Disconnect` would;
3. QoS 1/2 `Publish` calls still waiting return the `*StoreError`
   (`errors.Is(err, ErrStoreFailed)`); their messages stay in the store;
4. `WithOnStoreFailure` fires;
5. the next `Connect` reloads the session from the store and continues
   every exchange from the phase it holds.

The client treats every store error as final. A `session.Store` over a
backend with transient failures (a network database, say) retries them
itself within the ctx it is given. `store/file` returns an error only
when the file system does: check free space and the Error log, then
`Connect` again.

---

## Security

- **TLS.** `mqtts://`, `ssl://`, `tls://` and `wss://` verify the
  broker against the system roots by default; `WithTLSConfig` supplies
  CAs, client certificates or a server name. Never set
  `InsecureSkipVerify` outside tests.
- **Credentials.** `WithCredentials` and `WithConnectPacketBuilder`
  (per-attempt rotation) keep secrets in memory only; the client never
  logs them. Enhanced authentication (`WithAuthenticator`) supports
  SCRAM-style challenge/response and re-authentication
  (`Reauthenticate`).
- **Untrusted input.** Every packet from the broker is validated
  before it is acted on, and a violation closes the connection (see
  [Reliability semantics](#reliability-semantics)). Memory a broker can
  make the client hold is bounded: per packet by `WithMaximumPacketSize`
  (unset: the decoder holds at most eight times the bytes actually
  received until a packet is complete), per subscription by `SubBuffer`
  / `SubMaxQueueSize` for QoS 0 messages and by `WithReceiveMaximum`
  for QoS 1/2 ones.
- **Persistence.** `store/file` and `queue/file` write message payloads
  to disk unencrypted, with 0600 permissions by default under a 0700
  directory; encrypt the volume if payloads are sensitive.
- **Reporting.** Report vulnerabilities privately through GitHub
  security advisories on this repository.

---

## Architecture

One goroutine per connection drives
`read → decode → trie match → handlers (sync)`. On a TCP or Unix
connection, a publish that waits for its write (QoS 1/2, or QoS 0 with
`PublishWaitForFlush`) writes on its own goroutine when nothing is
queued: a QoS 0 header and its payload in one `writev`, without copying
the payload; a QoS 1/2 publish the packet the session keeps for
retransmission. If the caller's ctx ends mid-write, a deadline stops
the write and the writer goroutine sends the rest before anything else.
Otherwise — TLS and WebSocket connections, a busy writer, and
fire-and-forget QoS 0 — one writer goroutine per connection drains an
MPSC channel of encoded packets to the socket, coalescing queued
packets into one `writev` when `WithWriteBatch` is set; a write lock
keeps the two paths from interleaving, and a publish never overtakes a
packet its caller queued earlier. Before the read loop waits for more
input, it writes the acknowledgements and PUBRELs it has made ready
itself — on an idle TCP or Unix connection, on unix systems — as far as
the socket takes them without waiting; the writer goroutine sends the
rest, so a broker that stops reading never stops the client reading. A
supervisor goroutine owns the connection lifecycle: reconnect with
backoff, session resumption, and re-issuing subscriptions.

**Frames.** The decoder reads through a `WithReadBufferSize` window
(16 KiB). A body up to 64 KiB goes into a pooled frame from one of five
size classes (256 B – 64 KiB). A larger body is read into buffers that
grow as its bytes arrive — never more than eight times what the broker
has actually sent — and the final buffer is never pooled. A declared
length therefore costs nothing until its bytes arrive;
`WithMaximumPacketSize` rejects oversized packets before reading them.

**Messages.** A delivered message copies topic, payload and properties
out of a pooled frame into one exact-size buffer before any handler
runs, so the frame goes back to its pool at once. A message read into
an unpooled frame keeps that frame instead of copying it. `SubZeroCopy()`
subscriptions alias the pooled frame and release it when the last
handle is acked.

**Session engine** (`internal/inflight`). Owns packet identifiers, the
send quota (the broker's Receive Maximum), outbound and inbound QoS 1/2
state machines, ordered acknowledgements, replay on resumption, and
writes to the `session.Store`. Store writes run off the read goroutine
and are drained on `Disconnect`.

**Subscriptions.** One broker subscription per exact filter, shared by
the client's subscriptions with that filter; a topic trie maps topics
to them, and Subscription Identifiers select the subscriptions a
PUBLISH was sent for. SUBSCRIBE and UNSUBSCRIBE go through one ordered
control queue that survives reconnects.

---

## Code map

| Path | What |
|---|---|
| `client.go`, `lifecycle.go`, `conn.go`, `events.go` | `Client`, `Connect`/`Disconnect`, the supervisor, per-connection state, the lifecycle callback queue |
| `extensions.go`, `auth.go` | `Authenticator` and backoff policies; AUTH handling and `Reauthenticate` |
| `reader.go`, `writer.go`, `keepalive.go` | read loop and dispatch, writer goroutine, keep-alive |
| `publish.go`, `pool.go` | `Publish`, broker-limit checks, publisher pool |
| `subscribe.go`, `subscription.go`, `message.go`, `queue.go` | subscribe API, subscription registry and control queue, message ownership, `Queue[T]` |
| `publish_queue.go`, `publisher_queue.go` | `QueuePublisher` and the `PublisherQueue` interface with its in-memory implementation |
| `client_group.go`, `typed.go` | `ClientGroup`, `Typed[T]` |
| `options.go`, `types.go`, `errors.go`, `connack.go`, `redirect.go`, `stats.go`, `doc.go` | configuration, public value types, errors, CONNACK info, redirects, counters, package docs |
| `internal/inflight` | session engine: packet identifiers, QoS 1/2 state, quota, replay, store writes |
| `internal/trie` | topic trie |
| `internal/clock`, `internal/testbroker` | injectable clock; scripted broker for tests |
| `internal/filedb` | bbolt wrapper with group commit, shared by `store/file` and `queue/file` |
| `session`, `session/storetest` | `Store` interface, memory store, conformance tests for stores |
| `queuetest` | conformance tests for `PublisherQueue` implementations |
| `wire` | MQTT v5 codec (unstable API) |
| `transport`, `transport/ws` | TCP/TLS dialing; WebSocket |
| `store/file`, `queue/file` | durable session store and publish queue |
| `codec/json`, `codec/msgpack` | `Codec[T]` implementations |
| `conformance` | tests against real brokers (`-tags conformance`) |
| `benchmarks` | head-to-head benchmarks, raw results, `benchtab` |
| `examples`, `examples/readme` | runnable examples; the README's snippets as compiled code |

---

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

---

## Build and test

```bash
# Core — no broker required.
go test -race ./...

# Every module (each has its own go.mod).
for m in . codec/json codec/msgpack store/file queue/file internal/filedb transport/ws examples benchmarks; do
    (cd $m && go vet ./... && go test -race ./...)
done

# Conformance against mosquitto and EMQX (add `--profile hivemq` for
# HiveMQ CE; see conformance/README.md).
docker compose -f conformance/docker-compose.yml up -d
go -C conformance test -tags conformance -race ./...

# Benchmarks: see benchmarks/README.md.
docker compose -f benchmarks/docker-compose.yml up -d
benchmarks/scripts/run.sh e2e . -tags e2e -run '^$' -bench '^BenchmarkE2E_' -benchmem -timeout 3h
go -C benchmarks run ./cmd/benchtab README.md ../README.md
```

---

## Stability

mqttv5 is pre-1.0: any minor release may change the API.

- The client API is the `mqttv5` package: `Client`, `Config`, the
  options, `Stats`, and its value types (`PublishOptions`,
  `TopicFilter`, `Message`, `Properties`, `ConnackInfo`,
  `DisconnectInfo`, `WillOptions`, `ConnectOptions`,
  `DisconnectOptions`, `ServerRedirect`, results and errors).
- `ReasonCode`, `PacketType` and `UserProperty` are the protocol's own
  values, shared with the codec.
- `wire`, the codec underneath, is exported for tools, test brokers and
  storage formats. The client API never requires it, and it may change
  in any release.
- Submodules have their own `go.mod` and are tagged with the core at
  every release; use matching versions ([Modules](#modules)).

---

## Independence

Independent, clean-room implementation written from the
[MQTT v5.0 OASIS specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
Not a fork of any existing Go MQTT client.

The `benchmarks/` submodule imports `eclipse/paho.golang` for
head-to-head comparison only — it is not redistributed.

---

## License

Apache 2.0 — see [LICENSE](LICENSE) for the full text and
[NOTICE](NOTICE) for the attribution notice. Per-file headers carry
`SPDX-License-Identifier: Apache-2.0`.

<!-- MQTT v5.0 citations -->

[§3.1.2.11.2]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048 "3.1.2.11.2 Session Expiry Interval"
[§3.1.2.11.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050 "3.1.2.11.4 Maximum Packet Size"
[§3.1.2.11.5]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051 "3.1.2.11.5 Topic Alias Maximum"
[§3.1.2.11.6]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901052 "3.1.2.11.6 Request Response Information"
[§3.1.2.11.7]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053 "3.1.2.11.7 Request Problem Information"
[§3.2.2.3.14]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094 "3.2.2.3.14 Server Keep Alive"
[§3.8.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901170 "3.8.4 SUBSCRIBE Actions"
[§4.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901238 "4.4 Message delivery retry"
[§4.6]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901240 "4.6 Message ordering"
[§4.9]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901251 "4.9 Flow Control"
[§4.11]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901255 "4.11 Server redirection"
[§4.12]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256 "4.12 Enhanced authentication"
[MQTT-3.14.2-2]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211 "3.14.2.2.2 Session Expiry Interval"
