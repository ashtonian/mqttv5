# Operations

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

- [Health and metrics](#health-and-metrics)
- [Logs](#logs)
- [Troubleshooting](#troubleshooting)
- [Store failures](#store-failures)

## Health and metrics

`Client.Connected()` reports a live connection, and
`Client.ServerInfo()` which broker it reached (`Broker`) and what that
broker granted. The
[lifecycle callbacks](reference.md#lifecycle-callbacks) report connection events as
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

| Field | Kind | What it measures |
|---|---|---|
| `Connects` | counter | Successful CONNECT/CONNACK handshakes |
| `ConnectFailures` | counter | Connection attempts that failed, for any reason (dial, TLS, CONNACK refusal, authentication) |
| `Disconnects` | counter | Connections lost, for any reason but `Disconnect` |
| `ServerDisconnects` | counter | DISCONNECT packets the broker sent |
| `PingTimeouts` | counter | Connections declared dead after a PINGREQ went unanswered |
| `PublishesSent` | counter | `Publish` calls that returned nil — QoS 0 once queued (or written, with `PublishWaitForFlush`), QoS 1/2 once acknowledged — and `QueuePublisher` messages the broker accepted |
| `PublishesAcked` | counter | PUBACK and PUBCOMP packets received, refusals included, and those for publishes whose caller stopped waiting |
| `PublishesReplayed` | counter | QoS 1/2 publishes resent after a reconnect |
| `PublishesInflight` | gauge | QoS 1/2 publishes waiting for their acknowledgement |
| `SendQuota` | gauge | QoS 1/2 publishes the send quota still allows in flight: the broker's Receive Maximum, or `WithMaxInflight` when lower |
| `InboundPublishes` | counter | PUBLISH packets received |
| `InboundDropped` | counter | Received messages dropped because a subscription's buffer or queue was full |
| `InboundUnrouted` | counter | Received messages no subscription matched: acked and dropped |
| `DecodeFailures` | counter | Payloads a typed subscription's `Encoding` rejected: acked and not delivered |
| `SubscribesSent`, `UnsubscribesSent` | counter | SUBSCRIBE and UNSUBSCRIBE packets sent |
| `SubscriptionsActive` | gauge | Subscriptions registered on the client |
| `PoolFallbacks` | counter | Publishes no pool member could send, handed to the main connection, whether or not it could send them |
| `PoolConnected` | gauge | Publish pool members with a live connection |
| `AcksIgnored` | counter | Acknowledgements for an unknown or mismatched packet identifier |
| `StoreErrors` | counter | Failed `session.Store` operations |
| `ProtocolErrors` | counter | Broker protocol violations that closed the connection |

Counters start at zero in `New` and are never reset: they span every
`Connect` and `Disconnect` of the client. With a
[publish pool](publisher-pool.md#publish-pool-withpublisherpool), every field covers the
members' connections as well as the main one.

The library has no metrics dependency. Read `Stats()` once per
collection — from a Prometheus `Collector`, an OpenTelemetry observable
instrument's callback, or an `expvar.Func` — and export counters as
counters and gauges as gauges; reading it once per field takes the
gauges' locks once per field. [`examples/stats`](../examples/stats)
prints a snapshot every second.

Worth alerting on:

- `ConnectFailures` or `Disconnects` rising: the connection is
  flapping ([Troubleshooting](#troubleshooting)).
- `InboundDropped` rising: a consumer is behind the stream.
- `InboundUnrouted` rising outside unsubscribes: messages reach no
  subscription, typically a resumed session's queued messages arriving
  before the application subscribed.
- `DecodeFailures` rising: a publisher sends payloads the subscription's
  `Encoding` cannot decode.
- `PoolConnected` below the pool size, or `PoolFallbacks` rising: pool
  members cannot connect.
- `StoreErrors` or `ProtocolErrors` above zero.
- `PublishesInflight` at the send quota (the broker's Receive Maximum,
  or `WithMaxInflight`; `SendQuota` 0) for long: the broker is not
  acknowledging.

## Logs

Structured `log/slog` through `WithLogger` (default `slog.Default()`).
Normal message traffic logs nothing.

- **Info:** connects and disconnects.
- **Warn:** recoverable problems: reconnects, refused filters, stray
  acknowledgements, typed decode failures, a teardown waiting on a
  handler.
- **Error:** protocol violations, store failures and a reconnect limit
  stopping the client.

## Troubleshooting

| Symptom | Check | Then |
|---|---|---|
| `Connect` returns `ErrConnectRefused` | `errors.As(err, &rce)` for the CONNACK reason code | 0x86/0x87: credentials or authorization; 0x9C/0x9D: the broker redirects, see `WithFollowServerRedirects` |
| Repeated reconnects | `Stats().Connects`, `ProtocolErrors`; Error logs | Protocol errors name the violation; a broker that sends harmless non-minimal lengths needs `WithLenientDecoding()` |
| `Publish` blocks | `Stats().PublishesInflight` at the send quota (`SendQuota` 0): the broker's Receive Maximum, or `WithMaxInflight` | The broker is not acknowledging, or acknowledges slower than the client publishes; the call returns when it does or ctx ends. A low `WithMaxInflight` throttles on purpose |
| QoS 1/2 `Publish` slow while subscriptions are busy | how long `SubscribeCallback` handlers and `SubOnDrop` hooks take | They run on the read loop that also reads this client's publish acknowledgements: move slow work to a channel or queue subscription, or publish through a [publish pool](publisher-pool.md#publish-pool-withpublisherpool) |
| `ErrWriteQueueFull` | `WithWriteOverflowPolicy(WriteDropNewest)` is set and the writer is behind | Raise `WithWriteQueueSize`, or use the default blocking policy |
| Typed messages missing; Warn log `payload decode failed` | `Stats().DecodeFailures`; the topic in the log; `SubOnDecodeError` | The publisher sends another format or shape than the subscription's `Encoding` and type expect: fix the publisher, or use `SubEncoding` / another type for that topic |
| A durable consumer misses the messages queued while it was down | Warn log `a message matched no subscription`; `Stats().InboundUnrouted` | It subscribes after `Connect`, and the broker sends the queued messages right after the CONNACK: subscribe before `Connect` ([Durable consumers](subscribing.md#durable-consumers)) |
| Messages missing on a subscription | `Stats().InboundDropped`; `SubOnDrop` | The consumer is slower than the stream. Only QoS 0 messages are dropped, unless `SubAutoAck` or `DropOldest` is set: raise `SubBuffer` / `SubMaxQueueSize`, subscribe at QoS 1, consume faster, or use a callback (backpressure) |
| Duplicate messages after a restart | Session Present in `OnConnectionUp`; the store | Without a durable `WithStore`, a restart loses QoS 1/2 state; `WithSessionLossPolicy` decides what happens to unacknowledged publishes |
| `ErrLocked` from `store/file` / `queue/file` | another process holds the file | One process per store directory |
| Client stopped, `WithOnStoreFailure` fired, `Publish` returns `ErrStoreFailed` | the `*StoreError`'s `Op` and `Err`; the Error log; free space and permissions on the store directory | Fix the store, then `Connect` again: it reloads the session from the store (see [Store failures](#store-failures)) |
| Memory grows with large messages | the broker's message sizes | Set `WithMaximumPacketSize` to the largest message you expect |
| A publish pool is no faster than one connection | `Stats().PoolConnected` against the pool size; `PoolFallbacks` | Members not connected: check the broker's connection limit and that its ACLs admit `<ClientID>-pub-<n>`; members log with `component=publisher-pool`. All connected: the round trip is not what limits publishing ([Publish pool](publisher-pool.md#publish-pool-withpublisherpool)) |
| Own messages arrive on a No Local subscription | `WithPublisherPool` is set | Pooled publishes come from the members' ClientIDs; skip your own messages in the handler |
| `Disconnect` does not return; Warn log `disconnect is waiting for a SubscribeCallback handler` | goroutine dump (`SIGQUIT`) for the handler's stack | A handler or `SubOnDrop` hook is blocked, or called `Disconnect` itself: make it return, and run `Disconnect` from another goroutine |
| Reconnects start late | a lifecycle callback that takes long to return | The client waits for the callbacks before each attempt: keep them short, hand slow work to another goroutine |
| Client stopped, `WithOnReconnectLimit` fired, `AwaitConnection` returns `ErrReconnectLimit`, `Publish` returns `ErrNotConnected` | the `*ReconnectLimitError`'s `Err` (each broker's failure in the last attempt) and `Attempts` / `Elapsed`; the Error log `reconnect limit reached` | Dial errors: the broker or the network is down. CONNACK refusals: the reason code (0x86/0x87 credentials, 0x88/0x89 broker overloaded). Fix the cause, then `Connect` again; raise `WithMaxReconnectAttempts` / `WithMaxReconnectTime` if outages this long are expected |

## Store failures

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
