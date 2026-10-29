# Configuration and errors

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

- [Options](#options)
- [Errors](#errors)
- [Stability](#stability)

## Options

### Client

#### Broker and transport

| Option | Default | Effect |
|---|---|---|
| `WithBroker(url)` / `WithBrokers(urls...)` | (required) | Broker URL(s). `mqtt`/`tcp`/`mqtts`/`tls`/`ssl` schemes; default ports filled in. `ws`/`wss` via `WithDialFunc(ws.DialFunc(opts))`. Several URLs fail over in the `WithFailoverOrder` order ([Failover](multi-broker.md#failover-withbrokers)). |
| `WithFailoverOrder(o)` | `FailoverRotate` | How several brokers are tried. `FailoverRotate`: each client starts at a random entry and each reconnect attempt moves on one. `FailoverPriority`: the list is a preference order; `Connect` and each reconnect start at the first entry, and one reconnect attempt is a pass over the list with no backoff between entries ([Primary and standby](multi-broker.md#primary-and-standby-failoverpriority)). |
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
| `WithStore(s)` | none (in memory) | `session.Store` that persists session state across restarts (use `store/file`). Without it, state survives reconnects only. A failed write of a new message's record fails that `Publish`; any other failed write stops the client (see [Store failures](operations.md#store-failures)). |
| `WithReceiveMaximum(n)` | 256 (`DefaultReceiveMaximum`) | QoS 1/2 messages the broker may send unacknowledged, and so how many a subscription whose consumer falls behind holds beyond its buffer. A broker that exceeds it is disconnected with reason 0x93. Limits the broker, not the client's own publishes (`WithMaxInflight`). |
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
> Time the read loop spends in application code — a `SubscribeCallback`
> handler, a `SubOnDrop` or `SubOnDecodeError` hook, an `Encoding` — is
> not taken for the broker's silence, since the answer may be waiting
> unread: PINGREQs keep going out and the verdict waits. A broker that
> really is silent is then detected within two `PingTimeout`s of the
> loop being free, or, if no PINGREQ was outstanding, after the usual
> `KeepAlive + PingTimeout`.

#### Reconnect

| Option | Default | Effect |
|---|---|---|
| `WithReconnectBackoff(b)` | `ExponentialBackoff{Initial: 1s, Max: 30s, Jitter: 0.5}` | Delay before reconnect attempt n; see [Backoff policies](#backoff-policies). The attempt count carries over connections that drop sooner than the delay before them. `New` rejects an invalid policy. |
| `WithMaxReconnectAttempts(n)` | 0 (no limit) | Stop the client after n failed reconnect attempts in one outage (from the lost connection, or with `WithRetryInitialConnect` from `Connect`'s failed attempt, to the next connection). Under `FailoverPriority` an attempt is a pass over the list. See [Reconnect limits](#reconnect-limits). Negative rejected. |
| `WithMaxReconnectTime(d)` | 0 (no limit) | Stop the client once the next reconnect attempt would start more than d after the connection was lost. It stops as soon as the backoff shows that, without waiting out the delay; an attempt under way is not cut short. Negative rejected. |
| `WithPublishWaitForConnection()` | off | `Publish` with no connection up waits, bounded by its ctx, for the next one — before the first connection, and between a drop and the reconnect — instead of returning `ErrNotConnected`. After `Disconnect` it still returns `ErrNotConnected` at once. |
| `WithRetryInitialConnect()` | off | A failed first CONNECT is retried in the background like a reconnect and `Connect` returns nil; `Client.AwaitConnection(ctx)` waits for the connection. Without it `Connect` returns the failure: with `WithBrokers`, once every broker has failed. |
| `WithFollowServerRedirects()` | off | Connect where a `0x9C`/`0x9D` redirect points (see [Changing brokers at runtime](multi-broker.md#changing-brokers-at-runtime)). |

##### Backoff policies

`Backoff` is an interface with one method, `Delay(attempt int) time.Duration`,
where attempt counts from 0. The same policies serve
`WithReconnectBackoff` and `WithQueueRetryBackoff`.

| Policy | Fields | Validated by `New` / `NewQueuePublisher` |
|---|---|---|
| `ExponentialBackoff{Initial, Max, Multiplier, Jitter}` | `Initial` is the delay before attempt 0; each attempt multiplies it by `Multiplier` (zero means 2), up to `Max`. `Jitter` in [0, 1] is the random fraction of each delay: a delay d becomes a uniform draw from [(1−Jitter)·d, d]. 0 retries at exactly d, 0.5 keeps at least half, 1 ("full jitter") spreads a fleet widest. Jitter never lengthens a delay, so `Max` is a ceiling. | `Initial` > 0, `Max` ≥ `Initial`, `Multiplier` 0 or ≥ 1, `Jitter` within [0, 1] |
| `ConstantBackoff(d)` | The same delay before every attempt. Clients that fail together retry in lockstep. | d ≥ 0 |
| `BackoffFunc(fn)` | Any function `func(attempt int) time.Duration`; must be safe for concurrent use. | fn not nil |

A fleet that loses its broker at once reconnects together unless the
delays are spread: keep `Jitter` above 0 for reconnects.

##### Reconnect limits

Without limits the client reconnects for as long as it runs. With
`WithMaxReconnectAttempts` or `WithMaxReconnectTime`, an outage that
runs out of either stops the client as `Disconnect` would:

- a QoS 1/2 `Publish` still waiting, and `AwaitConnection`, return a
  `*ReconnectLimitError` (`errors.Is(err, ErrReconnectLimit)` and
  `errors.Is(err, ErrClosed)`), whose `Attempts`, `Elapsed` and `Err`
  (the last attempt's failure, one error per broker) say what happened;
  `Err` is not in the error's chain, so a classifier does not take the
  client stopping for one CONNACK refusal;
- `Publish`, with or without `WithPublishWaitForConnection`, returns
  `ErrNotConnected`;
- `WithOnReconnectLimit` fires once, with the error;
- `Connect` starts the client again, with a fresh budget.

A reconnect that succeeds ends the outage: the next one gets the whole
budget. `OnConnectionDown` returning false still stops the client at
once, without an error. Publisher pool members keep reconnecting while
the main connection stands and stop with it.

#### Write path and publishing

| Option | Default | Effect |
|---|---|---|
| `WithWriteQueueSize(n)` | 256 | Internal MPSC write buffer. |
| `WithWriteBatch(n)` | 0 (off) | Coalesce up to n queued packets per writev syscall. Saves syscalls when concurrent publishers queue packets behind the writer; with one publisher or large payloads there is little to coalesce. The benchmarks do not measure it: measure with your workload before enabling. |
| `WithWriteOverflowPolicy(p)` | `WriteBlock` | QoS 0 only. See note below. |
| `WithPublishMode(mode)` | `PublishFireAndForget` | `PublishWaitForFlush` makes QoS 0 wait until the packet is written; on an idle TCP or Unix connection the caller writes it itself, without copying the payload. ctx bounds the wait; a write it interrupts is finished by the writer goroutine. |
| `WithPublisherPool(N)` | 0 (off) | N publish-only connections to the client's brokers, beside the main one, that `Publish` spreads over ([Publish pool](publisher-pool.md#publish-pool-withpublisherpool)). |
| `WithPublisherPoolRouting(p)` | `PoolRoutingRoundRobin` | `PoolRoutingHashByTopic` preserves per-topic ordering. |
| `WithPublisherPoolClientIDFn(fn)` | `"%s-pub-%d"`; empty (broker-assigned) when the parent ClientID is | Each member's ClientID from the parent's and a 1-based index, e.g. to fit a broker ACL pattern. IDs must be unique across every client of the broker. |
| `WithEncoding(e)` | none | The `Encoding` of the typed calls (`PublishValue`, `SubscribeAs` and their variants), e.g. `jsoncodec.Encoding{}`; without one they return `ErrNoEncoding` ([Typed publish / subscribe](typed-payloads.md#typed-publish--subscribe)). |
| `WithMaxInflight(n)` | 0 (the broker's Receive Maximum) | QoS 1/2 publishes in flight at once, per connection: the lower of n and the broker's Receive Maximum. The spec lets a client send fewer than the Receive Maximum, never more ([MQTT-4.9.0-1]), so an n above it changes nothing. Further publishes wait, bounded by their ctx. Publisher pool members inherit it. |
| `WithQoSDowngrade()` | off | Send a publish whose QoS exceeds the broker's Maximum QoS at that maximum instead of failing it with `ErrQoSNotSupported`. Weakens the guarantee (QoS 2 becomes at-least-once). |

> **Broker limits.** Checked before a packet is written; see
> [Broker limits](publishing.md#broker-limits).

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
| `WithOnConnectionUp(fn)` | `func(ConnackInfo)` — after every successful CONNACK | The broker reached (`Broker`, with `WithBrokers` the one failed over to) and what it granted: session present, limits, assigned ClientID, effective keep-alive, server reference, ... |
| `WithOnConnectionDown(fn)` | `func() bool` — on unexpected disconnect (not user Disconnect) | Return false to terminate the supervisor. |
| `WithOnConnectError(fn)` | per failed CONNECT attempt (dial err, CONNACK refusal, AUTH-loop err) | Observability only. |
| `WithOnReconnectAttempt(fn)` | immediately before each reconnect dial | Receives `(attempt, brokerURL)`. |
| `WithOnServerDisconnect(fn)` | `func(DisconnectInfo)` — on broker-initiated DISCONNECT | Reason code, reason string, server reference, user properties. |
| `WithOnServerRedirect(fn)` | `func(ServerRedirect)` — CONNACK or DISCONNECT with `0x9C`/`0x9D` and a Server Reference | Observability, or custom handling instead of `WithFollowServerRedirects`. |
| `WithOnResubscribeError(fn)` | `func(SubscriptionToken, error)` — the broker refused filters when the client re-subscribed after a session loss | The token's `Err()` is set when no filter is left. |
| `WithOnReauthenticated(fn)` | when a re-authentication started with `Reauthenticate` concludes with broker AUTH `0x00` | Observability only. |
| `WithOnStoreFailure(fn)` | `func(error)` — once, after a `WithStore` write failed and the client stopped because of it | The error is a `*StoreError` (`errors.Is(err, ErrStoreFailed)`). Call `Connect` to start again from what the store holds. |
| `WithOnReconnectLimit(fn)` | `func(error)` — once, after `WithMaxReconnectAttempts` or `WithMaxReconnectTime` ran out and the client stopped | The error is a `*ReconnectLimitError` (`errors.Is(err, ErrReconnectLimit)`). Call `Connect` to start again. |

#### Observability

| Option | Default | Effect |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | Structured logging. |
| `WithStats()` | — | Enable in-memory counters for `Client.Stats()` (off by default to keep the hot path branch-predictor friendly). |

### Per-subscribe

`SubscribeOption` applies to `Subscribe`, `SubscribeQueue`,
`SubscribeCallback`, plus their typed (`SubscribeAs[T]`, …) and `ClientGroup` variants.

| Option | Effect |
|---|---|
| `SubBuffer(n)` | Channel buffer size (Subscribe only): the QoS 0 messages held before one is dropped; QoS 1/2 have room for Receive Maximum more. Default `DefaultSubscribeBuffer` (64). |
| `SubMaxQueueSize(n)` | Queue cap (SubscribeQueue only); under `DropNewest`, QoS 1/2 have room for Receive Maximum more. 0 keeps the client default; `UnboundedQueue` removes the cap. |
| `SubDropPolicy(p)` | `DropNewest` / `DropOldest`. SubscribeQueue honours both, and `DropOldest` evicts whatever the QoS; chan-based `Subscribe` returns `ErrChanDropOldestUnsupported` when DropOldest is set explicitly. |
| `SubOnDrop(fn)` | Hook run on the read goroutine before a dropped message is acked; every field is readable. Must not block or call `Disconnect`. |
| `SubAutoAck()` | Opt-in: the dispatcher acks each delivery before handing it to the consumer, so a full buffer or queue drops QoS 1/2 messages too. See note below. Ignored by `SubscribeCallback`. |
| `SubEncoding(e)` | Decode this typed subscription with `e` instead of the client's `Encoding`. Ignored by untyped subscriptions. |
| `SubOnDecodeError(fn)` | `func(*Message, error)`, run on the read goroutine for each payload a typed subscription's `Encoding` rejects, before the message is acked. Must not block or call `Disconnect`. |
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
| `WithQueueWindow(n)` | `int` | 32 (`DefaultQueueWindow`) | Messages in flight at once; the broker's Receive Maximum and `WithMaxInflight` still apply. Must be ≥ 1. |
| `WithQueueMaxSize(n)` | `int` | 0 (unbounded) | Bound on queued messages, enforced atomically by the queue. |
| `WithQueueDropPolicy(p)` | `DropPolicy` | `DropNewest` | At the bound: `DropNewest` returns `ErrQueueFull`; `DropOldest` evicts the oldest messages not in flight and dead-letters them. |
| `WithQueueTTL(d)` | `time.Duration` | 0 (none) | Longest a message may wait, from `Publish`; the remaining lifetime is sent as Message Expiry Interval. |
| `WithDeadLetter(fn)` | `func(QueueEntry, error)` | none | Messages removed without the broker accepting them, with the reason. Called from internal goroutines, possibly concurrently; must not block. |
| `WithQueueClassifier(fn)` | `func(error) bool` | `PermanentPublishError` | Which failures are dead-lettered rather than retried. |
| `WithQueueRetryBackoff(b)` | `Backoff` | `DefaultQueueRetryBackoff` (`ExponentialBackoff{Initial: 1s, Max: 1min, Jitter: 0.5}`) | Delay before retrying a message whose publish failed for a reason that may pass; see [Backoff policies](#backoff-policies). |
| `WithQueueIdempotencyKey()` | — | off | Adds the `mqttv5-msg-id` user property with the message's ID. |

### Per-`ClientGroup`

| Option | Type | Default | Effect |
|---|---|---|---|
| `WithGroupSharedOpts(opts...)` | `...Option` | none | Client options applied to every member before its `GroupMember.Opts`. |
| `WithGroupPublishPolicy(p)` | `GroupPublishPolicy` | `GroupPublishBroadcast` | Broadcast, RoundRobin or HashByTopic (see [Parallel sessions](multi-broker.md#parallel-sessions-clientgroup)). |
| `WithGroupSuccess(s)` | `GroupSuccess` | `GroupSuccessAll` | How many members must succeed for a broadcast Publish or a Subscribe; `GroupSuccessAny`, `GroupSuccessQuorum(n)`. |
| `WithGroupSequentialLifecycle()` | — | parallel | Connect, Disconnect and Subscribe members one after another. |

| `GroupMember` field | Default | Effect |
|---|---|---|
| `Brokers` | (required) | The member's broker URLs: one, or several it fails over between as `WithBrokers` does. `WithBroker`/`WithBrokers` in `Opts` or `WithGroupSharedOpts` is rejected. |
| `Name` | `member-N` (1-based) | Key in the Subscribe token map and for `ClientGroup.Member`; must be unique. |
| `Opts` | none | Client options for this member, applied after `WithGroupSharedOpts`. |

---

## Errors

Branch on sentinel errors with `errors.Is`. Typed errors
(`*ReasonCodeError`, `*SubscribeError`, …) carry the details; reach
them with `errors.As`.

### Connection lifecycle

| Error | Source | Meaning |
|---|---|---|
| `ErrNotConnected` | `Publish`, `Reauthenticate` | No live connection; from `Publish` it also guarantees the message was not sent. Retry / wait for reconnect (`AwaitConnection`), or let `Publish` wait (`WithPublishWaitForConnection`). |
| `ErrAlreadyConnected` | `Connect` | Connect called twice. |
| `ErrClosed` | any after `Disconnect` | Client torn down. Also matched by `*ReconnectLimitError`. |
| `ErrReconnectLimit` | QoS 1/2 `Publish`, `AwaitConnection`, `WithOnReconnectLimit` | A reconnect limit ran out and the client stopped; the error is a `*ReconnectLimitError` with `Attempts`, `Elapsed` and the last attempt's failure in `Err` (see [Reconnect limits](#reconnect-limits)). `Connect` starts again. |
| `ErrConnectRefused` | `Connect` | Broker non-success CONNACK reason; the error is a `*ReasonCodeError` carrying the code, reason string and server reference. |
| joined errors | `Connect` with `WithBrokers` | No broker accepted: one error per broker, each prefixed with its URL; `errors.Is` / `errors.As` see every one. |
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
| `ErrNoEncoding` | `PublishValue`, `SubscribeAs` and their variants | No `Encoding`: set `WithEncoding`, or `SubEncoding` for one subscription. Nothing was sent. |
| `ErrSessionLost` | `Publish` (QoS 1/2) | Session lost with `WithSessionLossPolicy(SessionLossFail)`. |
| `ErrMessageExpired` | `Publish` (QoS 1/2) | Message Expiry ran out before a republish after session loss. |
| `ErrPacketIDsExhausted` | `Publish`, `Subscribe*` | All 65,535 packet identifiers in use until ctx ended. |

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

## Stability

mqttv5 is pre-1.0: any minor release may change the API.

- The client API is the `mqttv5` package: `Client`, `Config`, the
  options, `Stats`, and its value types (`PublishOptions`,
  `TopicFilter`, `Message`, `TypedMessage`, `Properties`, `ConnackInfo`,
  `DisconnectInfo`, `WillOptions`, `ConnectOptions`,
  `DisconnectOptions`, `ServerRedirect`, results and errors).
- `ReasonCode`, `PacketType` and `UserProperty` are the protocol's own
  values, shared with the codec.
- `wire`, the codec underneath, is exported for tools, test brokers and
  storage formats. The client API never requires it, and it may change
  in any release.
- Submodules have their own `go.mod` and are tagged with the core at
  every release; use matching versions ([Modules](modules.md#modules)).

<!-- MQTT v5.0 citations -->

[§3.1.2.11.2]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048 "3.1.2.11.2 Session Expiry Interval"
[§3.1.2.11.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050 "3.1.2.11.4 Maximum Packet Size"
[§3.1.2.11.5]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051 "3.1.2.11.5 Topic Alias Maximum"
[§3.1.2.11.6]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901052 "3.1.2.11.6 Request Response Information"
[§3.1.2.11.7]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053 "3.1.2.11.7 Request Problem Information"
[§3.2.2.3.14]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094 "3.2.2.3.14 Server Keep Alive"
[§4.12]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256 "4.12 Enhanced authentication"
[MQTT-3.14.2-2]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211 "3.14.2.2.2 Session Expiry Interval"
[MQTT-4.9.0-1]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901251 "4.9 Flow Control"
