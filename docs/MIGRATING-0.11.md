# Migrating from v0.10 to v0.11

v0.11 changes the API where v0.10's could not be made correct, and it
changes behaviour wherever v0.10 broke MQTT v5 or lost messages. This
guide lists every breaking change with the edit it needs, then the
behaviour changes that need no edit but may surprise you. The complete
list of API differences, produced by `apidiff`, is at the end.

Most programs need only the edits in [The common edits](#the-common-edits).

## The common edits

| v0.10 | v0.11 |
|---|---|
| `wire.PublishOpts{...}` passed to `Publish` | `mqttv5.PublishOptions{...}` — same fields without `PacketID` and `Dup` |
| `wire.UserProperty` | `mqttv5.UserProperty` (the same type) |
| `wire.SubscribeFilter` / `mqttv5.TopicFilter` | `mqttv5.TopicFilter` (now its own type, same fields) |
| `m.Properties.String(wire.PropContentType)` | `m.Properties.ContentType()` — see [Message properties](#message-properties) |
| `m.Publish.X` (the embedded `*wire.Publish`) | `m.X` — `Topic`, `Payload`, `Properties`, `QoS`, `Retain`, `Dup`, `PacketID` |
| `mqttv5.WithWill(&wire.WillOpts{WillDelayInterval: d, ...})` | `mqttv5.WithWill(&mqttv5.WillOptions{DelayInterval: d, ...})` |
| `WithConnectPacketBuilder(func(ctx, *wire.ConnectOpts) error)` | `WithConnectPacketBuilder(func(ctx, *mqttv5.ConnectOptions) error)` — `Username`, `Password`, `UserProperties` |
| `WithOnConnectionUp(func(*wire.Connack))` | `WithOnConnectionUp(func(mqttv5.ConnackInfo))` |
| `WithOnServerDisconnect(func(*wire.Disconnect))` | `WithOnServerDisconnect(func(mqttv5.DisconnectInfo))` |
| `cli.DisconnectWith(ctx, wire.DisconnectOpts{...})` | `cli.DisconnectWith(ctx, mqttv5.DisconnectOptions{...})` — no `ServerReference` |
| `wire.Reason...` constants | `mqttv5.Reason...` (the same values) |
| `err := group.Publish(ctx, opts)` | `results, err := group.Publish(ctx, opts)` |

The `wire` package is still exported, for tools and test brokers, but it
is no longer part of the client's API or its stability promise. A
program that imports `wire` only for the types above can drop the
import.

## Publishing

```go
// v0.10
err := cli.Publish(ctx, wire.PublishOpts{
    Topic:          "a/b",
    Payload:        p,
    QoS:            1,
    UserProperties: []wire.UserProperty{{Key: "k", Value: "v"}},
})

// v0.11
err := cli.Publish(ctx, mqttv5.PublishOptions{
    Topic:          "a/b",
    Payload:        p,
    QoS:            1,
    UserProperties: []mqttv5.UserProperty{{Key: "k", Value: "v"}},
})
```

`Typed.Publish`, `QueuePublisher.Publish` and `ClientGroup.Publish` take
`PublishOptions` too.

Behaviour that changed:

- QoS 1/2 messages belong to the session once `Publish` registered them:
  a dropped connection resends them (DUP=1) when the session resumes,
  and `Publish` keeps waiting across the drop. If the broker lost the
  session, `WithSessionLossPolicy` decides: resend as new messages
  (default) or fail them with `ErrSessionLost`.
- The broker's CONNACK limits are enforced before sending: QoS above
  its Maximum QoS fails with `ErrQoSNotSupported` (`WithQoSDowngrade`
  sends it at the maximum instead), retain on a broker without retain
  fails with `ErrRetainNotSupported`, a packet above its Maximum Packet
  Size fails with `ErrPacketTooLarge`. At most Receive Maximum QoS 1/2
  messages are in flight; further ones wait.
- A broker refusal (PUBACK/PUBREC 0x80 or above) is a
  `*mqttv5.ReasonCodeError`; `errors.Is` matches `ErrNotAuthorized`,
  `ErrQuotaExceeded`, `ErrTopicNameInvalid`, `ErrPayloadFormatInvalid`,
  `ErrPacketTooLarge`.
- Invalid topics and options fail before anything is sent:
  `ErrInvalidTopic`, `ErrInvalidField`, `ErrFieldTooLong`.
- Outbound topic aliases are opt-in: `WithOutboundTopicAliases()`.
  v0.10 used them automatically whenever the broker allowed.
- With `WithPublisherPool`, a publish moves to another member only when
  its member could not send it at all. A refusal or a ctx error is
  returned as is; v0.10 retried it on every member and the main
  connection, putting duplicates on the wire.
- From `Publish`, `ErrNotConnected` now guarantees the message was not
  sent.
- A publish that waits for its write — QoS 1/2, or QoS 0 with
  `PublishWaitForFlush` — is written on the calling goroutine when the
  connection is TCP or a Unix socket and nothing is queued for its
  writer; a QoS 0 payload is then not copied. ctx still bounds the call:
  a write it interrupts is finished by the writer goroutine, so, as in
  v0.10, a packet may reach the broker after `Publish` returned ctx's
  error. TLS and WebSocket connections always write on the writer
  goroutine. Fire-and-forget QoS 0 is unchanged.
- With `WithOutboundTopicAliases`, an alias is registered only once its
  PUBLISH has been written or queued; a publish refused before that
  (too large, invalid, queue full, ctx ended) leaves no alias behind.

## Receiving

### Messages own their memory

`Message` no longer embeds the decoder's pooled `*wire.Publish`. Its
fields are its own: `Topic`, `Payload`, `Properties`, `QoS`, `Retain`,
`Dup`, `PacketID`. They stay valid for as long as you hold the message,
also after `Ack`. `SubZeroCopy()` keeps v0.10's aliasing for large
payloads, valid until every receiving subscription has acked.

`Ack` is idempotent per message: a second call does nothing. It can
never acknowledge another message.

`SubscribeCallback` takes `SubscribeOption`s now (only `SubZeroCopy`
applies) and delivers owned messages by default.

### Message properties

`Message.Properties` is an `mqttv5.Properties` with typed getters:

| v0.10 | v0.11 |
|---|---|
| `m.Properties.Byte(wire.PropPayloadFormat)` | `m.Properties.PayloadFormatIndicator()` |
| `m.Properties.Uint32(wire.PropMessageExpiryInterval)` | `m.Properties.MessageExpiryInterval()` |
| `m.Properties.String(wire.PropContentType)` | `m.Properties.ContentType()` (`""` when absent) |
| `m.Properties.String(wire.PropResponseTopic)` | `m.Properties.ResponseTopic()` |
| `m.Properties.Binary(wire.PropCorrelationData)` | `m.Properties.CorrelationData()` (nil when absent) |
| `m.Properties.UserProperties()` | `m.Properties.UserProperties()`, or `UserProperty(key)` |
| `m.Properties.Varint(wire.PropSubscriptionIdentifier)` | `m.Properties.SubscriptionIdentifiers()` |

## Subscribing

`TopicFilter` is its own type with the same fields; composite literals
compile unchanged.

What `Subscribe` returns changed:

- When the broker refuses some filters, `Subscribe` returns a
  `*mqttv5.SubscribeError` **together with** the channel and token: the
  subscription stays active for the granted filters and the channel
  carries their messages; `Unsubscribe` the token if a partial grant is
  not good enough. When it refuses all of them the channel is already
  closed. v0.10 reported success and returned a channel that never
  received anything or closed.
- `token.Results()` gives each filter's SUBACK code; `token.Err()`
  says why a subscription ended (every filter refused, initially or on a
  re-subscribe after a session loss).
- `Subscribe` and `Unsubscribe` no longer hang when the connection drops
  before the answer: the packet is sent again on the next connection and
  the call returns the answer, or ctx's error.

```go
msgs, tok, err := cli.Subscribe(ctx, filters)
var serr *mqttv5.SubscribeError
switch {
case errors.As(err, &serr):
    log.Printf("partly refused: %+v", serr.Results) // msgs carries the rest
case err != nil:
    return err
}
_ = tok
```

`Unsubscribe` closes the subscription's channel or queue before it
returns, then waits for the UNSUBACK; it no longer returns
`ErrNotConnected` (without a connection it waits for the next one, or
ctx). Refusals are an `*mqttv5.UnsubscribeError`.

A SUBSCRIBE or UNSUBSCRIBE that the broker answers by closing the
connection with a DISCONNECT blaming the packet (mosquitto does for a
filter deeper than 200 levels) is sent at most three times and then
fails with `*mqttv5.RejectedByDisconnectError`; the client stays
connected.

Other changes:

- Subscription Identifiers are allocated automatically when the broker
  supports them, and messages for overlapping filters (`a/#` and `a/b`)
  reach each subscription once. `ErrSubscriptionIDsUnsupported` is gone.
- Subscriptions of one client with the same filter share the broker's
  subscription (MQTT keeps one per filter): each receives every message,
  the most recent `Subscribe`'s options apply, and the UNSUBSCRIBE is
  sent only when the last of them unsubscribes. In v0.10 unsubscribing
  either one silently ended the other.
- `SubscribeQueue` is bounded by default at
  `DefaultMaxSubscribeQueueSize` (65,536). `SubMaxQueueSize(0)` and
  `WithMaxSubscribeQueueSize(0)` now mean "the default"; use
  `mqttv5.UnboundedQueue` for no bound (v0.10's 0).
- `Typed.Subscribe`, `Typed.SubscribeQueue` and the `ClientGroup`
  variants deliver straight from the connection's read loop with the
  same buffer and drop rules as `Client.Subscribe`, and their outputs
  close when the subscription ends. Typed decoding now runs on the read
  goroutine.

## Connecting and options

- Options are validated by `New`: `ws://`/`wss://` URLs need
  `WithDialFunc`; negative sizes and timeouts are rejected; the client
  ID, credentials, CONNECT user properties and the Will are encoded once
  so an invalid value fails `New` instead of every connection attempt.
- `PingTimeout` defaults to the smaller of 10 s and half the keep-alive,
  and must be shorter than the keep-alive.
- The Server Keep Alive from CONNACK replaces the requested keep-alive.
- `WithConnectPacketBuilder` may change only `Username`, `Password` and
  `UserProperties`; everything else in the CONNECT comes from the
  options.
- `ErrConnectRefused` is still matched with `errors.Is`; `errors.As`
  with `*mqttv5.ReasonCodeError` gives the code, reason string and
  server reference.
- A CONNECT is sent with `RequestProblemInformation` absent unless
  `WithRequestProblemInformation(false)` (the broker's default is 1, so
  nothing changes on the wire's meaning).
- The read window defaults to 16 KiB (`WithReadBufferSize`).
- `DisconnectWith` rejects a non-zero Session Expiry when CONNECT sent 0
  (`ErrInvalidSessionExpiry`).
- A broker AUTH outside a re-authentication the client started is a
  protocol error (DISCONNECT 0x82); `WithOnReauthenticated` fires only
  for `Reauthenticate`.
- Every broker packet is validated; a violation closes the connection
  with the right reason code and the client reconnects.
  `WithLenientDecoding()` tolerates the two harmless violations seen in
  practice.
- The reconnect backoff no longer starts over after every successful
  CONNECT: while connections drop sooner than the delay before them,
  the delay keeps growing, and `WithOnReconnectAttempt`'s attempt
  number keeps counting.
- `Connect` called while a `Disconnect` is still tearing down waits for
  it instead of returning `ErrAlreadyConnected`. A supervisor stopped by
  `OnConnectionDown` returning false now tears down like `Disconnect`:
  subscriptions close.
- Cancelling `Connect`'s ctx ends the handshake at once with ctx's
  error, rather than at the connect timeout. `Disconnect` ends a
  reconnect attempt in progress instead of waiting for it, without
  `OnConnectionDown` or `OnConnectError` firing for it. `Connect`
  returns `ErrClosed` when a `Disconnect` overlapping it wins.
- `Disconnect` waits for the goroutines that run the lifecycle
  callbacks and `SubscribeCallback` handlers: call it from one of them
  on a new goroutine, or it deadlocks.

New, opt-in:

- `WithRetryInitialConnect()` and `Client.AwaitConnection(ctx)`: start
  while the broker is unreachable.
- `WithFollowServerRedirects()` and `WithOnServerRedirect(fn)`: honour
  CONNACK/DISCONNECT 0x9C/0x9D with a Server Reference.
- `Client.ServerInfo()` and `WithOnConnectionUp(func(ConnackInfo))`:
  everything the broker granted.

## Sessions and persistence

`session.Store` is a record API now (v0.10's store was never read back,
so its data cannot be migrated and does not need to be):

```go
type Store interface {
    Load(ctx) (Meta, []Record, error)
    Put(ctx, Record) error
    Delete(ctx, RecordKey) error
    SetMeta(ctx, Meta) error
    Reset(ctx) error
    Close() error
}
```

Custom stores implement it and run `session/storetest`, keeping every
field of `Record` — `Seq` (the order PUBLISHes were sent in) and
`PubrecSeq` (the order PUBRECs arrived in, for AwaitPubcomp records)
both — and every phase `Record.Validate` accepts, including
`Completed` (an outbound message the broker accepted whose
`QueuePublisher` has not yet recorded it; no packet). The client treats any error from a write as final (see below):
a store over a backend with transient failures retries them itself.
`store/file` is a new bbolt-backed implementation with a sync policy
(group commit by default) and an exclusive lock (`ErrLocked`); delete
v0.10's `outbound/` and `inbound/` directories.

With a store holding unfinished QoS 1/2 flows, `Connect` resumes the
session (CleanStart=0) unless `WithCleanStart(true)` was set
explicitly, which discards it.

Store failures: a failed write of a new message's record fails that
`Publish` with a `*mqttv5.StoreError` (`errors.Is(err,
mqttv5.ErrStoreFailed)`). Any other failed write stops the client as a
crash would — DISCONNECT 0x80, waiting QoS 1/2 `Publish` calls return
the `*StoreError`, `WithOnStoreFailure` fires — and the next `Connect`
reloads the session from the store. v0.10 logged store errors and went
on.

## QueuePublisher

`PublisherQueue` changed to make bounds atomic and draining pipelined:

| v0.10 | v0.11 |
|---|---|
| `Enqueue(ctx, entry) error` | `Enqueue(ctx, entry, QueueLimit) (seq, evicted, error)` |
| `PeekBatch(ctx, n) ([]QueueEntry, []QueueToken, error)` | `Peek(ctx, after uint64, n) ([]QueueEntry, error)` |
| `Ack(ctx, QueueToken)` | `Ack(ctx, seq uint64)` |
| `EvictHead(ctx)` | removed: `Enqueue` evicts under `DropOldest` |
| `QueueEntry{Publish wire.PublishOpts, EnqueuedAt}` | adds `Seq`, `ID`, `ExpiresAt`; `Publish` is `PublishOptions` |

Custom queues implement the new interface and run `queuetest`.
`queue/file` is rebuilt on bbolt; move a v0.10 queue directory with
`queuefile.ImportV010`.

Options: `WithQueueBatchSize`, `WithQueueIdleInterval` and
`WithQueuePublishTimeout` are removed (the drain keeps
`WithQueueWindow` messages in flight and has no per-message timeout);
`WithQueueClassifier`, `WithQueueRetryBackoff` and
`WithQueueIdempotencyKey` are new. `ErrEvictionNotSupported` is gone.

Behaviour: refusals the broker will repeat are dead-lettered instead of
blocking the queue; other failures retry with backoff; a TTL sends the
remaining lifetime; with `queue/file` and `store/file` a restart
continues in-flight exchanges instead of publishing them again. When the
queue fails to remove a message the broker accepted, the removal is
retried and the message is not published again. The dead-letter
callback may run concurrently from internal goroutines.

## ClientGroup

```go
// v0.10
err := g.Publish(ctx, wire.PublishOpts{Topic: "t", QoS: 1})

// v0.11
results, err := g.Publish(ctx, mqttv5.PublishOptions{Topic: "t", QoS: 1})
```

- A broadcast now succeeds only when every member succeeded
  (`GroupSuccessAll`); v0.10 succeeded when any did. Keep v0.10's rule
  with `WithGroupSuccess(mqttv5.GroupSuccessAny)`. Failures are a
  `*mqttv5.GroupError` with each member's result.
- RoundRobin and HashByTopic send a message to one member and move on
  only when it could not be sent at all.
- `Subscribe` follows the same success policy and unsubscribes the
  members that succeeded when it fails or ctx ends; the merged channel
  or queue closes when every member's subscription has ended.
- A group cannot be connected again after `Disconnect` (`ErrClosed`).

## WebSocket

`wss://` without `DialOpts.TLSConfig` verifies the broker against the
system roots, like `mqtts://`; `ErrMissingTLSConfig` is gone. `Close`
no longer waits for a write in progress: it sends the Close frame only
when no write is (within the new `DialOpts.CloseFrameTimeout`, default
1 s) and then closes the socket, which ends a write stalled on a broker
that stopped reading.

## estavelle

The edits in estavelle at the time of v0.11 (paths relative to its
repository root):

| File | Edit |
|---|---|
| `plugin/bridge/mqtt/conn.go` | `wire.PublishOpts` → `mqttv5.PublishOptions`, `wire.UserProperty` → `mqttv5.UserProperty`; `translateInbound` reads `m.Properties.ContentType()`, `ResponseTopic()`, `CorrelationData()`, `MessageExpiryInterval()`, `PayloadFormatIndicator()` instead of the `wire.Prop*` getters; handle `*mqttv5.SubscribeError` from `Subscribe`. |
| `plugin/bridge/mqtt/mqtt.go` | the builder takes `*mqttv5.ConnectOptions` (it sets `Password`); `WithWill(&mqttv5.WillOptions{...})`, `WillDelayInterval` → `DelayInterval`. |
| `plugin/bridge/mqtt/*_test.go` | same type renames. |
| `bench/harness/internal/client/v5/client.go` | `wire.PublishOpts` → `mqttv5.PublishOptions`; map `*mqttv5.SubscribeError` and `*mqttv5.UnsubscribeError` to `*client.RefusedError` (codes from `Results[i].Reason`) and stop clearing `SubackCodes` in `Capabilities`. |
| `bench/harness/internal/client/v5/client_test.go` | delete `TestRefusedSubscribeIsInvisibleUpstream`: it asserts v0.10's silent refusal and fails by design on v0.11; the adapter contract's "refused subscribe carries the code" case now runs instead of skipping. |
| `bench/harness/internal/client/client.go` | `MQTTCapabilities` comment: v5 no longer clears `SubackCodes`. |
| `test/compliance/protocol/extras/topic_alias_lifecycle_test.go` | set `WithOutboundTopicAliases()` where the test expects the client to use aliases. |

## Complete API differences

`apidiff -m` between v0.10.1 and v0.11 for the core module, incompatible
changes only (the compatible additions are listed in the README and
godoc):

```text
session.Store: Load, Put, Delete, SetMeta, Close added; PutOutbound,
  DeleteOutbound, RangeOutbound, PutInbound, DeleteInbound,
  RangeInbound removed; Reset takes a context.
session: Session, Config, New, IDPool, NewIDPool, InboundTracker,
  NewInboundTracker, OutboundTracker, NewOutboundTracker,
  OutboundEntry, OutboundState, StateAwaiting*, AckSend, ReasonError,
  ErrDuplicateOutbound, ErrIDPoolExhausted, ErrUnknownOutbound removed.
(*Client).Publish, (*Typed[T]).Publish, (*QueuePublisher).Publish:
  take PublishOptions.
(*Client).DisconnectWith: takes DisconnectOptions.
(*Client).SubscribeCallback: takes ...SubscribeOption.
(*Client).Subscribe, SubscribeQueue, (*Typed[T]).Subscribe,
  SubscribeQueue, (*ClientGroup).Subscribe, SubscribeQueue: TopicFilter
  is a new type.
(*ClientGroup).Publish: takes PublishOptions, returns ([]GroupResult, error).
Message, TypedMessage: Publish (embedded *wire.Publish) removed;
  Properties is Properties.
PublisherQueue, MemoryPublisherQueue: Enqueue, Peek, Ack as above;
  PeekBatch, EvictHead removed. QueueToken removed.
QueueEntry.Publish: PublishOptions.
Config.ConnectPacketBuilder, OnConnectionUp, OnServerDisconnect,
  WillMessage and their With* options: new types as above.
WithQueueBatchSize, WithQueueIdleInterval, WithQueuePublishTimeout,
  DefaultQueueBatchSize, DefaultQueueIdleInterval,
  DefaultQueuePublishTimeout, ErrEvictionNotSupported,
  ErrSubscriptionIDsUnsupported removed.
transport/ws: ErrMissingTLSConfig removed.
```
