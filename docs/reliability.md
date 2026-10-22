# Reliability semantics

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

- [Connection](#connection)
- [Sessions](#sessions)
- [Delivery](#delivery)
- [Subscriptions](#subscriptions)
- [Protocol](#protocol)

## Connection

**Connect.** Blocks until the CONNACK, or until ctx ends. With
`WithBrokers` it tries each broker once, from a random one on. Without
`WithRetryInitialConnect()` it returns the failure, every broker's when
there are several. With it,
`Connect` returns nil, the supervisor retries with the reconnect
backoff, and `Client.AwaitConnection(ctx)` waits for the connection;
until then `Publish` returns `ErrNotConnected` (with
`WithPublishWaitForConnection()` it waits for the connection, bounded
by its ctx), `Subscribe` waits for the connection, and a
`QueuePublisher` keeps queuing.

**Reconnect.** `ExponentialBackoff(1s, 30s, 200ms)` by default. With
`WithBrokers`, URLs rotate per attempt and a successful connect sticks.
The backoff starts over only after a connection outlives the delay that
preceded it, so a broker that accepts the CONNECT and drops the
connection at once is retried at a growing interval, not in a tight
loop.

**Keep-alive.** After a PINGREQ, any packet from the broker counts as
the answer. With none within `PingTimeout` the connection is treated as
dead and the supervisor redials — unless application code held the read
loop meanwhile, which only delays the verdict (see the
[keep-alive note](reference.md#keepalive-and-timeouts)).

**Server-initiated DISCONNECT.** `WithOnServerDisconnect(fn)` receives
a `DisconnectInfo` (reason, reason string, server reference, user
properties) before `OnConnectionDown`. Redirects (`0x9C`/`0x9D`) are
also reported to `WithOnServerRedirect` and followed with
`WithFollowServerRedirects`.

## Sessions

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
`0x82`). With `WithBrokers`, failover to a broker that does not share
the session (not a node of the same cluster) is a session loss.

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

## Delivery

**Acknowledgement order.** A QoS 1 PUBACK is held until `m.Ack()` and
flushed in [§4.6] arrival order. A QoS 2 PUBREC is held until `m.Ack()`;
PUBCOMP goes out automatically when PUBREL arrives.

**A consumer that falls behind.** Holding the acknowledgement until
`m.Ack()` is also the flow control: the broker sends at most Receive
Maximum QoS 1/2 messages the client has not acknowledged, so a channel
or queue subscription keeps room for that many beyond its buffer and
drops none of them. QoS 0 messages beyond the buffer are dropped and
acked (see [Subscribing](subscribing.md#subscribing)).

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

## Subscriptions

**Across a drop.** A SUBSCRIBE or UNSUBSCRIBE still waiting for its
answer when the connection drops is sent again on the next one, in the
order the calls were made; `Subscribe` and `Unsubscribe` keep waiting
until the answer arrives or ctx ends. A `Subscribe` made while the
client reconnects waits for the next connection the same way.

**Before Connect.** A `Subscribe` before `Connect` (or between
`Disconnect` and the next `Connect`) returns at once, and its SUBSCRIBE
goes out first on the connection `Connect` makes. Its route is in place
before the CONNACK, so the messages a resumed session holds — which the
broker sends right after the CONNACK — reach it; `token.Results()`
reports the SUBACK once it arrives. A PUBLISH no subscription matches
is acknowledged and dropped, counted in `Stats().InboundUnrouted`; the
first on each connection is logged at Warn unless it was for a
subscription being unsubscribed, since on a resumed session it is most
likely a queued message that arrived before the application subscribed.
A filter that connection's CONNACK
rules out (shared or wildcard subscriptions unavailable) ends the
subscription with that error instead of being sent. If `Connect`
fails, its subscriptions end with it. When the broker has no session,
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
receives every copy. A PUBLISH tagged only with identifiers this client
never gave a subscription to its topic was sent for a subscription of
an earlier process, whose session this one resumed: it is matched by
topic, as without identifiers. One tagged with an identifier the
client has replaced, or is unsubscribing, is dropped.

**Broker capabilities.** When the CONNACK disables shared subscriptions,
wildcards or Subscription Identifiers, `Subscribe*` fails before
anything is sent.

**Teardown.** `Unsubscribe` closes the subscription's channel or queue
before it returns, then reports the broker's answer; `Disconnect` closes
all of them. Each closes exactly once; a PUBLISH already being
dispatched to a closed subscription is acked instead of delivered.
`Disconnect` clears all routes, so `Connect` followed by `Subscribe`
starts clean.

## Protocol

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

<!-- MQTT v5.0 citations -->

[§3.8.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901170 "3.8.4 SUBSCRIBE Actions"
[§4.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901238 "4.4 Message delivery retry"
[§4.6]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901240 "4.6 Message ordering"
[§4.9]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901251 "4.9 Flow Control"
[§4.12]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256 "4.12 Enhanced authentication"
