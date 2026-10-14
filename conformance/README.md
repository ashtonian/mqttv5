# conformance

Tests that run `github.com/ashtonian/mqttv5` against real brokers —
mosquitto and EMQX — for the protocol behaviour unit tests with a
scripted peer cannot prove: that real brokers accept what the client
sends and that the client handles what they send back. The tests are
behind the `conformance` build tag, so `go test ./...` elsewhere needs
no broker.

## Overview

Every test connects real clients to a broker and checks the outcome end
to end: payloads byte for byte, properties field by field, QoS levels
on the delivered message, absence of duplicates where the protocol
promises it, and absence of messages that must not arrive. Tests that
need a broker that is not running skip (`--- SKIP`) instead of failing.

The list below is generated from the suite: `TestREADMEListsEveryTest`
(no tag, no broker) fails when it does not match the test functions and
their doc comments, and prints the block to paste.

## What it tests

<!-- tests:begin -->
| Test | File | Checks |
|---|---|---|
| `TestConnect_Disconnect` | `conformance_test.go` | Connect reaches the broker and reports the connection and client ID. |
| `TestConnect_RoundTripsThroughBroker` | `conformance_test.go` | A connected client publishes and another receives it through the broker. |
| `TestConnect_CleanStartWipesPriorSubscription` | `conformance_test.go` | CleanStart=1 discards the session a previous connection with the same client ID left, including its subscriptions. |
| `TestSession_DurableConsumerRestart` | `conformance_test.go` | A durable consumer restarted as a new Client gets the messages that arrived while it was down: it subscribes before Connect, so its route is in place when the resumed session's messages arrive right after the CONNACK, tagged with the earlier process's Subscription Identifier. |
| `TestConnect_WithCredentials` | `conformance_test.go` | A CONNECT with a user name and password is accepted. |
| `TestPublish_QoS0_DeliveredToSubscriber` | `conformance_test.go` | A QoS 0 publish arrives at QoS 0 with its payload. |
| `TestPublish_QoS1_DeliveredAndAcked` | `conformance_test.go` | A QoS 1 publish returns after PUBACK and arrives at QoS 1. |
| `TestPublish_QoS2_ExactlyOnce` | `conformance_test.go` | A QoS 2 publish completes the PUBREC/PUBREL/PUBCOMP exchange and arrives exactly once. |
| `TestSubscribe_PlusWildcard_MatchesOneLevel` | `conformance_test.go` | "+" matches exactly one topic level. |
| `TestSubscribe_HashWildcard_MatchesParentAndChildren` | `conformance_test.go` | "#" matches the parent level and every level below it. |
| `TestPubSub_AllPublishProperties` | `conformance_test.go` | Content type, response topic, correlation data and user properties arrive as published. |
| `TestPublish_Retain_DeliveredToLateSubscriber` | `conformance_test.go` | A retained message reaches a later subscriber with Retain set, and an empty retained publish clears it. |
| `TestSubscribeQueue_OrderedDelivery` | `conformance_test.go` | SubscribeQueue delivers one publisher's QoS 1 messages in order. |
| `TestTypedJSON_FullStructEquality` | `conformance_test.go` | `SubscribeAs` and `PublishValue` with the JSON Encoding round-trip a struct; the broker forwards the Content Type. |
| `TestPublish_LargePayload_ByteEquality` | `conformance_test.go` | A 64 KiB payload arrives byte for byte. |
| `TestTopicAlias_OutboundReducesBytes` | `conformance_test.go` | With WithOutboundTopicAliases a repeated QoS 0 topic is sent as an alias, saving at least the topic's length, and the subscriber still sees the full topic. |
| `TestUnsubscribe_StopsDelivery` | `conformance_test.go` | Unsubscribe closes the subscription's channel and later publishes do not arrive. |
| `TestSubscribe_MultipleHandlersDispatch` | `conformance_test.go` | Two subscriptions of one client on the same filter each receive the message. |
| `TestSubscribe_SameFilterSurvivesUnsubscribe` | `conformance_test.go` | The broker holds one subscription per filter ([§3.8.4]): when one of two subscriptions with the same filter unsubscribes, the other keeps receiving. |
| `TestSubscribe_SharedSub_RoundRobinAcrossGroup` | `conformance_test.go` | A shared subscription ($share) delivers each message to exactly one member of the group. |
| `TestClientGroup_PublishFanOutToBothBrokers` | `conformance_test.go` | A ClientGroup broadcast publish reaches subscribers on both brokers. |
| `TestQueuePublisher_DrainsInOrderOnce` | `conformance_test.go` | A pipelined QueuePublisher delivers every queued message once, in order, through a real broker at QoS 1 and 2. |
| `TestSession_LostWhileDisconnected` | `faults_test.go` | When the broker has lost the session by the time the client reconnects (Session Present 0), an unacknowledged QoS 1 publish follows the session-loss policy: republished as a new message by default, or failed with ErrSessionLost. |
| `TestQoS2_ResumesAfterPUBREC` | `faults_test.go` | A QoS 2 exchange cut after PUBREC resumes with PUBREL on the next connection ([§4.4]): the Publish succeeds and the message arrives once. |
| `TestSubscribe_RefusedFilterReported` | `faults_test.go` | A filter a broker will not take is reported, and the client carries on. |
| `TestReconnect_QoS1SurvivesConnectionDrop` | `faults_test.go` | A QoS 1 message published across an ungraceful connection drop is resent after the reconnect, delivered at least once, and its Publish call returns success. |
| `TestSoak_DeliveryAcrossConnectionCuts` | `faults_test.go` | Publishers whose connection a proxy cuts at random moments keep their session (Session Expiry 300 s), so every message they publish arrives: QoS 2 exactly once, QoS 1 at least once. |
| `TestSoak_BrokerFaultAttribution` | `faults_test.go` | The soak blames the broker only for a message the client exchanged correctly; any fault of the client's own stays a failure. |
| `TestReceiveMaximumHonoured` | `features_test.go` | Publishes far more QoS 1 messages at once than the broker's Receive Maximum (mosquitto advertises 20, EMQX 32, HiveMQ 10, the strict HiveMQ 4) and checks the client never had more in flight than the broker allows and that every publish completes. |
| `TestSubscribe_NoLocal_SuppressesOwnPublishes` | `features_test.go` | Verifies the MQTT v5 [§3.8.3.1] No-Local subscription option: when a client subscribes with NoLocal=true, the broker must NOT deliver that client's own matching PUBLISHes back to it. |
| `TestOverlappingSubscriptionsOneCopyEach` | `features_test.go` | Checks against real brokers that, with two overlapping subscriptions on one client, each receives the message exactly once. |
| `TestSubscribe_RetainAsPublished` | `features_test.go` | Retain As Published decides whether a live forward keeps the publisher's RETAIN flag. |
| `TestSubscribe_RetainHandling` | `features_test.go` | Retain Handling decides whether a new subscription receives the stored retained message. |
| `TestSubscribe_SubscriptionIdentifier_EchoedOnDelivery` | `features_test.go` | Verifies the MQTT v5 Subscription Identifier echo ([§3.3.4]): when a SUBSCRIBE carries a Subscription Identifier ([§3.8.2.1.2]), every matching PUBLISH the broker dispatches to that subscription is tagged with the same identifier in PropSubscriptionIdentifier ([§3.3.2.3.8]). |
| `TestSubscribe_SubscriptionIdentifier_RoutesByID` | `features_test.go` | Verifies the [§3.8.4] routing invariant: when one session holds several subscriptions each carrying a distinct Subscription Identifier, a delivered PUBLISH is tagged with the identifier of the subscription it matched — so the receiver can route by id. |
| `TestWill_DeliveredOnUngracefulDisconnect` | `features_test.go` | Verifies the broker publishes the configured will (payload + will properties) when the will client's TCP connection drops without a DISCONNECT. |
| `TestWill_SuppressedOnGracefulDisconnect` | `features_test.go` | Verifies a normal DISCONNECT clears the will: the broker must NOT publish it. |
| `TestWill_DelayInterval` | `features_test.go` | Verifies WillDelayInterval defers publication: after an ungraceful drop the will must not arrive before the delay elapses, but must arrive once it does. |
| `TestReauthenticate_SCRAM_EMQX` | `reauth_test.go` | Exercises client-initiated MQTT 5 re-authentication ([§4.12]) end-to-end against a real broker. |
| `TestStrict_MaximumQoS` | `strict_test.go` | A QoS above the broker's Maximum QoS fails before anything is sent, or is downgraded with WithQoSDowngrade; the connection survives. |
| `TestStrict_MaximumPacketSize` | `strict_test.go` | A packet above the broker's Maximum Packet Size fails before it is sent; the connection survives and smaller packets go through. |
| `TestStrict_ServerKeepAlive` | `strict_test.go` | The broker's Server Keep Alive replaces the keep-alive the client asked for: idle longer than the broker's limit, the client keeps the connection by pinging at the broker's interval. |
| `TestStrict_TopicAliasMaximum` | `strict_test.go` | Outbound topic aliases stay within the broker's Topic Alias Maximum: more topics than aliases all arrive, without a protocol error. |
| `TestStrict_UnavailableFeatures` | `strict_test.go` | Features the broker says it lacks fail before anything is sent, and a subscription works without a Subscription Identifier the broker does not accept. |
<!-- tests:end -->

The client's protocol state machine — Session Present handling, QoS 2
resume after PUBREC, replay order, duplicate handling, refused
subscriptions, protocol violations — is covered in the core module
against `internal/testbroker`, a scripted peer that can produce the
exact packet sequences and failures a real broker produces only by
chance. The `wire` package carries fuzz targets for the decoder
(`FuzzReadPacket`, `FuzzProperties`, `FuzzDecodeVarint`,
`FuzzPublishRoundTrip`, `FuzzValidUTF8`, `FuzzScanTopicName`), run
nightly by `.github/workflows/fuzz.yml`.

## Brokers

[`docker-compose.yml`](docker-compose.yml) starts three by default, and
two more with `--profile hivemq`:

| Service | Port on 127.0.0.1 | Used for |
|---|---|---|
| `mosquitto` (eclipse-mosquitto 2.1.2) | 1883 | the default broker; [`mosquitto.conf`](mosquitto.conf) allows anonymous clients |
| `emqx` (EMQX 6.3.1, anonymous) | 1884 | the second broker of the ClientGroup test; the whole suite can also run against it |
| `emqx-scram` (EMQX 6.3.1) | 1885, REST 18084 | enhanced authentication: `TestReauthenticate_SCRAM_EMQX` provisions a SCRAM-SHA-256 authenticator and user through the REST API with the key in [`emqx/api_key.bootstrap`](emqx/api_key.bootstrap) |
| `hivemq` (HiveMQ CE 2026.5, profile `hivemq`) | 1886 | the whole suite against HiveMQ's defaults (Receive Maximum 10, five topic aliases) |
| `hivemq-strict` (HiveMQ CE 2026.5, profile `hivemq`) | 1887 | `strict_test.go`: Maximum QoS 1, 2 KiB packets, Server Keep Alive 4 s, Receive Maximum 4, two topic aliases, no retain, wildcards, shared subscriptions or Subscription Identifiers ([`hivemq/strict.xml`](hivemq/strict.xml)) |

## Configuration

| Variable | Default | Description |
|---|---|---|
| `MQTT_BROKER` | `mqtt://127.0.0.1:1883` | Broker for every test. |
| `MQTT_BROKER_2` | `mqtt://127.0.0.1:1884` | Second broker (ClientGroup). |
| `MQTT_BROKER_SCRAM` | `mqtt://127.0.0.1:1885` | SCRAM-enabled EMQX (re-authentication). |
| `MQTT_BROKER_STRICT` | `mqtt://127.0.0.1:1887` | Broker announcing tight limits (`strict_test.go`). |
| `MQTTV5_SOAK` | `3s` | How long `TestSoak_DeliveryAcrossConnectionCuts` publishes (a Go duration); the nightly job runs minutes. |
| `MQTTV5_SOAK_TRACE` | unset | When set, the soak decodes both connections, prints the packet history of any message that did not arrive, arrived twice or failed, and tells a broker's fault from the client's (see [Broker notes](#broker-notes)). Costs memory in proportion to the messages sent; the nightly job sets it. |

Some assertions are broker-specific and skip elsewhere: the shared
subscription test checks mosquitto's round-robin distribution only on
port 1883.

## Running

```bash
docker compose -f conformance/docker-compose.yml up -d --wait
go -C conformance test -tags conformance -race -timeout 180s ./...

# The same suite against EMQX, with mosquitto as the second broker:
MQTT_BROKER=mqtt://127.0.0.1:1884 MQTT_BROKER_2=mqtt://127.0.0.1:1883 \
  go -C conformance test -tags conformance -race -timeout 180s ./...

docker compose -f conformance/docker-compose.yml down -v

# HiveMQ: the whole suite, and the strict-limit tests.
docker compose -f conformance/docker-compose.yml --profile hivemq up -d --wait
MQTT_BROKER=mqtt://127.0.0.1:1886 go -C conformance test -tags conformance -race ./...
```

CI runs the suite against mosquitto on every push and pull request
(`.github/workflows/conformance.yml`) and before every release. A nightly
job (`.github/workflows/nightly.yml`) runs it against mosquitto, EMQX and
HiveMQ, the strict-limit tests, a ten-minute soak per broker, and the
model-based tests with a million new seeds.

## External dependencies

Docker with Compose, and the images above. Go dependencies: the mqttv5
modules (through `replace` directives) and `codec/json`.

## Operations and troubleshooting

| Symptom | Check | Then |
|---|---|---|
| Tests skip | `docker compose -f conformance/docker-compose.yml ps` | Start the brokers; the skip message names the unreachable one. |
| `TestReauthenticate_SCRAM_EMQX` skips or fails provisioning | `curl -u conformancekey:conformanceSecret0123456789 http://127.0.0.1:18084/api/v5/authentication` | EMQX takes several seconds to start its API; wait for it. |
| A test fails on one broker only | the broker's logs (`docker compose … logs`) | Brokers differ in defaults (Receive Maximum, topic alias limits, shared-subscription strategy); a difference the spec allows belongs in the test as a skip or a per-broker expectation, not in the client. |
| `TestREADMEListsEveryTest` fails | the block it prints | Paste it into this README between the `tests` markers. |

## Broker notes

- **EMQX 6.2.0 loses QoS 2 messages across a session resume.** When a
  publisher's connection drops with a QoS 2 PUBLISH unacknowledged and
  the client resumes the session and resends it with DUP=1 ([§4.4]),
  EMQX 6.2.0 completes PUBREC/PUBREL/PUBCOMP with success codes but
  never delivers the message — between one in 900 and one in 1,900 QoS 2
  messages in five runs of
  `TestSoak_DeliveryAcrossConnectionCuts`, all with the client's
  packets as the specification requires (shown by
  `MQTTV5_SOAK_TRACE`). EMQX 6.3.1 does not lose any; use it or later.
- **EMQX 6.3.1 duplicates QoS 2 messages across a session takeover.**
  When a publisher reconnects a few milliseconds after its connection
  drops, EMQX sometimes processes the old connection's last QoS 2
  PUBLISH after the new connection has taken the session over. The
  client's DUP=1 resend under the same identifier then draws PUBREC
  0x00, and the message is delivered twice, which [MQTT-4.3.3-10]
  forbids. The same race leaves identifiers marked in use after their
  exchange ended, so a later message reusing one is refused with
  PUBREC 0x91 and never delivered. In `TestSoak_DeliveryAcrossConnectionCuts`
  this showed as up to one duplicate in 14,000 QoS 2 messages and a few
  refusals per ten minutes, each with the client's packets as the
  specification requires. With `MQTTV5_SOAK_TRACE` set the test checks
  the client's packets and logs these as broker faults instead of
  failing; mosquitto and HiveMQ show neither.
- **Shared subscriptions:** EMQX distributes randomly by default, so
  the round-robin assertion runs against mosquitto only.

## Not covered here

- Broker conformance: this suite tests the client.
- Malformed or adversarial broker traffic: the scripted-peer tests in
  the core module and the `wire` fuzz targets cover it, since a real
  broker does not misbehave on request.

## Extending

Add a `Test` function with a doc comment whose first sentence says what
it checks, in a file with the `//go:build conformance` tag. Use
`connect(t, opts...)` from `helpers.go`: it skips when the broker is
down, gives each client a unique ID, and disconnects at cleanup. Then
run `go -C conformance test -run TestREADMEListsEveryTest .` and paste
the block it prints.

## Security

The brokers are test fixtures. mosquitto, EMQX and HiveMQ accept
anonymous connections, and `emqx-scram`'s REST API key and SCRAM user
are fixed values committed in `emqx/api_key.bootstrap` and the tests.
Every port is published on 127.0.0.1 only, so other machines cannot
reach them; never expose them or reuse those credentials elsewhere, and
stop the brokers after a run (`docker compose -f
conformance/docker-compose.yml --profile hivemq stop`). The tests send
synthetic payloads only.

## Code map

| File | What |
|---|---|
| `helpers.go` | broker URLs, `connect`, subscriber helpers, the fault-injecting proxy with its packet trace, the SCRAM-SHA-256 client |
| `conformance_test.go` | connect, publish, subscribe, properties, retain, topic aliases, shared subscriptions, ClientGroup, QueuePublisher |
| `features_test.go` | one protocol feature per test: Receive Maximum, No Local, overlapping subscriptions, retain options, Subscription Identifiers, wills |
| `faults_test.go` | session loss, QoS 2 resumption, refused filters, QoS 1 replay across a dropped connection, the connection-cut soak |
| `reauth_test.go` | SCRAM-SHA-256 re-authentication against EMQX |
| `strict_test.go` | a broker announcing tight limits |
| `readme_test.go` | generates and checks the test list above |
| `docker-compose.yml`, `mosquitto.conf`, `emqx/` | the brokers |

<!-- MQTT v5.0 citations -->

[§3.3.2.3.8]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901117 "3.3.2.3.8 Subscription Identifier"
[§3.3.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901120 "3.3.4 PUBLISH Actions"
[§3.8.2.1.2]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901166 "3.8.2.1.2 Subscription Identifier"
[§3.8.3.1]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169 "3.8.3.1 Subscription Options"
[§3.8.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901170 "3.8.4 SUBSCRIBE Actions"
[§4.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901238 "4.4 Message delivery retry"
[§4.12]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256 "4.12 Enhanced authentication"
