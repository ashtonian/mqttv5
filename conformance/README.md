# conformance

Broker-backed integration tests covering the MQTT v5 features the
client implements. Gated behind the `conformance` build tag so the
default `go test ./...` does not require a broker.

## What it tests

| Area | Tests |
|---|---|
| Connect / Disconnect | basic, CleanStart, credentials |
| Publish | QoS 0 / 1 / 2 ack handshakes |
| Pub→Sub round trip | QoS 0 + QoS 1 |
| Wildcards | `+` and `#` |
| Properties | UserProperty + ContentType round-trip |
| Retain | retained delivery to a late subscriber |
| Queue subscribe | N messages drain in order |
| Typed[T] | JSON codec end-to-end |
| Large payload | 32 KiB through the broker |
| Session | resume with CleanStart=false + SessionExpiry |
| ClientGroup | publish fan-out to two brokers |

The reconnect+replay paths are covered by the in-process fakeBroker
tests in the main module (no real broker bounce required).

## Why a build tag

Conformance tests are slow (real network, real broker). They are
opt-in so CI can run the unit suite in seconds and the conformance
suite on a dedicated job.

## Running

Spin up the brokers first:

```bash
docker compose -f conformance/docker-compose.yml up -d
```

Then run the suite:

```bash
go -C conformance test -tags conformance -race -v -timeout 120s
```

Cleanup:

```bash
docker compose -f conformance/docker-compose.yml down
```

## Two brokers

`docker-compose.yml` brings up `mosquitto` (port 1883) and `emqx`
(port 1884). The ClientGroup test requires both; other tests use just
mosquitto. To override:

```bash
MQTT_BROKER=mqtt://broker-a:1883 \
MQTT_BROKER_2=mqtt://broker-b:1883 \
  go -C conformance test -tags conformance -v
```

If a broker is unreachable, tests that need it skip rather than fail
— look for `--- SKIP` lines in the output.

## Last verified run

Against eclipse-mosquitto:2.0 on 2026-05-20 (Apple M2 Pro, race
detector on, strengthened deep-verification assertions):

```
--- PASS: TestConnect_Disconnect
--- PASS: TestConnect_RoundTripsThroughBroker
--- PASS: TestConnect_CleanStartWipesPriorSubscription
--- PASS: TestConnect_WithCredentials
--- PASS: TestPublish_QoS0_DeliveredToSubscriber
--- PASS: TestPublish_QoS1_DeliveredAndAcked
--- PASS: TestPublish_QoS2_ExactlyOnce
--- PASS: TestSubscribe_PlusWildcard_MatchesOneLevel
--- PASS: TestSubscribe_HashWildcard_MatchesParentAndChildren
--- PASS: TestPubSub_AllPublishProperties
--- PASS: TestPublish_Retain_DeliveredToLateSubscriber
--- PASS: TestSubscribeQueue_OrderedDelivery
--- PASS: TestTypedJSON_FullStructEquality
--- PASS: TestPublish_LargePayload_ByteEquality
--- PASS: TestSession_QueuedPublishesDeliveredOnResume
--- PASS: TestTopicAlias_OutboundReducesBytes
--- PASS: TestUnsubscribe_StopsDelivery
--- PASS: TestSubscribe_MultipleHandlersDispatch
--- SKIP: TestClientGroup_PublishFanOutToBothBrokers   (emqx unavailable)
ok  github.com/ashtonian/mqttv5/conformance  4.761s
```

18/18 mosquitto-only tests pass. The ClientGroup test skips when
the secondary broker isn't reachable.

These tests do **deep** verification: every publish has a
subscriber confirming the payload bytes (or struct equality) arrive;
every property is checked individually (not just counted); QoS
levels are exercised end-to-end (subscribe at the matching QoS so
the broker doesn't silently downgrade); retain-then-clear has a
negative subscriber check; session resume actually receives a
publish queued while offline; multi-handler dispatch is run with
the race detector to verify the Message refcount path.

The first run against the strengthened suite caught a real
client-side race where multiple matching handlers shared one
Message and could call Ack concurrently — fixed by adding a
refcount to Message so only the final Ack releases the underlying
wire.Publish.

## What's *not* covered

- MQTT 5.0 spec sections we don't implement yet (e.g., multi-step
  AUTH exchange, Shared Subscriptions).
- Broker-side conformance — there are existing test suites for that
  (Eclipse Paho Testing Project, OASIS conformance statements). This
  suite is strictly client-side.
- Adversarial / malformed-packet tests — see the `wire/` round-trip
  + fuzz tests in the main module.

## Extending

Add a new test function in `conformance_test.go` — every test starts
with:

```go
func TestSomeFeature(t *testing.T) {
    cli := connect(t, /* options */)
    // ...
}
```

`connect(t)` handles broker availability, ClientID uniqueness, and
defers Disconnect via t.Cleanup. See `helpers.go`.
