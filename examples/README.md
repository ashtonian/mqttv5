# mqttv5 examples

Runnable programs for [`github.com/ashtonian/mqttv5`](../README.md), one
usage pattern each, plus the README's code snippets kept as compiled
code. They share `examples/go.mod`, so the dependencies they need
(`codec/json`, `transport/ws`, `store/file`, `queue/file`) are pulled in
here and never by the core module.

## Overview

| Folder | What it shows | Broker |
|---|---|---|
| `readme/quickstart/` | The README's quick start: connect, subscribe through a channel, publish QoS 1 | MQTT on `localhost:1883` |
| `subscribe/` | Channel, queue and callback subscriptions; a slow channel reader loses QoS 0 messages, never QoS 1; a `DropOldest` queue keeps the freshest | MQTT on 1883 |
| `typed/` | `SubscribeAs[T]` and `PublishValue` with `codec/json`: two payload types on one client, and a payload that does not decode | MQTT on 1883 |
| `request/` | MQTT 5 request/response: a responder on a shared subscription, a requester matching replies by Correlation Data | MQTT on 1883 |
| `durable/` | `QueuePublisher` on `queue/file` with the session in `store/file`: stores while no broker is up, delivers in order once one is, and a restart neither loses nor repeats a message | MQTT on 1883; stop and start it, or the program, while running |
| `reconnect/` | The supervisor across a broker restart: lifecycle callbacks, QoS 1 replay, re-subscription | MQTT on 1883; restart it while running |
| `failover/` | `WithBrokers`: connects to whichever broker accepts, moves to the next when it drops, `ConnackInfo.Broker` names it | two brokers; stop the one it reports |
| `pool/` | `WithPublisherPool`: the same burst of QoS 1 publishes over the main connection alone and through a pool of four | MQTT on 1883 |
| `group/` | A central service over two independent clusters, acknowledged in the application as a device shadow or command RPC is: each desired-state update to both clusters, the devices' replies (their reported state) from either on one merged subscription, resent until the device replies; devices act once per command | two brokers |
| `bridge/` | An at-least-once bridge: acks the source only after the destination's PUBACK, resumes the source's session after a restart | two brokers; stop either, or the bridge, while running |
| `shared/` | Three workers sharing `$share/workers/jobs/#`, each a durable consumer | MQTT on 1883 |
| `stats/` | `WithStats` counters printed every second | MQTT on 1883 |
| `disconnect/` | `DisconnectWith`: reason code, reason string, Session Expiry override | MQTT on 1883 |
| `oauth/` | `WithConnectPacketBuilder` refreshing a bearer token on every connection attempt | MQTT on 1883 (accepts any password) |
| `ws/` | MQTT over WebSocket with `transport/ws` | a WebSocket listener (EMQX's, on 8083, in `conformance/docker-compose.yml`) |
| `readme/` | Every Go snippet of the repository README and `docs/*.md` guides, as code that compiles; `TestREADMESnippets` fails when the documentation and these files disagree | none |

Each program runs until it is done or interrupted (Ctrl-C), disconnects
cleanly, and exits 1 with the error on failure.

## Configuration

| Variable | Used by | Default | Description |
|---|---|---|---|
| `MQTT_BROKER` | all but `group`, `failover`, `bridge` and `readme/quickstart` | `mqtt://127.0.0.1:1883` (`ws`: `ws://127.0.0.1:8083/mqtt`) | Broker URL. |
| `MQTT_BROKERS` | `group`, `failover`, `durable` | `mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884` (`durable`: `MQTT_BROKER`) | Comma-separated broker URLs: one cluster (group member) each for `group`, the failover list for the others. |
| `MQTT_SOURCE` | `bridge` | `mqtt://127.0.0.1:1883` | Broker the bridge copies from. |
| `MQTT_DESTINATION` | `bridge` | `mqtt://127.0.0.1:1884` | Broker the bridge copies to. |
| `MQTT_DATA_DIR` | `durable` | `$TMPDIR/mqttv5-durable` | Directory of the queue and session files; delete it to start over. |

## External dependencies

A broker. Any MQTT v5 broker works; the commands below use mosquitto.
Go module dependencies are in `examples/go.mod`: the mqttv5 modules
through `replace` directives to this repository.

## Build, run, test

```bash
# A local broker:
docker run -d --name mq -p 1883:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf

go -C examples run ./readme/quickstart
go -C examples run ./typed
go -C examples run ./reconnect     # then: docker restart mq
go -C examples run ./stats
go -C examples run ./disconnect
go -C examples run ./oauth

go -C examples run ./subscribe
go -C examples run ./request
go -C examples run ./shared
go -C examples run ./pool
go -C examples run ./durable       # then: docker stop mq; docker start mq

# Two brokers for the multi-broker examples:
docker run -d --name mq2 -p 1884:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
go -C examples run ./group
go -C examples run ./failover      # then: docker stop the broker it reports
go -C examples run ./bridge        # then: stop mq2, or the bridge, and start it again

# WebSocket: a mosquitto with a "listener 8083" + "protocol websockets"
# configuration, then:
MQTT_BROKER=ws://127.0.0.1:8083/mqtt go -C examples run ./ws

# Compile everything and check the README snippets:
go -C examples vet ./...
go -C examples test ./...

# Run every example against the conformance brokers (mosquitto on 1883,
# EMQX on 1884 with WebSocket on 8083), as CI does: each must finish, or
# stop cleanly when interrupted after a few seconds, with exit status 0.
docker compose -f conformance/docker-compose.yml up -d --wait
go -C examples test -tags smoke -v -run TestExamplesRun .
```

Recent mosquitto images refuse anonymous clients unless the
configuration allows them; the image's `/mosquitto-no-auth.conf`, used
above, does, as does the repository's
[`conformance/mosquitto.conf`](../conformance/mosquitto.conf).

## Plugging in a different encoding

`WithEncoding` (or `SubEncoding` for one subscription) accepts any
`mqttv5.Encoding`:

- [`codec/json`](../codec/json) — standard library `encoding/json`
- [`codec/msgpack`](../codec/msgpack) — `github.com/vmihailenco/msgpack/v5`

A custom encoding is any type with `ContentType() string`,
`Marshal(any) ([]byte, error)` and `Unmarshal([]byte, any) error`; it
needs nothing from this repository.

## Operations

| Symptom | Check | Then |
|---|---|---|
| `connection refused` | `docker ps`; the port in `MQTT_BROKER` | Start the broker or point `MQTT_BROKER` at one. |
| `ErrConnectRefused` (0x87 Not authorized) | the broker's `allow_anonymous` setting | Allow anonymous clients or add credentials to the example. |
| `ws` example: `unexpected HTTP response status` | the broker's WebSocket listener and path | Configure a `protocol websockets` listener; most brokers serve it on `/mqtt`. |
| `TestREADMESnippets` fails | the documentation snippet it names | Change the Markdown and the matching file in `readme/` together. |
| `failover` never moves | which broker `UP` names; whether you stopped that one | Stop the broker it reports; with one broker up it stays there. |
| `durable` or `bridge` repeats old messages at start | `MQTT_DATA_DIR`, and the brokers' sessions for the example's ClientID | They resume earlier runs on purpose; delete `MQTT_DATA_DIR` or let the sessions expire to start over. |
| `pool` shows no gain | the round trip to the broker; its Receive Maximum (`ServerInfo().ReceiveMaximum`) and threading | A pool widens the window of unacknowledged QoS 1 publishes, which limits publishing only when the round trip is long or the window small. Against a broker on the same host or LAN the broker's own processing is the limit, and a single-threaded broker (mosquitto) gains little. |

## Security

The examples connect without TLS to a local broker. For anything else
use `mqtts://` or `wss://` (verified against the system roots by
default) and real credentials; `oauth/` shows where per-attempt
credentials belong. The OAuth token source in that example is a stub.

## Code map

| Path | What |
|---|---|
| `*/main.go` | one program per folder |
| `readme/readme.go`, `readme/brokers.go` | Documentation snippets that show no imports; `brokers.go` holds those of Multiple brokers and Recipes |
| `readme/typed.go`, `readme/queue.go`, `readme/websocket.go` | snippets whose import block the documentation shows, so each keeps a file of its own |
| `readme/quickstart/main.go` | the quick start, a program of its own |
| `readme/readme_test.go` | `TestREADMESnippets` |
