# mqttv5 examples

Runnable programs for [`github.com/ashtonian/mqttv5`](../README.md), one
usage pattern each, plus the README's code snippets kept as compiled
code. They share `examples/go.mod`, so the dependencies they need
(`codec/json`, `transport/ws`, `store/file`, `queue/file`) are pulled in
here and never by the core module.

## Overview

| Folder | What it shows | Broker |
|---|---|---|
| `basic/` | Connect, subscribe through a channel, publish QoS 1, shut down on SIGINT | MQTT on 1883 |
| `typed/` | `Typed[T]` with `codec/json`: typed publish and subscribe | MQTT on 1883 |
| `reconnect/` | The supervisor across a broker restart: lifecycle callbacks, QoS 1 replay, re-subscription | MQTT on 1883; restart it while running |
| `group/` | `ClientGroup` across two brokers: broadcast publish, merged subscribe, per-member results | two brokers |
| `stats/` | `WithStats` counters printed every second | MQTT on 1883 |
| `disconnect/` | `DisconnectWith`: reason code, reason string, Session Expiry override | MQTT on 1883 |
| `oauth/` | `WithConnectPacketBuilder` refreshing a bearer token on every connection attempt | MQTT on 1883 (accepts any password) |
| `ws/` | MQTT over WebSocket with `transport/ws` | a WebSocket listener |
| `readme/` | Every Go snippet of the repository README, as code that compiles; `TestREADMESnippets` fails when the README and these files disagree | none |

## Configuration

| Variable | Used by | Default | Description |
|---|---|---|---|
| `MQTT_BROKER` | all but `group` | `mqtt://127.0.0.1:1883` (`ws`: `ws://127.0.0.1:8083/mqtt`) | Broker URL. |
| `MQTT_BROKERS` | `group` | `mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884` | Comma-separated broker URLs, one group member each. |

## External dependencies

A broker. Any MQTT v5 broker works; the commands below use mosquitto.
Go module dependencies are in `examples/go.mod`: the mqttv5 modules
through `replace` directives to this repository.

## Build, run, test

```bash
# A local broker:
docker run -d --name mq -p 1883:1883 eclipse-mosquitto:2.1.2-alpine

go -C examples run ./basic
go -C examples run ./typed
go -C examples run ./reconnect     # then: docker restart mq
go -C examples run ./stats
go -C examples run ./disconnect
go -C examples run ./oauth

# Two brokers for the group example:
docker run -d -p 1884:1883 eclipse-mosquitto:2.1.2-alpine
go -C examples run ./group

# WebSocket: a mosquitto with a "listener 8083" + "protocol websockets"
# configuration, then:
MQTT_BROKER=ws://127.0.0.1:8083/mqtt go -C examples run ./ws

# Compile everything and check the README snippets:
go -C examples vet ./...
go -C examples test ./...
```

Recent mosquitto images refuse anonymous clients unless the
configuration allows them; the repository's
[`conformance/mosquitto.conf`](../conformance/mosquitto.conf) does.

## Plugging in a different codec

`Typed[T]` accepts any `mqttv5.Codec[T]`:

- [`codec/json`](../codec/json) — standard library `encoding/json`
- [`codec/msgpack`](../codec/msgpack) — `github.com/vmihailenco/msgpack/v5`

A custom codec is any type with `Encode(T) ([]byte, error)` and
`Decode([]byte) (T, error)`; it needs nothing from this repository.

## Operations

| Symptom | Check | Then |
|---|---|---|
| `connection refused` | `docker ps`; the port in `MQTT_BROKER` | Start the broker or point `MQTT_BROKER` at one. |
| `ErrConnectRefused` (0x87 Not authorized) | the broker's `allow_anonymous` setting | Allow anonymous clients or add credentials to the example. |
| `ws` example: `unexpected HTTP response status` | the broker's WebSocket listener and path | Configure a `protocol websockets` listener; most brokers serve it on `/mqtt`. |
| `TestREADMESnippets` fails | the README snippet it names | Change the README and the matching file in `readme/` together. |

## Security

The examples connect without TLS to a local broker. For anything else
use `mqtts://` or `wss://` (verified against the system roots by
default) and real credentials; `oauth/` shows where per-attempt
credentials belong. The OAuth token source in that example is a stub.

## Code map

| Path | What |
|---|---|
| `*/main.go` | one program per folder |
| `readme/*.go` | README snippets, grouped by README section |
| `readme/readme_test.go` | `TestREADMESnippets` |
