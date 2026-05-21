# mqttv5 examples

Each subdirectory is a runnable `main` showing a different usage
pattern. They share `examples/go.mod` so the **main mqttv5 module
stays stdlib-only** — any additional dependencies (codec/json,
codec/msgpack) are pulled in here, not in the core.

## Layout

| Folder | What it shows | Extra deps |
|---|---|---|
| `basic/` | Connect, subscribe via channel, publish QoS 1 | none |
| `typed/` | `Typed[T]` wrapper with `codec/json` | `codec/json` |
| `group/` | `ClientGroup` fan-out across two brokers | none |
| `reconnect/` | `OnConnectionUp/Down` callbacks while the broker bounces | none |

## Quickstart

```bash
# Run a local broker:
docker run -d --name mq -p 1883:1883 eclipse-mosquitto

# Run any example:
go -C examples run ./basic
go -C examples run ./typed
go -C examples run ./reconnect
```

For the group example, run two brokers on different ports:

```bash
docker run -d -p 1883:1883 eclipse-mosquitto
docker run -d -p 1884:1883 eclipse-mosquitto
MQTT_BROKERS=mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884 \
  go -C examples run ./group
```

## Plugging in a different codec

The `Typed[T]` wrapper accepts any `mqttv5.Codec[T]`. Use the shipped
implementations:

- `codec/json` — stdlib `encoding/json` (no external deps)
- `codec/msgpack` — `github.com/vmihailenco/msgpack/v5`

Or implement your own — see `codec/json/json.go` for the shape
(`Encode(T) ([]byte, error)` + `Decode([]byte) (T, error)`). Custom
codecs do not require any code outside your module.
