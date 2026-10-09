# transport/ws

MQTT over WebSocket (`ws://`, `wss://`) for
[`github.com/ashtonian/mqttv5`](../../README.md), as a separate module so
the core stays standard-library only.

```go
import "github.com/ashtonian/mqttv5/transport/ws"

cli, err := mqttv5.New(
    mqttv5.WithBroker("wss://broker.example.com/mqtt"),
    mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{})),
)
```

| | |
|---|---|
| Module | `github.com/ashtonian/mqttv5/transport/ws` |
| Depends on | `github.com/ashtonian/mqttv5`, [`github.com/gobwas/ws`](https://github.com/gobwas/ws) v1.4.0 |
| Spec | MQTT v5.0 [§6] (WebSocket transport), RFC 6455 |

## Overview

`DialFunc` returns a function for `mqttv5.WithDialFunc`. Each connection
attempt performs the HTTP upgrade with subprotocol `mqtt`, then carries
the MQTT byte stream in WebSocket binary frames:

- **Writes.** Every MQTT packet the client writes goes out as one masked
  binary frame, in a single write to the socket.
- **Reads.** Frames are reassembled into the byte stream the MQTT
  decoder reads, however the broker fragments them. Frames the broker
  sends right behind its handshake response are not lost.
- **Control frames.** A Ping is answered with a Pong carrying its
  payload; a Close is answered with a Close and ends the connection
  (the client's supervisor then reconnects). Pongs and close replies
  are written under the same lock as data frames, so they never split
  one.
- **Text frames** from the broker close the connection with
  `ErrTextFrame`: MQTT travels only in binary frames [MQTT-6.0.0-1].
- **TLS.** `wss://` verifies the broker against the system roots unless
  `DialOpts.TLSConfig` says otherwise, like `mqtts://`; the URL's host is
  the server name when `TLSConfig.ServerName` is empty.

`mqttv5.New` rejects `ws://` and `wss://` broker URLs unless
`WithDialFunc` is set.

## Configuration

`DialOpts`; the zero value is valid.

| Field | Type | Default | Description |
|---|---|---|---|
| `TLSConfig` | `*tls.Config` | system roots, server name from the URL | TLS for `wss://`; ignored for `ws://`. |
| `HTTPHeaders` | `http.Header` | none | Added to the upgrade request: auth tokens, routing headers. |
| `Subprotocols` | `[]string` | `["mqtt"]` | `Sec-WebSocket-Protocol` values offered. |
| `HandshakeTimeout` | `time.Duration` | `DefaultHandshakeTimeout` (10 s) | Bound on the upgrade handshake. The client's connect timeout (`mqttv5.WithConnectTimeout`) bounds the whole attempt as well. |
| `CloseFrameTimeout` | `time.Duration` | `DefaultCloseFrameTimeout` (1 s) | Bound on the Close frame sent when the connection closes. `Close` skips the frame when a write is in progress, which may be stalled behind a broker that stopped reading, and closes the socket at once. |
| `NetDial` | `func(ctx, network, addr string) (net.Conn, error)` | `net.Dialer` | Replaces the TCP dial, e.g. for a proxy. |

The URL's path and query go into the upgrade request
(`wss://host/mqtt`); a missing port means 80 for `ws://` and 443 for
`wss://`.

## External dependencies

| Module | Version | Used for |
|---|---|---|
| `github.com/gobwas/ws` | v1.4.0 | upgrade handshake, frame headers, control-frame replies |

## API

| Symbol | Description |
|---|---|
| `DialFunc(DialOpts) transport.DialFunc` | For `mqttv5.WithDialFunc`. |
| `Dial(ctx, *url.URL, DialOpts) (transport.Conn, error)` | One upgrade, outside the client. |
| `DialOpts` | See Configuration. |
| `DefaultHandshakeTimeout` | 10 s. |
| `DefaultCloseFrameTimeout` | 1 s. |
| `ErrTextFrame` | The broker sent a text frame. |
| `Conn` | The `transport.Conn` that `Dial` returns; treat it as opaque. |

## Build, run, test

```bash
go -C transport/ws test -race ./...
```

The tests run against an in-process WebSocket server: round trips,
byte-at-a-time reads, ping/pong under concurrent writes, broker Close,
text frames, closing during a stalled write, and TLS verification. [`examples/ws`](../../examples/ws)
connects to a real broker.

## Operations

| Symptom | Check | Then |
|---|---|---|
| `dial … : unexpected HTTP response status` | the URL path and the broker's WebSocket listener | Most brokers serve MQTT on a path such as `/mqtt`. |
| `x509: certificate signed by unknown authority` | the broker's certificate chain | Add its CA to `DialOpts.TLSConfig.RootCAs`. |
| Connections drop every few minutes behind a proxy | the proxy's idle timeout and whether it pings | Pings are answered; lower `mqttv5.WithKeepAlive` below the proxy's idle timeout. |
| The broker logs connections ending without a Close frame | whether the client was writing when it closed | Expected when a write was in progress or the broker stopped reading: `Close` does not wait for a write, and sends the Close frame for at most `CloseFrameTimeout`. |
| `ErrTextFrame` | the broker or a proxy injecting text frames | MQTT over WebSocket allows binary frames only; fix the peer. |

Connection-level health and counters come from the client
(`Client.Stats()`, the lifecycle callbacks); this package logs nothing.

## Security

- `wss://` verifies the broker's certificate by default. Do not set
  `InsecureSkipVerify` outside tests.
- `HTTPHeaders` may carry credentials; they are sent on every connection
  attempt and never logged.
- Client frames are masked as RFC 6455 requires, with a fresh mask per
  frame.

## Code map

| File | What |
|---|---|
| `ws.go` | `DialFunc`, `Dial`, `Conn` (framing, control frames) |
| `ws_test.go` | dialing, round trips, TLS |
| `control_test.go` | ping/pong, close, text frames, concurrent writes |

<!-- MQTT v5.0 citations -->

[§6]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285 "6 Using WebSocket as a network transport"
[MQTT-6.0.0-1]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285 "6 Using WebSocket as a network transport"
