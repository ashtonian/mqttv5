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
attempt performs the HTTP upgrade offering subprotocol `mqtt`, fails
with `ErrNoSubprotocol` unless the broker selects one
[MQTT-6.0.0-4], then carries the MQTT byte stream in WebSocket binary
frames:

- **Writes.** Each write from the client goes out as one masked binary
  frame, in a single write to the socket. The client gathers a packet,
  or a batch of queued packets, into one write of up to 16 KiB, so a
  packet is split across frames only when it is larger than that.
- **Reads.** Frames are reassembled into the byte stream the MQTT
  decoder reads, however the broker fragments them, through a 4 KiB
  buffer: a stream of small frames costs a socket read per buffer, not
  two per frame. Frames the broker sends right behind its handshake
  response are not lost.
- **Control frames.** A Ping is answered with a Pong carrying its
  payload. A broker's Close is answered with a Close echoing its status
  and ends the stream: `io.EOF` for Normal Closure or no status, a
  `*CloseError` carrying any other status and reason (the client logs
  it, and its supervisor reconnects). Pongs and Close frames are written
  under the same lock as data frames, so they never split one.
- **Closing.** `Close` runs the closing handshake (RFC 6455 §7.1): it
  sends a Close with Normal Closure (1000) and returns; the TCP
  connection closes when the broker answers or closes it, as RFC 6455
  §7.1.1 has the server close first, or after `CloseFrameTimeout`.
  Nothing is sent after a Close frame, Pongs included, and Read and
  Write then report `net.ErrClosed`.
- **Protocol errors.** When the broker breaks MQTT, the client sends
  its DISCONNECT and then `Abort`s: a Close with Protocol Error (1002)
  naming the MQTT reason code, and the TCP connection closes at once.
  A frame that breaks RFC 6455 (a masked or reserved one) gets a Close
  with 1002, and a text frame one with Unsupported Data (1003) and
  `ErrTextFrame`: MQTT travels only in binary frames [MQTT-6.0.0-1].
- **TLS.** `wss://` verifies the broker against the system roots unless
  `DialOpts.TLSConfig` says otherwise, like `mqtts://`; the URL's host is
  the server name when `TLSConfig.ServerName` is empty.
  `DialOpts.TLSConfigFunc` instead supplies the config on each `wss://`
  dial, so a long-lived `DialFunc` can rotate trusted roots or client
  certificates.
- **TLS session resumption.** The `wss://` dials of one `DialFunc` share
  an in-memory session cache, so a client's reconnects resume the TLS
  session, for a config that sets no `ClientSessionCache` and presents
  no client certificate (a resumed session does not present it again,
  which would delay a rotation). `Dial` on its own resumes only through
  the config's own `ClientSessionCache`. The caller's config is never
  modified. See [Certificate management](../../docs/security.md#certificate-management).

`mqttv5.New` rejects `ws://` and `wss://` broker URLs unless
`WithDialFunc` is set.

## Configuration

`DialOpts`; the zero value is valid.

| Field | Type | Default | Description |
|---|---|---|---|
| `TLSConfig` | `*tls.Config` | system roots, server name from the URL | TLS for `wss://`; ignored for `ws://`. Excludes `TLSConfigFunc`. |
| `TLSConfigFunc` | `transport.TLSConfigFunc` | none | Called on each `wss://` dial, before the TCP dial and bounded by the dial's `ctx`, for the config `TLSConfig` would give. An error fails the dial wrapped; a nil config fails it with `transport.ErrNilTLSConfig`. Not called for `ws://`. Setting both it and `TLSConfig` fails `Dial` with `ErrConflictingTLSConfig`. |
| `HTTPHeaders` | `http.Header` | none | Added to the upgrade request: auth tokens, routing headers. |
| `Subprotocols` | `[]string` | `["mqtt"]` | `Sec-WebSocket-Protocol` values offered. |
| `HandshakeTimeout` | `time.Duration` | `DefaultHandshakeTimeout` (10 s) | Bound on the upgrade handshake. The client's connect timeout (`mqttv5.WithConnectTimeout`) bounds the whole attempt as well. |
| `CloseFrameTimeout` | `time.Duration` | `DefaultCloseFrameTimeout` (1 s) | Bound on the closing handshake: writing the Close frame, then waiting for the broker's before the TCP connection closes. `Close` skips the frame when a write is in progress, which may be stalled behind a broker that stopped reading, and closes the socket at once. |
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
| `ErrNoSubprotocol` | The broker's handshake selected no subprotocol: not an MQTT endpoint. |
| `ErrConflictingTLSConfig` | `DialOpts` sets both `TLSConfig` and `TLSConfigFunc`. |
| `CloseError` | The broker's Close status (`Code`) and `Reason`, when the status is not Normal Closure. |
| `Conn` | The `transport.Conn` that `Dial` returns; treat it as opaque. |
| `(*Conn).ConnectionState() tls.ConnectionState` | The TLS handshake of a `wss://` connection; the zero state for `ws://`. Implements `transport.ConnectionStater`, so the client reports it in `ConnackInfo.TLS`. |
| `(*Conn).Abort(reason byte) error` | Close with Protocol Error naming `reason`, then the TCP connection at once. Implements `transport.Aborter`, which the client calls on a broker's protocol violation. |

## Build, run, test

```bash
go -C transport/ws test -race ./...
```

The tests run against an in-process WebSocket server: round trips,
byte-at-a-time and buffered reads, ping/pong under concurrent writes,
the closing handshake and its timeout, each broker Close status, text
frames and RFC 6455 violations, `Abort`, nothing sent after Close,
closing during a stalled write, the subprotocol check, TLS
verification, TLS state, the per-dial `TLSConfigFunc` and session
resumption. [`examples/ws`](../../examples/ws) runs against a real
broker, and the conformance suite runs over `ws://` and `wss://`
against EMQX ([`conformance`](../../conformance)).

## Operations

| Symptom | Check | Then |
|---|---|---|
| `dial … : unexpected HTTP response status` | the URL path and the broker's WebSocket listener | Most brokers serve MQTT on a path such as `/mqtt`. |
| `the broker selected no WebSocket subprotocol` | the URL points at an MQTT listener; `DialOpts.Subprotocols` | The endpoint is not MQTT over WebSocket, or does not accept the offered names: most brokers accept `mqtt`. |
| `broker closed the WebSocket with status 1008: …` (logged as a connection error) | the broker's logs for the client, its authorization rules | The broker refused the connection; the reason is the broker's. Other statuses are RFC 6455's (1001 the broker going away, 1011 an internal error). |
| `x509: certificate signed by unknown authority` | the broker's certificate chain | Add its CA to `DialOpts.TLSConfig.RootCAs`, or to the roots `TLSConfigFunc` returns. |
| `mqttv5/transport/ws: DialOpts sets both TLSConfig and TLSConfigFunc` | `DialOpts` | Set one: `TLSConfigFunc` for a config per dial, `TLSConfig` for a fixed one. |
| Connections drop every few minutes behind a proxy | the proxy's idle timeout and whether it pings | Pings are answered; lower `mqttv5.WithKeepAlive` below the proxy's idle timeout. |
| The broker logs connections ending without a Close frame | whether the client was writing when it closed | Expected when a write was in progress or the broker stopped reading: `Close` does not wait for a write, and sends the Close frame for at most `CloseFrameTimeout`. |
| The broker logs a Close with 1002 | the client's log for `broker protocol violation` at the same time | The client found the broker breaking MQTT (the log names the packet and reason) and aborted after its DISCONNECT. |
| `ErrTextFrame` | the broker or a proxy injecting text frames | MQTT over WebSocket allows binary frames only; fix the peer. |

Connection-level health and counters come from the client
(`Client.Stats()`, the lifecycle callbacks); this package logs nothing.

## Security

- `wss://` verifies the broker's certificate by default. Do not set
  `InsecureSkipVerify` outside tests.
- `HTTPHeaders` may carry credentials; they are sent on every connection
  attempt and never logged.
- Client frames are masked as RFC 6455 requires, with a fresh mask per
  frame from `math/rand`'s runtime-seeded ChaCha8 source.

## Code map

| File | What |
|---|---|
| `ws.go` | `DialFunc`, `Dial`, `Conn` (framing, control frames) |
| `ws_test.go` | dialing, round trips, TLS |
| `control_test.go` | ping/pong, the closing handshake, `Abort`, broker Close statuses, text frames and RFC 6455 violations, concurrent writes, buffered reads |

<!-- MQTT v5.0 citations -->

[§6]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285 "6 Using WebSocket as a network transport"
[MQTT-6.0.0-1]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285 "6 Using WebSocket as a network transport"
[MQTT-6.0.0-4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285 "6 Using WebSocket as a network transport"
