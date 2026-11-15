# transport/quic

MQTT over QUIC (`mqtt-quic://`) in Single Stream mode for
[`github.com/ashtonian/mqttv5`](../../README.md), as a separate module so
the core stays standard-library only.

```go
import "github.com/ashtonian/mqttv5/transport/quic"

cli, err := mqttv5.New(
    mqttv5.WithBroker("mqtt-quic://broker.example.com:14567"),
    mqttv5.WithDialFunc(quic.DialFunc(quic.DialOpts{})),
)
```

| | |
|---|---|
| Module | `github.com/ashtonian/mqttv5/transport/quic` |
| Depends on | `github.com/ashtonian/mqttv5`, [`github.com/quic-go/quic-go`](https://github.com/quic-go/quic-go) v0.65.0 |
| Spec | "MQTT Over QUIC — Single Stream Mode Version 1.0", OASIS MQTT TC Committee Note [Draft 04] (24 September 2026); QUIC: RFC 9000, RFC 9001 |
| Brokers | EMQX (`quic:default` listener, port 14567), NanoMQ |

## Overview

`DialFunc` returns a function for `mqttv5.WithDialFunc`. Each connection
attempt makes one QUIC connection to the broker, offering ALPN `mqtt`,
and opens one bidirectional stream that carries the MQTT byte stream
unchanged, packet for packet as over TCP ([draft 5.2]):

- **One stream.** The client opens the stream; its transport parameters
  allow the broker no streams of its own, which Single Stream mode
  forbids. A broker that selects any ALPN but `mqtt` fails the dial with
  `ErrALPN`.
- **Reads and writes** go to the stream; quic-go packs consecutive
  writes into packets. Head-of-line blocking is the same as over TCP.
- **Graceful close** ([draft 6.3.1]). `Close` sends a FIN after the last
  write, the client's DISCONNECT, and returns. In the background the
  connection then waits for the broker's FIN, discarding what the broker
  still sends, and closes the QUIC connection with code 0; after
  `CloseTimeout` without the FIN it resets the stream and closes.
  `Close` during a write in progress (stalled behind a broker that
  stopped reading) resets the stream and closes the connection at once
  instead: a FIN would end the stream on a cut-off packet.
- **Protocol errors** ([draft 6.7.2]). When the client finds a malformed
  packet or a protocol error in what the broker sent, it writes a
  DISCONNECT with reason 0x81 or 0x82 and calls `Conn.Abort` instead of
  `Close` (`transport.Aborter`). Abort resets the stream in both
  directions with QUIC error code `0x300 + reason` (0x381, 0x382). It
  uses RESET_STREAM_AT, so a broker that supports reliable stream resets
  still receives the DISCONNECT. The QUIC connection closes with the
  same code once the broker has closed it, or at `CloseTimeout`
  ([draft 6.7.2.3]).
- **Errors from the broker** ([draft 6.3.3]). A stream reset or
  connection close from the broker with code `0x300 + reason` comes back
  from `Read` and `Write` as a `*PeerError` carrying the MQTT reason
  code.
- **Session takeover** ([draft 6.6.4]). When the broker reports Session
  Taken Over, as a DISCONNECT with reason 0x8E (which the client passes
  on through `transport.DisconnectObserver`) or as QUIC code 0x38E, the
  Dialer waits `TakeoverBackoff` (5 s) before its next dial on either
  transport. This keeps two connections with the same client ID from
  taking the session from each other in a tight loop.
- **Fallback to TLS** ([draft 6.6]), off by default. With
  `DialOpts.Fallback` set, a dial that cannot reach the broker over QUIC
  connects over TCP/TLS instead, within the same `Dial`:
  - An ALPN mismatch, or a QUIC version the broker does not speak,
    falls back at once.
  - A network failure is retried after 1, 2 and 4 s ([draft 6.6.1]): a
    handshake or idle timeout, a stateless reset, the broker refusing
    the connection (CONNECTION_REFUSED), a DNS failure, or a socket
    error such as no route to the host. The QUIC attempts get at most 10 s,
    and at most half of what is left of the dial's `ctx`, so the
    fallback keeps the other half.
  - A certificate that fails verification is an error, never a reason
    to fall back.
  - After a dial that fell back over network failures, the next dial
    makes one QUIC attempt before falling back, until QUIC connects
    again. It always tries QUIC first ([draft 6.6.3]).
  - The fallback is TLS only, never plain TCP ([draft 6.6.2]). It never
    sends a CONNECT on both transports.
- **TLS.** QUIC uses TLS 1.3. The broker is verified against the system
  roots unless `DialOpts.TLSConfig` says otherwise, as for `mqtts://`;
  the URL's host is the server name when `TLSConfig.ServerName` is
  empty. `DialOpts.TLSConfigFunc` instead supplies the config on each
  dial, so a long-lived `DialFunc` can rotate trusted roots or client
  certificates.
- **TLS session resumption.** The dials of one `Dialer` (and so of one
  `DialFunc`) share in-memory session caches, one for QUIC and one for
  the fallback, so a client's reconnects resume the TLS session. This
  applies to a config that sets no `ClientSessionCache` and presents no
  client certificate: a resumed session does not present the
  certificate again, which would delay a rotation. `Dial` on its own
  resumes only through the config's own `ClientSessionCache`. The
  caller's config is never modified. See
  [Certificate management](../../docs/security.md#certificate-management).
- **0-RTT** ([draft 5.5.3]), off by default. With `Allow0RTT` and a
  cached session ticket from an earlier connection to the broker, the
  CONNECT goes out as early data with the first flight; packets after it
  wait for the handshake. If the broker rejects the early data, the
  CONNECT is sent once more after the handshake, as the draft requires.
  Read [Security](#security) before turning it on.
- **Sockets.** Each connection has a UDP socket of its own, of the
  broker's address family, closed with the connection.
  `DialOpts.PacketConn` puts every connection on one socket the caller
  owns instead; dials on the same `PacketConn`, from any number of
  Dialers, share it.
- **Keep-alive** ([draft 6.2]). QUIC PINGs every `KeepAlivePeriod` keep
  NAT bindings open; the QUIC idle timeout must exceed the MQTT
  keep-alive (default 30 s), or the connection dies between PINGREQs.

`mqttv5.New` rejects `mqtt-quic://` broker URLs unless `WithDialFunc` is
set. The client's own TLS options (`WithTLSConfig`, `WithTLSConfigFunc`)
do not apply beside `WithDialFunc`; pass TLS through `DialOpts`.

### Waiting for graceful closes on shutdown

`Close` returns before the broker's FIN arrives. To keep a process from
exiting before its connections have closed cleanly, build the `Dialer`
yourself and wait on it after `Disconnect`:

```go
d := quic.NewDialer(quic.DialOpts{TLSConfig: tlsCfg})
cli, err := mqttv5.New(
    mqttv5.WithBroker("mqtt-quic://broker.example.com:14567"),
    mqttv5.WithDialFunc(d.Dial),
)
// ...
_ = cli.Disconnect(ctx)
_ = d.Wait(ctx) // returns within CloseTimeout of the last Close, or when ctx ends
```

## Configuration

`DialOpts`; the zero value is valid.

| Field | Type | Default | Description |
|---|---|---|---|
| `TLSConfig` | `*tls.Config` | system roots, server name from the URL | TLS for QUIC and the fallback. For QUIC, `NextProtos` is replaced by `["mqtt"]`; the fallback uses the config as given. Never modified. Excludes `TLSConfigFunc`. |
| `TLSConfigFunc` | `transport.TLSConfigFunc` | none | Called once per dial, before any packet is sent and bounded by the dial's `ctx`, for the config `TLSConfig` would give; it serves that dial's QUIC attempts and fallback. An error fails the dial wrapped; a nil config fails it with `transport.ErrNilTLSConfig`. Setting both it and `TLSConfig` fails the dial with `ErrConflictingTLSConfig`. |
| `HandshakeTimeout` | `time.Duration` | `DefaultHandshakeTimeout` (5 s) | Bound on each QUIC handshake ([draft 6.5.1]). The client's connect timeout (`mqttv5.WithConnectTimeout`, 10 s) bounds the whole dial as well. |
| `MaxIdleTimeout` | `time.Duration` | `DefaultMaxIdleTimeout` (120 s) | QUIC idle timeout; the broker may negotiate it lower. Must exceed the MQTT keep-alive. |
| `KeepAlivePeriod` | `time.Duration` | `MaxIdleTimeout / 4` | Interval of QUIC PINGs on an idle connection; quic-go caps it at half the negotiated idle timeout. |
| `CloseTimeout` | `time.Duration` | `DefaultCloseTimeout` (10 s) | Bound on the wait for the broker's FIN after `Close`, and for the broker to close the connection after `Abort` ([draft 6.3.1], [draft 6.7.2.3]). Runs in the background; `Dialer.Wait` waits for it. |
| `Allow0RTT` | `bool` | `false` | Send the CONNECT as 0-RTT early data when a session ticket is cached. See [Security](#security). |
| `Fallback` | `*Fallback` | none (no fallback) | TCP/TLS endpoint to use when QUIC cannot reach the broker. |
| `Fallback.URL` | `string` | `mqtts://<mqtt-quic host>:8883` | The broker's TLS endpoint: `mqtts://`, `tls://` or `ssl://`; port 8883 when missing. Any other scheme fails the dial with `ErrFallbackNotTLS`. |
| `Fallback.Dialer` | `*net.Dialer` | `net.Dialer{}` | The fallback's TCP dial, e.g. to set a local address. |
| `TakeoverBackoff` | `time.Duration` | `DefaultTakeoverBackoff` (5 s) | Wait before the next dial after the broker reported Session Taken Over ([draft 6.6.4]). |
| `PacketConn` | `net.PacketConn` | a socket per connection | UDP socket for every connection, e.g. bound to a chosen address or interface. Dials on the same `PacketConn` share it; nothing else may read it while their connections are open. The caller owns it: it stays open when connections close. Must be comparable (a pointer). |
| `Logger` | `*slog.Logger` | `slog.Default()` | Receives the fallback and takeover events (see [Operations](#operations)). |

URLs are `mqtt-quic://host[:port]`; a missing port means 14567, the port
EMQX and NanoMQ use. A host name resolves to an IPv4 address when it has
one, else to IPv6: a QUIC dial has no fallback across address families.

## External dependencies

| Module | Version | Used for |
|---|---|---|
| `github.com/quic-go/quic-go` | v0.65.0 | QUIC (RFC 9000, RFC 9001), RESET_STREAM_AT, 0-RTT |
| `golang.org/x/crypto`, `golang.org/x/net`, `golang.org/x/sys` | v0.59.0, v0.62.0, v0.50.0 | indirect, through quic-go |

At runtime the client needs UDP egress to the broker's QUIC port, and
with a `Fallback`, TCP egress to its TLS port.

## API

| Symbol | Description |
|---|---|
| `DialFunc(DialOpts) transport.DialFunc` | `NewDialer(opts).Dial`, for `mqttv5.WithDialFunc`. |
| `NewDialer(DialOpts) *Dialer` | A Dialer: shared session caches, address-validation tokens, fallback and takeover state. Safe for concurrent use. |
| `(*Dialer).Dial(ctx, *url.URL) (transport.Conn, error)` | One connection; with a `Fallback`, a TLS connection when QUIC cannot connect. `ctx` bounds the whole dial. |
| `(*Dialer).Wait(ctx) error` | Blocks until every connection the Dialer made has finished closing, background graceful closes included, or `ctx` ends. |
| `Dial(ctx, *url.URL, DialOpts) (transport.Conn, error)` | One connection outside a Dialer; keeps no state. |
| `DialOpts`, `Fallback` | See Configuration. |
| `Conn` | The QUIC `transport.Conn`; treat it as opaque. A fallback connection is a TLS connection instead. |
| `(*Conn).ConnectionState() tls.ConnectionState` | The TLS handshake, waited for on a 0-RTT connection. Implements `transport.ConnectionStater`, so the client reports it in `ConnackInfo.TLS` and runs `WithTLSVerifier` on it. |
| `(*Conn).Abort(reason byte) error` | Ends the connection over a protocol error with QUIC code `0x300 + reason`. Implements `transport.Aborter`; the client calls it. |
| `(*Conn).ObserveDisconnect(reason byte)` | Takes note of the broker's DISCONNECT reason (Session Taken Over). Implements `transport.DisconnectObserver`; the client calls it. |
| `PeerError` | The broker aborted with QUIC code `0x300 + Reason`; `errors.As` it from `Read`/`Write`. Unwraps to the quic-go error. |
| `ErrALPN` | The broker did not select ALPN `mqtt`. |
| `ErrConflictingTLSConfig` | `DialOpts` sets both `TLSConfig` and `TLSConfigFunc`. |
| `ErrFallbackNotTLS` | `Fallback.URL` is not a TLS URL. |
| `DefaultHandshakeTimeout`, `DefaultMaxIdleTimeout`, `DefaultCloseTimeout`, `DefaultTakeoverBackoff` | 5 s, 120 s, 10 s, 5 s. |

## Build, run, test

```bash
go -C transport/quic test -race ./...
```

The tests run against an in-process quic-go server on loopback, with a
UDP relay that can hold the server's packets back and a UDP blackhole
for network failures. They cover:

- round trips; the broker's FIN; the stream limit that keeps the broker
  from opening streams;
- graceful close, its timeout, and close during a stalled write;
- Abort's codes and its reliable DISCONNECT; `PeerError` and the
  takeover wait;
- deadlines;
- TLS: verification, TLS state, the per-dial `TLSConfigFunc`, session
  resumption;
- ALPN mismatch;
- 0-RTT: what rides early data, and the CONNECT resent after a
  rejection;
- the fallback's triggers, retries and budget;
- shared PacketConns.

Timers run on a fake clock, so nothing waits out a real backoff.
[`examples/quic`](../../examples/quic) connects to a real broker, and the
conformance suite runs against EMQX's QUIC listener
([conformance/README.md](../../conformance/README.md)).

## Operations

This package logs two events through `DialOpts.Logger`:

| Level | Message | Attributes | Meaning |
|---|---|---|---|
| Warn | `QUIC unavailable, connected over TLS` | `broker`, `fallback`, `error` | A dial fell back; `error` is the last QUIC failure. |
| Info | `waiting before reconnecting after a session takeover` | `broker`, `wait` | The broker reported Session Taken Over; the next dial waits. |

Connection-level health and counters come from the client
(`Client.Stats()`, the lifecycle callbacks).

| Symptom | Check | Then |
|---|---|---|
| Dials fail with `context deadline exceeded` | UDP egress to the broker's port (14567); `nc -u` or a QUIC client from the same host | Open UDP on the firewall or security group. Where UDP is blocked by policy, set `Fallback`. |
| `ErrALPN` (`did not select ALPN "mqtt"`) | that the port is the broker's MQTT-over-QUIC listener, not HTTP/3 or another QUIC service | Point the URL at the MQTT QUIC listener (EMQX: `listeners.quic.default`), or set `Fallback`. |
| `x509: certificate signed by unknown authority` | the broker's certificate chain | Add its CA to `DialOpts.TLSConfig.RootCAs`, or to the roots `TLSConfigFunc` returns. QUIC does not fall back over this. |
| Warn `QUIC unavailable, connected over TLS` on every reconnect | the `error` attribute | A handshake timeout means UDP is lost on the path; an ALPN error means the listener is wrong. Each reconnect still tries QUIC once first, and returns to it as soon as it connects. |
| Info `waiting before reconnecting after a session takeover` repeatedly | other clients with the same client ID | Two clients sharing a client ID keep taking the session from each other; give each a unique ID. |
| `*PeerError` with reason 0x81 or 0x82 | the broker's log for what it rejected | The broker found the client's packets malformed or in violation; report it with the broker log. |
| Connections drop after a quiet period | the idle timeout the broker negotiates against `mqttv5.WithKeepAlive` | Keep the MQTT keep-alive below the broker's QUIC idle timeout. |
| quic-go prints `failed to sufficiently increase receive buffer size` | `net.core.rmem_max` / `wmem_max` (Linux) | quic-go asks for 7 MiB of socket buffer. MQTT rates rarely need it; raise the limits, or set `QUIC_GO_DISABLE_RECEIVE_BUFFER_WARNING=true`. |
| Handshakes stall with a custom `PacketConn` | whether anything else reads that socket | Only the dials on it may read it while their connections are open. |
| The broker logs idle-timeout closes at client shutdown | whether the process exits before the graceful close | Call `Dialer.Wait` after `Client.Disconnect` (see above). |

## Security

- The broker's certificate is verified by default, with the URL's host
  as the server name ([draft 7]). Do not set `InsecureSkipVerify`
  outside tests.
- The fallback is TLS only. A plain-TCP `Fallback.URL` is refused, and a
  certificate that fails verification over QUIC never falls back.
- **0-RTT early data can be replayed.** Leave `Allow0RTT` off unless the
  following is acceptable ([draft 7]):
  - An attacker on the path can replay the early CONNECT to the broker
    while the session ticket is valid. That opens a competing connection
    with the client's ID, which takes over the live session.
  - With `WithCleanStart(true)`, a replay also discards the session
    state.
  - Credentials in the CONNECT (user name, password, authentication
    data) are protected by the resumption secret, without the forward
    secrecy of the full handshake.
  - Only the CONNECT ever rides early data; everything after it waits
    for the handshake.
  - `WithTLSVerifier` makes the CONNECT wait for the verified handshake,
    so it never goes out early.
  - A config with a client certificate gets no session cache from the
    Dialer, and so no 0-RTT, unless it brings its own
    `ClientSessionCache`.
- Session tickets and address-validation tokens live in memory, per
  Dialer; nothing is written to disk.
- Nothing this package logs carries credentials or payloads.

## Code map

| File | What |
|---|---|
| `quic.go` | `DialOpts`, `Dialer`, `Dial`; QUIC attempts, retries, fallback, takeover wait |
| `conn.go` | `Conn`: the stream, graceful close, Abort, `PeerError`, 0-RTT early data |
| `socket.go` | sharing a caller's `PacketConn` among dials |
| `helpers_test.go` | in-process server, UDP relay and blackhole, TLS echo server |
| `quic_test.go` | dialing, URLs, TLS, sockets |
| `conn_test.go` | FIN, close, Abort, peer errors, takeover, deadlines |
| `early_test.go` | 0-RTT |
| `fallback_test.go` | fallback triggers, retries, budget |

<!-- MQTT Over QUIC Single Stream Mode, Committee Note Draft 04 -->

[Draft 04]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md
[draft 5.2]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#52-stream-establishment "5.2 Stream Establishment"
[draft 5.5.3]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#553-negotiating-among-multiple-operating-modes "5.5.3 Negotiating Among Multiple Operating Modes"
[draft 6.2]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#62-connection-keepalive "6.2 Connection Keepalive"
[draft 6.3.1]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#631-graceful-shutdown "6.3.1 Graceful Shutdown"
[draft 6.3.3]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#633-quic-error-code-semantics "6.3.3 QUIC Error Code Semantics"
[draft 6.5.1]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#651-initial-transport-selection "6.5.1 Initial Transport Selection"
[draft 6.6]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#66-fallback "6.6 Fallback"
[draft 6.6.1]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#661-fallback-categories "6.6.1 Fallback Categories"
[draft 6.6.2]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#662-fallback-behavior "6.6.2 Fallback Behavior"
[draft 6.6.3]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#663-post-session-fallback "6.6.3 Post-Session Fallback"
[draft 6.6.4]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#664-preventing-reconnection-livelock "6.6.4 Preventing Reconnection Livelock"
[draft 6.7.2]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#672-mapping-mqtt-protocol-errors-to-quic-transport-state "6.7.2 Mapping MQTT Protocol Errors to QUIC Transport State"
[draft 6.7.2.3]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#6723-error-stream-completion-and-cleanup "6.7.2.3 Error-Stream Completion and Cleanup"
[draft 7]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md#7-security-considerations "7 Security Considerations"
