# WebSocket

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

```go
import (
    "github.com/ashtonian/mqttv5"
    "github.com/ashtonian/mqttv5/transport/ws"
)

cli, _ := mqttv5.New(
    mqttv5.WithBroker("wss://broker.example.com/mqtt"),
    mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg})),
)
```

Without `TLSConfig`, `wss://` verifies the broker against the system
roots with the URL's host as server name, the same default as
`mqtts://`. `ws.DialOpts.TLSConfigFunc` supplies the config per dial
instead, as `WithTLSConfigFunc` does for `mqtts://`; the client's own
TLS options do not apply beside `WithDialFunc`, and `New` rejects them
there. `ws://` and `wss://` URLs need `WithDialFunc`; `New` rejects
them otherwise.

What WebSocket changes for a client:

- **The broker must speak MQTT.** The upgrade offers subprotocol `mqtt`
  and the dial fails with `ws.ErrNoSubprotocol` unless the broker
  selects it ([MQTT-6.0.0-4]): a plain WebSocket endpoint, or a proxy
  that drops the `Sec-WebSocket-Protocol` header, fails at dial time
  rather than at CONNACK.
- **Disconnect closes the WebSocket too.** After the DISCONNECT,
  `Close` sends a Close frame with Normal Closure (1000) and returns;
  the TCP connection closes when the broker answers, or after
  `ws.DialOpts.CloseFrameTimeout` (1 s), as the RFC 6455 closing
  handshake asks.
- **Protocol errors carry their reason.** When the client finds a
  protocol error in the broker's packets, it sends the DISCONNECT and
  then a Close with Protocol Error (1002) naming the MQTT reason code.
  A broker that ends the connection with any status but Normal Closure
  (or none) surfaces as a `*ws.CloseError` with the status and reason,
  logged on the disconnect, and the client reconnects.
- **Binary frames only.** A text frame from the broker is answered with
  Unsupported Data (1003) and ends the connection with
  `ws.ErrTextFrame` ([MQTT-6.0.0-1]).

The module's [README](../transport/ws/README.md) has the configuration
table, the HTTP headers for authentication, and troubleshooting.

[MQTT-6.0.0-1]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285 "6 Using WebSocket as a network transport"
[MQTT-6.0.0-4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285 "6 Using WebSocket as a network transport"
