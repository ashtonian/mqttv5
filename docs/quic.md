# QUIC

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

`transport/quic` carries MQTT over QUIC in Single Stream mode: one QUIC
stream holds the unchanged MQTT byte stream, ALPN `mqtt`, as the OASIS
MQTT TC's [MQTT Over QUIC Committee Note][draft] describes. EMQX
(listener `quic:default`, port 14567) and NanoMQ speak it.

```go
import (
    "github.com/ashtonian/mqttv5"
    "github.com/ashtonian/mqttv5/transport/quic"
)

cli, _ := mqttv5.New(
    mqttv5.WithBroker("mqtt-quic://broker.example.com:14567"),
    mqttv5.WithDialFunc(quic.DialFunc(quic.DialOpts{TLSConfig: tlsCfg})),
)
```

Without `TLSConfig` the broker is verified against the system roots
with the URL's host as server name, the same default as `mqtts://`.
`quic.DialOpts.TLSConfigFunc` supplies the config per dial instead, as
`WithTLSConfigFunc` does for `mqtts://`. The client's own TLS options do
not apply beside `WithDialFunc`, and `New` rejects them there.
`mqtt-quic://` URLs need `WithDialFunc`; `New` rejects them otherwise.

What QUIC changes for a client:

- **Reconnects are cheaper.** A `quic.DialFunc` resumes the TLS session
  on reconnect, as `mqtts://` does. With `Allow0RTT` the CONNECT can
  ride the first flight, at the cost of replay exposure (see the
  module's [Security](../transport/quic/README.md#security) section).
- **NAT rebinding.** A connection survives the NAT in front of the
  client changing its public address or port, where TCP would reset.
  Moving to another network (Wi-Fi to cellular) needs connection
  migration, which this transport does not do: the client reconnects,
  resuming the TLS session.
- **Protocol errors carry their reason.** When the client finds a
  protocol error in the broker's packets, it sends the DISCONNECT and
  then resets the stream with QUIC error code `0x300 + reason`.
  A broker that ends the connection the same way surfaces as a
  `*quic.PeerError` with the MQTT reason code.
- **UDP can be blocked.** Where a firewall drops UDP, set
  `DialOpts.Fallback` to fall back to TCP/TLS within the same dial,
  never to plain TCP:

```go
d := quic.NewDialer(quic.DialOpts{
    TLSConfig: tlsCfg,
    Fallback:  &quic.Fallback{URL: "mqtts://broker.example.com:8883"},
})
cli, _ := mqttv5.New(
    mqttv5.WithBroker("mqtt-quic://broker.example.com:14567"),
    mqttv5.WithDialFunc(d.Dial),
)
// On shutdown: disconnect, then let the graceful closes finish.
_ = cli.Disconnect(ctx)
_ = d.Wait(ctx)
```

`Close` returns before the broker acknowledges the end of the stream;
`Dialer.Wait` waits for that, bounded by `DialOpts.CloseTimeout`
(10 s). The module's [README](../transport/quic/README.md) has the
configuration table, the fallback rules, 0-RTT and troubleshooting.

[draft]: https://github.com/oasis-tcs/mqtt/blob/895d18a6c973566bfd7f54270a50abc55037d958/contributions/EMQX/mqtt-over-quic-cn/mqtt_over_quic_single_stream_CN_1.md "MQTT Over QUIC — Single Stream Mode Version 1.0, Committee Note Draft 04"
