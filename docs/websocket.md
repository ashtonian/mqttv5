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
`mqtts://`. `ws://` and `wss://` URLs need `WithDialFunc`; `New` rejects
them otherwise.
