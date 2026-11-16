package readme

import (
	"context"
)

import (
	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport/quic"
)

func quicTransport() {
	cli, _ := mqttv5.New(
		mqttv5.WithBroker("mqtt-quic://broker.example.com:14567"),
		mqttv5.WithDialFunc(quic.DialFunc(quic.DialOpts{TLSConfig: tlsCfg})),
	)
	_ = cli
}

func quicFallback(ctx context.Context) {
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
}
