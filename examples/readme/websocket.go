// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package readme

import (
	"crypto/tls"
)

import (
	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport/ws"
)

var tlsCfg *tls.Config

func websocket() {
	cli, _ := mqttv5.New(
		mqttv5.WithBroker("wss://broker.example.com/mqtt"),
		mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg})),
	)
	_ = cli
}
