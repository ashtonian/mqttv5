// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// WebSocket example: connect via ws:// (or wss://) by composing
// mqttv5.WithDialFunc with the transport/ws Dial. The core mqttv5
// module stays stdlib-only; the gobwas/ws dependency lives only in
// the transport/ws submodule, imported only here.
//
// Run a broker with WebSocket listener (e.g., mosquitto configured
// for port 8083 with listener "8083 protocol websockets") then:
//
//	MQTT_BROKER=ws://127.0.0.1:8083/mqtt go run ./examples/ws
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport/ws"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := envOr("MQTT_BROKER", "ws://127.0.0.1:8083/mqtt")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// For wss://, supply a *tls.Config. nil config + wss:// makes
	// DialFunc fail with ErrMissingTLSConfig at the first Connect
	// attempt — no implicit downgrade.
	var tlsCfg *tls.Config

	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg})),
		mqttv5.WithClientID("mqttv5-ws-example"),
		mqttv5.WithLogger(logger),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "new:", err)
		os.Exit(1)
	}

	if err := cli.Connect(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	defer cli.Disconnect(context.Background())

	logger.Info("connected over websocket", "broker", broker)
	<-ctx.Done()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
