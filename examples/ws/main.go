// WebSocket example: connect via ws:// (or wss://) by composing
// mqttv5.WithDialFunc with the transport/ws Dial. The core mqttv5
// module stays stdlib-only; the gobwas/ws dependency lives only in
// the transport/ws submodule, imported only here.
//
// Run a broker with WebSocket listener (e.g., mosquitto configured
// for port 8083 with listener "8083 protocol websockets") then:
//
//	MQTT_BROKER=ws://127.0.0.1:8083/mqtt go -C examples run ./ws
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport/ws"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	interrupted := ctx.Err() != nil
	stop()
	// An interrupt cancels whatever run was doing: that is a clean stop.
	if interrupted && errors.Is(err, context.Canceled) {
		err = nil
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "ws:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	broker := envOr("MQTT_BROKER", "ws://127.0.0.1:8083/mqtt")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// For wss://, a nil *tls.Config verifies the broker against the
	// system roots; set one for a private CA or client certificates.
	var tlsCfg *tls.Config

	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg})),
		mqttv5.WithClientID("mqttv5-ws-example"),
		mqttv5.WithLogger(logger),
	)
	if err != nil {
		return err
	}

	if err := cli.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cli.Disconnect(shutdown)
	}()

	logger.Info("connected over websocket", "broker", broker)
	<-ctx.Done()
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
