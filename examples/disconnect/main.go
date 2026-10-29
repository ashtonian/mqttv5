// DisconnectWith example: send a custom DISCONNECT with a reason
// code, ReasonString, and a SessionExpiryInterval override.
//
// Compare with the plain Disconnect path which always sends
// ReasonNormalDisconnection with no properties.
//
//	MQTT_BROKER=mqtt://127.0.0.1:1883 go -C examples run ./disconnect
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
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
		fmt.Fprintln(os.Stderr, "disconnect:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")

	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithClientID("mqttv5-disconnect"),
		// Long-lived session — the broker would keep state for 1 hour
		// after a normal Disconnect.
		mqttv5.WithSessionExpiry(3600),
	)
	if err != nil {
		return err
	}
	if err := cli.Connect(ctx); err != nil {
		return err
	}

	// Do some work.
	_ = cli.Publish(ctx, mqttv5.PublishOptions{
		Topic:   "demo/disconnect",
		Payload: []byte("about to leave"),
		QoS:     1,
	})

	// Override the session expiry to 0 on the way out so the broker
	// discards our session immediately. ReasonString is forwarded to
	// the broker's logs (when RequestProblemInformation was set on
	// CONNECT — otherwise the broker is free to omit it).
	expiry := uint32(0)
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := cli.DisconnectWith(shutdown, mqttv5.DisconnectOptions{
		ReasonCode:            mqttv5.ReasonAdministrativeAction,
		ReasonString:          "planned shutdown",
		SessionExpiryInterval: &expiry,
	}); err != nil {
		return fmt.Errorf("disconnect: %w", err)
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
