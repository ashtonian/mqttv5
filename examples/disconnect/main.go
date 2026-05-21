// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// DisconnectWith example: send a custom DISCONNECT with a reason
// code, ReasonString, and a SessionExpiryInterval override.
//
// Compare with the plain Disconnect path which always sends
// ReasonNormalDisconnection with no properties.
//
//	MQTT_BROKER=mqtt://127.0.0.1:1883 go run ./examples/disconnect
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

func main() {
	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")

	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithClientID("mqttv5-disconnect"),
		// Long-lived session — the broker would keep state for 1 hour
		// after a normal Disconnect.
		mqttv5.WithSessionExpiry(3600),
	)
	if err != nil {
		panic(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		panic(err)
	}

	// Do some work.
	_ = cli.Publish(context.Background(), wire.PublishOpts{
		Topic:   "demo/disconnect",
		Payload: []byte("about to leave"),
		QoS:     1,
	})

	// Override the session expiry to 0 on the way out so the broker
	// discards our session immediately. ReasonString is forwarded to
	// the broker's logs (when RequestProblemInformation was set on
	// CONNECT — otherwise the broker is free to omit it).
	expiry := uint32(0)
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.DisconnectWith(shutdown, wire.DisconnectOpts{
		ReasonCode:            wire.ReasonAdministrativeAction,
		ReasonString:          "planned shutdown",
		SessionExpiryInterval: &expiry,
	}); err != nil {
		fmt.Printf("disconnect err: %v\n", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
