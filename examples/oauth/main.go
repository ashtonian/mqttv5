// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// OAuth credential-rotation example: WithConnectPacketBuilder lets
// you mutate the CONNECT packet immediately before each attempt.
// This is the standard place to refresh an OAuth bearer token, fetch
// a SigV4-signed CONNECT credential, or any other per-attempt
// credential.
//
// The supplied context is the per-attempt context bounded by
// ConnectTimeout — short token fetches inline are fine; long ones
// belong in a background refresher that updates a cached value the
// builder reads.
//
//	MQTT_BROKER=mqtt://127.0.0.1:1883 go run ./examples/oauth
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

// tokenSource models an OAuth provider. Real implementations would
// hit an OIDC endpoint and cache the token until it expires; here a
// monotonic counter stands in.
type tokenSource struct{ n atomic.Int64 }

func (t *tokenSource) Token() string {
	return fmt.Sprintf("bearer-%d", t.n.Add(1))
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tokens := &tokenSource{}

	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithClientID("mqttv5-oauth"),
		mqttv5.WithLogger(logger),
		mqttv5.WithConnectPacketBuilder(func(_ context.Context, opts *wire.ConnectOpts) error {
			opts.Username = "service-account"
			opts.Password = []byte(tokens.Token())
			return nil
		}),
		// Observability: each reconnect carries a new token.
		mqttv5.WithOnReconnectAttempt(func(attempt int, brokerURL string) {
			fmt.Printf("reconnect attempt=%d broker=%s\n", attempt, brokerURL)
		}),
	)
	if err != nil {
		panic(err)
	}
	if err := cli.Connect(ctx); err != nil {
		panic(err)
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = cli.Disconnect(shutdown)
	}()

	<-ctx.Done()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
