// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Stats example: enable in-memory counters via WithStats, publish a
// stream of QoS 1 messages, and print a Stats snapshot every second.
//
// Bridge mqttv5.Stats into your own metrics surface (Prometheus,
// OpenTelemetry, expvar) by exporting each field as a gauge or
// monotonic counter.
//
//	MQTT_BROKER=mqtt://127.0.0.1:1883 go run ./examples/stats
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithClientID("mqttv5-stats"),
		mqttv5.WithLogger(logger),
		// Counters are opt-in to keep the hot path branch-predictor
		// friendly when nobody's scraping.
		mqttv5.WithStats(),
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

	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for i := 0; ; i++ {
			select {
			case <-t.C:
				_ = cli.Publish(ctx, wire.PublishOpts{
					Topic:   "metrics/demo",
					Payload: fmt.Appendf(nil, "tick %d", i),
					QoS:     1,
				})
			case <-ctx.Done():
				return
			}
		}
	}()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			s := cli.Stats()
			fmt.Printf("connects=%d sent=%d acked=%d inflight=%d failures=%d\n",
				s.Connects, s.PublishesSent, s.PublishesAcked,
				s.PublishesInflight, s.ConnectFailures)
		case <-ctx.Done():
			return
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
