// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Pool example: publish the same burst of QoS 1 messages over the main
// connection alone, then through a publisher pool, and compare.
//
//	go -C examples run ./pool
//	MQTT_BROKER=mqtt://127.0.0.1:1884 go -C examples run ./pool   # e.g. EMQX
//
// A broker lets each connection have at most its Receive Maximum QoS 1
// and 2 publishes unacknowledged — 20 for mosquitto, 32 for EMQX, 10 for
// HiveMQ CE by default — so concurrent publishers on one connection wait
// on the round trip; each pool member brings a window of its own, and a
// broker that serves connections on several cores takes them in
// parallel. PoolRoutingHashByTopic keeps each topic on one connection,
// and so in order.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
)

const (
	messages   = 20_000
	publishers = 64 // concurrent Publish calls
	topics     = 128
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
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")
	for _, size := range []int{0, 4} {
		rate, fallbacks, err := burst(ctx, broker, size)
		if err != nil {
			return err
		}
		fmt.Printf("pool=%d  %8.0f msg/s  pool_fallbacks=%d\n", size, rate, fallbacks)
	}
	return nil
}

// burst publishes messages QoS 1 messages over topics from publishers
// goroutines and returns the rate.
func burst(ctx context.Context, broker string, poolSize int) (float64, uint64, error) {
	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithClientID(fmt.Sprintf("mqttv5-pool-%d", poolSize)),
		mqttv5.WithPublisherPool(poolSize),
		mqttv5.WithPublisherPoolRouting(mqttv5.PoolRoutingHashByTopic),
		mqttv5.WithStats(),
		mqttv5.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
	)
	if err != nil {
		return 0, 0, err
	}
	if err := cli.Connect(ctx); err != nil {
		return 0, 0, err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cli.Disconnect(shutdown)
	}()

	payload := make([]byte, 256)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	start := time.Now()
	for p := range publishers {
		wg.Go(func() {
			for i := p; i < messages; i += publishers {
				err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: fmt.Sprintf("pool/demo/%d", i%topics), QoS: 1, Payload: payload})
				if err != nil {
					mu.Lock()
					firstErr = cmp.Or(firstErr, err)
					mu.Unlock()
					return
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	if firstErr != nil {
		return 0, 0, firstErr
	}
	return float64(messages) / elapsed.Seconds(), cli.Stats().PoolFallbacks, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
