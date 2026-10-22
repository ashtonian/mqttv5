// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Shared example: three workers share one subscription
// ($share/workers/jobs/#), and the broker hands each job to one of
// them. In production each worker is its own process; here they are
// three Clients in one, so their split shows side by side.
//
//	go -C examples run ./shared
//
// Each worker is a durable consumer (fixed ClientID, Clean Start 0, a
// session that outlives a restart) and subscribes before Connect, so a
// restarted worker comes back for the jobs its session holds.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
)

const (
	workers = 3
	jobs    = 12
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
		fmt.Fprintln(os.Stderr, "shared:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var (
		mu       sync.Mutex
		got      = make(map[string][]string)
		received int
		done     = make(chan struct{})
	)
	for w := range workers {
		name := fmt.Sprintf("worker-%d", w+1)
		cli, err := mqttv5.New(
			mqttv5.WithBroker(broker),
			mqttv5.WithClientID("mqttv5-shared-"+name),
			mqttv5.WithCleanStart(false),
			mqttv5.WithSessionExpiry(10*60),
			mqttv5.WithLogger(logger),
		)
		if err != nil {
			return err
		}
		_, err = cli.SubscribeCallback(ctx, []mqttv5.TopicFilter{{Topic: "$share/workers/jobs/#", QoS: 1}},
			func(m *mqttv5.Message) {
				mu.Lock()
				defer mu.Unlock()
				got[name] = append(got[name], strings.TrimPrefix(m.Topic, "jobs/"))
				// A session may also hold jobs of an earlier, interrupted run.
				if received++; received == jobs {
					close(done)
				}
			})
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
	}

	producer, err := mqttv5.New(mqttv5.WithBroker(broker), mqttv5.WithLogger(logger))
	if err != nil {
		return err
	}
	if err := producer.Connect(ctx); err != nil {
		return err
	}
	defer func() { _ = producer.Disconnect(context.WithoutCancel(ctx)) }()
	for i := range jobs {
		if err := producer.Publish(ctx, mqttv5.PublishOptions{Topic: fmt.Sprintf("jobs/%02d", i), QoS: 1}); err != nil {
			return err
		}
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("only some of the %d jobs arrived", jobs)
	case <-ctx.Done():
		return nil
	}
	mu.Lock()
	defer mu.Unlock()
	for _, n := range slices.Sorted(maps.Keys(got)) {
		fmt.Printf("%s: %s\n", n, strings.Join(got[n], " "))
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
