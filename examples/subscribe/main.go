// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Subscribe example: the three delivery shapes, and what each does with
// a consumer that falls behind.
//
//	go -C examples run ./subscribe
//
// The channel has a small buffer and a slow reader: QoS 0 messages
// beyond the buffer are dropped (SubOnDrop counts them), while QoS 1
// messages are all delivered, because the broker's flow control holds
// them back. The queue, with DropOldest, keeps the freshest messages
// whatever their QoS. The callback runs on the read goroutine and acks
// on return.
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
)

const burst = 200

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "subscribe:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")
	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithClientID("mqttv5-subscribe"),
		mqttv5.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
	)
	if err != nil {
		return err
	}
	if err = cli.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cli.Disconnect(shutdown)
	}()

	// Channel: buffer of 8, read slowly.
	var dropped atomic.Int64
	ch, _, err := cli.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "subscribe/channel/#", QoS: 1}},
		mqttv5.SubBuffer(8),
		mqttv5.SubOnDrop(func(*mqttv5.Message) { dropped.Add(1) }),
	)
	if err != nil {
		return err
	}
	// Queue: capped at 16, oldest evicted first.
	q, _, err := cli.SubscribeQueue(ctx,
		[]mqttv5.TopicFilter{{Topic: "subscribe/queue/#", QoS: 1}},
		mqttv5.SubMaxQueueSize(16),
		mqttv5.SubDropPolicy(mqttv5.DropOldest),
	)
	if err != nil {
		return err
	}
	// Callback: non-blocking work on the read goroutine.
	var called atomic.Int64
	if _, err := cli.SubscribeCallback(ctx,
		[]mqttv5.TopicFilter{{Topic: "subscribe/callback/#", QoS: 1}},
		func(*mqttv5.Message) { called.Add(1) },
	); err != nil {
		return err
	}

	for _, qos := range []byte{0, 1} {
		dropped.Store(0)
		for _, kind := range []string{"channel", "queue", "callback"} {
			for i := range burst {
				err := cli.Publish(ctx, mqttv5.PublishOptions{
					Topic: fmt.Sprintf("subscribe/%s/%d", kind, qos), QoS: qos, Payload: fmt.Appendf(nil, "%d", i),
				})
				if err != nil {
					return err
				}
			}
		}
		got := drainSlowly(ctx, ch)
		fmt.Printf("QoS %d channel:  %3d of %d delivered, %3d dropped\n", qos, got, burst, dropped.Load())

		first, n := "", 0
		for m, ok := q.TryDequeue(); ok; m, ok = q.TryDequeue() {
			if n == 0 {
				first = string(m.Payload)
			}
			n++
			_ = m.Ack()
		}
		fmt.Printf("QoS %d queue:    %3d kept, oldest kept is #%s\n", qos, n, first)
		fmt.Printf("QoS %d callback: %3d handled\n", qos, called.Swap(0))
	}
	return nil
}

// drainSlowly reads ch at 1 ms a message until it has been quiet for
// half a second.
func drainSlowly(ctx context.Context, ch <-chan *mqttv5.Message) int {
	n := 0
	for {
		select {
		case m := <-ch:
			time.Sleep(time.Millisecond)
			_ = m.Ack()
			n++
		case <-time.After(500 * time.Millisecond):
			return n
		case <-ctx.Done():
			return n
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
