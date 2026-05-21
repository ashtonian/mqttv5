// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Reconnect example: shows the supervisor surviving a broker restart.
// Connects, subscribes, then publishes once per second. The full
// operator-hook set (OnConnectionUp / OnConnectionDown /
// OnConnectError / OnReconnectAttempt) logs each lifecycle event so
// you can watch the supervisor at work when you bounce the broker:
//
//	docker run -d --name mq -p 1883:1883 eclipse-mosquitto
//	go run ./examples/reconnect
//	# in another terminal:
//	docker restart mq
//
// Pending QoS 1 publishes are replayed on reconnect and the SUBSCRIBE
// is re-issued automatically.
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

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	reconnects := atomic.Int32{}
	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithClientID("mqttv5-reconnect"),
		mqttv5.WithKeepAlive(10),
		mqttv5.WithReconnectBackoff(mqttv5.ExponentialBackoff(
			500*time.Millisecond, 5*time.Second, 100*time.Millisecond,
		)),
		mqttv5.WithOnConnectionUp(func(ack *wire.Connack) {
			fmt.Printf("UP    (reconnect #%d) session_present=%t\n",
				reconnects.Load(), ack != nil && ack.SessionPresent)
		}),
		mqttv5.WithOnConnectionDown(func() bool {
			n := reconnects.Add(1)
			fmt.Printf("DOWN  (#%d) — supervisor will redial\n", n)
			return true
		}),
		mqttv5.WithOnConnectError(func(err error) {
			fmt.Printf("FAIL  connect attempt: %v\n", err)
		}),
		mqttv5.WithOnReconnectAttempt(func(attempt int, brokerURL string) {
			fmt.Printf("DIAL  attempt=%d broker=%s\n", attempt, brokerURL)
		}),
		mqttv5.WithLogger(logger),
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

	msgs, _, err := cli.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "reconnect/demo", QoS: 1}}, mqttv5.SubBuffer(64))
	if err != nil {
		panic(err)
	}
	go func() {
		for m := range msgs {
			fmt.Printf("recv  %s\n", m.Payload)
			_ = m.Ack()
		}
	}()

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-t.C:
			err := cli.Publish(ctx, wire.PublishOpts{
				Topic:   "reconnect/demo",
				Payload: fmt.Appendf(nil, "tick %d", i),
				QoS:     1,
			})
			if err != nil {
				fmt.Printf("publish err (will retry): %v\n", err)
			}
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
