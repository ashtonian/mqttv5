// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Basic example: connect, subscribe to a topic via channel, publish a
// few messages, shut down cleanly on SIGINT.
//
// Run a local broker (e.g., `docker run -p 1883:1883 eclipse-mosquitto`)
// then:
//
//	MQTT_BROKER=mqtt://127.0.0.1:1883 go run ./examples/basic
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
		mqttv5.WithClientID("mqttv5-basic"),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithLogger(logger),
	)
	if err != nil {
		panic(err)
	}
	if err = cli.Connect(ctx); err != nil {
		panic(err)
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = cli.Disconnect(shutdown)
	}()

	msgs, _, err := cli.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "demo/#", QoS: 1}}, mqttv5.SubBuffer(64))
	if err != nil {
		panic(err)
	}
	go func() {
		for m := range msgs {
			fmt.Printf("recv  topic=%s qos=%d payload=%s\n", m.Topic, m.QoS, m.Payload)
			_ = m.Ack()
		}
	}()

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-t.C:
			err := cli.Publish(ctx, wire.PublishOpts{
				Topic:   fmt.Sprintf("demo/counter/%d", i%3),
				Payload: fmt.Appendf(nil, "tick %d", i),
				QoS:     1,
			})
			if err != nil {
				fmt.Printf("publish err: %v\n", err)
			}
		case <-ctx.Done():
			fmt.Println("shutting down")
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
