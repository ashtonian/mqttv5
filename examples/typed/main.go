// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Typed example: publish + subscribe with JSON codec and a strongly
// typed payload.
//
// Run a local broker (e.g., `docker run -p 1883:1883 eclipse-mosquitto`)
// then:
//
//	MQTT_BROKER=mqtt://127.0.0.1:1883 go run ./examples/typed
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
	jsoncodec "github.com/ashtonian/mqttv5/codec/json"
	"github.com/ashtonian/mqttv5/wire"
)

type Reading struct {
	DeviceID string  `json:"device_id"`
	Temp     float64 `json:"temp"`
	Site     string  `json:"site"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")

	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithClientID("mqttv5-typed"),
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

	typed := mqttv5.NewTyped[Reading](cli, jsoncodec.Codec[Reading]{})

	msgs, _, err := typed.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}}, mqttv5.SubBuffer(64))
	if err != nil {
		panic(err)
	}
	go func() {
		for m := range msgs {
			fmt.Printf("recv  topic=%s reading=%+v\n", m.Topic, m.Value)
			_ = m.Ack()
		}
	}()

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-t.C:
			r := Reading{
				DeviceID: fmt.Sprintf("sensor-%03d", i%4),
				Temp:     20.0 + float64(i%10),
				Site:     "us-west-2",
			}
			err := typed.Publish(ctx, wire.PublishOpts{
				Topic: "sensors/" + r.DeviceID,
				QoS:   1,
			}, r)
			if err != nil {
				fmt.Printf("publish err: %v\n", err)
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
