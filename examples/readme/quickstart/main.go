// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// The README's opening example.
package main

import (
	"context"
	"fmt"

	"github.com/ashtonian/mqttv5"
	jsoncodec "github.com/ashtonian/mqttv5/codec/json"
)

type Event struct {
	Device string  `json:"device"`
	Temp   float64 `json:"temp"`
}

func main() {
	ctx := context.Background()

	client, _ := mqttv5.New(mqttv5.WithBroker("mqtt://localhost:1883"))
	_ = client.Connect(ctx)
	defer client.Disconnect(ctx)

	// Generic typed pub/sub via Codec[T] (JSON ships in a sibling
	// submodule). Supervisor handles reconnect + auto-resubscribe +
	// QoS 1/2 replay underneath — you just write the consumer loop.
	events := mqttv5.NewTyped(client, jsoncodec.Codec[Event]{})

	msgs, _, _ := events.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "events/#", QoS: 1}})
	go func() {
		for m := range msgs {
			fmt.Printf("%s: %+v\n", m.Topic, m.Value) // m.Value already decoded
			_ = m.Ack()                               // PUBACK held for QoS 1 until you ack
		}
	}()

	_ = events.Publish(ctx, mqttv5.PublishOptions{Topic: "events/hello", QoS: 1}, Event{Device: "a1", Temp: 22.5})
}
