// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// The README's quick start.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ashtonian/mqttv5"
)

func main() {
	ctx := context.Background()

	cli, err := mqttv5.New(
		mqttv5.WithBroker("mqtt://localhost:1883"),
		mqttv5.WithClientID("quickstart"),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer cli.Disconnect(ctx)

	// Reconnects, session resumption and re-subscription happen underneath.
	msgs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}})
	if err != nil {
		log.Fatal(err)
	}

	err = cli.Publish(ctx, mqttv5.PublishOptions{Topic: "sensors/a1", QoS: 1, Payload: []byte("22.5")})
	if err != nil {
		log.Fatal(err)
	}

	m := <-msgs
	fmt.Printf("%s: %s\n", m.Topic, m.Payload)
	_ = m.Ack() // the broker gets its PUBACK once the message is acked
}
