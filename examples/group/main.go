// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Group example: parallel sessions to two brokers with per-broker
// auth, GroupPublishBroadcast (default) so every publish hits both
// brokers, and merged Subscribe.
//
// Run two local brokers — easy with two mosquitto containers on
// different ports:
//
//	docker run -d -p 1883:1883 eclipse-mosquitto
//	docker run -d -p 1884:1883 eclipse-mosquitto
//	MQTT_BROKERS=mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884 go run ./examples/group
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	raw := envOr("MQTT_BROKERS", "mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884")
	brokers := strings.Split(raw, ",")

	members := make([]mqttv5.GroupMember, len(brokers))
	for i, broker := range brokers {
		// Each member could carry per-broker credentials, TLS config,
		// or callbacks via Opts — see the per-broker comment below.
		members[i] = mqttv5.GroupMember{
			Broker: broker,
			Name:   fmt.Sprintf("broker-%d", i+1),
		}
	}

	g, err := mqttv5.NewClientGroup(members,
		mqttv5.WithGroupSharedOpts(
			mqttv5.WithClientID("mqttv5-group"),
		),
		// GroupPublishBroadcast is the default — explicit here for
		// clarity. Use GroupPublishRoundRobin for fleet-throughput.
		mqttv5.WithGroupPublishPolicy(mqttv5.GroupPublishBroadcast),
	)
	if err != nil {
		panic(err)
	}
	if err := g.Connect(ctx); err != nil {
		panic(err)
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = g.Disconnect(shutdown)
	}()

	ch, _, err := g.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "ha/#", QoS: 1}}, mqttv5.SubBuffer(128))
	if err != nil {
		panic(err)
	}
	go func() {
		for m := range ch {
			fmt.Printf("recv  topic=%s payload=%s\n", m.Topic, m.Payload)
			_ = m.Ack()
		}
	}()

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-t.C:
			err := g.Publish(ctx, wire.PublishOpts{
				Topic:   "ha/heartbeat",
				Payload: fmt.Appendf(nil, "tick %d", i),
				QoS:     1,
			})
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
