// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Group example: parallel sessions to two brokers with per-broker
// auth, GroupPublishBroadcast (default) so every publish hits both
// brokers, and merged Subscribe.
//
// Run two local brokers — easy with two mosquitto containers on
// different ports:
//
//	docker run -d -p 1883:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
//	docker run -d -p 1884:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
//	MQTT_BROKERS=mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884 go -C examples run ./group
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
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "group:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	raw := envOr("MQTT_BROKERS", "mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884")
	brokers := strings.Split(raw, ",")

	members := make([]mqttv5.GroupMember, len(brokers))
	for i, broker := range brokers {
		// Each member could carry per-broker credentials, TLS config,
		// or callbacks via Opts — see the per-broker comment below.
		members[i] = mqttv5.GroupMember{
			Brokers: []string{broker},
			Name:    fmt.Sprintf("broker-%d", i+1),
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
		return err
	}
	if err = g.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = g.Disconnect(shutdown)
	}()

	ch, _, err := g.Subscribe(ctx,
		[]mqttv5.TopicFilter{{Topic: "ha/#", QoS: 1}}, mqttv5.SubBuffer(128))
	if err != nil {
		return err
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
			results, err := g.Publish(ctx, mqttv5.PublishOptions{
				Topic:   "ha/heartbeat",
				Payload: fmt.Appendf(nil, "tick %d", i),
				QoS:     1,
			})
			if err != nil {
				fmt.Printf("publish err: %v\n", err)
			}
			for _, r := range results {
				if r.Err != nil {
					fmt.Printf("  %s: %v\n", r.Member, r.Err)
				}
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
