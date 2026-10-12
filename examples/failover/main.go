// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Failover example: one client over several interchangeable brokers.
// It connects to whichever accepts it, subscribes, and publishes once a
// second; stop the broker it reports and watch it move to the next.
//
//	docker run -d --name mq1 -p 1883:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
//	docker run -d --name mq2 -p 1884:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
//	go -C examples run ./failover
//	docker stop mq1   # or mq2: whichever it is connected to
//
// Two standalone mosquittos share no sessions: a failover starts a new
// one, so the client subscribes again and republishes what was
// unacknowledged, or resumes an older one the other broker kept for this
// ClientID, with what it queued since. Against the nodes of a cluster
// (EMQX, HiveMQ, VerneMQ) the session moves with the client.
package main

import (
	"context"
	"fmt"
	"log/slog"
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
		fmt.Fprintln(os.Stderr, "failover:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	brokers := strings.Split(envOr("MQTT_BROKERS", "mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884"), ",")

	cli, err := mqttv5.New(
		mqttv5.WithBrokers(brokers...),
		mqttv5.WithClientID("mqttv5-failover"),
		mqttv5.WithReconnectBackoff(mqttv5.ExponentialBackoff(250*time.Millisecond, 5*time.Second, 100*time.Millisecond)),
		mqttv5.WithOnConnectionUp(func(info mqttv5.ConnackInfo) {
			fmt.Printf("UP    %s session_present=%t\n", info.Broker, info.SessionPresent)
		}),
		mqttv5.WithOnConnectionDown(func() bool {
			fmt.Println("DOWN  moving to the next broker")
			return true
		}),
		mqttv5.WithOnConnectError(func(err error) {
			fmt.Printf("FAIL  %v\n", err)
		}),
		mqttv5.WithOnReconnectAttempt(func(attempt int, broker string) {
			fmt.Printf("DIAL  attempt=%d %s\n", attempt, broker)
		}),
		mqttv5.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))),
	)
	if err != nil {
		return err
	}
	msgs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "failover/demo", QoS: 1}})
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
	go func() {
		for m := range msgs {
			fmt.Printf("recv  %s\n", m.Payload)
			_ = m.Ack()
		}
	}()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		// A publish caught by a failover waits for the next broker.
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := cli.Publish(pctx, mqttv5.PublishOptions{Topic: "failover/demo", QoS: 1, Payload: fmt.Appendf(nil, "tick %d", i)})
		cancel()
		if err != nil {
			fmt.Printf("send  tick %d: %v\n", i, err)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
