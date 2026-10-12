// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Group example: a central service for devices split across independent
// clusters, acknowledged in the application as a device shadow or a
// command RPC is. Each device connects to one cluster; the service keeps
// a session on every cluster through a ClientGroup, publishes each
// command — here a desired shadow state — to all of them, and hears the
// devices' replies, carrying their reported state, from any of them on
// one merged subscription. A command goes out again until its device
// replies, and a device acts once per command however many copies reach
// it.
//
//	docker run -d --name mq1 -p 1883:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
//	docker run -d --name mq2 -p 1884:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
//	go -C examples run ./group
//
// MQTT_BROKERS names the clusters, one URL each. dev-1 is on the first,
// dev-2 and dev-3 on the last, and dev-3 comes online two seconds late:
// the copies sent before that find no subscriber, and a resend reaches
// it.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
)

// resendAfter is how long the service waits for an acknowledgement
// before it sends a command again.
const resendAfter = time.Second

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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clusters := strings.Split(envOr("MQTT_BROKERS", "mqtt://127.0.0.1:1883,mqtt://127.0.0.1:1884"), ",")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	svc, err := newService(ctx, clusters, logger)
	if err != nil {
		return err
	}
	defer svc.close()

	devices := []struct {
		id      string
		cluster string
		delay   time.Duration
	}{
		{"dev-1", clusters[0], 0},
		{"dev-2", clusters[len(clusters)-1], 0},
		{"dev-3", clusters[len(clusters)-1], 2 * time.Second},
	}
	ready := make([]chan struct{}, len(devices))
	for i, d := range devices {
		ready[i] = make(chan struct{})
		go runDevice(ctx, d.id, d.cluster, d.delay, ready[i], logger)
	}

	var wg sync.WaitGroup
	for i, d := range devices {
		wg.Go(func() {
			if d.delay == 0 {
				// A device online from the start has subscribed before
				// its first command; dev-3 has not.
				select {
				case <-ready[i]:
				case <-ctx.Done():
					return
				}
			}
			id := fmt.Sprintf("cmd-%d", i+1)
			start := time.Now()
			reported, sends, err := svc.send(ctx, d.id, id, []byte(`{"firmware":"1.4.2"}`))
			if err != nil {
				fmt.Printf("SVC   %s to %s: %v\n", id, d.id, err)
				return
			}
			fmt.Printf("SVC   %s: %s reports %s after %d send(s), %v\n", id, d.id, reported, sends, time.Since(start).Round(time.Millisecond))
		})
	}
	wg.Wait()
	return nil
}

// service sends commands to devices on any cluster and hands each
// device's reply to the command waiting for it.
type service struct {
	group *mqttv5.ClientGroup

	mu      sync.Mutex
	pending map[string]chan []byte // by command ID
}

func newService(ctx context.Context, clusters []string, logger *slog.Logger) (*service, error) {
	members := make([]mqttv5.GroupMember, len(clusters))
	for i, url := range clusters {
		members[i] = mqttv5.GroupMember{Name: fmt.Sprintf("cluster-%d", i+1), Brokers: []string{url}}
	}
	g, err := mqttv5.NewClientGroup(members,
		mqttv5.WithGroupSharedOpts(
			mqttv5.WithClientID("mqttv5-command-service"),
			mqttv5.WithRetryInitialConnect(), // a cluster down at start is tried again
			mqttv5.WithLogger(logger),
		),
		mqttv5.WithGroupSuccess(mqttv5.GroupSuccessAny), // one cluster down fails no command
	)
	if err != nil {
		return nil, err
	}
	s := &service{group: g, pending: make(map[string]chan []byte)}
	replies, _, err := g.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "devices/+/reply", QoS: 1}})
	if err != nil {
		return nil, err
	}
	if err := g.Connect(ctx); err != nil {
		return nil, err
	}
	go s.collect(replies)
	return s, nil
}

// send publishes desired state to every cluster until its device
// replies, and returns the state the device reports and how many times
// the command went out.
func (s *service) send(ctx context.Context, device, id string, desired []byte) ([]byte, int, error) {
	reply := make(chan []byte, 1)
	s.mu.Lock()
	s.pending[id] = reply
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
	}()

	expiry := uint32(60) // a copy no device collects goes stale
	for sends := 1; ; sends++ {
		results, err := s.group.Publish(ctx, mqttv5.PublishOptions{
			Topic:                 "devices/" + device + "/cmd",
			QoS:                   1,
			Payload:               desired,
			CorrelationData:       []byte(id),
			MessageExpiryInterval: &expiry,
		})
		if err != nil {
			fmt.Printf("SVC   %s: no cluster took it: %v\n", id, err)
		}
		for _, r := range results {
			if r.Err != nil {
				fmt.Printf("SVC   %s: %s: %v\n", id, r.Member, r.Err)
			}
		}
		select {
		case reported := <-reply:
			return reported, sends, nil
		case <-ctx.Done():
			return nil, sends, ctx.Err()
		case <-time.After(resendAfter):
		}
	}
}

// collect hands each reply to the command it names; a duplicate finds
// none.
func (s *service) collect(replies <-chan *mqttv5.Message) {
	for m := range replies {
		id := string(m.Properties.CorrelationData())
		s.mu.Lock()
		reply := s.pending[id]
		delete(s.pending, id)
		s.mu.Unlock()
		if reply != nil {
			reply <- m.Payload
		}
		_ = m.Ack()
	}
}

func (s *service) close() {
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.group.Disconnect(shutdown)
}

// runDevice connects a simulated device to its cluster after delay and
// closes ready once it is subscribed. It applies each command's desired
// state once per command ID and replies to every copy with the state it
// reports.
func runDevice(ctx context.Context, id, cluster string, delay time.Duration, ready chan<- struct{}, logger *slog.Logger) {
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return
	}
	dev, err := mqttv5.New(mqttv5.WithBroker(cluster), mqttv5.WithClientID("mqttv5-"+id), mqttv5.WithLogger(logger))
	if err != nil {
		fmt.Printf("DEV   %s: %v\n", id, err)
		return
	}
	if err = dev.Connect(ctx); err != nil {
		fmt.Printf("DEV   %s: %v\n", id, err)
		return
	}
	cmds, _, err := dev.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "devices/" + id + "/cmd", QoS: 1}})
	if err != nil {
		fmt.Printf("DEV   %s: %v\n", id, err)
		return
	}
	close(ready)
	// Disconnect closes cmds, which ends the loop.
	go func() {
		<-ctx.Done()
		_ = dev.Disconnect(context.WithoutCancel(ctx))
	}()
	fmt.Printf("DEV   %s online on %s\n", id, cluster)

	seen := make(map[string]bool)
	reported := []byte(`{"firmware":"1.4.1"}`)
	for m := range cmds {
		cmd := string(m.Properties.CorrelationData())
		if seen[cmd] {
			fmt.Printf("DEV   %s: %s again, answered without acting\n", id, cmd)
		} else {
			seen[cmd] = true
			reported = m.ClonePayload() // the update applied
			fmt.Printf("DEV   %s: %s desired %s\n", id, cmd, m.Payload)
		}
		err := dev.Publish(ctx, mqttv5.PublishOptions{
			Topic: "devices/" + id + "/reply", QoS: 1, CorrelationData: m.Properties.CorrelationData(), Payload: reported,
		})
		if err != nil && ctx.Err() == nil {
			fmt.Printf("DEV   %s: reply to %s: %v\n", id, cmd, err)
		}
		_ = m.Ack()
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
