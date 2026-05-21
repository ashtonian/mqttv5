// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

// e2e benchmarks compare mqttv5 against eclipse/paho.golang/autopaho
// against the same live MQTT broker. Build-tagged so the codec-only
// micro-benchmarks (the default `go test -bench`) don't require a
// broker.
//
// To run:
//
//	docker compose -f ../conformance/docker-compose.yml up -d mosquitto
//	go test -tags e2e -bench . -benchmem -benchtime=2s -count=3 -timeout 10m
//	docker compose -f ../conformance/docker-compose.yml down

package benchmarks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"github.com/ashtonian/mqttv5"
)

// quietLogger discards every log call so benchmark output isn't
// interleaved with INFO connect/disconnect lines from the client.
var quietLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

// brokerURL returns the configured broker, defaulting to a local
// mosquitto on the standard MQTT port.
func brokerURL() string {
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		return v
	}
	return "mqtt://127.0.0.1:1883"
}

// requireBroker skips the benchmark if the broker isn't reachable.
// Without this, the b.Fatal in setup would still fail — but skip is
// the friendlier signal for CI matrices where the broker is optional.
func requireBroker(b *testing.B) {
	b.Helper()
	host := stripScheme(brokerURL())
	c, err := net.DialTimeout("tcp", host, 500*time.Millisecond)
	if err != nil {
		b.Skipf("e2e: broker %s unreachable (%v) — start docker-compose first",
			brokerURL(), err)
	}
	_ = c.Close()
}

func stripScheme(u string) string {
	for _, prefix := range []string{"mqtt://", "tcp://", "mqtts://", "tls://", "ssl://"} {
		if len(u) > len(prefix) && u[:len(prefix)] == prefix {
			return u[len(prefix):]
		}
	}
	return u
}

// uniqueID returns a fresh ClientID; collisions across concurrent
// benchmarks would cause the broker to evict the older session
// (per MQTT spec, same ClientID = takeover).
func uniqueID(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

// ---------------- mqttv5 client setup ----------------

// connectMqttv5 dials the broker and returns a ready Client. Cleanup
// is registered with b.Cleanup. extra options are appended after the
// defaults — so they can override (e.g. WithPublishMode).
func connectMqttv5(b *testing.B, clientID string, extra ...mqttv5.Option) *mqttv5.Client {
	b.Helper()
	opts := []mqttv5.Option{
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(clientID),
		mqttv5.WithKeepAlive(60),
		mqttv5.WithConnectTimeout(5 * time.Second),
		mqttv5.WithLogger(quietLogger),
	}
	opts = append(opts, extra...)
	cli, err := mqttv5.New(opts...)
	if err != nil {
		b.Fatalf("mqttv5 New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		b.Fatalf("mqttv5 Connect: %v", err)
	}
	b.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = cli.Disconnect(shutdown)
	})
	return cli
}

// subscribeMqttv5 returns a connected client + channel of received
// messages for the topic filter.
func subscribeMqttv5(b *testing.B, clientID, filter string) (*mqttv5.Client, <-chan *mqttv5.Message) {
	b.Helper()
	cli := connectMqttv5(b, clientID)
	ch, _, err := cli.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: filter, QoS: 1}}, mqttv5.SubBuffer(1024))
	if err != nil {
		b.Fatalf("mqttv5 Subscribe: %v", err)
	}
	// Brief pause so the broker definitely installed the
	// subscription before the benchmark publishes anything.
	time.Sleep(100 * time.Millisecond)
	return cli, ch
}

// ---------------- eclipse autopaho client setup ----------------

func connectAutopaho(b *testing.B, clientID string) *autopaho.ConnectionManager {
	b.Helper()
	u, err := url.Parse(brokerURL())
	if err != nil {
		b.Fatalf("parse url: %v", err)
	}
	// autopaho uses the supplied ctx as the connection-manager
	// lifetime context — cancelling it disconnects the CM. We give it
	// a cancellable context whose cancel is wired into b.Cleanup so
	// the CM stays alive for the whole benchmark.
	cmCtx, cmCancel := context.WithCancel(context.Background())
	cm, err := autopaho.NewConnection(cmCtx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     60,
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         0,
		ConnectTimeout:                5 * time.Second,
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
		},
	})
	if err != nil {
		cmCancel()
		b.Fatalf("autopaho NewConnection: %v", err)
	}
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer awaitCancel()
	if err := cm.AwaitConnection(awaitCtx); err != nil {
		cmCancel()
		b.Fatalf("autopaho AwaitConnection: %v", err)
	}
	b.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = cm.Disconnect(shutdown)
		cmCancel()
	})
	return cm
}

// subscribeAutopaho returns a connected CM + a channel of received
// publishes. Subscribe is issued AFTER AwaitConnection — calling it
// inside OnConnectionUp violates the "must not block" contract.
func subscribeAutopaho(b *testing.B, clientID, filter string) (*autopaho.ConnectionManager, <-chan *paho.Publish) {
	b.Helper()
	u, err := url.Parse(brokerURL())
	if err != nil {
		b.Fatal(err)
	}
	ch := make(chan *paho.Publish, 1024)

	// CM lifetime context — must not be cancelled until Cleanup.
	cmCtx, cmCancel := context.WithCancel(context.Background())
	cm, err := autopaho.NewConnection(cmCtx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     60,
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         0,
		ConnectTimeout:                5 * time.Second,
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					select {
					case ch <- pr.Packet:
					default:
					}
					return true, nil
				},
			},
		},
	})
	if err != nil {
		cmCancel()
		b.Fatal(err)
	}
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer awaitCancel()
	if err := cm.AwaitConnection(awaitCtx); err != nil {
		cmCancel()
		b.Fatal(err)
	}
	if _, err := cm.Subscribe(awaitCtx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}},
	}); err != nil {
		cmCancel()
		b.Fatalf("autopaho Subscribe: %v", err)
	}
	b.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = cm.Disconnect(shutdown)
		cmCancel()
	})
	// Brief pause so SUBSCRIBE is fully installed on the broker side.
	time.Sleep(100 * time.Millisecond)
	return cm, ch
}

// ---------------- per-library benchmark adapters ----------------

// benchLib is one library under test. Each function runs a self-contained
// benchmark loop against the configured broker.
type benchLib struct {
	name string

	pubQoS0             func(b *testing.B, payload []byte)
	pubQoS0WaitForFlush func(b *testing.B, payload []byte)
	pubQoS1             func(b *testing.B, payload []byte)
	pubQoS2             func(b *testing.B, payload []byte)
	pubConcQoS1         func(b *testing.B, payload []byte)
	rtt                 func(b *testing.B, payload []byte)
	sub                 func(b *testing.B, payload []byte)
	subFireHose         func(b *testing.B, payload []byte)
}

// libs is the side-by-side list iterated by every benchmark.
var libs = []benchLib{
	{
		name:                "mqttv5",
		pubQoS0:             pubQoS0_mqttv5,             // default PublishMode: fire-and-forget
		pubQoS0WaitForFlush: pubQoS0WaitForFlush_mqttv5, // WithPublishMode(PublishWaitForFlush)
		pubQoS1:             pubQoS1_mqttv5,
		pubQoS2:             pubQoS2_mqttv5,
		pubConcQoS1:         pubConcQoS1_mqttv5,
		rtt:                 rtt_mqttv5,
		sub:                 sub_mqttv5,
		subFireHose:         subFireHose_mqttv5,
	},
	{
		name:                "autopaho",
		pubQoS0:             pubQoS0_autopaho,
		pubQoS0WaitForFlush: pubQoS0_autopaho, // autopaho's QoS 0 Publish always waits for the writer mutex
		pubQoS1:             pubQoS1_autopaho,
		pubQoS2:             pubQoS2_autopaho,
		pubConcQoS1:         pubConcQoS1_autopaho,
		rtt:                 rtt_autopaho,
		sub:                 sub_autopaho,
		subFireHose:         subFireHose_autopaho,
	},
}

// e2eSizes is the payload-size sweep. Smaller than the codec micro
// benches because the broker round-trip dominates the per-publish
// time and we don't need fine-grained granularity.
var e2eSizes = []PayloadSize{
	{"64B", 64},
	{"256B", 256},
	{"1KiB", 1024},
}
