// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
)

// strictBrokerURL is a broker announcing tight limits in CONNACK: the
// hivemq-strict service (hivemq/strict.xml), or MQTT_BROKER_STRICT.
func strictBrokerURL() string {
	if v := os.Getenv("MQTT_BROKER_STRICT"); v != "" {
		return v
	}
	return "mqtt://127.0.0.1:1887"
}

// connectStrict connects to the strict broker and returns what it
// granted.
func connectStrict(t *testing.T, opts ...mqttv5.Option) (*mqttv5.Client, mqttv5.ConnackInfo) {
	t.Helper()
	requireBroker(t, strictBrokerURL())
	cli := connectTo(t, strictBrokerURL(), append([]mqttv5.Option{mqttv5.WithStats()}, opts...)...)
	info, ok := cli.ServerInfo()
	if !ok {
		t.Fatal("not connected")
	}
	return cli, info
}

// A QoS above the broker's Maximum QoS fails before anything is sent,
// or is downgraded with WithQoSDowngrade; the connection survives.
func TestStrict_MaximumQoS(t *testing.T) {
	cli, info := connectStrict(t)
	if info.MaximumQoS != 1 {
		t.Skipf("broker grants Maximum QoS %d, want 1", info.MaximumQoS)
	}
	topic := "conformance/strict/qos/" + randSuffix()
	ctx := context.Background()
	if err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 2}); !errors.Is(err, mqttv5.ErrQoSNotSupported) {
		t.Fatalf("QoS 2 publish: %v, want ErrQoSNotSupported", err)
	}

	sub, _ := connectStrict(t)
	// Maximum QoS limits what the client publishes (§3.2.2.3.4), not
	// what it subscribes to.
	ch, _, err := sub.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: topic, QoS: 2}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	down, _ := connectStrict(t, mqttv5.WithQoSDowngrade())
	if err := down.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 2, Payload: []byte("downgraded")}); err != nil {
		t.Fatalf("QoS 2 publish with WithQoSDowngrade: %v", err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if m.QoS != 1 || string(m.Payload) != "downgraded" {
		t.Errorf("delivered QoS %d %q", m.QoS, m.Payload)
	}
	_ = m.Ack()
	if s := cli.Stats(); s.Connects != 1 || !cli.Connected() {
		t.Errorf("connection lost after a refused publish: %+v", s)
	}
}

// A packet above the broker's Maximum Packet Size fails before it is
// sent; the connection survives and smaller packets go through.
func TestStrict_MaximumPacketSize(t *testing.T) {
	cli, info := connectStrict(t)
	if info.MaximumPacketSize == 0 || info.MaximumPacketSize > 64<<10 {
		t.Skipf("broker grants Maximum Packet Size %d", info.MaximumPacketSize)
	}
	limit := int(info.MaximumPacketSize)
	topic := "conformance/strict/size/" + randSuffix()
	ctx := context.Background()
	sub, _ := connectStrict(t)
	ch, _, err := sub.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 1, Payload: make([]byte, limit)}); !errors.Is(err, mqttv5.ErrPacketTooLarge) {
		t.Fatalf("publish of a %d-byte payload: %v, want ErrPacketTooLarge", limit, err)
	}
	small := bytes.Repeat([]byte{'s'}, limit/2)
	if err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 1, Payload: small}); err != nil {
		t.Fatalf("publish within the limit: %v", err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if !bytes.Equal(m.Payload, small) {
		t.Errorf("delivered %d bytes, want %d", len(m.Payload), len(small))
	}
	_ = m.Ack()
	if s := cli.Stats(); s.Connects != 1 {
		t.Errorf("reconnected %d times", s.Connects-1)
	}
}

// The broker's Server Keep Alive replaces the keep-alive the client
// asked for: idle longer than the broker's limit, the client keeps the
// connection by pinging at the broker's interval.
func TestStrict_ServerKeepAlive(t *testing.T) {
	cli, info := connectStrict(t, mqttv5.WithKeepAlive(600))
	if info.KeepAlive == 0 || info.KeepAlive >= 600 {
		t.Skipf("broker granted Server Keep Alive %d", info.KeepAlive)
	}
	idle := 3 * time.Duration(info.KeepAlive) * time.Second
	time.Sleep(idle)
	if s := cli.Stats(); s.Connects != 1 || s.Disconnects != 0 || !cli.Connected() {
		t.Fatalf("after %v idle with Server Keep Alive %ds: %+v", idle, info.KeepAlive, s)
	}
}

// Outbound topic aliases stay within the broker's Topic Alias Maximum:
// more topics than aliases all arrive, without a protocol error.
func TestStrict_TopicAliasMaximum(t *testing.T) {
	cli, info := connectStrict(t, mqttv5.WithOutboundTopicAliases())
	if info.TopicAliasMaximum == 0 || info.TopicAliasMaximum > 4 {
		t.Skipf("broker grants Topic Alias Maximum %d", info.TopicAliasMaximum)
	}
	prefix := "conformance/strict/alias/" + randSuffix()
	ctx := context.Background()
	sub, _ := connectStrict(t)
	topics := 2*int(info.TopicAliasMaximum) + 1
	chans := make([]<-chan *mqttv5.Message, topics)
	for i := range topics {
		ch, _, err := sub.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: fmt.Sprintf("%s/%d", prefix, i)}}, mqttv5.SubBuffer(8))
		if err != nil {
			t.Fatal(err)
		}
		chans[i] = ch
	}
	const rounds = 3
	for r := range rounds {
		for i := range topics {
			if err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: fmt.Sprintf("%s/%d", prefix, i), Payload: []byte{byte(r)}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i, ch := range chans {
		for r := range rounds {
			m := expectMessage(t, ch, 3*time.Second)
			if want := fmt.Sprintf("%s/%d", prefix, i); m.Topic != want || m.Payload[0] != byte(r) {
				t.Errorf("got %q %v, want %q %d", m.Topic, m.Payload, want, r)
			}
			_ = m.Ack()
		}
	}
	if s := cli.Stats(); s.Connects != 1 || s.ProtocolErrors != 0 {
		t.Errorf("stats after aliasing %d topics through %d aliases: %+v", topics, info.TopicAliasMaximum, s)
	}
}

// Features the broker says it lacks fail before anything is sent, and
// a subscription works without a Subscription Identifier the broker
// does not accept.
func TestStrict_UnavailableFeatures(t *testing.T) {
	cli, info := connectStrict(t)
	if info.WildcardSubscriptionAvailable || info.SharedSubscriptionAvailable || info.RetainAvailable || info.SubscriptionIdentifiersAvailable {
		t.Skipf("broker grants features hivemq/strict.xml disables: %+v", info)
	}
	ctx := context.Background()
	sent := cli.Stats().SubscribesSent
	if _, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "conformance/strict/+"}}); !errors.Is(err, mqttv5.ErrWildcardSubsUnsupported) {
		t.Errorf("wildcard subscribe: %v", err)
	}
	if _, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "$share/g/conformance/strict/x"}}); !errors.Is(err, mqttv5.ErrSharedSubsUnsupported) {
		t.Errorf("shared subscribe: %v", err)
	}
	if got := cli.Stats().SubscribesSent; got != sent {
		t.Errorf("%d SUBSCRIBE packets sent for refused features", got-sent)
	}
	topic := "conformance/strict/plain/" + randSuffix()
	if err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 1, Retain: true}); !errors.Is(err, mqttv5.ErrRetainNotSupported) {
		t.Errorf("retained publish: %v", err)
	}
	ch, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(2))
	if err != nil {
		t.Fatalf("plain subscribe: %v", err)
	}
	if err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 1, Payload: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	_ = m.Ack()
	if s := cli.Stats(); s.Connects != 1 || s.ProtocolErrors != 0 {
		t.Errorf("stats: %+v", s)
	}
}
