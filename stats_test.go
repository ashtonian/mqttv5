// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// pubackBroker accepts CONNECT and PUBACKs every QoS>0 PUBLISH.
func pubackBroker(t *testing.T) *fakeBroker {
	t.Helper()
	return newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			pub, ok := pkt.(*wire.Publish)
			if !ok {
				pkt.Release()
				continue
			}
			id := pub.PacketID
			pkt.Release()
			if id != 0 {
				_, _ = wire.WritePuback(c, wire.PubRespOpts{
					PacketID:   id,
					ReasonCode: wire.ReasonSuccess,
				})
			}
		}
	})
}

func TestStatsCountersTickOnPublish(t *testing.T) {
	fb := pubackBroker(t)

	cli, err := New(WithBroker(fb.URL()), WithStats())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if got := cli.Stats().Connects; got != 1 {
		t.Errorf("Stats.Connects after Connect = %d, want 1", got)
	}

	for i := 0; i < 3; i++ {
		if err := cli.Publish(context.Background(), wire.PublishOpts{
			Topic: "stats/test", QoS: 1, Payload: []byte("x"),
		}); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := cli.Stats()
		if s.PublishesSent >= 3 && s.PublishesAcked >= 3 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s := cli.Stats()
	t.Fatalf("after 3 Publishes: PublishesSent=%d PublishesAcked=%d (want both >= 3)",
		s.PublishesSent, s.PublishesAcked)
}

// TestStatsDisabledReturnsZero verifies that WithStats() is required
// to populate counters. Without it, every Stats() field stays at the
// zero value even after activity that would otherwise tick counters.
// (PublishesInflight and SubscriptionsActive are point-in-time gauges
// and stay zero only because we haven't subscribed or pre-loaded any
// outbound entries.)
func TestStatsDisabledReturnsZero(t *testing.T) {
	fb := pubackBroker(t)

	cli, err := New(WithBroker(fb.URL())) // no WithStats
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	for i := 0; i < 3; i++ {
		if err := cli.Publish(context.Background(), wire.PublishOpts{
			Topic: "stats/disabled", QoS: 1, Payload: []byte("x"),
		}); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}
	// Give the broker a beat to ack so the publish path completes.
	time.Sleep(100 * time.Millisecond)

	s := cli.Stats()
	if s.Connects != 0 || s.PublishesSent != 0 || s.PublishesAcked != 0 {
		t.Errorf("Stats without WithStats should be zero, got %+v", s)
	}
}
