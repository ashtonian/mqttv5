// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
)

// TestOverlappingSubscriptionsOneCopyEach checks against real brokers
// that, with two overlapping subscriptions on one client, each receives
// the message exactly once. The broker sends one copy per
// subscription, each carrying that subscription's identifier.
func TestOverlappingSubscriptionsOneCopyEach(t *testing.T) {
	for _, url := range []string{brokerURL(), secondaryBrokerURL()} {
		t.Run(url, func(t *testing.T) {
			requireBroker(t, url)
			cli := connectTo(t, url)
			if info, _ := cli.ServerInfo(); !info.SubscriptionIdentifiersAvailable {
				t.Skip("broker does not support subscription identifiers")
			}
			topic := "conformance/overlap/" + randSuffix()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			wide, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "conformance/overlap/#", QoS: 1}}, mqttv5.SubBuffer(8))
			if err != nil {
				t.Fatal(err)
			}
			narrow, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(8))
			if err != nil {
				t.Fatal(err)
			}
			pub := connectTo(t, url)
			if err := pub.Publish(ctx, mqttv5.PublishOptions{Topic: topic, Payload: []byte("once"), QoS: 1}); err != nil {
				t.Fatal(err)
			}
			for name, ch := range map[string]<-chan *mqttv5.Message{"wide": wide, "narrow": narrow} {
				got := 0
				for done := false; !done; {
					select {
					case m := <-ch:
						_ = m.Ack()
						got++
					case <-time.After(500 * time.Millisecond):
						done = true
					}
				}
				if got != 1 {
					t.Errorf("%s subscription received %d copies, want 1", name, got)
				}
			}
		})
	}
}
