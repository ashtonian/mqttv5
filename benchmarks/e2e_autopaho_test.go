// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

func pubQoS0_autopaho(b *testing.B, payload []byte) {
	cm := connectAutopaho(b, uniqueID("autopaho-pub0"))
	topic := "bench/pub0/" + uniqueID("")
	ctx := context.Background()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		// Allocate a fresh Publish per iteration — autopaho.Publish
		// retains the pointer for the duration of the call, so we
		// can't safely reuse one. (mqttv5's PublishOpts is a value
		// type, so its loop reuses naturally.)
		if _, err := cm.Publish(ctx, &paho.Publish{
			Topic: topic, Payload: payload, QoS: 0,
		}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

func pubQoS1_autopaho(b *testing.B, payload []byte) {
	cm := connectAutopaho(b, uniqueID("autopaho-pub1"))
	topic := "bench/pub1/" + uniqueID("")
	ctx := context.Background()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := cm.Publish(ctx, &paho.Publish{
			Topic: topic, Payload: payload, QoS: 1,
		}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

func pubQoS2_autopaho(b *testing.B, payload []byte) {
	cm := connectAutopaho(b, uniqueID("autopaho-pub2"))
	topic := "bench/pub2/" + uniqueID("")
	ctx := context.Background()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := cm.Publish(ctx, &paho.Publish{
			Topic: topic, Payload: payload, QoS: 2,
		}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

func rtt_autopaho(b *testing.B, payload []byte) {
	topic := "bench/rtt/" + uniqueID("")
	pub := connectAutopaho(b, uniqueID("autopaho-rtt-pub"))
	_, ch := subscribeAutopaho(b, uniqueID("autopaho-rtt-sub"), topic)
	ctx := context.Background()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := pub.Publish(ctx, &paho.Publish{
			Topic: topic, Payload: payload, QoS: 1,
		}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			b.Fatal("RTT: timeout waiting for delivery")
		}
	}
}

// pubConcQoS1_autopaho is the autopaho equivalent of
// pubConcQoS1_mqttv5 — 8 goroutines on the same ConnectionManager.
func pubConcQoS1_autopaho(b *testing.B, payload []byte) {
	cm := connectAutopaho(b, uniqueID("autopaho-pubc"))
	topic := "bench/pubc/" + uniqueID("")
	ctx := context.Background()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := cm.Publish(ctx, &paho.Publish{
				Topic: topic, Payload: payload, QoS: 1,
			}); err != nil {
				b.Fatalf("Publish: %v", err)
			}
		}
	})
}

// subFireHose_autopaho is the autopaho counterpart of
// subFireHose_mqttv5. Uses OnPublishReceived with an atomic counter
// directly (no channel buffer to drop) so the comparison is purely
// the reader-loop dispatch cost.
func subFireHose_autopaho(b *testing.B, payload []byte) {
	topic := "bench/firehose/" + uniqueID("")
	pub := connectAutopaho(b, uniqueID("autopaho-fh-pub"))

	received := atomic.Int64{}
	u, err := url.Parse(brokerURL())
	if err != nil {
		b.Fatal(err)
	}
	cmCtx, cmCancel := context.WithCancel(context.Background())
	cm, err := autopaho.NewConnection(cmCtx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     60,
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         0,
		ConnectTimeout:                5 * time.Second,
		ClientConfig: paho.ClientConfig{
			ClientID: uniqueID("autopaho-fh-sub"),
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(_ paho.PublishReceived) (bool, error) {
					received.Add(1)
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
		Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 0}},
	}); err != nil {
		cmCancel()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = cm.Disconnect(shutdown)
		cmCancel()
	})
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range b.N {
			_, _ = pub.Publish(ctx, &paho.Publish{
				Topic: topic, Payload: payload, QoS: 0,
			})
		}
	}()
	wg.Wait()

	target := int64(b.N)
	deadline := time.Now().Add(30 * time.Second)
	for received.Load() < target && time.Now().Before(deadline) {
		time.Sleep(time.Microsecond)
	}
	b.StopTimer()
	if received.Load() < target {
		b.Fatalf("firehose: received %d/%d after publisher done", received.Load(), target)
	}
}

func sub_autopaho(b *testing.B, payload []byte) {
	topic := "bench/sub/" + uniqueID("")
	pub := connectAutopaho(b, uniqueID("autopaho-sub-pub"))
	_, ch := subscribeAutopaho(b, uniqueID("autopaho-sub-sub"), topic)
	ctx := context.Background()

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range b.N {
			_, _ = pub.Publish(ctx, &paho.Publish{
				Topic: topic, Payload: payload, QoS: 1,
			})
		}
	}()
	for range b.N {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			b.Fatal("sub: stalled waiting for delivery")
		}
	}
	wg.Wait()
}
