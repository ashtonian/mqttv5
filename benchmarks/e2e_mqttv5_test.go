// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

// pubQoS0_mqttv5: tight Publish loop, QoS 0 = no broker ack required.
// Times the cost of encode + writer-queue send under the default
// PublishMode (fire-and-forget).
func pubQoS0_mqttv5(b *testing.B, payload []byte) {
	cli := connectMqttv5(b, uniqueID("mqttv5-pub0"))
	topic := "bench/pub0/" + uniqueID("")
	ctx := context.Background()
	opts := wire.PublishOpts{Topic: topic, Payload: payload, QoS: 0}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := cli.Publish(ctx, opts); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

// pubQoS1_mqttv5: each Publish waits for PUBACK. Times one full
// round-trip per call.
func pubQoS1_mqttv5(b *testing.B, payload []byte) {
	cli := connectMqttv5(b, uniqueID("mqttv5-pub1"))
	topic := "bench/pub1/" + uniqueID("")
	ctx := context.Background()
	opts := wire.PublishOpts{Topic: topic, Payload: payload, QoS: 1}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := cli.Publish(ctx, opts); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

// pubQoS2_mqttv5: each Publish runs the full QoS 2 four-step handshake.
func pubQoS2_mqttv5(b *testing.B, payload []byte) {
	cli := connectMqttv5(b, uniqueID("mqttv5-pub2"))
	topic := "bench/pub2/" + uniqueID("")
	ctx := context.Background()
	opts := wire.PublishOpts{Topic: topic, Payload: payload, QoS: 2}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := cli.Publish(ctx, opts); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

// rtt_mqttv5: pub client A publishes QoS 1, sub client B receives and
// acks. Timer covers the full broker round trip including subscriber
// dispatch.
func rtt_mqttv5(b *testing.B, payload []byte) {
	topic := "bench/rtt/" + uniqueID("")
	pub := connectMqttv5(b, uniqueID("mqttv5-rtt-pub"))
	_, ch := subscribeMqttv5(b, uniqueID("mqttv5-rtt-sub"), topic)
	ctx := context.Background()
	opts := wire.PublishOpts{Topic: topic, Payload: payload, QoS: 1}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := pub.Publish(ctx, opts); err != nil {
			b.Fatalf("Publish: %v", err)
		}
		select {
		case m := <-ch:
			_ = m.Ack()
		case <-time.After(5 * time.Second):
			b.Fatal("RTT: timeout waiting for delivery")
		}
	}
}

// pubConcQoS1_mqttv5: 8 goroutines publishing concurrently from one
// client. Exercises the writer-queue's ability to serialise network
// writes without per-call mutex contention.
func pubConcQoS1_mqttv5(b *testing.B, payload []byte) {
	cli := connectMqttv5(b, uniqueID("mqttv5-pubc"))
	topic := "bench/pubc/" + uniqueID("")
	ctx := context.Background()
	opts := wire.PublishOpts{Topic: topic, Payload: payload, QoS: 1}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := cli.Publish(ctx, opts); err != nil {
				b.Fatalf("Publish: %v", err)
			}
		}
	})
}

// pubQoS0WaitForFlush_mqttv5 exercises the slower QoS 0 path where
// the client is configured to wait for the writer goroutine to flush
// each PUBLISH before returning. Apples-to-apples comparison with
// autopaho's QoS 0 Publish (which also waits for its writer mutex).
//
// Compare with pubQoS0_mqttv5 (default PublishFireAndForget) to see
// the latency cost of opting into write-completion confirmation.
func pubQoS0WaitForFlush_mqttv5(b *testing.B, payload []byte) {
	cli := connectMqttv5(b, uniqueID("mqttv5-pubwait"),
		mqttv5.WithPublishMode(mqttv5.PublishWaitForFlush))
	topic := "bench/pubwait/" + uniqueID("")
	ctx := context.Background()
	opts := wire.PublishOpts{Topic: topic, Payload: payload, QoS: 0}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := cli.Publish(ctx, opts); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
}

// subFireHose_mqttv5 measures pure subscriber throughput. Publisher
// uses the default QoS 0 Publish (fire-and-forget, no PUBACK gating).
// Subscriber uses SubscribeCallback with an atomic counter — no
// channel buffer to fill, no drops, no spurious "stalled" failures.
//
// Final tally: time / b.N is the per-message receive cost on the
// subscriber side. MB/s is sustained subscriber throughput.
func subFireHose_mqttv5(b *testing.B, payload []byte) {
	topic := "bench/firehose/" + uniqueID("")
	pub := connectMqttv5(b, uniqueID("mqttv5-fh-pub"))
	sub := connectMqttv5(b, uniqueID("mqttv5-fh-sub"))

	received := atomic.Int64{}
	if _, err := sub.SubscribeCallback(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 0}},
		func(m *mqttv5.Message) {
			received.Add(1)
		}); err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	// Brief pause so SUBSCRIBE is installed before publishing starts.
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	opts := wire.PublishOpts{Topic: topic, Payload: payload, QoS: 0}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range b.N {
			_ = pub.Publish(ctx, opts)
		}
	}()
	wg.Wait()

	// Drain stragglers — broker may still be delivering after publisher
	// finished. Bounded wait so a wedged subscriber fails the bench.
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

// sub_mqttv5: one publisher fires QoS 1 messages async; one subscriber
// counts deliveries. Timer covers "publish N + receive N". Throughput
// = msg/sec.
func sub_mqttv5(b *testing.B, payload []byte) {
	topic := "bench/sub/" + uniqueID("")
	pub := connectMqttv5(b, uniqueID("mqttv5-sub-pub"))
	_, ch := subscribeMqttv5(b, uniqueID("mqttv5-sub-sub"), topic)
	ctx := context.Background()
	opts := wire.PublishOpts{Topic: topic, Payload: payload, QoS: 1}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range b.N {
			_ = pub.Publish(ctx, opts)
		}
	}()
	for range b.N {
		select {
		case m := <-ch:
			_ = m.Ack()
		case <-time.After(10 * time.Second):
			b.Fatal("sub: stalled waiting for delivery")
		}
	}
	wg.Wait()
}
