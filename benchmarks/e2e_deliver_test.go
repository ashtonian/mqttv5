// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"bytes"
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

// Tier-2 end-to-end DELIVERY benchmarks: one publisher client + one
// subscriber client, on two separate connections, through the broker.
// These generalise the QoS-1-only RoundTrip / Subscribe benches to the
// full QoS 0/1/2 matrix and validate the delivered payload once outside
// the timed region so a dropped or corrupted delivery path fails the
// bench instead of benchmarking garbage. The isolated publish/subscribe
// micro-benches (e2e_mqttv5_test.go, e2e_autopaho_test.go) are left
// untouched.

// e2ePair is a publisher + subscriber pair through the broker. publish
// sends one message at the fixture's QoS to the fixture's topic; recv
// yields each delivered payload (decoupled from the underlying client
// type so the latency/throughput loops are library-agnostic). The
// fixture owns acking — for mqttv5 it acks inside the recv path so the
// timed loop never has to know about per-message Ack.
type e2ePair struct {
	publish func(payload []byte) error
	recv    <-chan []byte
	qos     byte
}

// newPair_mqttv5 connects a publisher + subscriber (two connections),
// subscribes the subscriber to a unique topic at qos, and returns a pair
// that publishes at qos and surfaces delivered payloads on recv. The
// subscriber acks each message as it is forwarded, so QoS 1/2 flow
// control completes without the benchmark loop touching Message.Ack.
func newPair_mqttv5(b *testing.B, qos byte) *e2ePair {
	b.Helper()
	topic := "bench/deliver/" + uniqueID("")
	pub := connectMqttv5(b, uniqueID("mqttv5-deliver-pub"))
	sub := connectMqttv5(b, uniqueID("mqttv5-deliver-sub"))

	src, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: qos}}, mqttv5.SubBuffer(1024))
	if err != nil {
		b.Fatalf("mqttv5 Subscribe: %v", err)
	}
	// Brief pause so the broker installed the subscription before the
	// publisher fires anything, mirroring subscribeMqttv5.
	time.Sleep(100 * time.Millisecond)

	// Forward delivered payloads onto a typed channel, acking each so
	// QoS 1/2 redelivery doesn't stall. The payload is copied off the
	// frame before Ack because Ack invalidates the embedded fields.
	out := make(chan []byte, 1024)
	go func() {
		for m := range src {
			out <- bytes.Clone(m.Payload)
			_ = m.Ack()
		}
	}()

	opts := wire.PublishOpts{Topic: topic, QoS: qos}
	return &e2ePair{
		publish: func(payload []byte) error {
			opts.Payload = payload
			return pub.Publish(context.Background(), opts)
		},
		recv: out,
		qos:  qos,
	}
}

// newPair_autopaho is the autopaho counterpart of newPair_mqttv5. The
// subscriber wires OnPublishReceived -> channel (the sub_autopaho
// pattern) at NewConnection time; autopaho acks QoS 1/2 internally, so
// the handler only forwards the payload.
func newPair_autopaho(b *testing.B, qos byte) *e2ePair {
	b.Helper()
	topic := "bench/deliver/" + uniqueID("")
	pub := connectAutopaho(b, uniqueID("autopaho-deliver-pub"))

	u, err := url.Parse(brokerURL())
	if err != nil {
		b.Fatal(err)
	}
	out := make(chan []byte, 1024)

	// CM lifetime context — must not be cancelled until Cleanup.
	cmCtx, cmCancel := context.WithCancel(context.Background())
	cm, err := autopaho.NewConnection(cmCtx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     60,
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         0,
		ConnectTimeout:                5 * time.Second,
		ClientConfig: paho.ClientConfig{
			ClientID: uniqueID("autopaho-deliver-sub"),
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					// Copy the payload — the packet is reused after the
					// callback returns. Block on the send (do NOT drop on
					// a full channel) so a slow receiver throttles paho's
					// reader/ack loop instead of silently losing messages.
					// This mirrors newPair_mqttv5's blocking forward and
					// keeps the QoS-0 throughput sweep from stalling at
					// b.N when the firehose outruns the single-goroutine
					// drain. cmCtx.Done() unblocks at Cleanup so the
					// callback can't wedge the CM on shutdown.
					select {
					case out <- bytes.Clone(pr.Packet.Payload):
					case <-cmCtx.Done():
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
		Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: qos}},
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

	return &e2ePair{
		publish: func(payload []byte) error {
			_, err := pub.Publish(context.Background(), &paho.Publish{
				Topic: topic, Payload: payload, QoS: qos,
			})
			return err
		},
		recv: out,
		qos:  qos,
	}
}

// deliverPairs maps each library to its pair constructor, mirroring the
// libs dispatch table. Both libraries implement delivery for every QoS.
var deliverPairs = []struct {
	name    string
	newPair func(b *testing.B, qos byte) *e2ePair
}{
	{"mqttv5", newPair_mqttv5},
	{"autopaho", newPair_autopaho},
}

// validateDelivery publishes one sentinel payload at the fixture's QoS
// and asserts the subscriber receives it back byte-for-byte, failing the
// benchmark on any mismatch. Run before b.ResetTimer so the timed loop
// is guaranteed to exercise a verified-correct delivery path. The
// sentinel is the same size as the loop payload so the fixture warms the
// same buffers.
//
// The delivered QoS is intentionally NOT re-asserted here: e2ePair.recv
// is a library-agnostic <-chan []byte that deliberately strips the
// message header (mqttv5.Message / paho.Publish) down to payload bytes,
// so the QoS field is unrecoverable at this layer. The fixture still
// pins the wire QoS — both newPair_* subscribe AND publish at p.qos —
// and RFC §4.1's require.EqualValues(qos, m.QoS) check is covered by the
// conformance suite (which keeps the typed message), not by the
// throughput/latency benches.
func validateDelivery(b *testing.B, p *e2ePair, size int) {
	b.Helper()
	sentinel := Payload(size)
	if err := p.publish(sentinel); err != nil {
		b.Fatalf("validate publish: %v", err)
	}
	select {
	case got := <-p.recv:
		if !bytes.Equal(sentinel, got) {
			b.Fatalf("validate: delivered payload mismatch: got %d bytes, want %d bytes (qos %d)",
				len(got), len(sentinel), p.qos)
		}
	case <-time.After(5 * time.Second):
		b.Fatalf("validate: timeout waiting for sentinel delivery (qos %d)", p.qos)
	}
}

// BenchmarkE2E_DeliverLatency measures end-to-end per-message delivery
// latency: publish one, block until the subscriber receives it. Swept
// over both libraries, QoS 0/1/2, and the e2e payload sizes. This
// generalises BenchmarkE2E_RoundTrip (the qos=1 row reproduces it).
func BenchmarkE2E_DeliverLatency(b *testing.B) {
	requireBroker(b)
	for _, lib := range deliverPairs {
		for _, qos := range []byte{0, 1, 2} {
			for _, sz := range e2eSizes {
				name := lib.name + "/qos" + string(rune('0'+qos)) + "/" + sz.Name
				b.Run(name, func(b *testing.B) {
					p := lib.newPair(b, qos)
					payload := Payload(sz.Size)

					validateDelivery(b, p, sz.Size)

					b.SetBytes(int64(sz.Size))
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if err := p.publish(payload); err != nil {
							b.Fatalf("Publish: %v", err)
						}
						select {
						case <-p.recv:
						case <-time.After(5 * time.Second):
							b.Fatal("deliver-latency: timeout waiting for delivery")
						}
					}
				})
			}
		}
	}
}

// BenchmarkE2E_DeliverThroughput measures sustained pub->sub throughput:
// the publisher fires b.N messages async while the main loop receives
// b.N. Swept over both libraries, QoS 0/1, and the e2e payload sizes.
// QoS 2 throughput is intentionally omitted (per RFC §7 — QoS-2
// throughput is dominated by the 4-step handshake and is rarely the
// real-world hot path; QoS 2 stays latency-only). This generalises
// BenchmarkE2E_Subscribe (the qos=1 row reproduces it).
func BenchmarkE2E_DeliverThroughput(b *testing.B) {
	requireBroker(b)
	for _, lib := range deliverPairs {
		for _, qos := range []byte{0, 1} {
			for _, sz := range e2eSizes {
				name := lib.name + "/qos" + string(rune('0'+qos)) + "/" + sz.Name
				b.Run(name, func(b *testing.B) {
					p := lib.newPair(b, qos)
					payload := Payload(sz.Size)

					validateDelivery(b, p, sz.Size)

					b.SetBytes(int64(sz.Size))
					b.ReportAllocs()
					b.ResetTimer()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range b.N {
							_ = p.publish(payload)
						}
					}()
					for range b.N {
						select {
						case <-p.recv:
						case <-time.After(10 * time.Second):
							b.Fatal("deliver-throughput: stalled waiting for delivery")
						}
					}
					wg.Wait()
				})
			}
		}
	}
}
