// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"fmt"
	"testing"
)

// Each Benchmark iterates libs × payload-size so a single
// `go test -tags e2e -bench BenchmarkE2E_PublishQoS1` produces the
// side-by-side comparison.

func BenchmarkE2E_PublishQoS0(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.pubQoS0(b, Payload(sz.Size))
			})
		}
	}
}

func BenchmarkE2E_PublishQoS1(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.pubQoS1(b, Payload(sz.Size))
			})
		}
	}
}

func BenchmarkE2E_PublishQoS2(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.pubQoS2(b, Payload(sz.Size))
			})
		}
	}
}

// BenchmarkE2E_PublishQoS0WaitForFlush measures the slower QoS 0
// path where the publisher waits for the writer goroutine to flush
// each PUBLISH before returning.
//
// For mqttv5 this uses WithPublishMode(PublishWaitForFlush); for
// autopaho its only QoS 0 Publish always waits for the writer mutex,
// so this is an apples-to-apples comparison vs the same-shape
// autopaho call. Compare against BenchmarkE2E_PublishQoS0 (default
// fire-and-forget for mqttv5) to see the cost of opting in to
// write-completion confirmation.
func BenchmarkE2E_PublishQoS0WaitForFlush(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.pubQoS0WaitForFlush(b, Payload(sz.Size))
			})
		}
	}
}

// BenchmarkE2E_SubscribeFireHose measures pure subscriber throughput.
// The publisher fires QoS 0 with no per-message wait (mqttv5 default
// PublishFireAndForget; autopaho QoS 0 Publish) so the consumer rate
// is the limiting factor.
//
// Compare with BenchmarkE2E_Subscribe (which uses QoS 1 publishes
// gated on PUBACK) to see how much of that benchmark's time was
// publisher-bound vs subscriber-bound.
func BenchmarkE2E_SubscribeFireHose(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.subFireHose(b, Payload(sz.Size))
			})
		}
	}
}

// BenchmarkE2E_PublishConcurrentQoS1 stresses N goroutines publishing
// on the same client — showcases the writer-queue / mutex behaviour
// under contention. The interesting columns here are aggregate ns/op
// (lower = more total throughput) and allocs/op (per publish).
func BenchmarkE2E_PublishConcurrentQoS1(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.pubConcQoS1(b, Payload(sz.Size))
			})
		}
	}
}

func BenchmarkE2E_RoundTrip(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.rtt(b, Payload(sz.Size))
			})
		}
	}
}

func BenchmarkE2E_Subscribe(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.sub(b, Payload(sz.Size))
			})
		}
	}
}

// BenchmarkE2E_SubscribeQueue mirrors BenchmarkE2E_Subscribe but with
// the SubscribeQueue API (q.Dequeue) instead of the channel-based
// Subscribe. Pair-compare with the same Subscribe row at the same
// payload size to isolate per-message channel-vs-queue overhead.
// mqttv5-only — autopaho has no Queue equivalent.
func BenchmarkE2E_SubscribeQueue(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		if lib.subQueue == nil {
			continue
		}
		for _, sz := range e2eSizes {
			b.Run(lib.name+"/"+sz.Name, func(b *testing.B) {
				lib.subQueue(b, Payload(sz.Size))
			})
		}
	}
}

// BenchmarkE2E_SubscribeFireHoseFanOut measures raw chan/queue
// dispatch throughput. QoS 0 fire-hose publisher (no PUBACK gating)
// pushes at line speed; N consumer goroutines drain the delivery
// surface. This is the test for "channels are locky under fan-out" —
// the QoS 1 MultiConsumer benchmark is broker-bound (~170 µs RTT
// floor) and hides per-message dispatch cost; this one isn't.
func BenchmarkE2E_SubscribeFireHoseFanOut(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, n := range multiConsumerCounts {
			for _, sz := range e2eSizes {
				if lib.fhMultiChan != nil {
					b.Run(fmt.Sprintf("%s-chan/c%d/%s", lib.name, n, sz.Name), func(b *testing.B) {
						lib.fhMultiChan(b, Payload(sz.Size), n)
					})
				}
				if lib.fhMultiQueue != nil {
					b.Run(fmt.Sprintf("%s-queue/c%d/%s", lib.name, n, sz.Name), func(b *testing.B) {
						lib.fhMultiQueue(b, Payload(sz.Size), n)
					})
				}
			}
		}
	}
}

// BenchmarkE2E_SubscribeMultiConsumer measures throughput under
// consumer fan-out: N goroutines race for messages on the same
// delivery surface. Iterates (library, consumer style, consumer
// count, payload size). Aggregate ns/op + MB/s show how each
// delivery primitive scales as fan-out grows. mqttv5 reports both
// chan and queue rows; autopaho is chan-only.
func BenchmarkE2E_SubscribeMultiConsumer(b *testing.B) {
	requireBroker(b)
	for _, lib := range libs {
		for _, n := range multiConsumerCounts {
			for _, sz := range e2eSizes {
				if lib.subMultiChan != nil {
					b.Run(fmt.Sprintf("%s-chan/c%d/%s", lib.name, n, sz.Name), func(b *testing.B) {
						lib.subMultiChan(b, Payload(sz.Size), n)
					})
				}
				if lib.subMultiQueue != nil {
					b.Run(fmt.Sprintf("%s-queue/c%d/%s", lib.name, n, sz.Name), func(b *testing.B) {
						lib.subMultiQueue(b, Payload(sz.Size), n)
					})
				}
			}
		}
	}
}
