// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import "testing"

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
