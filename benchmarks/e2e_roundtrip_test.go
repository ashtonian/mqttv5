// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkE2E_RoundTrip publishes one message at a time from a client
// of the library under test to a subscriber of the same library and
// waits for it to arrive before the next: ns/op is the closed-loop
// publish-to-delivery latency, allocations are both clients'.
func BenchmarkE2E_RoundTrip(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, qos := range []byte{0, 1, 2} {
			for _, sz := range []size{size64B, size1KiB, size1MiB} {
				b.Run(fmt.Sprintf("lib=%s/qos=%d/size=%s", l.name, qos, sz.name), func(b *testing.B) {
					runRoundTrip(b, l, qos, sz.bytes)
				})
			}
		}
	}
}

func runRoundTrip(b *testing.B, l lib, qos byte, size int) {
	topic := "bench/roundtrip/" + uniqueID("")
	s := newSink(size, b.N)
	arrived := make(chan uint64, 1)
	sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-sub")}, topic, qos, modeCallback, 1, func(p []byte) {
		if len(p) > 0 {
			// Torn stamps are the sink's to report, and so is a delivery
			// the loop is not waiting for.
			seq, _ := readSeq(p)
			select {
			case arrived <- seq:
			default:
			}
		}
		s.onMsg(p)
	})
	p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pub")})
	ctx := context.Background()
	waitLive(b, s, func() error { return p.publish(ctx, topic, 0, nil) })
	// One buffer, stamped for each message: the previous one has
	// arrived, so no library still holds it.
	payload := Payload(size)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		stampSeq(payload, uint64(i))
		if err := p.publish(ctx, topic, qos, payload); err != nil {
			b.Fatalf("publish %d: %v", i, err)
		}
		select {
		case seq := <-arrived:
			if seq != uint64(i) {
				b.Fatalf("waiting for message %d, message %d arrived; %s", i, seq, s)
			}
		case <-time.After(10 * time.Second):
			b.Fatalf("message %d never arrived", i)
		}
	}
	b.StopTimer()
	s.check(b, sub)
}

// latencyWorkers publish on behalf of the open-loop pacer, so a slow
// acknowledgement delays later sends only once every worker is busy.
const latencyWorkers = 32

// BenchmarkE2E_Latency is an open-loop latency test: messages are
// published on a fixed schedule (rate per second) regardless of how fast
// earlier ones complete, and each message's latency is measured from the
// time it was scheduled to the time the subscriber receives it. A
// stalled client therefore shows up as latency instead of as a lower
// send rate (no coordinated omission). The p50/p90/p99/p99.9/max metrics
// are in nanoseconds; ns/op, the schedule plus the wait for the last
// deliveries over N, is about the schedule interval. Run with a fixed
// iteration count, e.g. -benchtime=20000x.
func BenchmarkE2E_Latency(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, qos := range []byte{0, 1} {
			for _, rate := range []int{1000, 10000} {
				b.Run(fmt.Sprintf("lib=%s/qos=%d/rate=%d/size=%s", l.name, qos, rate, size256B.name), func(b *testing.B) {
					runLatency(b, l, qos, rate, size256B.bytes)
				})
			}
		}
	}
}

func runLatency(b *testing.B, l lib, qos byte, rate, size int) {
	topic := "bench/latency/" + uniqueID("")
	interval := time.Second / time.Duration(rate)
	n := b.N
	s := newSink(size, n)
	// Written by the subscriber's one callback goroutine, read once the
	// subscription has stopped.
	latency := make([]time.Duration, n)
	var start atomic.Pointer[time.Time]
	sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-sub")}, topic, qos, modeCallback, 1, func(p []byte) {
		if t0 := start.Load(); t0 != nil && len(p) > 0 {
			// The first delivery of each message counts; the sink fails
			// the run on any other.
			if seq, ok := readSeq(p); ok && seq < uint64(n) && latency[seq] == 0 {
				latency[seq] = time.Since(t0.Add(time.Duration(seq) * interval))
			}
		}
		s.onMsg(p)
	})
	p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pub")})
	ctx := context.Background()
	waitLive(b, s, func() error { return p.publish(ctx, topic, 0, nil) })

	payloads := make([][]byte, n)
	slab := make([]byte, n*size)
	for i := range payloads {
		payloads[i] = slab[i*size : (i+1)*size : (i+1)*size]
		copy(payloads[i], Payload(size))
		stampSeq(payloads[i], uint64(i))
	}

	jobs := make(chan int, n)
	var failed atomic.Pointer[error]
	var wg sync.WaitGroup
	for range latencyWorkers {
		wg.Go(func() {
			for i := range jobs {
				if err := p.publish(ctx, topic, qos, payloads[i]); err != nil {
					failed.CompareAndSwap(nil, &err)
				}
			}
		})
	}

	b.ResetTimer()
	t0 := time.Now()
	start.Store(&t0)
	for i := range n {
		sleepUntil(t0.Add(time.Duration(i) * interval))
		jobs <- i
	}
	close(jobs)
	s.wait(b, time.Minute)
	b.StopTimer()
	wg.Wait()
	if err := failed.Load(); err != nil {
		b.Fatalf("publish: %v", *err)
	}
	s.check(b, sub)

	slices.Sort(latency)
	at := func(q float64) float64 { return float64(latency[int(q*float64(n-1))].Nanoseconds()) }
	b.ReportMetric(at(0.50), "p50-ns")
	b.ReportMetric(at(0.90), "p90-ns")
	b.ReportMetric(at(0.99), "p99-ns")
	b.ReportMetric(at(0.999), "p99.9-ns")
	b.ReportMetric(float64(latency[n-1].Nanoseconds()), "max-ns")
}

// sleepUntil sleeps until t, spinning for the last stretch because a
// timer can wake late by more than a schedule interval.
func sleepUntil(t time.Time) {
	const spin = 200 * time.Microsecond
	if d := time.Until(t) - spin; d > 0 {
		time.Sleep(d)
	}
	for time.Now().Before(t) {
	}
}
