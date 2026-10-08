// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"context"
	"encoding/binary"
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
					topic := "bench/roundtrip/" + uniqueID("")
					arrived := make(chan int, 1)
					ready := make(chan struct{})
					var once sync.Once
					l.subscribe(b, clientConfig{id: uniqueID(l.name + "-sub")}, topic, qos, modeCallback, 1, func(p []byte) {
						if len(p) == 0 {
							once.Do(func() { close(ready) })
							return
						}
						arrived <- len(p)
					})
					p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pub")})
					ctx := context.Background()
					probe := &sink{ready: ready}
					waitLive(b, probe, func() error { return p.publish(ctx, topic, 0, nil) })
					payload := Payload(sz.bytes)

					b.SetBytes(int64(sz.bytes))
					b.ReportAllocs()
					b.ResetTimer()
					for i := range b.N {
						if err := p.publish(ctx, topic, qos, payload); err != nil {
							b.Fatalf("publish %d: %v", i, err)
						}
						select {
						case n := <-arrived:
							if n != sz.bytes {
								b.Fatalf("message %d arrived with %d bytes, want %d", i, n, sz.bytes)
							}
						case <-time.After(10 * time.Second):
							b.Fatalf("message %d never arrived", i)
						}
					}
				})
			}
		}
	}
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
// are in nanoseconds; ns/op is just the schedule interval. Run with a
// fixed iteration count, e.g. -benchtime=20000x.
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
	latency := make([]time.Duration, n)
	var start atomic.Pointer[time.Time]
	var got atomic.Int64
	done := make(chan struct{})
	ready := make(chan struct{})
	var once sync.Once
	l.subscribe(b, clientConfig{id: uniqueID(l.name + "-sub")}, topic, qos, modeCallback, 1, func(p []byte) {
		if len(p) == 0 {
			once.Do(func() { close(ready) })
			return
		}
		seq := binary.BigEndian.Uint64(p)
		t0 := start.Load()
		if t0 == nil || seq >= uint64(n) || latency[seq] != 0 {
			return // a probe or a QoS 1 duplicate
		}
		latency[seq] = time.Since(t0.Add(time.Duration(seq) * interval))
		if got.Add(1) == int64(n) {
			close(done)
		}
	})
	p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pub")})
	ctx := context.Background()
	waitLive(b, &sink{ready: ready}, func() error { return p.publish(ctx, topic, 0, nil) })

	payloads := make([][]byte, n)
	slab := make([]byte, n*size)
	for i := range payloads {
		payloads[i] = slab[i*size : (i+1)*size : (i+1)*size]
		copy(payloads[i], Payload(size))
		binary.BigEndian.PutUint64(payloads[i], uint64(i))
	}

	jobs := make(chan int, n)
	var failed atomic.Pointer[error]
	var wg sync.WaitGroup
	for range latencyWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := p.publish(ctx, topic, qos, payloads[i]); err != nil {
					failed.CompareAndSwap(nil, &err)
				}
			}
		}()
	}

	b.ResetTimer()
	t0 := time.Now()
	start.Store(&t0)
	for i := range n {
		sleepUntil(t0.Add(time.Duration(i) * interval))
		jobs <- i
	}
	close(jobs)
	await(b, done, time.Minute, func() string { return fmt.Sprintf("received %d of %d", got.Load(), n) })
	b.StopTimer()
	wg.Wait()
	if err := failed.Load(); err != nil {
		b.Fatalf("publish: %v", *err)
	}

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
