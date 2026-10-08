// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"fmt"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkE2E_SlowConsumer shows what a library holds in memory when
// its consumer stops. The consumer blocks on its first message while the
// raw publisher sends b.N QoS 0 messages; once the client has stopped
// taking in data the consumer is released and drains. peak-heap-B is the
// largest growth of the live Go heap over the run, delivered-% how many
// messages reached the consumer; the rest were dropped by the library.
// A library whose consumer holds back its read loop stops reading, and
// the backlog stays in the broker. Run with a fixed iteration count,
// e.g. -benchtime=200000x; ns/op is not meaningful here.
func BenchmarkE2E_SlowConsumer(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, m := range l.modes {
			b.Run(fmt.Sprintf("lib=%s/mode=%s/size=%s", l.name, m, size1KiB.name), func(b *testing.B) {
				runSlowConsumer(b, l, m, size1KiB.bytes)
			})
		}
	}
}

func runSlowConsumer(b *testing.B, l lib, m mode, size int) {
	topic := "bench/slow/" + uniqueID("")
	release := make(chan struct{})
	var delivered atomic.Int64
	ready := make(chan struct{})
	var once sync.Once
	sub := l.subscribe(b, clientConfig{id: uniqueID(l.name + "-slow")}, topic, 0, m, 1, func(p []byte) {
		if len(p) == 0 {
			once.Do(func() { close(ready) })
			return
		}
		<-release
		delivered.Add(1)
	})
	pub := dialRaw(b, uniqueID("raw-pub"))
	waitLive(b, &sink{ready: ready}, func() error { return pub.publish(topic, 0, nil, 1) })

	runtime.GC()
	sampler := startHeapSampler()
	b.ResetTimer()
	if err := pub.publish(topic, 0, Payload(size), b.N); err != nil {
		b.Fatalf("raw publisher: %v", err)
	}
	// The client has stopped taking in data once neither its heap nor
	// its drop count has grown for a while.
	settled := func() (uint64, int64) { return sampler.current(), sub.dropped() }
	h, d := settled()
	for still := 0; still < 5; {
		time.Sleep(100 * time.Millisecond)
		h2, d2 := settled()
		if h2 <= h+h/100 && d2 == d {
			still++
		} else {
			still = 0
		}
		h, d = h2, d2
	}
	close(release)
	deadline := time.Now().Add(2 * time.Minute)
	for delivered.Load()+sub.dropped() < int64(b.N) {
		if time.Now().After(deadline) {
			b.Fatalf("delivered %d and dropped %d of %d", delivered.Load(), sub.dropped(), b.N)
		}
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()
	peak := sampler.stop()
	b.ReportMetric(float64(peak), "peak-heap-B")
	b.ReportMetric(100*float64(delivered.Load())/float64(b.N), "delivered-%")
}

// heapSampler records the largest live heap seen above the level at
// which it started.
type heapSampler struct {
	base  uint64
	peak  atomic.Uint64
	last  atomic.Uint64
	stopc chan struct{}
	done  chan struct{}
}

const heapMetric = "/memory/classes/heap/objects:bytes"

func heapBytes() uint64 {
	s := []metrics.Sample{{Name: heapMetric}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

func startHeapSampler() *heapSampler {
	h := &heapSampler{base: heapBytes(), stopc: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(h.done)
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-h.stopc:
				return
			case <-t.C:
				v := heapBytes()
				h.last.Store(v)
				if v > h.base && v-h.base > h.peak.Load() {
					h.peak.Store(v - h.base)
				}
			}
		}
	}()
	return h
}

func (h *heapSampler) current() uint64 { return h.last.Load() }

func (h *heapSampler) stop() uint64 {
	close(h.stopc)
	<-h.done
	return h.peak.Load()
}
