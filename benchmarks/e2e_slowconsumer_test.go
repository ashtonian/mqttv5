// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"fmt"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkE2E_SlowConsumer shows what a library holds in memory when
// its consumer stops. The consumer blocks on its first message while the
// raw publisher sends b.N messages at QoS 0 or 1; once the client has
// stopped taking in data the consumer is released and drains. held-B
// is what the library holds at that point: how far the process's live
// heap, measured after a collection, has grown over its level before
// the burst. delivered-% is how many messages reached the consumer
// intact; the library dropped, and counted, the rest.
// A library whose consumer holds back its read loop stops reading, and
// the backlog stays in the broker; so does one that leaves QoS 1
// messages unacknowledged until its consumer takes them. Run with a
// fixed iteration count, e.g. -benchtime=200000x; ns/op is not
// meaningful here.
func BenchmarkE2E_SlowConsumer(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, m := range l.modes {
			for _, qos := range []byte{0, 1} {
				b.Run(fmt.Sprintf("lib=%s/mode=%s/qos=%d/size=%s", l.name, m, qos, size1KiB.name), func(b *testing.B) {
					runSlowConsumer(b, l, m, qos, size1KiB.bytes)
				})
			}
		}
	}
}

func runSlowConsumer(b *testing.B, l lib, m mode, qos byte, size int) {
	topic := "bench/slow/" + uniqueID("")
	release := make(chan struct{})
	// The library may drop what does not fit; the rest must arrive once
	// and intact.
	s := newSink(size, b.N)
	s.allowDrops = true
	var delivered atomic.Int64
	sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-slow")}, topic, qos, m, 1, func(p []byte) {
		if len(p) > 0 {
			<-release
			delivered.Add(1)
		}
		s.onMsg(p)
	})
	pub := dialRaw(b, uniqueID("raw-pub"))
	waitLive(b, s, func() error { return pub.publish(topic, 0, nil, 1) })

	runtime.GC()
	base := heapBytes()
	b.ResetTimer()
	if err := pub.publish(topic, qos, Payload(size), b.N); err != nil {
		b.Fatalf("raw publisher: %v", err)
	}
	// The client has stopped taking in data once neither its heap nor
	// its drop count has grown for a while.
	settled := func() (uint64, int64) { return heapBytes(), sub.dropped() }
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
	// Garbage is collected first: what is left is what the library
	// keeps, not how far the collector let the heap run.
	runtime.GC()
	held := max(int64(heapBytes())-int64(base), 0)
	close(release)
	deadline := time.Now().Add(2 * time.Minute)
	for delivered.Load()+sub.dropped() < int64(b.N) {
		select {
		case <-s.invalid:
			b.Fatalf("invalid delivery: %s", s)
		default:
		}
		if time.Now().After(deadline) {
			b.Fatalf("delivered %d and dropped %d of %d", delivered.Load(), sub.dropped(), b.N)
		}
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()
	s.check(b, sub)
	b.ReportMetric(float64(held), "held-B")
	b.ReportMetric(100*float64(s.got.Load())/float64(b.N), "delivered-%")
}

const heapMetric = "/memory/classes/heap/objects:bytes"

func heapBytes() uint64 {
	s := []metrics.Sample{{Name: heapMetric}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}
