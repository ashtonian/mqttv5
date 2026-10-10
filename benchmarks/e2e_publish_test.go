// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkE2E_Publish measures one publisher calling Publish in a loop
// against the broker: ns/op is the time per call, which at QoS 1 and 2
// includes the broker's acknowledgements. A QoS 0 call returns once the
// packet is written to the connection in every library (mqttv5 with
// PublishWaitForFlush), so the libraries complete the same work.
func BenchmarkE2E_Publish(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, qos := range []byte{0, 1, 2} {
			for _, sz := range []size{size64B, size1KiB, size1MiB} {
				b.Run(fmt.Sprintf("lib=%s/qos=%d/size=%s", l.name, qos, sz.name), func(b *testing.B) {
					p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pub"), written: true})
					topic := "bench/publish/" + uniqueID("")
					payload := Payload(sz.bytes)
					ctx := context.Background()

					b.SetBytes(int64(sz.bytes))
					b.ReportAllocs()
					b.ResetTimer()
					cpu := startCPU(b)
					for range b.N {
						if err := p.publish(ctx, topic, qos, payload); err != nil {
							b.Fatalf("publish: %v", err)
						}
					}
					b.StopTimer()
					cpu.stop()
				})
			}
		}
	}
}

// BenchmarkE2E_PublishConcurrent runs a fixed number of goroutines
// publishing QoS 1 on one client, each waiting for its PUBACK before the
// next publish. ns/op is aggregate: elapsed time divided by messages
// acknowledged across all workers. Every library keeps within the
// broker's Receive Maximum (20 on the benchmark broker), so with 64
// workers the MQTT 5 clients queue for send quota; MQTT 3.1.1 has no
// such limit.
func BenchmarkE2E_PublishConcurrent(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, workers := range []int{8, 64} {
			b.Run(fmt.Sprintf("lib=%s/workers=%d/size=%s", l.name, workers, size256B.name), func(b *testing.B) {
				p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pubc")})
				topic := "bench/concurrent/" + uniqueID("")
				payload := Payload(size256B.bytes)

				var failed atomic.Pointer[error]
				start := make(chan struct{})
				var wg sync.WaitGroup
				for w := range workers {
					n := b.N / workers
					if w < b.N%workers {
						n++
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						labels := pprof.Labels("bench-worker", strconv.Itoa(w))
						pprof.Do(context.Background(), labels, func(ctx context.Context) {
							<-start
							for range n {
								if err := p.publish(ctx, topic, 1, payload); err != nil {
									failed.CompareAndSwap(nil, &err)
									return
								}
							}
						})
					}()
				}

				b.SetBytes(int64(size256B.bytes))
				b.ReportAllocs()
				b.ResetTimer()
				cpu := startCPU(b)
				close(start)
				wg.Wait()
				b.StopTimer()
				cpu.stop()
				if err := failed.Load(); err != nil {
					b.Fatalf("publish: %v", *err)
				}
			})
		}
	}
}
