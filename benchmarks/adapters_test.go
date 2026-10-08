// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"bytes"
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// adapterGoroutines counts goroutines running in this package's
// subscribe adapters.
func adapterGoroutines() int {
	var b bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	n := 0
	for _, g := range strings.Split(b.String(), "\n\n") {
		if strings.Contains(g, "benchmarks.subscribe") {
			n++
		}
	}
	return n
}

// Every adapter's consumers end with the benchmark that started them,
// so repeated runs do not accumulate goroutines that skew later ones.
func TestAdapterConsumersEndWithTheBenchmark(t *testing.T) {
	for _, l := range append(libs, floorLib) {
		for _, m := range l.modes {
			for _, qos := range []byte{0, 1} {
				t.Run(fmt.Sprintf("lib=%s/mode=%s/qos=%d", l.name, m, qos), func(t *testing.T) {
					before := adapterGoroutines()
					res := testing.Benchmark(func(b *testing.B) {
						srv := newRawServer(b, l.v5)
						s := newSink(64, 1)
						subscribe(b, l, clientConfig{id: uniqueID("adapter"), addr: srv.addr(), receiveMaximum: receiveWindow},
							"adapter/t", qos, m, 4, s.onMsg)
						srv.awaitSubscribed(b)
						if err := srv.publish("adapter/t", qos, Payload(64), 1); err != nil {
							b.Fatal(err)
						}
						s.wait(b, 5*time.Second)
					})
					if res.N == 0 {
						t.Fatal("the benchmark failed")
					}
					deadline := time.Now().Add(5 * time.Second)
					for adapterGoroutines() > before {
						if time.Now().After(deadline) {
							t.Fatalf("adapter goroutines after the benchmark: %d, before: %d", adapterGoroutines(), before)
						}
						time.Sleep(10 * time.Millisecond)
					}
				})
			}
		}
	}
}

// Stopping a real adapter's subscription while a duplicate is still
// being delivered waits for that delivery, so the check counts it. The
// delivery is held inside the subscription's gate, which waits for it,
// or before it, where only the library's own stop can: a library whose
// stop returned with its callback still running would let the
// duplicate reach a closed gate after the check.
func TestAdapterStopWaitsForADeliveryInProgress(t *testing.T) {
	for _, held := range []string{"inside-gate", "before-gate"} {
		for _, l := range append(libs, floorLib) {
			t.Run(fmt.Sprintf("held=%s/lib=%s", held, l.name), func(t *testing.T) {
				var checked error
				res := testing.Benchmark(func(b *testing.B) {
					entered, release := make(chan struct{}), make(chan struct{})
					var calls atomic.Int64
					hold := func(onMsg func([]byte)) func([]byte) {
						return func(p []byte) {
							if calls.Add(1) == 2 {
								close(entered)
								<-release
							}
							onMsg(p)
						}
					}
					s := newSink(64, 1)
					onMsg := s.onMsg
					if held == "inside-gate" {
						onMsg = hold(onMsg)
					} else {
						adapter := l.subscribe
						l.subscribe = func(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) subscriber {
							return adapter(b, cfg, filter, qos, m, consumers, hold(onMsg))
						}
					}
					srv := newRawServer(b, l.v5)
					sub := subscribe(b, l, clientConfig{id: uniqueID("adapter-stop"), addr: srv.addr(), receiveMaximum: receiveWindow},
						"adapter/t", 0, modeCallback, 1, onMsg)
					srv.awaitSubscribed(b)
					for range 2 {
						if err := srv.publish("adapter/t", 0, Payload(64), 1); err != nil {
							b.Fatal(err)
						}
					}
					await(b, entered, 5*time.Second, s.String)
					// Held past paho3's default 250 ms quiesce.
					go func() {
						time.Sleep(time.Second)
						close(release)
					}()
					checked = s.finish(sub, stopBound)
				})
				if res.N == 0 {
					t.Fatal("the benchmark failed")
				}
				if checked == nil || !strings.Contains(checked.Error(), "1 duplicates") {
					t.Fatalf("check = %v, want the duplicate counted", checked)
				}
			})
		}
	}
}

// A client cleanup that does not return within its bound fails the
// benchmark rather than holding it up.
func TestCleanupIsBounded(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ended := make(chan testing.BenchmarkResult, 1)
	go func() {
		ended <- testing.Benchmark(func(b *testing.B) {
			cleanupWithin(b, "stuck", 50*time.Millisecond, func(context.Context) error {
				<-release
				return nil
			})
		})
	}()
	select {
	case res := <-ended:
		if res.N != 0 {
			t.Fatal("a cleanup that did not return passed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the benchmark waited for a stuck cleanup")
	}
}
