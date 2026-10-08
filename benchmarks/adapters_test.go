// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"bytes"
	"fmt"
	"runtime/pprof"
	"strings"
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
						l.subscribe(b, clientConfig{id: uniqueID("adapter"), addr: srv.addr(), receiveMaximum: receiveWindow},
							"adapter/t", qos, m, 4, s.onMsg)
						srv.awaitSubscribed(b)
						if err := srv.publish("adapter/t", qos, Payload(64), 1); err != nil {
							b.Fatal(err)
						}
						await(b, s.done, 5*time.Second, s.String)
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
