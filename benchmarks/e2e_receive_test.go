// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"fmt"
	"testing"
	"time"
)

// receiveWindow is the Receive Maximum the MQTT 5 subscribers advertise:
// the benchmark broker's in-flight limit towards MQTT 3.1.1 clients, so
// every library receives QoS 1 with the same window.
const receiveWindow = 20

// BenchmarkE2E_Receive measures a subscriber of the library under test
// fed by the library-neutral raw publisher at the rate the broker
// sustains. ns/op is the time per delivered message; allocs/op and
// cpu-ns/op are the subscriber's cost (the raw publisher allocates
// nothing per message and spends little CPU). Every message is checked
// for size and counted; a lost or dropped message fails the run.
//
// QoS 0 is measured with the callback mode only: a library's read loop
// is then held back by the handler, so nothing is dropped. Channel and
// queue consumers run at QoS 1, where the receive window bounds how far
// the broker gets ahead of them.
func BenchmarkE2E_Receive(b *testing.B) {
	requireBroker(b)
	type variant struct {
		mode      mode
		consumers int
		qos       byte
	}
	variants := []variant{
		{modeCallback, 1, 0},
		{modeCallback, 1, 1},
		{modeChan, 1, 1}, {modeChan, 4, 1}, {modeChan, 8, 1},
		{modeQueue, 1, 1}, {modeQueue, 4, 1}, {modeQueue, 8, 1},
	}
	for _, l := range libs {
		for _, v := range variants {
			if !l.supports(v.mode) {
				continue
			}
			for _, sz := range []size{size64B, size1KiB} {
				name := fmt.Sprintf("lib=%s/mode=%s/consumers=%d/qos=%d/size=%s", l.name, v.mode, v.consumers, v.qos, sz.name)
				b.Run(name, func(b *testing.B) {
					topic := "bench/receive/" + uniqueID("")
					s := newSink(sz.bytes, b.N)
					sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-sub"), receiveMaximum: receiveWindow},
						topic, v.qos, v.mode, v.consumers, s.onMsg)
					pub := dialRaw(b, uniqueID("raw-pub"))
					waitLive(b, s, func() error { return pub.publish(topic, 0, nil, 1) })
					payload := Payload(sz.bytes)

					b.SetBytes(int64(sz.bytes))
					b.ReportAllocs()
					b.ResetTimer()
					cpu := startCPU(b)
					published := pub.publishAsync(topic, v.qos, payload, b.N)
					s.wait(b, 2*time.Minute)
					b.StopTimer()
					cpu.stop()
					if err := published(); err != nil {
						b.Fatalf("raw publisher: %v", err)
					}
					s.check(b, sub)
				})
			}
		}
	}
}
