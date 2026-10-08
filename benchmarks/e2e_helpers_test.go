// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

// The end-to-end benchmarks run each client library against the same
// live broker. Every scenario is written once against the lib adapter
// (e2e_libs_test.go), so all libraries run the same workload; scenarios
// that measure only the receiving side are fed by a library-neutral load
// generator (rawClient) so the publisher's cost is not attributed to the
// library under test.
//
// Sub-benchmark names are key=value pairs (lib=mqttv5/qos=1/size=64B)
// so cmd/benchtab can build tables from raw results. See README.md.
package benchmarks

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"
)

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

// brokerURL is the broker under test: MQTT_BROKER, or the benchmark
// broker from docker-compose.yml.
func brokerURL() string {
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		return v
	}
	return "mqtt://127.0.0.1:1893"
}

func brokerAddr(b *testing.B) string {
	b.Helper()
	u, err := url.Parse(brokerURL())
	if err != nil {
		b.Fatalf("MQTT_BROKER: %v", err)
	}
	return u.Host
}

// requireBroker skips the benchmark when no broker is listening.
func requireBroker(b *testing.B) {
	b.Helper()
	c, err := net.DialTimeout("tcp", brokerAddr(b), 500*time.Millisecond)
	if err != nil {
		b.Skipf("broker %s unreachable (%v): docker compose -f benchmarks/docker-compose.yml up -d", brokerURL(), err)
	}
	_ = c.Close()
}

// uniqueID returns a fresh client ID or topic segment: a reused client
// ID would take over another benchmark's session.
func uniqueID(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

type size struct {
	name  string
	bytes int
}

var (
	size64B  = size{"64B", 64}
	size256B = size{"256B", 256}
	size1KiB = size{"1KiB", 1 << 10}
	size1MiB = size{"1MiB", 1 << 20}
)

// cpuTime is the CPU time this process has used, user plus system.
func cpuTime(b *testing.B) time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		b.Fatalf("getrusage: %v", err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// cpuMeter reports the process CPU time per operation as cpu-ns/op:
// what the client costs per message regardless of whether the broker or
// the client limited the rate. Start it where the timer starts.
type cpuMeter struct {
	b     *testing.B
	start time.Duration
}

func startCPU(b *testing.B) *cpuMeter { return &cpuMeter{b: b, start: cpuTime(b)} }

func (m *cpuMeter) stop() {
	used := cpuTime(m.b) - m.start
	m.b.ReportMetric(float64(used.Nanoseconds())/float64(m.b.N), "cpu-ns/op")
}

// await waits for done or fails the benchmark after d with what.
func await(b *testing.B, done <-chan struct{}, d time.Duration, what func() string) {
	b.Helper()
	select {
	case <-done:
	case <-time.After(d):
		b.Fatalf("timed out after %v: %s", d, what())
	}
}
