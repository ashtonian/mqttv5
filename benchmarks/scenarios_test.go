// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// publishFunc is a publisher made of a function.
type publishFunc func(ctx context.Context, topic string, qos byte, payload []byte) error

func (f publishFunc) publish(ctx context.Context, topic string, qos byte, payload []byte) error {
	return f(ctx, topic, qos, payload)
}

// loopbackLib is a library double whose publisher hands each payload to
// its subscriber through deliver, one at a time, without a broker: tests
// use it to make a scenario see deliveries a real library would not.
func loopbackLib(deliver func(onMsg func([]byte), p []byte)) lib {
	var mu sync.Mutex
	var onMsg func([]byte)
	return lib{name: "loopback", v5: true, modes: []mode{modeCallback},
		subscribe: func(_ *testing.B, _ clientConfig, _ string, _ byte, _ mode, _ int, f func([]byte)) subscriber {
			mu.Lock()
			onMsg = f
			mu.Unlock()
			return subscriber{dropped: noDrops, stop: func(context.Context) error { return nil }}
		},
		connect: func(*testing.B, clientConfig) publisher {
			return publishFunc(func(_ context.Context, _ string, _ byte, p []byte) error {
				mu.Lock()
				defer mu.Unlock()
				deliver(onMsg, p)
				return nil
			})
		},
	}
}

func corrupted(p []byte) []byte {
	c := bytes.Clone(p)
	c[len(c)/2] ^= 0xff
	return c
}

var faults = []struct {
	name    string
	deliver func(onMsg func([]byte), p []byte)
	ok      bool
}{
	{"intact", func(onMsg func([]byte), p []byte) { onMsg(p) }, true},
	{"corrupted", func(onMsg func([]byte), p []byte) {
		if len(p) > 0 {
			p = corrupted(p)
		}
		onMsg(p)
	}, false},
	{"duplicated", func(onMsg func([]byte), p []byte) {
		onMsg(p)
		if len(p) > 0 {
			onMsg(p)
		}
	}, false},
}

// The round-trip and latency scenarios accept a run only when every
// message arrived once and intact.
func TestScenariosCheckEveryDelivery(t *testing.T) {
	for _, f := range faults {
		t.Run("RoundTrip/"+f.name, func(t *testing.T) {
			res := testing.Benchmark(func(b *testing.B) { runRoundTrip(b, loopbackLib(f.deliver), 1, 64) })
			if (res.N > 0) != f.ok {
				t.Fatalf("run accepted: %v, want %v", res.N > 0, f.ok)
			}
		})
		t.Run("Latency/"+f.name, func(t *testing.T) {
			res := testing.Benchmark(func(b *testing.B) { runLatency(b, loopbackLib(f.deliver), 1, 10000, 256) })
			if (res.N > 0) != f.ok {
				t.Fatalf("run accepted: %v, want %v", res.N > 0, f.ok)
			}
		})
	}
}

// requireBrokerT skips t when the benchmark broker is unreachable.
func requireBrokerT(t *testing.T) {
	t.Helper()
	c, err := net.DialTimeout("tcp", brokerHost(), 500*time.Millisecond)
	if err != nil {
		t.Skipf("broker %s unreachable (%v)", brokerURL(), err)
	}
	_ = c.Close()
}

// The slow-consumer and reconnect scenarios, which go through the
// broker, reject a corrupted delivery.
func TestBrokerScenariosRejectCorruption(t *testing.T) {
	requireBrokerT(t)
	l := libs[0]
	t.Run("SlowConsumer", func(t *testing.T) {
		corrupting := l
		corrupting.subscribe = func(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) subscriber {
			return l.subscribe(b, cfg, filter, qos, m, consumers, func(p []byte) {
				if len(p) > 0 {
					p = corrupted(p)
				}
				onMsg(p)
			})
		}
		res := testing.Benchmark(func(b *testing.B) { runSlowConsumer(b, corrupting, modeCallback, 1024) })
		if res.N > 0 {
			t.Fatal("a run with corrupted deliveries was accepted")
		}
	})
	t.Run("Reconnect", func(t *testing.T) {
		corrupting := l
		corrupting.connect = func(b *testing.B, cfg clientConfig) publisher {
			p := l.connect(b, cfg)
			return publishFunc(func(ctx context.Context, topic string, qos byte, payload []byte) error {
				if len(payload) > 0 {
					payload = corrupted(payload)
				}
				return p.publish(ctx, topic, qos, payload)
			})
		}
		res := testing.Benchmark(func(b *testing.B) { runReconnect(b, corrupting) })
		if res.N > 0 {
			t.Fatal("a run with corrupted deliveries was accepted")
		}
	})
}
