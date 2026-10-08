// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
)

// soakDuration is how long TestSoak_DeliveryAcrossConnectionCuts
// publishes: MQTTV5_SOAK (a Go duration) or a few seconds, so the
// regular suite runs it briefly and the nightly job for minutes.
func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	if v := os.Getenv("MQTTV5_SOAK"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("MQTTV5_SOAK=%q: %v", v, err)
		}
		return d
	}
	return 3 * time.Second
}

// Publishers whose connection a proxy cuts at random moments keep their
// session (Session Expiry 300 s), so every message they publish arrives:
// QoS 2 exactly once, QoS 1 at least once. A Publish made while the
// connection is down returns ErrNotConnected without sending, and is
// retried once the client has reconnected; every other Publish returns
// success. A subscriber on a stable connection counts arrivals.
func TestSoak_DeliveryAcrossConnectionCuts(t *testing.T) {
	requireBroker(t, brokerURL())
	duration := soakDuration(t)
	topic := "conformance/soak/" + randSuffix()

	var mu sync.Mutex
	arrived := map[string]int{}
	var subTrace *packetTrace
	subOpts := []mqttv5.Option{}
	if os.Getenv("MQTTV5_SOAK_TRACE") != "" {
		subPx := newFaultProxy(t, stripScheme(brokerURL()), true)
		subTrace = subPx.trace
		subOpts = append(subOpts, mqttv5.WithBroker(subPx.url()))
	}
	sub := connect(t, subOpts...)
	if _, err := sub.SubscribeCallback(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic + "/#", QoS: 2}},
		func(m *mqttv5.Message) {
			mu.Lock()
			arrived[string(m.Payload)]++
			mu.Unlock()
		}); err != nil {
		t.Fatal(err)
	}

	px := newFaultProxy(t, stripScheme(brokerURL()), os.Getenv("MQTTV5_SOAK_TRACE") != "")
	pub := connect(t,
		mqttv5.WithBroker(px.url()),
		mqttv5.WithSessionExpiry(300),
		mqttv5.WithReconnectBackoff(mqttv5.ConstantBackoff(5*time.Millisecond)),
		mqttv5.WithStats(),
	)
	time.Sleep(100 * time.Millisecond)

	stop := make(chan struct{})
	var cuts atomic.Int64
	go func() {
		rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(20+rng.IntN(280)) * time.Millisecond):
				cuts.Add(int64(px.cut()))
			}
		}
	}()

	const workers = 8
	type sent struct {
		id  string
		qos byte
	}
	var sentMu sync.Mutex
	var published []sent
	var failures []error
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; time.Now().Before(deadline); seq++ {
				qos := byte(1 + (w+seq)%2)
				id := fmt.Sprintf("w%d-%d", w, seq)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := pub.Publish(ctx, mqttv5.PublishOptions{Topic: fmt.Sprintf("%s/%d", topic, w), QoS: qos, Payload: []byte(id)})
				for errors.Is(err, mqttv5.ErrNotConnected) {
					// Not sent: wait for the reconnect and try again.
					if err = pub.AwaitConnection(ctx); err == nil {
						err = pub.Publish(ctx, mqttv5.PublishOptions{Topic: fmt.Sprintf("%s/%d", topic, w), QoS: qos, Payload: []byte(id)})
					}
				}
				cancel()
				sentMu.Lock()
				if err != nil {
					failures = append(failures, fmt.Errorf("%s (QoS %d): %w", id, qos, err))
				} else {
					published = append(published, sent{id, qos})
				}
				sentMu.Unlock()
				if err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	for _, err := range failures {
		t.Error(err)
	}

	missing := func() []string {
		mu.Lock()
		defer mu.Unlock()
		var out []string
		for _, p := range published {
			if arrived[p.id] == 0 {
				out = append(out, p.id)
			}
		}
		return out
	}
	for end := time.Now().Add(30 * time.Second); len(missing()) > 0 && time.Now().Before(end); {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // room for a late duplicate to show
	if m := missing(); len(m) > 0 {
		t.Errorf("%d of %d messages never arrived, e.g. %v", len(m), len(published), m[:min(5, len(m))])
		if px.trace != nil {
			for _, id := range m[:min(5, len(m))] {
				t.Logf("%s: publisher %s; subscriber %s", id, px.trace.history(id), subTrace.history(id))
			}
			if len(published) > 0 {
				ok := published[len(published)/2].id
				t.Logf("for comparison, %s (arrived): publisher %s; subscriber %s", ok, px.trace.history(ok), subTrace.history(ok))
			}
			t.Logf("CONNACKs: %s", px.trace.sessions())
		}
	}
	mu.Lock()
	duplicates := 0
	for _, p := range published {
		if p.qos == 2 && arrived[p.id] > 1 {
			t.Errorf("QoS 2 message %s arrived %d times", p.id, arrived[p.id])
		}
		if arrived[p.id] > 1 {
			duplicates++
		}
	}
	mu.Unlock()
	s := pub.Stats()
	t.Logf("%v: %d messages, %d connection cuts, %d reconnects, %d publishes resent, %d QoS 1 duplicates",
		duration, len(published), cuts.Load(), s.Connects-1, s.PublishesReplayed, duplicates)
	if cuts.Load() == 0 {
		t.Error("the proxy never cut a connection")
	}
}
