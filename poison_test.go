// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// poisonBroker serves every connection like the auto-responder, but
// answers a SUBSCRIBE for filter with DISCONNECT 0x81 and a close, as
// mosquitto does for a filter deeper than its limit. It counts those.
func poisonBroker(t *testing.T, filter string) (*testbroker.Broker, *atomic.Int32) {
	var poisoned atomic.Int32
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		if c.AcceptResume(wire.ConnackOpts{}) == nil {
			return
		}
		for {
			p, ok, err := c.Next(time.Minute)
			if !ok {
				if errors.Is(err, testbroker.ErrTimeout) {
					continue
				}
				return
			}
			switch p.Type {
			case wire.SUBSCRIBE:
				if slices.ContainsFunc(p.Filters, func(f wire.SubscribeFilter) bool { return f.Topic == filter }) {
					poisoned.Add(1)
					_ = c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonMalformedPacket})
					c.Close()
					return
				}
				codes := make([]wire.ReasonCode, len(p.Filters))
				for i, f := range p.Filters {
					codes[i] = wire.ReasonCode(f.QoS)
				}
				_ = c.Suback(p.PacketID, codes...)
			case wire.PINGREQ:
				_ = c.Write(wire.WritePingresp)
			case wire.DISCONNECT:
				return
			}
		}
	})
	return b, &poisoned
}

// A SUBSCRIBE the broker answers by disconnecting is sent
// maxCtrlAttempts times, then fails with RejectedByDisconnectError; the
// client carries on, and other subscriptions work.
func TestPoisonedSubscribeGivesUp(t *testing.T) {
	b, poisoned := poisonBroker(t, "deep/filter")
	cli := tbClient(t, b, WithReconnectBackoff(ConstantBackoff(time.Millisecond)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "deep/filter", QoS: 1}})
	var rej *RejectedByDisconnectError
	if !errors.As(err, &rej) || rej.Attempts != maxCtrlAttempts || rej.Disconnect.ReasonCode != wire.ReasonMalformedPacket {
		t.Fatalf("Subscribe: %v, want RejectedByDisconnectError after %d attempts", err, maxCtrlAttempts)
	}
	if n := poisoned.Load(); n != maxCtrlAttempts {
		t.Fatalf("broker received the SUBSCRIBE %d times", n)
	}
	if err := cli.AwaitConnection(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "fine", QoS: 1}}); err != nil {
		t.Fatalf("a later Subscribe: %v", err)
	}
	if n := poisoned.Load(); n != maxCtrlAttempts {
		t.Fatalf("the abandoned SUBSCRIBE was sent again: %d", n)
	}
}

// A caller that stops waiting does not keep the poisoned SUBSCRIBE
// alive: it still stops after maxCtrlAttempts.
func TestAbandonedPoisonedSubscribeStops(t *testing.T) {
	b, poisoned := poisonBroker(t, "deep/filter")
	cli := tbClient(t, b, WithReconnectBackoff(ConstantBackoff(time.Millisecond)))
	short, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, _, err := cli.Subscribe(short, []TopicFilter{{Topic: "deep/filter"}}); err == nil {
		t.Fatal("Subscribe succeeded")
	}
	deadline := time.Now().Add(5 * time.Second)
	for poisoned.Load() < maxCtrlAttempts && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if n := poisoned.Load(); n != maxCtrlAttempts {
		t.Fatalf("broker received the SUBSCRIBE %d times, want %d", n, maxCtrlAttempts)
	}
}

// A connection that ends as soon as it is up does not restart the
// reconnect backoff; one that lasted longer than the delay before it
// does.
func TestReconnectBackoffGrowsWhileConnectionsDieAtOnce(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_000, 0))
	drop := func(c *testbroker.Conn) { c.AcceptResume(wire.ConnackOpts{}); c.Close() }
	hold := func(c *testbroker.Conn) { c.AcceptResume(wire.ConnackOpts{}); c.ServeAuto() }
	b := testbroker.New(t, drop, drop, drop, hold)
	b.SetFallback(hold)

	var mu sync.Mutex
	var attempts []int
	reconnected := make(chan struct{}, 8)
	cli, err := New(WithBroker(b.URL()), WithClientID("backoff"), WithLogger(quietLogger()), WithoutKeepAlive(),
		withClock(clk), WithReconnectBackoff(ExponentialBackoff(time.Second, 30*time.Second, 0)),
		WithOnReconnectAttempt(func(n int, _ string) {
			mu.Lock()
			attempts = append(attempts, n)
			mu.Unlock()
		}),
		WithOnConnectionUp(func(ConnackInfo) { reconnected <- struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	<-reconnected

	// Three connections die at once: the next delay is still pending
	// until the fake clock passes it.
	for range 3 {
		if !clk.WaitPending(1, 5*time.Second) {
			t.Fatal("no reconnect timer")
		}
		clk.Advance(30 * time.Second)
		<-reconnected
	}
	// The fourth stays up longer than the delay before the next attempt.
	clk.Advance(time.Minute)
	b.DropAll()
	if !clk.WaitPending(1, 5*time.Second) {
		t.Fatal("no reconnect timer")
	}
	clk.Advance(30 * time.Second)
	<-reconnected

	mu.Lock()
	defer mu.Unlock()
	if want := []int{1, 2, 3, 1}; !slices.Equal(attempts, want) {
		t.Fatalf("reconnect attempts %v, want %v", attempts, want)
	}
}
