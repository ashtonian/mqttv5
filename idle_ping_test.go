// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func TestKeepAliveDecide(t *testing.T) {
	base := time.Unix(1_000, 0)
	keep, timeout := 10*time.Second, 2*time.Second
	tests := []struct {
		name       string
		k          keepAlive
		send, dead bool
		wait       time.Duration
	}{
		{"fresh both ways", keepAlive{now: base, lastWrite: base, lastRead: base}, false, false, keep},
		{"outbound quiet", keepAlive{now: base, lastWrite: base.Add(-keep), lastRead: base}, true, false, 0},
		{"inbound quiet", keepAlive{now: base, lastWrite: base, lastRead: base.Add(-keep)}, true, false, 0},
		{"earlier window wins", keepAlive{now: base, lastWrite: base.Add(-4 * time.Second), lastRead: base}, false, false, 6 * time.Second},
		{"ping outstanding", keepAlive{now: base, lastWrite: base, lastRead: base.Add(-keep), pingSent: base.Add(-time.Second)}, false, false, time.Second},
		{"ping timed out", keepAlive{now: base, lastWrite: base, lastRead: base.Add(-keep), pingSent: base.Add(-timeout)}, false, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.k.keep, tt.k.timeout = keep, timeout
			send, dead, wait := tt.k.decide()
			if send != tt.send || dead != tt.dead || wait != tt.wait {
				t.Fatalf("decide() = send %v dead %v wait %v, want %v %v %v", send, dead, wait, tt.send, tt.dead, tt.wait)
			}
		})
	}
}

func TestEffectivePingTimeout(t *testing.T) {
	if got := effectivePingTimeout(3*time.Second, 10*time.Second); got != 3*time.Second {
		t.Fatalf("within keep-alive: %v", got)
	}
	// The broker granted a keep-alive shorter than the configured timeout.
	if got := effectivePingTimeout(10*time.Second, 4*time.Second); got != 2*time.Second {
		t.Fatalf("above granted keep-alive: %v", got)
	}
}

// PingTimeout defaults below the keep-alive and is validated.
func TestPingTimeoutDefaultsAndValidation(t *testing.T) {
	for _, tt := range []struct {
		keep uint16
		want time.Duration
	}{{30, 10 * time.Second}, {8, 4 * time.Second}, {1, 500 * time.Millisecond}} {
		c, err := New(WithBroker("mqtt://127.0.0.1:1"), WithKeepAlive(tt.keep))
		if err != nil {
			t.Fatal(err)
		}
		if c.cfg.PingTimeout != tt.want {
			t.Errorf("keep-alive %ds: default PingTimeout %v, want %v", tt.keep, c.cfg.PingTimeout, tt.want)
		}
	}
	_, err := New(WithBroker("mqtt://127.0.0.1:1"), WithKeepAlive(5), WithPingTimeout(5*time.Second))
	if err == nil || !strings.Contains(err.Error(), "PingTimeout") {
		t.Fatalf("PingTimeout equal to KeepAlive accepted: %v", err)
	}
	if _, err := New(WithBroker("mqtt://127.0.0.1:1"), WithoutKeepAlive(), WithPingTimeout(time.Minute)); err != nil {
		t.Fatalf("PingTimeout with keep-alive disabled: %v", err)
	}
}

// pingHarness runs pingLoop against a fake clock, playing writer and
// broker: it reads the write queue, records each PINGREQ, and optionally
// answers it.
type pingHarness struct {
	t     *testing.T
	clk   *clock.Fake
	cs    *connState
	pings []time.Time
	died  time.Time

	answer  bool                                // reply to PINGREQ
	traffic func(now time.Time, h *pingHarness) // application traffic per step
}

func newPingHarness(t *testing.T, keep time.Duration, timeout time.Duration) *pingHarness {
	clk := clock.NewFake(time.Unix(1_000, 0))
	c := &Client{cfg: &Config{PingTimeout: timeout, Logger: quietLogger(), clock: clk}}
	life := newLifecycle()
	c.life.Store(life)
	cs := &connState{
		clk:        clk,
		writeQueue: make(chan writeReq, 64),
		life:       life,
		dying:      make(chan struct{}),
		info:       ConnackInfo{KeepAlive: uint16(keep / time.Second)},
	}
	now := clk.Now().UnixNano()
	cs.lastWriteUnixNano.Store(now)
	cs.lastReadUnixNano.Store(now)
	cs.wg.Add(1)
	go c.pingLoop(cs)
	t.Cleanup(func() {
		life.end()
		cs.wg.Wait()
	})
	return &pingHarness{t: t, clk: clk, cs: cs, answer: true}
}

func (h *pingHarness) write(now time.Time) { h.cs.lastWriteUnixNano.Store(now.UnixNano()) }

func (h *pingHarness) read(now time.Time) {
	h.cs.lastReadUnixNano.Store(now.UnixNano())
	h.cs.reads.Add(1)
}

// run advances the clock by total in steps, servicing the write queue
// whenever the loop has parked on its timer.
func (h *pingHarness) run(total, step time.Duration) {
	h.t.Helper()
	for elapsed := time.Duration(0); elapsed <= total; elapsed += step {
		if !h.parked() {
			return
		}
		now := h.clk.Now()
		for drained := false; !drained; {
			select {
			case req := <-h.cs.writeQueue:
				if req.fn == nil {
					continue
				}
				var buf strings.Builder
				if _, err := req.fn(&writerTo{&buf}); err != nil {
					h.t.Fatal(err)
				}
				h.pings = append(h.pings, now)
				h.write(now)
				if h.answer {
					h.read(now)
				}
			default:
				drained = true
			}
		}
		if h.traffic != nil {
			h.traffic(now, h)
		}
		h.clk.Advance(step)
	}
}

// parked waits until the loop is blocked on its timer, or reports false
// once the connection was declared dead.
func (h *pingHarness) parked() bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-h.cs.dying:
			if h.died.IsZero() {
				h.died = h.clk.Now()
			}
			return false
		default:
		}
		if h.clk.Pending() > 0 {
			return true
		}
		time.Sleep(50 * time.Microsecond)
	}
	h.t.Fatal("ping loop neither parked nor died")
	return false
}

type writerTo struct{ b *strings.Builder }

func (w *writerTo) Write(p []byte) (int, error) { return w.b.Write(p) }

// On an idle connection PINGREQs are never further apart than the
// keep-alive, whatever the ping timeout.
func TestKeepAliveIdleGapsWithinKeepAlive(t *testing.T) {
	keep := 10 * time.Second
	for _, timeout := range []time.Duration{100 * time.Millisecond, keep / 4, keep / 2, keep - time.Millisecond} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			h := newPingHarness(t, keep, timeout)
			h.run(2*time.Minute, 100*time.Millisecond)
			if !h.died.IsZero() {
				t.Fatalf("answered pings, yet declared dead at %v", h.died)
			}
			if len(h.pings) < 11 {
				t.Fatalf("%d PINGREQs in 2 minutes", len(h.pings))
			}
			prev := time.Unix(1_000, 0)
			for _, p := range h.pings {
				if gap := p.Sub(prev); gap > keep {
					t.Fatalf("PINGREQ gap %v exceeds keep-alive %v; pings %v", gap, keep, h.pings)
				}
				prev = p
			}
		})
	}
}

// Traffic in both directions makes PINGREQ unnecessary.
func TestKeepAliveBusyConnectionSendsNoPing(t *testing.T) {
	h := newPingHarness(t, 10*time.Second, 2*time.Second)
	h.traffic = func(now time.Time, h *pingHarness) {
		h.write(now)
		h.read(now)
	}
	h.run(time.Minute, time.Second)
	if len(h.pings) != 0 {
		t.Fatalf("%d PINGREQs on a busy connection", len(h.pings))
	}
}

// Any inbound packet after a PINGREQ proves the broker alive: a stream of
// PUBLISHes whose PINGRESPs are stuck behind the backlog must not tear
// the connection down (estavelle RFC 0009 regression).
func TestKeepAliveInboundTrafficCountsAsAnswer(t *testing.T) {
	h := newPingHarness(t, 2*time.Second, time.Second)
	h.answer = false
	h.traffic = func(now time.Time, h *pingHarness) { h.read(now) }
	h.run(30*time.Second, 100*time.Millisecond)
	if !h.died.IsZero() {
		t.Fatalf("declared dead at %v despite inbound traffic", h.died)
	}
	if len(h.pings) == 0 {
		t.Fatal("outbound-idle connection sent no PINGREQ")
	}
}

// A broker that answers nothing is detected one ping timeout after the
// PINGREQ.
func TestKeepAliveDetectsHalfOpen(t *testing.T) {
	keep, timeout := 10*time.Second, 3*time.Second
	h := newPingHarness(t, keep, timeout)
	h.answer = false
	h.run(time.Minute, 100*time.Millisecond)
	if h.died.IsZero() {
		t.Fatal("silent broker not detected")
	}
	if len(h.pings) != 1 {
		t.Fatalf("%d PINGREQs before detection, want 1", len(h.pings))
	}
	if got := h.died.Sub(h.pings[0]); got < timeout || got > timeout+100*time.Millisecond {
		t.Fatalf("detected %v after the PINGREQ, want %v", got, timeout)
	}
}

// The broker's Server Keep Alive replaces the requested one.
func TestServerKeepAliveOverridesRequested(t *testing.T) {
	ka := uint16(2)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		if ci := c.AcceptConnect(wire.ConnackOpts{ServerKeepAlive: &ka}); ci.KeepAlive != 60 {
			c.T.Errorf("CONNECT keep-alive %d", ci.KeepAlive)
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	if info, _ := cli.ServerInfo(); info.KeepAlive != 2 {
		t.Fatalf("effective keep-alive %d, want the broker's 2", info.KeepAlive)
	}
	// With a 2 s keep-alive an idle connection must ping within ~2 s.
	waitLogFor(t, b.Conn(0, time.Second), wire.PINGREQ, 1, 4*time.Second)
}

func waitLogFor(t *testing.T, conn *testbroker.Conn, pt wire.PacketType, n int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		count := 0
		for _, p := range conn.Log() {
			if p.Type == pt {
				count++
			}
		}
		if count >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fewer than %d %s within %v", n, pt, d)
}
