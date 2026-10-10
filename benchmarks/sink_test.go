// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"context"
	"errors"
	"testing"
	"time"
)

func stamped(seq uint64) []byte {
	p := Payload(64)
	stampSeq(p, seq)
	return p
}

// The sink accepts each message once, intact, and nothing else.
func TestSinkChecksEveryDelivery(t *testing.T) {
	msg := stamped
	for _, tc := range []struct {
		name    string
		deliver func(s *sink)
		ok      bool
	}{
		{"every message once", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)) }, true},
		{"a duplicate", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)); s.onMsg(msg(1)) }, false},
		{"one missing", func(s *sink) { s.onMsg(msg(0)) }, false},
		{"an interior byte changed", func(s *sink) {
			p := msg(1)
			p[32] ^= 0xff
			s.onMsg(msg(0))
			s.onMsg(p)
		}, false},
		{"two messages' halves", func(s *sink) {
			p := msg(1)
			copy(p[32:], msg(0)[32:])
			s.onMsg(msg(0))
			s.onMsg(p)
		}, false},
		{"a sequence number never sent", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)); s.onMsg(msg(7)) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSink(64, 2)
			tc.deliver(s)
			err := s.verify(func() int64 { return 0 })
			if (err == nil) != tc.ok {
				t.Fatalf("verify: %v, want ok %v", err, tc.ok)
			}
		})
	}
}

// A sink that allows drops counts each message its library dropped in
// place of a delivery, and one that allows duplicates counts them
// without failing; neither accepts a damaged or missing message.
func TestSinkPolicies(t *testing.T) {
	for _, tc := range []struct {
		name             string
		allowDrops, dups bool
		deliver          []uint64
		dropped          int64
		damaged          bool
		ok               bool
	}{
		{name: "drops counted in place of deliveries", allowDrops: true, deliver: []uint64{0}, dropped: 1, ok: true},
		{name: "drops that leave a message missing", allowDrops: true, deliver: []uint64{0}, ok: false},
		{name: "drops refused by default", deliver: []uint64{0}, dropped: 1, ok: false},
		{name: "a duplicate where drops are allowed", allowDrops: true, deliver: []uint64{0, 0}, dropped: 1, ok: false},
		{name: "duplicates allowed", dups: true, deliver: []uint64{0, 1, 1}, ok: true},
		{name: "duplicates allowed, one missing", dups: true, deliver: []uint64{0, 0}, ok: false},
		{name: "damage where duplicates are allowed", dups: true, deliver: []uint64{0, 1}, damaged: true, ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSink(64, 2)
			s.allowDrops, s.allowDups = tc.allowDrops, tc.dups
			for i, seq := range tc.deliver {
				p := stamped(seq)
				if tc.damaged && i == len(tc.deliver)-1 {
					p[32] ^= 0xff
				}
				s.onMsg(p)
			}
			err := s.verify(func() int64 { return tc.dropped })
			if (err == nil) != tc.ok {
				t.Fatalf("verify: %v, want ok %v", err, tc.ok)
			}
		})
	}
}

func fakeSubscription(stop func(ctx context.Context) error) *subscription {
	s := newSubscription()
	s.subscriber = subscriber{dropped: noDrops, stop: stop}
	return s
}

// A delivery in progress when the library's stop returns, as paho3's
// does after its quiesce period, is part of the run: the check waits for
// it, so a duplicate in a consumer's hands fails the run.
func TestCheckWaitsForADeliveryInProgress(t *testing.T) {
	s := newSink(64, 1)
	sub := fakeSubscription(func(context.Context) error { return nil })
	release := make(chan struct{})
	entered := make(chan struct{})
	deliver := sub.through(func(p []byte) {
		if s.got.Load() == 1 {
			close(entered)
			<-release
		}
		s.onMsg(p)
	})
	deliver(stamped(0))
	go deliver(stamped(0))
	<-entered
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	if err := s.finish(sub, 5*time.Second); err == nil {
		t.Fatal("a duplicate delivered while the subscriber stopped passed the check")
	}
}

// Nothing reaches onMsg once Stop has returned, and a message the library
// delivers after its stop returned fails the check.
func TestDeliveryAfterStopFailsTheCheck(t *testing.T) {
	s := newSink(64, 1)
	sub := fakeSubscription(func(context.Context) error { return nil })
	deliver := sub.through(s.onMsg)
	deliver(stamped(0))
	if err := sub.Stop(time.Second); err != nil {
		t.Fatal(err)
	}
	deliver(stamped(0))
	deliver(nil) // a trailing probe is not a delivery
	if s.dup.Load() != 0 {
		t.Fatal("a delivery after Stop reached onMsg")
	}
	if err := s.finish(sub, time.Second); err == nil {
		t.Fatal("a delivery after the library stopped passed the check")
	}
}

// A subscriber that does not stop within the bound fails the check, a
// failed library stop fails it too, and the benchmark's cleanup, which
// stops the subscription again, does not wait a second bound.
func TestStopIsBounded(t *testing.T) {
	stuck := fakeSubscription(func(context.Context) error { select {} })
	if err := newSink(64, 0).finish(stuck, 10*time.Millisecond); err == nil {
		t.Fatal("a subscriber that never stops passed the check")
	}
	again := make(chan error, 1)
	go func() { again <- stuck.Stop(stopBound) }()
	select {
	case err := <-again:
		if err == nil {
			t.Fatal("a second Stop of a stuck subscriber succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("a second Stop waited again for a subscriber that already timed out")
	}

	failing := fakeSubscription(func(context.Context) error { return errors.New("disconnect failed") })
	if err := newSink(64, 0).finish(failing, time.Second); err == nil {
		t.Fatal("a failed library stop passed the check")
	}
}
