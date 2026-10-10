// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package clock abstracts time so timing-dependent code (keep-alive, backoff,
// queue draining) can be tested without sleeping. Production code uses Real;
// tests use Fake and advance it explicitly.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock is the subset of the time package the client needs.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	After(d time.Duration) <-chan time.Time
}

// Timer mirrors *time.Timer.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Real is the wall clock.
type Real struct{}

// Now returns time.Now().
func (Real) Now() time.Time { return time.Now() }

// After returns time.After(d).
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTimer wraps time.NewTimer(d).
func (Real) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

// C is the timer's channel.
func (r realTimer) C() <-chan time.Time { return r.t.C }

// Stop stops the timer, as time.Timer.Stop.
func (r realTimer) Stop() bool { return r.t.Stop() }

// Reset rearms the timer, as time.Timer.Reset.
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// Fake is a manually advanced clock. Timers fire (in deadline order) when
// Advance moves the time past their deadline; their channels have capacity 1,
// like time.Timer's.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	changed chan struct{} // closed and replaced whenever the timer set changes
}

// NewFake returns a fake clock starting at start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start, changed: make(chan struct{})}
}

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After is NewTimer(d).C().
func (f *Fake) After(d time.Duration) <-chan time.Time { return f.NewTimer(d).C() }

// NewTimer creates a timer that fires once the fake time reaches now+d.
func (f *Fake) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{f: f, c: make(chan time.Time, 1), at: f.now.Add(d)}
	if d <= 0 {
		t.c <- f.now
		return t
	}
	t.active = true
	f.timers = append(f.timers, t)
	f.notifyLocked()
	return t
}

// Advance moves the clock forward by d, firing every timer that comes due.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now
	sort.Slice(f.timers, func(i, j int) bool { return f.timers[i].at.Before(f.timers[j].at) })
	kept := f.timers[:0]
	for _, t := range f.timers {
		if !t.at.After(now) {
			t.active = false
			select {
			case t.c <- t.at:
			default:
			}
			continue
		}
		kept = append(kept, t)
	}
	f.timers = kept
	f.notifyLocked()
	f.mu.Unlock()
}

// Pending reports how many timers are waiting to fire.
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

// WaitPending blocks until at least n timers are pending or timeout passes
// (real time). It lets a test synchronise with goroutines that arm timers.
func (f *Fake) WaitPending(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		f.mu.Lock()
		if len(f.timers) >= n {
			f.mu.Unlock()
			return true
		}
		ch := f.changed
		f.mu.Unlock()
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		select {
		case <-ch:
		case <-time.After(left):
			return false
		}
	}
}

func (f *Fake) notifyLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

type fakeTimer struct {
	f      *Fake
	c      chan time.Time
	at     time.Time
	active bool
}

// C is the timer's channel.
func (t *fakeTimer) C() <-chan time.Time { return t.c }

// Stop stops the timer and reports whether it was pending.
func (t *fakeTimer) Stop() bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	return t.removeLocked()
}

// Reset rearms the timer d after the fake's current time and reports
// whether it was pending.
func (t *fakeTimer) Reset(d time.Duration) bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	was := t.removeLocked()
	t.at = t.f.now.Add(d)
	if d <= 0 {
		select {
		case t.c <- t.f.now:
		default:
		}
		return was
	}
	t.active = true
	t.f.timers = append(t.f.timers, t)
	t.f.notifyLocked()
	return was
}

func (t *fakeTimer) removeLocked() bool {
	if !t.active {
		return false
	}
	t.active = false
	for i, x := range t.f.timers {
		if x == t {
			t.f.timers = append(t.f.timers[:i], t.f.timers[i+1:]...)
			break
		}
	}
	t.f.notifyLocked()
	return true
}
