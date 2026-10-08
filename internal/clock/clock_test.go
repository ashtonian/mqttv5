// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package clock

import (
	"testing"
	"time"
)

func TestFakeTimersFireInOrderOnAdvance(t *testing.T) {
	start := time.Unix(1000, 0)
	f := NewFake(start)
	a := f.NewTimer(2 * time.Second)
	b := f.NewTimer(1 * time.Second)
	if got := f.Pending(); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}

	f.Advance(500 * time.Millisecond)
	select {
	case <-a.C():
		t.Fatal("a fired early")
	case <-b.C():
		t.Fatal("b fired early")
	default:
	}

	f.Advance(600 * time.Millisecond)
	if got := <-b.C(); !got.Equal(start.Add(time.Second)) {
		t.Fatalf("b fired at %v", got)
	}
	if got := f.Pending(); got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}

	f.Advance(time.Second)
	if got := <-a.C(); !got.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("a fired at %v", got)
	}
	if got := f.Pending(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
}

func TestFakeTimerStopAndReset(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	tm := f.NewTimer(time.Second)
	if !tm.Stop() {
		t.Fatal("first Stop should report an active timer")
	}
	if tm.Stop() {
		t.Fatal("second Stop should report inactive")
	}
	f.Advance(2 * time.Second)
	select {
	case <-tm.C():
		t.Fatal("stopped timer fired")
	default:
	}
	if tm.Reset(time.Second) {
		t.Fatal("Reset of a stopped timer should report inactive")
	}
	f.Advance(time.Second)
	<-tm.C()
}

func TestFakeWaitPending(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	go func() {
		time.Sleep(10 * time.Millisecond)
		f.NewTimer(time.Second)
	}()
	if !f.WaitPending(1, time.Second) {
		t.Fatal("timer never armed")
	}
	if f.WaitPending(2, 20*time.Millisecond) {
		t.Fatal("unexpected second timer")
	}
}
