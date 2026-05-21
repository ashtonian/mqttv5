// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestIDPoolAllocateRelease(t *testing.T) {
	p := NewIDPool(4)
	ctx := context.Background()

	ids := make(map[uint16]bool)
	for range 4 {
		id, err := p.Allocate(ctx)
		if err != nil {
			t.Fatalf("allocate: %v", err)
		}
		if id == 0 {
			t.Fatal("allocate returned reserved id 0")
		}
		if ids[id] {
			t.Fatalf("duplicate id %d", id)
		}
		ids[id] = true
	}
	if p.Available() != 0 {
		t.Fatalf("Available = %d, want 0", p.Available())
	}

	// TryAllocate returns false when drained.
	if _, ok := p.TryAllocate(); ok {
		t.Fatal("TryAllocate succeeded on empty pool")
	}

	// Allocate with a cancelled ctx returns immediately.
	deadCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := p.Allocate(deadCtx); err == nil {
		t.Fatal("Allocate on empty pool with cancelled ctx returned nil error")
	}

	// Release one, then Allocate succeeds again.
	p.Release(2)
	id, err := p.Allocate(ctx)
	if err != nil {
		t.Fatalf("Allocate after release: %v", err)
	}
	if id != 2 {
		t.Fatalf("got %d, want 2 (release order)", id)
	}
}

func TestIDPoolConcurrent(t *testing.T) {
	p := NewIDPool(1024)
	ctx := context.Background()

	const goroutines = 8
	const perGoroutine = 128
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				id, err := p.Allocate(ctx)
				if err != nil {
					t.Errorf("allocate: %v", err)
					return
				}
				p.Release(id)
			}
		}()
	}
	wg.Wait()

	if p.Available() != 1024 {
		t.Fatalf("Available = %d, want 1024", p.Available())
	}
}

func TestIDPoolReleaseZeroNoop(t *testing.T) {
	p := NewIDPool(1)
	before := p.Available()
	p.Release(0)
	if p.Available() != before {
		t.Fatal("Release(0) should be a no-op")
	}
}

func TestIDPoolBlocksThenWakes(t *testing.T) {
	p := NewIDPool(1)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	id, err := p.Allocate(ctx)
	if err != nil {
		t.Fatalf("first allocate: %v", err)
	}

	done := make(chan uint16, 1)
	go func() {
		id2, err := p.Allocate(ctx)
		if err != nil {
			t.Errorf("second allocate: %v", err)
			done <- 0
			return
		}
		done <- id2
	}()

	// Release after a brief delay; the blocked allocator should wake.
	time.Sleep(20 * time.Millisecond)
	p.Release(id)

	select {
	case id2 := <-done:
		if id2 != id {
			t.Errorf("got %d, want %d (released id)", id2, id)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("blocked allocator did not wake after Release")
	}
}
