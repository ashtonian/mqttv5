// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ashtonian/mqttv5/session"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOpen_CreatesSubdirs(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.Root() != dir {
		t.Errorf("Root() = %q, want %q", s.Root(), dir)
	}
	for _, sub := range []string{"outbound", "inbound"} {
		info, err := os.Stat(filepath.Join(dir, sub))
		if err != nil {
			t.Fatalf("subdir %s: %v", sub, err)
		}
		if !info.IsDir() {
			t.Errorf("subdir %s is not a directory", sub)
		}
	}
}

func TestPutRangeDelete_Outbound(t *testing.T) {
	s := newStore(t)

	want := map[uint16][]byte{
		1:    []byte("first"),
		42:   []byte("second"),
		1000: []byte("third"),
	}
	for id, b := range want {
		if err := s.PutOutbound(id, b); err != nil {
			t.Fatal(err)
		}
	}

	got := map[uint16][]byte{}
	err := s.RangeOutbound(func(id uint16, packet []byte) bool {
		got[id] = packet
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Errorf("got %d entries, want %d", len(got), len(want))
	}
	for id, w := range want {
		if !bytes.Equal(got[id], w) {
			t.Errorf("entry %d: got %q, want %q", id, got[id], w)
		}
	}

	// Delete one.
	if err := s.DeleteOutbound(42); err != nil {
		t.Fatal(err)
	}
	count := 0
	_ = s.RangeOutbound(func(uint16, []byte) bool { count++; return true })
	if count != 2 {
		t.Errorf("after delete: %d entries, want 2", count)
	}

	// Deleting a missing entry is not an error.
	if err := s.DeleteOutbound(9999); err != nil {
		t.Errorf("Delete missing returned %v, want nil", err)
	}
}

func TestPutRangeDelete_Inbound(t *testing.T) {
	s := newStore(t)
	for _, id := range []uint16{7, 8, 9} {
		if err := s.PutInbound(id); err != nil {
			t.Fatal(err)
		}
	}
	got := map[uint16]bool{}
	err := s.RangeInbound(func(id uint16) bool {
		got[id] = true
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint16{7, 8, 9} {
		if !got[want] {
			t.Errorf("missing inbound id %d", want)
		}
	}
	if err := s.DeleteInbound(8); err != nil {
		t.Fatal(err)
	}
	count := 0
	_ = s.RangeInbound(func(uint16) bool { count++; return true })
	if count != 2 {
		t.Errorf("after delete: %d entries, want 2", count)
	}
}

func TestReset_WipesBoth(t *testing.T) {
	s := newStore(t)
	_ = s.PutOutbound(1, []byte("x"))
	_ = s.PutInbound(2)
	if err := s.Reset(); err != nil {
		t.Fatal(err)
	}
	outCount, inCount := 0, 0
	_ = s.RangeOutbound(func(uint16, []byte) bool { outCount++; return true })
	_ = s.RangeInbound(func(uint16) bool { inCount++; return true })
	if outCount != 0 || inCount != 0 {
		t.Errorf("after Reset: outCount=%d inCount=%d, want 0/0", outCount, inCount)
	}
}

// TestSurvivesReopen is the headline test: write entries, close the
// Store, open a new Store at the same path, verify the entries are
// still there. This is what disk-backed storage is for.
func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	first, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("survives-the-restart")
	if err := first.PutOutbound(99, want); err != nil {
		t.Fatal(err)
	}
	if err := first.PutInbound(101); err != nil {
		t.Fatal(err)
	}

	// Simulate process restart.
	second, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	var gotPacket []byte
	_ = second.RangeOutbound(func(id uint16, p []byte) bool {
		if id == 99 {
			gotPacket = p
		}
		return true
	})
	if !bytes.Equal(gotPacket, want) {
		t.Errorf("after reopen: outbound[99] = %q, want %q", gotPacket, want)
	}
	gotInbound := false
	_ = second.RangeInbound(func(id uint16) bool {
		if id == 101 {
			gotInbound = true
		}
		return true
	})
	if !gotInbound {
		t.Error("after reopen: inbound[101] missing")
	}
}

// TestImplementsStoreInterface verifies *Store satisfies
// session.Store. The var _ = at the top of store.go already does
// this at compile time; the test makes it visible to humans reading
// the test output.
func TestImplementsStoreInterface(t *testing.T) {
	var _ session.Store = newStore(t)
}

// TestConcurrent runs concurrent Puts + Deletes on different IDs to
// confirm the mutex is doing its job.
func TestConcurrent(t *testing.T) {
	s := newStore(t)
	const goroutines = 8
	const perGoroutine = 16

	wg := sync.WaitGroup{}
	wg.Add(goroutines)
	for g := range goroutines {
		go func(base int) {
			defer wg.Done()
			for i := range perGoroutine {
				id := uint16(base*100 + i + 1)
				if err := s.PutOutbound(id, []byte{byte(i)}); err != nil {
					t.Errorf("PutOutbound: %v", err)
					return
				}
				if err := s.DeleteOutbound(id); err != nil {
					t.Errorf("DeleteOutbound: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	count := 0
	_ = s.RangeOutbound(func(uint16, []byte) bool { count++; return true })
	if count != 0 {
		t.Errorf("after concurrent put+delete: %d entries, want 0", count)
	}
}
