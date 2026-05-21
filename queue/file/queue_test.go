// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

func ent(b byte) mqttv5.QueueEntry {
	return mqttv5.QueueEntry{
		Publish: wire.PublishOpts{
			Topic:   "t",
			Payload: []byte{b},
			QoS:     1,
		},
		EnqueuedAt: time.Now(),
	}
}

func TestEnqueuePeekAck(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := q.Enqueue(ctx, ent(byte(i))); err != nil {
			t.Fatal(err)
		}
	}

	n, _ := q.Len(ctx)
	if n != 3 {
		t.Fatalf("Len = %d, want 3", n)
	}

	entries, tokens, err := q.PeekBatch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("PeekBatch returned %d, want 3", len(entries))
	}
	for i, e := range entries {
		if e.Publish.Payload[0] != byte(i) {
			t.Fatalf("entries[%d].Payload[0] = %d, want %d", i, e.Publish.Payload[0], i)
		}
	}

	if err := q.Ack(ctx, tokens[1]); err != nil {
		t.Fatal(err)
	}
	n, _ = q.Len(ctx)
	if n != 2 {
		t.Fatalf("Len after Ack = %d, want 2", n)
	}
}

// TestRecoveryAfterReopen verifies the queue survives a process
// restart: enqueue 3, close, reopen, peek — should see the same 3
// entries in the same order.
func TestRecoveryAfterReopen(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := q.Enqueue(ctx, ent(byte(i))); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()

	q2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()

	entries, _, err := q2.PeekBatch(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("after reopen, got %d entries, want 3", len(entries))
	}
	for i, e := range entries {
		if e.Publish.Payload[0] != byte(i) {
			t.Fatalf("entries[%d].Payload[0] = %d, want %d", i, e.Publish.Payload[0], i)
		}
	}

	// Enqueue more — sequence numbers must not collide with recovered ones.
	if err := q2.Enqueue(ctx, ent(99)); err != nil {
		t.Fatal(err)
	}
	entries, _, _ = q2.PeekBatch(ctx, 10)
	if len(entries) != 4 {
		t.Fatalf("after appending, got %d entries, want 4", len(entries))
	}
	if entries[3].Publish.Payload[0] != 99 {
		t.Fatalf("appended entry payload[0] = %d, want 99", entries[3].Publish.Payload[0])
	}
}

func TestClosedRejectsEnqueue(t *testing.T) {
	q, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	q.Close()
	err = q.Enqueue(context.Background(), ent(1))
	if !errors.Is(err, mqttv5.ErrQueueClosed) {
		t.Fatalf("got %v, want ErrQueueClosed", err)
	}
}

// TestPartialFilesIgnored verifies the directory can contain non-queue
// files (e.g., a stray *.tmp from an interrupted write) and Open
// silently ignores them.
func TestPartialFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".tmp-stray"), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	q, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer q.Close()
	n, _ := q.Len(context.Background())
	if n != 0 {
		t.Fatalf("Len with stray files = %d, want 0", n)
	}
}

func TestAckMissingIsNoop(t *testing.T) {
	q, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	// Acking a nonexistent sequence must not error — matches
	// MemoryPublisherQueue semantics.
	if err := q.Ack(context.Background(), uint64(9999)); err != nil {
		t.Fatalf("Ack missing: %v", err)
	}
}
