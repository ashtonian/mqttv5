// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package file is a filesystem-backed implementation of
// mqttv5.PublisherQueue. Each enqueued entry becomes one file
// named by its monotonically increasing sequence number, so:
//
//   - PeekBatch reads the oldest N entries by sorting directory
//     listings as fixed-width hex (lexical sort == numeric sort).
//   - Ack deletes the entry's file.
//   - Open scans the directory to recover state: max-seq + 1 becomes
//     the next allocation.
//
// Writes use the write-temp-then-rename + fsync pattern so a crash
// mid-write can never produce a partial entry — at-least-once
// semantics across process restart.
//
// Layout under root/:
//
//	0000000000000001.qent
//	0000000000000002.qent
//	...
//
// Stdlib only. Opt-in via this submodule so the core mqttv5 module
// stays free of filesystem deps.
package file

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ashtonian/mqttv5"
)

// Queue persists mqttv5.QueueEntry values into root.
type Queue struct {
	root string

	mu     sync.Mutex
	next   uint64
	closed bool
}

const fileSuffix = ".qent"

var _ mqttv5.PublisherQueue = (*Queue)(nil)

// Open returns a Queue rooted at dir. The directory is created if
// missing. Existing entries are kept (resume-after-crash path) and
// the sequence counter is restored to max(existing) + 1.
//
// Empty dir uses a fresh temp directory — handy for tests; production
// callers should provide an explicit path.
func Open(dir string) (*Queue, error) {
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "mqttv5-queue-*")
		if err != nil {
			return nil, fmt.Errorf("mqttv5/queue/file: mkdir tmp: %w", err)
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mqttv5/queue/file: mkdir %s: %w", dir, err)
	}

	q := &Queue{root: dir, next: 1}
	if err := q.restore(); err != nil {
		return nil, err
	}
	return q, nil
}

// Root returns the directory the queue is rooted in.
func (q *Queue) Root() string { return q.root }

// restore scans the queue directory for existing entries to recover
// state. Sets q.next to max(seq) + 1 so new entries don't collide
// with anything from a prior process.
func (q *Queue) restore() error {
	entries, err := os.ReadDir(q.root)
	if err != nil {
		return fmt.Errorf("mqttv5/queue/file: read root: %w", err)
	}
	var maxSeq uint64
	for _, e := range entries {
		seq, ok := parseSeq(e.Name())
		if !ok {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	q.next = maxSeq + 1
	return nil
}

// Enqueue persists entry as one file. Returns ErrQueueClosed after
// Close. The write is fsync'd before Enqueue returns so a crash can
// never leave the file partially written.
func (q *Queue) Enqueue(_ context.Context, entry mqttv5.QueueEntry) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return mqttv5.ErrQueueClosed
	}

	seq := q.next
	q.next++

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return fmt.Errorf("mqttv5/queue/file: encode: %w", err)
	}

	path := filepath.Join(q.root, formatSeq(seq)+fileSuffix)
	if err := writeAtomic(path, buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// PeekBatch reads up to n oldest entries (by sequence number) and
// returns them with parallel tokens. The tokens are uint64 sequence
// numbers — the same values used as file names — passed back to Ack.
func (q *Queue) PeekBatch(_ context.Context, n int) ([]mqttv5.QueueEntry, []mqttv5.QueueToken, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, nil, mqttv5.ErrQueueClosed
	}
	if n <= 0 {
		return nil, nil, nil
	}

	entries, err := os.ReadDir(q.root)
	if err != nil {
		return nil, nil, fmt.Errorf("mqttv5/queue/file: read root: %w", err)
	}

	seqs := make([]uint64, 0, len(entries))
	for _, e := range entries {
		if seq, ok := parseSeq(e.Name()); ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	if len(seqs) > n {
		seqs = seqs[:n]
	}

	out := make([]mqttv5.QueueEntry, 0, len(seqs))
	tokens := make([]mqttv5.QueueToken, 0, len(seqs))
	for _, seq := range seqs {
		path := filepath.Join(q.root, formatSeq(seq)+fileSuffix)
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Concurrent Ack removed it — skip.
				continue
			}
			return nil, nil, fmt.Errorf("mqttv5/queue/file: read %s: %w", path, err)
		}
		var entry mqttv5.QueueEntry
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&entry); err != nil {
			return nil, nil, fmt.Errorf("mqttv5/queue/file: decode %s: %w", path, err)
		}
		out = append(out, entry)
		tokens = append(tokens, seq)
	}
	return out, tokens, nil
}

// EvictHead implements mqttv5.PublisherQueue by reading the oldest
// entry, deleting its file, and returning the decoded entry. This
// is the file-backed counterpart to MemoryPublisherQueue.EvictHead
// — used by QueuePublisher when WithQueueDropPolicy(DropOldest) is
// enabled and the queue is at capacity.
func (q *Queue) EvictHead(_ context.Context) (mqttv5.QueueEntry, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return mqttv5.QueueEntry{}, false, mqttv5.ErrQueueClosed
	}
	entries, err := os.ReadDir(q.root)
	if err != nil {
		return mqttv5.QueueEntry{}, false, fmt.Errorf("mqttv5/queue/file: read root: %w", err)
	}
	var minSeq uint64
	var minName string
	found := false
	for _, e := range entries {
		seq, ok := parseSeq(e.Name())
		if !ok {
			continue
		}
		if !found || seq < minSeq {
			minSeq = seq
			minName = e.Name()
			found = true
		}
	}
	if !found {
		return mqttv5.QueueEntry{}, false, nil
	}
	path := filepath.Join(q.root, minName)
	data, err := os.ReadFile(path)
	if err != nil {
		return mqttv5.QueueEntry{}, false, fmt.Errorf("mqttv5/queue/file: read %s: %w", path, err)
	}
	var entry mqttv5.QueueEntry
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&entry); err != nil {
		return mqttv5.QueueEntry{}, false, fmt.Errorf("mqttv5/queue/file: decode %s: %w", path, err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return mqttv5.QueueEntry{}, false, fmt.Errorf("mqttv5/queue/file: remove %s: %w", path, err)
	}
	return entry, true, nil
}

// Ack removes the entry identified by tok. Acking a missing entry is
// a no-op (an entry already removed via another path).
func (q *Queue) Ack(_ context.Context, tok mqttv5.QueueToken) error {
	seq, ok := tok.(uint64)
	if !ok {
		return fmt.Errorf("mqttv5/queue/file: bad token type %T", tok)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	path := filepath.Join(q.root, formatSeq(seq)+fileSuffix)
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("mqttv5/queue/file: remove %s: %w", path, err)
	}
	return nil
}

// Len returns the count of entries currently on disk.
func (q *Queue) Len(_ context.Context) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	entries, err := os.ReadDir(q.root)
	if err != nil {
		return 0, fmt.Errorf("mqttv5/queue/file: read root: %w", err)
	}
	count := 0
	for _, e := range entries {
		if _, ok := parseSeq(e.Name()); ok {
			count++
		}
	}
	return count, nil
}

// Close marks the queue closed. Subsequent Enqueue calls return
// ErrQueueClosed. On-disk files are left in place for next Open.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return nil
}

// formatSeq renders a sequence number as a 16-digit zero-padded hex
// string. Fixed width keeps lexical sort consistent with numeric sort.
func formatSeq(seq uint64) string {
	return fmt.Sprintf("%016x", seq)
}

// parseSeq inverts formatSeq from a filename. Returns (0, false) for
// names that don't match the *.qent pattern with a 16-digit hex
// prefix — non-queue files in the dir are silently ignored.
func parseSeq(name string) (uint64, bool) {
	if !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}
	hex := strings.TrimSuffix(name, fileSuffix)
	if len(hex) != 16 {
		return 0, false
	}
	seq, err := strconv.ParseUint(hex, 16, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// writeAtomic writes b to path via write-temp + fsync + rename so a
// crash mid-write cannot leave a partial file. The temp file lives
// in the same directory so the rename is atomic on POSIX.
func writeAtomic(path string, b []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("mqttv5/queue/file: create temp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("mqttv5/queue/file: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("mqttv5/queue/file: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("mqttv5/queue/file: close: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("mqttv5/queue/file: rename: %w", err)
	}
	return nil
}
