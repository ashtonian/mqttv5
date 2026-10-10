// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package file is a file-backed [mqttv5.PublisherQueue]: messages a
// [mqttv5.QueuePublisher] accepted survive process crashes and,
// depending on the [SyncPolicy], power loss.
//
//	q, err := file.Open("/var/lib/myapp/mqtt-queue")
//	if err != nil { ... }
//	pub, err := mqttv5.NewQueuePublisher(cli, q)
//
// Entries live in one bbolt database file, queue.db, inside the given
// directory, keyed by sequence number, with the entry count kept in the
// same transactions so Len is constant-time. bbolt holds an exclusive
// file lock: a second Open of the same directory, from this process or
// another, fails with [ErrLocked] instead of two queues overwriting each
// other's entries.
//
// Every Enqueue and Ack returns only after its change is committed per
// the SyncPolicy; the default, [SyncGroupCommit], shares one fsync among
// concurrent writers while each still waits for its own write to be
// durable. An entry that fails its checksum or cannot be decoded is
// moved to a quarantine bucket, counted out, and reported through
// [Queue.Corrupt] and [WithOnCorrupt]; it never blocks the entries
// behind it.
package file

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/internal/filedb"
)

// FileName is the database file created inside the queue directory.
const FileName = "queue.db"

var (
	// ErrLocked is returned by Open when another Queue holds the
	// directory.
	ErrLocked = filedb.ErrLocked
	// ErrCorrupt wraps an entry that failed its checksum or could not be
	// decoded.
	ErrCorrupt = filedb.ErrCorrupt
)

// SyncPolicy says when a write reaches stable storage.
type SyncPolicy = filedb.SyncPolicy

const (
	// SyncGroupCommit (default) commits concurrent writes together and
	// fsyncs once per transaction; each write returns after its
	// transaction is durable. Survives process crashes and power loss.
	SyncGroupCommit = filedb.SyncGroupCommit
	// SyncEveryWrite commits and fsyncs each write on its own. Same
	// guarantee as SyncGroupCommit with no batching delay, at a
	// throughput of one fsync per write.
	SyncEveryWrite = filedb.SyncEveryWrite
	// SyncNone commits without fsync. Writes survive a process crash
	// (they are in the operating system's cache) but not power loss or
	// a kernel crash.
	SyncNone = filedb.SyncNone
)

const (
	// DefaultCommitWindow is the extra time SyncGroupCommit waits to
	// gather writes before committing: none.
	DefaultCommitWindow = 0
	// DefaultLockTimeout is how long Open waits for another holder to
	// release the directory before returning ErrLocked.
	DefaultLockTimeout = time.Second
)

type options struct {
	db        filedb.Config
	onCorrupt func(seq uint64, err error)
}

// Option configures Open.
type Option func(*options)

// WithSyncPolicy sets the durability policy. Default SyncGroupCommit.
func WithSyncPolicy(p SyncPolicy) Option { return func(o *options) { o.db.Sync = p } }

// WithCommitWindow adds a wait before each SyncGroupCommit transaction
// to gather more writes per fsync, at the cost of that much latency.
// Default DefaultCommitWindow.
func WithCommitWindow(d time.Duration) Option { return func(o *options) { o.db.CommitWindow = d } }

// WithLockTimeout sets how long Open waits for the directory lock.
// Default DefaultLockTimeout.
func WithLockTimeout(d time.Duration) Option { return func(o *options) { o.db.LockTimeout = d } }

// WithFileMode sets the permissions of a newly created database file.
// Default 0600.
func WithFileMode(m os.FileMode) Option { return func(o *options) { o.db.Mode = m } }

// WithOnCorrupt is called, from the goroutine that found it, for every
// entry quarantined as corrupt.
func WithOnCorrupt(fn func(seq uint64, err error)) Option {
	return func(o *options) { o.onCorrupt = fn }
}

var (
	bucketMeta       = []byte("meta")
	bucketEntries    = []byte("entries")
	bucketQuarantine = []byte("quarantine")
	keyNext          = []byte("next")  // last Seq assigned
	keyCount         = []byte("count") // entries in bucketEntries
)

// Queue is a [mqttv5.PublisherQueue] backed by a bbolt database. Safe
// for concurrent use.
type Queue struct {
	db        *filedb.DB
	onCorrupt func(uint64, error)

	count  atomic.Int64
	closed atomic.Bool

	mu      sync.Mutex
	corrupt []error
}

var _ mqttv5.PublisherQueue = (*Queue)(nil)

// Open opens or creates the queue in dir, creating dir if needed.
// Existing entries are kept: that is the crash-recovery path.
func Open(dir string, opts ...Option) (*Queue, error) {
	o := options{db: filedb.Config{CommitWindow: DefaultCommitWindow, LockTimeout: DefaultLockTimeout, Mode: 0o600}}
	for _, opt := range opts {
		opt(&o)
	}
	db, err := filedb.Open(dir, FileName, o.db, bucketMeta, bucketEntries, bucketQuarantine)
	if err != nil {
		return nil, err
	}
	q := &Queue{db: db, onCorrupt: o.onCorrupt}
	if err := db.View(func(tx *bolt.Tx) error {
		q.count.Store(int64(getUint(tx.Bucket(bucketMeta), keyCount)))
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return q, nil
}

// Path returns the database file path.
func (q *Queue) Path() string { return q.db.Path() }

// Corrupt returns the entries quarantined since Open.
func (q *Queue) Corrupt() []error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return slices.Clone(q.corrupt)
}

// Enqueue implements [mqttv5.PublisherQueue].
func (q *Queue) Enqueue(ctx context.Context, e mqttv5.QueueEntry, limit mqttv5.QueueLimit) (uint64, []mqttv5.QueueEntry, error) {
	if q.closed.Load() {
		return 0, nil, mqttv5.ErrQueueClosed
	}
	v, encErr := encodeEntry(e)
	if encErr != nil {
		return 0, nil, encErr
	}
	var (
		seq      uint64
		evicted  []mqttv5.QueueEntry
		full     bool
		delta    int64
		bad      []corruptEntry
		maxCount = uint64(max(limit.Max, 0))
	)
	err := q.db.Update(ctx, func(tx *bolt.Tx) error {
		seq, evicted, full, delta, bad = 0, nil, false, 0, nil
		meta, entries := tx.Bucket(bucketMeta), tx.Bucket(bucketEntries)
		count := getUint(meta, keyCount)
		if maxCount > 0 && count >= maxCount {
			if limit.Policy != mqttv5.DropOldest {
				full = true
				return nil
			}
			need := int(count - maxCount + 1)
			var victims [][]byte
			c := entries.Cursor()
			for k, val := c.Seek(seqKey(limit.Keep + 1)); k != nil && len(victims) < need; k, val = c.Next() {
				victims = append(victims, bytes.Clone(k))
				ev, err := decodeEntry(k, val)
				if err != nil {
					bad = append(bad, corruptEntry{seq: binary.BigEndian.Uint64(k), err: err, value: bytes.Clone(val)})
					continue
				}
				evicted = append(evicted, ev)
			}
			if len(victims) < need {
				full, evicted, bad = true, nil, nil
				return nil
			}
			quarantine := tx.Bucket(bucketQuarantine)
			for _, b := range bad {
				if err := quarantine.Put(seqKey(b.seq), b.value); err != nil {
					return err
				}
			}
			for _, k := range victims {
				if err := entries.Delete(k); err != nil {
					return err
				}
			}
			count -= uint64(len(victims))
			delta -= int64(len(victims))
		}
		seq = getUint(meta, keyNext) + 1
		if err := entries.Put(seqKey(seq), v); err != nil {
			return err
		}
		delta++
		if err := putUint(meta, keyNext, seq); err != nil {
			return err
		}
		return putUint(meta, keyCount, count+1)
	})
	if err != nil {
		return 0, nil, q.mapErr(err)
	}
	if full {
		return 0, nil, mqttv5.ErrQueueFull
	}
	q.count.Add(delta)
	q.report(bad)
	return seq, evicted, nil
}

// Peek implements [mqttv5.PublisherQueue].
func (q *Queue) Peek(ctx context.Context, after uint64, n int) ([]mqttv5.QueueEntry, error) {
	if q.closed.Load() {
		return nil, mqttv5.ErrQueueClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var (
		out []mqttv5.QueueEntry
		bad []corruptEntry
	)
	err := q.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketEntries).Cursor()
		for k, v := c.Seek(seqKey(after + 1)); k != nil && len(out) < n; k, v = c.Next() {
			e, err := decodeEntry(k, v)
			if err != nil {
				bad = append(bad, corruptEntry{seq: binary.BigEndian.Uint64(k), err: err, value: bytes.Clone(v)})
				continue
			}
			out = append(out, e)
		}
		return nil
	})
	if err != nil {
		return nil, q.mapErr(err)
	}
	if len(bad) > 0 {
		if err := q.quarantine(ctx, bad); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Ack implements [mqttv5.PublisherQueue].
func (q *Queue) Ack(ctx context.Context, seq uint64) error {
	if q.closed.Load() {
		return mqttv5.ErrQueueClosed
	}
	k := seqKey(seq)
	var removed bool
	err := q.db.Update(ctx, func(tx *bolt.Tx) error {
		removed = false
		entries := tx.Bucket(bucketEntries)
		if entries.Get(k) == nil {
			return nil
		}
		removed = true
		if err := entries.Delete(k); err != nil {
			return err
		}
		meta := tx.Bucket(bucketMeta)
		return putUint(meta, keyCount, max(getUint(meta, keyCount), 1)-1)
	})
	if err != nil {
		return q.mapErr(err)
	}
	if removed {
		q.count.Add(-1)
	}
	return nil
}

// Len implements [mqttv5.PublisherQueue].
func (q *Queue) Len(context.Context) (int, error) {
	if q.closed.Load() {
		return 0, mqttv5.ErrQueueClosed
	}
	return int(q.count.Load()), nil
}

// Close implements [mqttv5.PublisherQueue]. The entries stay on disk
// for the next Open.
func (q *Queue) Close() error {
	q.closed.Store(true)
	return q.db.Close()
}

func (q *Queue) mapErr(err error) error {
	if errors.Is(err, filedb.ErrClosed) {
		return mqttv5.ErrQueueClosed
	}
	return err
}

type corruptEntry struct {
	seq   uint64
	err   error
	value []byte
}

// quarantine moves corrupt entries out of the queue.
func (q *Queue) quarantine(ctx context.Context, bad []corruptEntry) error {
	var moved int64
	err := q.db.Update(ctx, func(tx *bolt.Tx) error {
		moved = 0
		entries, quarantine, meta := tx.Bucket(bucketEntries), tx.Bucket(bucketQuarantine), tx.Bucket(bucketMeta)
		for _, b := range bad {
			k := seqKey(b.seq)
			if entries.Get(k) == nil {
				continue
			}
			if err := quarantine.Put(k, b.value); err != nil {
				return err
			}
			if err := entries.Delete(k); err != nil {
				return err
			}
			moved++
		}
		return putUint(meta, keyCount, getUint(meta, keyCount)-uint64(moved))
	})
	if err != nil {
		return q.mapErr(err)
	}
	q.count.Add(-moved)
	q.report(bad)
	return nil
}

func (q *Queue) report(bad []corruptEntry) {
	if len(bad) == 0 {
		return
	}
	q.mu.Lock()
	for _, b := range bad {
		q.corrupt = append(q.corrupt, fmt.Errorf("%w: entry %d: %v", ErrCorrupt, b.seq, b.err))
	}
	q.mu.Unlock()
	if q.onCorrupt != nil {
		for _, b := range bad {
			q.onCorrupt(b.seq, fmt.Errorf("%w: %v", ErrCorrupt, b.err))
		}
	}
}

func seqKey(seq uint64) []byte { return binary.BigEndian.AppendUint64(nil, seq) }

func getUint(b *bolt.Bucket, k []byte) uint64 {
	if v := b.Get(k); len(v) == 8 {
		return binary.BigEndian.Uint64(v)
	}
	return 0
}

func putUint(b *bolt.Bucket, k []byte, v uint64) error {
	return b.Put(k, binary.BigEndian.AppendUint64(nil, v))
}

// Entry encoding, version 1 (integers big-endian):
//
//	version   1 byte (1)
//	flags     1 byte (bit 0: EnqueuedAt present, bit 1: ExpiresAt present)
//	enqueued  8 bytes Unix nanoseconds, when flag bit 0 is set
//	expires   8 bytes Unix nanoseconds, when flag bit 1 is set
//	id        1 byte length + bytes
//	publish   the PUBLISH packet as MQTT encodes it (packet identifier 1
//	          as a placeholder), up to the checksum
//	crc32c    4 bytes over everything before it
//
// The PUBLISH packet keeps every option, with presence, exactly as the
// protocol does.
const entryVersion = 1

const (
	flagEnqueued = 1 << iota
	flagExpires
)

func encodeEntry(e mqttv5.QueueEntry) ([]byte, error) {
	if len(e.ID) > 255 {
		return nil, fmt.Errorf("mqttv5/queue/file: entry ID of %d bytes, at most 255", len(e.ID))
	}
	frame, err := mqttv5.EncodePublish(e.Publish, 1)
	if err != nil {
		return nil, fmt.Errorf("mqttv5/queue/file: encode entry: %w", err)
	}
	var flags byte
	if !e.EnqueuedAt.IsZero() {
		flags |= flagEnqueued
	}
	if !e.ExpiresAt.IsZero() {
		flags |= flagExpires
	}
	b := make([]byte, 0, 2+16+1+len(e.ID)+len(frame)+4)
	b = append(b, entryVersion, flags)
	if flags&flagEnqueued != 0 {
		b = binary.BigEndian.AppendUint64(b, uint64(e.EnqueuedAt.UnixNano()))
	}
	if flags&flagExpires != 0 {
		b = binary.BigEndian.AppendUint64(b, uint64(e.ExpiresAt.UnixNano()))
	}
	b = append(append(b, byte(len(e.ID))), e.ID...)
	b = append(b, frame...)
	return filedb.Seal(b), nil
}

func decodeEntry(k, v []byte) (mqttv5.QueueEntry, error) {
	if len(k) != 8 {
		return mqttv5.QueueEntry{}, fmt.Errorf("key % x", k)
	}
	body, err := filedb.Unseal(v)
	if err != nil {
		return mqttv5.QueueEntry{}, err
	}
	if len(body) < 3 || body[0] != entryVersion {
		return mqttv5.QueueEntry{}, fmt.Errorf("unsupported entry layout (%d bytes)", len(body))
	}
	e := mqttv5.QueueEntry{Seq: binary.BigEndian.Uint64(k)}
	flags, rest := body[1], body[2:]
	for _, f := range []struct {
		bit byte
		t   *time.Time
	}{{flagEnqueued, &e.EnqueuedAt}, {flagExpires, &e.ExpiresAt}} {
		if flags&f.bit == 0 {
			continue
		}
		if len(rest) < 8 {
			return mqttv5.QueueEntry{}, errors.New("truncated time")
		}
		*f.t = time.Unix(0, int64(binary.BigEndian.Uint64(rest)))
		rest = rest[8:]
	}
	if len(rest) < 1 || len(rest) < 1+int(rest[0]) {
		return mqttv5.QueueEntry{}, errors.New("truncated id")
	}
	e.ID, rest = string(rest[1:1+int(rest[0])]), rest[1+int(rest[0]):]
	if e.Publish, _, err = mqttv5.DecodePublish(rest); err != nil {
		return mqttv5.QueueEntry{}, fmt.Errorf("publish: %w", err)
	}
	return e, nil
}
