// Package filedb is the bbolt layer shared by the file-backed session
// store (store/file) and publisher queue (queue/file): one database file
// held under an exclusive lock, writes committed per a [SyncPolicy] with
// group commit, and checksummed values.
package filedb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

var (
	// ErrLocked is returned by Open when another process or handle holds
	// the database.
	ErrLocked = errors.New("mqttv5: database is locked by another process or handle")
	// ErrClosed is returned by operations on a closed database.
	ErrClosed = errors.New("mqttv5: database closed")
	// ErrCorrupt marks a value that failed its checksum or could not be
	// decoded.
	ErrCorrupt = errors.New("mqttv5: corrupt record")
)

// SyncPolicy says when a write reaches stable storage.
type SyncPolicy uint8

const (
	// SyncGroupCommit commits concurrent writes together and fsyncs once
	// per transaction; each write returns after its transaction is
	// durable.
	SyncGroupCommit SyncPolicy = iota
	// SyncEveryWrite commits and fsyncs each write on its own.
	SyncEveryWrite
	// SyncNone commits concurrent writes together, without fsync: writes
	// survive a process crash but not power loss or a kernel crash.
	SyncNone
)

// Config configures Open.
type Config struct {
	Sync SyncPolicy
	// CommitWindow is an extra wait before each group commit to gather
	// more writes. SyncEveryWrite does not group commits.
	CommitWindow time.Duration
	// LockTimeout bounds the wait for the file lock.
	LockTimeout time.Duration
	// Mode is the permission of a newly created file.
	Mode os.FileMode
}

// DB is a bbolt database opened for one owner.
type DB struct {
	bolt   *bolt.DB
	path   string
	sync   SyncPolicy
	commit committer
}

// Open opens or creates the database file name in dir, creating dir if
// needed, and makes sure buckets exist. Unless the policy is SyncNone, a
// file or directory it creates is durable when it returns: bbolt syncs
// the file's contents, and Open syncs each directory that gained an
// entry.
func Open(dir, name string, cfg Config, buckets ...[]byte) (*DB, error) {
	if dir == "" {
		return nil, errors.New("mqttv5: empty directory")
	}
	if cfg.Sync > SyncNone {
		return nil, fmt.Errorf("mqttv5: invalid SyncPolicy %d", cfg.Sync)
	}
	created, err := missingDirs(dir)
	if err != nil {
		return nil, fmt.Errorf("mqttv5: create %s: %w", dir, err)
	}
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mqttv5: create %s: %w", dir, err)
	}
	path := filepath.Join(dir, name)
	_, statErr := os.Stat(path)
	newFile := errors.Is(statErr, fs.ErrNotExist)
	db, err := bolt.Open(path, cfg.Mode, &bolt.Options{Timeout: cfg.LockTimeout, NoSync: cfg.Sync == SyncNone})
	if errors.Is(err, bolterrors.ErrTimeout) {
		return nil, fmt.Errorf("%w: %s", ErrLocked, path)
	}
	if err != nil {
		return nil, fmt.Errorf("mqttv5: open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mqttv5: initialise %s: %w", path, err)
	}
	if cfg.Sync != SyncNone {
		var parents []string
		for _, d := range created {
			parents = append(parents, filepath.Dir(d))
		}
		if newFile {
			parents = append(parents, dir)
		}
		for _, p := range slices.Compact(parents) {
			if err := syncDir(p); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("mqttv5: sync directory %s: %w", p, err)
			}
		}
	}
	return &DB{bolt: db, path: path, sync: cfg.Sync, commit: committer{db: db, window: cfg.CommitWindow}}, nil
}

// missingDirs returns dir and those of its ancestors that do not exist,
// outermost first: the directories MkdirAll(dir) is about to create.
func missingDirs(dir string) ([]string, error) {
	var missing []string
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		_, err := os.Stat(d)
		if err == nil {
			break
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		missing = append(missing, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	slices.Reverse(missing)
	return missing, nil
}

// syncDir makes the entries of directory dir durable; a variable so
// tests can see which directories Open syncs.
var syncDir = syncDirEntries

// Path returns the database file path.
func (d *DB) Path() string { return d.path }

// Update runs fn in a write transaction and returns once it is committed
// as the SyncPolicy promises. Unless the policy is SyncEveryWrite, fn may
// share its transaction with other writes and may run twice: once in a
// shared transaction that another write failed, then alone.
func (d *DB) Update(ctx context.Context, fn func(*bolt.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.sync == SyncEveryWrite {
		return mapErr(d.bolt.Update(fn))
	}
	return mapErr(d.commit.do(fn))
}

// View runs fn in a read transaction.
func (d *DB) View(fn func(*bolt.Tx) error) error { return mapErr(d.bolt.View(fn)) }

// Close releases the file and its lock.
func (d *DB) Close() error { return d.bolt.Close() }

func mapErr(err error) error {
	if errors.Is(err, bolterrors.ErrDatabaseNotOpen) {
		return ErrClosed
	}
	return err
}

// committer is a group commit. A write that finds no commit running
// leads: on its own goroutine it commits every pending write, its own
// among them, in one transaction. Writes that arrive meanwhile wait.
// When the commit ends the lead is free, and the first write to take it
// commits all that are pending: a waiter woken to take it, or a write
// that arrives before the waiter runs, which then commits the waiter's
// write too instead of waiting for its goroutine to be scheduled. Unlike
// bbolt's Batch it never delays a lone writer, which also never waits on
// another goroutine.
type committer struct {
	db     *bolt.DB
	window time.Duration

	mu      sync.Mutex
	pending []*commitOp
	leading bool
}

type commitOp struct {
	fn func(*bolt.Tx) error
	// wake asks the write's goroutine to look again: its write has
	// committed (done, with err), or the lead is free.
	wake chan struct{}
	done bool
	err  error
}

func (c *committer) do(fn func(*bolt.Tx) error) error {
	op := &commitOp{fn: fn, wake: make(chan struct{}, 1)}
	c.mu.Lock()
	c.pending = append(c.pending, op)
	for !op.done {
		if c.leading {
			c.mu.Unlock()
			<-op.wake
			c.mu.Lock()
			continue
		}
		c.leading = true
		c.mu.Unlock()
		c.commitPending()
		c.mu.Lock()
	}
	c.mu.Unlock()
	return op.err
}

// commitPending commits every pending write, then frees the lead and
// wakes the oldest write still waiting to take it.
func (c *committer) commitPending() {
	if c.window > 0 {
		time.Sleep(c.window)
	}
	c.mu.Lock()
	batch := c.pending
	c.pending = nil
	c.mu.Unlock()

	c.commit(batch)

	c.mu.Lock()
	for _, op := range batch {
		op.done = true
		nudge(op)
	}
	c.leading = false
	if len(c.pending) > 0 {
		nudge(c.pending[0])
	}
	c.mu.Unlock()
}

// nudge wakes op's goroutine unless a wake is already waiting for it;
// it looks at op's state when it runs.
func nudge(op *commitOp) {
	select {
	case op.wake <- struct{}{}:
	default:
	}
}

// commit commits batch in one transaction and sets each write's err,
// which its goroutine reads once done is set under c.mu.
func (c *committer) commit(batch []*commitOp) {
	err := c.db.Update(func(tx *bolt.Tx) error {
		for _, op := range batch {
			if err := op.fn(tx); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil || len(batch) == 1 {
		for _, op := range batch {
			op.err = err
		}
		return
	}
	// One write failed the shared transaction: commit each on its own so
	// only the failing write reports the error.
	for _, op := range batch {
		op.err = c.db.Update(op.fn)
	}
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Seal appends a CRC32C of b.
func Seal(b []byte) []byte {
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// Unseal verifies and strips the CRC32C Seal appended.
func Unseal(v []byte) ([]byte, error) {
	if len(v) < 4 {
		return nil, errors.New("too short")
	}
	body, sum := v[:len(v)-4], binary.BigEndian.Uint32(v[len(v)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, errors.New("checksum mismatch")
	}
	return body, nil
}
