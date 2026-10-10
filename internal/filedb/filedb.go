// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

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
	// SyncNone commits without fsync: writes survive a process crash but
	// not power loss or a kernel crash.
	SyncNone
)

// Config configures Open.
type Config struct {
	Sync SyncPolicy
	// CommitWindow is an extra wait before each group commit to gather
	// more writes.
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
// as the SyncPolicy promises. Under SyncGroupCommit fn may share its
// transaction with other writes and may run twice: once in a shared
// transaction that another write failed, then alone.
func (d *DB) Update(ctx context.Context, fn func(*bolt.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.sync == SyncGroupCommit {
		return mapErr(d.commit.do(fn))
	}
	return mapErr(d.bolt.Update(fn))
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

// committer is a leader/follower group commit. A write that finds no
// commit running starts one; writes arriving meanwhile queue and go into
// the next transaction together. Unlike bbolt's Batch it never delays a
// lone writer.
type committer struct {
	db     *bolt.DB
	window time.Duration

	mu      sync.Mutex
	pending []*commitOp
	running bool
}

type commitOp struct {
	fn   func(*bolt.Tx) error
	done chan error
}

func (c *committer) do(fn func(*bolt.Tx) error) error {
	op := &commitOp{fn: fn, done: make(chan error, 1)}
	c.mu.Lock()
	c.pending = append(c.pending, op)
	start := !c.running
	c.running = true
	c.mu.Unlock()
	if start {
		go c.run()
	}
	return <-op.done
}

func (c *committer) run() {
	for {
		if c.window > 0 {
			time.Sleep(c.window)
		}
		c.mu.Lock()
		batch := c.pending
		c.pending = nil
		if len(batch) == 0 {
			c.running = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		err := c.db.Update(func(tx *bolt.Tx) error {
			for _, op := range batch {
				if err := op.fn(tx); err != nil {
					return err
				}
			}
			return nil
		})
		if err == nil {
			for _, op := range batch {
				op.done <- nil
			}
			continue
		}
		// One write failed the shared transaction: commit each on its
		// own so only the failing write reports the error.
		for _, op := range batch {
			op.done <- c.db.Update(op.fn)
		}
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
