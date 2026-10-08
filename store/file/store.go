// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package file is a file-backed [session.Store]: the in-flight QoS 1/2
// state of an mqttv5 client survives process crashes and, depending on
// the [SyncPolicy], power loss.
//
//	st, err := file.Open("/var/lib/myapp/mqtt-session")
//	if err != nil { ... }
//	defer st.Close()
//	cli, err := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithClientID("dev-1"), mqttv5.WithStore(st))
//
// State lives in one bbolt database file, session.db, inside the given
// directory. bbolt writes each transaction atomically and holds an
// exclusive file lock, so a second Open of the same directory — from
// this process or another — fails with [ErrLocked] instead of two
// clients corrupting one session.
//
// Durability: every Put and Delete returns only after its change is
// committed according to the SyncPolicy. The default, [SyncGroupCommit],
// commits a write immediately when no commit is running and otherwise
// joins it to the next one, so a lone writer pays one fsync and
// concurrent writers share fsyncs while each still waits for its own
// write to reach stable storage.
package file

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/ashtonian/mqttv5/internal/filedb"
	"github.com/ashtonian/mqttv5/session"
)

// FileName is the database file created inside the store directory.
const FileName = "session.db"

var (
	// ErrLocked is returned by Open when another Store holds the
	// directory.
	ErrLocked = filedb.ErrLocked
	// ErrClosed is returned by operations on a closed Store.
	ErrClosed = filedb.ErrClosed
	// ErrCorrupt wraps a record that failed its checksum or could not
	// be decoded. Load skips such records and reports them through
	// [Store.Corrupt].
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
	// gather writes before committing. Zero commits as soon as the
	// previous commit finishes; writes that arrive during an fsync are
	// batched either way.
	DefaultCommitWindow = 0
	// DefaultLockTimeout is how long Open waits for another holder to
	// release the directory before returning ErrLocked.
	DefaultLockTimeout = time.Second
)

// Option configures Open.
type Option func(*filedb.Config)

// WithSyncPolicy sets the durability policy. Default SyncGroupCommit.
func WithSyncPolicy(p SyncPolicy) Option { return func(c *filedb.Config) { c.Sync = p } }

// WithCommitWindow adds a wait before each SyncGroupCommit transaction
// to gather more writes per fsync, at the cost of that much latency.
// Default DefaultCommitWindow (none).
func WithCommitWindow(d time.Duration) Option { return func(c *filedb.Config) { c.CommitWindow = d } }

// WithLockTimeout sets how long Open waits for the directory lock.
// Default DefaultLockTimeout.
func WithLockTimeout(d time.Duration) Option { return func(c *filedb.Config) { c.LockTimeout = d } }

// WithFileMode sets the permissions of a newly created database file.
// Default 0600.
func WithFileMode(m os.FileMode) Option { return func(c *filedb.Config) { c.Mode = m } }

var (
	bucketMeta = []byte("meta")
	bucketOut  = []byte("out")
	bucketIn   = []byte("in")
	keyMeta    = []byte("meta")
)

// Store is a [session.Store] backed by a bbolt database. Safe for
// concurrent use.
type Store struct {
	db *filedb.DB

	mu      sync.Mutex
	corrupt []error
}

var _ session.Store = (*Store)(nil)

// Open opens or creates the store in dir, creating dir if needed.
// Existing state is kept: that is the crash-recovery path.
func Open(dir string, opts ...Option) (*Store, error) {
	cfg := filedb.Config{CommitWindow: DefaultCommitWindow, LockTimeout: DefaultLockTimeout, Mode: 0o600}
	for _, opt := range opts {
		opt(&cfg)
	}
	db, err := filedb.Open(dir, FileName, cfg, bucketMeta, bucketOut, bucketIn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Path returns the database file path.
func (s *Store) Path() string { return s.db.Path() }

// Corrupt returns the records the last Load skipped because they failed
// their checksum or could not be decoded.
func (s *Store) Corrupt() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.corrupt)
}

// Load implements [session.Store].
func (s *Store) Load(ctx context.Context) (session.Meta, []session.Record, error) {
	if err := ctx.Err(); err != nil {
		return session.Meta{}, nil, err
	}
	var (
		meta    session.Meta
		recs    []session.Record
		corrupt []error
	)
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketMeta).Get(keyMeta); v != nil {
			m, err := decodeMeta(v)
			if err != nil {
				corrupt = append(corrupt, fmt.Errorf("%w: meta: %v", ErrCorrupt, err))
			} else {
				meta = m
			}
		}
		for _, dir := range []session.Direction{session.Outbound, session.Inbound} {
			err := tx.Bucket(bucketFor(dir)).ForEach(func(k, v []byte) error {
				if len(k) != 2 {
					corrupt = append(corrupt, fmt.Errorf("%w: key % x", ErrCorrupt, k))
					return nil
				}
				id := binary.BigEndian.Uint16(k)
				r, err := decodeRecord(v)
				if err != nil {
					corrupt = append(corrupt, fmt.Errorf("%w: dir %d packet id %d: %v", ErrCorrupt, dir, id, err))
					return nil
				}
				r.Key = session.RecordKey{Dir: dir, PacketID: id}
				recs = append(recs, r)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return session.Meta{}, nil, err
	}
	slices.SortFunc(recs, func(a, b session.Record) int {
		if c := cmp.Compare(a.Key.Dir, b.Key.Dir); c != 0 {
			return c
		}
		if a.Key.Dir == session.Outbound {
			return cmp.Compare(a.Seq, b.Seq)
		}
		return cmp.Compare(a.Key.PacketID, b.Key.PacketID)
	})
	s.mu.Lock()
	s.corrupt = corrupt
	s.mu.Unlock()
	return meta, recs, nil
}

// Put implements [session.Store].
func (s *Store) Put(ctx context.Context, r session.Record) error {
	if err := r.Validate(); err != nil {
		return err
	}
	v := encodeRecord(r)
	k := idKey(r.Key.PacketID)
	return s.db.Update(ctx, func(tx *bolt.Tx) error { return tx.Bucket(bucketFor(r.Key.Dir)).Put(k, v) })
}

// Delete implements [session.Store].
func (s *Store) Delete(ctx context.Context, k session.RecordKey) error {
	key := idKey(k.PacketID)
	return s.db.Update(ctx, func(tx *bolt.Tx) error { return tx.Bucket(bucketFor(k.Dir)).Delete(key) })
}

// SetMeta implements [session.Store].
func (s *Store) SetMeta(ctx context.Context, m session.Meta) error {
	v := encodeMeta(m)
	return s.db.Update(ctx, func(tx *bolt.Tx) error { return tx.Bucket(bucketMeta).Put(keyMeta, v) })
}

// Reset implements [session.Store].
func (s *Store) Reset(ctx context.Context) error {
	return s.db.Update(ctx, func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketOut, bucketIn} {
			if err := tx.DeleteBucket(b); err != nil {
				return err
			}
			if _, err := tx.CreateBucket(b); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close implements [session.Store].
func (s *Store) Close() error { return s.db.Close() }

func bucketFor(d session.Direction) []byte {
	if d == session.Inbound {
		return bucketIn
	}
	return bucketOut
}

func idKey(id uint16) []byte { return binary.BigEndian.AppendUint16(nil, id) }

// Record encoding, version 1 (all integers big-endian):
//
//	version    1 byte  (1)
//	qos        1 byte
//	phase      1 byte
//	flags      1 byte  (bit 0: ExpiresAt, bit 1: Ref, bit 2: PubrecSeq present)
//	seq        8 bytes
//	pubrec seq 8 bytes, only when flag bit 2 is set
//	expires    8 bytes Unix nanoseconds, only when flag bit 0 is set
//	ref        1 byte length + bytes, only when flag bit 1 is set
//	packet     remaining bytes before the checksum
//	crc32c     4 bytes over everything before it
const recordVersion = 1

const (
	flagExpires = 1 << iota
	flagRef
	flagPubrecSeq
)

func encodeRecord(r session.Record) []byte {
	b := make([]byte, 0, 4+8+8+8+1+len(r.Ref)+len(r.Packet)+4)
	var flags byte
	if !r.ExpiresAt.IsZero() {
		flags |= flagExpires
	}
	if r.Ref != nil {
		flags |= flagRef
	}
	if r.PubrecSeq != 0 {
		flags |= flagPubrecSeq
	}
	b = append(b, recordVersion, r.QoS, byte(r.Phase), flags)
	b = binary.BigEndian.AppendUint64(b, r.Seq)
	if flags&flagPubrecSeq != 0 {
		b = binary.BigEndian.AppendUint64(b, r.PubrecSeq)
	}
	if flags&flagExpires != 0 {
		b = binary.BigEndian.AppendUint64(b, uint64(r.ExpiresAt.UnixNano()))
	}
	if flags&flagRef != 0 {
		b = append(append(b, byte(len(r.Ref))), r.Ref...)
	}
	b = append(b, r.Packet...)
	return filedb.Seal(b)
}

func decodeRecord(v []byte) (session.Record, error) {
	body, err := filedb.Unseal(v)
	if err != nil {
		return session.Record{}, err
	}
	if len(body) < 12 || body[0] != recordVersion {
		return session.Record{}, fmt.Errorf("unsupported record layout (version %d, %d bytes)", firstByte(body), len(body))
	}
	r := session.Record{QoS: body[1], Phase: session.Phase(body[2]), Seq: binary.BigEndian.Uint64(body[4:12])}
	flags, rest := body[3], body[12:]
	if flags&flagPubrecSeq != 0 {
		if len(rest) < 8 {
			return session.Record{}, errors.New("truncated pubrec seq")
		}
		r.PubrecSeq = binary.BigEndian.Uint64(rest)
		rest = rest[8:]
	}
	if flags&flagExpires != 0 {
		if len(rest) < 8 {
			return session.Record{}, errors.New("truncated expiry")
		}
		r.ExpiresAt = time.Unix(0, int64(binary.BigEndian.Uint64(rest)))
		rest = rest[8:]
	}
	if flags&flagRef != 0 {
		if len(rest) < 1 || len(rest) < 1+int(rest[0]) {
			return session.Record{}, errors.New("truncated ref")
		}
		r.Ref = bytes.Clone(rest[1 : 1+int(rest[0])])
		rest = rest[1+int(rest[0]):]
	}
	if len(rest) > 0 {
		r.Packet = bytes.Clone(rest)
	}
	return r, nil
}

// Meta encoding, version 1: version, session expiry (4 bytes), client ID
// (remaining bytes), crc32c.
func encodeMeta(m session.Meta) []byte {
	b := make([]byte, 0, 1+4+len(m.ClientID)+4)
	b = append(b, recordVersion)
	b = binary.BigEndian.AppendUint32(b, m.SessionExpiry)
	b = append(b, m.ClientID...)
	return filedb.Seal(b)
}

func decodeMeta(v []byte) (session.Meta, error) {
	body, err := filedb.Unseal(v)
	if err != nil {
		return session.Meta{}, err
	}
	if len(body) < 5 || body[0] != recordVersion {
		return session.Meta{}, fmt.Errorf("unsupported meta layout (version %d, %d bytes)", firstByte(body), len(body))
	}
	return session.Meta{SessionExpiry: binary.BigEndian.Uint32(body[1:5]), ClientID: string(body[5:])}, nil
}

func firstByte(b []byte) int {
	if len(b) == 0 {
		return -1
	}
	return int(b[0])
}
