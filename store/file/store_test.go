// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/session/storetest"
)

func factory(opts ...Option) storetest.Factory {
	return storetest.Factory{
		Open: func(t *testing.T) session.Store {
			s, err := Open(t.TempDir(), opts...)
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
		Reopen: func(t *testing.T, s session.Store) session.Store {
			dir := filepathDir(s.(*Store).Path())
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := Open(dir, opts...)
			if err != nil {
				t.Fatal(err)
			}
			return r
		},
	}
}

func filepathDir(p string) string { return p[:len(p)-len(FileName)-1] }

func TestConformanceGroupCommit(t *testing.T) { storetest.Run(t, factory()) }

func TestConformanceSyncEveryWrite(t *testing.T) {
	storetest.Run(t, factory(WithSyncPolicy(SyncEveryWrite)))
}

func TestConformanceSyncNone(t *testing.T) { storetest.Run(t, factory(WithSyncPolicy(SyncNone))) }

func TestSecondOpenIsLocked(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now()
	_, err = Open(dir, WithLockTimeout(50*time.Millisecond))
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("lock timeout not honoured")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(dir, WithLockTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Open after the holder closed: %v", err)
	}
	_ = r.Close()
}

func TestCorruptRecordsAreSkippedAndReported(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	good := session.Record{Key: session.RecordKey{Dir: session.Outbound, PacketID: 1}, Seq: 1, QoS: 1, Phase: session.AwaitPuback, Packet: []byte("ok")}
	if err := s.Put(ctx, good); err != nil {
		t.Fatal(err)
	}
	path := s.Path()
	_ = s.Close()

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		v := encodeRecord(session.Record{Seq: 2, QoS: 1, Phase: session.AwaitPuback, Packet: []byte("flipped")})
		v[len(v)/2] ^= 0xff
		return tx.Bucket(bucketOut).Put(idKey(2), v)
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, recs, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load with a corrupt record: %v", err)
	}
	if len(recs) != 1 || recs[0].Key.PacketID != 1 {
		t.Fatalf("records = %+v, want only the intact one", recs)
	}
	if c := s.Corrupt(); len(c) != 1 || !errors.Is(c[0], ErrCorrupt) {
		t.Fatalf("Corrupt() = %v", c)
	}
}

func TestClosedStoreReturnsErrClosed(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if err := s.Delete(context.Background(), session.RecordKey{Dir: session.Outbound, PacketID: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after Close = %v", err)
	}
}

// Group commit batches concurrent writers into shared transactions; all
// of them must still land.
func TestGroupCommitUnderConcurrency(t *testing.T) {
	s, err := Open(t.TempDir(), WithCommitWindow(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Go(func() {
			for i := 0; i < 20; i++ {
				id := uint16(w*20 + i + 1)
				r := session.Record{Key: session.RecordKey{Dir: session.Outbound, PacketID: id}, Seq: uint64(id), QoS: 1, Phase: session.AwaitPuback, Packet: []byte{byte(id)}}
				if err := s.Put(ctx, r); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	_, recs, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 32*20 {
		t.Fatalf("%d records, want %d", len(recs), 32*20)
	}
}
