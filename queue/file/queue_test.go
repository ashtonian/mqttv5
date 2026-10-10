// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/queuetest"
)

func factory(opts ...Option) queuetest.Factory {
	return queuetest.Factory{
		Open: func(t *testing.T) mqttv5.PublisherQueue {
			q, err := Open(t.TempDir(), opts...)
			if err != nil {
				t.Fatal(err)
			}
			return q
		},
		Reopen: func(t *testing.T, q mqttv5.PublisherQueue) mqttv5.PublisherQueue {
			fq := q.(*Queue)
			if err := fq.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := Open(dirOf(fq), opts...)
			if err != nil {
				t.Fatal(err)
			}
			return r
		},
	}
}

func dirOf(q *Queue) string { return q.Path()[:len(q.Path())-len(FileName)-1] }

// The conformance round trip keeps an explicit zero Message Expiry
// Interval apart from none.
func TestConformanceGroupCommit(t *testing.T) { queuetest.Run(t, factory()) }

func TestConformanceSyncEveryWrite(t *testing.T) {
	queuetest.Run(t, factory(WithSyncPolicy(SyncEveryWrite)))
}

func TestConformanceSyncNone(t *testing.T) { queuetest.Run(t, factory(WithSyncPolicy(SyncNone))) }

func entry(payload string) mqttv5.QueueEntry {
	return mqttv5.QueueEntry{ID: "id-" + payload, Publish: mqttv5.PublishOptions{Topic: "t", QoS: 1, Payload: []byte(payload)}}
}

// A second handle on the same directory is refused instead of
// overwriting the first one's entries.
func TestSecondOpenIsLocked(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if _, err := Open(dir, WithLockTimeout(10*time.Millisecond)); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open: %v, want ErrLocked", err)
	}
}

// An empty path is an error, not a temporary directory.
func TestOpenNeedsADirectory(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("Open(\"\") succeeded")
	}
}

// A corrupt entry is quarantined and reported; the entries around
// it keep flowing.
func TestCorruptEntriesAreQuarantined(t *testing.T) {
	var reported []uint64
	q, err := Open(t.TempDir(), WithOnCorrupt(func(seq uint64, err error) {
		if !errors.Is(err, ErrCorrupt) {
			t.Errorf("reported %v", err)
		}
		reported = append(reported, seq)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()
	var seqs []uint64
	for i := range 3 {
		seq, _, err := q.Enqueue(ctx, entry(fmt.Sprint(i)), mqttv5.QueueLimit{})
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	if err := q.db.Update(ctx, func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketEntries)
		v := append([]byte(nil), b.Get(seqKey(seqs[1]))...)
		v[len(v)/2] ^= 0xff
		return b.Put(seqKey(seqs[1]), v)
	}); err != nil {
		t.Fatal(err)
	}
	got, err := q.Peek(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].Publish.Payload) != "0" || string(got[1].Publish.Payload) != "2" {
		t.Fatalf("Peek around a corrupt entry = %+v", got)
	}
	if n, _ := q.Len(ctx); n != 2 {
		t.Fatalf("Len = %d after quarantine, want 2", n)
	}
	if len(reported) != 1 || reported[0] != seqs[1] || len(q.Corrupt()) != 1 {
		t.Fatalf("reported %v, Corrupt() %v", reported, q.Corrupt())
	}
	if err := q.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketQuarantine).Get(seqKey(seqs[1])) == nil {
			return errors.New("corrupt entry not kept in quarantine")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Len is constant-time.
func BenchmarkLen(b *testing.B) {
	q, err := Open(b.TempDir(), WithSyncPolicy(SyncNone))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()
	for i := range 50_000 {
		if _, _, err := q.Enqueue(ctx, entry(fmt.Sprint(i)), mqttv5.QueueLimit{}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		if n, _ := q.Len(ctx); n != 50_000 {
			b.Fatalf("Len = %d", n)
		}
	}
}

// BenchmarkEnqueue reports enqueues per second by SyncPolicy, with one
// writer and with 64 concurrent writers.
func BenchmarkEnqueue(b *testing.B) {
	for _, p := range []struct {
		name   string
		policy SyncPolicy
	}{{"group-commit", SyncGroupCommit}, {"every-write", SyncEveryWrite}, {"none", SyncNone}} {
		for _, writers := range []int{1, 64} {
			b.Run(fmt.Sprintf("%s/writers=%d", p.name, writers), func(b *testing.B) {
				q, err := Open(b.TempDir(), WithSyncPolicy(p.policy))
				if err != nil {
					b.Fatal(err)
				}
				defer q.Close()
				e := entry("payload-of-sixty-four-bytes-0123456789012345678901234567890123")
				var next atomic.Int64
				var wg sync.WaitGroup
				b.ResetTimer()
				start := time.Now()
				for range writers {
					wg.Go(func() {
						for next.Add(1) <= int64(b.N) {
							if _, _, err := q.Enqueue(context.Background(), e, mqttv5.QueueLimit{}); err != nil {
								b.Error(err)
								return
							}
						}
					})
				}
				wg.Wait()
				b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "enqueues/s")
			})
		}
	}
}
