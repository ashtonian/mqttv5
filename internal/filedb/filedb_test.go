package filedb

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

var bucket = []byte("b")

func open(t *testing.T, dir string, cfg Config) *DB {
	t.Helper()
	cfg.Mode = 0o600
	db, err := Open(dir, "test.db", cfg, bucket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSecondOpenIsLocked(t *testing.T) {
	dir := t.TempDir()
	open(t, dir, Config{})
	if _, err := Open(dir, "test.db", Config{LockTimeout: 1, Mode: 0o600}, bucket); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open: %v, want ErrLocked", err)
	}
}

// The policies whose writes share transactions.
var grouping = []struct {
	name string
	sync SyncPolicy
}{{"group commit", SyncGroupCommit}, {"none", SyncNone}}

// A write that fails does not fail the writes committed with it.
func TestGroupCommitIsolatesFailures(t *testing.T) {
	for _, tt := range grouping {
		t.Run(tt.name, func(t *testing.T) {
			db := open(t, t.TempDir(), Config{Sync: tt.sync})
			bad := errors.New("refused")
			var wg sync.WaitGroup
			errs := make([]error, 64)
			for i := range errs {
				wg.Go(func() {
					errs[i] = db.Update(context.Background(), func(tx *bolt.Tx) error {
						if i%8 == 0 {
							return bad
						}
						return tx.Bucket(bucket).Put(fmt.Appendf(nil, "%02d", i), []byte{byte(i)})
					})
				})
			}
			wg.Wait()
			n := 0
			if err := db.View(func(tx *bolt.Tx) error { n = tx.Bucket(bucket).Stats().KeyN; return nil }); err != nil {
				t.Fatal(err)
			}
			for i, err := range errs {
				if (i%8 == 0) != errors.Is(err, bad) {
					t.Fatalf("write %d: %v", i, err)
				}
			}
			if n != 56 {
				t.Fatalf("%d keys committed, want 56", n)
			}
		})
	}
}

// Writes that arrive while a transaction commits all go into the next
// one.
func TestWritesArrivingDuringACommitShareTheNext(t *testing.T) {
	for _, tt := range grouping {
		t.Run(tt.name, func(t *testing.T) {
			db := open(t, t.TempDir(), Config{Sync: tt.sync})
			entered, hold := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(hold) })
			first := make(chan error, 1)
			go func() {
				first <- db.Update(context.Background(), func(tx *bolt.Tx) error {
					close(entered)
					<-hold
					return tx.Bucket(bucket).Put([]byte("first"), []byte{0})
				})
			}()
			<-entered

			const n = 16
			txs := make([]int, n)
			var wg sync.WaitGroup
			// On failure, let every write finish before the file closes.
			t.Cleanup(wg.Wait)
			t.Cleanup(release)
			for i := range n {
				wg.Go(func() {
					if err := db.Update(context.Background(), func(tx *bolt.Tx) error {
						txs[i] = tx.ID()
						return tx.Bucket(bucket).Put(fmt.Appendf(nil, "%02d", i), []byte{byte(i)})
					}); err != nil {
						t.Errorf("write %d: %v", i, err)
					}
				})
			}
			waitPending(t, db, n)
			release()
			wg.Wait()
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			if txs = slices.Compact(txs); len(txs) != 1 {
				t.Fatalf("the waiting writes committed in transactions %v, want one", txs)
			}
		})
	}
}

// waitPending waits until n writes are queued behind the running commit.
func waitPending(t *testing.T, db *DB, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		db.commit.mu.Lock()
		got := len(db.commit.pending)
		db.commit.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d writes queued, want %d", got, n)
		}
		runtime.Gosched()
	}
}

func TestClosed(t *testing.T) {
	db := open(t, t.TempDir(), Config{Sync: SyncEveryWrite})
	_ = db.Close()
	if err := db.Update(context.Background(), func(*bolt.Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Update after Close: %v", err)
	}
	if err := db.View(func(*bolt.Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("View after Close: %v", err)
	}
}

func TestSeal(t *testing.T) {
	v := Seal([]byte("payload"))
	if b, err := Unseal(v); err != nil || string(b) != "payload" {
		t.Fatalf("Unseal = %q, %v", b, err)
	}
	v[0] ^= 1
	if _, err := Unseal(v); err == nil {
		t.Fatal("flipped bit not detected")
	}
	if _, err := Unseal([]byte{1, 2}); err == nil {
		t.Fatal("short value accepted")
	}
}

// Under every policy, concurrent writes all commit and are there after
// the file is reopened.
func TestWritesPersistUnderEveryPolicy(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  Config
	}{
		{"group commit", Config{Sync: SyncGroupCommit}},
		{"group commit with window", Config{Sync: SyncGroupCommit, CommitWindow: 1}},
		{"every write", Config{Sync: SyncEveryWrite}},
		{"none", Config{Sync: SyncNone}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			db := open(t, dir, tt.cfg)
			const n = 100
			var wg sync.WaitGroup
			for i := range n {
				wg.Go(func() {
					if err := db.Update(context.Background(), func(tx *bolt.Tx) error {
						return tx.Bucket(bucket).Put(fmt.Appendf(nil, "%03d", i), Seal([]byte{byte(i)}))
					}); err != nil {
						t.Errorf("write %d: %v", i, err)
					}
				})
			}
			wg.Wait()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = open(t, dir, tt.cfg)
			if err := db.View(func(tx *bolt.Tx) error {
				for i := range n {
					v, err := Unseal(tx.Bucket(bucket).Get(fmt.Appendf(nil, "%03d", i)))
					if err != nil || len(v) != 1 || v[0] != byte(i) {
						return fmt.Errorf("key %d after reopen: %v %v", i, v, err)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenRejectsUnknownPolicy(t *testing.T) {
	if _, err := Open(t.TempDir(), "test.db", Config{Sync: SyncNone + 1, Mode: 0o600}, bucket); err == nil {
		t.Fatal("Open accepted an unknown SyncPolicy")
	}
}

// recordSyncs replaces syncDir for the test and returns the directories
// it is asked to sync.
func recordSyncs(t *testing.T) *[]string {
	t.Helper()
	var synced []string
	real := syncDir
	syncDir = func(dir string) error {
		synced = append(synced, dir)
		return real(dir)
	}
	t.Cleanup(func() { syncDir = real })
	return &synced
}

// A store created under new directories survives power loss: Open syncs
// every directory that gained an entry, outermost first.
func TestOpenSyncsNewDirectoryEntries(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a", "b")
	synced := recordSyncs(t)
	db := open(t, dir, Config{})
	want := []string{root, filepath.Join(root, "a"), dir}
	if !slices.Equal(*synced, want) {
		t.Fatalf("synced %q, want %q", *synced, want)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	*synced = nil
	open(t, dir, Config{})
	if len(*synced) != 0 {
		t.Fatalf("reopening an existing store synced %q", *synced)
	}

	*synced = nil
	open(t, filepath.Join(root, "c"), Config{Sync: SyncNone})
	if len(*synced) != 0 {
		t.Fatalf("SyncNone synced %q", *synced)
	}
}
