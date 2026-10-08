# internal/filedb

The bbolt layer under [`store/file`](../../store/file) and
[`queue/file`](../../queue/file). Internal to this repository: only
modules under `github.com/ashtonian/mqttv5` can import it, and its API
may change in any release.

| | |
|---|---|
| Module | `github.com/ashtonian/mqttv5/internal/filedb` (its own `go.mod`, so the core module stays dependency-free) |
| Depends on | [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt) v1.5.0 |

## Overview

- **One owner per file.** `Open` takes bbolt's exclusive file lock,
  waiting up to `LockTimeout`; a second process or handle gets
  `ErrLocked`. The directory is created with mode 0700, the file with
  `Mode`.
- **Durability by policy.** `Update` returns once its transaction is
  committed as the `SyncPolicy` promises. Unless the policy is
  `SyncNone`, `Open` also syncs every directory that gained an entry —
  the new file's directory and the parents of directories it created —
  because bbolt syncs the file's contents but not its name. Windows
  cannot sync a directory; there a new name is as durable as NTFS's
  metadata journaling makes it.
- **Group commit.** Under `SyncGroupCommit` a write that finds no commit
  running starts one; writes that arrive meanwhile queue and commit
  together in the next transaction, with one fsync. A lone writer is
  never delayed (unlike `bolt.DB.Batch`), unless `CommitWindow` asks for
  a wait. If one write fails a shared transaction, each write of that
  batch is committed on its own, so only the failing one reports an
  error — which is why a write function may run twice and must be
  idempotent within a transaction.
- **Checksums.** `Seal` appends a CRC32C to a value and `Unseal` checks
  it, so the stores can tell a corrupt record (`ErrCorrupt`) from a
  valid one.

## Configuration

`Config`, filled in by the stores from their own options:

| Field | Type | Default (set by the stores) | Description |
|---|---|---|---|
| `Sync` | `SyncPolicy` | `SyncGroupCommit` | `SyncGroupCommit`, `SyncEveryWrite` (fsync per write), or `SyncNone` (no fsync: survives a process crash, not power loss). |
| `CommitWindow` | `time.Duration` | 0 | Extra wait before each group commit to gather more writes. |
| `LockTimeout` | `time.Duration` | the store's `DefaultLockTimeout` | Wait for the file lock before `ErrLocked`. |
| `Mode` | `os.FileMode` | 0600 | Permission of a newly created file. |

## External dependencies

| Module | Version | Used for |
|---|---|---|
| `go.etcd.io/bbolt` | v1.5.0 | storage engine, file lock |

## API

| Symbol | Description |
|---|---|
| `Open(dir, name, Config, buckets...)` | Open or create `dir/name`, creating the buckets. |
| `(*DB) Update(ctx, fn)` | Write transaction, committed per the policy. |
| `(*DB) View(fn)` | Read transaction. |
| `(*DB) Path()`, `(*DB) Close()` | File path; release the file and lock. |
| `Seal`, `Unseal` | Append and verify a CRC32C. |
| `ErrLocked`, `ErrClosed`, `ErrCorrupt` | Sentinel errors. |

## Build, run, test

```bash
go -C internal/filedb test -race ./...
```

The tests cover the lock, concurrent writes persisting across a reopen
under every sync policy, the group-commit fallback when one write fails
a batch, the directories `Open` syncs, closed handles, and checksums.
The stores' own suites (`session/storetest`, `queuetest`, crash tests)
exercise it end to end.

## Operations

| Symptom | Check | Then |
|---|---|---|
| `ErrLocked` | another process using the same directory | One process per store directory. |
| `Open` fails with `sync directory …` | the file system under the store directory | Directory fsync is needed for power-loss durability; use a local file system that supports it, or `SyncNone` if a process crash is all the store must survive. |
| Slow writes | the sync policy and the disk's fsync latency | `SyncGroupCommit` amortises fsync across concurrent writers; a single writer pays one fsync per write. |
| `ErrCorrupt` | the stores' corruption hooks (`WithOnCorrupt` in `queue/file`) | The record is skipped or quarantined by the store. |

## Security

Files hold message payloads unencrypted with mode 0600 in a 0700
directory; encrypt the volume for sensitive data.

## Code map

| File | What |
|---|---|
| `filedb.go` | `Open`, `DB`, group commit, `Seal`/`Unseal` |
| `syncdir_unix.go`, `syncdir_other.go` | Syncing a directory's entries; a no-op where that is impossible |
| `filedb_test.go` | tests |
