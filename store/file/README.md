# store/file

A file-backed `session.Store` for [`github.com/ashtonian/mqttv5`](../../README.md):
unfinished QoS 1/2 flows survive a process crash (and, with the default
sync policy, power loss), and a restarted client resumes them.

```go
import storefile "github.com/ashtonian/mqttv5/store/file"

st, err := storefile.Open("/var/lib/myapp/mqtt-session")
if err != nil {
    return err // storefile.ErrLocked: another process owns this session
}
defer st.Close()

cli, err := mqttv5.New(
    mqttv5.WithBroker("mqtts://broker:8883"),
    mqttv5.WithClientID("device-42"),
    mqttv5.WithStore(st),
)
```

## Overview

The client keeps its session state in memory and writes each change to
the store before acting on it on the network (see the `session`
package documentation for the exact order). After a crash, the next
process opens the same directory, `Connect` loads the state, sends
CONNECT with CleanStart=0, and the session engine:

- resends unacknowledged QoS 1/2 PUBLISHes with DUP=1, in their
  original order;
- resends PUBREL for QoS 2 messages whose PUBREC had arrived;
- answers the broker's PUBREL for inbound QoS 2 messages it had already
  acknowledged with PUBCOMP, without delivering them again;
- reuses a broker-assigned ClientID;
- keeps each record's `Ref`, so a `mqttv5.QueuePublisher` on a
  [`queue/file`](../../queue/file/README.md) queue finds the exchange it
  had started for a queued message and continues it instead of
  publishing the message again.

If the broker no longer has the session (CONNACK Session Present = 0),
unacknowledged publishes follow `mqttv5.WithSessionLossPolicy`.
`mqttv5.WithCleanStart(true)` discards the stored session instead of
resuming it.

The store is one [bbolt](https://github.com/etcd-io/bbolt) database file,
`session.db`, inside the directory you pass to `Open`. Each record
carries a CRC32C; a record that fails it is skipped on load and
reported by `Store.Corrupt()` rather than stopping the client.

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `WithSyncPolicy(p)` | `SyncPolicy` | `SyncGroupCommit` | When a write reaches stable storage; see below. |
| `WithCommitWindow(d)` | `time.Duration` | `0` | Extra wait before each group commit to gather more writes per fsync. Zero commits as soon as the previous commit finishes. Only used by `SyncGroupCommit`. |
| `WithLockTimeout(d)` | `time.Duration` | `1s` | How long `Open` waits for another holder of the directory before returning `ErrLocked`. |
| `WithFileMode(m)` | `os.FileMode` | `0600` | Permissions of a newly created `session.db`. The directory is created `0700`. |

### Sync policies

| Policy | Guarantee when Put/Delete returns | Throughput |
|---|---|---|
| `SyncGroupCommit` | On stable storage: survives process crash and power loss. `Open` syncs the directories it creates entries in, so a new store survives power loss too (on Windows, as far as NTFS journaling keeps new names). | One fsync per transaction; concurrent writes share a transaction. A lone writer pays one fsync, no batching delay. |
| `SyncEveryWrite` | Same as group commit. | One fsync per write, regardless of concurrency. |
| `SyncNone` | In the operating system's page cache: survives a process crash, not power loss or a kernel crash. | No fsync. |

Measured on 2026-10-07 with `BenchmarkPut` (256-byte packets, three
runs of 2 s each):

| Policy, writers | macOS, Apple M2 Max internal SSD | Linux VM (OrbStack) on the same Mac |
|---|---:|---:|
| group commit, 1 | 5.7 ms/write | 6.1 ms/write |
| group commit, 8 | 1.5 ms/write | 2.2 ms/write |
| group commit, 64 | 0.14 ms/write (~7,000/s) | 0.32 ms/write (~3,100/s) |
| every write, 1–64 | 4.7–5.1 ms/write (~200/s) | 2.8–5.8 ms/write |
| none, 1–64 | 0.026–0.033 ms/write | 0.036–0.042 ms/write |

macOS fsync is `F_FULLFSYNC`, which waits for the drive's cache. fsync
latency varies by orders of magnitude between drives and file systems,
so these figures do not carry over to other hardware: measure on your
own with `go test -run '^$' -bench BenchmarkPut ./store/file`.

For the client, every QoS 1/2 Publish waits for one store write before
the PUBLISH is sent, and a QoS 2 exchange writes twice more; inbound QoS
2 messages write once on Ack and once on PUBREL. Size the policy to your
publish rate and latency budget.

## External dependencies

| Dependency | Version | Why |
|---|---|---|
| `go.etcd.io/bbolt` | v1.5.0 | Transactional single-file B+tree with an exclusive file lock. |
| `golang.org/x/sys` | v0.48.0 (indirect) | Required by bbolt. |
| `github.com/ashtonian/mqttv5/internal/filedb` | this repository | The bbolt layer shared with `queue/file`: open under lock, group commit, checksums. |

The core `mqttv5` module stays dependency-free; only programs that
import this submodule pull these in.

## API

| Symbol | Description |
|---|---|
| `Open(dir string, opts ...Option) (*Store, error)` | Opens or creates the store in `dir`. Returns `ErrLocked` when another `Store` (in any process) holds it. |
| `(*Store).Load / Put / Delete / SetMeta / Reset / Close` | The `session.Store` contract. Put and Delete are durable per the sync policy when they return. |
| `(*Store).Path() string` | Path of `session.db`. |
| `(*Store).Corrupt() []error` | Records the last `Load` skipped (checksum or decode failure), each wrapping `ErrCorrupt`. |
| `ErrLocked`, `ErrClosed`, `ErrCorrupt` | Sentinels for `errors.Is`. |
| `FileName` | `"session.db"`. |

## Build, run, test

```bash
go -C store/file test -race ./...                      # conformance, locking, corruption, crash recovery
go -C store/file test -run TestStoreCrashRecovery -v   # SIGKILL a child client mid-flight and resume
go -C store/file test -run '^$' -bench BenchmarkPut    # throughput per sync policy
```

`TestStoreCrashRecovery` re-executes the test binary as a child client,
kills it with SIGKILL while QoS 1, QoS 2 (after PUBREC) and inbound QoS 2
flows are open, and checks that a new client on the same directory
resumes each of them. The conformance suite is
`github.com/ashtonian/mqttv5/session/storetest`; run it against any
custom `session.Store`.

## Operations

**Health.** `Open` failing is the only startup signal; at run time,
store errors are counted in `mqttv5.Client.Stats().StoreErrors` and
logged at Error level with the operation name (`put outbound`,
`delete inbound`, …).

**Disk usage.** bbolt reuses freed pages but never shrinks the file.
Its size tracks the peak number of unfinished flows times the packet
size; with the default limits that is bounded by 65,535 outbound
packets. To reclaim space after an unusual backlog, stop the client
and delete or compact `session.db` (`bbolt compact`).

**Failure modes.**

| Symptom | Cause | What to do |
|---|---|---|
| `Open` returns `ErrLocked` | Another process (or another `Store` in this process) has the directory open. | Check for a second instance with the same session directory (`lsof session.db`). Each client identity needs its own directory. A crashed process releases the lock when it exits. |
| `Connect` returns `load session store: …` | `session.db` cannot be read (permissions, disk failure). | Check the file's owner and mode; check the disk. Deleting `session.db` starts a fresh session (unacknowledged messages are lost). |
| `Stats().StoreErrors` increasing | Writes failing (disk full, I/O errors). | A failed write before a PUBLISH fails that Publish; later failures are logged and the protocol continues in memory, so a crash at that point can resend or drop the affected messages. Free disk space or replace the volume. |
| `Store.Corrupt()` non-empty after `Load` | A record failed its checksum (torn hardware write, external modification). | The affected flows are not resumed. Inspect the logs for the packet identifiers; the rest of the session is intact. |
| Messages resent after a restart that the broker had already acknowledged | The acknowledgement arrived but its Delete had not committed when the process died. | Expected at-least-once behaviour for QoS 1; QoS 2 stays exactly-once while the broker keeps the session. |

## Security

`session.db` holds the full bytes of unacknowledged outbound PUBLISH
packets — topics, payloads and properties — in plain form. The file is
created `0600` inside a `0700` directory; keep it on storage with the
same protection you give the payloads, and encrypt the volume if they
are sensitive. No credentials are stored.

## Code map

| File | Contents |
|---|---|
| `store.go` | `Open`, options, the `session.Store` implementation, record encoding. |
| `../../internal/filedb` | Opening under the file lock, the group committer, CRC32C sealing (shared with `queue/file`). |
| `store_test.go` | Conformance suite for each sync policy, lock, corruption, closed-store tests. |
| `crash_test.go` | Kill-and-resume test with a child process. |
| `bench_test.go` | `BenchmarkPut` per sync policy and writer count. |

## Upgrading from v0.10

Earlier versions wrote an `outbound/` and `inbound/` directory of
individual files that the client never read back. They are ignored; you
may delete them. The `session.Store` interface itself changed; see the
migration guide.
