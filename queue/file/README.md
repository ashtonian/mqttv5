# queue/file

A file-backed `mqttv5.PublisherQueue` for [`github.com/ashtonian/mqttv5`](../../README.md):
messages a `QueuePublisher` accepted survive a process crash (and, with
the default sync policy, power loss) and are published once the client
is connected again.

```go
import (
    queuefile "github.com/ashtonian/mqttv5/queue/file"
    storefile "github.com/ashtonian/mqttv5/store/file"
)

st, err := storefile.Open("/var/lib/myapp/mqtt-session")
if err != nil {
    return err
}
defer st.Close()
q, err := queuefile.Open("/var/lib/myapp/mqtt-queue")
if err != nil {
    return err // queuefile.ErrLocked: another process owns this queue
}

cli, err := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithClientID("device-42"), mqttv5.WithStore(st))
if err != nil {
    return err
}
pub, err := mqttv5.NewQueuePublisher(cli, q) // closes q in pub.Close
if err != nil {
    return err
}
_ = pub.Publish(ctx, wire.PublishOpts{Topic: "telemetry", QoS: 1, Payload: reading})
```

## Overview

The queue holds every message from `QueuePublisher.Publish` until the
broker has accepted it. Pair it with a persistent session store
(`store/file`): the `QueuePublisher` stores each message's ID with the
session record of its MQTT exchange and removes the message from the
queue before the session forgets the exchange. After a crash the next
process therefore

- continues every exchange that was in flight — QoS 1 PUBLISHes are
  resent with DUP=1 under their packet identifier, a QoS 2 message whose
  PUBREC had arrived resumes with PUBREL — instead of publishing those
  messages again;
- publishes the messages that were still waiting, in order.

Without a persistent session store the queue still keeps the messages,
but one that was in flight at the crash is published again as a new
message (consumers can recognise it with
`mqttv5.WithQueueIdempotencyKey`).

The queue is one [bbolt](https://github.com/etcd-io/bbolt) database file,
`queue.db`, inside the directory given to `Open`. Entries are keyed by
sequence number; the entry count is updated in the same transaction as
each insert and removal, so `Len` is constant-time, and `Peek` reads
forward from a sequence number with a cursor. A bounded `Enqueue` checks
the bound, evicts (under `DropOldest`) and inserts in one transaction, so
concurrent producers can never exceed it.

Each entry is stored as a small header (sequence, ID, enqueue and expiry
times with presence flags) followed by the PUBLISH packet exactly as MQTT
encodes it, so every option round-trips with its presence — an explicit
Message Expiry Interval of 0 stays 0 — and a CRC32C over both. An entry
that fails the checksum or does not decode is moved to a `quarantine`
bucket and reported instead of blocking the entries behind it.

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `WithSyncPolicy(p)` | `SyncPolicy` | `SyncGroupCommit` | When an Enqueue or Ack reaches stable storage; see below. |
| `WithCommitWindow(d)` | `time.Duration` | `0` | Extra wait before each group commit to gather more writes per fsync. Only used by `SyncGroupCommit`. |
| `WithLockTimeout(d)` | `time.Duration` | `1s` | How long `Open` waits for another holder of the directory before returning `ErrLocked`. |
| `WithFileMode(m)` | `os.FileMode` | `0600` | Permissions of a newly created `queue.db`. The directory is created `0700`. |
| `WithOnCorrupt(fn)` | `func(seq uint64, err error)` | none | Called for each entry quarantined as corrupt. |

The bound and drop policy are not queue options: `QueuePublisher` passes
them with every Enqueue (`mqttv5.WithQueueMaxSize`,
`mqttv5.WithQueueDropPolicy`).

### Sync policies

| Policy | Guarantee when Enqueue/Ack returns | Measured (macOS, M2 Max, 64-byte payload) |
|---|---|---|
| `SyncGroupCommit` | On stable storage: survives process crash and power loss. `Open` syncs the directories it creates entries in, so a new queue survives power loss too (on Windows, as far as NTFS journaling keeps new names). | 180/s with one producer, 5,300–6,050/s with 64 |
| `SyncEveryWrite` | Same as group commit. | ~180/s at any concurrency |
| `SyncNone` | In the OS page cache: survives a process crash, not power loss. | 3,500–10,400/s |

macOS fsync is `F_FULLFSYNC` (~5.5 ms); fsync latency varies by orders
of magnitude between drives and file systems, so these figures do not
carry over to other hardware. Measure on your own hardware with
`go -C queue/file test -run '^$' -bench BenchmarkEnqueue`.

## External dependencies

| Dependency | Version | Why |
|---|---|---|
| `go.etcd.io/bbolt` | v1.5.0 | Transactional single-file B+tree with an exclusive file lock. |
| `golang.org/x/sys` | v0.48.0 (indirect) | Required by bbolt. |
| `github.com/ashtonian/mqttv5/internal/filedb` | this repository | The bbolt layer shared with `store/file`: open under lock, group commit, checksums. |
| `github.com/ashtonian/mqttv5/store/file` | this repository | Tests only: the crash-recovery test runs a client with a file session store. |

## API

| Symbol | Description |
|---|---|
| `Open(dir string, opts ...Option) (*Queue, error)` | Opens or creates the queue in `dir`. Returns `ErrLocked` when another `Queue` (in any process) holds it. Existing entries are kept. |
| `(*Queue).Enqueue / Peek / Ack / Len / Close` | The `mqttv5.PublisherQueue` contract; Enqueue and Ack are durable per the sync policy when they return. |
| `(*Queue).Path() string` | Path of `queue.db`. |
| `(*Queue).Corrupt() []error` | Entries quarantined since `Open`, each wrapping `ErrCorrupt`. |
| `ErrLocked`, `ErrCorrupt` | Sentinels for `errors.Is`. After `Close`, calls return `mqttv5.ErrQueueClosed`. |
| `FileName` | `"queue.db"`. |

## Build, run, test

```bash
go -C queue/file test -race ./...                         # conformance per sync policy, locking, quarantine, crash recovery
go -C queue/file test -run TestQueueCrashRecovery -v      # SIGKILL a publishing child and restart on its files
go -C queue/file test -run '^$' -bench 'BenchmarkLen|BenchmarkEnqueue'
```

`TestQueueCrashRecovery` re-executes the test binary as a child that
publishes five messages through a `QueuePublisher` (window 4) on this
queue and a `store/file` session store, kills it with SIGKILL while four
exchanges are open, restarts on the same directories and checks the
broker sees exactly the PUBREL and DUP resends of those four and one new
PUBLISH for the fifth. The conformance suite is
`github.com/ashtonian/mqttv5/queuetest`; run it against any custom
`PublisherQueue`.

## Operations

**Health.** `Open` failing is the only startup signal. At run time,
`Len` is the backlog: alert when it grows while the client is connected.
Messages that cannot be published go to `mqttv5.WithDeadLetter`; queue
I/O errors are logged by the `QueuePublisher` at Error level
(`component=queue-publisher`).

**Disk usage.** bbolt reuses freed pages but never shrinks the file; its
size tracks the peak backlog. Bound the backlog with
`mqttv5.WithQueueMaxSize`. To reclaim space after an outage, stop the
process and compact `queue.db` (`bbolt compact`).

**Failure modes.**

| Symptom | Cause | What to do |
|---|---|---|
| `Open` returns `ErrLocked` | Another process (or another `Queue` here) has the directory open. | Find the other instance (`lsof queue.db`). Each publisher needs its own queue directory. A crashed process releases the lock when it exits. |
| `Publish` returns `mqttv5.ErrQueueFull` | The queue is at `WithQueueMaxSize` under `DropNewest`, or under `DropOldest` everything left is already in flight. | The broker is unreachable or slower than the producers. Check the connection; raise the bound or shed load. |
| `Corrupt()` non-empty, `WithOnCorrupt` firing | An entry failed its checksum or did not decode (torn hardware write, external modification). | The entry is in the `quarantine` bucket of `queue.db` with its raw bytes; it is not published. Inspect it offline with `bbolt get`. |
| A message published twice after a restart | It was in flight at the crash and no persistent session store was configured, the broker lost the session, or the process died between the broker's acknowledgement and the queue removal. | Use `store/file` with the same client ID; enable `WithQueueIdempotencyKey` so consumers can drop the copy. |
| Backlog not draining while connected | Messages failing with a reason that may pass (logged with `will retry`), or the broker's Receive Maximum is small. | Read the warnings for the reason code; raise `WithQueueWindow` if the link has high latency. |

## Security

`queue.db` holds the full content of queued messages — topics, payloads,
properties — in plain form. The file is created `0600` in a `0700`
directory; protect the volume like the payloads themselves and encrypt it
if they are sensitive. No credentials are stored.

## Code map

| File | Contents |
|---|---|
| `queue.go` | `Open`, options, the `PublisherQueue` implementation, entry encoding, quarantine. |
| `queue_test.go` | Conformance suite per sync policy, lock, quarantine, `BenchmarkLen`, `BenchmarkEnqueue`. |
| `crash_test.go` | Kill-and-restart test with a child process, a `QueuePublisher` and a `store/file` session. |
| `../../internal/filedb` | Opening under the file lock, the group committer, CRC32C sealing (shared with `store/file`). |
