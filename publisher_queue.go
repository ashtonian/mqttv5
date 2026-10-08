// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	// ErrQueueClosed is returned by a [PublisherQueue] or
	// [QueuePublisher] after Close.
	ErrQueueClosed = errors.New("mqttv5: queue is closed")

	// ErrQueueFull is returned when a bounded queue has no room: under
	// [DropNewest] when it holds its maximum, under [DropOldest] when
	// every entry it holds is already being published.
	ErrQueueFull = errors.New("mqttv5: queue is full")
)

// QueueEntry is one message in a [PublisherQueue].
type QueueEntry struct {
	// Seq is assigned by the queue at Enqueue. It increases strictly
	// within a queue and is never reused.
	Seq uint64

	// ID identifies the message for good. [QueuePublisher] stores it with
	// the session record of the message's exchange, so after a restart
	// it continues that exchange instead of starting another, and sends
	// it as the [QueueIDProperty] user property when
	// [WithQueueIdempotencyKey] is set.
	ID string

	// Publish is the message, QoS 1 or 2. PacketID and Dup are unused;
	// the Message Expiry Interval is carried by ExpiresAt.
	Publish PublishOptions

	EnqueuedAt time.Time

	// ExpiresAt is when the message may no longer be sent: the earlier of
	// its Message Expiry Interval and [WithQueueTTL], counted from
	// EnqueuedAt. Zero means never.
	ExpiresAt time.Time
}

// clone returns a deep copy of e.
func (e QueueEntry) clone() QueueEntry {
	e.Publish = e.Publish.clone()
	return e
}

// QueueLimit bounds a [PublisherQueue] for one Enqueue.
type QueueLimit struct {
	// Max is the most entries the queue may hold; 0 means no bound.
	Max int
	// Policy says what Enqueue does when the queue holds Max entries:
	// [DropNewest] refuses the new entry with [ErrQueueFull];
	// [DropOldest] removes the oldest entries with Seq above Keep to make
	// room.
	Policy DropPolicy
	// Keep is the highest Seq DropOldest must not remove: the entries up
	// to it are being published.
	Keep uint64
}

// PublisherQueue stores the messages a [QueuePublisher] has accepted
// until the broker has them. Implementations must be safe for concurrent
// use and must not share memory with their callers: Enqueue copies the
// entry, and the entries they return belong to the caller.
// [github.com/ashtonian/mqttv5/queuetest] checks an implementation
// against this contract.
type PublisherQueue interface {
	// Enqueue stores e, assigning it the next Seq, which it returns.
	// When the queue already holds limit.Max entries it refuses with
	// ErrQueueFull or, under DropOldest, removes the oldest entries above
	// limit.Keep and returns them; the check, the removal and the insert
	// are one atomic step. It returns ErrQueueFull when there is nothing
	// it may remove.
	Enqueue(ctx context.Context, e QueueEntry, limit QueueLimit) (seq uint64, evicted []QueueEntry, err error)

	// Peek returns up to n entries with Seq above after, oldest first.
	Peek(ctx context.Context, after uint64, n int) ([]QueueEntry, error)

	// Ack removes the entry with seq. Acking a missing entry is not an
	// error.
	Ack(ctx context.Context, seq uint64) error

	// Len returns the number of entries, in constant time.
	Len(ctx context.Context) (int, error)

	// Close releases the queue's resources. Later calls return
	// ErrQueueClosed.
	Close() error
}

// MemoryPublisherQueue is an in-memory [PublisherQueue]: entries do not
// survive the process. Use the queue/file submodule to keep them across
// restarts.
type MemoryPublisherQueue struct {
	mu      sync.Mutex
	closed  bool
	nextSeq uint64
	// entries is ordered by Seq. An acked entry leaves a hole that keeps
	// only its Seq, for search, until reclaim removes it.
	entries []memQueueEntry
	head    int
	count   int // entries not acked
}

type memQueueEntry struct {
	e     QueueEntry
	acked bool
}

var _ PublisherQueue = (*MemoryPublisherQueue)(nil)

// NewMemoryPublisherQueue returns an empty in-memory queue.
func NewMemoryPublisherQueue() *MemoryPublisherQueue {
	return &MemoryPublisherQueue{}
}

// Enqueue implements [PublisherQueue].
func (q *MemoryPublisherQueue) Enqueue(_ context.Context, e QueueEntry, limit QueueLimit) (uint64, []QueueEntry, error) {
	e = e.clone()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, nil, ErrQueueClosed
	}
	var evicted []QueueEntry
	if limit.Max > 0 && q.count >= limit.Max {
		if limit.Policy != DropOldest {
			return 0, nil, ErrQueueFull
		}
		need := q.count - limit.Max + 1
		var victims []int
		for i := q.search(limit.Keep); i < len(q.entries) && len(victims) < need; i++ {
			if !q.entries[i].acked {
				victims = append(victims, i)
			}
		}
		if len(victims) < need {
			return 0, nil, ErrQueueFull
		}
		for _, i := range victims {
			evicted = append(evicted, q.entries[i].e)
			q.entries[i] = hole(q.entries[i].e.Seq)
		}
		q.count -= len(victims)
		q.reclaim()
	}
	q.nextSeq++
	e.Seq = q.nextSeq
	q.entries = append(q.entries, memQueueEntry{e: e})
	q.count++
	return e.Seq, evicted, nil
}

// Peek implements [PublisherQueue].
func (q *MemoryPublisherQueue) Peek(_ context.Context, after uint64, n int) ([]QueueEntry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	var out []QueueEntry
	for i := q.search(after); i < len(q.entries) && len(out) < n; i++ {
		if !q.entries[i].acked {
			out = append(out, q.entries[i].e.clone())
		}
	}
	return out, nil
}

// Ack implements [PublisherQueue].
func (q *MemoryPublisherQueue) Ack(_ context.Context, seq uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if i := q.search(seq - 1); i < len(q.entries) && q.entries[i].e.Seq == seq && !q.entries[i].acked {
		q.entries[i] = hole(seq)
		q.count--
		q.reclaim()
	}
	return nil
}

// Len implements [PublisherQueue].
func (q *MemoryPublisherQueue) Len(context.Context) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, ErrQueueClosed
	}
	return q.count, nil
}

// Close implements [PublisherQueue].
func (q *MemoryPublisherQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.entries, q.head, q.count = nil, 0, 0
	return nil
}

// search returns the index of the first entry with Seq above seq.
func (q *MemoryPublisherQueue) search(seq uint64) int {
	return q.head + sort.Search(len(q.entries)-q.head, func(i int) bool { return q.entries[q.head+i].e.Seq > seq })
}

// hole is what an acked entry leaves behind: its Seq and nothing it
// referenced.
func hole(seq uint64) memQueueEntry {
	return memQueueEntry{e: QueueEntry{Seq: seq}, acked: true}
}

// reclaim drops the holes acked entries leave: those at the head at once,
// and all of them once they outnumber the entries left, so the queue's
// memory follows what it holds rather than the traffic it has seen —
// also while an entry at the head stays unacked.
func (q *MemoryPublisherQueue) reclaim() {
	for q.head < len(q.entries) && q.entries[q.head].acked {
		q.entries[q.head] = memQueueEntry{}
		q.head++
	}
	holes := len(q.entries) - q.head - q.count
	if (q.head <= 1024 || q.head <= len(q.entries)/2) && (holes <= 64 || holes <= q.count) {
		return
	}
	live := q.entries[:0]
	for _, e := range q.entries[q.head:] {
		if !e.acked {
			live = append(live, e)
		}
	}
	clear(q.entries[len(live):])
	if cap(live) > 1024 && cap(live) > 4*len(live) {
		live = append([]memQueueEntry(nil), live...)
	}
	q.entries, q.head = live, 0
}
