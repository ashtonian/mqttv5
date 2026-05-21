// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"sync"
)

// Queue is an unbounded MPSC FIFO of T. Enqueue is non-blocking;
// Dequeue blocks until an item arrives, the queue is closed, or ctx
// cancels. After Close, queued items are still returned by Dequeue;
// once drained, Dequeue returns (zero, false).
type Queue[T any] struct {
	mu     sync.Mutex
	head   *queueNode[T]
	tail   *queueNode[T]
	count  int
	notify chan struct{}
	done   chan struct{}
	closed bool
}

type queueNode[T any] struct {
	value T
	next  *queueNode[T]
}

// NewQueue returns a ready-to-use Queue.
func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

// Enqueue appends an item. Returns false if the queue is closed.
func (q *Queue[T]) Enqueue(item T) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	n := &queueNode[T]{value: item}
	if q.tail == nil {
		q.head = n
	} else {
		q.tail.next = n
	}
	q.tail = n
	q.count++
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

// Dequeue blocks until an item is available, the queue is closed, or
// ctx cancels. ok is false when no item could be retrieved.
func (q *Queue[T]) Dequeue(ctx context.Context) (T, bool) {
	for {
		q.mu.Lock()
		if item, ok := q.popLocked(); ok {
			q.mu.Unlock()
			return item, true
		}
		closed := q.closed
		q.mu.Unlock()

		if closed {
			var zero T
			return zero, false
		}

		select {
		case <-q.notify:
		case <-q.done:
		case <-ctx.Done():
			var zero T
			return zero, false
		}
	}
}

// TryDequeue removes and returns the head without blocking.
func (q *Queue[T]) TryDequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.popLocked()
}

func (q *Queue[T]) popLocked() (T, bool) {
	var zero T
	if q.head == nil {
		return zero, false
	}
	n := q.head
	q.head = n.next
	if q.head == nil {
		q.tail = nil
	}
	q.count--
	n.next = nil // release for GC — important when T is a pointer
	return n.value, true
}

// Len returns the current item count. Mostly for tests and metrics.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// Close signals waiting Dequeue calls to drain. Items already queued
// remain available.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.done)
}
