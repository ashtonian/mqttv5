// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package queuetest checks a [mqttv5.PublisherQueue] implementation
// against the interface's contract. Use it from the implementation's
// tests:
//
//	func TestConformance(t *testing.T) {
//		queuetest.Run(t, queuetest.Factory{
//			Open: func(t *testing.T) mqttv5.PublisherQueue { ... },
//		})
//	}
package queuetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
)

// Factory opens queues for the suite.
type Factory struct {
	// Open returns a new, empty queue. The suite closes it.
	Open func(t *testing.T) mqttv5.PublisherQueue
	// Reopen closes q and opens it again from its persisted state, for
	// queues that keep entries across restarts; nil otherwise.
	Reopen func(t *testing.T, q mqttv5.PublisherQueue) mqttv5.PublisherQueue
}

// Run runs the conformance suite.
func Run(t *testing.T, f Factory) {
	open := func(t *testing.T) mqttv5.PublisherQueue {
		q := f.Open(t)
		t.Cleanup(func() { _ = q.Close() })
		return q
	}
	t.Run("OrderAndPeekAfter", func(t *testing.T) { testOrder(t, open(t)) })
	t.Run("Ack", func(t *testing.T) { testAck(t, open(t)) })
	t.Run("SeqNeverReused", func(t *testing.T) { testSeqNeverReused(t, open(t)) })
	t.Run("RoundTrip", func(t *testing.T) { testRoundTrip(t, open(t)) })
	t.Run("Copies", func(t *testing.T) { testCopies(t, open(t)) })
	t.Run("DropNewest", func(t *testing.T) { testDropNewest(t, open(t)) })
	t.Run("DropOldest", func(t *testing.T) { testDropOldest(t, open(t)) })
	t.Run("ConcurrentBound", func(t *testing.T) { testConcurrentBound(t, open) })
	t.Run("Closed", func(t *testing.T) { testClosed(t, f.Open(t)) })
	if f.Reopen != nil {
		t.Run("Reopen", func(t *testing.T) {
			testReopen(t, f.Open(t), func(q mqttv5.PublisherQueue) mqttv5.PublisherQueue {
				r := f.Reopen(t, q)
				t.Cleanup(func() { _ = r.Close() })
				return r
			})
		})
	}
}

var ctx = context.Background()

func entry(payload string) mqttv5.QueueEntry {
	return mqttv5.QueueEntry{
		ID:         "id-" + payload,
		Publish:    mqttv5.PublishOptions{Topic: "q/t", Payload: []byte(payload), QoS: 1},
		EnqueuedAt: time.Unix(1_800_000_000, 0),
	}
}

func enqueue(t *testing.T, q mqttv5.PublisherQueue, e mqttv5.QueueEntry) uint64 {
	t.Helper()
	seq, evicted, err := q.Enqueue(ctx, e, mqttv5.QueueLimit{})
	if err != nil || len(evicted) != 0 {
		t.Fatalf("Enqueue(%s) = %d, %v, %v", e.ID, seq, evicted, err)
	}
	return seq
}

func peek(t *testing.T, q mqttv5.PublisherQueue, after uint64, n int) []mqttv5.QueueEntry {
	t.Helper()
	es, err := q.Peek(ctx, after, n)
	if err != nil {
		t.Fatalf("Peek(%d, %d): %v", after, n, err)
	}
	return es
}

func payloads(es []mqttv5.QueueEntry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = string(e.Publish.Payload)
	}
	return out
}

func length(t *testing.T, q mqttv5.PublisherQueue) int {
	t.Helper()
	n, err := q.Len(ctx)
	if err != nil {
		t.Fatalf("Len: %v", err)
	}
	return n
}

func testOrder(t *testing.T, q mqttv5.PublisherQueue) {
	var seqs []uint64
	for i := range 5 {
		seqs = append(seqs, enqueue(t, q, entry(fmt.Sprint(i))))
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("Seq not increasing: %v", seqs)
		}
	}
	if got := payloads(peek(t, q, 0, 10)); !reflect.DeepEqual(got, []string{"0", "1", "2", "3", "4"}) {
		t.Fatalf("Peek(0, 10) = %q", got)
	}
	got := peek(t, q, seqs[1], 2)
	if !reflect.DeepEqual(payloads(got), []string{"2", "3"}) || got[0].Seq != seqs[2] {
		t.Fatalf("Peek(after second, 2) = %+v", got)
	}
	if len(peek(t, q, seqs[4], 10)) != 0 {
		t.Fatal("Peek after the last entry returned entries")
	}
	if n := length(t, q); n != 5 {
		t.Fatalf("Len = %d, want 5", n)
	}
}

func testAck(t *testing.T, q mqttv5.PublisherQueue) {
	var seqs []uint64
	for i := range 4 {
		seqs = append(seqs, enqueue(t, q, entry(fmt.Sprint(i))))
	}
	for _, s := range []uint64{seqs[2], seqs[0], seqs[2], seqs[3] + 1000} {
		if err := q.Ack(ctx, s); err != nil {
			t.Fatalf("Ack(%d): %v", s, err)
		}
	}
	if got := payloads(peek(t, q, 0, 10)); !reflect.DeepEqual(got, []string{"1", "3"}) {
		t.Fatalf("after acks Peek = %q", got)
	}
	if n := length(t, q); n != 2 {
		t.Fatalf("Len = %d, want 2", n)
	}
}

func testSeqNeverReused(t *testing.T, q mqttv5.PublisherQueue) {
	first := enqueue(t, q, entry("a"))
	if err := q.Ack(ctx, first); err != nil {
		t.Fatal(err)
	}
	if next := enqueue(t, q, entry("b")); next <= first {
		t.Fatalf("Seq %d after acking %d", next, first)
	}
}

// Every field comes back as stored, with nil and zero kept apart.
func testRoundTrip(t *testing.T, q mqttv5.PublisherQueue) {
	zero, pfi := uint32(0), byte(1)
	full := mqttv5.QueueEntry{
		ID: "0123456789abcdef0123456789abcdef",
		Publish: mqttv5.PublishOptions{
			Topic: "r/t", Payload: []byte("payload"), QoS: 2, Retain: true,
			PayloadFormatIndicator: &pfi, MessageExpiryInterval: &zero,
			ContentType: "text/plain", ResponseTopic: "r/reply", CorrelationData: []byte{},
			UserProperties: []mqttv5.UserProperty{{Key: "k", Value: "v"}, {Key: "k", Value: ""}},
		},
		EnqueuedAt: time.Unix(1_800_000_000, 123_456_789),
		ExpiresAt:  time.Unix(1_800_000_060, 5),
	}
	bare := mqttv5.QueueEntry{ID: "bare", Publish: mqttv5.PublishOptions{Topic: "r/b", QoS: 1}, EnqueuedAt: time.Unix(1_800_000_000, 0)}
	for _, want := range []mqttv5.QueueEntry{full, bare} {
		want.Seq = enqueue(t, q, want)
		got := peek(t, q, want.Seq-1, 1)
		if len(got) != 1 {
			t.Fatalf("entry %s not found", want.ID)
		}
		if !sameEntry(got[0], want) {
			t.Errorf("round trip of %s:\n got %+v\nwant %+v", want.ID, got[0], want)
		}
	}
}

func sameEntry(a, b mqttv5.QueueEntry) bool {
	pa, pb := a.Publish, b.Publish
	if len(pa.Payload) == 0 && len(pb.Payload) == 0 {
		pa.Payload, pb.Payload = nil, nil
	}
	return a.Seq == b.Seq && a.ID == b.ID && a.EnqueuedAt.Equal(b.EnqueuedAt) && a.ExpiresAt.Equal(b.ExpiresAt) &&
		a.ExpiresAt.IsZero() == b.ExpiresAt.IsZero() && reflect.DeepEqual(pa, pb)
}

func testCopies(t *testing.T, q mqttv5.PublisherQueue) {
	mei := uint32(9)
	e := entry("orig")
	e.Publish.MessageExpiryInterval = &mei
	e.Publish.CorrelationData = []byte("corr")
	e.Publish.UserProperties = []mqttv5.UserProperty{{Key: "k", Value: "v"}}
	seq := enqueue(t, q, e)
	e.Publish.Payload[0], e.Publish.CorrelationData[0], e.Publish.UserProperties[0].Value, mei = 'X', 'X', "X", 1
	got := peek(t, q, seq-1, 1)[0]
	if string(got.Publish.Payload) != "orig" || string(got.Publish.CorrelationData) != "corr" ||
		got.Publish.UserProperties[0].Value != "v" || *got.Publish.MessageExpiryInterval != 9 {
		t.Fatalf("Enqueue kept the caller's memory: %+v", got.Publish)
	}
	got.Publish.Payload[0], got.Publish.UserProperties[0].Value = 'Y', "Y"
	again := peek(t, q, seq-1, 1)[0]
	if !bytes.Equal(again.Publish.Payload, []byte("orig")) || again.Publish.UserProperties[0].Value != "v" {
		t.Fatalf("Peek returned the queue's own memory: %+v", again.Publish)
	}
}

func testDropNewest(t *testing.T, q mqttv5.PublisherQueue) {
	limit := mqttv5.QueueLimit{Max: 3, Policy: mqttv5.DropNewest}
	for i := range 3 {
		if _, _, err := q.Enqueue(ctx, entry(fmt.Sprint(i)), limit); err != nil {
			t.Fatal(err)
		}
	}
	if _, ev, err := q.Enqueue(ctx, entry("3"), limit); !errors.Is(err, mqttv5.ErrQueueFull) || len(ev) != 0 {
		t.Fatalf("Enqueue on a full queue = %v, %v", ev, err)
	}
	if got := payloads(peek(t, q, 0, 10)); !reflect.DeepEqual(got, []string{"0", "1", "2"}) {
		t.Fatalf("queue after refusal: %q", got)
	}
}

func testDropOldest(t *testing.T, q mqttv5.PublisherQueue) {
	var seqs []uint64
	for i := range 4 {
		seqs = append(seqs, enqueue(t, q, entry(fmt.Sprint(i))))
	}
	// Room for one more at Max 4: the oldest goes.
	_, ev, err := q.Enqueue(ctx, entry("4"), mqttv5.QueueLimit{Max: 4, Policy: mqttv5.DropOldest})
	if err != nil || !reflect.DeepEqual(payloads(ev), []string{"0"}) || ev[0].Seq != seqs[0] {
		t.Fatalf("DropOldest evicted %+v, %v", ev, err)
	}
	// Entries up to Keep are being published and must stay.
	_, ev, err = q.Enqueue(ctx, entry("5"), mqttv5.QueueLimit{Max: 3, Policy: mqttv5.DropOldest, Keep: seqs[2]})
	if err != nil || !reflect.DeepEqual(payloads(ev), []string{"3", "4"}) {
		t.Fatalf("DropOldest with Keep evicted %q, %v", payloads(ev), err)
	}
	if got := payloads(peek(t, q, 0, 10)); !reflect.DeepEqual(got, []string{"1", "2", "5"}) {
		t.Fatalf("queue after evictions: %q", got)
	}
	// Nothing above Keep to evict: refused, and nothing removed.
	last := peek(t, q, 0, 10)[2].Seq
	if _, ev, err := q.Enqueue(ctx, entry("6"), mqttv5.QueueLimit{Max: 3, Policy: mqttv5.DropOldest, Keep: last}); !errors.Is(err, mqttv5.ErrQueueFull) || len(ev) != 0 {
		t.Fatalf("Enqueue with everything kept = %v, %v", ev, err)
	}
	if n := length(t, q); n != 3 {
		t.Fatalf("Len = %d, want 3", n)
	}
}

// The bound holds however many producers race for the last place.
func testConcurrentBound(t *testing.T, open func(*testing.T) mqttv5.PublisherQueue) {
	for _, policy := range []mqttv5.DropPolicy{mqttv5.DropNewest, mqttv5.DropOldest} {
		t.Run(fmt.Sprint("policy=", policy), func(t *testing.T) {
			q := open(t)
			const max = 50
			var wg sync.WaitGroup
			var mu sync.Mutex
			stored, evicted := 0, 0
			for g := range 8 {
				wg.Go(func() {
					for i := range 25 {
						_, ev, err := q.Enqueue(ctx, entry(fmt.Sprint(g, "-", i)), mqttv5.QueueLimit{Max: max, Policy: policy})
						mu.Lock()
						if err == nil {
							stored++
						} else if !errors.Is(err, mqttv5.ErrQueueFull) {
							t.Error(err)
						}
						evicted += len(ev)
						mu.Unlock()
						if n, _ := q.Len(ctx); n > max {
							t.Errorf("Len %d above the bound %d", n, max)
						}
					}
				})
			}
			wg.Wait()
			if n := length(t, q); n != max || n != stored-evicted {
				t.Fatalf("Len %d, stored %d, evicted %d, bound %d", n, stored, evicted, max)
			}
		})
	}
}

func testClosed(t *testing.T, q mqttv5.PublisherQueue) {
	seq := enqueue(t, q, entry("a"))
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.Enqueue(ctx, entry("b"), mqttv5.QueueLimit{}); !errors.Is(err, mqttv5.ErrQueueClosed) {
		t.Errorf("Enqueue after Close: %v", err)
	}
	if _, err := q.Peek(ctx, 0, 1); !errors.Is(err, mqttv5.ErrQueueClosed) {
		t.Errorf("Peek after Close: %v", err)
	}
	if err := q.Ack(ctx, seq); !errors.Is(err, mqttv5.ErrQueueClosed) {
		t.Errorf("Ack after Close: %v", err)
	}
	if _, err := q.Len(ctx); !errors.Is(err, mqttv5.ErrQueueClosed) {
		t.Errorf("Len after Close: %v", err)
	}
}

func testReopen(t *testing.T, q mqttv5.PublisherQueue, reopen func(mqttv5.PublisherQueue) mqttv5.PublisherQueue) {
	var seqs []uint64
	for i := range 3 {
		seqs = append(seqs, enqueue(t, q, entry(fmt.Sprint(i))))
	}
	if err := q.Ack(ctx, seqs[0]); err != nil {
		t.Fatal(err)
	}
	want := peek(t, q, 0, 10)
	r := reopen(q)
	got := peek(t, r, 0, 10)
	if len(got) != len(want) {
		t.Fatalf("after reopen %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if !sameEntry(got[i], want[i]) {
			t.Fatalf("entry %d after reopen:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
	if n := length(t, r); n != 2 {
		t.Fatalf("Len after reopen = %d, want 2", n)
	}
	if next := enqueue(t, r, entry("3")); next <= seqs[2] {
		t.Fatalf("Seq %d after reopen reuses one up to %d", next, seqs[2])
	}
}
