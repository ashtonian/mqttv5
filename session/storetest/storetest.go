// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package storetest checks that a [session.Store] implementation honours
// the contract the client relies on. Call [Run] from the
// implementation's tests:
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, storetest.Factory{
//			Open: func(t *testing.T) session.Store { return mystore.Open(t.TempDir()) },
//		})
//	}
package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/session"
)

// Factory opens stores under test.
type Factory struct {
	// Open returns a new, empty store. Run closes it on cleanup.
	Open func(t *testing.T) session.Store

	// Reopen, when set, closes s and opens the same backing storage
	// again, so Run can check that state survives a restart. Leave it
	// nil for stores that live only in memory.
	Reopen func(t *testing.T, s session.Store) session.Store
}

// Run executes the conformance suite as subtests of t.
func Run(t *testing.T, f Factory) {
	open := func(t *testing.T) session.Store {
		t.Helper()
		s := f.Open(t)
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	t.Run("EmptyLoad", func(t *testing.T) { testEmptyLoad(t, open(t)) })
	t.Run("RoundTrip", func(t *testing.T) { testRoundTrip(t, open(t)) })
	t.Run("PutReplaces", func(t *testing.T) { testPutReplaces(t, open(t)) })
	t.Run("OutboundOrderedBySeq", func(t *testing.T) { testOrder(t, open(t)) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, open(t)) })
	t.Run("KeysAreDirectional", func(t *testing.T) { testDirectional(t, open(t)) })
	t.Run("CopiesPackets", func(t *testing.T) { testCopies(t, open(t)) })
	t.Run("Meta", func(t *testing.T) { testMeta(t, open(t)) })
	t.Run("Reset", func(t *testing.T) { testReset(t, open(t)) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrent(t, open(t)) })
	t.Run("ClosedStoreFails", func(t *testing.T) { testClosed(t, f.Open(t)) })
	if f.Reopen != nil {
		t.Run("SurvivesReopen", func(t *testing.T) {
			s := f.Open(t)
			testReopen(t, s, func(s session.Store) session.Store {
				r := f.Reopen(t, s)
				t.Cleanup(func() { _ = r.Close() })
				return r
			})
		})
	}
}

var ctx = context.Background()

func outRec(id uint16, seq uint64, phase session.Phase, payload string) session.Record {
	qos := byte(1)
	if phase != session.AwaitPuback {
		qos = 2
	}
	return session.Record{
		Key:    session.RecordKey{Dir: session.Outbound, PacketID: id},
		Seq:    seq,
		QoS:    qos,
		Phase:  phase,
		Packet: []byte("packet-" + payload),
	}
}

func inRec(id uint16) session.Record {
	return session.Record{Key: session.RecordKey{Dir: session.Inbound, PacketID: id}, QoS: 2, Phase: session.AwaitPubrel}
}

func load(t *testing.T, s session.Store) (session.Meta, []session.Record) {
	t.Helper()
	m, recs, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return m, recs
}

func put(t *testing.T, s session.Store, r session.Record) {
	t.Helper()
	if err := s.Put(ctx, r); err != nil {
		t.Fatalf("Put(%+v): %v", r.Key, err)
	}
}

func equal(a, b session.Record) bool {
	return a.Key == b.Key && a.Seq == b.Seq && a.PubrecSeq == b.PubrecSeq && a.QoS == b.QoS && a.Phase == b.Phase &&
		bytes.Equal(a.Packet, b.Packet) && a.ExpiresAt.Equal(b.ExpiresAt) &&
		bytes.Equal(a.Ref, b.Ref) && (a.Ref == nil) == (b.Ref == nil)
}

func find(recs []session.Record, k session.RecordKey) (session.Record, bool) {
	for _, r := range recs {
		if r.Key == k {
			return r, true
		}
	}
	return session.Record{}, false
}

func testEmptyLoad(t *testing.T, s session.Store) {
	m, recs := load(t, s)
	if m != (session.Meta{}) || len(recs) != 0 {
		t.Fatalf("empty store Load = %+v, %d records", m, len(recs))
	}
}

func testRoundTrip(t *testing.T, s session.Store) {
	exp := time.Unix(1_800_000_000, 123_456_789)
	want := []session.Record{
		outRec(1, 10, session.AwaitPuback, "a"),
		func() session.Record { r := outRec(2, 11, session.AwaitPubrec, "b"); r.ExpiresAt = exp; return r }(),
		func() session.Record {
			r := outRec(3, 12, session.AwaitPubcomp, "c")
			r.PubrecSeq = 15
			r.Ref = []byte("entry-3")
			return r
		}(),
		func() session.Record { r := outRec(5, 13, session.AwaitPuback, "d"); r.Ref = []byte{}; return r }(),
		inRec(4),
	}
	for _, r := range want {
		put(t, s, r)
	}
	_, got := load(t, s)
	if len(got) != len(want) {
		t.Fatalf("Load returned %d records, want %d", len(got), len(want))
	}
	for _, w := range want {
		g, ok := find(got, w.Key)
		if !ok || !equal(g, w) {
			t.Errorf("record %+v: got %+v (found %v), want %+v", w.Key, g, ok, w)
		}
	}
	if r, _ := find(got, session.RecordKey{Dir: session.Outbound, PacketID: 1}); !r.ExpiresAt.IsZero() || r.Ref != nil {
		t.Errorf("unset ExpiresAt and Ref came back as %v, %q", r.ExpiresAt, r.Ref)
	}
	bad := inRec(7)
	bad.Phase = session.AwaitPuback
	if err := s.Put(ctx, bad); !errors.Is(err, session.ErrInvalidRecord) {
		t.Errorf("Put of an inbound record at an outbound phase: %v, want ErrInvalidRecord", err)
	}
	long := outRec(6, 14, session.AwaitPuback, "e")
	long.Ref = make([]byte, session.MaxRefLen+1)
	if err := s.Put(ctx, long); !errors.Is(err, session.ErrInvalidRecord) {
		t.Errorf("Put with a %d-byte Ref: %v, want ErrInvalidRecord", len(long.Ref), err)
	}
}

func testPutReplaces(t *testing.T, s session.Store) {
	put(t, s, outRec(7, 1, session.AwaitPubrec, "x"))
	put(t, s, outRec(7, 5, session.AwaitPubcomp, "x"))
	_, got := load(t, s)
	if len(got) != 1 || got[0].Phase != session.AwaitPubcomp || got[0].Seq != 5 {
		t.Fatalf("after replacing Put: %+v", got)
	}
}

func testOrder(t *testing.T, s session.Store) {
	seqs := []uint64{40, 3, 1000, 17, 2}
	for i, seq := range seqs {
		put(t, s, outRec(uint16(100-i), seq, session.AwaitPuback, fmt.Sprint(seq)))
	}
	put(t, s, inRec(1))
	_, got := load(t, s)
	var outs []uint64
	for _, r := range got {
		if r.Key.Dir == session.Outbound {
			outs = append(outs, r.Seq)
		}
	}
	for i := 1; i < len(outs); i++ {
		if outs[i-1] >= outs[i] {
			t.Fatalf("outbound records not ordered by Seq: %v", outs)
		}
	}
	if len(outs) != len(seqs) {
		t.Fatalf("got %d outbound records, want %d", len(outs), len(seqs))
	}
}

func testDelete(t *testing.T, s session.Store) {
	put(t, s, outRec(1, 1, session.AwaitPuback, "a"))
	put(t, s, outRec(2, 2, session.AwaitPuback, "b"))
	if err := s.Delete(ctx, session.RecordKey{Dir: session.Outbound, PacketID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, session.RecordKey{Dir: session.Outbound, PacketID: 999}); err != nil {
		t.Fatalf("deleting a missing record: %v", err)
	}
	_, got := load(t, s)
	if len(got) != 1 || got[0].Key.PacketID != 2 {
		t.Fatalf("after Delete: %+v", got)
	}
}

func testDirectional(t *testing.T, s session.Store) {
	put(t, s, outRec(5, 1, session.AwaitPubrec, "o"))
	put(t, s, inRec(5))
	if err := s.Delete(ctx, session.RecordKey{Dir: session.Inbound, PacketID: 5}); err != nil {
		t.Fatal(err)
	}
	_, got := load(t, s)
	if len(got) != 1 || got[0].Key.Dir != session.Outbound {
		t.Fatalf("inbound and outbound records with the same packet ID interfere: %+v", got)
	}
}

func testCopies(t *testing.T, s session.Store) {
	r := outRec(1, 1, session.AwaitPuback, "orig")
	put(t, s, r)
	copy(r.Packet, "XXXXXXXXXXX")
	_, got := load(t, s)
	if string(got[0].Packet) != "packet-orig" {
		t.Fatalf("Put kept the caller's slice: %q", got[0].Packet)
	}
	copy(got[0].Packet, "YYYYYYYYYYY")
	_, again := load(t, s)
	if string(again[0].Packet) != "packet-orig" {
		t.Fatalf("Load returned the store's own slice: %q", again[0].Packet)
	}
}

func testMeta(t *testing.T, s session.Store) {
	want := session.Meta{ClientID: "assigned-1", SessionExpiry: 3600}
	if err := s.SetMeta(ctx, want); err != nil {
		t.Fatal(err)
	}
	if m, _ := load(t, s); m != want {
		t.Fatalf("meta = %+v, want %+v", m, want)
	}
	if err := s.SetMeta(ctx, session.Meta{}); err != nil {
		t.Fatal(err)
	}
	if m, _ := load(t, s); m != (session.Meta{}) {
		t.Fatalf("meta after reset to zero = %+v", m)
	}
}

func testReset(t *testing.T, s session.Store) {
	put(t, s, outRec(1, 1, session.AwaitPuback, "a"))
	put(t, s, inRec(2))
	if err := s.SetMeta(ctx, session.Meta{ClientID: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	m, got := load(t, s)
	if m != (session.Meta{}) || len(got) != 0 {
		t.Fatalf("after Reset: meta %+v, %d records", m, len(got))
	}
	put(t, s, outRec(3, 1, session.AwaitPuback, "c"))
	if _, got := load(t, s); len(got) != 1 {
		t.Fatalf("store unusable after Reset: %d records", len(got))
	}
}

func testConcurrent(t *testing.T, s session.Store) {
	const workers, per = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for i := 0; i < per; i++ {
				id := uint16(w*per + i + 1)
				if err := s.Put(ctx, outRec(id, uint64(id), session.AwaitPuback, "c")); err != nil {
					t.Error(err)
					return
				}
				if i%2 == 0 {
					if err := s.Delete(ctx, session.RecordKey{Dir: session.Outbound, PacketID: id}); err != nil {
						t.Error(err)
						return
					}
				}
			}
		})
	}
	wg.Wait()
	if _, got := load(t, s); len(got) != workers*per/2 {
		t.Fatalf("after concurrent writes: %d records, want %d", len(got), workers*per/2)
	}
}

func testClosed(t *testing.T, s session.Store) {
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, outRec(1, 1, session.AwaitPuback, "a")); err == nil {
		t.Error("Put after Close succeeded")
	}
	if _, _, err := s.Load(ctx); err == nil {
		t.Error("Load after Close succeeded")
	}
}

func testReopen(t *testing.T, s session.Store, reopen func(session.Store) session.Store) {
	exp := time.Unix(1_900_000_000, 42)
	recs := []session.Record{
		outRec(9, 2, session.AwaitPubcomp, "z"),
		func() session.Record {
			r := outRec(8, 1, session.AwaitPuback, "y")
			r.ExpiresAt, r.Ref = exp, []byte("ref-8")
			return r
		}(),
		inRec(3),
	}
	for _, r := range recs {
		put(t, s, r)
	}
	meta := session.Meta{ClientID: "keep-me", SessionExpiry: 60}
	if err := s.SetMeta(ctx, meta); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, session.RecordKey{Dir: session.Outbound, PacketID: 9}); err != nil {
		t.Fatal(err)
	}
	r := reopen(s)
	m, got := load(t, r)
	if m != meta {
		t.Errorf("meta after reopen = %+v, want %+v", m, meta)
	}
	if len(got) != 2 {
		t.Fatalf("records after reopen: %+v", got)
	}
	for _, w := range recs[1:] {
		if g, ok := find(got, w.Key); !ok || !equal(g, w) {
			t.Errorf("record %+v after reopen: %+v (found %v)", w.Key, g, ok)
		}
	}
}
