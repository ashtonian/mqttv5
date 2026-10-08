// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

// opLog is a store that records its writes alongside settle calls.
type opLog struct {
	*session.MemoryStore
	mu  sync.Mutex
	ops []string
}

func (s *opLog) add(op string) {
	s.mu.Lock()
	s.ops = append(s.ops, op)
	s.mu.Unlock()
}

func (s *opLog) log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ops)
}

func (s *opLog) Put(ctx context.Context, r session.Record) error {
	s.add(fmt.Sprintf("put %d ref=%s", r.Key.PacketID, r.Ref))
	return s.MemoryStore.Put(ctx, r)
}

func (s *opLog) Delete(ctx context.Context, k session.RecordKey) error {
	s.add(fmt.Sprintf("delete %d", k.PacketID))
	return s.MemoryStore.Delete(ctx, k)
}

// register publishes with a Ref and a Settle that logs into st.
func (h *harness) register(st *opLog, qos byte, ref string) (*Out, chan error) {
	h.t.Helper()
	id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: qos, PacketID: id})
	settled := make(chan error, 1)
	o, _, err := h.e.Register(context.Background(), Message{
		ID: id, QoS: qos, Packet: pkt, Ref: []byte(ref),
		Settle: func(_ context.Context, err error) error {
			if st != nil {
				st.add("settle " + ref)
			}
			settled <- err
			return nil
		},
	})
	if err != nil {
		h.t.Fatal(err)
	}
	return o, settled
}

func recvSettle(t *testing.T, ch chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-t.Context().Done():
		t.Fatal("not settled")
		return nil
	}
}

// Settle runs once with the outcome, before the record is deleted, so a
// producer that records the outcome first cannot lose it to a crash.
func TestSettleRunsBeforeTheRecordIsDeleted(t *testing.T) {
	st := &opLog{MemoryStore: session.NewMemoryStore()}
	h := newHarness(t, Config{Store: st})
	h.connect(false, 0)
	ok, okSettled := h.register(st, 1, "a")
	refused, refusedSettled := h.register(st, 2, "b")
	h.collect()
	h.e.HandlePuback(ok.PacketID(), nil)
	no := errors.New("refused")
	h.e.HandlePubrec(refused.PacketID(), no)
	if err := recvSettle(t, okSettled); err != nil {
		t.Fatalf("accepted message settled with %v", err)
	}
	if err := recvSettle(t, refusedSettled); !errors.Is(err, no) {
		t.Fatalf("refused message settled with %v", err)
	}
	if err := h.e.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	ops := st.log()
	for _, p := range []struct {
		ref string
		id  uint16
	}{{"a", ok.PacketID()}, {"b", refused.PacketID()}} {
		put := slices.Index(ops, fmt.Sprintf("put %d ref=%s", p.id, p.ref))
		settle := slices.Index(ops, "settle "+p.ref)
		del := slices.Index(ops, fmt.Sprintf("delete %d", p.id))
		if put < 0 || settle < put || del < settle {
			t.Fatalf("%s: want put, settle, delete in that order; got %q", p.ref, ops)
		}
	}
}

// With an in-memory store Settle still leaves the read goroutine alone.
func TestSettleNeverBlocksTheAck(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	release := make(chan struct{})
	id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 1, PacketID: id})
	done := make(chan struct{})
	o, _, _ := h.e.Register(context.Background(), Message{ID: id, QoS: 1, Packet: pkt, Ref: []byte("r"),
		Settle: func(context.Context, error) error { <-release; close(done); return nil }})
	h.collect()
	h.e.HandlePuback(o.PacketID(), nil) // returns although Settle is blocked
	close(release)
	<-done
}

func TestSettleOnSessionLoss(t *testing.T) {
	t.Run("fail policy", func(t *testing.T) {
		h := newHarness(t, Config{SessionLoss: Fail})
		h.connect(false, 0)
		_, settled := h.register(nil, 1, "x")
		h.collect()
		h.disconnect()
		h.connect(false, 0)
		if err := recvSettle(t, settled); !errors.Is(err, ErrSessionLost) {
			t.Fatalf("settled with %v", err)
		}
	})
	t.Run("discard", func(t *testing.T) {
		h := newHarness(t, Config{Store: session.NewMemoryStore()})
		_, settled := h.register(nil, 2, "y")
		if err := h.e.Discard(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := recvSettle(t, settled); !errors.Is(err, ErrSessionLost) {
			t.Fatalf("settled with %v", err)
		}
	})
}

// A restarted producer finds its unfinished flows by Ref, whether they
// are still running or finished before it asked.
func TestAdoptRestoredFlows(t *testing.T) {
	st := session.NewMemoryStore()
	h := newHarness(t, Config{Store: st})
	h.connect(false, 0)
	running, _ := h.register(nil, 1, "running")
	early, _ := h.register(nil, 1, "early")
	h.register(nil, 1, "")
	h.collect()

	// A new process with the same store; nobody has adopted yet.
	h2 := newHarness(t, Config{Store: st})
	if err := h2.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h2.connect(true, 0)
	h2.collect()
	h2.e.HandlePuback(early.PacketID(), nil)

	if h2.e.Adopt([]byte("unknown"), func(context.Context, error) error { t.Error("settled an unknown ref"); return nil }) {
		t.Fatal("adopted an unknown ref")
	}
	finished := make(chan error, 1)
	if !h2.e.Adopt([]byte("early"), func(_ context.Context, err error) error { finished <- err; return nil }) {
		t.Fatal("a flow that finished before adoption was not found")
	}
	if err := recvSettle(t, finished); err != nil {
		t.Fatalf("finished flow settled with %v", err)
	}
	if h2.e.Adopt([]byte("early"), func(context.Context, error) error { return nil }) {
		t.Fatal("a finished flow was adopted twice")
	}
	live := make(chan error, 1)
	if !h2.e.Adopt([]byte("running"), func(_ context.Context, err error) error { live <- err; return nil }) {
		t.Fatal("restored flow not found by its ref")
	}
	h2.e.HandlePuback(running.PacketID(), nil)
	if err := recvSettle(t, live); err != nil {
		t.Fatalf("adopted flow settled with %v", err)
	}
}
