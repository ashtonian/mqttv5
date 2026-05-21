// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"

	"github.com/ashtonian/mqttv5/wire"
)

func TestInboundQoS0NoOp(t *testing.T) {
	tr := NewInboundTracker(NewMemoryStore())
	if !tr.RegisterPublish(1, 0) {
		t.Fatal("QoS 0 should always be reported fresh")
	}
	if tr.Len() != 0 {
		t.Fatalf("Len = %d, want 0 (QoS 0 not tracked)", tr.Len())
	}
	if got := tr.AckPublish(1); got != nil {
		t.Fatalf("AckPublish(QoS0) = %v, want nil", got)
	}
}

func TestInboundQoS1OrderedFlush(t *testing.T) {
	tr := NewInboundTracker(NewMemoryStore())
	for _, id := range []uint16{1, 2, 3} {
		if !tr.RegisterPublish(id, 1) {
			t.Fatalf("RegisterPublish(%d) reported duplicate", id)
		}
	}

	// Ack out of order: 2 first. Nothing should flush — 1 is still pending.
	if got := tr.AckPublish(2); len(got) != 0 {
		t.Fatalf("Ack(2) before Ack(1): got %v, want empty", got)
	}

	// Ack 3 — still nothing should flush.
	if got := tr.AckPublish(3); len(got) != 0 {
		t.Fatalf("Ack(3) before Ack(1): got %v, want empty", got)
	}

	// Ack 1 — all three should flush in receive order.
	got := tr.AckPublish(1)
	if len(got) != 3 {
		t.Fatalf("Ack(1) flushed %d, want 3", len(got))
	}
	for i, want := range []uint16{1, 2, 3} {
		if got[i].PacketID != want {
			t.Errorf("flush[%d]: id=%d, want %d", i, got[i].PacketID, want)
		}
		if got[i].Type != wire.PUBACK {
			t.Errorf("flush[%d]: type=%s, want PUBACK", i, got[i].Type)
		}
	}
	if tr.Len() != 0 {
		t.Fatalf("Len after full flush = %d, want 0", tr.Len())
	}
}

func TestInboundQoS2FullHandshake(t *testing.T) {
	store := NewMemoryStore()
	tr := NewInboundTracker(store)

	if !tr.RegisterPublish(7, 2) {
		t.Fatal("first register should be fresh")
	}

	got := tr.AckPublish(7)
	if len(got) != 1 || got[0].Type != wire.PUBREC || got[0].PacketID != 7 {
		t.Fatalf("Ack(QoS2) = %v, want one PUBREC for id 7", got)
	}

	// PUBREC has been emitted; store should now have the ID pending.
	count := 0
	_ = store.RangeInbound(func(_ uint16) bool { count++; return true })
	if count != 1 {
		t.Fatalf("store.RangeInbound = %d, want 1 entry after PUBREC", count)
	}
	if tr.Len() != 1 {
		t.Fatalf("Len after Ack = %d, want 1 (awaiting PUBREL)", tr.Len())
	}

	// Acking again does nothing.
	if got := tr.AckPublish(7); got != nil {
		t.Fatalf("double Ack = %v, want nil", got)
	}

	// PUBREL arrives. Should signal PUBCOMP and remove the entry.
	if !tr.HandlePubRel(7) {
		t.Fatal("HandlePubRel returned false for known ID")
	}
	if tr.Len() != 0 {
		t.Fatalf("Len after PUBREL = %d, want 0", tr.Len())
	}
	count = 0
	_ = store.RangeInbound(func(_ uint16) bool { count++; return true })
	if count != 0 {
		t.Fatalf("store.RangeInbound after PUBREL = %d, want 0", count)
	}
}

func TestInboundDuplicatePublish(t *testing.T) {
	tr := NewInboundTracker(NewMemoryStore())
	if !tr.RegisterPublish(1, 1) {
		t.Fatal("first register should be fresh")
	}
	if tr.RegisterPublish(1, 1) {
		t.Fatal("duplicate register should report not fresh")
	}
	if tr.Len() != 1 {
		t.Fatalf("Len = %d, want 1", tr.Len())
	}
}

func TestInboundHandlePubRelUnknown(t *testing.T) {
	tr := NewInboundTracker(NewMemoryStore())
	if tr.HandlePubRel(42) {
		t.Fatal("HandlePubRel(unknown) should return false")
	}
}
