// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"sync"

	"github.com/ashtonian/mqttv5/wire"
)

// inboundState is the per-PUBLISH state machine on the receive side.
type inboundState uint8

const (
	// inPendingUserAck: PUBLISH received and routed to the user; the
	// session is waiting for Ack() to be called before emitting the
	// matching PUBACK/PUBREC.
	inPendingUserAck inboundState = iota
	// inUserAcked: the user has acked, but the FIFO position has not
	// yet reached the head — the actual broker ack is held.
	inUserAcked
	// inAwaitingPubRel (QoS 2 only): the broker-facing ack (PUBREC)
	// has been emitted and we are waiting for the matching PUBREL.
	inAwaitingPubRel
)

// AckSend instructs the client to emit one acknowledgement packet to
// the broker. Type is one of wire.PUBACK (QoS 1) or wire.PUBREC
// (QoS 2 first ack).
type AckSend struct {
	PacketID uint16
	Type     wire.PacketType
}

// inboundEntry tracks the state of one received PUBLISH.
type inboundEntry struct {
	id    uint16
	qos   byte
	state inboundState
}

// InboundTracker enforces MQTT v5 §4.6 acknowledgment ordering for
// QoS 1/2 messages with manual acks: acks are emitted to the broker in
// the order their PUBLISHes were received, even when the user calls
// Ack out of order.
//
// Internally, a FIFO of inboundEntry pointers plus an id→entry map.
// Out-of-order user acks mark the entry as inUserAcked; a head-walk
// then drains every consecutive acked entry and returns them to the
// caller for serialisation.
type InboundTracker struct {
	mu    sync.Mutex
	fifo  []*inboundEntry
	byID  map[uint16]*inboundEntry
	store Store // PUBREL-pending IDs persisted here; nil disables
}

// NewInboundTracker returns a tracker. If store is non-nil, inbound
// QoS 2 entries are persisted when their PUBREC is emitted so they
// survive reconnect.
func NewInboundTracker(store Store) *InboundTracker {
	return &InboundTracker{
		fifo:  make([]*inboundEntry, 0, 16),
		byID:  make(map[uint16]*inboundEntry),
		store: store,
	}
}

// RegisterPublish records an incoming PUBLISH at the FIFO tail.
//
// Returns true if the packet ID is fresh (caller routes to user) and
// false if it is a duplicate retransmit (caller drops it; the broker
// will get the held ack when ordering allows).
//
// For QoS 0 this returns true unconditionally and records nothing —
// QoS 0 has no ack and needs no ordering.
func (t *InboundTracker) RegisterPublish(id uint16, qos byte) bool {
	if qos == 0 {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, dup := t.byID[id]; dup {
		return false
	}
	e := &inboundEntry{id: id, qos: qos, state: inPendingUserAck}
	t.byID[id] = e
	t.fifo = append(t.fifo, e)
	return true
}

// AckPublish marks the entry as user-acked and returns the list of
// AckSend instructions the caller should emit, in FIFO order.
//
// QoS 2 first-ack semantics: when the user acks a QoS 2 PUBLISH, the
// emitted AckSend has Type == wire.PUBREC. The entry transitions to
// inAwaitingPubRel and stays in byID until the PUBREL arrives.
func (t *InboundTracker) AckPublish(id uint16) []AckSend {
	t.mu.Lock()
	defer t.mu.Unlock()

	e, ok := t.byID[id]
	if !ok {
		return nil
	}
	if e.state != inPendingUserAck {
		return nil
	}
	e.state = inUserAcked

	return t.flushLocked()
}

// flushLocked walks the FIFO head, emitting AckSends for each
// consecutive inUserAcked entry until it hits one still pending.
//
// For QoS 1 the entry is removed from byID entirely. For QoS 2 the
// entry persists (state = inAwaitingPubRel) until HandlePubRel is
// called; only the FIFO position is freed.
func (t *InboundTracker) flushLocked() []AckSend {
	var out []AckSend
	for len(t.fifo) > 0 {
		head := t.fifo[0]
		if head.state != inUserAcked {
			break
		}
		t.fifo[0] = nil
		t.fifo = t.fifo[1:]

		switch head.qos {
		case 1:
			out = append(out, AckSend{PacketID: head.id, Type: wire.PUBACK})
			delete(t.byID, head.id)
		case 2:
			out = append(out, AckSend{PacketID: head.id, Type: wire.PUBREC})
			head.state = inAwaitingPubRel
			if t.store != nil {
				_ = t.store.PutInbound(head.id)
			}
		}
	}
	// Compact the slice when more than half is wasted prefix slots so
	// the FIFO doesn't grow without bound under steady churn.
	if cap(t.fifo) > 64 && len(t.fifo)*2 < cap(t.fifo) {
		fresh := make([]*inboundEntry, len(t.fifo), cap(t.fifo)/2)
		copy(fresh, t.fifo)
		t.fifo = fresh
	}
	return out
}

// HandlePubRel (QoS 2 second half) records that the broker has
// confirmed our PUBREC. The session removes its inbound entry and
// instructs the caller to emit PUBCOMP. Returns true if PUBCOMP should
// be sent; false if the packet ID is unknown (broker double-sent).
func (t *InboundTracker) HandlePubRel(id uint16) bool {
	t.mu.Lock()
	e, ok := t.byID[id]
	if !ok {
		t.mu.Unlock()
		return false
	}
	delete(t.byID, id)
	t.mu.Unlock()

	if t.store != nil {
		_ = t.store.DeleteInbound(id)
	}
	// Sanity: should be in awaiting-pubrel state by here. We don't
	// fail otherwise; an unexpected state means broker behaviour
	// outside the spec, and a PUBCOMP is still the right reply.
	_ = e
	return true
}

// Len reports total entries (both pending user-ack and pending PUBREL).
// Mostly for tests.
func (t *InboundTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.byID)
}
