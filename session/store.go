// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"maps"
	"sync"
)

// Store persists session state across reconnects so that QoS 1/2
// guarantees can be honored even when the underlying connection drops.
//
// Implementations MUST be safe for concurrent use. The package ships a
// MemoryStore default; disk-backed stores are out of scope for this
// package and should live under their own module (avoid pulling
// filesystem deps into the core).
//
// Lifetime rule: the byte slices passed to PutOutbound and returned
// from ListOutbound are owned by the store. The session copies into
// the store on Put and copies out on List — implementations may keep
// or release as they see fit.
type Store interface {
	// PutOutbound records a serialised PUBLISH packet for replay on
	// reconnect. The packet ID is keyed by id.
	PutOutbound(id uint16, packet []byte) error

	// DeleteOutbound removes the entry once the publish has been
	// acknowledged (PUBACK for QoS 1; PUBCOMP for QoS 2).
	DeleteOutbound(id uint16) error

	// RangeOutbound iterates pending outbound entries in arbitrary
	// order. yield returns false to stop early.
	RangeOutbound(yield func(id uint16, packet []byte) bool) error

	// PutInbound records that we have sent PUBREC for an inbound QoS 2
	// PUBLISH and are awaiting PUBREL. State the broker can rebuild
	// after a reconnect.
	PutInbound(id uint16) error

	// DeleteInbound removes the entry after the matching PUBREL has
	// been processed (PUBCOMP sent).
	DeleteInbound(id uint16) error

	// RangeInbound iterates pending inbound entries.
	RangeInbound(yield func(id uint16) bool) error

	// Reset clears every entry. Called on a CleanStart connection.
	Reset() error
}

// MemoryStore is the default Store: two maps behind a single mutex.
// Adequate for short-lived sessions; persistent storage requires a
// dedicated impl.
type MemoryStore struct {
	mu       sync.RWMutex
	outbound map[uint16][]byte
	inbound  map[uint16]struct{}
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		outbound: make(map[uint16][]byte),
		inbound:  make(map[uint16]struct{}),
	}
}

// PutOutbound copies packet into the store.
func (s *MemoryStore) PutOutbound(id uint16, packet []byte) error {
	dup := make([]byte, len(packet))
	copy(dup, packet)
	s.mu.Lock()
	s.outbound[id] = dup
	s.mu.Unlock()
	return nil
}

// DeleteOutbound is idempotent — deleting a missing ID is not an error.
func (s *MemoryStore) DeleteOutbound(id uint16) error {
	s.mu.Lock()
	delete(s.outbound, id)
	s.mu.Unlock()
	return nil
}

// RangeOutbound iterates a snapshot of the outbound map so callers do
// not contend with concurrent updates.
func (s *MemoryStore) RangeOutbound(yield func(id uint16, packet []byte) bool) error {
	s.mu.RLock()
	snapshot := make(map[uint16][]byte, len(s.outbound))
	maps.Copy(snapshot, s.outbound)
	s.mu.RUnlock()
	for id, pkt := range snapshot {
		if !yield(id, pkt) {
			return nil
		}
	}
	return nil
}

// PutInbound records an inbound QoS 2 packet awaiting PUBREL.
func (s *MemoryStore) PutInbound(id uint16) error {
	s.mu.Lock()
	s.inbound[id] = struct{}{}
	s.mu.Unlock()
	return nil
}

// DeleteInbound removes an inbound entry.
func (s *MemoryStore) DeleteInbound(id uint16) error {
	s.mu.Lock()
	delete(s.inbound, id)
	s.mu.Unlock()
	return nil
}

// RangeInbound iterates inbound IDs.
func (s *MemoryStore) RangeInbound(yield func(id uint16) bool) error {
	s.mu.RLock()
	snapshot := make([]uint16, 0, len(s.inbound))
	for id := range s.inbound {
		snapshot = append(snapshot, id)
	}
	s.mu.RUnlock()
	for _, id := range snapshot {
		if !yield(id) {
			return nil
		}
	}
	return nil
}

// Reset clears both maps.
func (s *MemoryStore) Reset() error {
	s.mu.Lock()
	clear(s.outbound)
	clear(s.inbound)
	s.mu.Unlock()
	return nil
}

// _ enforces interface conformance at compile time.
var _ Store = (*MemoryStore)(nil)
