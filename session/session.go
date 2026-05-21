// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
)

// Config bundles tunable knobs.
type Config struct {
	// ReceiveMaximum bounds the number of concurrent in-flight QoS 1/2
	// publishes (per MQTT v5 §3.1.2.11.3). Zero means no cap (use the
	// 65535 protocol maximum).
	ReceiveMaximum uint16

	// Store persists session state for replay across reconnects.
	// Defaults to NewMemoryStore when nil.
	Store Store
}

// Session bundles the packet ID allocator, the outbound and inbound
// trackers, and a Store under one façade. The client layer owns one
// Session per connection lifecycle.
//
// Methods are thread-safe; the embedded trackers each carry their own
// mutex.
type Session struct {
	IDs       *IDPool
	Outbound  *OutboundTracker
	Inbound   *InboundTracker
	Store     Store
}

// New constructs a Session. Pass a zero Config for sensible defaults
// (no Receive Maximum cap, in-memory store).
func New(cfg Config) *Session {
	store := cfg.Store
	if store == nil {
		store = NewMemoryStore()
	}
	return &Session{
		IDs:      NewIDPool(cfg.ReceiveMaximum),
		Outbound: NewOutboundTracker(),
		Inbound:  NewInboundTracker(store),
		Store:    store,
	}
}

// Reset clears every tracked entry and reseeds the ID pool. Called
// before a new connection with CleanStart=true.
//
// Pending OutboundEntry.Done channels are closed with no error — the
// caller's publish future completes, but the publish itself is lost.
// Pre-condition: caller ensures no in-flight handlers will reference
// the cleared state.
func (s *Session) Reset() error {
	s.Outbound.Range(func(e *OutboundEntry) bool {
		close(e.Done)
		return true
	})
	*s.Outbound = *NewOutboundTracker()
	*s.Inbound = *NewInboundTracker(s.Store)
	return s.Store.Reset()
}

// PendingOutbound returns a snapshot of in-flight outbound packets, in
// arbitrary order. Used after reconnect to replay QoS 1/2 messages
// that were sent before the disconnect.
func (s *Session) PendingOutbound(yield func(*OutboundEntry) bool) {
	s.Outbound.Range(yield)
}

// AllocateID is a thin pass-through to IDPool.Allocate for callers
// that prefer the Session-level API.
func (s *Session) AllocateID(ctx context.Context) (uint16, error) {
	return s.IDs.Allocate(ctx)
}

// ReleaseID is a thin pass-through to IDPool.Release.
func (s *Session) ReleaseID(id uint16) {
	s.IDs.Release(id)
}
