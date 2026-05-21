// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package session manages MQTT v5 session state: packet ID allocation,
// in-flight QoS 1/2 tracking (both outbound and inbound), and a Store
// interface so the state can survive a reconnect.
//
// The session has no opinion on the wire — it consumes wire.PacketType
// and wire.ReasonCode but does not parse or emit packets. The client
// layer marries the session to the wire codec and a transport.
package session

import (
	"context"
	"errors"
)

// ErrIDPoolExhausted is returned by IDPool.Allocate when the pool is
// drained AND the caller's context has expired.
var ErrIDPoolExhausted = errors.New("mqttv5/session: packet ID pool exhausted")

// IDPool is a channel-backed packet identifier allocator. Allocate
// blocks (bounded by ctx) when the pool is drained; Release returns an
// ID to the pool.
//
// Packet IDs are 16-bit and 0 is reserved by the spec, so the maximum
// is 65535. Most clients should size the pool to Receive Maximum (a
// CONNECT property capping concurrent in-flight messages, default
// 65535).
type IDPool struct {
	free chan uint16
}

// NewIDPool returns a pool pre-filled with IDs 1..size. size == 0 is
// treated as the protocol maximum (65535).
func NewIDPool(size uint16) *IDPool {
	if size == 0 {
		size = 65535
	}
	p := &IDPool{free: make(chan uint16, size)}
	for i := uint16(1); i != 0 && i <= size; i++ {
		p.free <- i
	}
	return p
}

// Allocate hands out the next free ID. Blocks until one is available or
// ctx is cancelled.
func (p *IDPool) Allocate(ctx context.Context) (uint16, error) {
	select {
	case id := <-p.free:
		return id, nil
	case <-ctx.Done():
		return 0, errors.Join(ErrIDPoolExhausted, ctx.Err())
	}
}

// TryAllocate is the non-blocking variant. Returns 0, false if no ID is
// immediately available.
func (p *IDPool) TryAllocate() (uint16, bool) {
	select {
	case id := <-p.free:
		return id, true
	default:
		return 0, false
	}
}

// Release returns id to the pool. Dropping (rather than blocking) is
// the safe choice if Release is called with an ID that was never
// allocated — the pool's capacity caps wasted slots.
func (p *IDPool) Release(id uint16) {
	if id == 0 {
		return
	}
	select {
	case p.free <- id:
	default:
	}
}

// Available reports the number of IDs currently free in the pool.
// Useful for metrics; not safe to use for "is there room" decisions
// (use TryAllocate for that).
func (p *IDPool) Available() int { return len(p.free) }
