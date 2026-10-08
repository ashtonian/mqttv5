// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"context"
	"errors"
)

// Owner records what a packet identifier is allocated to. An
// acknowledgement only ever acts on a flow of the matching kind, so a
// stray PUBACK cannot free an identifier a SUBSCRIBE is using.
type Owner uint8

// The owners of a packet identifier.
const (
	OwnerNone Owner = iota
	OwnerPublish
	OwnerSubscribe
	OwnerUnsubscribe
)

// ErrIDsExhausted is returned by AllocateID when all 65,535 packet
// identifiers are in use and ctx ends before one is released.
var ErrIDsExhausted = errors.New("mqttv5: all packet identifiers in use")

// ErrStopped is returned by AllocateID when its stop channel closes while
// it waits for an identifier.
var ErrStopped = errors.New("mqttv5: stopped waiting for a packet identifier")

// idTable hands out packet identifiers 1..65535 and remembers each
// one's owner. It is guarded by the engine's mutex except for inUse,
// whose buffered capacity counts allocated identifiers so a full table
// blocks AllocateID without holding the mutex.
type idTable struct {
	owner [65536]Owner
	next  uint16
	max   uint16 // highest identifier handed out; 65535 outside tests
	inUse chan struct{}
}

func newIDTable(max uint16) idTable {
	return idTable{next: 1, max: max, inUse: make(chan struct{}, int(max))}
}

// take marks the first free identifier at or after the hint as owned.
// The caller holds a reservation in inUse, so one is always free.
func (t *idTable) take(o Owner) uint16 {
	for {
		id := t.next
		if t.next == t.max {
			t.next = 1
		} else {
			t.next++
		}
		if t.owner[id] == OwnerNone {
			t.owner[id] = o
			return id
		}
	}
}

// claim marks a specific identifier as owned, for state restored from a
// store. It reports false when the identifier is already taken.
func (t *idTable) claim(id uint16, o Owner) bool {
	if id == 0 || id > t.max || t.owner[id] != OwnerNone {
		return false
	}
	select {
	case t.inUse <- struct{}{}:
	default:
		return false
	}
	t.owner[id] = o
	return true
}

// AllocateID reserves a packet identifier for o, blocking while all are
// in use until ctx ends or stop closes (nil never does).
func (e *Engine) AllocateID(ctx context.Context, o Owner, stop <-chan struct{}) (uint16, error) {
	select {
	case e.ids.inUse <- struct{}{}:
	default:
		select {
		case e.ids.inUse <- struct{}{}:
		case <-ctx.Done():
			return 0, errors.Join(ErrIDsExhausted, ctx.Err())
		case <-stop:
			return 0, ErrStopped
		}
	}
	e.mu.Lock()
	id := e.ids.take(o)
	e.mu.Unlock()
	return id, nil
}

// ReleaseID frees id if o owns it and reports whether it did.
func (e *Engine) ReleaseID(id uint16, o Owner) bool {
	e.mu.Lock()
	ok := e.releaseLocked(id, o)
	e.mu.Unlock()
	return ok
}

func (e *Engine) releaseLocked(id uint16, o Owner) bool {
	if id == 0 || e.ids.owner[id] != o {
		return false
	}
	e.ids.owner[id] = OwnerNone
	<-e.ids.inUse
	return true
}
