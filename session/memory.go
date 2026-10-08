// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
)

// ErrClosed is returned by [MemoryStore] methods after Close.
var ErrClosed = errors.New("mqttv5/session: store closed")

// MemoryStore is a [Store] held in process memory. It survives
// reconnects but not a process restart, and is the reference
// implementation the store conformance suite (session/storetest) is
// written against. The client needs no Store for in-process
// resumption; use MemoryStore in tests or as a model for your own.
type MemoryStore struct {
	mu      sync.Mutex
	closed  bool
	meta    Meta
	records map[RecordKey]Record
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[RecordKey]Record)}
}

// Load implements [Store].
func (s *MemoryStore) Load(context.Context) (Meta, []Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Meta{}, nil, ErrClosed
	}
	out := make([]Record, 0, len(s.records))
	for _, r := range s.records {
		r.Packet = bytes.Clone(r.Packet)
		r.Ref = bytes.Clone(r.Ref)
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b Record) int {
		if c := cmp.Compare(a.Key.Dir, b.Key.Dir); c != 0 {
			return c
		}
		if a.Key.Dir == Outbound {
			return cmp.Compare(a.Seq, b.Seq)
		}
		return cmp.Compare(a.Key.PacketID, b.Key.PacketID)
	})
	return s.meta, out, nil
}

// Put implements [Store].
func (s *MemoryStore) Put(_ context.Context, r Record) error {
	if err := r.Validate(); err != nil {
		return err
	}
	r.Packet = bytes.Clone(r.Packet)
	r.Ref = bytes.Clone(r.Ref)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.records[r.Key] = r
	return nil
}

// Delete implements [Store].
func (s *MemoryStore) Delete(_ context.Context, k RecordKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	delete(s.records, k)
	return nil
}

// SetMeta implements [Store].
func (s *MemoryStore) SetMeta(_ context.Context, m Meta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.meta = m
	return nil
}

// Reset implements [Store].
func (s *MemoryStore) Reset(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.meta = Meta{}
	clear(s.records)
	return nil
}

// Close implements [Store].
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

var _ Store = (*MemoryStore)(nil)
