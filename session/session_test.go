// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"

	"github.com/ashtonian/mqttv5/wire"
)

func TestSessionDefaults(t *testing.T) {
	s := New(Config{})
	if s.IDs == nil || s.Outbound == nil || s.Inbound == nil || s.Store == nil {
		t.Fatal("New() left a nil field with zero Config")
	}
	if s.IDs.Available() != 65535 {
		t.Fatalf("ID pool size = %d, want 65535", s.IDs.Available())
	}
}

func TestSessionAllocateAndPublishLifecycle(t *testing.T) {
	s := New(Config{ReceiveMaximum: 4})
	id, err := s.AllocateID(context.Background())
	if err != nil {
		t.Fatalf("AllocateID: %v", err)
	}
	e, err := s.Outbound.Register(id, 1, nil)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := s.Outbound.HandlePubAck(id, wire.ReasonSuccess); err != nil {
		t.Fatalf("HandlePubAck: %v", err)
	}
	<-e.Done
	s.ReleaseID(id)
	if s.IDs.Available() != 4 {
		t.Fatalf("Available = %d, want 4 after release", s.IDs.Available())
	}
}

func TestSessionResetClearsState(t *testing.T) {
	s := New(Config{ReceiveMaximum: 4})

	// Outbound entry
	if _, err := s.Outbound.Register(1, 1, nil); err != nil {
		t.Fatal(err)
	}
	// Inbound QoS 2 entry mid-handshake
	s.Inbound.RegisterPublish(2, 2)
	_ = s.Inbound.AckPublish(2) // emits PUBREC, persists in store

	count := 0
	_ = s.Store.RangeInbound(func(_ uint16) bool { count++; return true })
	if count != 1 {
		t.Fatalf("pre-Reset store inbound = %d, want 1", count)
	}

	if err := s.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if s.Outbound.Len() != 0 {
		t.Errorf("Outbound.Len after Reset = %d, want 0", s.Outbound.Len())
	}
	if s.Inbound.Len() != 0 {
		t.Errorf("Inbound.Len after Reset = %d, want 0", s.Inbound.Len())
	}
	count = 0
	_ = s.Store.RangeInbound(func(_ uint16) bool { count++; return true })
	if count != 0 {
		t.Errorf("post-Reset store inbound = %d, want 0", count)
	}
}
