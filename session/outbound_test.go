// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"testing"

	"github.com/ashtonian/mqttv5/wire"
)

func TestOutboundQoS1Success(t *testing.T) {
	tr := NewOutboundTracker()
	e, err := tr.Register(1, 1, nil)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if tr.Len() != 1 {
		t.Fatalf("Len = %d, want 1", tr.Len())
	}
	if err := tr.HandlePubAck(1, wire.ReasonSuccess); err != nil {
		t.Fatalf("HandlePubAck: %v", err)
	}
	if tr.Len() != 0 {
		t.Fatalf("Len after ack = %d, want 0", tr.Len())
	}
	select {
	case <-e.Done:
	default:
		t.Fatal("Done not closed after PUBACK")
	}
	if e.Err != nil {
		t.Errorf("Err = %v, want nil", e.Err)
	}
}

func TestOutboundQoS1Error(t *testing.T) {
	tr := NewOutboundTracker()
	e, _ := tr.Register(2, 1, nil)
	if err := tr.HandlePubAck(2, wire.ReasonNotAuthorized); err != nil {
		t.Fatalf("HandlePubAck: %v", err)
	}
	<-e.Done
	var re *ReasonError
	if !errors.As(e.Err, &re) || re.Reason != wire.ReasonNotAuthorized {
		t.Fatalf("Err = %v, want ReasonError(NotAuthorized)", e.Err)
	}
}

func TestOutboundQoS2Handshake(t *testing.T) {
	tr := NewOutboundTracker()
	e, _ := tr.Register(3, 2, nil)
	if e.State != StateAwaitingPubRec {
		t.Fatalf("State = %d, want AwaitingPubRec", e.State)
	}

	send, err := tr.HandlePubRec(3, wire.ReasonSuccess)
	if err != nil {
		t.Fatalf("HandlePubRec: %v", err)
	}
	if !send {
		t.Fatal("HandlePubRec should signal PUBREL send on success")
	}
	if e.State != StateAwaitingPubComp {
		t.Fatalf("State = %d, want AwaitingPubComp", e.State)
	}

	select {
	case <-e.Done:
		t.Fatal("Done closed prematurely after PUBREC")
	default:
	}

	if err := tr.HandlePubComp(3, wire.ReasonSuccess); err != nil {
		t.Fatalf("HandlePubComp: %v", err)
	}
	<-e.Done
	if e.Err != nil {
		t.Errorf("Err = %v, want nil", e.Err)
	}
}

func TestOutboundQoS2PubRecError(t *testing.T) {
	tr := NewOutboundTracker()
	e, _ := tr.Register(4, 2, nil)

	send, err := tr.HandlePubRec(4, wire.ReasonQuotaExceeded)
	if err != nil {
		t.Fatalf("HandlePubRec: %v", err)
	}
	if send {
		t.Fatal("HandlePubRec on error reason must not signal PUBREL send")
	}
	<-e.Done
	var re *ReasonError
	if !errors.As(e.Err, &re) || re.Reason != wire.ReasonQuotaExceeded {
		t.Fatalf("Err = %v", e.Err)
	}
	if tr.Len() != 0 {
		t.Fatalf("Len = %d, want 0 (entry should be removed)", tr.Len())
	}
}

func TestOutboundDuplicateRegister(t *testing.T) {
	tr := NewOutboundTracker()
	if _, err := tr.Register(5, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Register(5, 1, nil); !errors.Is(err, ErrDuplicateOutbound) {
		t.Fatalf("got %v, want ErrDuplicateOutbound", err)
	}
}

func TestOutboundUnknownAck(t *testing.T) {
	tr := NewOutboundTracker()
	if err := tr.HandlePubAck(99, wire.ReasonSuccess); !errors.Is(err, ErrUnknownOutbound) {
		t.Fatalf("got %v, want ErrUnknownOutbound", err)
	}
}

func TestOutboundRegisterRejectsBadQoS(t *testing.T) {
	tr := NewOutboundTracker()
	if _, err := tr.Register(1, 0, nil); err == nil {
		t.Fatal("Register(QoS=0) should error")
	}
	if _, err := tr.Register(1, 3, nil); err == nil {
		t.Fatal("Register(QoS=3) should error")
	}
}
