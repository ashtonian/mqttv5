// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"sync"

	"github.com/ashtonian/mqttv5/wire"
)

// OutboundState is the current step in the QoS 1/2 publish handshake.
type OutboundState uint8

const (
	// StateAwaitingPubAck (QoS 1): sent PUBLISH, waiting for PUBACK.
	StateAwaitingPubAck OutboundState = iota
	// StateAwaitingPubRec (QoS 2): sent PUBLISH, waiting for PUBREC.
	StateAwaitingPubRec
	// StateAwaitingPubComp (QoS 2): sent PUBREL, waiting for PUBCOMP.
	StateAwaitingPubComp
)

// OutboundEntry tracks one in-flight outbound publish.
//
// Done is closed when the publish has been fully acknowledged. If the
// publish failed (broker returned an error reason code), Err carries
// the reason before Done is closed.
//
// Packet holds the serialised PUBLISH bytes for retransmit on
// reconnect. The bytes are owned by the entry; do not mutate.
type OutboundEntry struct {
	PacketID uint16
	QoS      byte
	State    OutboundState
	Packet   []byte // serialised PUBLISH for replay; nil if retain-on-store disabled
	Done     chan struct{}
	Err      error
}

// ErrDuplicateOutbound is returned by Register when the same packet ID
// is already in flight. Callers should not see this — IDPool prevents
// reuse — but defending here makes debugging easier.
var ErrDuplicateOutbound = errors.New("mqttv5/session: outbound packet id already in flight")

// ErrUnknownOutbound is returned by handlers when an ack arrives for a
// packet ID the tracker has no record of (often a duplicate ack the
// broker re-sent).
var ErrUnknownOutbound = errors.New("mqttv5/session: outbound packet id not found")

// OutboundTracker is the in-memory in-flight publish table. The state
// machine is small enough that one mutex over a map is the right
// answer; the alternative (per-ID locks, lock-stripping) would dwarf
// the actual ack-handling work.
type OutboundTracker struct {
	mu       sync.Mutex
	inflight map[uint16]*OutboundEntry
}

// NewOutboundTracker returns an empty tracker.
func NewOutboundTracker() *OutboundTracker {
	return &OutboundTracker{inflight: make(map[uint16]*OutboundEntry)}
}

// Register records a fresh outbound publish. The returned entry's Done
// channel signals completion. qos must be 1 or 2.
//
// packet may be nil; pass the serialised PUBLISH bytes when you intend
// to persist them via a Store for replay-on-reconnect.
func (t *OutboundTracker) Register(id uint16, qos byte, packet []byte) (*OutboundEntry, error) {
	if qos != 1 && qos != 2 {
		return nil, errors.New("mqttv5/session: outbound register requires QoS 1 or 2")
	}
	state := StateAwaitingPubAck
	if qos == 2 {
		state = StateAwaitingPubRec
	}
	e := &OutboundEntry{
		PacketID: id,
		QoS:      qos,
		State:    state,
		Packet:   packet,
		Done:     make(chan struct{}),
	}
	t.mu.Lock()
	if _, dup := t.inflight[id]; dup {
		t.mu.Unlock()
		return nil, ErrDuplicateOutbound
	}
	t.inflight[id] = e
	t.mu.Unlock()
	return e, nil
}

// HandlePubAck (QoS 1) completes the publish.
func (t *OutboundTracker) HandlePubAck(id uint16, reason wire.ReasonCode) error {
	t.mu.Lock()
	e, ok := t.inflight[id]
	if !ok {
		t.mu.Unlock()
		return ErrUnknownOutbound
	}
	delete(t.inflight, id)
	t.mu.Unlock()

	if reason.IsError() {
		e.Err = reasonError(reason)
	}
	close(e.Done)
	return nil
}

// HandlePubRec (QoS 2 first half) advances the entry from
// AwaitingPubRec to AwaitingPubComp. Returns true to indicate the
// caller (the client) should now send a PUBREL.
//
// If reason is an error code, the publish is failed immediately and
// PUBREL is NOT sent — the entry is completed with the reason.
func (t *OutboundTracker) HandlePubRec(id uint16, reason wire.ReasonCode) (sendPubRel bool, err error) {
	t.mu.Lock()
	e, ok := t.inflight[id]
	if !ok {
		t.mu.Unlock()
		return false, ErrUnknownOutbound
	}
	if reason.IsError() {
		delete(t.inflight, id)
		t.mu.Unlock()
		e.Err = reasonError(reason)
		close(e.Done)
		return false, nil
	}
	e.State = StateAwaitingPubComp
	t.mu.Unlock()
	return true, nil
}

// HandlePubComp (QoS 2 second half) completes the publish.
func (t *OutboundTracker) HandlePubComp(id uint16, reason wire.ReasonCode) error {
	t.mu.Lock()
	e, ok := t.inflight[id]
	if !ok {
		t.mu.Unlock()
		return ErrUnknownOutbound
	}
	delete(t.inflight, id)
	t.mu.Unlock()

	if reason.IsError() {
		e.Err = reasonError(reason)
	}
	close(e.Done)
	return nil
}

// Len reports the count of in-flight outbound publishes. Mostly useful
// for tests and metrics.
func (t *OutboundTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.inflight)
}

// Range iterates a snapshot of the in-flight table. yield returns
// false to stop early.
func (t *OutboundTracker) Range(yield func(*OutboundEntry) bool) {
	t.mu.Lock()
	snapshot := make([]*OutboundEntry, 0, len(t.inflight))
	for _, e := range t.inflight {
		snapshot = append(snapshot, e)
	}
	t.mu.Unlock()
	for _, e := range snapshot {
		if !yield(e) {
			return
		}
	}
}

// ReasonError wraps a non-success ReasonCode as an error. Kept as a
// helper so callers can unwrap and inspect via errors.As.
type ReasonError struct{ Reason wire.ReasonCode }

// Error implements the error interface, formatting the wrapped MQTT
// reason code into a human-readable message.
func (e *ReasonError) Error() string {
	return "mqttv5: publish refused with reason code " + reasonName(e.Reason)
}

// Unwrap is used by errors.As to extract the ReasonCode.
func (e *ReasonError) Unwrap() error { return nil }

func reasonError(r wire.ReasonCode) error { return &ReasonError{Reason: r} }

// reasonName is a compact diagnostic name; we deliberately do not
// include the full §3.x.2 prose to keep the binary small. Callers who
// want the long description can map it themselves.
func reasonName(r wire.ReasonCode) string {
	switch r {
	case wire.ReasonNoMatchingSubscribers:
		return "no matching subscribers"
	case wire.ReasonNotAuthorized:
		return "not authorized"
	case wire.ReasonTopicNameInvalid:
		return "topic name invalid"
	case wire.ReasonPacketIdentifierInUse:
		return "packet identifier in use"
	case wire.ReasonPacketIdentifierNotFound:
		return "packet identifier not found"
	case wire.ReasonQuotaExceeded:
		return "quota exceeded"
	case wire.ReasonPayloadFormatInvalid:
		return "payload format invalid"
	default:
		return "code 0x" + hexByte(byte(r))
	}
}

func hexByte(b byte) string {
	const hex = "0123456789abcdef"
	return string([]byte{hex[b>>4], hex[b&0x0F]})
}
