// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package session defines how an MQTT v5 client persists its session
// state: the [Store] contract, the [Record]s it holds, and [MemoryStore],
// an in-memory reference implementation.
//
// The client keeps the authoritative session state in memory. A Store is
// the write-ahead copy that lets a restarted process resume the session:
// every QoS 1/2 flow that is not yet complete, the protocol phase it has
// reached, and the order in which the flows were sent.
//
// # Write-ahead order
//
// The client writes a record before it acts on the network:
//
//   - outbound PUBLISH: Put (phase [AwaitPuback] or [AwaitPubrec]), then send;
//   - PUBREC received: Put (phase [AwaitPubcomp]), then send PUBREL;
//   - PUBACK / PUBCOMP received, or a refusal: Delete — or, when the
//     message's producer could not yet record the outcome, Put (phase
//     [Completed], with the [Record.Outcome]) until it has;
//   - inbound QoS 2 acknowledged by the application: Put (phase
//     [AwaitPubrel]), then send PUBREC;
//   - PUBREL received: Delete, then send PUBCOMP.
//
// Put and Delete must therefore not return before the change is as
// durable as the implementation promises; a Store that buffers writes
// weakens the guarantee to whatever survives its buffering. Writes for
// one record arrive in the order the client decided them, never
// concurrently.
//
// # Errors
//
// The client treats an error from Put, Delete or SetMeta as final: a
// failed Put of a new outbound record fails that publish, and any other
// failed write stops the client, which drops its session state in memory
// as a crash would and reloads it with Load on the next Connect. A Store
// whose backend has transient failures (a network database, say)
// retries them itself, within the ctx it is given, before it reports an
// error.
package session

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Direction says which side sent the PUBLISH a record tracks.
type Direction uint8

const (
	// Outbound records track PUBLISH packets this client sent.
	Outbound Direction = 1
	// Inbound records track QoS 2 PUBLISH packets this client received.
	Inbound Direction = 2
)

// Phase is the step a QoS 1/2 flow has reached.
type Phase uint8

const (
	// AwaitPuback is an outbound QoS 1 PUBLISH sent and waiting for
	// its PUBACK.
	AwaitPuback Phase = 1
	// AwaitPubrec is an outbound QoS 2 PUBLISH sent and waiting for its
	// PUBREC.
	AwaitPubrec Phase = 2
	// AwaitPubcomp is an outbound QoS 2 flow whose PUBREL was sent,
	// waiting for PUBCOMP.
	AwaitPubcomp Phase = 3
	// AwaitPubrel is an inbound QoS 2 PUBLISH whose PUBREC was sent,
	// waiting for PUBREL.
	AwaitPubrel Phase = 4
	// Completed is an outbound message whose exchange has ended — the
	// broker accepted or refused it, or the client ended it — and whose
	// producer (a QueuePublisher, by its [Record.Ref]) has not yet
	// recorded the outcome, which [Record.Outcome] holds. It is never sent
	// again; the record is deleted once the producer has the outcome. Its
	// Packet is empty.
	Completed Phase = 5
)

// Outcomes of a [Completed] record other than a broker's refusal, which
// is its reason code (0x80 or above).
const (
	// OutcomeAccepted: the broker accepted the message.
	OutcomeAccepted byte = 0x00
	// OutcomeSessionLost: the broker lost the session and the client's
	// session-loss policy failed the message.
	OutcomeSessionLost byte = 0x01
	// OutcomeExpired: its Message Expiry Interval ran out before it was
	// sent.
	OutcomeExpired byte = 0x02
	// OutcomePacketTooLarge: it is above the broker's Maximum Packet Size.
	OutcomePacketTooLarge byte = 0x03
	// OutcomeRetainNotSupported: it is retained and the broker has no
	// retain.
	OutcomeRetainNotSupported byte = 0x04
	// OutcomeQoSNotSupported: its QoS is above the broker's Maximum QoS.
	OutcomeQoSNotSupported byte = 0x05
)

// Valid reports whether p is a defined phase for direction d.
func (p Phase) Valid(d Direction) bool {
	switch d {
	case Outbound:
		return p == AwaitPuback || p == AwaitPubrec || p == AwaitPubcomp || p == Completed
	case Inbound:
		return p == AwaitPubrel
	}
	return false
}

// RecordKey identifies a record. Packet identifiers are unique per
// direction while a flow is live.
type RecordKey struct {
	Dir      Direction
	PacketID uint16
}

// Record is one QoS 1/2 flow that has not completed.
type Record struct {
	Key RecordKey

	// Seq orders outbound records by the order their PUBLISH packets
	// were first sent in. Resumption resends PUBLISHes in Seq order
	// (§4.6), and a lost session republishes every message in it. Unused
	// for inbound records.
	Seq uint64

	// PubrecSeq orders [AwaitPubcomp] records by the order their PUBRECs
	// arrived in, which is the order their PUBRELs are resent in (§4.6).
	// Zero in any other phase. It counts on the same sequence as Seq.
	PubrecSeq uint64

	// Outcome is how a [Completed] record's exchange ended: one of the
	// Outcome* values, or the reason code of the broker's refusing PUBACK
	// or PUBREC (0x80 or above). Zero in any other phase.
	Outcome byte

	// QoS is 1 or 2.
	QoS byte

	Phase Phase

	// Packet is the encoded outbound PUBLISH with DUP=0, as first sent.
	// Nil for inbound records. A Store keeps its own copy: the slice
	// passed to Put may be reused by the caller after Put returns, and
	// the slice returned by Load is owned by the caller.
	Packet []byte

	// ExpiresAt is when the outbound message's Message Expiry Interval
	// runs out; zero means it never expires.
	ExpiresAt time.Time

	// Ref identifies what published an outbound message, so it can find
	// the message again after a restart (a QueuePublisher stores its
	// queue entry's ID). Opaque to the Store, at most [MaxRefLen] bytes,
	// nil when unset. Copied like Packet.
	Ref []byte
}

// MaxRefLen is the longest [Record.Ref] a Store accepts.
const MaxRefLen = 255

// ErrInvalidRecord marks a record no Store may hold.
var ErrInvalidRecord = errors.New("mqttv5/session: invalid record")

// Validate reports why r cannot be stored, wrapping [ErrInvalidRecord]:
// a phase not defined for its direction, or a Ref over [MaxRefLen].
func (r Record) Validate() error {
	if !r.Phase.Valid(r.Key.Dir) {
		return fmt.Errorf("%w: phase %d for direction %d", ErrInvalidRecord, r.Phase, r.Key.Dir)
	}
	if len(r.Ref) > MaxRefLen {
		return fmt.Errorf("%w: Ref of %d bytes", ErrInvalidRecord, len(r.Ref))
	}
	return nil
}

// Meta is session-wide state that must survive a restart.
type Meta struct {
	// ClientID is the identifier the broker assigned when the client
	// connected without one; empty when the configured identifier is in
	// use. Reused on every later CONNECT so the session can resume.
	ClientID string

	// SessionExpiry is the effective Session Expiry Interval in seconds
	// (the broker's CONNACK value when it overrode the requested one).
	SessionExpiry uint32
}

// Store persists session state. Implementations must be safe for
// concurrent use and must keep the copies of byte slices they are given.
type Store interface {
	// Load returns the stored meta and every record, outbound records
	// ordered by Seq. An empty store returns a zero Meta and no records.
	Load(ctx context.Context) (Meta, []Record, error)

	// Put inserts or replaces the record with r.Key. A record that fails
	// [Record.Validate] is refused with its error.
	Put(ctx context.Context, r Record) error

	// Delete removes the record with key k. Deleting a missing record is
	// not an error.
	Delete(ctx context.Context, k RecordKey) error

	// SetMeta replaces the stored meta.
	SetMeta(ctx context.Context, m Meta) error

	// Reset removes every record and the meta.
	Reset(ctx context.Context) error

	// Close releases the store's resources. Further calls return errors.
	Close() error
}
