// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

// Out is one outbound QoS 1/2 flow.
type Out struct {
	id        uint16
	qos       byte
	phase     session.Phase
	seq       uint64 // position in pub (send order) or rel (PUBREC order)
	pubSeq    uint64 // original send order, kept for republishing
	packet    []byte // encoded PUBLISH, DUP=0
	expiresAt time.Time
	expiry    uint32 // Message Expiry Interval in packet, when expiresAt is set

	attempted bool // handed to a connection at least once
	counted   bool // its PUBLISH holds one unit of this connection's send quota
	finished  bool
	// settling is set while a job gives the outcome to settle; adopted,
	// when Adopt replaced settle meanwhile, so the new one gets it too.
	settling bool
	adopted  bool
	// reason is the reason code of the broker's refusal, when err is one;
	// outcome is the stored form of the outcome once the flow is Completed.
	reason  wire.ReasonCode
	outcome byte

	ref    []byte
	settle func(context.Context, error) error

	list       *flowList
	prev, next *Out

	done chan struct{}
	err  error
}

// PacketID returns the flow's packet identifier.
func (o *Out) PacketID() uint16 { return o.id }

// Seq returns the flow's send sequence number, for Link.SendOrdered.
func (o *Out) Seq() uint64 { return o.pubSeq }

// Done is closed when the flow completes.
func (o *Out) Done() <-chan struct{} { return o.done }

// Err is the flow's outcome once Done is closed: nil when the broker
// accepted the message, otherwise why it did not.
func (o *Out) Err() error { return o.err }

// Message is an outbound QoS 1/2 PUBLISH to register.
type Message struct {
	ID        uint16 // from AllocateID(OwnerPublish)
	QoS       byte
	Packet    []byte    // encoded with DUP=0
	ExpiresAt time.Time // zero: never
	// Ref identifies the message to its producer across restarts and is
	// stored with its record; see Adopt.
	Ref []byte
	// Settle, when set, is called with the flow's outcome after it
	// finishes and before its record is deleted, so a producer that
	// records the outcome first never loses it to a crash. It runs on an
	// engine goroutine, or inside Connected or Discard when a session is
	// lost, never on the caller's or the read goroutine otherwise.
	//
	// An error means the producer could not record the outcome: the
	// record and the packet identifier are kept, and the outcome is
	// offered again to the next Adopt of the Ref, in this process or,
	// from the stored record, after a restart. When the session fails
	// (see [Config.OnFailure]) Settle gets the [*StoreError]; the record
	// is kept and Adopt finds the flow again after Restore.
	Settle func(ctx context.Context, err error) error
}

// Register adds an outbound flow for m. The record is stored before the
// flow can be sent; if that fails the flow is removed, the identifier
// released, and a [*StoreError] returned. An error means the flow does
// not exist and m.Settle is never called; otherwise the flow completes
// through Done and Settle, also when the session fails meanwhile. The
// returned Link, when non-nil, is the live connection: the caller passes
// out.Seq() to its SendOrdered so the PUBLISH keeps its place relative
// to other writes.
func (e *Engine) Register(ctx context.Context, m Message) (*Out, Link, error) {
	o := &Out{
		id: m.ID, qos: m.QoS, packet: m.Packet, expiresAt: m.ExpiresAt,
		ref: m.Ref, settle: m.Settle,
		phase: session.AwaitPuback,
		done:  make(chan struct{}),
	}
	if m.QoS == 2 {
		o.phase = session.AwaitPubrec
	}

	e.mu.Lock()
	if e.failure != nil {
		e.releaseLocked(m.ID, OwnerPublish)
		defer e.mu.Unlock()
		return nil, nil, e.failure
	}
	if !o.expiresAt.IsZero() {
		o.expiry = secondsUntil(o.expiresAt, e.cfg.Now())
	}
	epoch := e.epoch
	o.seq = e.nextSeq
	o.pubSeq = o.seq
	e.nextSeq++
	e.out[o.id] = o
	if len(o.ref) > 0 {
		e.refs[string(o.ref)] = o
	}
	e.pub.push(o)
	if e.cursor == nil {
		e.cursor = o
	}
	var put *job
	var putErr error
	if e.store != nil {
		r := o.record()
		// A fresh flow has no earlier write, so this Put is first in line
		// and runs below on the caller's goroutine, under its ctx.
		put = e.queueLocked(outKey(o.id), o, &job{
			op: "put outbound",
			do: func(context.Context) error { return e.store.Put(ctx, r) },
			after: func(err error) error {
				if err == nil {
					return nil
				}
				// Never stored: the caller learns it was not accepted, and
				// writes queued for it since (a reconnect's) never run.
				putErr = &StoreError{Op: "put outbound", Err: err}
				e.cancelLocked(outKey(o.id), o)
				e.unlinkLocked(o)
				delete(e.out, o.id)
				if len(o.ref) > 0 {
					delete(e.refs, string(o.ref))
					// A session loss may have ended it meanwhile.
					if e.orphans[string(o.ref)] == o {
						delete(e.orphans, string(o.ref))
					}
				}
				o.finished = true
				e.releaseLocked(o.id, OwnerPublish)
				return nil
			},
		})
	}
	e.mu.Unlock()

	if put != nil {
		e.run(put)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case putErr != nil:
		return nil, nil, putErr
	case e.epoch != epoch:
		// The session failed while the record was written. o was part of
		// it, so it has completed, and its Settle run, with the failure.
		return o, nil, nil
	}
	return o, e.link, nil
}

// Withdraw removes o if it has never been handed to a connection, so a
// caller that gives up early gets a definite "not sent". It reports
// whether o was withdrawn; false means the broker may already have it.
func (e *Engine) Withdraw(o *Out) bool {
	e.mu.Lock()
	if o.finished || o.attempted {
		e.mu.Unlock()
		return false
	}
	e.unlinkLocked(o)
	e.finishLocked(o, context.Canceled)
	j := e.retireLocked(o)
	e.mu.Unlock()
	e.start(j)
	return true
}

// Adopt makes settle the Settle of the flow whose Ref is ref — one
// restored from the store, or one an earlier producer started and gave
// up on — and reports whether there is such a flow. If it has finished
// without its outcome being recorded, settle gets that outcome on an
// engine goroutine. The Settle it replaces is never called.
func (e *Engine) Adopt(ref []byte, settle func(context.Context, error) error) bool {
	e.mu.Lock()
	if o := e.refs[string(ref)]; o != nil {
		o.settle = settle
		e.mu.Unlock()
		return true
	}
	o := e.orphans[string(ref)]
	if o == nil {
		e.mu.Unlock()
		return false
	}
	o.settle = settle
	var j *job
	if o.settling {
		o.adopted = true
	} else {
		j = e.settleLocked(o)
	}
	e.mu.Unlock()
	e.start(j)
	return true
}

// HandlePuback applies a PUBACK with reason code rc. refused is non-nil
// when rc is 0x80 or above and becomes the publish's error.
func (e *Engine) HandlePuback(id uint16, rc wire.ReasonCode, refused error) {
	e.mu.Lock()
	if e.failure != nil {
		e.mu.Unlock()
		return
	}
	o := e.out[id]
	if o == nil || o.qos != 1 || !o.attempted {
		e.mu.Unlock()
		e.stray(wire.PUBACK, id)
		return
	}
	e.unlinkLocked(o)
	if refused != nil {
		o.reason = rc
	}
	e.finishLocked(o, refused)
	e.creditLocked(o)
	j := e.retireLocked(o)
	e.mu.Unlock()
	e.start(j)
	e.wake()
}

// HandlePubrec applies a PUBREC with reason code rc. A refusal (refused
// non-nil, rc 0x80 or above) completes the flow; success moves it to
// AwaitPubcomp and queues the PUBREL, which waits until that phase is
// stored. A PUBREC for a flow already at AwaitPubcomp is answered with
// another PUBREL, and one for an unknown identifier with PUBREL 0x92.
func (e *Engine) HandlePubrec(id uint16, rc wire.ReasonCode, refused error) {
	var j *job
	e.mu.Lock()
	if e.failure != nil {
		e.mu.Unlock()
		return
	}
	o := e.out[id]
	switch {
	case o == nil:
		e.pushCtrlLocked(wire.PUBREL, id, wire.ReasonPacketIdentifierNotFound, false, nil)
	case o.qos != 2 || !o.attempted:
		e.mu.Unlock()
		e.stray(wire.PUBREC, id)
		return
	case o.phase == session.AwaitPubcomp:
		e.pushCtrlLocked(wire.PUBREL, id, wire.ReasonSuccess, true, nil)
	case refused != nil:
		e.unlinkLocked(o)
		o.reason = rc
		e.finishLocked(o, refused)
		e.creditLocked(o)
		j = e.retireLocked(o)
	default:
		e.unlinkLocked(o)
		o.phase = session.AwaitPubcomp
		o.seq = e.nextSeq
		e.nextSeq++
		e.rel.push(o)
		e.pushCtrlLocked(wire.PUBREL, id, wire.ReasonSuccess, true, nil)
		j = e.putOutLocked(o)
	}
	e.mu.Unlock()
	e.start(j)
	e.wake()
}

// HandlePubcomp applies a PUBCOMP, completing a flow at AwaitPubcomp.
// Reason 0x92 means the broker had already finished the exchange, so it
// is not an error.
func (e *Engine) HandlePubcomp(id uint16) {
	e.mu.Lock()
	if e.failure != nil {
		e.mu.Unlock()
		return
	}
	o := e.out[id]
	if o == nil || o.phase != session.AwaitPubcomp {
		e.mu.Unlock()
		e.stray(wire.PUBCOMP, id)
		return
	}
	e.unlinkLocked(o)
	e.finishLocked(o, nil)
	e.creditLocked(o)
	j := e.retireLocked(o)
	e.mu.Unlock()
	e.start(j)
	e.wake()
}

// finishLocked completes o with err. Its record and packet identifier
// go with retireLocked.
func (e *Engine) finishLocked(o *Out, err error) {
	delete(e.out, o.id)
	o.finished = true
	o.err = err
	close(o.done)
	if len(o.ref) > 0 {
		delete(e.refs, string(o.ref))
	}
}

// retireLocked disposes of a finished o: its producer settles the
// outcome, then the record is deleted and only then the identifier
// released, so it is never reused while a stale record could still name
// it. A flow with a Ref is an orphan from now until its record is gone,
// so Adopt can always find it; one whose outcome nobody has recorded
// keeps its record, Completed, and identifier until Adopt settles it.
// It returns the job to start, or nil.
func (e *Engine) retireLocked(o *Out) *job {
	if len(o.ref) > 0 {
		e.orphans[string(o.ref)] = o
		if o.settle == nil {
			return e.completeLocked(o)
		}
	} else if e.store == nil && o.settle == nil {
		e.releaseLocked(o.id, OwnerPublish)
		return nil
	}
	return e.settleLocked(o)
}

// completeLocked stores the outcome of finished o as a Completed record
// in place of the phase before it, so a restart never runs the ended
// exchange again (a refused PUBLISH, too, must never be sent again,
// [MQTT-4.4.0-2]); Restore gives the outcome back to Adopt. It returns
// the job to start, or nil: without a store, for a record already
// Completed, and for an outcome without a stored form.
func (e *Engine) completeLocked(o *Out) *job {
	if e.store == nil || o.phase == session.Completed {
		return nil
	}
	outcome, ok := encodeOutcome(o)
	if !ok {
		return nil
	}
	o.phase, o.outcome = session.Completed, outcome
	return e.putOutLocked(o)
}

// encodeOutcome is the stored form of finished o's outcome.
func encodeOutcome(o *Out) (byte, bool) {
	switch {
	case o.err == nil:
		return session.OutcomeAccepted, true
	case o.reason.IsError():
		return byte(o.reason), true
	case errors.Is(o.err, ErrSessionLost):
		return session.OutcomeSessionLost, true
	case errors.Is(o.err, ErrMessageExpired):
		return session.OutcomeExpired, true
	case errors.Is(o.err, ErrPacketTooLarge):
		return session.OutcomePacketTooLarge, true
	case errors.Is(o.err, ErrRetainNotSupported):
		return session.OutcomeRetainNotSupported, true
	case errors.Is(o.err, ErrQoSNotSupported):
		return session.OutcomeQoSNotSupported, true
	}
	return 0, false
}

// decodeOutcomeLocked gives restored o the outcome its Completed record
// holds, as the error its producer gets. It reports false for a value
// this version does not know.
func (e *Engine) decodeOutcomeLocked(o *Out) bool {
	switch b := o.outcome; {
	case b == session.OutcomeAccepted:
		o.err = nil
	case wire.ReasonCode(b).IsError():
		t := wire.PUBACK
		if o.qos == 2 {
			t = wire.PUBREC
		}
		o.reason = wire.ReasonCode(b)
		o.err = e.cfg.Refusal(t, o.reason)
	case b == session.OutcomeSessionLost:
		o.err = ErrSessionLost
	case b == session.OutcomeExpired:
		o.err = ErrMessageExpired
	case b == session.OutcomePacketTooLarge:
		o.err = ErrPacketTooLarge
	case b == session.OutcomeRetainNotSupported:
		o.err = ErrRetainNotSupported
	case b == session.OutcomeQoSNotSupported:
		o.err = ErrQoSNotSupported
	default:
		return false
	}
	return true
}

// settleLocked queues the job that gives o's outcome to its Settle and
// then deletes its record. When the Settle fails o stays an orphan,
// record and identifier kept; when Adopt gave o another Settle
// meanwhile, that one gets the outcome next. It returns the job to
// start, or nil.
func (e *Engine) settleLocked(o *Out) *job {
	o.settling = true
	settle, outcome, ref := o.settle, o.err, o.ref
	k := outKey(o.id)
	return e.queueLocked(k, o, &job{
		op: "delete outbound",
		do: func(ctx context.Context) error {
			if settle != nil {
				if err := settle(ctx, outcome); err != nil && len(ref) > 0 {
					return &settleError{err}
				}
			}
			if e.store == nil {
				return nil
			}
			return e.store.Delete(ctx, k)
		},
		after: func(err error) error {
			o.settling = false
			var se *settleError
			failed := errors.As(err, &se)
			if err != nil && !failed {
				return &StoreError{Op: "delete outbound", Err: err}
			}
			if failed {
				// Queued behind this job, which starts it.
				e.completeLocked(o)
			}
			switch {
			case o.adopted:
				o.adopted = false
				// Queued behind this job, which starts it.
				e.settleLocked(o)
			case failed:
				o.settle = nil
			default:
				if len(o.ref) > 0 && e.orphans[string(o.ref)] == o {
					delete(e.orphans, string(o.ref))
				}
				e.releaseLocked(o.id, OwnerPublish)
			}
			return nil
		},
		goroutine: settle != nil,
	})
}

// creditLocked returns the unit of send quota o's PUBLISH took on this
// connection (§4.9). A flow whose PUBLISH went out on an earlier
// connection holds none: crediting its PUBCOMP, as the spec's counting
// rule would, lets one more PUBLISH out than the server's Receive
// Maximum while the resent PUBREL is outstanding.
func (e *Engine) creditLocked(o *Out) {
	if o.counted {
		o.counted = false
		e.quota++
	}
}

func (e *Engine) unlinkLocked(o *Out) {
	if e.cursor == o {
		e.cursor = o.next
	}
	if o.list != nil {
		o.list.remove(o)
	}
}

func (o *Out) record() session.Record {
	r := session.Record{
		Key:       session.RecordKey{Dir: session.Outbound, PacketID: o.id},
		Seq:       o.pubSeq,
		QoS:       o.qos,
		Phase:     o.phase,
		Packet:    o.packet,
		ExpiresAt: o.expiresAt,
		Ref:       o.ref,
	}
	switch o.phase {
	case session.AwaitPubcomp:
		r.PubrecSeq = o.seq
	case session.Completed:
		r.Packet = nil
		r.Outcome = o.outcome
	}
	return r
}

// putOutLocked queues a write of o's current state, snapshotted now. It
// returns the job to start, or nil.
func (e *Engine) putOutLocked(o *Out) *job {
	if e.store == nil {
		return nil
	}
	r := o.record()
	return e.queueLocked(outKey(o.id), o, &job{op: "put outbound", do: func(ctx context.Context) error { return e.store.Put(ctx, r) }})
}

func sortByPubSeq(flows []*Out) {
	slices.SortFunc(flows, func(a, b *Out) int { return cmp.Compare(a.pubSeq, b.pubSeq) })
}

// flowList is an intrusive doubly linked list of flows.
type flowList struct {
	head, tail *Out
	len        int
}

func (l *flowList) push(o *Out) {
	o.list = l
	o.prev = l.tail
	o.next = nil
	if l.tail != nil {
		l.tail.next = o
	} else {
		l.head = o
	}
	l.tail = o
	l.len++
}

func (l *flowList) remove(o *Out) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		l.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		l.tail = o.prev
	}
	o.prev, o.next, o.list = nil, nil, nil
	l.len--
}
