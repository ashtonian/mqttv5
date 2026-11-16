package inflight

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	// settling is set while the producer records the outcome; adopted,
	// when Adopt replaced settle meanwhile, so the new one gets it too.
	// settleJob is the job that gives the outcome to the producer.
	settling  bool
	adopted   bool
	settleJob *job
	// reason is the reason code of the broker's refusal, when err is one;
	// outcome is the stored form of the outcome once the flow is Completed.
	reason  wire.ReasonCode
	outcome byte

	ref    []byte
	settle SettleFunc

	list       *flowList
	prev, next *Out

	err error // the outcome, once finished
	// done is closed, once, when the flow finishes or when Interrupt ends
	// the wait on it first; waitErr is what Err reports from then on, and
	// onDone is told it then.
	done    chan struct{}
	woken   bool
	waitErr error
	onDone  func(err error)
}

// PacketID returns the flow's packet identifier.
func (o *Out) PacketID() uint16 { return o.id }

// Seq returns the flow's send sequence number, for Link.SendOrdered.
func (o *Out) Seq() uint64 { return o.pubSeq }

// Done is closed when the flow completes, or when [Engine.Interrupt]
// ends the wait on it before that.
func (o *Out) Done() <-chan struct{} { return o.done }

// Err is why Done closed: the flow's outcome, nil when the broker
// accepted the message and otherwise why it did not, or
// [ErrInterrupted].
func (o *Out) Err() error { return o.waitErr }

// wake ends the wait on Done with err, unless it has ended already. The
// caller holds the engine's mutex.
func (o *Out) wake(err error) {
	if o.woken {
		return
	}
	o.woken = true
	o.waitErr = err
	close(o.done)
	if o.onDone != nil {
		o.onDone(err)
	}
}

// SettleFunc gives a finished flow's outcome to its producer, which
// records it and then calls done, once, from any goroutine: with nil once
// it has, and the session forgets the flow — Adopt stops finding it at
// once, and its record is deleted behind; or with the error that kept
// it from recording the outcome, and the flow keeps its record and packet
// identifier, the outcome offered again to the next Adopt of its Ref, in
// this process or, from the stored record, after a restart. A
// SettleFunc must not block: it may run on the connection's read
// goroutine. For a flow a session failure or Discard drops, done changes
// nothing.
type SettleFunc func(outcome error, done func(error))

// ignoreDone is the done of an outcome nothing waits for.
func ignoreDone(error) {}

// Message is an outbound QoS 1/2 PUBLISH to register.
type Message struct {
	QoS byte
	// Packet is the PUBLISH, encoded with DUP=0. Register writes the
	// identifier it allocates into it, so the one it was encoded with is
	// a placeholder.
	Packet    []byte
	ExpiresAt time.Time // zero: never
	// Ref identifies the message to its producer across restarts and is
	// stored with its record; see Adopt.
	Ref []byte
	// Settle, when set, is given the flow's outcome once it finishes. The
	// record is deleted only after the producer has recorded the outcome
	// (see [SettleFunc]), so a crash never loses it. When the session
	// fails (see [Config.OnFailure]) Settle gets the [*StoreError]; the
	// record is kept and Adopt finds the flow again after Restore.
	Settle SettleFunc
	// OnDone, when set, is called once when the flow's Done closes, with
	// what Err reports from then on, unless Withdraw closes it: the
	// caller that takes the flow back knows. It runs with the engine's
	// mutex held, so it must return quickly and must not call the engine.
	OnDone func(err error)
	// BeforeWait, when set, is called once, without the engine's mutex,
	// if Register has to wait — for a packet identifier, or for the
	// previous flow with the same Ref to retire — before it waits. A
	// caller that releases the flows it registers together, with one
	// marker (see [Link.QueueOrdered]), queues that marker here: those
	// flows' acknowledgements may be what frees an identifier.
	BeforeWait func()
}

// Register allocates a packet identifier for m, writes it into m.Packet
// and adds an outbound flow for it, in one critical section. While every
// identifier is in use it waits for one, until ctx ends
// ([ErrIDsExhausted]) or stop closes ([ErrStopped]; nil never does).
// While a flow with the same Ref is retiring — its producer has recorded
// its outcome and its record is being deleted — it waits for that too,
// so the store never holds two records for one Ref.
// The record is stored before the flow can be sent; if that fails the
// flow is removed, the identifier released, and a [*StoreError]
// returned. An error means the flow does not exist and m.Settle is never
// called; otherwise the flow completes through Done and Settle, also
// when the session fails meanwhile. The returned Link, when non-nil, is
// the live connection: the caller passes out.Seq() to its SendOrdered so
// the PUBLISH keeps its place relative to other writes.
func (e *Engine) Register(ctx context.Context, m Message, stop <-chan struct{}) (*Out, Link, error) {
	idAt, ok := wire.PublishPacketIDOffset(m.Packet)
	if !ok || m.QoS > 2 || m.Packet[0]>>1&0x03 != m.QoS {
		return nil, nil, fmt.Errorf("mqttv5: register outbound publish: want a PUBLISH encoded with QoS %d, got % .8x", m.QoS, m.Packet)
	}
	o := &Out{
		qos: m.QoS, packet: m.Packet, expiresAt: m.ExpiresAt,
		ref: m.Ref, settle: m.Settle, onDone: m.OnDone,
		phase: session.AwaitPuback,
		done:  make(chan struct{}),
	}
	if m.QoS == 2 {
		o.phase = session.AwaitPubrec
	}

	e.mu.Lock()
	for e.failure == nil {
		// A retiring flow with the same Ref releases its identifier once
		// its record is deleted. Waiting for that keeps one record per
		// Ref in the store: a restart never finds the old exchange beside
		// the new one and runs both.
		full, previous := e.ids.full(), e.retiringLocked(m.Ref)
		if !full && !previous {
			break
		}
		if before := m.BeforeWait; before != nil {
			m.BeforeWait = nil
			e.mu.Unlock()
			before()
			e.mu.Lock()
			continue
		}
		if err := e.awaitReleaseLocked(ctx, stop); err != nil {
			e.mu.Unlock()
			if full && !errors.Is(err, ErrStopped) {
				err = errors.Join(ErrIDsExhausted, err)
			}
			return nil, nil, err
		}
	}
	if e.failure != nil {
		defer e.mu.Unlock()
		return nil, nil, e.failure
	}
	o.id = e.ids.take(OwnerPublish)
	binary.BigEndian.PutUint16(o.packet[idAt:], o.id)
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
	if e.store == nil {
		link := e.link
		e.mu.Unlock()
		return o, link, nil
	}
	var putErr error
	r := o.record()
	// A fresh flow has no earlier write, so this Put is first in line
	// and runs below on the caller's goroutine, under its ctx.
	put := e.queueLocked(outKey(o.id), o, &job{
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
			e.ids.release(o.id, OwnerPublish)
			return nil
		},
	})
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
	o.onDone = nil
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
// without its outcome being recorded, settle gets that outcome, possibly
// before Adopt returns. The Settle it replaces is never called. A flow
// whose producer has recorded its outcome is over, though its record
// may still be being deleted: Adopt does not find it, and Register
// with its Ref waits for the deletion.
func (e *Engine) Adopt(ref []byte, settle SettleFunc) bool {
	e.mu.Lock()
	if o := e.refs[string(ref)]; o != nil {
		o.settle = settle
		e.mu.Unlock()
		return true
	}
	o := e.orphans[string(ref)]
	if o == nil || o.retiring() {
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
// when rc is 0x80 or above and becomes the publish's error. The caller
// sends what it makes ready, the send quota it frees included (see
// Pending).
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
}

// HandlePubrec applies a PUBREC with reason code rc. A refusal (refused
// non-nil, rc 0x80 or above) completes the flow; success moves it to
// AwaitPubcomp and queues the PUBREL, which waits until that phase is
// stored. A PUBREC for a flow already at AwaitPubcomp is answered with
// another PUBREL, and one for an unknown identifier with PUBREL 0x92.
// The caller sends what it makes ready (see Pending).
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
}

// HandlePubcomp applies a PUBCOMP, completing a flow at AwaitPubcomp.
// Reason 0x92 means the broker had already finished the exchange, so it
// is not an error. The caller sends what it makes ready (see Pending).
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
}

// finishLocked completes o with err. Its record and packet identifier
// go with retireLocked.
func (e *Engine) finishLocked(o *Out, err error) {
	delete(e.out, o.id)
	o.finished = true
	o.err = err
	o.wake(err)
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
		e.ids.release(o.id, OwnerPublish)
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

// settleLocked queues the job that gives o's outcome to its Settle and,
// once the producer has recorded it, deletes its record. When the
// producer could not, o stays an orphan, record and identifier kept;
// when Adopt gave o another Settle meanwhile, that one gets the outcome
// next. It returns the job to start, or nil.
func (e *Engine) settleLocked(o *Out) *job {
	o.settling = true
	settle, outcome, ref := o.settle, o.err, o.ref
	k := outKey(o.id)
	j := &job{
		op: "delete outbound",
		do: func(ctx context.Context) error {
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
				// No longer retiring: a Register waiting for it looks again.
				e.ids.wake()
			case failed:
				o.settle = nil
			default:
				if len(o.ref) > 0 && e.orphans[string(o.ref)] == o {
					delete(e.orphans, string(o.ref))
				}
				e.ids.release(o.id, OwnerPublish)
			}
			return nil
		},
	}
	o.settleJob = j
	if settle != nil {
		j.begin = func(finish func(error)) {
			settle(outcome, func(err error) {
				if err != nil && len(ref) > 0 {
					finish(&settleError{err})
					return
				}
				finish(nil)
			})
		}
	}
	return e.queueLocked(k, o, j)
}

// retiring reports whether o's producer has recorded its outcome, so o
// is over and only waits for its record to be deleted and its
// identifier released.
func (o *Out) retiring() bool {
	return o.settling && o.settleJob != nil && o.settleJob.begunOK.Load()
}

// retiringLocked reports whether the flow with Ref ref is retiring.
func (e *Engine) retiringLocked(ref []byte) bool {
	if len(ref) == 0 {
		return false
	}
	o := e.orphans[string(ref)]
	return o != nil && o.retiring()
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
