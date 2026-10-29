// Package inflight is the client's MQTT v5 session state machine: packet
// identifier ownership, outbound QoS 1/2 flows, inbound QoS 1/2 flows and
// the acknowledgements owed to the broker.
//
// The engine owns what to send and in which order; the connection's
// writer pulls batches with [Engine.Collect]. Pulling, rather than having
// each producer write, is what keeps the protocol's ordering rules
// (§4.6) in one place:
//
//   - PUBLISH packets go out in sequence order, and a resumed session
//     resends them in that same order;
//   - PUBRELs go out in the order their PUBRECs arrived;
//   - PUBACK / PUBREC go out in the order the PUBLISHes arrived, however
//     the application orders its acknowledgements;
//   - nothing is sent before the record it depends on is in the Store;
//   - no more QoS > 0 PUBLISHes are in flight than the server's Receive
//     Maximum allows.
//
// Store writes that gate a packet run on their own goroutine when the
// store may block, so the read loop never waits on disk. One record's
// writes run in the order they were decided (see queueLocked). A write
// that fails ends the session: see [Config.OnFailure].
package inflight

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

// SessionLossPolicy decides what happens to unacknowledged outbound
// QoS 1/2 messages when a connection starts without the old session
// (Session Present = 0).
type SessionLossPolicy uint8

const (
	// Republish sends them again as new messages, in their original
	// order. Delivery stays at-least-once; QoS 2 may be duplicated.
	Republish SessionLossPolicy = iota
	// Fail completes them with ErrSessionLost.
	Fail
)

var (
	// ErrSessionLost completes outbound messages discarded because the
	// session was lost and the policy is Fail.
	ErrSessionLost = errors.New("mqttv5: session lost before the message was acknowledged")
	// ErrMessageExpired completes outbound messages whose Message Expiry
	// Interval ran out before they could be republished.
	ErrMessageExpired = errors.New("mqttv5: message expired before it could be sent again")
	// ErrReceiveMaximumExceeded is returned by Receive when the broker
	// sends more unacknowledged QoS > 0 PUBLISHes than the client allows.
	ErrReceiveMaximumExceeded = errors.New("mqttv5: broker exceeded the client's Receive Maximum")

	// ErrQoSNotSupported reports a QoS above the broker's Maximum QoS.
	ErrQoSNotSupported = errors.New("mqttv5: QoS above the broker's Maximum QoS")
	// ErrRetainNotSupported reports a retained message to a broker that
	// granted Retain Available = 0.
	ErrRetainNotSupported = errors.New("mqttv5: broker does not support retained messages")
	// ErrPacketTooLarge reports a packet above the broker's Maximum Packet
	// Size.
	ErrPacketTooLarge = errors.New("mqttv5: packet larger than the broker's Maximum Packet Size")
)

// Link is the engine's handle on the live connection.
type Link interface {
	// Wake asks the writer to Collect. It never blocks.
	Wake()
	// SendOrdered queues a marker in the connection's write order; when
	// the writer reaches it, flows up to seq may be sent. It blocks
	// while the write queue is full.
	SendOrdered(ctx context.Context, seq uint64) error
}

// Config configures an Engine.
type Config struct {
	// Store persists session state. Nil keeps state in memory only.
	Store session.Store

	// ReceiveMaximum is the limit the client advertised in CONNECT for
	// unacknowledged inbound QoS > 0 PUBLISHes; 0 means 65,535.
	ReceiveMaximum uint16

	// SessionLoss applies when a connection starts without the session.
	SessionLoss SessionLossPolicy

	Logger *slog.Logger
	Now    func() time.Time

	// OnStoreError observes failed store operations (logged as well).
	OnStoreError func(op string, err error)
	// OnFailure is called once when a store write fails, with a
	// [*StoreError], from the goroutine that ran the write. By then the
	// engine has dropped its session state, as a crash would: unfinished
	// outbound flows have completed with the error, nothing more is
	// collected, and new flows are refused until Restore reloads what
	// the store holds. Drain waits for it to return, so it must not block
	// or call Drain.
	OnFailure func(error)
	// OnStrayAck observes acknowledgements for unknown or mismatched
	// packet identifiers, which are ignored.
	OnStrayAck func(t wire.PacketType, id uint16)
	// OnReplay observes each PUBLISH resent with DUP=1.
	OnReplay func()
	// Refusal builds the error a broker's refusing PUBACK or PUBREC with
	// reason code rc becomes, for an outcome restored from the store.
	// Nil gives a plain error naming the reason code.
	Refusal func(t wire.PacketType, rc wire.ReasonCode) error
}

// ConnInfo describes a connection that has just completed CONNACK.
type ConnInfo struct {
	SessionPresent bool
	// ReceiveMaximum is the server's limit from CONNACK; 0 means 65,535.
	ReceiveMaximum uint16
	// Limits, when non-nil, are the broker's packet limits. Unsent
	// messages that break them are failed rather than sent.
	Limits *Limits
	Link   Link
	// HoldRepublished keeps the PUBLISHes a lost session sends again
	// from the writer until a Collect or SendOrdered marker releases
	// them, so the client can re-issue its subscriptions ahead of them.
	// It has no effect when SessionPresent is set.
	HoldRepublished bool
}

// Limits are the CONNACK properties that rule out a PUBLISH.
type Limits struct {
	MaximumQoS        byte
	RetainAvailable   bool
	MaximumPacketSize uint32 // 0: no limit
	// Downgrade sends a QoS 2 message as QoS 1 on a broker whose
	// Maximum QoS is 1, instead of failing it. Only applied to messages
	// sent again as new after a session loss.
	Downgrade bool
}

// Engine holds one client's session state. All methods are safe for
// concurrent use.
type Engine struct {
	cfg    Config
	store  session.Store
	inline bool // store calls are cheap enough to make on any goroutine

	mu sync.Mutex

	ids idTable

	out      map[uint16]*Out
	pub      flowList // AwaitPuback / AwaitPubrec, by sequence
	rel      flowList // AwaitPubcomp, by sequence (PUBREC order)
	nextSeq  uint64
	cursor   *Out   // first flow in pub not yet sent on this connection
	released uint64 // flows with seq ≤ released may be sent
	quota    int
	quotaMax int

	in   map[uint16]*In
	fifo []*In // inbound flows in arrival order, until their ack is queued

	ctrl []ctrlFrame

	link    Link
	gen     uint64 // current connection; 0 when disconnected
	lastGen uint64

	meta session.Meta

	refs map[string]*Out // unfinished flows with a Ref
	// orphans are finished flows with a Ref whose producer has not
	// recorded the outcome: one restored without a Settle, or whose
	// Settle failed. Each keeps its record and identifier until Adopt
	// settles it, so a restart continues the exchange instead of the
	// producer publishing the message again.
	orphans map[string]*Out

	// queues holds each record's store writes, queued and running (the
	// head); writes counts them all and writesIdle is closed while it is
	// zero.
	queues     map[session.RecordKey][]*job
	writes     int
	writesIdle chan struct{}
	// epoch changes when the session fails; a write from an earlier
	// epoch no longer touches the state in memory.
	epoch   uint64
	failure error

	bg     context.Context
	cancel context.CancelFunc
}

// ctrlFrame is an acknowledgement or PUBREL waiting to be written.
// Frames leave in order; one whose record still has store writes
// pending holds back the frames behind it.
type ctrlFrame struct {
	typ wire.PacketType
	id  uint16
	rc  wire.ReasonCode
	// gated frames wait until their record is stored: a PUBREL for the
	// outbound AwaitPubcomp record, a PUBREC for the inbound AwaitPubrel
	// record and a PUBCOMP for its deletion.
	gated bool
	// in is the inbound flow a PUBACK or PUBCOMP completes. The flow
	// stays registered until that frame is written, so a retransmission
	// that arrives first is recognised as a duplicate.
	in *In
}

// readyLocked reports whether f may be written.
func (e *Engine) readyLocked(f *ctrlFrame) bool {
	if !f.gated {
		return true
	}
	if f.typ == wire.PUBREL {
		return e.storedLocked(outKey(f.id))
	}
	return e.storedLocked(inKey(f.id))
}

// New returns an engine with no state.
func New(cfg Config) *Engine {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Refusal == nil {
		cfg.Refusal = func(t wire.PacketType, rc wire.ReasonCode) error {
			return fmt.Errorf("mqttv5: %s refused with reason code %#x", t, byte(rc))
		}
	}
	_, mem := cfg.Store.(*session.MemoryStore)
	bg, cancel := context.WithCancel(context.Background())
	idle := make(chan struct{})
	close(idle)
	return &Engine{
		cfg:        cfg,
		store:      cfg.Store,
		inline:     cfg.Store == nil || mem,
		ids:        newIDTable(65535),
		out:        make(map[uint16]*Out),
		in:         make(map[uint16]*In),
		refs:       make(map[string]*Out),
		orphans:    make(map[string]*Out),
		queues:     make(map[session.RecordKey][]*job),
		writesIdle: idle,
		nextSeq:    1,
		quota:      65535,
		quotaMax:   65535,
		bg:         bg,
		cancel:     cancel,
	}
}

// Restore loads persisted state, replacing what the engine holds. Call
// it before the first connection, and again after the session failed
// (see [Config.OnFailure]): it waits for the store writes still running,
// until ctx ends, then reloads what the store holds. Records that cannot
// be restored are logged and skipped.
func (e *Engine) Restore(ctx context.Context) error {
	if err := e.Drain(ctx); err != nil {
		return err
	}
	var (
		meta session.Meta
		recs []session.Record
	)
	if e.store != nil {
		var err error
		if meta, recs, err = e.store.Load(ctx); err != nil {
			return err
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.out) > 0 || len(e.in) > 0 {
		return errors.New("mqttv5: Restore over a live session")
	}
	e.failure = nil
	e.ids = newIDTable(e.ids.max)
	clear(e.orphans)
	e.meta = meta
	var rel []*Out
	for _, r := range recs {
		if !r.Phase.Valid(r.Key.Dir) || (r.QoS != 1 && r.QoS != 2) {
			e.cfg.Logger.Warn("mqttv5: skipping invalid stored session record",
				slog.Int("dir", int(r.Key.Dir)), slog.Int("packet_id", int(r.Key.PacketID)), slog.Int("phase", int(r.Phase)))
			continue
		}
		switch {
		case r.Key.Dir == session.Outbound && r.Phase == session.Completed:
			// Ended; its producer has yet to record the outcome.
			o := &Out{
				id: r.Key.PacketID, qos: r.QoS, phase: r.Phase, seq: r.Seq, pubSeq: r.Seq,
				expiresAt: r.ExpiresAt, ref: r.Ref, outcome: r.Outcome,
				finished: true, done: make(chan struct{}),
			}
			if !e.decodeOutcomeLocked(o) || len(r.Ref) == 0 || !e.ids.claim(r.Key.PacketID, OwnerPublish) {
				e.cfg.Logger.Warn("mqttv5: skipping unusable stored outbound record",
					slog.Int("packet_id", int(r.Key.PacketID)), slog.Int("outcome", int(r.Outcome)))
				continue
			}
			close(o.done)
			e.orphans[string(o.ref)] = o
			e.nextSeq = max(e.nextSeq, r.Seq+1)
		case r.Key.Dir == session.Outbound:
			if len(r.Packet) == 0 || !e.ids.claim(r.Key.PacketID, OwnerPublish) {
				e.cfg.Logger.Warn("mqttv5: skipping unusable stored outbound record",
					slog.Int("packet_id", int(r.Key.PacketID)))
				continue
			}
			o := &Out{
				id: r.Key.PacketID, qos: r.QoS, phase: r.Phase, seq: r.Seq, pubSeq: r.Seq,
				packet: r.Packet, expiresAt: r.ExpiresAt, ref: r.Ref,
				attempted: true, done: make(chan struct{}),
			}
			e.out[o.id] = o
			if len(o.ref) > 0 {
				e.refs[string(o.ref)] = o
			}
			if o.phase == session.AwaitPubcomp {
				o.seq = r.PubrecSeq
				rel = append(rel, o)
			} else {
				e.pub.push(o)
			}
			e.nextSeq = max(e.nextSeq, r.Seq+1, r.PubrecSeq+1)
		case r.Key.Dir == session.Inbound:
			e.in[r.Key.PacketID] = &In{id: r.Key.PacketID, qos: 2, state: inAwaitPubrel}
		}
	}
	// PUBRELs are resent in the order their PUBRECs arrived (§4.6).
	slices.SortFunc(rel, func(a, b *Out) int { return cmp.Compare(a.seq, b.seq) })
	for _, o := range rel {
		e.rel.push(o)
	}
	return nil
}

// HasState reports whether there is session state worth resuming.
func (e *Engine) HasState() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.out) > 0 || len(e.in) > 0 || len(e.orphans) > 0
}

// Meta returns the session meta.
func (e *Engine) Meta() session.Meta {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.meta
}

// SetMeta records m, persisting it when it changed. A failed write ends
// the session like any other (see [Config.OnFailure]) and is returned.
func (e *Engine) SetMeta(ctx context.Context, m session.Meta) error {
	e.mu.Lock()
	if e.failure != nil {
		defer e.mu.Unlock()
		return e.failure
	}
	changed := e.meta != m
	if e.store == nil {
		e.meta = m
	}
	e.mu.Unlock()
	if !changed || e.store == nil {
		return nil
	}
	err := e.store.SetMeta(ctx, m)
	e.mu.Lock()
	if err == nil {
		e.meta = m
		e.mu.Unlock()
		return nil
	}
	e.storeError("set meta", err)
	failure := &StoreError{Op: "set meta", Err: err}
	dropped, first := e.failLocked(failure)
	e.mu.Unlock()
	if first {
		e.afterFailure(failure, dropped)
	}
	return failure
}

// SendQuota reports how many more QoS 1/2 PUBLISHes the current
// connection allows in flight, and the connection's limit.
func (e *Engine) SendQuota() (available, limit int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.quota, e.quotaMax
}

// OutboundLen reports the number of outbound flows not yet complete.
func (e *Engine) OutboundLen() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.out)
}

// Connected applies a new connection's CONNACK to the session and binds
// the engine to the connection. With Session Present = 0 inbound state
// is discarded and unacknowledged outbound messages are handled per the
// SessionLoss policy; with Session Present = 1 everything unacknowledged
// is resent. A flow whose record it changes is sent once the new record
// is stored. It returns the connection's generation, which its writer
// passes to Collect, and, when ci.HoldRepublished kept the republished
// PUBLISHes back, the sequence number that releases them all (0 when
// none are held). It fails only when the session has failed and not
// been restored.
func (e *Engine) Connected(ci ConnInfo) (gen, held uint64, err error) {
	var jobs []*job

	e.mu.Lock()
	if e.failure != nil {
		defer e.mu.Unlock()
		return 0, 0, e.failure
	}
	e.lastGen++
	e.gen = e.lastGen
	e.link = ci.Link
	e.quotaMax = int(ci.ReceiveMaximum)
	if e.quotaMax == 0 {
		e.quotaMax = 65535
	}
	e.quota = e.quotaMax
	for _, o := range e.out {
		o.counted = false
	}

	if ci.SessionPresent {
		e.resumeLocked()
	} else {
		jobs = e.sessionLostLocked()
	}
	if ci.Limits != nil {
		jobs = append(jobs, e.enforceLimitsLocked(*ci.Limits, !ci.SessionPresent)...)
	}
	e.cursor = e.pub.head
	e.released = e.nextSeq - 1
	if !ci.SessionPresent && ci.HoldRepublished && e.pub.head != nil {
		// Every flow is in pub as new and none is sent before the
		// marker; flows registered from now on wait for their own.
		held = e.released
		e.released = 0
	}
	gen = e.gen
	e.mu.Unlock()

	e.start(jobs...)
	ci.Link.Wake()
	return gen, held, nil
}

// resumeLocked prepares a resumed session: PUBRELs are regenerated in
// PUBREC order (pending ones from the old connection are dropped so none
// is sent twice), each still waiting for its flow's record, and inbound
// acknowledgements still owed stay queued.
func (e *Engine) resumeLocked() {
	kept := e.ctrl[:0]
	for _, f := range e.ctrl {
		if f.typ != wire.PUBREL {
			kept = append(kept, f)
		}
	}
	clear(e.ctrl[len(kept):])
	e.ctrl = kept
	for o := e.rel.head; o != nil; o = o.next {
		e.pushCtrlLocked(wire.PUBREL, o.id, wire.ReasonSuccess, true, nil)
	}
}

// sessionLostLocked discards inbound state and applies the session-loss
// policy to outbound flows. It returns the store writes to start.
func (e *Engine) sessionLostLocked() []*job {
	var jobs []*job
	for id, in := range e.in {
		in.dead = true
		if in.state == inAwaitPubrel && e.store != nil {
			k := inKey(id)
			jobs = append(jobs, e.queueLocked(k, in, &job{
				op: "delete inbound", do: func(ctx context.Context) error { return e.store.Delete(ctx, k) },
			}))
		}
	}
	clear(e.in)
	clear(e.fifo)
	e.fifo = e.fifo[:0]
	clear(e.ctrl)
	e.ctrl = e.ctrl[:0]

	flows := e.drainFlowsLocked()
	now := e.cfg.Now()
	for _, o := range flows {
		var reason error
		switch {
		case e.cfg.SessionLoss == Fail:
			reason = ErrSessionLost
		case !o.expiresAt.IsZero() && !now.Before(o.expiresAt):
			reason = ErrMessageExpired
		}
		if reason != nil {
			e.finishLocked(o, reason)
			jobs = append(jobs, e.retireLocked(o))
			continue
		}
		o.phase = session.AwaitPuback
		if o.qos == 2 {
			o.phase = session.AwaitPubrec
		}
		o.seq = o.pubSeq
		o.attempted = false
		if !o.expiresAt.IsZero() {
			o.expiry = secondsUntil(o.expiresAt, now)
			o.packet = wire.WithMessageExpiry(o.packet, o.expiry)
		}
		e.pub.push(o)
		jobs = append(jobs, e.putOutLocked(o))
	}
	return jobs
}

// secondsUntil is the time from now to t in whole seconds, rounded up.
func secondsUntil(t, now time.Time) uint32 {
	return uint32((t.Sub(now) + time.Second - 1) / time.Second)
}

// enforceLimitsLocked fails, or downgrades, PUBLISHes waiting to be sent
// that the connection's limits rule out. A PUBLISH already sent in the
// resumed session is the broker's to judge; only fresh sends are checked
// when the session survived. It returns the store writes to start.
func (e *Engine) enforceLimitsLocked(l Limits, fresh bool) []*job {
	var jobs []*job
	for o := e.pub.head; o != nil; {
		next := o.next
		if !fresh && o.attempted {
			o = next
			continue
		}
		var reason error
		switch {
		case l.MaximumPacketSize > 0 && uint32(len(o.packet)) > l.MaximumPacketSize:
			reason = ErrPacketTooLarge
		case o.packet[0]&0x01 != 0 && !l.RetainAvailable:
			reason = ErrRetainNotSupported
		case o.qos > l.MaximumQoS && l.Downgrade && l.MaximumQoS == 1:
			o.packet = append([]byte(nil), o.packet...)
			o.packet[0] = o.packet[0]&^0x06 | 1<<1
			o.qos = 1
			o.phase = session.AwaitPuback
			jobs = append(jobs, e.putOutLocked(o))
		case o.qos > l.MaximumQoS:
			reason = ErrQoSNotSupported
		}
		if reason != nil {
			e.unlinkLocked(o)
			e.finishLocked(o, reason)
			jobs = append(jobs, e.retireLocked(o))
		}
		o = next
	}
	return jobs
}

// drainFlowsLocked removes every outbound flow from both lists and
// returns them in original send order.
func (e *Engine) drainFlowsLocked() []*Out {
	flows := make([]*Out, 0, e.pub.len+e.rel.len)
	for o := e.pub.head; o != nil; o = o.next {
		flows = append(flows, o)
	}
	for o := e.rel.head; o != nil; o = o.next {
		flows = append(flows, o)
	}
	for _, o := range flows {
		o.list.remove(o)
	}
	sortByPubSeq(flows)
	return flows
}

// Discard resets the store and drops all session state, completing
// unfinished outbound messages with ErrSessionLost. Call it before a
// connection that deliberately starts a new session (an explicit
// CleanStart=1) so stored state is not republished. It first waits for
// the store writes still running, until ctx ends. When the reset fails
// nothing is dropped.
func (e *Engine) Discard(ctx context.Context) error {
	if err := e.Drain(ctx); err != nil {
		return err
	}
	if e.store != nil {
		if err := e.store.Reset(ctx); err != nil {
			e.storeError("reset", err)
			return &StoreError{Op: "reset", Err: err}
		}
	}
	e.mu.Lock()
	for _, in := range e.in {
		in.dead = true
	}
	clear(e.in)
	clear(e.fifo)
	e.fifo = e.fifo[:0]
	clear(e.ctrl)
	e.ctrl = e.ctrl[:0]
	var settles []func(context.Context, error) error
	for _, o := range e.drainFlowsLocked() {
		e.finishLocked(o, ErrSessionLost)
		e.releaseLocked(o.id, OwnerPublish)
		if o.settle != nil {
			settles = append(settles, o.settle)
		}
	}
	for ref, o := range e.orphans {
		e.releaseLocked(o.id, OwnerPublish)
		delete(e.orphans, ref)
		if o.settle != nil {
			settles = append(settles, o.settle)
		}
	}
	e.cursor = nil
	e.meta = session.Meta{}
	e.mu.Unlock()
	// The store no longer holds these flows, so a producer's failure to
	// record the outcome changes nothing here: it publishes the message
	// again as new.
	for _, settle := range settles {
		_ = settle(ctx, ErrSessionLost)
	}
	return nil
}

// Disconnected unbinds the engine from l's connection. State is kept
// for the next connection.
func (e *Engine) Disconnected(l Link) {
	e.mu.Lock()
	if e.link == l {
		e.link = nil
		e.gen = 0
	}
	e.mu.Unlock()
}

// Collect fills f with the next packets to write on connection gen:
// ready acknowledgements and PUBRELs first, then PUBLISHes in sequence
// order up to upTo (0 leaves the limit unchanged), as far as the send
// quota allows. It returns the number of packets added. A writer for a
// connection the engine is no longer bound to, or for a failed session,
// gets nothing.
//
// A message about to be sent for the first time has its Message Expiry
// Interval brought down to the time it has left; one whose time ran out
// while it waited is completed with ErrMessageExpired instead.
func (e *Engine) Collect(gen, upTo uint64, f *Frames) int {
	var jobs []*job
	e.mu.Lock()
	if !e.servesLocked(gen) {
		e.mu.Unlock()
		return 0
	}
	if upTo > e.released {
		e.released = upTo
	}
	n := e.collectControlLocked(f)
	sent := 0
	var now time.Time
	for o := e.cursor; o != nil && sent < maxPublishesPerCollect; o = e.cursor {
		if o.seq > e.released || !e.storedLocked(outKey(o.id)) || e.quota == 0 {
			break
		}
		if !o.attempted && !o.expiresAt.IsZero() {
			if now.IsZero() {
				now = e.cfg.Now()
			}
			if !now.Before(o.expiresAt) {
				e.unlinkLocked(o)
				e.finishLocked(o, ErrMessageExpired)
				jobs = append(jobs, e.retireLocked(o))
				continue
			}
			if left := secondsUntil(o.expiresAt, now); left < o.expiry {
				o.expiry = left
				o.packet = wire.WithMessageExpiry(o.packet, left)
				if e.store != nil {
					// Sent once the record holds the packet as sent.
					jobs = append(jobs, e.putOutLocked(o))
					break
				}
			}
		}
		f.appendPublish(o.packet, o.attempted)
		if o.attempted && e.cfg.OnReplay != nil {
			e.cfg.OnReplay()
		}
		o.attempted = true
		o.counted = true
		e.quota--
		e.cursor = o.next
		sent++
	}
	e.mu.Unlock()
	e.start(jobs...)
	return n + sent
}

// CollectControl is Collect for the ready acknowledgements and PUBRELs
// alone.
func (e *Engine) CollectControl(gen uint64, f *Frames) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.servesLocked(gen) {
		return 0
	}
	return e.collectControlLocked(f)
}

// servesLocked reports whether a writer for connection gen may collect.
func (e *Engine) servesLocked(gen uint64) bool {
	return gen != 0 && gen == e.gen && e.failure == nil
}

func (e *Engine) collectControlLocked(f *Frames) int {
	n := 0
	for e.controlReadyLocked() {
		c := e.ctrl[0]
		f.appendAck(c.typ, c.id, c.rc)
		if (c.typ == wire.PUBACK || c.typ == wire.PUBCOMP) && c.in != nil && e.in[c.id] == c.in {
			delete(e.in, c.id)
		}
		e.ctrl[0] = ctrlFrame{}
		e.ctrl = e.ctrl[1:]
		n++
	}
	if len(e.ctrl) == 0 {
		e.ctrl = e.ctrl[:0:0]
	}
	return n
}

// pushCtrlLocked queues an acknowledgement or PUBREL; see ctrlFrame for
// gated and in.
func (e *Engine) pushCtrlLocked(t wire.PacketType, id uint16, rc wire.ReasonCode, gated bool, in *In) {
	e.ctrl = append(e.ctrl, ctrlFrame{typ: t, id: id, rc: rc, gated: gated, in: in})
}

// wake tells the writer to collect, when there is something to collect:
// waking it for nothing costs two goroutine switches.
func (e *Engine) wake() {
	e.mu.Lock()
	var l Link
	if e.writableLocked() {
		l = e.link
	}
	e.mu.Unlock()
	if l != nil {
		l.Wake()
	}
}

// Pending reports what Collect would give now: control is a ready
// acknowledgement or PUBREL, publishes a PUBLISH released to the writer,
// stored, and within the send quota. The read loop, which handles the
// broker's acknowledgements and its own acks without waking the writer,
// checks it before it waits for more input.
func (e *Engine) Pending() (control, publishes bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.controlReadyLocked(), e.publishReadyLocked()
}

func (e *Engine) writableLocked() bool {
	return e.controlReadyLocked() || e.publishReadyLocked()
}

func (e *Engine) controlReadyLocked() bool {
	return len(e.ctrl) > 0 && e.readyLocked(&e.ctrl[0])
}

func (e *Engine) publishReadyLocked() bool {
	o := e.cursor
	return o != nil && e.quota > 0 && o.seq <= e.released && e.storedLocked(outKey(o.id))
}

func (e *Engine) storeError(op string, err error) {
	e.cfg.Logger.Error("mqttv5: session store "+op+" failed", slog.Any("error", err))
	if e.cfg.OnStoreError != nil {
		e.cfg.OnStoreError(op, err)
	}
}

func (e *Engine) stray(t wire.PacketType, id uint16) {
	e.cfg.Logger.Warn("mqttv5: ignoring acknowledgement for unknown packet identifier",
		slog.String("type", t.String()), slog.Int("packet_id", int(id)))
	if e.cfg.OnStrayAck != nil {
		e.cfg.OnStrayAck(t, id)
	}
}

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

// maxPublishesPerCollect bounds one Collect so a single writev stays
// well under the platform iovec limit and a large backlog is written in
// slices rather than one unbounded batch.
const maxPublishesPerCollect = 64

// Frames is one batch of packets for the writer: acknowledgements and
// PUBRELs encoded into a small buffer, and stored PUBLISH packets
// referenced without copying. It is owned by one writer goroutine and
// reused across batches.
type Frames struct {
	small []byte    // acks and DUP-flagged PUBLISH first bytes
	segs  []segment // write order
	out   net.Buffers
	n     int // packets in the batch
}

// segment is either a range of small or a stored packet.
type segment struct {
	off, end int
	pkt      []byte
}

// Reset empties the batch, dropping references to stored packets.
func (f *Frames) Reset() {
	f.small = f.small[:0]
	clear(f.segs)
	f.segs = f.segs[:0]
	clear(f.out)
	f.out = f.out[:0]
	f.n = 0
}

// Len reports how many packets the batch holds.
func (f *Frames) Len() int { return f.n }

func (f *Frames) appendAck(t wire.PacketType, id uint16, rc wire.ReasonCode) {
	off := len(f.small)
	f.small = wire.AppendPubResp(f.small, t, id, rc)
	f.extendSmall(off)
	f.n++
}

// appendPublish adds a stored PUBLISH. A retransmission gets DUP=1 by
// writing a modified first byte from small followed by the rest of the
// stored packet, so the stored DUP=0 bytes are never mutated.
func (f *Frames) appendPublish(pkt []byte, dup bool) {
	if dup {
		off := len(f.small)
		f.small = append(f.small, pkt[0]|0x08)
		f.extendSmall(off)
		pkt = pkt[1:]
	}
	f.segs = append(f.segs, segment{pkt: pkt})
	f.n++
}

// extendSmall records small[off:] as the next segment, merging it into
// the previous segment when that one ends where this starts.
func (f *Frames) extendSmall(off int) {
	if last := len(f.segs) - 1; last >= 0 && f.segs[last].pkt == nil && f.segs[last].end == off {
		f.segs[last].end = len(f.small)
		return
	}
	f.segs = append(f.segs, segment{off: off, end: len(f.small)})
}

// WriteTo writes the batch with a single Write or writev.
func (f *Frames) WriteTo(w io.Writer) (int64, error) {
	if len(f.segs) == 1 {
		n, err := w.Write(f.bytes(f.segs[0]))
		return int64(n), err
	}
	bufs := f.Buffers()
	return bufs.WriteTo(w)
}

// Buffers returns the batch as one buffer per segment, for one writev.
// They belong to the batch and stay valid until Reset; writing them with
// net.Buffers.WriteTo consumes the returned slice, which then holds what
// is left unwritten.
func (f *Frames) Buffers() net.Buffers {
	f.out = f.out[:0]
	for _, s := range f.segs {
		f.out = append(f.out, f.bytes(s))
	}
	return f.out
}

func (f *Frames) bytes(s segment) []byte {
	if s.pkt != nil {
		return s.pkt
	}
	return f.small[s.off:s.end]
}
