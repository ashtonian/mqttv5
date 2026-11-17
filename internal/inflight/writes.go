package inflight

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/ashtonian/mqttv5/session"
)

// ErrStoreFailed is matched by every [StoreError].
var ErrStoreFailed = errors.New("mqttv5: session store write failed")

// StoreError reports a session store write that failed.
type StoreError struct {
	Op  string // what was written, e.g. "put outbound"
	Err error  // the store's error
}

func (e *StoreError) Error() string {
	return "mqttv5: session store " + e.Op + " failed: " + e.Err.Error()
}

// Unwrap returns [ErrStoreFailed] and the store's error, so errors.Is
// matches either.
func (e *StoreError) Unwrap() []error { return []error{ErrStoreFailed, e.Err} }

// settleError is what a producer that could not record an outcome gave
// done. It is not a store failure: the flow's record is kept until the
// outcome is recorded.
type settleError struct{ err error }

func (e *settleError) Error() string { return "settle: " + e.err.Error() }
func (e *settleError) Unwrap() error { return e.err }

// job is one store write for a record and what follows from it.
type job struct {
	op string
	// begin, when set, starts the job, which goes on when something else
	// calls finish: a producer that has recorded an outcome (see
	// SettleFunc). With nil the change follows; an error ends the job
	// with it. The record's later writes wait meanwhile.
	begin func(finish func(error))
	// change is the write; nil writes nothing.
	change *session.Change
	// after runs with e.mu held once the job has ended, unless the
	// session failed in between. It returns, optionally, then: a call to
	// make once e.mu is released; and the error that fails the session —
	// nil when the job succeeded or after has dealt with its error.
	// Without an after, any error fails the session.
	after func(err error) (then func(), failure error)

	key   session.RecordKey
	owner any // the flow the write is for; see cancelLocked
	epoch uint64
	begun atomic.Bool // finish has been called; later calls do nothing
	// begunOK is set with begun when that first finish had no error: for
	// a settle, the producer has recorded the outcome.
	begunOK atomic.Bool
}

// queueLocked appends j to the write queue of the record it writes.
// Each record's writes run one at a time, in the order they were
// decided, so an older state never overwrites a newer one: not when the
// connection changes while a write is slow, and not when a new flow
// reuses the packet identifier of one whose writes are still running. A
// record whose queue is empty is stored as it stands; the packets that
// depend on it may be sent (see storedLocked).
//
// It returns j when j is first in line, for the caller to pass to start
// once it has released e.mu, and nil when the job before it will start
// it.
func (e *Engine) queueLocked(key session.RecordKey, owner any, j *job) *job {
	j.key, j.owner, j.epoch = key, owner, e.epoch
	q := append(e.queues[key], j)
	e.queues[key] = q
	if e.writes == 0 {
		e.writesIdle = make(chan struct{})
	}
	e.writes++
	if len(q) == 1 {
		return j
	}
	return nil
}

// storedLocked reports whether every write decided for key has
// finished.
func (e *Engine) storedLocked(key session.RecordKey) bool { return len(e.queues[key]) == 0 }

// cancelLocked drops the writes queued for key on behalf of owner that
// have not started, for a flow that is gone before they ran. It is
// called from the after of the write at the head of the queue.
func (e *Engine) cancelLocked(key session.RecordKey, owner any) {
	q := e.queues[key]
	kept := q[:1]
	for _, j := range q[1:] {
		if j.owner == owner {
			e.writes--
			continue
		}
		kept = append(kept, j)
	}
	clear(q[len(kept):])
	e.queues[key] = kept
}

func outKey(id uint16) session.RecordKey {
	return session.RecordKey{Dir: session.Outbound, PacketID: id}
}

func inKey(id uint16) session.RecordKey {
	return session.RecordKey{Dir: session.Inbound, PacketID: id}
}

// start runs the jobs queueLocked returned (see run).
func (e *Engine) start(jobs ...*job) {
	for _, j := range jobs {
		e.run(j)
	}
}

// run goes on with j, first in its record's queue, and then with the
// jobs queued behind it: a begin on this goroutine, and a change here
// when the store is in memory, otherwise on the store writer (see
// writeLoop). A job with a begin goes on from whoever finishes it.
func (e *Engine) run(j *job) {
	for j != nil {
		if j.begin != nil {
			j.begin(func(err error) {
				if j.begun.CompareAndSwap(false, true) {
					if err == nil {
						j.begunOK.Store(true)
					}
					e.proceed(j, err)
				}
			})
			return
		}
		j = e.write(j)
	}
}

// proceed goes on with j once its begin has finished with err: its
// change, unless err ends it, then the jobs queued behind it.
func (e *Engine) proceed(j *job, err error) {
	if err != nil {
		e.run(e.end(j, err))
		return
	}
	e.run(e.write(j))
}

// write makes j's change: on this goroutine when the store is in memory,
// returning the job queued behind it; otherwise it hands j to the store
// writer and returns nil.
func (e *Engine) write(j *job) *job {
	switch {
	case j.change == nil:
		return e.end(j, nil)
	case e.inline:
		return e.end(j, e.store.Apply(e.bg, *j.change))
	}
	e.mu.Lock()
	e.due = append(e.due, j)
	idle := !e.writing
	e.writing = true
	e.mu.Unlock()
	if idle {
		go e.writeLoop()
	}
	return nil
}

// Bounds on one Store.Apply, so a burst of flows registered at once
// becomes several transactions of bounded size: the first flows go out
// after the first commit rather than the last, and the store never
// holds a second copy of the whole burst's packets at once.
const (
	maxApplyChanges = 1024
	maxApplyBytes   = 4 << 20
)

// writeLoop is the store writer: it applies the changes that are due in
// one Store.Apply, up to maxApplyChanges and maxApplyBytes, then those
// that are due next, until none is left. One write of many changes costs
// a durable store about what one change does — a transaction, an fsync —
// so the flows that run at once share it.
func (e *Engine) writeLoop() {
	for {
		e.mu.Lock()
		if len(e.due) == 0 {
			e.writing = false
			e.mu.Unlock()
			return
		}
		batch := nextBatch(e.due)
		// Without spare capacity, the next append moves the rest to a new
		// array and the old one, which holds the taken jobs, is freed.
		e.due = e.due[len(batch):len(e.due):len(e.due)]
		e.mu.Unlock()
		errs := e.apply(batch)
		for i, j := range batch {
			e.run(e.end(j, errs[i]))
		}
	}
}

// nextBatch returns the jobs at the head of due that go in the next
// Apply: at most maxApplyChanges, and no more packet and Ref bytes than
// maxApplyBytes unless a single change has more on its own.
func nextBatch(due []*job) []*job {
	size := 0
	for i, j := range due {
		if i == maxApplyChanges {
			return due[:i]
		}
		if r, put := j.change.Record(); put {
			size += len(r.Packet) + len(r.Ref)
		}
		if size > maxApplyBytes && i > 0 {
			return due[:i]
		}
	}
	return due
}

// apply makes the changes of batch in one Store.Apply and returns each
// job's error. When that fails, Apply being atomic, none of the changes
// was made: each is applied again on its own, so only the jobs whose
// change fails get an error.
func (e *Engine) apply(batch []*job) []error {
	errs := make([]error, len(batch))
	changes := make([]session.Change, len(batch))
	for i, j := range batch {
		changes[i] = *j.change
	}
	err := e.store.Apply(e.bg, changes...)
	switch {
	case err == nil:
	case len(changes) == 1:
		errs[0] = err
	default:
		e.cfg.Logger.Warn("mqttv5: session store apply failed; applying each change on its own",
			slog.Int("changes", len(changes)), slog.Any("error", err))
		for i, c := range changes {
			errs[i] = e.store.Apply(e.bg, c)
		}
	}
	return errs
}

// end completes j with err and returns the job queued behind it, for the
// caller to run.
func (e *Engine) end(j *job, err error) *job {
	if err != nil {
		e.logWriteError(j.op, err)
	}
	e.mu.Lock()
	var (
		failure error
		then    func()
	)
	if j.epoch == e.epoch {
		if j.after != nil {
			then, failure = j.after(err)
		} else if err != nil {
			failure = &StoreError{Op: j.op, Err: err}
		}
	}
	q := e.queues[j.key]
	q[0] = nil
	q = q[1:]
	var next *job
	if len(q) > 0 {
		next = q[0]
		e.queues[j.key] = q
	} else {
		delete(e.queues, j.key)
	}
	var dropped []SettleFunc
	first := false
	if failure != nil {
		dropped, first = e.failLocked(failure)
	}
	// A write may have made a gated frame or a PUBLISH ready.
	var l Link
	if e.writableLocked() {
		l = e.link
	}
	e.mu.Unlock()

	if then != nil {
		then()
	}
	if first {
		e.afterFailure(failure, dropped)
	}
	// The write counts until its failure, if any, has been reported,
	// so Drain does not return before OnFailure has run.
	e.mu.Lock()
	e.writes--
	if e.writes == 0 {
		close(e.writesIdle)
	}
	e.mu.Unlock()
	if l != nil {
		l.Wake()
	}
	return next
}

func (e *Engine) logWriteError(op string, err error) {
	if se := (*settleError)(nil); errors.As(err, &se) {
		e.cfg.Logger.Warn("mqttv5: producer did not record a finished publish; keeping its session record",
			slog.String("op", op), slog.Any("error", se.err))
		return
	}
	e.storeError(op, err)
}

// failLocked ends the session after a store write failed: the store no
// longer holds what the session in memory says, so nothing more may be
// sent from it. The in-memory state is dropped as a crash would drop it,
// unfinished flows complete with err, and the engine refuses new flows
// until Restore reloads what the store holds. It returns the Settles of
// the dropped flows and whether this is the session's first failure.
func (e *Engine) failLocked(err error) (dropped []SettleFunc, first bool) {
	if e.failure != nil {
		return nil, false
	}
	e.failure = err
	e.epoch++
	e.ids.wake()
	for _, in := range e.in {
		in.dead = true
	}
	clear(e.in)
	clear(e.fifo)
	e.fifo = e.fifo[:0]
	clear(e.ctrl)
	e.ctrl = e.ctrl[:0]
	for _, o := range e.drainFlowsLocked() {
		o.finished = true
		o.err = err
		o.wake(err)
		if o.settle != nil {
			dropped = append(dropped, o.settle)
		}
	}
	for _, o := range e.orphans {
		// An adopter still waiting for the outcome; the record stays.
		if o.adopted || (!o.settling && o.settle != nil) {
			dropped = append(dropped, o.settle)
		}
	}
	clear(e.out)
	clear(e.refs)
	clear(e.orphans)
	e.cursor = nil
	return dropped, true
}

// afterFailure tells the producers of dropped flows and then the client.
// Called without e.mu.
func (e *Engine) afterFailure(err error, dropped []SettleFunc) {
	for _, settle := range dropped {
		settle(err, ignoreDone)
	}
	if e.cfg.OnFailure != nil {
		e.cfg.OnFailure(err)
	}
}

// Failure returns the store failure that ended the session, or nil.
func (e *Engine) Failure() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.failure
}

// Drain waits until no store write is queued or running, and the failure
// of any that failed has been reported to OnFailure, or for ctx to end.
func (e *Engine) Drain(ctx context.Context) error {
	e.mu.Lock()
	idle := e.writesIdle
	e.mu.Unlock()
	select {
	case <-idle:
		return nil
	default:
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
