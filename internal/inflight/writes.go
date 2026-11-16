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
	// SettleFunc). With nil do follows; an error ends the job with it.
	// The record's later writes wait meanwhile.
	begin func(finish func(error))
	do    func(ctx context.Context) error
	// after runs with e.mu held once the job has ended, unless the
	// session failed in between. It returns the error that fails the
	// session: nil when the job succeeded or after has dealt with its
	// error. Without an after, any error fails the session.
	after func(err error) error

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

// start runs jobs that queueLocked returned, each on this goroutine
// when it may run here (see runsHere), otherwise on its own.
func (e *Engine) start(jobs ...*job) {
	for _, j := range jobs {
		switch {
		case j == nil:
		case e.runsHere(j):
			e.run(j)
		default:
			go e.run(j)
		}
	}
}

// runsHere reports whether j may start on whichever goroutine reaches
// it: a store write when the store is in memory, and a job that begins
// by handing something over.
func (e *Engine) runsHere(j *job) bool { return e.inline || j.begin != nil }

// run performs j, then each job queued behind it for the same record
// that may run here. A job with a begin goes on from whoever finishes
// it (see proceed).
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
		j = e.end(j, j.do(e.bg))
	}
}

// proceed goes on with j once its begin has finished with err: its do,
// unless err ends it — on this goroutine when the store is in memory,
// otherwise on one of its own — then the jobs queued behind it.
func (e *Engine) proceed(j *job, err error) {
	switch {
	case err != nil || j.do == nil:
		e.run(e.end(j, err))
	case e.inline:
		e.run(e.end(j, j.do(e.bg)))
	default:
		go func() { e.run(e.end(j, j.do(e.bg))) }()
	}
}

// end completes j with err and returns the job queued behind it when
// that may run on this goroutine; otherwise it starts that job on its
// own and returns nil.
func (e *Engine) end(j *job, err error) *job {
	if err != nil {
		e.logWriteError(j.op, err)
	}
	e.mu.Lock()
	var failure error
	if j.epoch == e.epoch {
		if j.after != nil {
			failure = j.after(err)
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
	switch {
	case next == nil:
		return nil
	case e.runsHere(next):
		return next
	default:
		go e.run(next)
		return nil
	}
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
