// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"context"
	"errors"
	"log/slog"
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

// settleError is a producer's Settle that failed. It is not a store
// failure: the flow's record is kept until the outcome is settled.
type settleError struct{ err error }

func (e *settleError) Error() string { return "settle: " + e.err.Error() }
func (e *settleError) Unwrap() error { return e.err }

// job is one store write for a flow and what follows from it.
type job struct {
	op string
	do func(ctx context.Context) error
	// after runs with e.mu held once do has returned, unless the session
	// failed in between. It returns the error that fails the session: nil
	// when do succeeded or after has dealt with its error. Without an
	// after, any error fails the session.
	after func(err error) error
	// goroutine keeps the job off the submitting goroutine even when the
	// store is in memory: it runs a producer's Settle.
	goroutine bool

	q     *writeQueue
	epoch uint64
}

// writeQueue orders one flow's store writes: each starts once the one
// before it has finished, so an older state never overwrites a newer
// one, also when the connection changes while a write is slow. A flow
// whose queue is empty is stored as it stands, and the packets that
// depend on its record may be sent.
type writeQueue struct{ jobs []*job }

func (q *writeQueue) idle() bool { return len(q.jobs) == 0 }

// queueLocked appends j to q. It returns j when j is first in line, for
// the caller to pass to start once it has released e.mu, and nil when
// the job before it will start it.
func (e *Engine) queueLocked(q *writeQueue, j *job) *job {
	j.q, j.epoch = q, e.epoch
	q.jobs = append(q.jobs, j)
	if e.writes == 0 {
		e.writesIdle = make(chan struct{})
	}
	e.writes++
	if len(q.jobs) == 1 {
		return j
	}
	return nil
}

// start runs jobs that queueLocked returned: on this goroutine when the
// store is in memory, otherwise each on its own.
func (e *Engine) start(jobs ...*job) {
	for _, j := range jobs {
		switch {
		case j == nil:
		case e.inline && !j.goroutine:
			e.run(j)
		default:
			go e.run(j)
		}
	}
}

// run performs j, then each job queued behind it in the same flow.
func (e *Engine) run(j *job) {
	for j != nil {
		err := j.do(e.bg)
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
		q := j.q
		q.jobs[0] = nil
		q.jobs = q.jobs[1:]
		var next *job
		if len(q.jobs) > 0 {
			next = q.jobs[0]
		} else {
			q.jobs = nil
		}
		e.writes--
		if e.writes == 0 {
			close(e.writesIdle)
		}
		var dropped []func(context.Context, error) error
		first := false
		if failure != nil {
			dropped, first = e.failLocked(failure)
		}
		l := e.link
		e.mu.Unlock()

		if first {
			e.afterFailure(failure, dropped)
		}
		if l != nil {
			l.Wake()
		}
		switch {
		case next == nil:
			j = nil
		case e.inline && !next.goroutine:
			j = next
		default:
			go e.run(next)
			j = nil
		}
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
func (e *Engine) failLocked(err error) (dropped []func(context.Context, error) error, first bool) {
	if e.failure != nil {
		return nil, false
	}
	e.failure = err
	e.epoch++
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
		close(o.done)
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
func (e *Engine) afterFailure(err error, dropped []func(context.Context, error) error) {
	if len(dropped) > 0 {
		go func() {
			for _, settle := range dropped {
				_ = settle(e.bg, err)
			}
		}()
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

// Drain waits until no store write is queued or running, or for ctx to
// end.
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
