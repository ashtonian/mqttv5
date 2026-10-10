// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"context"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

type inState uint8

const (
	inDelivered   inState = iota // handed to the application, not yet acked
	inAcked                      // acked; its PUBACK/PUBREC waits for earlier ones
	inAwaitPubrel                // QoS 2: PUBREC queued or sent, waiting for PUBREL
	inReleased                   // QoS 2: PUBREL received, PUBCOMP queued
)

// In is one inbound QoS 1/2 PUBLISH. The application acknowledges it
// through the handle rather than by packet identifier, so an Ack that
// arrives after the session was replaced cannot touch a new message
// that reuses the identifier.
type In struct {
	id    uint16
	qos   byte
	state inState
	dead  bool // its session is gone; Ack is a no-op
}

// Receive registers an inbound QoS 1/2 PUBLISH. deliver is false for a
// retransmission of a message already delivered: if it is a QoS 2
// message whose PUBREC was queued, the PUBREC is queued again for the
// caller to send (§4.3.3; see Pending). ErrReceiveMaximumExceeded means
// the broker broke the client's Receive Maximum; the caller must
// disconnect with reason 0x93. Nothing is delivered once the session has
// failed.
func (e *Engine) Receive(id uint16, qos byte) (in *In, deliver bool, err error) {
	e.mu.Lock()
	if e.failure != nil {
		e.mu.Unlock()
		return nil, false, nil
	}
	if x := e.in[id]; x != nil {
		if x.state == inAwaitPubrel && qos == 2 {
			e.pushCtrlLocked(wire.PUBREC, id, wire.ReasonSuccess, true, nil)
		}
		e.mu.Unlock()
		return nil, false, nil
	}
	if limit := int(e.cfg.ReceiveMaximum); limit > 0 && len(e.in) >= limit {
		e.mu.Unlock()
		return nil, false, ErrReceiveMaximumExceeded
	}
	in = &In{id: id, qos: qos}
	e.in[id] = in
	e.fifo = append(e.fifo, in)
	e.mu.Unlock()
	return in, true, nil
}

// Ack records that the application is done with in. Acknowledgements go
// to the broker in arrival order (§4.6): this queues the PUBACK / PUBREC
// of every consecutive acked message at the head of the arrival order,
// and wakes the writer. A QoS 2 message's PUBREC waits until its record
// is stored.
func (e *Engine) Ack(in *In) {
	if e.ack(in) {
		e.wake()
	}
}

// AckDeferred is Ack for a caller that sends what it made ready itself
// (see Pending), such as the read loop.
func (e *Engine) AckDeferred(in *In) { e.ack(in) }

// ack queues what acking in releases and reports whether anything was.
func (e *Engine) ack(in *In) bool {
	var jobs []*job
	e.mu.Lock()
	if in.dead || in.state != inDelivered {
		e.mu.Unlock()
		return false
	}
	in.state = inAcked
	n := 0
	for ; n < len(e.fifo) && e.fifo[n].state == inAcked; n++ {
		head := e.fifo[n]
		if head.qos == 1 {
			e.pushCtrlLocked(wire.PUBACK, head.id, wire.ReasonSuccess, false, head)
			continue
		}
		head.state = inAwaitPubrel
		e.pushCtrlLocked(wire.PUBREC, head.id, wire.ReasonSuccess, true, nil)
		if e.store != nil {
			k := inKey(head.id)
			jobs = append(jobs, e.queueLocked(k, head, &job{
				op: "put inbound",
				do: func(ctx context.Context) error {
					return e.store.Put(ctx, session.Record{Key: k, QoS: 2, Phase: session.AwaitPubrel})
				},
			}))
		}
	}
	if n > 0 {
		clear(e.fifo[:n])
		e.fifo = e.fifo[n:]
		if len(e.fifo) == 0 {
			e.fifo = e.fifo[:0:0]
		}
	}
	e.mu.Unlock()
	if n == 0 {
		return false
	}
	e.start(jobs...)
	return true
}

// HandlePubrel completes an inbound QoS 2 flow: its record is deleted,
// then the caller sends PUBCOMP (see Pending). A PUBREL for an
// identifier with no PUBREC sent is answered with PUBCOMP 0x92 (§4.3.3);
// one repeated while the PUBCOMP is still queued is answered by that
// PUBCOMP.
func (e *Engine) HandlePubrel(id uint16) {
	var j *job
	e.mu.Lock()
	if e.failure != nil {
		e.mu.Unlock()
		return
	}
	in := e.in[id]
	switch {
	case in != nil && in.state == inReleased:
	case in == nil || in.state != inAwaitPubrel:
		e.pushCtrlLocked(wire.PUBCOMP, id, wire.ReasonPacketIdentifierNotFound, false, nil)
	default:
		in.state = inReleased
		e.pushCtrlLocked(wire.PUBCOMP, id, wire.ReasonSuccess, true, in)
		if e.store != nil {
			k := inKey(id)
			j = e.queueLocked(k, in, &job{
				op: "delete inbound",
				do: func(ctx context.Context) error { return e.store.Delete(ctx, k) },
			})
		}
	}
	e.mu.Unlock()
	e.start(j)
}
