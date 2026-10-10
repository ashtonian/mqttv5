// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"io"
	"net"

	"github.com/ashtonian/mqttv5/wire"
)

// writeFn produces wire bytes into w. Closures over the caller's
// per-call opts. The writer goroutine invokes them against the transport.
type writeFn func(io.Writer) (int64, error)

// writeBytes returns a writeFn that writes b as is.
func writeBytes(b []byte) writeFn {
	return func(w io.Writer) (int64, error) {
		n, err := w.Write(b)
		return int64(n), err
	}
}

// writeReq is a unit of work for the writer goroutine. Exactly one of
// fn, pkt or mark is set.
//
// The pkt path carries a QoS 0 PUBLISH the caller does not write itself
// (see acquireIdle): the packet is pre-encoded into a pooled buffer
// (wire.EncodePublish) and the writer writes it, then releases the
// buffer.
//
// The fn path is used for SUBSCRIBE, UNSUBSCRIBE, AUTH, PINGREQ and
// DISCONNECT, whose encoding happens on the writer.
//
// A mark lets the writer send QoS 1/2 PUBLISH packets held by the
// session engine up to that sequence number. Queuing the mark, rather
// than writing the packet directly, keeps a QoS 1/2 publish behind any
// write its caller queued before it.
type writeReq struct {
	fn   writeFn
	pkt  *[]byte // pre-encoded packet; writer releases after Write
	mark uint64
	done chan<- error // nil for fire-and-forget
}

// writeBatchHardCap caps any user-supplied WithWriteBatch(n) to avoid
// runaway iovec sizes. 64 is more than enough for any realistic
// concurrent-publisher workload (writev iovec limit is system-defined
// but typically 1024; we stay well below).
const writeBatchHardCap = 64

// writeBatch accumulates pre-encoded packets to coalesce into one
// net.Buffers.WriteTo (= one writev syscall on *net.TCPConn). Lives
// on the writer goroutine's stack frame and reuses the same backing
// arrays across batches — zero alloc per batch. cap is set at
// construction time from Config.WriteBatchMax (clamped to
// writeBatchHardCap).
type writeBatch struct {
	pkts  []*[]byte
	dones []chan<- error
	bufs  net.Buffers
	n     int
	cap   int
}

// newWriteBatch sizes a batch for cap packets. cap=1 disables
// coalescing (every flush goes straight to conn.Write).
func newWriteBatch(cap int) writeBatch {
	if cap < 1 {
		cap = 1
	} else if cap > writeBatchHardCap {
		cap = writeBatchHardCap
	}
	return writeBatch{
		pkts:  make([]*[]byte, cap),
		dones: make([]chan<- error, cap),
		cap:   cap,
	}
}

// add appends a pkt-mode req to the batch. Caller guarantees req.pkt
// is non-nil and n < writeBatchMax.
func (b *writeBatch) add(req writeReq) {
	b.pkts[b.n] = req.pkt
	b.dones[b.n] = req.done
	b.n++
}

// flush coalesces every queued pkt into one writev. Updates
// lastWriteUnixNano on success, releases every buffer, and signals
// every done channel (with err for failures, nil for success). Resets
// the batch to empty.
//
// A batch of one is a plain conn.Write.
func (b *writeBatch) flush(cs *connState) error {
	if b.n == 0 {
		return nil
	}
	var err error
	if b.n == 1 {
		_, err = cs.conn.Write(*b.pkts[0])
	} else {
		full := b.bufs[:0]
		for i := 0; i < b.n; i++ {
			full = append(full, *b.pkts[i])
		}
		// WriteTo advances the slice it writes from; keep the full one so
		// the next batch reuses its array.
		b.bufs = full
		_, err = b.bufs.WriteTo(cs.conn)
		clear(full)
		b.bufs = full[:0]
	}
	if err == nil {
		cs.lastWriteUnixNano.Store(cs.clk.Now().UnixNano())
	}
	// Uncounted before the callers hear back, so a caller's next publish
	// can be written directly.
	cs.queued.Add(-int64(b.n))
	for i := 0; i < b.n; i++ {
		wire.ReleaseBuf(b.pkts[i])
		if b.dones[i] != nil {
			b.dones[i] <- err
		}
		b.pkts[i] = nil
		b.dones[i] = nil
	}
	b.n = 0
	return err
}

// writeLoop drains the write queue for one connection and sends what
// the session engine has ready, holding cs.wmu while it writes so a
// direct write (acquireIdle) never interleaves with it; the tail of a
// direct write that ctx interrupted goes first. Exits on shutdown,
// dying, or write error. On any exit path it closes the queue and
// answers the requests left in it with ErrNotConnected, so callers
// waiting on req.done never orphan.
//
// Batching: pkt-mode reqs coalesce into one writev when WithWriteBatch
// is set; engine packets (QoS 1/2 PUBLISH, PUBREL and the PUBACK /
// PUBREC / PUBCOMP owed to the broker) always go out as one batch per
// collection. Channel FIFO order is preserved across all request kinds.
func (c *Client) writeLoop(cs *connState) {
	defer cs.wg.Done()
	defer close(cs.writerDone)
	defer drainWriteQueue(cs)
	batch := newWriteBatch(c.cfg.WriteBatchMax)
	for {
		var err error
		select {
		case req, ok := <-cs.writeQueue:
			if !ok {
				return
			}
			cs.wmu.Lock()
			if err = cs.writeRest(); err == nil {
				err = c.processWrite(cs, &batch, req)
			} else {
				c.failRequest(cs, req, err)
			}
			cs.wmu.Unlock()
		case <-cs.wake:
			cs.wmu.Lock()
			if err = cs.writeRest(); err == nil {
				_, err = cs.flushEngine(context.Background(), 0)
			}
			cs.wmu.Unlock()
		case <-cs.dying:
			return
		case <-c.done():
			return
		}
		if err != nil {
			c.handleConnError(cs, err)
			return
		}
	}
}

// processWrite handles one writeReq received from the queue; the caller
// holds cs.wmu. For pkt-mode reqs it drains any further pkt-mode reqs
// into the batch (bounded by writeBatchMax) and flushes once via writev.
// For fn-mode reqs it flushes any pending batch first, then invokes
// req.fn directly. Every request taken from the queue is uncounted once
// it is written or failed.
func (c *Client) processWrite(cs *connState, batch *writeBatch, req writeReq) error {
	for {
		if req.mark != 0 || req.fn != nil {
			if err := batch.flush(cs); err != nil {
				c.failRequest(cs, req, err)
				return err
			}
			return c.writeOne(cs, req)
		}
		batch.add(req)
		if batch.n == batch.cap {
			return batch.flush(cs)
		}
		select {
		case more, ok := <-cs.writeQueue:
			if !ok {
				return batch.flush(cs)
			}
			req = more
		default:
			return batch.flush(cs)
		}
	}
}

// writeOne writes a mark (the engine's packets up to it) or a fn-mode
// request and answers it.
func (c *Client) writeOne(cs *connState, req writeReq) error {
	var err error
	if req.mark != 0 {
		_, err = cs.flushEngine(context.Background(), req.mark)
	} else {
		_, err = req.fn(cs.conn)
		if err == nil {
			cs.lastWriteUnixNano.Store(cs.clk.Now().UnixNano())
		}
	}
	cs.queued.Add(-1)
	if req.done != nil {
		req.done <- err
	}
	return err
}

// failRequest answers a request taken from the queue that will not be
// written.
func (c *Client) failRequest(cs *connState, req writeReq, err error) {
	cs.queued.Add(-1)
	if req.pkt != nil {
		wire.ReleaseBuf(req.pkt)
	}
	if req.done != nil {
		req.done <- err
	}
}

// drainWriteQueue runs as the writer exits: it marks the connection
// dying, closes the queue so nothing more is admitted, drops the tail of
// an interrupted direct write, and answers every request still queued
// with ErrNotConnected without writing it, releasing its buffer.
func drainWriteQueue(cs *connState) {
	cs.signalDown()
	cs.closeQueue()
	cs.wmu.Lock()
	if cs.rest != nil {
		cs.rest = nil
		cs.queued.Add(-1)
	}
	cs.wmu.Unlock()
	for {
		select {
		case req := <-cs.writeQueue:
			cs.queued.Add(-1)
			if req.pkt != nil {
				wire.ReleaseBuf(req.pkt)
			}
			if req.done != nil {
				select {
				case req.done <- ErrNotConnected:
				default:
				}
			}
		default:
			return
		}
	}
}

// enqueueFireAndForget pushes a write without waiting. Drops the write
// if the connection is dying.
func (c *Client) enqueueFireAndForget(cs *connState, fn writeFn) {
	_ = cs.queue(context.Background(), writeReq{fn: fn}, true)
}

// awaitWrite waits for the writer's answer to a queued request: nil once
// it was written, ErrNotConnected when it never will be, or ctx's error,
// in which case it may still be written.
func awaitWrite(ctx context.Context, cs *connState, done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		// The writer answers every request it takes, also when it
		// drains the queue on exit; once it is gone, no answer means
		// the packet was never written.
		<-cs.writerDone
		select {
		case err := <-done:
			return err
		default:
			return ErrNotConnected
		}
	case <-cs.life.shutdown:
		return cs.life.closedErr()
	}
}
