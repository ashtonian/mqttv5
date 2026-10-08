// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// connState bundles everything tied to one TCP/TLS connection. Each
// reconnect allocates a fresh connState; per-connection goroutines
// hold it by pointer so the old one stays valid even after the
// Client has moved on.
type connState struct {
	clk  clock.Clock
	conn transport.Conn
	// connectedAt is when the CONNACK arrived; the supervisor compares
	// how long the connection lasted with its reconnect delay.
	connectedAt time.Time
	decoder     *wire.Decoder
	writeQueue  chan writeReq

	// wmu serializes writes to conn. The writer goroutine holds it while
	// it writes; a caller that would only wait for an idle writer takes
	// it to write itself (see acquireIdle).
	wmu sync.Mutex
	// queued counts writes handed to the writer and not yet written: the
	// requests in writeQueue and rest. A sender counts its request before
	// the send and the writer uncounts it after the write, so a direct
	// write never overtakes a packet its caller queued earlier.
	queued atomic.Int64
	// admit fences the write queue against the writer's exit: a sender
	// holds it shared while it sends, and the exiting writer sets closed
	// under it before answering what is left, so no request is admitted
	// that nobody would answer.
	admit  sync.RWMutex
	closed bool
	// direct is set when callers may write on their own goroutine (see
	// acquireIdle): conn sends net.Buffers with one writev, and a write
	// that a deadline cuts short leaves the connection usable, so ctx can
	// end a caller's wait mid-write and the writer finishes the packet.
	// TCP and Unix sockets qualify; TLS does not (a timed-out write
	// leaves its record layer unusable), nor does WebSocket. hdr and vec
	// are the reusable header buffer and buffer list of direct QoS 0
	// writes, and rest the unwritten tail of a direct write ctx cut
	// short, which the writer sends before anything else; all three are
	// guarded by wmu.
	direct bool
	hdr    []byte
	vec    net.Buffers
	rest   []byte
	// engine is the client's session engine; flushEngine collects from
	// it. writeFailed reports a failed direct write, which ends the
	// connection like a failed write on the writer goroutine.
	engine      *inflight.Engine
	writeFailed func(error)

	// gen is the session engine's generation for this connection; the
	// writer passes it to Collect so a stale writer never sends. The
	// engine hands this connection to publishers before runConnection
	// learns gen, so it is zero until then and SendOrdered queues.
	gen atomic.Uint64
	// wake asks the writer to collect from the session engine.
	wake chan struct{}
	// frames is the writer's reusable batch for engine packets.
	frames inflight.Frames
	// life is the client's span this connection belongs to.
	life *lifecycle

	dying chan struct{}
	// writerDone is closed once the writer goroutine has exited and
	// answered every request it took.
	writerDone chan struct{}
	dyingOnce  sync.Once
	done       chan struct{}
	doneOnce   sync.Once
	wg         sync.WaitGroup

	// reads counts packets read on this connection; the keep-alive
	// treats any packet read after a PINGREQ as the broker's answer.
	reads atomic.Uint64

	// Last successful network activity timestamps (unix nano) for the
	// idle-aware PINGREQ scheduler. writeLoop updates lastWriteUnixNano
	// after every successful conn.Write; readLoop updates
	// lastReadUnixNano after every successful packet read. The ping
	// scheduler only emits PINGREQ when EITHER window has been quiet
	// for KeepAlive seconds — outbound activity satisfies MQTT v5
	// §3.1.2.10 (the broker won't disconnect us), and inbound activity
	// satisfies broker-liveness detection (we know the broker is alive).
	lastWriteUnixNano atomic.Int64
	lastReadUnixNano  atomic.Int64

	// Inbound topic-alias cache. The server may register an alias by
	// sending PUBLISH (Topic="x/y", TopicAlias=N), then re-use it by
	// sending PUBLISH (Topic="", TopicAlias=N). The cache is fresh
	// per-connection per MQTT v5 §3.3.2.3.4. Stored strings are
	// strings.Clone'd so they outlive the frame they were decoded from.
	aliasMu  sync.RWMutex
	aliasMap map[uint16]string

	// Outbound topic-alias allocation. The budget is the broker's
	// TopicAliasMaximum (info.TopicAliasMaximum); 0 means the broker
	// won't accept aliases at all. outAliasMap maps a publish topic to
	// the alias we've registered with the broker on this connection.
	//
	// Only applied to QoS 0 publishes — QoS 1/2 publishes must use
	// the full topic string so the supervisor's replay path works
	// across a reconnect (the broker's alias state resets per
	// §3.3.2.3.4). aliasSlot, a one-slot semaphore a caller can stop
	// waiting for when its ctx ends, guards both.
	aliasSlot    chan struct{}
	outAliasMap  map[string]uint16
	outAliasNext uint16

	// serverDisconnect carries the broker-side DISCONNECT when the
	// connection goes down due to a broker-initiated disconnect. Read by the
	// supervisor to invoke OnServerDisconnect before the generic
	// OnConnectionDown, and by Reauthenticate to distinguish a broker
	// rejection (ErrReauthRejected) from a plain drop (ErrNotConnected).
	serverDisconnect atomic.Pointer[DisconnectInfo]

	// reauth is non-nil while a client-initiated re-authentication
	// (Reauthenticate) is in flight on this connection. The read loop
	// routes inbound AUTH to the in-flight call and clears the slot when
	// the exchange resolves on the wire (0x00 Success, or a DISCONNECT /
	// drop that disposes of this connState). It is deliberately NOT
	// cleared when the caller's ctx elapses, so a late terminal AUTH
	// cannot be misattributed to a subsequent Reauthenticate on the same
	// connection.
	reauth atomic.Pointer[reauthCall]

	// info is what the broker granted in CONNACK; publish and subscribe
	// check their packets against it before writing.
	info      ConnackInfo
	brokerURL string
	// connectExpiry is the Session Expiry Interval this connection's
	// CONNECT carried; a DISCONNECT may not raise it from 0.
	connectExpiry uint32
}

// Wake implements inflight.Link.
func (cs *connState) Wake() {
	select {
	case cs.wake <- struct{}{}:
	default:
	}
}

// SendOrdered implements inflight.Link: the session engine's QoS 1/2
// PUBLISH packets up to seq go out behind every write already queued,
// so one never overtakes an earlier QoS 0 publish from the same caller.
// With nothing queued the caller writes them itself; ctx ending stops
// that write and the writer goroutine sends the rest.
func (cs *connState) SendOrdered(ctx context.Context, seq uint64) error {
	if cs.gen.Load() != 0 && cs.acquireIdle() {
		_, err := cs.flushEngine(ctx, seq)
		cs.wmu.Unlock()
		return err
	}
	return cs.queue(ctx, writeReq{mark: seq}, true)
}

// queue hands req to the writer goroutine, waiting for room or ctx unless
// block is false, in which case a full queue fails with
// ErrWriteQueueFull. nil means the writer has the request and will
// answer it.
func (cs *connState) queue(ctx context.Context, req writeReq, block bool) error {
	cs.admit.RLock()
	if cs.closed {
		cs.admit.RUnlock()
		return ErrNotConnected
	}
	cs.queued.Add(1)
	err := cs.send(ctx, req, block)
	if err != nil {
		cs.queued.Add(-1)
	}
	cs.admit.RUnlock()
	return err
}

func (cs *connState) send(ctx context.Context, req writeReq, block bool) error {
	if !block {
		select {
		case cs.writeQueue <- req:
			return nil
		case <-cs.dying:
			return ErrNotConnected
		case <-cs.life.shutdown:
			return cs.life.closedErr()
		default:
			return ErrWriteQueueFull
		}
	}
	select {
	case cs.writeQueue <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		return ErrNotConnected
	case <-cs.life.shutdown:
		return cs.life.closedErr()
	}
}

// closeQueue stops admitting requests; queue fails with ErrNotConnected
// from then on. The caller has closed dying, so senders blocked on a
// full queue give up and let it through.
func (cs *connState) closeQueue() {
	cs.admit.Lock()
	cs.closed = true
	cs.admit.Unlock()
}

// acquireIdle takes the write lock when the caller may write on its own
// goroutine: the connection supports it (see direct), nothing is queued
// for the writer goroutine or left over from an interrupted write, no
// write is in progress, and the connection is up. The caller then
// writes and unlocks: handing the write to an idle writer goroutine
// costs two goroutine switches, more than the write itself. False means
// the caller queues its write instead.
func (cs *connState) acquireIdle() bool {
	if !cs.direct || cs.queued.Load() != 0 || !cs.wmu.TryLock() {
		return false
	}
	if cs.rest != nil {
		cs.wmu.Unlock()
		return false
	}
	select {
	case <-cs.dying:
		cs.wmu.Unlock()
		return false
	default:
		return true
	}
}

// flushEngine writes what the session engine has ready, allowing QoS 1/2
// publishes up to sequence upTo (0 keeps the current limit). The caller
// holds wmu. ctx bounds a direct write (see writeDirect); the writer
// goroutine passes context.Background. started reports whether anything
// was, or will be, written.
func (cs *connState) flushEngine(ctx context.Context, upTo uint64) (started bool, err error) {
	for {
		cs.frames.Reset()
		if cs.engine.Collect(cs.gen.Load(), upTo, &cs.frames) == 0 {
			return started, nil
		}
		upTo = 0
		started = true
		bufs := cs.frames.Buffers()
		// Collect has handed these packets to the connection, so a write
		// ctx interrupts is finished by the writer.
		_, err = cs.writeDirect(ctx, &bufs, true)
		cs.frames.Reset()
		if err != nil {
			return started, err
		}
	}
}

// writePublish writes a QoS 0 PUBLISH on the caller's goroutine; the
// caller holds wmu. The payload goes out from the caller's slice, in the
// same writev as the header, without being copied. started reports
// whether any of the packet was, or will be, written: false after an
// encoding or size error, which leaves the connection alone, and when
// ctx ended before the first byte.
func (cs *connState) writePublish(ctx context.Context, opts *wire.PublishOpts) (started bool, err error) {
	cs.hdr, err = wire.AppendPublishHeader(cs.hdr[:0], *opts)
	if err != nil {
		return false, err
	}
	if err = checkPacketSize(cs, len(cs.hdr)+len(opts.Payload)); err != nil {
		return false, err
	}
	// WriteTo advances the slice it writes from; keep the full one so the
	// next write reuses its array, and drop the payload reference.
	full := append(cs.vec[:0], cs.hdr, opts.Payload)
	cs.vec = full
	started, err = cs.writeDirect(ctx, &cs.vec, false)
	clear(full)
	cs.vec = full[:0]
	// A header grown by large properties is not kept for the life of the
	// connection.
	if cap(cs.hdr) > maxRetainedHeader {
		cs.hdr = nil
	}
	return started, err
}

// writeSlice bounds how long a direct write runs without checking the
// caller's ctx: a write that ctx ends stops within about this long,
// or at ctx's deadline.
const writeSlice = 20 * time.Millisecond

// writeDirect writes bufs; the caller holds wmu. When ctx can end, the
// write runs under a deadline renewed every writeSlice (or ctx's own
// deadline, if sooner), so a write still blocked when ctx ends is
// interrupted without a goroutine or an allocation: the caller gets
// ctx's error, and the bytes not yet written go to the writer goroutine,
// which sends them before anything else, so the stream stays whole. A
// packet ctx ended before its first byte is not sent at all, unless must
// is set (its bytes were promised to the connection). started reports
// whether any of bufs was, or will be, written. Any other write error
// ends the connection.
func (cs *connState) writeDirect(ctx context.Context, bufs *net.Buffers, must bool) (started bool, err error) {
	if ctx.Done() == nil {
		_, err = bufs.WriteTo(cs.conn)
		return cs.wrote(err)
	}
	if err = ctx.Err(); err != nil && !must {
		return false, err
	}
	var written int64
	for {
		deadline := time.Now().Add(writeSlice)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		_ = cs.conn.SetWriteDeadline(deadline)
		n, werr := bufs.WriteTo(cs.conn)
		written += n
		if werr == nil || !errors.Is(werr, os.ErrDeadlineExceeded) {
			_ = cs.conn.SetWriteDeadline(time.Time{})
			started, err = cs.wrote(werr)
			return started || written > 0, err
		}
		if ctx.Err() == nil {
			continue
		}
		_ = cs.conn.SetWriteDeadline(time.Time{})
		if written == 0 && !must {
			return false, ctx.Err()
		}
		cs.handOff(*bufs)
		return true, ctx.Err()
	}
}

// wrote finishes a direct write that ran to its end: err ends the
// connection.
func (cs *connState) wrote(err error) (started bool, _ error) {
	if err != nil {
		cs.writeFailed(err)
		return false, err
	}
	cs.lastWriteUnixNano.Store(cs.clk.Now().UnixNano())
	return true, nil
}

// handOff gives the writer goroutine rest, the unwritten tail of a
// direct write; the caller holds wmu. It is copied: the caller's payload
// and the connection's reusable buffers may change once the caller
// returns. A dying connection drops it.
func (cs *connState) handOff(rest net.Buffers) {
	select {
	case <-cs.dying:
		return
	default:
	}
	var size int
	for _, b := range rest {
		size += len(b)
	}
	tail := make([]byte, 0, size)
	for _, b := range rest {
		tail = append(tail, b...)
	}
	cs.rest = tail
	cs.queued.Add(1)
	cs.Wake()
}

// writeRest sends the tail an interrupted direct write handed off; the
// caller is the writer goroutine and holds wmu.
func (cs *connState) writeRest() error {
	if cs.rest == nil {
		return nil
	}
	_, err := cs.conn.Write(cs.rest)
	cs.rest = nil
	cs.queued.Add(-1)
	if err == nil {
		cs.lastWriteUnixNano.Store(cs.clk.Now().UnixNano())
	}
	return err
}

// maxRetainedHeader bounds the header buffer a connection keeps between
// direct writes.
const maxRetainedHeader = 4 << 10

// supportsDirectWrites reports whether callers may write to conn on
// their own goroutine (see connState.direct).
func supportsDirectWrites(conn transport.Conn) bool {
	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		return true
	}
	return false
}

var _ inflight.Link = (*connState)(nil)

// signalDown marks this connection as dying. Idempotent.
func (cs *connState) signalDown() {
	cs.dyingOnce.Do(func() {
		close(cs.dying)
		if cs.conn != nil {
			_ = cs.conn.Close()
		}
	})
}

// handleConnError is the per-connection error handler. The first
// caller wins via cs.signalDown's Once.
func (c *Client) handleConnError(cs *connState, err error) {
	select {
	case <-cs.dying:
		// The connection was closed on purpose; err is that close.
		return
	default:
	}
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
		c.cfg.Logger.Warn("mqttv5: connection error", slog.Any("error", err))
	}
	cs.signalDown()
}
