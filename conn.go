// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/internal/trie"
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
	// raw, set on a direct connection where the platform allows it,
	// writes without waiting for room in the socket (see flushControl).
	// It is guarded by wmu.
	raw *rawWriter
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

// flushControl writes the session engine's ready acknowledgements and
// PUBRELs on the read loop; the caller holds wmu and cs.raw is set. It
// writes what the socket takes without waiting and hands the rest to the
// writer goroutine: the broker may be waiting for the client to read
// before it reads in turn. A write error ends the connection.
func (cs *connState) flushControl() error {
	defer cs.frames.Reset()
	for {
		cs.frames.Reset()
		if cs.engine.CollectControl(cs.gen.Load(), &cs.frames) == 0 {
			return nil
		}
		for bufs := cs.frames.Buffers(); len(bufs) > 0; bufs = bufs[1:] {
			n, err := cs.raw.tryWrite(bufs[0])
			if err != nil {
				_, err = cs.wrote(err)
				return err
			}
			if n < len(bufs[0]) {
				bufs[0] = bufs[0][n:]
				cs.handOff(bufs)
				return nil
			}
		}
		_, _ = cs.wrote(nil)
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

// readLoop drives the decoder for one connection. Before it waits for
// more input it sends what the packets read so far made ready.
func (c *Client) readLoop(cs *connState) {
	defer cs.wg.Done()
	for {
		if !cs.decoder.Ready() {
			c.sendReady(cs)
		}
		pkt, err := cs.decoder.ReadPacket()
		if perr := decodeViolation(err); perr != nil {
			c.protocolViolation(cs, perr)
			return
		}
		if err != nil {
			c.handleConnError(cs, err)
			return
		}
		cs.lastReadUnixNano.Store(cs.clk.Now().UnixNano())
		cs.reads.Add(1)
		c.dispatch(cs, pkt)
	}
}

// sendReady sends what the packets read so far made ready. The broker's
// acknowledgements and the read loop's own acks queue it without waking
// the writer goroutine. The read loop writes the acknowledgements and
// PUBRELs itself when the connection is idle, in one write and without
// the two goroutine switches a hand-off costs; the writer takes them
// when it is busy already, and always takes the PUBLISHes a freed send
// quota lets out, so payloads never hold up reading.
func (c *Client) sendReady(cs *connState) {
	control, publishes := c.engine.Pending()
	if control && cs.raw != nil && cs.acquireIdle() {
		// A write error ends the connection, which the read loop sees
		// on its next read.
		_ = cs.flushControl()
		cs.wmu.Unlock()
		control = false
	}
	if control || publishes {
		cs.Wake()
	}
}

// dispatch routes one decoded packet for the given connection.
func (c *Client) dispatch(cs *connState, pkt wire.Packet) {
	switch p := pkt.(type) {
	case *wire.Publish:
		c.handlePublish(cs, p)
	case *wire.PubResp:
		c.handlePubResp(p)
	case *wire.Suback, *wire.Unsuback:
		c.deliverControlAck(cs, pkt)
	case *wire.Pingresp:
		pkt.Release()
	case *wire.Auth:
		c.handleServerAuth(cs, p)
	case *wire.Disconnect:
		// Always retain the reason: the supervisor uses it for the
		// ServerDisconnects stat and OnServerDisconnect, and
		// Reauthenticate uses it to surface ErrReauthRejected with the
		// broker's code.
		info := disconnectInfoOf(p)
		cs.serverDisconnect.Store(&info)
		c.cfg.Logger.Warn("mqttv5: broker DISCONNECT", slog.Int("reason", int(p.ReasonCode)))
		pkt.Release()
		cs.signalDown()
	default:
		// CONNECT, SUBSCRIBE, UNSUBSCRIBE and PINGREQ only travel to the
		// server; CONNACK only during the handshake.
		t := pkt.Type()
		pkt.Release()
		c.protocolViolation(cs, &ProtocolError{Reason: wire.ReasonProtocolError, Packet: t,
			Detail: "a server must not send " + t.String() + " here"})
	}
}

// decodeViolation turns a decoder error that means the broker broke the
// protocol into the ProtocolError to disconnect with; other errors (I/O)
// give nil.
func decodeViolation(err error) *ProtocolError {
	if err == nil {
		return nil
	}
	var pe *wire.PacketError
	if errors.As(err, &pe) {
		return &ProtocolError{Reason: pe.Reason, Packet: pe.Packet, Detail: pe.Detail}
	}
	if errors.Is(err, wire.ErrPacketTooLarge) {
		return &ProtocolError{Reason: wire.ReasonPacketTooLarge, Detail: err.Error()}
	}
	return nil
}

// handlePublish dispatches an inbound PUBLISH to every matching
// subscription. Each subscription gets its own *Message handle onto one
// shared delivery; the broker acknowledgement goes out when the last
// handle is acked. Unless every matched subscription is zero-copy, the
// frame is copied into an owned buffer and released before any handler
// runs. If nothing matches, the PUBLISH is acked here so the broker
// doesn't retry it forever.
func (c *Client) handlePublish(cs *connState, pub *wire.Publish) {
	c.stats.addInboundPublish()
	// Resolve topic aliases per §3.3.2.3.4 before anything else —
	// the trie matcher needs the real topic, and any subscriber
	// reading msg.Topic expects the substituted value.
	if perr := c.resolveTopicAlias(cs, pub); perr != nil {
		pub.Release()
		c.protocolViolation(cs, perr)
		return
	}

	var in *inflight.In
	if pub.QoS > 0 {
		var deliver bool
		var err error
		in, deliver, err = c.engine.Receive(pub.PacketID, pub.QoS)
		if err != nil {
			pub.Release()
			c.protocolViolation(cs, &ProtocolError{
				Reason: wire.ReasonReceiveMaximumExceeded, Packet: wire.PUBLISH, Detail: err.Error(),
			})
			return
		}
		if !deliver {
			// A retransmission of a message still in flight: the
			// application already has it.
			pub.Release()
			return
		}
	}

	// The common single-match case needs no slice.
	var (
		first    *route
		rest     []*route
		zeroCopy = true
	)
	add := func(r *route) {
		if r == first || slices.Contains(rest, r) {
			return
		}
		zeroCopy = zeroCopy && r.zeroCopy
		if first == nil {
			first = r
		} else {
			rest = append(rest, r)
		}
	}
	// A PUBLISH carrying Subscription Identifiers was sent for the
	// subscriptions they name: with overlapping filters the broker may
	// send one copy per subscription, each tagged with its own, and
	// a copy reaches only the subscriptions it was sent for. Without
	// identifiers every matching subscription gets it.
	var tagged [4]uint32
	ids := tagged[:0]
	for id := range pub.Properties.SubscriptionIdentifiers() {
		ids = append(ids, id)
	}
	foreign := len(ids) > 0
	c.router.Match(pub.Topic, func(h trie.Handler) {
		v := h.(*brokerSub).view.Load()
		if len(ids) > 0 {
			foreign = foreign && !v.issuedAny(ids)
			if !v.accepts(ids) {
				return
			}
		}
		for _, r := range v.routes {
			add(r)
		}
	})
	if first == nil && foreign && !c.retired(ids) {
		// Tagged only with identifiers this client never gave a
		// subscription to the topic: sent for one of an earlier process,
		// whose session this connection resumed. Match it by topic, as
		// without identifiers.
		c.router.Match(pub.Topic, func(h trie.Handler) {
			for _, r := range h.(*brokerSub).view.Load().routes {
				add(r)
			}
		})
	}

	if first == nil {
		if in != nil {
			c.engine.AckDeferred(in)
		}
		pub.Release()
		return
	}

	d := &delivery{client: c, in: in}
	d.refs.Store(int32(1 + len(rest)))
	m := &d.first
	m.d = d
	switch {
	case zeroCopy:
		fillZeroCopy(m, pub)
		d.frame = pub
	case !pub.Pooled():
		// A frame that is never recycled is already the message's own.
		fillZeroCopy(m, pub)
		pub.Release()
	default:
		fillOwned(m, pub)
		pub.Release()
	}

	// Every handle is built before the first dispatch: a synchronous
	// handler may modify its Message or ack it while we are still here.
	var handles []*Message
	if len(rest) > 0 {
		handles = make([]*Message, len(rest))
		for i := range rest {
			handles[i] = handleFor(m)
		}
	}
	first.dispatch(m)
	for i, r := range rest {
		r.dispatch(handles[i])
	}
}

// resolveTopicAlias applies the Topic Alias property (§3.3.2.3.4): a
// PUBLISH with a topic registers or replaces the alias, one with an
// empty topic uses it. An alias of 0 or above the maximum the client
// advertised is a Topic Alias invalid (0x94) violation; using an alias
// that was never registered is a Protocol Error (0x82).
func (c *Client) resolveTopicAlias(cs *connState, pub *wire.Publish) *ProtocolError {
	alias, ok := pub.Properties.Uint16(wire.PropTopicAlias)
	if !ok {
		return nil
	}
	if alias == 0 || alias > c.cfg.InboundTopicAliasMaximum {
		return &ProtocolError{Reason: wire.ReasonTopicAliasInvalid, Packet: wire.PUBLISH,
			Detail: fmt.Sprintf("topic alias %d outside 1..%d", alias, c.cfg.InboundTopicAliasMaximum)}
	}
	if pub.Topic != "" {
		// Clone so the cached string outlives the frame.
		topic := strings.Clone(pub.Topic)
		cs.aliasMu.Lock()
		cs.aliasMap[alias] = topic
		cs.aliasMu.Unlock()
		return nil
	}
	cs.aliasMu.RLock()
	topic, ok := cs.aliasMap[alias]
	cs.aliasMu.RUnlock()
	if !ok {
		return &ProtocolError{Reason: wire.ReasonProtocolError, Packet: wire.PUBLISH,
			Detail: fmt.Sprintf("topic alias %d used before it was registered", alias)}
	}
	pub.Topic = topic
	return nil
}

// protocolViolation handles a broker packet that breaks MQTT v5:
// DISCONNECT with the error's reason code, then close. The supervisor
// reconnects.
func (c *Client) protocolViolation(cs *connState, perr *ProtocolError) {
	c.stats.addProtocolError()
	c.cfg.Logger.Error("mqttv5: broker protocol violation; disconnecting",
		slog.String("packet", perr.Packet.String()),
		slog.Int("reason", int(perr.Reason)),
		slog.String("detail", perr.Detail))
	c.writeDisconnectAndClose(cs, wire.DisconnectOpts{ReasonCode: perr.Reason})
}

// handlePubResp routes PUBACK/PUBREC/PUBREL/PUBCOMP to the session
// engine.
func (c *Client) handlePubResp(p *wire.PubResp) {
	defer p.Release()
	var refused error
	if p.ReasonCode.IsError() {
		refused = newReasonCodeError(p.Type(), p.ReasonCode, p.Properties)
	}
	switch p.Type() {
	case wire.PUBACK:
		c.engine.HandlePuback(p.PacketID, p.ReasonCode, refused)
		c.stats.addPublishAcked()
	case wire.PUBREC:
		c.engine.HandlePubrec(p.PacketID, p.ReasonCode, refused)
	case wire.PUBREL:
		c.engine.HandlePubrel(p.PacketID)
	case wire.PUBCOMP:
		c.engine.HandlePubcomp(p.PacketID)
		c.stats.addPublishAcked()
	}
}

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

// keepAlive is a snapshot the keep-alive scheduler decides from.
type keepAlive struct {
	now       time.Time
	lastWrite time.Time
	lastRead  time.Time
	pingSent  time.Time // zero when no PINGREQ is outstanding
	// heard reports a packet read since pingSent.
	heard   bool
	keep    time.Duration
	timeout time.Duration
}

// decide reports whether to send a PINGREQ now, whether the connection
// is dead, and otherwise how long to wait before deciding again.
//
// A PINGREQ is due once either direction has been quiet for the
// keep-alive: outbound silence would let the broker drop us
// (§3.1.2.10), inbound silence leaves us unsure the broker is alive.
// After a PINGREQ, any inbound packet — not only the PINGRESP — proves
// the broker alive: under an inbound flood the PINGRESP can sit behind
// a long read backlog. With nothing inbound for the ping timeout the
// connection is dead. Because the timeout is shorter than the
// keep-alive, a dead connection is detected before the next PINGREQ
// would be due, and a live one never goes a keep-alive without one.
func (k keepAlive) decide() (send, dead bool, wait time.Duration) {
	if !k.pingSent.IsZero() && !k.heard {
		deadline := k.pingSent.Add(k.timeout)
		if !k.now.Before(deadline) {
			return false, true, 0
		}
		return false, false, deadline.Sub(k.now)
	}
	quiet := k.lastWrite
	if k.lastRead.Before(quiet) {
		quiet = k.lastRead
	}
	due := quiet.Add(k.keep)
	if !k.now.Before(due) {
		return true, false, 0
	}
	return false, false, due.Sub(k.now)
}

// effectivePingTimeout keeps the ping timeout below the keep-alive the
// broker actually granted, which can be shorter than the one configured.
func effectivePingTimeout(configured, keep time.Duration) time.Duration {
	if configured >= keep {
		return keep / 2
	}
	return configured
}

// pingLoop runs the keep-alive for one connection using the effective
// keep-alive from CONNACK (the broker's Server Keep Alive when sent).
func (c *Client) pingLoop(cs *connState) {
	defer cs.wg.Done()
	keep := time.Duration(cs.info.KeepAlive) * time.Second
	timeout := effectivePingTimeout(c.cfg.PingTimeout, keep)
	var timer clock.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	var (
		pingSent  time.Time
		readsThen uint64
	)
	for {
		k := keepAlive{
			now:       cs.clk.Now(),
			lastWrite: time.Unix(0, cs.lastWriteUnixNano.Load()),
			lastRead:  time.Unix(0, cs.lastReadUnixNano.Load()),
			pingSent:  pingSent,
			heard:     cs.reads.Load() != readsThen,
			keep:      keep,
			timeout:   timeout,
		}
		if !pingSent.IsZero() && k.heard {
			pingSent = time.Time{}
			k.pingSent = pingSent
		}
		send, dead, wait := k.decide()
		if dead {
			c.cfg.Logger.Warn("mqttv5: no response to PINGREQ; connection presumed dead",
				slog.Duration("ping_timeout", timeout))
			c.stats.addPingTimeout()
			c.handleConnError(cs, errors.New("PINGRESP timeout"))
			return
		}
		if send {
			c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
				return wire.WritePingreq(w)
			})
			pingSent = k.now
			readsThen = cs.reads.Load()
			continue
		}
		if timer == nil {
			timer = cs.clk.NewTimer(wait)
		} else {
			timer.Reset(wait)
		}
		select {
		case <-timer.C():
		case <-cs.dying:
			return
		case <-c.done():
			return
		}
	}
}
