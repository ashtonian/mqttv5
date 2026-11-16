package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/wire"
)

// Publish sends a PUBLISH packet.
//
//   - QoS 0: returns per [PublishMode] ([PublishFireAndForget] or
//     [PublishWaitForFlush]).
//   - QoS 1: returns after PUBACK.
//   - QoS 2: returns after PUBCOMP.
//
// QoS 1/2 messages belong to the session once registered: a dropped
// connection resends them (DUP=1) when the session resumes, and callers
// stay blocked on the acknowledgement across the drop. A refusal (reason
// code 0x80 or above) is returned as a [*ReasonCodeError]. Each QoS 1/2
// call holds its goroutine until the acknowledgement arrives;
// [Client.PublishAsync] keeps many in flight from one goroutine.
//
// A call that waits for its packet to be written — QoS 1/2, or QoS 0
// with [PublishWaitForFlush] — writes it on the calling goroutine when
// the connection is a TCP or Unix socket and nothing is queued for its
// writer goroutine, saving the hand-off; otherwise the writer sends it
// behind the queued packets. ctx bounds the call either way — a write
// still blocked when ctx is cancelled stops within 20 ms, one at ctx's
// deadline at once — and a write it interrupts is finished by the
// writer goroutine, so the packet may still reach the broker after
// Publish returned ctx's error.
//
// With no connection up Publish returns [ErrNotConnected], which
// guarantees the message was not sent; with
// [WithPublishWaitForConnection] it waits for the next connection.
//
// With [WithPublisherPool] the call first tries the pool per
// [PoolRoutingPolicy] and falls back to the main connection if every
// pool member is unhealthy.
func (c *Client) Publish(ctx context.Context, po PublishOptions) error {
	if po.QoS > 2 {
		return fmt.Errorf("mqttv5: invalid QoS %d (must be 0, 1, or 2)", po.QoS)
	}
	if c.pool != nil {
		err := c.pool.publish(ctx, po)
		if !errors.Is(err, errNoHealthyPublishers) {
			return err
		}
		// No member could send it: use the main connection.
		c.stats.addPoolFallback()
	}

	cs, err := c.publishConnection(ctx)
	if err != nil {
		return err
	}
	opts := po.wire()
	if err := c.applyServerLimits(cs, &opts); err != nil {
		return err
	}

	switch opts.QoS {
	case 0:
		err := c.publishQoS0(ctx, cs, opts)
		if err == nil {
			c.stats.addPublishSent()
		}
		return err
	case 1, 2:
		err := c.publishQoSReliable(ctx, opts)
		if err == nil {
			c.stats.addPublishSent()
		}
		return err
	}
	return nil
}

// PublishAsync starts a publish and returns without waiting for the
// broker's answer, which the [PublishResult] reports. One goroutine can
// so keep many QoS 1/2 messages in flight, where [Client.Publish] needs
// a goroutine for each, parked until its acknowledgement arrives.
//
// An error means the message was not accepted: the session does not
// hold it. Once accepted, a QoS 1/2 message belongs to the session as
// with Publish (a dropped connection resends it) and every outcome comes
// through the result, a refusal as a [*ReasonCodeError] included. When
// Disconnect ends the client's run first, the result completes with
// [ErrClosed], or with what ended the run, and the message stays in the
// session for the next Connect.
//
// ctx bounds the start only: the wait for a connection
// ([WithPublishWaitForConnection]), for a packet identifier, for the
// session store's write and for room in the write queue. Once the
// message is accepted ctx neither bounds nor withdraws it;
// [PublishResult.Wait] takes a ctx of its own. Each unfinished message
// holds a packet identifier, so at most 65,535 are unfinished at once
// and a further call waits for one.
//
// A QoS 1/2 message is never written on the calling goroutine: the
// writer sends those started meanwhile together, in one write. How many
// are in flight is bounded by the send quota (see [WithMaxInflight]);
// the rest wait in the session for acknowledgements to make room.
//
// A QoS 0 message, or one [WithQoSDowngrade] lowers to QoS 0, has no
// acknowledgement: it is sent as Publish sends it, per [PublishMode]
// (with PublishWaitForFlush PublishAsync waits for the write), and its
// result is complete when PublishAsync returns.
//
// With [WithPublisherPool] the message goes to a pool member as with
// Publish, and Disconnect waits for unfinished results as it waits for
// running Publish calls.
func (c *Client) PublishAsync(ctx context.Context, po PublishOptions) (*PublishResult, error) {
	if po.QoS > 2 {
		return nil, fmt.Errorf("mqttv5: invalid QoS %d (must be 0, 1, or 2)", po.QoS)
	}
	if c.pool != nil {
		r, err := c.pool.publishAsync(ctx, po)
		if !errors.Is(err, errNoHealthyPublishers) {
			return r, err
		}
		c.stats.addPoolFallback()
	}
	return c.publishAsync(ctx, po, nil)
}

// publishAsync is PublishAsync on c's own connection. done, when set,
// runs once when the returned result completes, for a QoS 1/2 message
// with the session engine's mutex held, and never when an error is
// returned.
func (c *Client) publishAsync(ctx context.Context, po PublishOptions, done func()) (*PublishResult, error) {
	cs, err := c.publishConnection(ctx)
	if err != nil {
		return nil, err
	}
	opts := po.wire()
	if err = c.applyServerLimits(cs, &opts); err != nil {
		return nil, err
	}
	if opts.QoS == 0 {
		if err = c.publishQoS0(ctx, cs, opts); err != nil {
			return nil, err
		}
		c.stats.addPublishSent()
		if done != nil {
			done()
		}
		return &PublishResult{}, nil
	}
	life := c.life.Load()
	flow, link, err := c.startReliable(ctx, opts, exchange{onDone: func(err error) {
		if err == nil {
			c.stats.addPublishSent()
		}
		if done != nil {
			done()
		}
	}})
	if err != nil {
		return nil, err
	}
	if link != nil {
		// Through the writer, which sends the flows released meanwhile in
		// one write. A flow left without a marker when ctx ends would wait
		// for some later one, so it is withdrawn unless already sent; a
		// marker the connection's end refused leaves it to the next.
		if err = link.QueueOrdered(ctx, flow.Seq()); err != nil && ctx.Err() != nil && c.engine.Withdraw(flow) {
			return nil, ctx.Err()
		}
	}
	if life.endedErr() != nil {
		// The span ended before the flow was registered, so its end did
		// not end this wait.
		c.engine.InterruptWait(flow)
	}
	return &PublishResult{flow: flow, life: life}, nil
}

// PublishResult is the outcome of a publish started with
// [Client.PublishAsync].
type PublishResult struct {
	flow *inflight.Out // nil: complete when PublishAsync returned
	life *lifecycle    // the span the publish was started in
}

// closedChan is the Done of a result that is complete from the start.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Done is closed when the publish completes: for QoS 1 when the PUBACK
// arrives, for QoS 2 the PUBCOMP, or when Disconnect ends the client's
// run first.
func (r *PublishResult) Done() <-chan struct{} {
	if r.flow == nil {
		return closedChan
	}
	return r.flow.Done()
}

// Err returns nil until Done is closed, and then the publish's outcome:
// nil when the broker accepted the message, otherwise the error
// [Client.Publish] would have returned.
func (r *PublishResult) Err() error {
	if r.flow == nil {
		return nil
	}
	select {
	case <-r.flow.Done():
		return flowOutcome(r.life, r.flow)
	default:
		return nil
	}
}

// Wait waits for Done and returns Err, or returns ctx's error when ctx
// ends first. The publish goes on either way.
func (r *PublishResult) Wait(ctx context.Context) error {
	if r.flow == nil {
		return nil
	}
	// A completed result returns without the select below, which would
	// lock ctx's channel: one every waiter may share.
	select {
	case <-r.flow.Done():
		return flowOutcome(r.life, r.flow)
	default:
	}
	select {
	case <-r.flow.Done():
		return flowOutcome(r.life, r.flow)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// publishConnection is the connection a Publish sends on, once it has
// re-issued its subscriptions. With none up, or when it ends before
// that, it returns ErrNotConnected or, with
// [WithPublishWaitForConnection], waits for the next one, bounded by
// ctx; a client that has been disconnected answers ErrNotConnected
// either way.
func (c *Client) publishConnection(ctx context.Context) (*connState, error) {
	var ended *connState
	for {
		changed := c.connChanged()
		cs := c.cur.Load()
		if cs != nil && cs != ended {
			err := cs.awaitResumed(ctx)
			if err == nil {
				return cs, nil
			}
			if !errors.Is(err, ErrNotConnected) || !c.cfg.PublishWaitForConnection {
				return nil, err
			}
			// It ended before it resumed; it stays current until the
			// supervisor clears it, so wait for a change, not for it.
			ended = cs
			continue
		}
		if !c.cfg.PublishWaitForConnection {
			return nil, ErrNotConnected
		}
		select {
		case <-changed:
		case <-c.done():
			return nil, ErrNotConnected
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// publishTracked starts a QoS 1/2 exchange on the current connection
// and returns without waiting for it: x.settle receives the outcome (see
// [inflight.SettleFunc]) and x.ref is stored with the session record so
// the exchange can be adopted after a restart. The publisher pool is not
// used. The flow waits on link (nil when there is none) for a marker,
// QueueOrdered with seq or a later one, which the caller queues once for
// all it started, so they go out in one write — or, should registering
// one have to wait for a packet identifier, in x.beforeWait.
func (c *Client) publishTracked(ctx context.Context, po PublishOptions, x exchange) (seq uint64, link inflight.Link, err error) {
	if po.QoS != 1 && po.QoS != 2 {
		return 0, nil, fmt.Errorf("mqttv5: tracked publish needs QoS 1 or 2, not %d", po.QoS)
	}
	cs := c.cur.Load()
	if cs == nil {
		return 0, nil, ErrNotConnected
	}
	opts := po.wire()
	if err = c.applyServerLimits(cs, &opts); err != nil {
		return 0, nil, err
	}
	if opts.QoS == 0 {
		// WithQoSDowngrade against a broker that grants only QoS 0: no
		// acknowledgement would ever settle the message.
		return 0, nil, fmt.Errorf("%w: the broker's Maximum QoS is 0 and a tracked publish needs an acknowledgement", ErrQoSNotSupported)
	}
	flow, link, err := c.startReliable(ctx, opts, x)
	if err != nil {
		return 0, nil, err
	}
	return flow.Seq(), link, nil
}

// applyServerLimits checks opts against what the broker granted in
// CONNACK, so a publish it would refuse — usually by dropping the
// connection — fails here instead (§3.2.2.3). With [WithQoSDowngrade]
// a QoS above the broker's maximum is lowered rather than refused.
func (c *Client) applyServerLimits(cs *connState, opts *wire.PublishOpts) error {
	if opts.QoS > cs.info.MaximumQoS {
		if !c.cfg.QoSDowngrade {
			return fmt.Errorf("%w: QoS %d, broker maximum %d", ErrQoSNotSupported, opts.QoS, cs.info.MaximumQoS)
		}
		opts.QoS = cs.info.MaximumQoS
	}
	if opts.Retain && !cs.info.RetainAvailable {
		return ErrRetainNotSupported
	}
	return nil
}

// checkPacketSize enforces the broker's Maximum Packet Size on an
// encoded packet.
func checkPacketSize(cs *connState, n int) error {
	if max := cs.info.MaximumPacketSize; max > 0 && uint32(n) > max {
		return fmt.Errorf("%w: %d bytes, broker maximum %d", ErrPacketTooLarge, n, max)
	}
	return nil
}

// publishQoS0 sends a QoS 0 PUBLISH per [PublishMode] and
// [WriteOverflowPolicy]. With [PublishWaitForFlush] the caller writes
// the packet itself when acquireIdle allows: one writev of the encoded
// header and the caller's payload, which is not copied. Otherwise the
// packet is encoded into a pooled buffer and queued; the writer writes
// it without a closure allocation and may batch it.
//
// With [WithOutboundTopicAliases] a topic is replaced by an alias the
// broker learned from an earlier PUBLISH on the same connection. The
// alias is allocated and the packet given its place in the write order
// under one semaphore, so a PUBLISH that uses an alias can never be
// written before the one that registers it, and a new alias is kept
// only once its PUBLISH is admitted — written, or queued for the
// writer — so one the broker never saw is never used. Waiting for the
// semaphore ends with ctx; waiting for a queued write happens after it
// is released.
func (c *Client) publishQoS0(ctx context.Context, cs *connState, opts wire.PublishOpts) error {
	if opts.TopicAlias != 0 && opts.TopicAlias > cs.info.TopicAliasMaximum {
		return fmt.Errorf("%w: alias %d, broker maximum %d", ErrTopicAliasInvalid, opts.TopicAlias, cs.info.TopicAliasMaximum)
	}
	var (
		done <-chan error
		err  error
	)
	if c.cfg.OutboundTopicAliases && opts.TopicAlias == 0 && opts.Topic != "" && cs.info.TopicAliasMaximum > 0 {
		select {
		case cs.aliasSlot <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		case <-cs.dying:
			return ErrNotConnected
		}
		topic, registered := assignOutboundAlias(cs, &opts)
		var admitted bool
		admitted, done, err = c.admitQoS0(ctx, cs, &opts)
		if registered && !admitted {
			delete(cs.outAliasMap, topic)
			cs.outAliasNext--
		}
		<-cs.aliasSlot
	} else {
		_, done, err = c.admitQoS0(ctx, cs, &opts)
	}
	if err != nil || done == nil {
		return err
	}
	return awaitWrite(ctx, cs, done)
}

// admitQoS0 writes or queues a QoS 0 PUBLISH. admitted reports whether
// the packet entered the connection's write order: it was written, is
// being finished by the writer, or is queued for it. done, when not
// nil, is where the writer reports a queued write the caller waits for.
func (c *Client) admitQoS0(ctx context.Context, cs *connState, opts *wire.PublishOpts) (admitted bool, done <-chan error, err error) {
	waitForFlush := c.cfg.PublishMode == PublishWaitForFlush
	if waitForFlush && cs.acquireIdle() {
		admitted, err = cs.writePublish(ctx, opts)
		cs.wmu.Unlock()
		return admitted, nil, err
	}

	bp, err := wire.EncodePublish(*opts)
	if err != nil {
		return false, nil, err
	}
	if err = checkPacketSize(cs, len(*bp)); err != nil {
		wire.ReleaseBuf(bp)
		return false, nil, err
	}

	req := writeReq{pkt: bp}
	var answer chan error
	if waitForFlush {
		answer = make(chan error, 1)
		req.done = answer
	}
	// WriteDropNewest fails at once when the queue is full; WriteBlock
	// waits for room, ctx or teardown.
	if err = cs.queue(ctx, req, c.cfg.WriteOverflowPolicy != WriteDropNewest); err != nil {
		wire.ReleaseBuf(bp)
		return false, nil, err
	}
	// The writer owns bp now; it releases it after the write.
	if !waitForFlush {
		return true, nil, nil
	}
	return true, answer, nil
}

// assignOutboundAlias replaces opts.Topic with an alias registered on cs,
// or registers a new one while the broker's budget lasts, reporting the
// topic it registered. The caller holds cs.aliasSlot until the packet is
// admitted, and undoes a registration whose packet was not.
func assignOutboundAlias(cs *connState, opts *wire.PublishOpts) (topic string, registered bool) {
	if alias, ok := cs.outAliasMap[opts.Topic]; ok {
		opts.TopicAlias = alias
		opts.Topic = ""
		return "", false
	}
	if cs.outAliasNext >= cs.info.TopicAliasMaximum {
		return "", false
	}
	cs.outAliasNext++
	cs.outAliasMap[opts.Topic] = cs.outAliasNext
	opts.TopicAlias = cs.outAliasNext
	return opts.Topic, true
}

// publishQoSReliable starts a QoS 1/2 exchange and waits for the
// broker's answer. If ctx ends before the PUBLISH was ever handed to a
// connection the message is withdrawn; after that it may still be
// delivered. When the span ends first, the session engine ends the wait
// (see endSpan): a select on the span's shutdown would have every
// waiting publisher lock that one channel, twice per publish.
func (c *Client) publishQoSReliable(ctx context.Context, opts wire.PublishOpts) error {
	life := c.life.Load()
	flow, link, err := c.startReliable(ctx, opts, exchange{})
	if err != nil {
		return err
	}
	if link != nil {
		// The caller waits for its write anyway, so it may make it
		// itself. A failed marker is harmless: a flow nothing sent is
		// withdrawn below when ctx ends, and the next connection sends
		// any other.
		_ = link.SendOrdered(ctx, flow.Seq())
	}
	// A span that ended before the flow was registered ended the waits
	// before this one began.
	if err = life.endedErr(); err != nil {
		return err
	}
	select {
	case <-flow.Done():
		return flowOutcome(life, flow)
	case <-ctx.Done():
		c.engine.Withdraw(flow)
		return ctx.Err()
	}
}

// flowOutcome is what a QoS 1/2 publish reports once flow's Done has
// closed: the broker's answer, or, when the end of the span the flow was
// started in ended the wait first (see endSpan), what that end gives.
func flowOutcome(life *lifecycle, flow *inflight.Out) error {
	err := flow.Err()
	if errors.Is(err, inflight.ErrInterrupted) {
		return life.closedErr()
	}
	return err
}

// exchange is what a QoS 1/2 publish is started with besides its packet.
type exchange struct {
	// ref and settle make the exchange durable for a QueuePublisher; see
	// [inflight.Message].
	ref    []byte
	settle inflight.SettleFunc
	// onDone runs when the wait on the flow ends; see
	// [inflight.Message.OnDone].
	onDone func(error)
	// beforeWait queues the marker for flows the caller registered
	// before this one; see [inflight.Message.BeforeWait].
	beforeWait func()
}

// placeholderPacketID is the identifier a QoS 1/2 PUBLISH is encoded
// with before the session engine writes the one it allocates into it.
const placeholderPacketID = 1

// startReliable encodes the PUBLISH once and registers the flow with the
// session engine, which allocates its packet identifier and stores it
// first when a Store is configured. The flow belongs to the session from
// registration on: a dropped connection resends it, and a session loss
// applies the [SessionLossPolicy]. On the live connection, link (nil
// when there is none), the flow is sent once the caller's marker, from
// SendOrdered or QueueOrdered, releases it behind the caller's earlier
// writes.
func (c *Client) startReliable(ctx context.Context, opts wire.PublishOpts, x exchange) (*inflight.Out, inflight.Link, error) {
	// A QoS 1/2 message may be resent on a later connection, where the
	// broker no longer knows any alias, so it always carries its topic.
	if opts.TopicAlias != 0 {
		if opts.Topic == "" {
			return nil, nil, fmt.Errorf("%w: QoS %d needs a topic name, not only an alias", ErrTopicAliasInvalid, opts.QoS)
		}
		opts.TopicAlias = 0
	}
	opts.PacketID = placeholderPacketID
	opts.Dup = false

	packet, err := wire.MarshalPublish(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("mqttv5: encode PUBLISH: %w", err)
	}
	if cs := c.cur.Load(); cs != nil {
		if err = checkPacketSize(cs, len(packet)); err != nil {
			return nil, nil, err
		}
	}
	var expiresAt time.Time
	if opts.MessageExpiryInterval != nil {
		expiresAt = c.cfg.clock.Now().Add(time.Duration(*opts.MessageExpiryInterval) * time.Second)
	}

	flow, link, err := c.engine.Register(ctx, inflight.Message{
		QoS: opts.QoS, Packet: packet, ExpiresAt: expiresAt,
		Ref: x.ref, Settle: x.settle, OnDone: x.onDone, BeforeWait: x.beforeWait,
	}, c.done())
	switch {
	case errors.Is(err, inflight.ErrStopped):
		return nil, nil, ErrClosed
	case errors.Is(err, inflight.ErrIDsExhausted):
		return nil, nil, fmt.Errorf("mqttv5: allocate packet id: %w", err)
	case err != nil:
		return nil, nil, fmt.Errorf("mqttv5: store outbound publish: %w", err)
	}
	return flow, link, nil
}

// errNoHealthyPublishers is the pool's signal that no member could send
// a message; [Client.Publish] then sends it on the main connection,
// counted in Stats.PoolFallbacks.
var errNoHealthyPublishers = errors.New("mqttv5: no healthy publishers in pool")

// notSent reports whether a Publish error guarantees the message never
// reached the network, so another connection may send it instead.
func notSent(err error) bool {
	return errors.Is(err, ErrNotConnected) || errors.Is(err, ErrWriteQueueFull)
}

// publisherPool is a set of N publish-only Clients. Each member has
// its own connection, transient session and supervisor, so QoS 1/2 acks
// land on the member that emitted the publish. The parent's SetBrokers,
// Reauthenticate and Stats cover every member.
//
// Spec-compliant but easy to miss:
//
//   - Ordering. MQTT v5 §4.6 in-order delivery is per-ClientID.
//     Round-robin across members can reorder publishes to the same
//     topic. For ordered topics, use
//     WithPublisherPoolRouting(PoolRoutingHashByTopic).
//   - QoS 1/2 across reconnect. Pool members connect with
//     SessionExpiry=0 (and CleanStart=true the first time), so a
//     member's session ends with its connection and a mid-flight drop
//     becomes at-least-once with possible duplicates. Use the main
//     connection for ledger-style work.
//
// What is propagated to pool members from the parent Config:
//
//   - Wire layer: BrokerURLs (and later SetBrokers calls),
//     Username/Password, TLSConfig, Dialer,
//     DialFunc (required for WebSocket / proxy transports),
//     Authenticator.
//   - CONNECT v5 properties: ReceiveMaximum, MaximumPacketSize,
//     InboundTopicAliasMaximum, RequestResponseInformation,
//     RequestProblemInformation, ConnectUserProperties.
//   - Timing & sizing: KeepAlive (or WithoutKeepAlive),
//     ConnectTimeout, PingTimeout, DisconnectFlushTimeout,
//     ReadBufferSize, WriteQueueSize, WriteBatch, WriteOverflowPolicy,
//     ReconnectBackoff, PublishMode.
//   - Publish behaviour: MaxInflight, QoSDowngrade, SessionLossPolicy,
//     OutboundTopicAliases, LenientDecoding, FollowServerRedirects
//     (each member follows a redirect on its own connection).
//   - ConnectPacketBuilder (so per-attempt credential rotation
//     covers pool members too).
//   - StatsEnabled (pool-member Stats() reports independently).
//
// What is NOT propagated (parent is the canonical lifecycle holder):
//
//   - Will. WithWill on the parent is only emitted from the parent's
//     session.
//   - Lifecycle callbacks: OnConnectionUp, OnConnectionDown,
//     OnConnectError, OnReconnectAttempt, OnServerDisconnect,
//     OnServerRedirect.
//     Members manage their own reconnect silently; observe the
//     parent for application-visible lifecycle events.
//   - CleanStartOnReconnect — with SessionExpiry=0 there is no
//     session for a member's reconnect to resume, whatever it asks.
//   - RetryInitialConnect — pool members always retry a failed first
//     CONNECT in the background, as after a lost connection: the
//     parent is connected, so a member's refusal is taken as passing
//     (a connection-rate limit, a busy broker), and publishes fall
//     back to the other members and the parent meanwhile.
//   - Store. Each pool member uses an in-memory session by default;
//     CleanStart=true makes durable session storage moot.
type publisherPool struct {
	members []*Client
	cursor  atomic.Uint64
	routing PoolRoutingPolicy
	logger  *slog.Logger

	// calls counts the Publish calls running through the pool, with
	// poolClosing set while the client disconnects; the call that ends
	// the last of them then closes drained.
	calls   atomic.Int64
	drained atomic.Pointer[chan struct{}]
}

// poolClosing marks publisherPool.calls while the pool admits no calls.
const poolClosing = 1 << 62

// newPublisherPool constructs the pool but does not connect it. Pool
// members inherit the parent's broker URL + credentials + TLS config
// (or TLS config function) + keepalive. Their ClientID is suffixed (-pub-1, -pub-2, ...) for
// per-member uniqueness.
func newPublisherPool(parent *Config, size int, logger *slog.Logger) (*publisherPool, error) {
	clientIDFn := parent.PublisherPoolClientIDFn
	if clientIDFn == nil {
		clientIDFn = defaultPoolClientIDFn
	}
	members := make([]*Client, 0, size)
	for i := 1; i <= size; i++ {
		opts := []Option{
			WithBrokers(parent.BrokerURLs...),
			WithFailoverOrder(parent.FailoverOrder),
			WithClientID(clientIDFn(parent.ClientID, i)),
			// A member's session ends with its connection (Session
			// Expiry 0) — see the publisherPool type doc.
			WithCleanStart(true),
			WithSessionExpiry(0),
			WithRetryInitialConnect(),
			WithConnectTimeout(parent.ConnectTimeout),
			WithPingTimeout(parent.PingTimeout),
			WithDisconnectFlushTimeout(parent.DisconnectFlushTimeout),
			WithReconnectBackoff(parent.ReconnectBackoff),
			WithLogger(logger.With("component", "publisher-pool", "member", i)),
			WithPublishMode(parent.PublishMode),
			WithSessionLossPolicy(parent.SessionLossPolicy),
			WithReadBufferSize(parent.ReadBufferSize),
			WithWriteQueueSize(parent.WriteQueueSize),
			WithWriteBatch(parent.WriteBatchMax),
			WithWriteOverflowPolicy(parent.WriteOverflowPolicy),
			WithMaximumPacketSize(parent.MaximumPacketSize),
			WithInboundTopicAliasMaximum(parent.InboundTopicAliasMaximum),
			WithRequestResponseInformation(parent.RequestResponseInformation),
			WithRequestProblemInformation(parent.RequestProblemInformation),
		}
		if parent.keepAliveDisabled {
			opts = append(opts, WithoutKeepAlive())
		} else {
			opts = append(opts, WithKeepAlive(parent.KeepAlive))
		}
		if parent.Username != "" {
			opts = append(opts, WithCredentials(parent.Username, parent.Password))
		}
		if parent.TLSConfig != nil {
			opts = append(opts, WithTLSConfig(parent.TLSConfig))
		}
		if parent.TLSConfigFunc != nil {
			opts = append(opts, WithTLSConfigFunc(parent.TLSConfigFunc))
		}
		if parent.Dialer != nil {
			opts = append(opts, WithDialer(parent.Dialer))
		}
		if parent.DialFunc != nil {
			// Required for WebSocket / proxy / custom transport — the
			// pool members must reach the broker via the same dial
			// path as the parent.
			opts = append(opts, WithDialFunc(parent.DialFunc))
		}
		if parent.Authenticator != nil {
			opts = append(opts, WithAuthenticator(parent.Authenticator))
		}
		if parent.ReceiveMaximum != 0 {
			opts = append(opts, WithReceiveMaximum(parent.ReceiveMaximum))
		}
		if parent.MaxInflight != 0 {
			opts = append(opts, WithMaxInflight(parent.MaxInflight))
		}
		if len(parent.ConnectUserProperties) > 0 {
			opts = append(opts, WithConnectUserProperties(parent.ConnectUserProperties))
		}
		if parent.ConnectPacketBuilder != nil {
			opts = append(opts, WithConnectPacketBuilder(parent.ConnectPacketBuilder))
		}
		if parent.StatsEnabled {
			opts = append(opts, WithStats())
		}
		if parent.QoSDowngrade {
			opts = append(opts, WithQoSDowngrade())
		}
		if parent.OutboundTopicAliases {
			opts = append(opts, WithOutboundTopicAliases())
		}
		if parent.LenientDecoding {
			opts = append(opts, WithLenientDecoding())
		}
		if parent.FollowServerRedirects {
			opts = append(opts, WithFollowServerRedirects())
		}
		opts = append(opts, func(c *Config) error {
			c.clock = parent.clock
			return nil
		})
		m, err := New(opts...)
		if err != nil {
			return nil, fmt.Errorf("mqttv5: pool member %d: %w", i, err)
		}
		members = append(members, m)
	}
	return &publisherPool{
		members: members,
		routing: parent.PublisherPoolRouting,
		logger:  logger,
	}, nil
}

// connect dials every pool member at once and waits, bounded by ctx, for
// each one's first attempt. A member that fails keeps retrying on its own
// supervisor, so an error means ctx ended or the client stopped first;
// the pool stays usable either way.
func (p *publisherPool) connect(ctx context.Context) error {
	p.calls.And(^poolClosing)
	p.drained.Store(nil)
	// A member whose first attempt ctx cut short would end instead of
	// retrying in the background, leaving the client without its pool:
	// members connect on a context ctx cannot cancel, each attempt bounded
	// by the connect timeout, and ctx bounds only the wait for them.
	done := make(chan error, 1)
	go func() {
		done <- p.each(func(m *Client) error { return m.Connect(context.WithoutCancel(ctx)) })
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enter admits a Publish or PublishAsync call unless the pool is
// closing. It counts the
// call first and backs out when closing is set, so admission costs one
// atomic add however many goroutines publish.
func (p *publisherPool) enter() bool {
	if p.calls.Add(1)&poolClosing != 0 {
		p.exit()
		return false
	}
	return true
}

// exit ends a call enter counted. Calls backing out of a closing pool
// can reach zero more than once, so the channel is taken before it is
// closed.
func (p *publisherPool) exit() {
	if p.calls.Add(-1) == poolClosing {
		if d := p.drained.Swap(nil); d != nil {
			close(*d)
		}
	}
}

// drain stops admitting calls and waits, bounded by ctx, for the
// Publish calls running to return and the PublishAsync results to
// complete. A member's session ends with its connection, so a publish it
// still awaits an acknowledgement for has nothing to finish it once the
// member disconnects.
func (p *publisherPool) drain(ctx context.Context) error {
	done := make(chan struct{})
	p.drained.Store(&done)
	if p.calls.Or(poolClosing)&^poolClosing == 0 {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// publish sends a Publish call's message through a member (see route).
func (p *publisherPool) publish(ctx context.Context, opts PublishOptions) error {
	if !p.enter() {
		return ErrClosed
	}
	defer p.exit()
	return p.route(opts, func(m *Client) error { return m.Publish(ctx, opts) })
}

// publishAsync starts a PublishAsync call's message on a member (see
// route). The call counts as running until its result completes, so
// drain waits for it.
func (p *publisherPool) publishAsync(ctx context.Context, opts PublishOptions) (*PublishResult, error) {
	if !p.enter() {
		return nil, ErrClosed
	}
	var r *PublishResult
	err := p.route(opts, func(m *Client) error {
		var err error
		r, err = m.publishAsync(ctx, opts, p.exit)
		return err
	})
	if err != nil {
		p.exit()
		return nil, err
	}
	return r, nil
}

// route picks a starting member per the routing policy and tries
// members from there until send reports an outcome. It moves on only
// when a member could not send the message at all (see notSent): any
// other outcome, a broker refusal or ctx ending included, is the
// caller's, so one message never goes out on two connections. Returns
// errNoHealthyPublishers when no member could send it.
func (p *publisherPool) route(opts PublishOptions, send func(m *Client) error) error {
	n := len(p.members)
	start := p.startIndex(opts)
	var lastErr error
	for i := range n {
		m := p.members[(start+uint64(i))%uint64(n)]
		err := send(m)
		if !notSent(err) {
			return err
		}
		lastErr = err
		p.logger.Debug("mqttv5: pool member could not send; trying the next",
			slog.String("client_id", m.ClientID()), slog.Any("error", err))
	}
	return fmt.Errorf("%w: %w", errNoHealthyPublishers, lastErr)
}

// startIndex returns the member-index to start the probe loop at,
// chosen per the configured routing policy.
func (p *publisherPool) startIndex(opts PublishOptions) uint64 {
	switch p.routing {
	case PoolRoutingHashByTopic:
		h := fnv.New32a()
		_, _ = h.Write([]byte(opts.Topic))
		return uint64(h.Sum32())
	default:
		return p.cursor.Add(1)
	}
}

// disconnect tears down every pool member at once, joining any errors.
func (p *publisherPool) disconnect(ctx context.Context) error {
	return p.each(func(m *Client) error { return m.Disconnect(ctx) })
}

// each runs op on every member concurrently and joins the errors.
func (p *publisherPool) each(op func(*Client) error) error {
	errs := make([]error, len(p.members))
	var wg sync.WaitGroup
	for i, m := range p.members {
		wg.Go(func() {
			if err := op(m); err != nil {
				errs[i] = fmt.Errorf("pool member %d: %w", i+1, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}
