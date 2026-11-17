package mqttv5

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/wire"
)

// ErrQoS0NotQueueable is returned by [QueuePublisher.Publish] for a QoS 0
// message: with no acknowledgement there is nothing to keep it queued
// for.
var ErrQoS0NotQueueable = errors.New("mqttv5: QueuePublisher requires QoS >= 1")

// QueueIDProperty is the user property [WithQueueIdempotencyKey] adds to
// each queued message, holding its [QueueEntry.ID].
const QueueIDProperty = "mqttv5-msg-id"

const (
	// DefaultQueueWindow is how many queued messages a QueuePublisher
	// holds at once when [WithQueueWindow] is not set (see there); under
	// [DropOldest], at most half of [WithQueueMaxSize].
	DefaultQueueWindow = 4096
	// DefaultQueueWindowBytes bounds the total [QueueEntry.Size] of the
	// messages a QueuePublisher holds at once when [WithQueueWindowBytes]
	// is not set.
	DefaultQueueWindowBytes = 8 << 20
)

// DefaultQueueRetryBackoff spaces the attempts of a message whose
// publish failed for a reason that may pass (a refusal such as 0x97
// Quota exceeded, a store error): 1 s doubling to 1 min, each delay
// drawn from its upper half.
var DefaultQueueRetryBackoff = ExponentialBackoff{Initial: time.Second, Max: time.Minute, Jitter: 0.5}

// QueuePublisher accepts QoS 1/2 messages into a [PublisherQueue] and
// publishes them in the background, many at a time.
//
// [QueuePublisher.Publish] returns once the message is stored. A drain
// goroutine publishes the queue's messages in order while the client is
// connected, holding up to [WithQueueWindow] messages and
// [WithQueueWindowBytes] bytes of them at once, and a recorder
// goroutine removes them from the queue once the broker has accepted
// them: the outcomes that come in together leave in one [PublisherQueue]
// Ack. A message holds its place in the window until it has left the
// queue. A refusal that
// cannot pass (see
// [WithQueueClassifier]) and a message that expired go to the
// [WithDeadLetter] callback and are removed; other failures are retried
// with [WithQueueRetryBackoff], after the messages behind them if need
// be, so one bad message never holds up the rest.
//
// Each message has one MQTT exchange at a time. Its [QueueEntry.ID] is
// stored with the exchange in the client's session ([WithStore]), and the
// message is removed from the queue before the session record, so with
// a persistent queue and store a restarted process continues an
// interrupted exchange instead of publishing the message again:
// QoS 2 stays exactly-once across a crash, and QoS 1 is at most resent
// with DUP=1. When removing a finished message from the queue fails, the
// session keeps the exchange's record and the removal is retried with
// [WithQueueRetryBackoff], in this process or after a restart, without
// publishing the message again. A message is published again as new only
// when the broker lost the session (see [SessionLossPolicy]).
//
// The QueuePublisher publishes on the client's own connection, never
// through [WithPublisherPool].
type QueuePublisher struct {
	client *Client
	queue  PublisherQueue
	cfg    queueConfig
	logger *slog.Logger
	clk    clock.Clock

	// claimMu keeps DropOldest from evicting an entry the drain has
	// claimed: when the queue can evict (see queueConfig.evicts), Publish
	// holds it shared around Enqueue, and the drain holds it exclusively
	// while it peeks and advances claimed. Without eviction Publish leaves
	// it alone, so the drain never waits for an Enqueue's write.
	claimMu sync.RWMutex
	claimed uint64 // highest Seq handed to pending
	// readFailures counts the queue reads that failed in a row, which
	// space the drain's retries (see claim).
	readFailures int

	mu      sync.Mutex
	pending map[uint64]*queuedMessage // claimed and unfinished, at most cfg.window
	// pendingBytes is the total size of pending, held to cfg.windowBytes
	// but for the message that crossed it.
	pendingBytes int
	// windowFull is set when the drain last found no room in the window:
	// a place the recorder frees then has to wake it. Otherwise the drain
	// claimed all the queue had, and a free place changes nothing for it.
	windowFull bool
	// outcomes are the outcomes of finished exchanges, waiting for the
	// recorder; running counts the exchanges started whose outcome it has
	// not recorded. Once the recorder has stopped, stopped is set and an
	// outcome is left to the session.
	outcomes []exchangeOutcome
	running  int
	stopped  bool

	notify    chan struct{} // wakes the drain
	finished  chan struct{} // wakes the recorder
	closing   chan struct{}
	drained   chan struct{}   // closed when the drain has returned
	done      chan struct{}   // closed when the recorder has returned, after the drain
	ctx       context.Context // canceled by Close
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// exchangeOutcome is how a message's exchange ended. done tells the
// session once the recorder has recorded it, which lets the session
// forget the exchange (see [inflight.SettleFunc]).
type exchangeOutcome struct {
	m    *queuedMessage
	err  error
	done func(error)

	// What the recorder makes of it: m leaves the queue, or is started
	// again at retryAt.
	gone    bool
	retryAt time.Time
}

type queuedMessage struct {
	entry    QueueEntry
	size     int       // entry.Size(), counted in pendingBytes
	inFlight bool      // an exchange is running
	attempts int       // exchanges started, or outcomes recorded again, in this process
	retryAt  time.Time // when to start the next; zero: once connected
	// accepted and deadLettered are set once the broker accepted the
	// message or it was passed to the dead-letter callback, so retrying a
	// failed removal does neither twice.
	accepted     bool
	deadLettered bool
}

type queueConfig struct {
	window      int
	windowSet   bool
	windowBytes int
	maxSize     int
	dropPolicy  DropPolicy
	ttl         time.Duration
	deadLetter  func(QueueEntry, error)
	permanent   func(error) bool
	backoff     Backoff
	idempotency bool
}

// evicts reports whether Enqueue may remove entries to make room.
func (c queueConfig) evicts() bool { return c.maxSize > 0 && c.dropPolicy == DropOldest }

// defaultWindow is the window when WithQueueWindow is not set. A queue
// that evicts gets at most half its bound, so that eviction always finds
// messages the window does not hold.
func (c queueConfig) defaultWindow() int {
	if c.evicts() {
		return max(1, min(DefaultQueueWindow, c.maxSize/2))
	}
	return DefaultQueueWindow
}

// QueueOption customises a QueuePublisher.
type QueueOption func(*queueConfig)

// WithQueueWindow sets how many messages a QueuePublisher holds at once,
// each from when the drain takes it until it has left the queue: the
// broker's round trip and, with a durable queue, the sync of its
// removal. The broker's Receive Maximum and [WithMaxInflight] still bound
// the messages sent and not yet acknowledged. Throughput approaches
// window ÷ (round trip + removal), and a group commit removes about half
// the window per sync, so a slow disk needs a larger window than the
// round trip alone does.
//
// The window's messages are held in memory, which [WithQueueWindowBytes]
// bounds as well. Without [WithStore], a restart publishes again the ones
// the broker had accepted that were still in the queue, at most the
// window; under [DropOldest] they cannot be evicted. Default
// [DefaultQueueWindow], or under DropOldest at most half of
// [WithQueueMaxSize].
func WithQueueWindow(n int) QueueOption {
	return func(c *queueConfig) { c.window, c.windowSet = n, true }
}

// WithQueueWindowBytes bounds the window by the total [QueueEntry.Size]
// of its messages as well as by their number ([WithQueueWindow]): the
// drain takes no more once the messages it holds reach n bytes. It
// always takes the next message when it holds none, so a message larger
// than n is published on its own. While a message is published and
// stored the client also holds its encoded packet and write buffers: the
// window's live memory measured two to three times n. Default
// [DefaultQueueWindowBytes]; 0 means no byte bound.
func WithQueueWindowBytes(n int) QueueOption {
	return func(c *queueConfig) { c.windowBytes = n }
}

// WithQueueMaxSize bounds the queue at n entries; at the bound
// [WithQueueDropPolicy] decides. 0 (default) means unbounded.
func WithQueueMaxSize(n int) QueueOption {
	return func(c *queueConfig) { c.maxSize = n }
}

// WithQueueDropPolicy decides what Publish does when the queue is at
// [WithQueueMaxSize]: [DropNewest] (default) returns [ErrQueueFull];
// [DropOldest] removes the oldest messages not yet being published,
// passes them to the dead-letter callback with ErrQueueFull, and stores
// the new one.
func WithQueueDropPolicy(p DropPolicy) QueueOption {
	return func(c *queueConfig) { c.dropPolicy = p }
}

// WithQueueTTL limits how long a message may wait: it expires d after
// Publish (or earlier, at its own Message Expiry Interval). An expired
// message goes to the dead-letter callback with [ErrMessageExpired];
// one that is sent carries the time it has left, in whole seconds
// rounded up, as its Message Expiry Interval.
func WithQueueTTL(d time.Duration) QueueOption {
	return func(c *queueConfig) { c.ttl = d }
}

// WithDeadLetter receives every message removed without the broker
// accepting it, with the reason: a refusal [WithQueueClassifier] calls
// permanent, [ErrMessageExpired], or [ErrQueueFull] for an eviction. It
// is called from internal goroutines, possibly concurrently, before the
// message leaves the queue; it must not block.
func WithDeadLetter(fn func(QueueEntry, error)) QueueOption {
	return func(c *queueConfig) { c.deadLetter = fn }
}

// WithQueueClassifier decides which failed publishes are dead-lettered
// rather than retried: fn reports whether err is permanent. The default,
// [PermanentPublishError], treats the broker's refusals for the message
// itself as permanent and everything else as passing.
func WithQueueClassifier(fn func(err error) bool) QueueOption {
	return func(c *queueConfig) { c.permanent = fn }
}

// WithQueueRetryBackoff spaces the attempts of a message whose publish
// failed for a reason that may pass, and the drain's attempts to read a
// queue that failed. [NewQueuePublisher] rejects an
// invalid [ExponentialBackoff] or [ConstantBackoff]. Default
// [DefaultQueueRetryBackoff].
func WithQueueRetryBackoff(b Backoff) QueueOption {
	return func(c *queueConfig) { c.backoff = b }
}

// WithQueueIdempotencyKey adds each message's [QueueEntry.ID] as the
// [QueueIDProperty] user property, so consumers can recognise a message
// published again after a session loss.
func WithQueueIdempotencyKey() QueueOption {
	return func(c *queueConfig) { c.idempotency = true }
}

// PermanentPublishError reports whether err means the broker will never
// accept the message as it is: PUBACK/PUBREC 0x87 Not authorized, 0x90
// Topic Name invalid, 0x95 Packet too large, 0x99 Payload format
// invalid, 0x9A Retain not supported, 0x9B QoS not supported, or the
// equivalent checks against the broker's CONNACK limits and MQTT's rules
// before sending. 0x97 Quota exceeded and other failures may pass.
func PermanentPublishError(err error) bool {
	var rc *ReasonCodeError
	if errors.As(err, &rc) {
		switch rc.Code {
		case wire.ReasonNotAuthorized, wire.ReasonTopicNameInvalid, wire.ReasonPacketTooLarge,
			wire.ReasonPayloadFormatInvalid, wire.ReasonRetainNotSupported, wire.ReasonQoSNotSupported:
			return true
		}
		return false
	}
	for _, e := range []error{ErrQoSNotSupported, ErrRetainNotSupported, ErrPacketTooLarge,
		ErrInvalidTopic, ErrInvalidField, ErrFieldTooLong, ErrTopicAliasInvalid} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// NewQueuePublisher starts a QueuePublisher draining queue through
// client. Entries already in the queue are published in order once the
// client is connected; nothing is removed by construction.
func NewQueuePublisher(client *Client, queue PublisherQueue, opts ...QueueOption) (*QueuePublisher, error) {
	cfg := queueConfig{windowBytes: DefaultQueueWindowBytes, dropPolicy: DropNewest, permanent: PermanentPublishError, backoff: DefaultQueueRetryBackoff}
	for _, opt := range opts {
		opt(&cfg)
	}
	if !cfg.windowSet {
		cfg.window = cfg.defaultWindow()
	}
	switch {
	case cfg.window < 1:
		return nil, fmt.Errorf("mqttv5: WithQueueWindow(%d): must be at least 1", cfg.window)
	case cfg.windowBytes < 0:
		return nil, fmt.Errorf("mqttv5: WithQueueWindowBytes(%d): must not be negative", cfg.windowBytes)
	case cfg.maxSize < 0:
		return nil, fmt.Errorf("mqttv5: WithQueueMaxSize(%d): must not be negative", cfg.maxSize)
	case cfg.ttl < 0:
		return nil, fmt.Errorf("mqttv5: WithQueueTTL(%v): must not be negative", cfg.ttl)
	case cfg.dropPolicy != DropNewest && cfg.dropPolicy != DropOldest:
		return nil, fmt.Errorf("mqttv5: WithQueueDropPolicy(%d): unknown policy", cfg.dropPolicy)
	case cfg.permanent == nil:
		return nil, errors.New("mqttv5: WithQueueClassifier needs a function")
	}
	if err := validateBackoff(cfg.backoff); err != nil {
		return nil, fmt.Errorf("mqttv5: WithQueueRetryBackoff: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &QueuePublisher{
		client:   client,
		queue:    queue,
		cfg:      cfg,
		logger:   client.cfg.Logger.With("component", "queue-publisher"),
		clk:      client.cfg.clock,
		pending:  make(map[uint64]*queuedMessage),
		notify:   make(chan struct{}, 1),
		finished: make(chan struct{}, 1),
		closing:  make(chan struct{}),
		drained:  make(chan struct{}),
		done:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}
	go p.run()
	go p.recorder()
	return p, nil
}

// Publish stores a copy of opts in the queue and returns; the drain
// publishes it. The message must be valid to send (QoS 1 or 2, a topic
// name, no Topic Alias): an invalid one is refused here rather than
// dead-lettered later.
func (p *QueuePublisher) Publish(ctx context.Context, opts PublishOptions) error {
	select {
	case <-p.closing:
		return ErrQueueClosed
	default:
	}
	if opts.QoS == 0 {
		return ErrQoS0NotQueueable
	}
	if opts.TopicAlias != 0 {
		return fmt.Errorf("%w: a queued message carries its topic name, not an alias", ErrTopicAliasInvalid)
	}
	id, err := newQueueID()
	if err != nil {
		return err
	}
	opts = opts.clone()
	if p.cfg.idempotency {
		opts.UserProperties = append(opts.UserProperties, UserProperty{Key: QueueIDProperty, Value: id})
	}
	if _, err = EncodePublish(opts, 1); err != nil {
		return err
	}
	now := p.clk.Now()
	e := QueueEntry{ID: id, EnqueuedAt: now}
	if opts.MessageExpiryInterval != nil {
		e.ExpiresAt = now.Add(time.Duration(*opts.MessageExpiryInterval) * time.Second)
	}
	if p.cfg.ttl > 0 && (e.ExpiresAt.IsZero() || now.Add(p.cfg.ttl).Before(e.ExpiresAt)) {
		e.ExpiresAt = now.Add(p.cfg.ttl)
	}
	opts.MessageExpiryInterval = nil
	e.Publish = opts

	limit := QueueLimit{Max: p.cfg.maxSize, Policy: p.cfg.dropPolicy}
	var evicted []QueueEntry
	if p.cfg.evicts() {
		p.claimMu.RLock()
		limit.Keep = p.claimed
		_, evicted, err = p.queue.Enqueue(ctx, e, limit)
		p.claimMu.RUnlock()
	} else {
		_, evicted, err = p.queue.Enqueue(ctx, e, limit)
	}
	for _, ev := range evicted {
		p.deadLetter(ev, ErrQueueFull)
	}
	if err != nil {
		return err
	}
	p.signal()
	return nil
}

// Close stops publishing, waits until ctx for the exchanges in flight to
// finish and their outcomes to be recorded, and closes the queue.
// Messages whose exchange is still running stay in the queue and in the
// session; the next QueuePublisher on them continues those exchanges.
func (p *QueuePublisher) Close(ctx context.Context) error {
	p.closeOnce.Do(func() { close(p.closing) })
	var err error
	select {
	case <-p.done:
	case <-ctx.Done():
		err = fmt.Errorf("mqttv5: QueuePublisher.Close: messages still in flight: %w", ctx.Err())
	}
	// Whatever the drain and the recorder are waiting on gives up.
	p.cancel()
	<-p.done
	if cerr := p.queue.Close(); err == nil {
		err = cerr
	}
	return err
}

func (p *QueuePublisher) signal() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// run is the drain goroutine: while connected it starts messages whose
// turn has come and claims new ones until the window is full, then
// sleeps until something changes. It may block meanwhile — on a packet
// identifier, the deletion of a retried message's earlier record, room
// in the write queue — which is why the
// outcomes that free those are the recorder's to record, not its own.
func (p *QueuePublisher) run() {
	defer close(p.drained)
	timer := p.clk.NewTimer(time.Hour)
	timer.Stop()
	for {
		changed := p.client.connChanged()
		var retry time.Time
		if p.client.cur.Load() != nil {
			retry = p.fill()
		}
		var wake <-chan time.Time
		if !retry.IsZero() {
			timer.Reset(max(retry.Sub(p.clk.Now()), 0))
			wake = timer.C()
		}
		select {
		case <-p.closing:
			timer.Stop()
			return
		case <-p.notify:
		case <-changed:
		case <-wake:
		}
		timer.Stop()
	}
}

// recorder is the goroutine that records the outcomes of finished
// exchanges, all that came in since it last ran together (see record).
// Once Close is called it goes on until the drain has returned and every
// exchange started has its outcome recorded, or until Close gives up.
func (p *QueuePublisher) recorder() {
	defer p.stop()
	drained := p.drained
	for {
		select {
		case <-p.finished:
		case <-drained:
			drained = nil
		case <-p.ctx.Done():
			return
		}
		p.record()
		if drained == nil && p.idle() {
			return
		}
	}
}

// idle reports whether every exchange started has its outcome recorded.
func (p *QueuePublisher) idle() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running == 0
}

// stop ends the recorder once the drain has returned. The outcomes it
// has not recorded, and any that come later, are left to the session,
// which keeps those exchanges for the next QueuePublisher on the queue.
func (p *QueuePublisher) stop() {
	<-p.drained
	p.mu.Lock()
	p.stopped = true
	left := p.outcomes
	p.outcomes = nil
	p.mu.Unlock()
	for _, o := range left {
		o.done(ErrQueueClosed)
	}
	close(p.done)
}

// fill starts every pending message that is due and claims more from the
// queue while the window has room, until the connection or the
// QueuePublisher goes away. It returns when the next retry is due — of a
// message, or of reading the queue — or zero.
func (p *QueuePublisher) fill() time.Time {
	rel := &release{ctx: p.ctx}
	flush := rel.flush
	for {
		due, next, room := p.due()
		for _, m := range due {
			rel.add(p.start(m, flush))
		}
		if room.Entries <= 0 || p.client.cur.Load() == nil || p.ctx.Err() != nil {
			rel.flush()
			return next
		}
		claimed, readRetry := p.claim(room)
		for _, m := range claimed {
			rel.add(p.start(m, flush))
		}
		rel.flush()
		if !readRetry.IsZero() && (next.IsZero() || readRetry.Before(next)) {
			next = readRetry
		}
		if len(claimed) == 0 || p.client.cur.Load() == nil {
			return next
		}
	}
}

// release lets the exchanges fill started since the last go out
// together, with one marker: the last one's, on the connection it was
// registered on. It releases every flow before it there, and a flow
// registered before that connection came up went out when it did.
type release struct {
	ctx  context.Context
	link inflight.Link
	seq  uint64
}

func (r *release) add(seq uint64, link inflight.Link) {
	if link != nil {
		r.link, r.seq = link, seq
	}
}

// flush sends the marker, if there is one. The flows it releases are
// the whole batch, so on an idle TCP or Unix connection they go out in
// one write on the drain's goroutine rather than waking the writer's;
// otherwise the writer sends them. A marker that fails leaves the flows
// to the next marker or connection: each is stored with its Ref, and
// Adopt finds it.
func (r *release) flush() {
	if r.link != nil {
		_ = r.link.SendOrdered(r.ctx, r.seq)
		r.link = nil
	}
}

// due returns the pending messages to start now, in queue order, when
// the next one waiting for a retry is due, and the window's free room:
// none once either of its bounds is reached.
func (p *QueuePublisher) due() (due []*queuedMessage, next time.Time, room PeekLimit) {
	now := p.clk.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range p.pending {
		switch {
		case m.inFlight:
		case !m.retryAt.After(now):
			m.inFlight = true
			due = append(due, m)
		case next.IsZero() || m.retryAt.Before(next):
			next = m.retryAt
		}
	}
	slices.SortFunc(due, func(a, b *queuedMessage) int { return cmp.Compare(a.entry.Seq, b.entry.Seq) })
	room.Entries = p.cfg.window - len(p.pending)
	if p.cfg.windowBytes > 0 {
		room.Bytes = p.cfg.windowBytes - p.pendingBytes
		if room.Bytes <= 0 {
			room.Entries = 0
		}
	}
	p.windowFull = room.Entries <= 0
	return due, next, room
}

// claim takes the entries after the last claimed one that room allows
// into pending. When reading the queue fails it returns when to read it
// again, spaced by the retry backoff: nothing else would wake the drain
// for it.
func (p *QueuePublisher) claim(room PeekLimit) (claimed []*queuedMessage, readRetry time.Time) {
	p.claimMu.Lock()
	defer p.claimMu.Unlock()
	entries, err := p.queue.Peek(p.ctx, p.claimed, room)
	if err != nil {
		if errors.Is(err, ErrQueueClosed) || p.ctx.Err() != nil {
			return nil, time.Time{}
		}
		p.readFailures++
		p.logger.Error("reading the queue failed; will retry",
			slog.Int("attempt", p.readFailures), slog.Any("error", err))
		return nil, p.clk.Now().Add(p.cfg.backoff.Delay(p.readFailures - 1))
	}
	p.readFailures = 0
	out := make([]*queuedMessage, len(entries))
	p.mu.Lock()
	for i, e := range entries {
		m := &queuedMessage{entry: e, size: e.Size(), inFlight: true}
		p.pending[e.Seq] = m
		p.pendingBytes += m.size
		out[i] = m
	}
	p.mu.Unlock()
	if len(entries) > 0 {
		p.claimed = entries[len(entries)-1].Seq
	}
	return out, time.Time{}
}

// start begins an exchange for m, which the caller has marked in
// flight. When the session already holds one for m — from an earlier
// process, from before the session store failed, or one whose outcome
// could not be recorded yet — it continues that exchange; otherwise it
// publishes m, and returns the new flow's sequence number and link for
// the marker that releases it (see release). flush queues the marker for
// the flows started before m, should m have to wait for a packet
// identifier.
func (p *QueuePublisher) start(m *queuedMessage, flush func()) (seq uint64, link inflight.Link) {
	e := m.entry
	p.mu.Lock()
	m.attempts++
	p.running++
	p.mu.Unlock()

	settle := func(outcome error, done func(error)) { p.settled(m, outcome, done) }
	if p.client.engine.Adopt([]byte(e.ID), settle) {
		return 0, nil
	}
	now := p.clk.Now()
	if !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt) {
		p.notStarted(m, ErrMessageExpired)
		return 0, nil
	}
	opts := e.Publish
	if !e.ExpiresAt.IsZero() {
		left := uint32((e.ExpiresAt.Sub(now) + time.Second - 1) / time.Second)
		opts.MessageExpiryInterval = &left
	}
	seq, link, err := p.client.publishTracked(p.ctx, opts, exchange{ref: []byte(e.ID), settle: settle, beforeWait: flush})
	if err != nil {
		p.notStarted(m, err)
		return 0, nil
	}
	return seq, link
}

// notStarted acts on err, why m's exchange could not start.
func (p *QueuePublisher) notStarted(m *queuedMessage, err error) {
	p.mu.Lock()
	p.running--
	p.mu.Unlock()
	gone, retryAt := p.decide(m, err)
	if gone {
		if p.remove(m) == nil {
			return
		}
		retryAt = p.removalRetry(m)
	}
	p.reschedule(m, retryAt)
	p.signal()
}

// settled is the Settle of m's exchange: it hands the outcome to the
// recorder. It never blocks.
func (p *QueuePublisher) settled(m *queuedMessage, outcome error, done func(error)) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		done(ErrQueueClosed)
		return
	}
	// One wake for the outcomes the recorder has yet to take.
	wake := len(p.outcomes) == 0
	p.outcomes = append(p.outcomes, exchangeOutcome{m: m, err: outcome, done: done})
	p.mu.Unlock()
	if wake {
		select {
		case p.finished <- struct{}{}:
		default:
		}
	}
}

// record records the outcomes that came in since it last ran. The
// messages they take out of the queue go in one Ack, and only then does
// the session forget the exchanges, so a crash in between leaves each
// exchange to be continued rather than its message to be published
// again. A message to try again is rescheduled once the session has
// forgotten its exchange, so the retry starts a new one.
func (p *QueuePublisher) record() {
	p.mu.Lock()
	outcomes := p.outcomes
	p.outcomes = nil
	p.mu.Unlock()
	if len(outcomes) == 0 {
		return
	}
	var gone []*queuedMessage
	for i := range outcomes {
		o := &outcomes[i]
		if o.gone, o.retryAt = p.decide(o.m, o.err); o.gone {
			gone = append(gone, o.m)
		}
	}
	err := p.remove(gone...)
	// The drain has work when a message waits to start again, or when it
	// found the window full and the removals made room.
	wake := false
	for _, o := range outcomes {
		switch {
		case !o.gone:
			o.done(nil)
			p.reschedule(o.m, o.retryAt)
			wake = true
		case err != nil:
			// The session keeps the exchange; the retry adopts it and
			// gets the outcome again.
			o.done(err)
			p.reschedule(o.m, p.removalRetry(o.m))
			wake = true
		default:
			o.done(nil)
		}
	}
	p.mu.Lock()
	p.running -= len(outcomes)
	wake = wake || p.windowFull
	p.mu.Unlock()
	if wake {
		p.signal()
	}
}

// decide acts on err, the outcome of m's exchange or why it could not
// start, and reports whether m leaves the queue — accepted, or
// dead-lettered — or otherwise when to start it again: at retryAt, or
// once connected when that is zero.
func (p *QueuePublisher) decide(m *queuedMessage, err error) (gone bool, retryAt time.Time) {
	switch {
	case err == nil:
		p.mu.Lock()
		first := !m.accepted
		m.accepted = true
		p.mu.Unlock()
		if first {
			p.client.stats.addPublishSent()
		}
		return true, time.Time{}
	case errors.Is(err, ErrSessionLost):
		// The broker never confirmed it; publish it again as new.
		return false, time.Time{}
	case errors.Is(err, ErrMessageExpired) || p.cfg.permanent(err):
		p.deadLetterOnce(m, err)
		return true, time.Time{}
	case errors.Is(err, ErrNotConnected), errors.Is(err, ErrClosed), p.ctx.Err() != nil:
		return false, time.Time{}
	default:
		p.mu.Lock()
		attempts := m.attempts
		p.mu.Unlock()
		p.logger.Warn("publishing a queued message failed; will retry",
			slog.String("topic", m.entry.Publish.Topic), slog.Int("attempt", attempts), slog.Any("error", err))
		return false, p.clk.Now().Add(p.cfg.backoff.Delay(attempts - 1))
	}
}

// reschedule makes m wait, out of flight, to be started at retryAt.
func (p *QueuePublisher) reschedule(m *queuedMessage, retryAt time.Time) {
	p.mu.Lock()
	m.inFlight, m.retryAt = false, retryAt
	p.mu.Unlock()
}

// removalRetry is when to retry taking m out of the queue after it
// failed.
func (p *QueuePublisher) removalRetry(m *queuedMessage) time.Time {
	p.mu.Lock()
	attempts := m.attempts
	p.mu.Unlock()
	return p.clk.Now().Add(p.cfg.backoff.Delay(attempts - 1))
}

// deadLetterOnce dead-letters m unless it already was; retrying a
// failed removal does not do it twice.
func (p *QueuePublisher) deadLetterOnce(m *queuedMessage, reason error) {
	p.mu.Lock()
	first := !m.deadLettered
	m.deadLettered = true
	p.mu.Unlock()
	if first {
		p.deadLetter(m.entry, reason)
	}
}

func (p *QueuePublisher) deadLetter(e QueueEntry, reason error) {
	p.logger.Warn("dropping a queued message", slog.String("topic", e.Publish.Topic),
		slog.String("id", e.ID), slog.Any("reason", reason))
	if p.cfg.deadLetter != nil {
		p.cfg.deadLetter(e, reason)
	}
}

// remove takes ms out of the queue, in one Ack, and frees their places
// in the window. When the queue fails they stay pending, for the caller
// to reschedule.
func (p *QueuePublisher) remove(ms ...*queuedMessage) error {
	if len(ms) == 0 {
		return nil
	}
	seqs := make([]uint64, len(ms))
	for i, m := range ms {
		seqs[i] = m.entry.Seq
	}
	if err := p.queue.Ack(p.ctx, seqs...); err != nil {
		if p.ctx.Err() == nil {
			p.logger.Error("removing finished messages from the queue failed; will retry",
				slog.Int("messages", len(ms)), slog.String("first_id", ms[0].entry.ID), slog.Any("error", err))
		}
		return err
	}
	p.mu.Lock()
	for _, m := range ms {
		delete(p.pending, m.entry.Seq)
		p.pendingBytes -= m.size
	}
	p.mu.Unlock()
	return nil
}

func newQueueID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mqttv5: message ID: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

var (
	// ErrQueueClosed is returned by a [PublisherQueue] or
	// [QueuePublisher] after Close.
	ErrQueueClosed = errors.New("mqttv5: queue is closed")

	// ErrQueueFull is returned when a bounded queue has no room: under
	// [DropNewest] when it holds its maximum, under [DropOldest] when
	// every entry it holds is already being published.
	ErrQueueFull = errors.New("mqttv5: queue is full")
)

// QueueEntry is one message in a [PublisherQueue].
type QueueEntry struct {
	// Seq is assigned by the queue at Enqueue. It increases strictly
	// within a queue and is never reused.
	Seq uint64

	// ID identifies the message for good. [QueuePublisher] stores it with
	// the session record of the message's exchange, so after a restart
	// it continues that exchange instead of starting another, and sends
	// it as the [QueueIDProperty] user property when
	// [WithQueueIdempotencyKey] is set.
	ID string

	// Publish is the message, QoS 1 or 2. PacketID and Dup are unused;
	// the Message Expiry Interval is carried by ExpiresAt.
	Publish PublishOptions

	EnqueuedAt time.Time

	// ExpiresAt is when the message may no longer be sent: the earlier of
	// its Message Expiry Interval and [WithQueueTTL], counted from
	// EnqueuedAt. Zero means never.
	ExpiresAt time.Time
}

// Size is the length of the message's variable content in bytes: its ID,
// topic, payload and properties. It is what [WithQueueWindowBytes] and
// [PeekLimit.Bytes] count.
func (e QueueEntry) Size() int {
	o := e.Publish
	n := len(e.ID) + len(o.Topic) + len(o.Payload) + len(o.ContentType) + len(o.ResponseTopic) + len(o.CorrelationData)
	for _, up := range o.UserProperties {
		n += len(up.Key) + len(up.Value)
	}
	return n
}

// clone returns a deep copy of e.
func (e QueueEntry) clone() QueueEntry {
	e.Publish = e.Publish.clone()
	return e
}

// QueueLimit bounds a [PublisherQueue] for one Enqueue.
type QueueLimit struct {
	// Max is the most entries the queue may hold; 0 means no bound.
	Max int
	// Policy says what Enqueue does when the queue holds Max entries:
	// [DropNewest] refuses the new entry with [ErrQueueFull];
	// [DropOldest] removes the oldest entries with Seq above Keep to make
	// room.
	Policy DropPolicy
	// Keep is the highest Seq DropOldest must not remove: the entries up
	// to it are being published.
	Keep uint64
}

// PeekLimit bounds the entries one [PublisherQueue] Peek returns.
type PeekLimit struct {
	// Entries is the most entries to return, at least 1.
	Entries int
	// Bytes, when positive, ends the entries returned with the one that
	// brings their total [QueueEntry.Size] to Bytes or more. The first
	// entry is returned whatever its size.
	Bytes int
}

// Reached reports whether entries, of total size bytes, are all that l
// allows: no further entry may follow them.
func (l PeekLimit) Reached(entries, bytes int) bool {
	return entries >= l.Entries || (l.Bytes > 0 && bytes >= l.Bytes)
}

// PublisherQueue stores the messages a [QueuePublisher] has accepted
// until the broker has them. Implementations must be safe for concurrent
// use and must not share memory with their callers: Enqueue copies the
// entry, and the entries they return belong to the caller.
// [github.com/ashtonian/mqttv5/queue/queuetest] checks an implementation
// against this contract.
type PublisherQueue interface {
	// Enqueue stores e, assigning it the next Seq, which it returns.
	// When the queue already holds limit.Max entries it refuses with
	// ErrQueueFull or, under DropOldest, removes the oldest entries above
	// limit.Keep and returns them; the check, the removal and the insert
	// are one atomic step. It returns ErrQueueFull when there is nothing
	// it may remove. Entries become visible to Peek in Seq order: once
	// Peek returns an entry, every stored entry with a lower Seq is
	// visible as well, however many Enqueue calls run at once. The
	// QueuePublisher peeks after the last Seq it took while other
	// goroutines enqueue, and would never see an entry that appeared
	// behind it.
	Enqueue(ctx context.Context, e QueueEntry, limit QueueLimit) (seq uint64, evicted []QueueEntry, err error)

	// Peek returns entries with Seq above after, oldest first, until
	// limit is reached (see [PeekLimit.Reached]).
	Peek(ctx context.Context, after uint64, limit PeekLimit) ([]QueueEntry, error)

	// Ack removes the entries with seqs, in one atomic step: all or, with
	// an error, none. Acking a missing entry is not an error.
	Ack(ctx context.Context, seqs ...uint64) error

	// Len returns the number of entries, in constant time.
	Len(ctx context.Context) (int, error)

	// Close releases the queue's resources. Later calls return
	// ErrQueueClosed.
	Close() error
}

// MemoryPublisherQueue is an in-memory [PublisherQueue]: entries do not
// survive the process. Use the queue/file submodule to keep them across
// restarts.
type MemoryPublisherQueue struct {
	mu      sync.Mutex
	closed  bool
	nextSeq uint64
	// entries is ordered by Seq. An acked entry leaves a hole that keeps
	// only its Seq, for search, until reclaim removes it.
	entries []memQueueEntry
	head    int
	count   int // entries not acked
}

type memQueueEntry struct {
	e     QueueEntry
	acked bool
}

var _ PublisherQueue = (*MemoryPublisherQueue)(nil)

// NewMemoryPublisherQueue returns an empty in-memory queue.
func NewMemoryPublisherQueue() *MemoryPublisherQueue {
	return &MemoryPublisherQueue{}
}

// Enqueue implements [PublisherQueue].
func (q *MemoryPublisherQueue) Enqueue(_ context.Context, e QueueEntry, limit QueueLimit) (uint64, []QueueEntry, error) {
	e = e.clone()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, nil, ErrQueueClosed
	}
	var evicted []QueueEntry
	if limit.Max > 0 && q.count >= limit.Max {
		if limit.Policy != DropOldest {
			return 0, nil, ErrQueueFull
		}
		need := q.count - limit.Max + 1
		var victims []int
		for i := q.search(limit.Keep); i < len(q.entries) && len(victims) < need; i++ {
			if !q.entries[i].acked {
				victims = append(victims, i)
			}
		}
		if len(victims) < need {
			return 0, nil, ErrQueueFull
		}
		for _, i := range victims {
			evicted = append(evicted, q.entries[i].e)
			q.entries[i] = hole(q.entries[i].e.Seq)
		}
		q.count -= len(victims)
		q.reclaim()
	}
	q.nextSeq++
	e.Seq = q.nextSeq
	q.entries = append(q.entries, memQueueEntry{e: e})
	q.count++
	return e.Seq, evicted, nil
}

// Peek implements [PublisherQueue].
func (q *MemoryPublisherQueue) Peek(_ context.Context, after uint64, limit PeekLimit) ([]QueueEntry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	var out []QueueEntry
	size := 0
	for i := q.search(after); i < len(q.entries) && !limit.Reached(len(out), size); i++ {
		if !q.entries[i].acked {
			e := q.entries[i].e.clone()
			out = append(out, e)
			size += e.Size()
		}
	}
	return out, nil
}

// Ack implements [PublisherQueue].
func (q *MemoryPublisherQueue) Ack(_ context.Context, seqs ...uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	for _, seq := range seqs {
		if i := q.search(seq - 1); i < len(q.entries) && q.entries[i].e.Seq == seq && !q.entries[i].acked {
			q.entries[i] = hole(seq)
			q.count--
		}
	}
	q.reclaim()
	return nil
}

// Len implements [PublisherQueue].
func (q *MemoryPublisherQueue) Len(context.Context) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, ErrQueueClosed
	}
	return q.count, nil
}

// Close implements [PublisherQueue].
func (q *MemoryPublisherQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.entries, q.head, q.count = nil, 0, 0
	return nil
}

// search returns the index of the first entry with Seq above seq.
func (q *MemoryPublisherQueue) search(seq uint64) int {
	return q.head + sort.Search(len(q.entries)-q.head, func(i int) bool { return q.entries[q.head+i].e.Seq > seq })
}

// hole is what an acked entry leaves behind: its Seq and nothing it
// referenced.
func hole(seq uint64) memQueueEntry {
	return memQueueEntry{e: QueueEntry{Seq: seq}, acked: true}
}

// reclaim drops the holes acked entries leave: those at the head at once,
// and all of them once they outnumber the entries left, so the queue's
// memory follows what it holds rather than the traffic it has seen —
// also while an entry at the head stays unacked.
func (q *MemoryPublisherQueue) reclaim() {
	for q.head < len(q.entries) && q.entries[q.head].acked {
		q.entries[q.head] = memQueueEntry{}
		q.head++
	}
	holes := len(q.entries) - q.head - q.count
	if (q.head <= 1024 || q.head <= len(q.entries)/2) && (holes <= 64 || holes <= q.count) {
		return
	}
	live := q.entries[:0]
	for _, e := range q.entries[q.head:] {
		if !e.acked {
			live = append(live, e)
		}
	}
	clear(q.entries[len(live):])
	if cap(live) > 1024 && cap(live) > 4*len(live) {
		live = append([]memQueueEntry(nil), live...)
	}
	q.entries, q.head = live, 0
}
