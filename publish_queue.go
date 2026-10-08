// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

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
	"sync"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
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
	// publishes at once when [WithQueueWindow] is not set.
	DefaultQueueWindow = 32
)

// DefaultQueueRetryBackoff spaces the attempts of a message whose
// publish failed for a reason that may pass (a refusal such as 0x97
// Quota exceeded, a store error).
var DefaultQueueRetryBackoff = ExponentialBackoff(time.Second, time.Minute, 500*time.Millisecond)

// QueuePublisher accepts QoS 1/2 messages into a [PublisherQueue] and
// publishes them in the background, many at a time.
//
// [QueuePublisher.Publish] returns once the message is stored. A drain
// goroutine keeps up to [WithQueueWindow] messages in flight, in queue
// order, while the client is connected, and removes each from the queue
// when the broker has accepted it. A refusal that cannot pass (see
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
	// claimed: Publish holds it shared around Enqueue, the drain
	// exclusively while it peeks and advances claimed.
	claimMu sync.RWMutex
	claimed uint64 // highest Seq handed to pending

	mu      sync.Mutex
	pending map[uint64]*queuedMessage // claimed and unfinished, at most cfg.window

	notify    chan struct{}
	closing   chan struct{}
	done      chan struct{}
	ctx       context.Context // canceled by Close
	cancel    context.CancelFunc
	closeOnce sync.Once
	settling  sync.WaitGroup // exchanges whose outcome is still to come
}

type queuedMessage struct {
	entry    QueueEntry
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
	maxSize     int
	dropPolicy  DropPolicy
	ttl         time.Duration
	deadLetter  func(QueueEntry, error)
	permanent   func(error) bool
	backoff     Backoff
	idempotency bool
}

// QueueOption customises a QueuePublisher.
type QueueOption func(*queueConfig)

// WithQueueWindow sets how many messages are published at once.
// Default [DefaultQueueWindow]; the broker's Receive Maximum still
// applies on top.
func WithQueueWindow(n int) QueueOption {
	return func(c *queueConfig) { c.window = n }
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
// failed for a reason that may pass. Default
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
	cfg := queueConfig{window: DefaultQueueWindow, dropPolicy: DropNewest, permanent: PermanentPublishError, backoff: DefaultQueueRetryBackoff}
	for _, opt := range opts {
		opt(&cfg)
	}
	switch {
	case cfg.window < 1:
		return nil, fmt.Errorf("mqttv5: WithQueueWindow(%d): must be at least 1", cfg.window)
	case cfg.maxSize < 0:
		return nil, fmt.Errorf("mqttv5: WithQueueMaxSize(%d): must not be negative", cfg.maxSize)
	case cfg.ttl < 0:
		return nil, fmt.Errorf("mqttv5: WithQueueTTL(%v): must not be negative", cfg.ttl)
	case cfg.dropPolicy != DropNewest && cfg.dropPolicy != DropOldest:
		return nil, fmt.Errorf("mqttv5: WithQueueDropPolicy(%d): unknown policy", cfg.dropPolicy)
	case cfg.permanent == nil || cfg.backoff == nil:
		return nil, errors.New("mqttv5: WithQueueClassifier and WithQueueRetryBackoff need a function")
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &QueuePublisher{
		client:  client,
		queue:   queue,
		cfg:     cfg,
		logger:  client.cfg.Logger.With("component", "queue-publisher"),
		clk:     client.cfg.clock,
		pending: make(map[uint64]*queuedMessage),
		notify:  make(chan struct{}, 1),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}
	go p.run()
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

	p.claimMu.RLock()
	_, evicted, err := p.queue.Enqueue(ctx, e, QueueLimit{Max: p.cfg.maxSize, Policy: p.cfg.dropPolicy, Keep: p.claimed})
	p.claimMu.RUnlock()
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
// finish, and closes the queue. Messages whose exchange is still running
// stay in the queue and in the session; the next QueuePublisher on them
// continues those exchanges.
func (p *QueuePublisher) Close(ctx context.Context) error {
	p.closeOnce.Do(func() { close(p.closing) })
	select {
	case <-p.done:
	case <-ctx.Done():
		return fmt.Errorf("mqttv5: QueuePublisher.Close: %w", ctx.Err())
	}
	settled := make(chan struct{})
	go func() {
		p.settling.Wait()
		close(settled)
	}()
	var err error
	select {
	case <-settled:
	case <-ctx.Done():
		err = fmt.Errorf("mqttv5: QueuePublisher.Close: messages still in flight: %w", ctx.Err())
	}
	p.cancel()
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
// sleeps until something changes.
func (p *QueuePublisher) run() {
	defer close(p.done)
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

// fill starts every pending message that is due and claims more from the
// queue while the window has room, until the connection or the
// QueuePublisher goes away. It returns when the next retry is due, or
// zero.
func (p *QueuePublisher) fill() time.Time {
	for {
		due, next, room := p.due()
		for _, m := range due {
			p.start(m)
		}
		if room <= 0 || p.client.cur.Load() == nil || p.ctx.Err() != nil {
			return next
		}
		claimed := p.claim(room)
		if len(claimed) == 0 {
			return next
		}
		for _, m := range claimed {
			p.start(m)
		}
		if p.client.cur.Load() == nil {
			return next
		}
	}
}

// due returns the pending messages to start now, in queue order, when
// the next one waiting for a retry is due, and the window's free room.
func (p *QueuePublisher) due() (due []*queuedMessage, next time.Time, room int) {
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
	return due, next, p.cfg.window - len(p.pending)
}

// claim takes up to n entries after the last claimed one into pending.
func (p *QueuePublisher) claim(n int) []*queuedMessage {
	p.claimMu.Lock()
	defer p.claimMu.Unlock()
	entries, err := p.queue.Peek(p.ctx, p.claimed, n)
	if err != nil {
		if !errors.Is(err, ErrQueueClosed) && p.ctx.Err() == nil {
			p.logger.Error("reading the queue failed", slog.Any("error", err))
		}
		return nil
	}
	out := make([]*queuedMessage, len(entries))
	p.mu.Lock()
	for i, e := range entries {
		m := &queuedMessage{entry: e, inFlight: true}
		p.pending[e.Seq] = m
		out[i] = m
	}
	p.mu.Unlock()
	if len(entries) > 0 {
		p.claimed = entries[len(entries)-1].Seq
	}
	return out
}

// start begins an exchange for m, which the caller has marked in
// flight. When the session already holds one for m — from an earlier
// process, from before the session store failed, or one whose outcome
// could not be recorded yet — it continues that exchange; otherwise it
// publishes m.
func (p *QueuePublisher) start(m *queuedMessage) {
	e := m.entry
	p.mu.Lock()
	m.attempts++
	p.mu.Unlock()

	p.settling.Add(1)
	settle := func(_ context.Context, err error) error {
		defer p.settling.Done()
		return p.settled(m, err)
	}
	if p.client.engine.Adopt([]byte(e.ID), settle) {
		return
	}
	now := p.clk.Now()
	if !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt) {
		p.settling.Done()
		_ = p.drop(m, ErrMessageExpired)
		return
	}
	opts := e.Publish
	if !e.ExpiresAt.IsZero() {
		left := uint32((e.ExpiresAt.Sub(now) + time.Second - 1) / time.Second)
		opts.MessageExpiryInterval = &left
	}
	if err := p.client.publishTracked(p.ctx, opts, []byte(e.ID), settle); err != nil {
		p.settling.Done()
		_ = p.failed(m, err)
	}
}

// settled handles the outcome of m's exchange. It runs before the
// session forgets the exchange, so the queue entry is removed first; an
// error tells the session to keep the exchange because the removal
// failed.
func (p *QueuePublisher) settled(m *queuedMessage, err error) error {
	switch {
	case err == nil:
		p.mu.Lock()
		first := !m.accepted
		m.accepted = true
		p.mu.Unlock()
		if first {
			p.client.stats.addPublishSent()
		}
		return p.remove(m)
	case errors.Is(err, ErrSessionLost):
		// The broker never confirmed it; publish it again as new.
		p.retry(m, time.Time{})
		return nil
	default:
		return p.failed(m, err)
	}
}

// failed decides what a failed attempt for m leads to. It returns the
// error of a removal that failed.
func (p *QueuePublisher) failed(m *queuedMessage, err error) error {
	switch {
	case errors.Is(err, ErrMessageExpired) || p.cfg.permanent(err):
		return p.drop(m, err)
	case errors.Is(err, ErrNotConnected):
		p.retry(m, time.Time{})
	case errors.Is(err, ErrClosed) || p.ctx.Err() != nil:
		p.retry(m, time.Time{})
	default:
		p.mu.Lock()
		attempts := m.attempts
		p.mu.Unlock()
		p.logger.Warn("publishing a queued message failed; will retry",
			slog.String("topic", m.entry.Publish.Topic), slog.Int("attempt", attempts), slog.Any("error", err))
		p.retry(m, p.clk.Now().Add(p.cfg.backoff(attempts-1)))
	}
	return nil
}

func (p *QueuePublisher) retry(m *queuedMessage, at time.Time) {
	p.mu.Lock()
	m.inFlight, m.retryAt = false, at
	p.mu.Unlock()
	p.signal()
}

// drop dead-letters m, once, and removes it.
func (p *QueuePublisher) drop(m *queuedMessage, reason error) error {
	p.mu.Lock()
	first := !m.deadLettered
	m.deadLettered = true
	p.mu.Unlock()
	if first {
		p.deadLetter(m.entry, reason)
	}
	return p.remove(m)
}

func (p *QueuePublisher) deadLetter(e QueueEntry, reason error) {
	p.logger.Warn("dropping a queued message", slog.String("topic", e.Publish.Topic),
		slog.String("id", e.ID), slog.Any("reason", reason))
	if p.cfg.deadLetter != nil {
		p.cfg.deadLetter(e, reason)
	}
}

// remove takes m out of the queue and frees its place in the window.
// When the queue fails, m stays pending and the removal is retried with
// the retry backoff; the error goes back to the session, which keeps
// the exchange so the outcome is not lost.
func (p *QueuePublisher) remove(m *queuedMessage) error {
	if err := p.queue.Ack(context.Background(), m.entry.Seq); err != nil {
		p.mu.Lock()
		attempts := m.attempts
		p.mu.Unlock()
		p.logger.Error("removing a finished message from the queue failed; will retry",
			slog.String("id", m.entry.ID), slog.Int("attempt", attempts), slog.Any("error", err))
		p.retry(m, p.clk.Now().Add(p.cfg.backoff(attempts-1)))
		return err
	}
	p.mu.Lock()
	delete(p.pending, m.entry.Seq)
	p.mu.Unlock()
	p.signal()
	return nil
}

func newQueueID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mqttv5: message ID: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
