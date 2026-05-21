// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// Errors returned by the [QueuePublisher] / [PublisherQueue] surface.
var (
	// ErrQueueClosed is returned by Enqueue / Publish after Close.
	ErrQueueClosed = errors.New("mqttv5: queue is closed")

	// ErrQueueFull is returned by [QueuePublisher.Publish] when the
	// queue is at its [WithQueueMaxSize] cap and DropNewest is in
	// effect.
	ErrQueueFull = errors.New("mqttv5: queue is full")

	// ErrQoS0NotQueueable is returned when a QoS 0 publish reaches
	// [QueuePublisher.Publish] — durable enqueue is meaningless when
	// the broker has no delivery obligation.
	ErrQoS0NotQueueable = errors.New("mqttv5: QueuePublisher requires QoS >= 1")

	// ErrEvictionNotSupported is returned by
	// [PublisherQueue.EvictHead] for backends that cannot evict
	// (append-only disk queues, etc.). [NewQueuePublisher] surfaces
	// this at construct time when [DropOldest] is requested.
	ErrEvictionNotSupported = errors.New("mqttv5: queue backend does not support eviction")
)

// Default values used by NewQueuePublisher when QueueOptions leave a
// field unset.
const (
	// DefaultQueueBatchSize caps the per-PeekBatch entry count.
	DefaultQueueBatchSize = 16

	// DefaultQueueIdleInterval is the drain-loop wakeup tick when no
	// Enqueue signals have arrived. Lower values reduce the worst-
	// case latency to pick up restored entries; higher values reduce
	// wakeup cost.
	DefaultQueueIdleInterval = 500 * time.Millisecond

	// DefaultQueuePublishTimeout caps each per-message broker
	// handshake (ack wait) inside the drain loop.
	DefaultQueuePublishTimeout = 30 * time.Second
)

// QueueEntry is one durable publish in flight between Publish
// (enqueue) and broker PUBACK (Ack). The drain loop sets PacketID
// and Dup per attempt; values in Publish are ignored.
type QueueEntry struct {
	Publish    wire.PublishOpts
	EnqueuedAt time.Time
}

// QueueToken identifies one entry. Opaque to QueuePublisher —
// implementations choose whatever value lets Ack remove the exact
// entry PeekBatch returned (sequence number, byte offset, etc.).
type QueueToken any

// PublisherQueue is the durable backing store for QueuePublisher.
// Implementations may be in-memory or disk-backed. Every method must
// be safe for concurrent use.
type PublisherQueue interface {
	// Enqueue durably stores entry. Returns ErrQueueClosed after Close.
	Enqueue(ctx context.Context, entry QueueEntry) error

	// PeekBatch returns up to n oldest entries without removing them.
	// The tokens slice is parallel to entries; the caller passes each
	// token to Ack to mark the entry delivered. An empty result with
	// nil error means "queue is empty right now".
	PeekBatch(ctx context.Context, n int) (entries []QueueEntry, tokens []QueueToken, err error)

	// Ack removes the entry identified by token. Acking an unknown
	// token is a no-op (an entry already removed via another path).
	Ack(ctx context.Context, token QueueToken) error

	// EvictHead removes the oldest entry and returns it. Returns
	// (zero, false, nil) when the queue is empty. Implementations
	// that cannot evict the head out-of-band return
	// ErrEvictionNotSupported and the caller must use DropNewest.
	EvictHead(ctx context.Context) (QueueEntry, bool, error)

	// Len returns the current entry count.
	Len(ctx context.Context) (int, error)

	// Close releases any underlying resources. After Close, subsequent
	// Enqueue calls must return ErrQueueClosed.
	Close() error
}

// QueuePublisher durably enqueues publishes and drains them to the
// broker in order from a background goroutine. [QueuePublisher.Publish]
// returns once the entry is stored, before the broker round-trip.
// For crash safety, pair the queue/file submodule with the
// store/file submodule (in-flight ack state) — independent layers.
type QueuePublisher struct {
	client *Client
	queue  PublisherQueue
	cfg    queueConfig
	logger *slog.Logger

	notify    chan struct{}
	closeOnce sync.Once
	closeCh   chan struct{}
	doneCh    chan struct{}
}

// queueConfig holds resolved QueuePublisher options.
type queueConfig struct {
	batchSize      int
	maxSize        int
	dropPolicy     DropPolicy
	ttl            time.Duration
	deadLetter     func(QueueEntry, error)
	idleInterval   time.Duration
	publishTimeout time.Duration
}

func defaultQueueConfig() queueConfig {
	return queueConfig{
		batchSize:      DefaultQueueBatchSize,
		dropPolicy:     DropNewest,
		idleInterval:   DefaultQueueIdleInterval,
		publishTimeout: DefaultQueuePublishTimeout,
	}
}

// QueueOption customises a QueuePublisher.
type QueueOption func(*queueConfig)

// WithQueueBatchSize caps the number of entries the drain goroutine
// pulls in one PeekBatch. Default 16.
func WithQueueBatchSize(n int) QueueOption {
	return func(c *queueConfig) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithQueueMaxSize bounds the queue. At capacity, Publish either returns
// ErrQueueFull (DropNewest) or evicts the oldest entry via EvictHead
// (DropOldest — see WithQueueDropPolicy).
func WithQueueMaxSize(n int) QueueOption {
	return func(c *queueConfig) {
		if n >= 0 {
			c.maxSize = n
		}
	}
}

// WithQueueDropPolicy controls full-queue behaviour. DropNewest
// (default) returns ErrQueueFull from Publish when the queue is at
// capacity. DropOldest evicts the oldest entry via
// PublisherQueue.EvictHead and dead-letters it (if a callback is
// registered) before enqueuing the new one. Backends that cannot
// evict (return ErrEvictionNotSupported) cause NewQueuePublisher to
// fail rather than silently downgrade.
func WithQueueDropPolicy(p DropPolicy) QueueOption {
	return func(c *queueConfig) {
		c.dropPolicy = p
	}
}

// WithQueueIdleInterval overrides the drain loop's idle-tick
// wakeup. Default DefaultQueueIdleInterval.
func WithQueueIdleInterval(d time.Duration) QueueOption {
	return func(c *queueConfig) {
		if d > 0 {
			c.idleInterval = d
		}
	}
}

// WithQueuePublishTimeout caps each per-message broker handshake
// inside the drain loop. Default DefaultQueuePublishTimeout. Set
// short to keep retries snappy at the cost of more wasted work
// against a slow broker; set long for occasional huge payloads.
func WithQueuePublishTimeout(d time.Duration) QueueOption {
	return func(c *queueConfig) {
		if d > 0 {
			c.publishTimeout = d
		}
	}
}

// WithQueueTTL drops entries older than d at drain time and fires
// the dead-letter callback. When d > 0 the value also mirrors into
// each entry's MessageExpiryInterval so the broker enforces TTL
// after hand-off.
func WithQueueTTL(d time.Duration) QueueOption {
	return func(c *queueConfig) {
		c.ttl = d
	}
}

// WithDeadLetter registers a callback for terminally failed entries
// (e.g. TTL expiry). Runs on the drain goroutine — must not block.
func WithDeadLetter(fn func(QueueEntry, error)) QueueOption {
	return func(c *queueConfig) {
		c.deadLetter = fn
	}
}

// NewQueuePublisher wires a [QueuePublisher] over client + queue.
// The drain goroutine starts immediately and runs until
// [QueuePublisher.Close]. When [DropOldest] is requested,
// queue.EvictHead is probed; [ErrEvictionNotSupported] aborts
// construction rather than silently downgrading.
func NewQueuePublisher(client *Client, queue PublisherQueue, opts ...QueueOption) (*QueuePublisher, error) {
	cfg := defaultQueueConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.dropPolicy == DropOldest {
		// Probe: a no-op evict on an empty queue should return
		// (zero, false, nil). ErrEvictionNotSupported here means
		// the backend cannot evict at all.
		if _, _, err := queue.EvictHead(context.Background()); err != nil &&
			errors.Is(err, ErrEvictionNotSupported) {
			return nil, fmt.Errorf("mqttv5: WithQueueDropPolicy(DropOldest): %w", err)
		}
	}
	p := &QueuePublisher{
		client:  client,
		queue:   queue,
		cfg:     cfg,
		logger:  client.cfg.Logger.With("component", "queue-publisher"),
		notify:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	// Prime notify so any entries already in a restored queue
	// (file-backed) drain immediately on next connection.
	p.signal()
	go p.drain()
	return p, nil
}

// Publish durably enqueues opts and returns once the queue has
// stored it. The broker handshake happens asynchronously on the
// drain goroutine. Rejects QoS 0 with [ErrQoS0NotQueueable].
func (p *QueuePublisher) Publish(ctx context.Context, opts wire.PublishOpts) error {
	if opts.QoS == 0 {
		return ErrQoS0NotQueueable
	}

	// Detach Payload from the caller's buffer so they can reuse it.
	pubCopy := opts
	if len(opts.Payload) > 0 {
		pubCopy.Payload = append([]byte(nil), opts.Payload...)
	}
	// PacketID and Dup are per-attempt — clear so the drain loop
	// (via client.Publish) allocates fresh.
	pubCopy.PacketID = 0
	pubCopy.Dup = false

	if p.cfg.ttl > 0 && pubCopy.MessageExpiryInterval == nil {
		exp := uint32(p.cfg.ttl.Seconds())
		pubCopy.MessageExpiryInterval = &exp
	}

	entry := QueueEntry{
		Publish:    pubCopy,
		EnqueuedAt: time.Now(),
	}

	if p.cfg.maxSize > 0 {
		n, err := p.queue.Len(ctx)
		if err == nil && n >= p.cfg.maxSize {
			if p.cfg.dropPolicy == DropOldest {
				evicted, ok, err := p.queue.EvictHead(ctx)
				if err != nil {
					return fmt.Errorf("mqttv5: queue evict: %w", err)
				}
				if ok && p.cfg.deadLetter != nil {
					p.cfg.deadLetter(evicted, ErrQueueFull)
				}
				// fall through to Enqueue
			} else {
				return ErrQueueFull
			}
		}
	}

	if err := p.queue.Enqueue(ctx, entry); err != nil {
		return err
	}
	p.signal()
	return nil
}

// Close stops the drain goroutine and closes the backing queue. ctx
// bounds how long Close waits for the drain to settle.
func (p *QueuePublisher) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		close(p.closeCh)
	})
	select {
	case <-p.doneCh:
	case <-ctx.Done():
		return fmt.Errorf("mqttv5: QueuePublisher.Close: %w", ctx.Err())
	}
	return p.queue.Close()
}

// signal wakes the drain goroutine. Non-blocking — multiple
// concurrent Enqueues collapse into a single wake.
func (p *QueuePublisher) signal() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// drain is the long-running goroutine that publishes queued entries
// in order. It wakes on Enqueue (via notify), every idleInterval (so
// it picks up restored entries even when nothing is being enqueued),
// or on Close.
func (p *QueuePublisher) drain() {
	defer close(p.doneCh)
	t := time.NewTicker(p.cfg.idleInterval)
	defer t.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-p.notify:
		case <-t.C:
		}

		for {
			select {
			case <-p.closeCh:
				return
			default:
			}

			if !p.client.Connected() {
				break
			}

			more, err := p.drainOneBatch()
			if err != nil {
				p.logger.Warn("drain batch failed",
					slog.Any("error", err),
				)
				break
			}
			if !more {
				break
			}
		}
	}
}

// drainOneBatch peeks up to batchSize entries and tries to publish
// each in order. Returns (more=true) if the queue may still hold
// entries (so the caller loops again).
func (p *QueuePublisher) drainOneBatch() (bool, error) {
	ctx := context.Background()
	entries, tokens, err := p.queue.PeekBatch(ctx, p.cfg.batchSize)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}

	for i, e := range entries {
		select {
		case <-p.closeCh:
			return false, nil
		default:
		}

		// TTL drop at drain time — entries that aged out before
		// the broker accepted them are dead-lettered.
		if p.cfg.ttl > 0 && time.Since(e.EnqueuedAt) > p.cfg.ttl {
			if dl := p.cfg.deadLetter; dl != nil {
				dl(e, fmt.Errorf("ttl expired after %s", time.Since(e.EnqueuedAt)))
			}
			_ = p.queue.Ack(ctx, tokens[i])
			continue
		}

		pubCtx, cancel := context.WithTimeout(ctx, p.cfg.publishTimeout)
		err := p.client.Publish(pubCtx, e.Publish)
		cancel()
		if err != nil {
			// Transient — leave the entry, retry on next wake.
			p.logger.Debug("publish failed; will retry",
				slog.String("topic", e.Publish.Topic),
				slog.Any("error", err),
			)
			return false, nil
		}
		if err := p.queue.Ack(ctx, tokens[i]); err != nil {
			p.logger.Warn("queue ack failed",
				slog.String("topic", e.Publish.Topic),
				slog.Any("error", err),
			)
		}
	}
	return len(entries) == p.cfg.batchSize, nil
}

// ---------- MemoryPublisherQueue ----------

// MemoryPublisherQueue is an in-memory PublisherQueue. It is the
// default when no durable store is required. Crash semantics: all
// queued entries are lost. For at-least-once across process restarts,
// use the queue/file/ submodule instead.
type MemoryPublisherQueue struct {
	mu     sync.Mutex
	closed bool
	next   uint64
	items  []memQueueItem
}

type memQueueItem struct {
	token uint64
	entry QueueEntry
}

// NewMemoryPublisherQueue constructs an empty in-memory queue.
func NewMemoryPublisherQueue() *MemoryPublisherQueue {
	return &MemoryPublisherQueue{}
}

// Enqueue implements PublisherQueue.
func (q *MemoryPublisherQueue) Enqueue(_ context.Context, entry QueueEntry) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	q.next++
	q.items = append(q.items, memQueueItem{token: q.next, entry: entry})
	return nil
}

// PeekBatch implements PublisherQueue.
func (q *MemoryPublisherQueue) PeekBatch(_ context.Context, n int) ([]QueueEntry, []QueueToken, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, nil, ErrQueueClosed
	}
	if len(q.items) == 0 || n <= 0 {
		return nil, nil, nil
	}
	if n > len(q.items) {
		n = len(q.items)
	}
	entries := make([]QueueEntry, n)
	tokens := make([]QueueToken, n)
	for i := 0; i < n; i++ {
		entries[i] = q.items[i].entry
		tokens[i] = q.items[i].token
	}
	return entries, tokens, nil
}

// Ack implements PublisherQueue. Removes the entry identified by tok.
// Acking an unknown token is a no-op.
func (q *MemoryPublisherQueue) Ack(_ context.Context, tok QueueToken) error {
	id, ok := tok.(uint64)
	if !ok {
		return fmt.Errorf("mqttv5: MemoryPublisherQueue: bad token type %T", tok)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, it := range q.items {
		if it.token == id {
			q.items = append(q.items[:i], q.items[i+1:]...)
			return nil
		}
	}
	return nil
}

// EvictHead implements PublisherQueue by popping the oldest in-memory
// item.
func (q *MemoryPublisherQueue) EvictHead(_ context.Context) (QueueEntry, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return QueueEntry{}, false, ErrQueueClosed
	}
	if len(q.items) == 0 {
		return QueueEntry{}, false, nil
	}
	head := q.items[0]
	q.items = q.items[1:]
	return head.entry, true, nil
}

// Len implements PublisherQueue.
func (q *MemoryPublisherQueue) Len(_ context.Context) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items), nil
}

// Close implements PublisherQueue.
func (q *MemoryPublisherQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return nil
}
