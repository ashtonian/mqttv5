// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ashtonian/mqttv5/internal/trie"
	"github.com/ashtonian/mqttv5/wire"
)

// ---------------- Message (handler-facing view of inbound PUBLISH) ----------------

// messagePool reuses *Message wrappers across inbound dispatches.
var messagePool = sync.Pool{
	New: func() any { return &Message{} },
}

func acquireMessage(pub *wire.Publish, client *Client, cs *connState) *Message {
	m := messagePool.Get().(*Message)
	m.Publish = pub
	m.client = client
	m.cs = cs
	return m
}

func releaseMessage(m *Message) {
	m.Publish = nil
	m.client = nil
	m.cs = nil
	messagePool.Put(m)
}

// Message is the handler-facing view of an inbound PUBLISH. The
// embedded *wire.Publish exposes Topic, Payload, Properties, QoS,
// Retain, Dup, PacketID — those fields alias the pooled frame and are
// valid only until Ack returns. Use CloneTopic / ClonePayload to
// retain past Ack.
//
// SubscribeCallback auto-acks after the handler returns. Subscribe
// (chan) and SubscribeQueue require manual Ack — for QoS 1 the PUBACK
// is held until Ack (flushed in §4.6 arrival order); for QoS 2 the
// PUBREC is held until Ack, PUBCOMP fires automatically on PUBREL.
//
// When SubAutoAck is set on Subscribe / SubscribeQueue the library
// delivers a detached *Message instead: Topic / Payload / Properties
// are heap copies safe to retain indefinitely, and Ack is a no-op
// (the original frame is already released).
//
// A PUBLISH matching multiple overlapping filters delivers the same
// *Message to every matched handler. Ack is refcounted — only the
// final call releases the frame.
type Message struct {
	*wire.Publish

	client *Client
	cs     *connState

	// refs is initialised by the dispatcher to the matched-handler
	// count. Each Ack decrements; the final caller releases the frame.
	refs atomic.Int32

	// detached is set on auto-ack copies returned to SubAutoAck
	// consumers. The embedded Publish is heap-allocated with cloned
	// fields, not pooled, so Ack must not Release or pool-return.
	detached bool
}

// CloneTopic returns a heap-allocated copy of Topic safe to retain
// past [Message.Ack]. One allocation; prefer reading m.Topic inside
// the handler when retention isn't needed.
func (m *Message) CloneTopic() string {
	if m.Publish == nil {
		return ""
	}
	return strings.Clone(m.Topic)
}

// ClonePayload returns a heap-allocated copy of Payload safe to
// retain past [Message.Ack].
func (m *Message) ClonePayload() []byte {
	if m.Publish == nil {
		return nil
	}
	return bytes.Clone(m.Payload)
}

// Ack acknowledges the message. Idempotent — calls beyond the
// matched-handler count are silent no-ops. After the final Ack the
// embedded Publish fields (Topic, Payload, Properties) are invalid;
// use [Message.CloneTopic] / [Message.ClonePayload] for retention.
//
// On a detached message (delivered when [SubAutoAck] is set) Ack is
// always a no-op — the broker ack and frame release already happened
// inside the dispatcher.
func (m *Message) Ack() error {
	if m.refs.Add(-1) > 0 {
		// Other handlers still hold this message; defer cleanup.
		return nil
	}
	if m.detached {
		// Heap-allocated standalone; nothing pooled, nothing to ack.
		return nil
	}
	pub := m.Publish
	if pub == nil {
		return nil
	}
	if pub.QoS > 0 && m.client != nil && m.cs != nil {
		m.client.flushPendingAcks(m.cs, pub.PacketID)
	}
	pub.Release()
	releaseMessage(m)
	return nil
}

// detachMessage returns a heap-allocated standalone copy of m with
// Topic, Payload, and Properties cloned off the frame so they survive
// the caller's subsequent m.Ack(). Used by SubAutoAck to deliver
// already-acked messages to channel / queue consumers.
//
// Costs one *Message allocation, one *wire.Publish allocation, plus
// the three field clones. Reserved for opt-in auto-ack delivery; the
// default manual-ack path remains zero-alloc on receive.
func detachMessage(m *Message) *Message {
	return &Message{
		Publish: &wire.Publish{
			Topic:      strings.Clone(m.Topic),
			Payload:    bytes.Clone(m.Payload),
			Properties: wire.PropertiesFromBytes(bytes.Clone(m.Properties.Raw())),
			QoS:        m.QoS,
			Retain:     m.Retain,
			Dup:        m.Dup,
			PacketID:   m.PacketID,
		},
		detached: true,
	}
}

// ---------------- Subscribe API ----------------

// TopicFilter is one entry in a SUBSCRIBE packet: a topic pattern
// plus the per-filter MQTT v5 §3.8.3.1 flags. Only Topic is required;
// the rest default to MQTT-spec zero values.
//
//	{Topic: "events/#"}                        // QoS 0
//	{Topic: "events/#", QoS: 1}                // at-least-once
//	{Topic: "events/#", QoS: 1, NoLocal: true} // don't echo own publishes back
type TopicFilter = wire.SubscribeFilter

// SubscribeOption configures client-side delivery for one Subscribe
// call. Protocol flags (QoS, NoLocal, etc.) live on TopicFilter
// itself — these options control what happens after delivery.
type SubscribeOption func(*subscribeConfig)

// DefaultSubscribeBuffer is the channel buffer size used by Subscribe
// when SubBuffer is not called.
const DefaultSubscribeBuffer = 64

// sharedSubPrefix is the MQTT v5 §4.8.2 shared-subscription marker.
// A filter "$share/{group}/{filter}" routes inbound PUBLISHes from the
// group across whichever client picks them up first.
const sharedSubPrefix = "$share/"

// ErrChanDropOldestUnsupported is returned by [Client.Subscribe] when
// called with SubDropPolicy(DropOldest); use [Client.SubscribeQueue]
// for DropOldest semantics.
var ErrChanDropOldestUnsupported = errors.New(
	"mqttv5: Subscribe (chan) does not support DropOldest; use SubscribeQueue",
)

// ErrNilHandler is returned by [Client.SubscribeCallback] when the
// handler argument is nil.
var ErrNilHandler = errors.New("mqttv5: subscribe handler must not be nil")

// subscribeConfig is the internal accumulator the SubscribeOptions
// apply to. Defaults are set by newSubscribeConfig before applying.
type subscribeConfig struct {
	bufferSize         int            // chan flavour only
	maxQueueSize       int            // queue flavour only (0 = unbounded)
	dropPolicy         DropPolicy     // queue flavour honors both; chan rejects explicit DropOldest
	dropPolicyExplicit bool           // SubDropPolicy was called for this Subscribe
	onDrop             func(*Message) // optional: fires after a chan/queue drop+ack
	autoAck            bool           // SubAutoAck: ack on delivery, deliver detached copy
}

func newSubscribeConfig(c *Client) subscribeConfig {
	return subscribeConfig{
		bufferSize:   DefaultSubscribeBuffer,
		maxQueueSize: c.cfg.MaxSubscribeQueueSize,
		dropPolicy:   c.cfg.DropPolicy,
	}
}

// SubBuffer sets [Client.Subscribe]'s channel buffer size. Default
// [DefaultSubscribeBuffer]. Ignored by [Client.SubscribeQueue] and
// [Client.SubscribeCallback].
func SubBuffer(n int) SubscribeOption {
	return func(cfg *subscribeConfig) {
		if n > 0 {
			cfg.bufferSize = n
		}
	}
}

// SubMaxQueueSize caps [Client.SubscribeQueue]'s length.
// 0 = unbounded. Default from [WithMaxSubscribeQueueSize] (0).
func SubMaxQueueSize(n int) SubscribeOption {
	return func(cfg *subscribeConfig) { cfg.maxQueueSize = n }
}

// SubDropPolicy chooses behaviour when the channel buffer or queue
// cap fills.
//
//   - Subscribe (chan): only DropNewest is supported. Passing
//     DropOldest here causes Subscribe to return
//     ErrChanDropOldestUnsupported — peek-and-pop on a consumer-owned
//     channel would race the receiver. Use SubscribeQueue for
//     DropOldest semantics.
//   - SubscribeQueue: both policies are honored.
//   - SubscribeCallback: ignored (callback never drops; slow handler
//     stalls the connection instead).
//
// Default is the client-level WithDropPolicy (DropNewest).
func SubDropPolicy(p DropPolicy) SubscribeOption {
	return func(cfg *subscribeConfig) {
		cfg.dropPolicy = p
		cfg.dropPolicyExplicit = true
	}
}

// SubOnDrop fires when a message is dropped by [Client.Subscribe]
// (chan full) or [Client.SubscribeQueue] (cap reached). The dropped
// message is already ack'd; fn is for metrics / logging only — must
// NOT call [Message.Ack], must NOT retain Topic / Payload past
// return (both alias the frame buffer).
func SubOnDrop(fn func(*Message)) SubscribeOption {
	return func(cfg *subscribeConfig) { cfg.onDrop = fn }
}

// SubAutoAck makes [Client.Subscribe] / [Client.SubscribeQueue] ack
// each delivery on the dispatcher goroutine, before handing the
// message to the consumer. The consumer receives a detached
// *Message whose Topic / Payload / Properties are heap copies safe
// to retain indefinitely; [Message.Ack] becomes a no-op.
//
// Trade-off: convenience for two allocations per delivery (Topic
// clone + Payload clone) on top of the *Message + *wire.Publish
// header allocations — the default manual-ack path is zero-alloc on
// the receive hot path. More importantly, auto-ack breaks
// at-least-once semantics: a consumer that crashes between delivery
// and processing has nothing to replay, since the broker considers
// the message delivered. Reach for this on QoS 0 / observational
// consumers; keep manual ack for ledger-style workloads.
//
// Ignored by [Client.SubscribeCallback] (already auto-acks).
func SubAutoAck() SubscribeOption {
	return func(cfg *subscribeConfig) { cfg.autoAck = true }
}

// Subscribe sends one SUBSCRIBE for filters and returns a buffered
// channel of matching inbound messages. Callers MUST call
// [Message.Ack] on each. Returns once SUBACK arrives; the channel
// closes on [Client.Unsubscribe] or [Client.Disconnect]. Full-buffer
// messages are dropped + acked (DropNewest); observe drops via
// [SubOnDrop]. Returns [ErrChanDropOldestUnsupported] when called
// with explicit [SubDropPolicy] of [DropOldest].
func (c *Client) Subscribe(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *Message, SubscriptionToken, error) {
	cfg := newSubscribeConfig(c)
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.dropPolicy == DropOldest {
		if cfg.dropPolicyExplicit {
			return nil, SubscriptionToken{}, ErrChanDropOldestUnsupported
		}
		// Client-level WithDropPolicy(DropOldest) propagated to a chan
		// Subscribe — silently use DropNewest here rather than fail
		// every chan call.
		cfg.dropPolicy = DropNewest
	}

	ch := make(chan *Message, cfg.bufferSize)
	onDrop := cfg.onDrop
	autoAck := cfg.autoAck
	handler := func(m *Message) {
		if autoAck {
			// Detach + ack the original frame BEFORE handing to the
			// consumer so Topic / Payload survive past delivery.
			detached := detachMessage(m)
			_ = m.Ack()
			m = detached
		}
		select {
		case ch <- m:
		default:
			// Channel full: drop + ack. DropOldest is rejected at
			// entry so only DropNewest semantics reach here.
			c.stats.addInboundDropped()
			_ = m.Ack() // no-op for detached; releases frame otherwise
			if onDrop != nil {
				onDrop(m)
			}
		}
	}
	onUnsub := func() { close(ch) }
	token, err := c.subscribeImpl(ctx, filters, handler, onUnsub)
	if err != nil {
		return nil, SubscriptionToken{}, err
	}
	return ch, token, nil
}

// SubscribeQueue sends one SUBSCRIBE and returns a [Queue] of
// matching messages. Unbounded by default; cap with
// [SubMaxQueueSize]. On overflow, [SubDropPolicy] decides:
// [DropNewest] acks + drops the inbound; [DropOldest] dequeues +
// acks the head and admits the new one. Closes on
// [Client.Unsubscribe] or [Client.Disconnect].
func (c *Client) SubscribeQueue(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*Message], SubscriptionToken, error) {
	cfg := newSubscribeConfig(c)
	for _, opt := range opts {
		opt(&cfg)
	}

	q := NewQueue[*Message]()
	maxQueue := cfg.maxQueueSize
	policy := cfg.dropPolicy
	onDrop := cfg.onDrop
	autoAck := cfg.autoAck
	handler := func(m *Message) {
		if autoAck {
			detached := detachMessage(m)
			_ = m.Ack()
			m = detached
		}
		if maxQueue > 0 && q.Len() >= maxQueue {
			c.stats.addInboundDropped()
			switch policy {
			case DropOldest:
				if old, ok := q.TryDequeue(); ok {
					_ = old.Ack() // no-op for detached
					if onDrop != nil {
						onDrop(old)
					}
				}
				q.Enqueue(m)
			default: // DropNewest
				_ = m.Ack() // no-op for detached
				if onDrop != nil {
					onDrop(m)
				}
			}
			return
		}
		if !q.Enqueue(m) {
			c.stats.addInboundDropped()
			_ = m.Ack() // no-op for detached
			if onDrop != nil {
				onDrop(m)
			}
		}
	}
	onUnsub := func() { q.Close() }
	token, err := c.subscribeImpl(ctx, filters, handler, onUnsub)
	if err != nil {
		return nil, SubscriptionToken{}, err
	}
	return q, token, nil
}

// SubscribeCallback registers a subscription that invokes h for every
// matching inbound PUBLISH.
//
// h runs synchronously on the read goroutine and MUST NOT block — a
// slow handler stalls PINGRESP and drops the connection. The runtime
// auto-acks via [Message.Ack] after h returns; use [Client.Subscribe]
// or [Client.SubscribeQueue] when ack must be deferred past h.
// Returns [ErrNilHandler] when h is nil.
func (c *Client) SubscribeCallback(ctx context.Context, filters []TopicFilter, h HandlerFunc) (SubscriptionToken, error) {
	if h == nil {
		return SubscriptionToken{}, ErrNilHandler
	}
	wrapped := func(m *Message) {
		h(m)
		_ = m.Ack()
	}
	return c.subscribeImpl(ctx, filters, wrapped, nil)
}

// subscribeImpl is the shared implementation behind Subscribe,
// SubscribeQueue, and SubscribeCallback. onUnsub (if non-nil) runs
// when the subscription is torn down — by Unsubscribe or Disconnect
// — and is the right place to close any per-sub channel/queue.
func (c *Client) subscribeImpl(ctx context.Context, filters []TopicFilter, handler HandlerFunc, onUnsub func()) (SubscriptionToken, error) {
	cs := c.cur.Load()
	if cs == nil {
		return SubscriptionToken{}, ErrNotConnected
	}
	if len(filters) == 0 {
		return SubscriptionToken{}, wire.ErrEmptyFilterList
	}
	if handler == nil {
		return SubscriptionToken{}, ErrNilHandler
	}
	if err := c.validateFiltersAgainstBroker(cs, filters, nil); err != nil {
		return SubscriptionToken{}, err
	}

	id, err := c.session.AllocateID(ctx)
	if err != nil {
		return SubscriptionToken{}, fmt.Errorf("mqttv5: allocate packet id: %w", err)
	}
	defer c.session.ReleaseID(id)

	// Pre-register handlers before sending SUBSCRIBE: registering
	// after SUBACK opens a race where the reader could dispatch a
	// matching PUBLISH against an empty trie.
	sub := &activeSub{
		id:      c.nextSubID.Add(1),
		filters: filters,
		handler: handler,
		trieIDs: make([]trieRegEntry, 0, len(filters)),
		onUnsub: onUnsub,
	}
	for _, f := range filters {
		c.registerHandler(f.Topic, handler, sub)
	}

	wait := c.installCtrlWaiter(id)

	opts := wire.SubscribeOpts{PacketID: id, Filters: filters}
	c.stats.addSubscribeSent()
	if err := c.enqueueAwait(ctx, func(w io.Writer) (int64, error) {
		return wire.WriteSubscribe(w, opts)
	}); err != nil {
		c.removeCtrlWaiter(id)
		c.unregisterTrieEntries(sub.trieIDs)
		return SubscriptionToken{}, fmt.Errorf("mqttv5: write SUBSCRIBE: %w", err)
	}

	suback, err := c.awaitControlAck(ctx, id, wait)
	if err != nil {
		c.unregisterTrieEntries(sub.trieIDs)
		return SubscriptionToken{}, err
	}
	defer suback.Release()
	sa, ok := suback.(*wire.Suback)
	if !ok {
		c.unregisterTrieEntries(sub.trieIDs)
		return SubscriptionToken{}, fmt.Errorf("%w: expected SUBACK got %s", ErrUnexpectedPacket, suback.Type())
	}

	if len(sa.ReasonCodes) != len(filters) {
		c.unregisterTrieEntries(sub.trieIDs)
		return SubscriptionToken{}, fmt.Errorf("mqttv5: SUBACK has %d reason codes, expected %d",
			len(sa.ReasonCodes), len(filters))
	}

	// Drop trie entries for any filter the broker refused, and drop
	// them from the activeSub's filter list so resubscribe doesn't
	// re-request the refused ones.
	c.dropRefused(sub, filters, sa.ReasonCodes)

	if len(sub.filters) == 0 {
		// Every filter refused — nothing to track.
		return SubscriptionToken{client: c}, nil
	}

	c.subsMu.Lock()
	c.activeSubs[sub.id] = sub
	c.subsMu.Unlock()

	return SubscriptionToken{client: c, subID: sub.id}, nil
}

// Unsubscribe sends an UNSUBSCRIBE for the topics covered by token,
// removes the handlers from the matcher, and drops them from the
// resubscribe set so reconnects don't re-request them.
func (c *Client) Unsubscribe(ctx context.Context, token SubscriptionToken) error {
	if !c.Connected() {
		return ErrNotConnected
	}
	if token.subID == 0 {
		return nil
	}

	c.subsMu.Lock()
	sub := c.activeSubs[token.subID]
	if sub != nil {
		delete(c.activeSubs, token.subID)
	}
	c.subsMu.Unlock()
	if sub == nil {
		return nil
	}

	id, err := c.session.AllocateID(ctx)
	if err != nil {
		return fmt.Errorf("mqttv5: allocate packet id: %w", err)
	}
	defer c.session.ReleaseID(id)

	topics := make([]string, len(sub.filters))
	for i, f := range sub.filters {
		topics[i] = f.Topic
	}

	wait := c.installCtrlWaiter(id)

	c.stats.addUnsubscribeSent()
	if werr := c.enqueueAwait(ctx, func(w io.Writer) (int64, error) {
		return wire.WriteUnsubscribe(w, wire.UnsubscribeOpts{
			PacketID: id,
			Topics:   topics,
		})
	}); werr != nil {
		c.removeCtrlWaiter(id)
		return fmt.Errorf("mqttv5: write UNSUBSCRIBE: %w", werr)
	}

	unsuback, err := c.awaitControlAck(ctx, id, wait)
	if err != nil {
		return err
	}
	defer unsuback.Release()
	if _, ok := unsuback.(*wire.Unsuback); !ok {
		return fmt.Errorf("%w: expected UNSUBACK got %s", ErrUnexpectedPacket, unsuback.Type())
	}

	c.unregisterTrieEntries(sub.trieIDs)
	if sub.onUnsub != nil {
		sub.onUnsub()
	}
	return nil
}

// SubscriptionToken identifies an active subscription so Unsubscribe
// can find and remove it.
type SubscriptionToken struct {
	client *Client
	subID  uint64
}

// installCtrlWaiter sets up the SUBACK/UNSUBACK waiter for id.
func (c *Client) installCtrlWaiter(id uint16) <-chan wire.Packet {
	ch := make(chan wire.Packet, 1)
	c.ctrlMu.Lock()
	c.ctrlWaiters[id] = ch
	c.ctrlMu.Unlock()
	return ch
}

func (c *Client) removeCtrlWaiter(id uint16) {
	c.ctrlMu.Lock()
	delete(c.ctrlWaiters, id)
	c.ctrlMu.Unlock()
}

// awaitControlAck blocks on the waiter chan with ctx + shutdown
// supervision.
func (c *Client) awaitControlAck(ctx context.Context, id uint16, wait <-chan wire.Packet) (wire.Packet, error) {
	select {
	case pkt := <-wait:
		return pkt, nil
	case <-ctx.Done():
		c.removeCtrlWaiter(id)
		return nil, ctx.Err()
	case <-c.shutdown:
		c.removeCtrlWaiter(id)
		return nil, ErrClosed
	}
}

// registerHandler installs h in the trie under filter, recording the
// returned id in sub.trieIDs for later Unsubscribe.
//
// Shared-subscription filters ($share/{group}/{filter}) register
// under the underlying filter (no $share prefix), because inbound
// PUBLISHes arrive with the original topic per §4.8.2. The CoW
// CAS-retry handles concurrent SubscribeCallback collisions.
func (c *Client) registerHandler(filter string, h HandlerFunc, sub *activeSub) {
	_, trieKey, _ := parseShareFilter(filter)
	for {
		old := c.router.Load()
		newTree := old.Clone()
		id := newTree.Register(trieKey, h)
		if c.router.CompareAndSwap(old, newTree) {
			sub.trieIDs = append(sub.trieIDs, trieRegEntry{filter: trieKey, id: id})
			return
		}
	}
}

// validateFiltersAgainstBroker fails a Subscribe call before sending
// the SUBSCRIBE packet if any filter or property names a feature the
// broker disabled in CONNACK (§3.2.2.3.{11,12,13}). Wraps a typed
// sentinel: ErrSharedSubsUnsupported, ErrWildcardSubsUnsupported,
// or ErrSubscriptionIDsUnsupported. subID is the optional
// SubscribeOpts.SubscriptionIdentifier; pass nil when unused.
func (c *Client) validateFiltersAgainstBroker(cs *connState, filters []TopicFilter, subID *uint32) error {
	for i, f := range filters {
		topic, isShared := stripShareGroup(f.Topic)
		if isShared && !cs.sharedSubsAvail {
			return fmt.Errorf("%w: filter[%d] = %q", ErrSharedSubsUnsupported, i, f.Topic)
		}
		if containsWildcard(topic) && !cs.wildcardSubsAvail {
			return fmt.Errorf("%w: filter[%d] = %q", ErrWildcardSubsUnsupported, i, f.Topic)
		}
	}
	if subID != nil && !cs.subIDsAvail {
		return fmt.Errorf("%w: SubscriptionIdentifier=%d", ErrSubscriptionIDsUnsupported, *subID)
	}
	return nil
}

// stripShareGroup removes a leading "$share/{group}/" prefix if
// present, returning the remaining filter and whether the prefix
// existed. Used by validateFiltersAgainstBroker so wildcard checks
// run on the actual filter, not on the share group name.
func stripShareGroup(filter string) (rest string, isShared bool) {
	if !strings.HasPrefix(filter, sharedSubPrefix) {
		return filter, false
	}
	tail := filter[len(sharedSubPrefix):]
	slash := strings.IndexByte(tail, '/')
	if slash < 0 {
		// malformed but treat as shared so the typed error reports
		// it correctly; broker will SUBACK-reject anyway.
		return tail, true
	}
	return tail[slash+1:], true
}

// containsWildcard reports whether filter has any '+' or '#'
// character. Used only for capability validation — actual MQTT
// wildcard syntax rules are enforced by the broker.
func containsWildcard(filter string) bool {
	return strings.ContainsAny(filter, "+#")
}

// parseShareFilter extracts the (share group, underlying filter) from
// a "$share/{group}/{filter}" string. Returns isShared=false if the
// filter doesn't have the share prefix, in which case underlying is
// the input verbatim.
//
// Per MQTT v5 §4.8.2.1, the share name must be non-empty, must not
// contain '/', '+', or '#', and the filter portion follows the same
// rules as a regular topic filter.
func parseShareFilter(filter string) (group, underlying string, isShared bool) {
	if !strings.HasPrefix(filter, sharedSubPrefix) {
		return "", filter, false
	}
	rest := filter[len(sharedSubPrefix):]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		// Malformed: missing slash or empty group. Treat as a regular
		// (non-shared) filter — the broker will reject it via SUBACK
		// if it really is invalid.
		return "", filter, false
	}
	return rest[:slash], rest[slash+1:], true
}

// unregisterTrieEntries removes a set of (filter, id) entries from
// the trie via the same CoW pattern.
func (c *Client) unregisterTrieEntries(entries []trieRegEntry) {
	if len(entries) == 0 {
		return
	}
	for {
		old := c.router.Load()
		newTree := old.Clone()
		for _, e := range entries {
			newTree.Unregister(e.filter, e.id)
		}
		if c.router.CompareAndSwap(old, newTree) {
			return
		}
	}
}

// dropRefused removes the trie entries for filters the broker refused
// and trims them out of the activeSub so resubscribe doesn't re-request.
//
// Allocates fresh slices for keptFilters/keptIDs rather than
// reslicing the caller's input — callers (notably ClientGroup) may
// pass the same filter slice into multiple concurrent Subscribe
// calls, and an in-place compaction would race on the shared backing
// array.
func (c *Client) dropRefused(sub *activeSub, filters []TopicFilter, codes []wire.ReasonCode) {
	var refused []trieRegEntry
	keptFilters := make([]TopicFilter, 0, len(filters))
	keptIDs := make([]trieRegEntry, 0, len(sub.trieIDs))
	for i, f := range filters {
		if i < len(codes) && codes[i].IsError() {
			if i < len(sub.trieIDs) {
				refused = append(refused, sub.trieIDs[i])
			}
			continue
		}
		keptFilters = append(keptFilters, f)
		if i < len(sub.trieIDs) {
			keptIDs = append(keptIDs, sub.trieIDs[i])
		}
	}
	sub.filters = keptFilters
	sub.trieIDs = keptIDs
	c.unregisterTrieEntries(refused)
}

// _ enforces that trie.Handler is satisfied by HandlerFunc.
var _ trie.Handler = HandlerFunc(nil)
