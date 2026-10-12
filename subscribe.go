// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/wire"
)

// SubscribeOption configures client-side delivery for one Subscribe
// call. Protocol flags (QoS, NoLocal, etc.) live on TopicFilter
// itself — these options control what happens after delivery.
type SubscribeOption func(*subscribeConfig)

// DefaultSubscribeBuffer is the channel buffer size used by Subscribe
// when SubBuffer is not called: how many QoS 0 messages the channel
// holds before it drops one.
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
	maxQueueSize       int            // queue flavour only (< 0 = unbounded)
	room               int            // room for QoS 1/2 beyond the buffer or cap (see qosRoom)
	dropPolicy         DropPolicy     // queue flavour honors both; chan rejects explicit DropOldest
	dropPolicyExplicit bool           // SubDropPolicy was called for this Subscribe
	onDrop             func(*Message) // optional: fires before a dropped message is acked
	autoAck            bool           // SubAutoAck: ack before delivery
	zeroCopy           bool           // SubZeroCopy: fields alias the network frame
}

func newSubscribeConfig(c *Client) subscribeConfig {
	return subscribeConfig{
		bufferSize:   DefaultSubscribeBuffer,
		maxQueueSize: c.cfg.MaxSubscribeQueueSize,
		dropPolicy:   c.cfg.DropPolicy,
	}
}

// SubBuffer sets [Client.Subscribe]'s channel buffer size: how many
// QoS 0 messages the channel holds before it drops one. QoS 1 and 2
// messages have room beyond it (see [Client.Subscribe]). Default
// [DefaultSubscribeBuffer]. Ignored by [Client.SubscribeQueue] and
// [Client.SubscribeCallback].
func SubBuffer(n int) SubscribeOption {
	return func(cfg *subscribeConfig) {
		if n > 0 {
			cfg.bufferSize = n
		}
	}
}

// SubMaxQueueSize caps [Client.SubscribeQueue]'s length for this
// subscription. QoS 1 and 2 messages have room beyond the cap (see
// [Client.SubscribeQueue]). [UnboundedQueue] removes the cap; 0 keeps
// the client default from [WithMaxSubscribeQueueSize]
// ([DefaultMaxSubscribeQueueSize]).
func SubMaxQueueSize(n int) SubscribeOption {
	return func(cfg *subscribeConfig) {
		if n != 0 {
			cfg.maxQueueSize = n
		}
	}
}

// SubDropPolicy chooses behaviour when the channel buffer or queue
// cap fills.
//
//   - Subscribe (chan): only DropNewest is supported. Passing
//     DropOldest here causes Subscribe to return
//     ErrChanDropOldestUnsupported — peek-and-pop on a consumer-owned
//     channel would race the receiver. Use SubscribeQueue for
//     DropOldest semantics.
//   - SubscribeQueue: both policies are honored. DropOldest keeps the
//     newest messages up to the cap whatever their QoS, so it may drop
//     QoS 1 and 2 messages too.
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
// (buffer full) or [Client.SubscribeQueue] (cap reached). It runs on the
// read goroutine before the dropped message is acknowledged, so every field
// is readable; it must not block, nor call [Client.Disconnect]. On a
// [SubZeroCopy] subscription the fields must not be retained past return.
func SubOnDrop(fn func(*Message)) SubscribeOption {
	return func(cfg *subscribeConfig) { cfg.onDrop = fn }
}

// SubZeroCopy delivers Messages whose Topic, Payload and Properties alias
// the pooled network frame instead of an owned copy, saving one copy of
// the packet per message. The fields are valid only until the Message is
// acked by every subscription it reached (for [Client.SubscribeCallback],
// until the callback returns); use [Message.CloneTopic] /
// [Message.ClonePayload] to keep them longer. QoS 0 messages must also be
// acked so the frame returns to the pool. A PUBLISH that also matches a
// subscription without SubZeroCopy is delivered as an owned copy to all
// of them. Ignored together with [SubAutoAck], which acks before
// delivery and therefore needs an owned copy.
func SubZeroCopy() SubscribeOption {
	return func(cfg *subscribeConfig) { cfg.zeroCopy = true }
}

// SubAutoAck makes [Client.Subscribe] / [Client.SubscribeQueue] ack
// each delivery on the read goroutine before handing it to the
// consumer; [Message.Ack] is then a no-op.
//
// Auto-ack gives up at-least-once processing: a consumer that crashes
// between delivery and processing has nothing to replay, because the
// broker already considers the message delivered. It also gives up the
// broker's flow control, so a full buffer or queue drops QoS 1 and 2
// messages as it drops QoS 0. Use it for QoS 0 or observational
// consumers; keep manual ack for ledger-style workloads.
//
// Ignored by [Client.SubscribeCallback] (already auto-acks).
func SubAutoAck() SubscribeOption {
	return func(cfg *subscribeConfig) { cfg.autoAck = true }
}

// Subscribe sends one SUBSCRIBE for filters and returns a buffered
// channel of matching inbound messages. Callers MUST call
// [Message.Ack] on each. Returns once the SUBACK arrives — if the
// connection drops first the SUBSCRIBE is sent again on the next one —
// or when ctx ends. The channel closes on [Client.Unsubscribe] or
// [Client.Disconnect], or when the broker refuses every filter.
//
// When the broker refuses filters Subscribe returns a
// [*SubscribeError] together with the channel and token: the channel
// carries the granted filters, or is already closed if none were.
// token.Results() holds each filter's outcome.
//
// The broker keeps one subscription per filter: subscriptions of this
// client with the same filter share it, each receives every message for
// it, and the options of the most recent Subscribe apply to all of them.
//
// A consumer that falls behind loses QoS 0 messages, never QoS 1 or 2:
// once the channel holds [SubBuffer] messages, a QoS 0 message is
// dropped and acked (observe drops via [SubOnDrop]), while QoS 1 and 2
// messages have room for [WithReceiveMaximum] more. That is as many as
// the broker sends before the consumer acks one, so the broker's flow
// control holds the rest back. The exceptions: with [SubAutoAck], QoS 1
// and 2 messages are dropped as QoS 0 ones are, and messages left
// unread from a session the broker discarded take room of their own.
// Returns [ErrChanDropOldestUnsupported] when called with explicit
// [SubDropPolicy] of [DropOldest].
func (c *Client) Subscribe(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *Message, SubscriptionToken, error) {
	cfg, err := c.chanSubscribeConfig(opts)
	if err != nil {
		return nil, SubscriptionToken{}, err
	}
	cfg.room = c.qosRoom(cfg, filters)
	ch := make(chan *Message, cfg.bufferSize+cfg.room)
	r := &route{zeroCopy: cfg.zeroCopyDelivery(), deliver: chanDeliver(c, cfg, ch, ownMessage)}
	token, err := c.subscribe(ctx, filters, r, func() { close(ch) })
	if token.sub == nil {
		return nil, token, err
	}
	// A *SubscribeError still comes with the subscription: active for
	// the granted filters, or closed when none were.
	return ch, token, err
}

// SubscribeQueue sends one SUBSCRIBE and returns a [Queue] of
// matching messages, capped at [DefaultMaxSubscribeQueueSize] unless
// [WithMaxSubscribeQueueSize] or [SubMaxQueueSize] say otherwise. On
// overflow, [SubDropPolicy] decides: [DropNewest] acks and drops the
// inbound message, except that QoS 1 and 2 messages have room for
// [WithReceiveMaximum] more beyond the cap, as on [Client.Subscribe];
// [DropOldest] dequeues and acks the head, whatever its QoS, and admits
// the new one. Closes on [Client.Unsubscribe] or [Client.Disconnect].
// Refusals and reconnects behave as for [Client.Subscribe].
func (c *Client) SubscribeQueue(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*Message], SubscriptionToken, error) {
	cfg := c.subscribeConfigFrom(opts)
	cfg.room = c.qosRoom(cfg, filters)
	q := NewQueue[*Message]()
	r := &route{zeroCopy: cfg.zeroCopyDelivery(), deliver: queueDeliver(c, cfg, q, ownMessage, func(m *Message) *Message { return m })}
	token, err := c.subscribe(ctx, filters, r, q.Close)
	if token.sub == nil {
		return nil, token, err
	}
	return q, token, err
}

func ownMessage(m *Message) (*Message, bool) { return m, true }

func (c *Client) subscribeConfigFrom(opts []SubscribeOption) subscribeConfig {
	cfg := newSubscribeConfig(c)
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// chanSubscribeConfig is subscribeConfigFrom for channel outputs, which
// cannot drop their oldest element: an explicit DropOldest is an error,
// a client-wide one falls back to DropNewest.
func (c *Client) chanSubscribeConfig(opts []SubscribeOption) (subscribeConfig, error) {
	cfg := c.subscribeConfigFrom(opts)
	if cfg.dropPolicy == DropOldest {
		if cfg.dropPolicyExplicit {
			return cfg, ErrChanDropOldestUnsupported
		}
		cfg.dropPolicy = DropNewest
	}
	return cfg, nil
}

// zeroCopyDelivery reports whether messages may alias the frame:
// SubAutoAck acks before delivery and so needs an owned copy.
func (cfg subscribeConfig) zeroCopyDelivery() bool { return cfg.zeroCopy && !cfg.autoAck }

// qosRoom is the room a channel or queue subscription keeps beyond its
// buffer or cap for QoS 1 and 2 messages, so that none is dropped. A
// message stays unacknowledged until its consumer acks it, and the
// broker sends at most Receive Maximum unacknowledged ones, so that much
// room always suffices. A subscription that acks on receipt has no flow
// control to rely on, and one that asks only for QoS 0 needs none.
func (c *Client) qosRoom(cfg subscribeConfig, filters []TopicFilter) int {
	if cfg.autoAck {
		return 0
	}
	for _, f := range filters {
		if f.QoS > 0 {
			return int(c.cfg.ReceiveMaximum)
		}
	}
	return 0
}

// usesRoom reports whether m may take the room beyond the buffer or cap.
func (cfg subscribeConfig) usesRoom(m *Message) bool { return cfg.room > 0 && m.QoS > 0 }

// chanDeliver hands each message, converted by wrap, to ch without
// blocking the read loop. ch holds cfg.bufferSize messages plus
// cfg.room: a message that may not use the room is dropped and acked
// once ch holds cfg.bufferSize, and any message when ch is full. wrap
// returning false drops and acks the message too (it reports why
// itself). Like every delivery function, it runs on the read loop.
func chanDeliver[T any](c *Client, cfg subscribeConfig, ch chan<- T, wrap func(*Message) (T, bool)) HandlerFunc {
	return func(m *Message) {
		v, ok := wrap(m)
		if !ok {
			m.ackOnReader()
			return
		}
		if cfg.autoAck {
			m.ackOnReader()
		}
		if !cfg.usesRoom(m) && len(ch) >= cfg.bufferSize {
			c.dropInbound(cfg, m)
			return
		}
		select {
		case ch <- v:
		default:
			c.dropInbound(cfg, m)
		}
	}
}

// queueDeliver appends each message, converted by wrap, to q within
// cfg's bound, raised by cfg.room for a message that may use it, and
// drop policy; message recovers the *Message an evicted element carries
// so it can be acked.
func queueDeliver[T any](c *Client, cfg subscribeConfig, q *Queue[T], wrap func(*Message) (T, bool), message func(T) *Message) HandlerFunc {
	return func(m *Message) {
		v, ok := wrap(m)
		if !ok {
			m.ackOnReader()
			return
		}
		if cfg.autoAck {
			m.ackOnReader()
		}
		limit := cfg.maxQueueSize
		if limit > 0 && cfg.dropPolicy == DropNewest && cfg.usesRoom(m) {
			limit += cfg.room
		}
		evicted, wasEvicted, accepted := q.push(v, limit, cfg.dropPolicy == DropOldest)
		if wasEvicted {
			c.dropInbound(cfg, message(evicted))
		}
		if !accepted {
			c.dropInbound(cfg, m)
		}
	}
}

// dropInbound discards m for want of room, reporting it first.
func (c *Client) dropInbound(cfg subscribeConfig, m *Message) {
	c.stats.addInboundDropped()
	if cfg.onDrop != nil {
		cfg.onDrop(m)
	}
	m.ackOnReader()
}

// SubscribeCallback registers a subscription that invokes h for every
// matching inbound PUBLISH.
//
// h runs synchronously on the read goroutine and MUST NOT block — a
// slow handler stalls PINGRESP and drops the connection. It must not
// call [Client.Disconnect], which waits for the read goroutine: start
// it on another goroutine. The runtime
// auto-acks via [Message.Ack] after h returns; use [Client.Subscribe]
// or [Client.SubscribeQueue] when ack must be deferred past h.
//
// Of the [SubscribeOption]s only [SubZeroCopy] applies; with it, the
// message's fields are valid only until h returns.
// Returns [ErrNilHandler] when h is nil.
func (c *Client) SubscribeCallback(ctx context.Context, filters []TopicFilter, h HandlerFunc, opts ...SubscribeOption) (SubscriptionToken, error) {
	if h == nil {
		return SubscriptionToken{}, ErrNilHandler
	}
	cfg := newSubscribeConfig(c)
	for _, opt := range opts {
		opt(&cfg)
	}
	r := &route{zeroCopy: cfg.zeroCopy, sync: true}
	r.deliver = func(m *Message) {
		h(m)
		m.ackOnReader()
	}
	return c.subscribe(ctx, filters, r, nil)
}

// stripShareGroup removes a leading "$share/{group}/" prefix if
// present, returning the remaining filter and whether the prefix
// existed. Used by checkFilterCapabilities so wildcard checks run on
// the actual filter, not on the share group name.
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
// Per MQTT v5 §4.8.2, the share name must be non-empty, must not
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

// SubscribeResult is the broker's answer for one filter of a SUBSCRIBE.
type SubscribeResult struct {
	Filter TopicFilter
	// Reason is the SUBACK reason code: 0x00–0x02 grant that QoS,
	// 0x80 and above refuse the filter.
	Reason wire.ReasonCode
}

// Granted reports whether the broker accepted the filter.
func (r SubscribeResult) Granted() bool { return !r.Reason.IsError() }

// QoS is the maximum QoS the broker granted; meaningful when Granted.
func (r SubscribeResult) QoS() byte { return byte(r.Reason) }

// SubscribeError reports filters the broker refused. Results holds the
// outcome of every filter of the SUBSCRIBE. When some filters were
// granted the subscription is active for those; when none were, its
// channel or queue is closed.
type SubscribeError struct {
	Results []SubscribeResult
}

func (e *SubscribeError) Error() string {
	var refused []string
	for _, r := range e.Results {
		if !r.Granted() {
			refused = append(refused, fmt.Sprintf("%q (0x%02X %s)", r.Filter.Topic, byte(r.Reason), reasonName(r.Reason)))
		}
	}
	return fmt.Sprintf("mqttv5: broker refused %d of %d filters: %s", len(refused), len(e.Results), strings.Join(refused, ", "))
}

// Is matches the sentinel of any refusal's reason code (see
// [ReasonCodeError]).
func (e *SubscribeError) Is(target error) bool {
	for _, r := range e.Results {
		if !r.Granted() && (&ReasonCodeError{Packet: wire.SUBACK, Code: r.Reason}).Is(target) {
			return true
		}
	}
	return false
}

// UnsubscribeResult is the broker's answer for one topic filter of an
// UNSUBSCRIBE.
type UnsubscribeResult struct {
	Topic string
	// Reason is the UNSUBACK reason code: 0x00 success, 0x11 there was no
	// such subscription (not an error), 0x80 and above refused.
	Reason wire.ReasonCode
}

// UnsubscribeError reports topic filters the broker refused to
// unsubscribe. The local subscription is closed regardless; the broker
// may keep delivering messages for them, which the client then drops.
type UnsubscribeError struct {
	Results []UnsubscribeResult
}

func (e *UnsubscribeError) Error() string {
	var refused []string
	for _, r := range e.Results {
		if r.Reason.IsError() {
			refused = append(refused, fmt.Sprintf("%q (0x%02X %s)", r.Topic, byte(r.Reason), reasonName(r.Reason)))
		}
	}
	return "mqttv5: broker refused to unsubscribe " + strings.Join(refused, ", ")
}

// SubscriptionToken identifies a subscription for [Client.Unsubscribe]
// and reports what the broker decided.
type SubscriptionToken struct {
	client *Client
	sub    *activeSub
}

// Results returns the broker's answer per filter from the subscription's
// SUBACK, or nil before it arrived.
func (t SubscriptionToken) Results() []SubscribeResult {
	if t.sub == nil {
		return nil
	}
	t.client.subsMu.Lock()
	defer t.client.subsMu.Unlock()
	return slices.Clone(t.sub.results)
}

// Err reports why the subscription ended other than by Unsubscribe or
// Disconnect: the broker refused every filter, initially or when the
// client re-subscribed after a session loss. Nil while it is active.
func (t SubscriptionToken) Err() error {
	if t.sub == nil {
		return nil
	}
	t.client.subsMu.Lock()
	defer t.client.subsMu.Unlock()
	return t.sub.err
}

// Message is one inbound PUBLISH as seen by one subscription handler.
//
// By default a Message owns its memory: Topic, Payload and Properties are
// copied out of the network frame before any handler runs, stay valid for
// as long as the Message is reachable, and may be retained freely. When a
// PUBLISH matches several subscriptions, each handler receives its own
// *Message; the handles share the same read-only bytes.
//
// Subscriptions created with [SubZeroCopy] instead receive Messages whose
// Topic, Payload and Properties alias the pooled network frame. Those fields
// are valid only until every handler's Ack has returned.
//
// Ack acknowledges the message to the broker (PUBACK for QoS 1, PUBREC for
// QoS 2, emitted in arrival order per §4.6) once every handler that received
// the PUBLISH has acked. Ack is idempotent per Message: later calls are
// no-ops and never affect any other message.
type Message struct {
	Topic      string
	Payload    []byte
	Properties Properties
	QoS        byte
	Retain     bool
	Dup        bool
	PacketID   uint16

	d     *delivery
	acked atomic.Bool
}

// Ack acknowledges the message for this handler. It is safe to call more
// than once and from any goroutine; only the first call counts. For QoS 0
// messages Ack is only needed on zero-copy subscriptions, where it returns
// the network frame to the pool.
func (m *Message) Ack() error {
	if m.d == nil || !m.acked.CompareAndSwap(false, true) {
		return nil
	}
	m.d.release(false)
	return nil
}

// ackOnReader acks m on the read loop, which sends the acknowledgement
// itself before it next waits for the broker (see Client.sendReady).
func (m *Message) ackOnReader() {
	if m.d == nil || !m.acked.CompareAndSwap(false, true) {
		return
	}
	m.d.release(true)
}

// CloneTopic returns a copy of Topic. Only needed on zero-copy
// subscriptions; owned messages may simply keep m.Topic.
func (m *Message) CloneTopic() string { return strings.Clone(m.Topic) }

// ClonePayload returns a copy of Payload. Only needed on zero-copy
// subscriptions; owned messages may simply keep m.Payload.
func (m *Message) ClonePayload() []byte { return bytes.Clone(m.Payload) }

// PublishOptions returns options that publish m again as it arrived:
// topic, payload, QoS, Retain, and the properties a client may send —
// Payload Format Indicator, the Message Expiry Interval the broker left
// it, Content Type, Response Topic, Correlation Data and User
// Properties. A bridge changes the Topic or QoS and publishes it on
// another connection. Retain is the flag as delivered: the publisher's
// only on a subscription with RetainAsPublished. The options share m's
// memory, so on a zero-copy subscription publish them before acking m.
func (m *Message) PublishOptions() PublishOptions {
	o := PublishOptions{
		Topic:           m.Topic,
		Payload:         m.Payload,
		QoS:             m.QoS,
		Retain:          m.Retain,
		ContentType:     m.Properties.ContentType(),
		ResponseTopic:   m.Properties.ResponseTopic(),
		CorrelationData: m.Properties.CorrelationData(),
	}
	if v, ok := m.Properties.PayloadFormatIndicator(); ok {
		o.PayloadFormatIndicator = &v
	}
	if v, ok := m.Properties.MessageExpiryInterval(); ok {
		o.MessageExpiryInterval = &v
	}
	for k, v := range m.Properties.UserProperties() {
		o.UserProperties = append(o.UserProperties, UserProperty{Key: k, Value: v})
	}
	return o
}

// delivery is one inbound PUBLISH shared by every handler it matched. The
// broker acknowledgement is queued, and a zero-copy frame released, when
// the last handler's Message is acked.
type delivery struct {
	client *Client
	refs   atomic.Int32
	in     *inflight.In  // QoS 1/2 flow; nil for QoS 0
	frame  *wire.Publish // zero-copy deliveries only
	first  Message       // the first handler's handle, saving one allocation
}

// release drops one handle; the last queues the broker acknowledgement.
// onReader means the read loop is releasing it and sends the
// acknowledgement itself.
func (d *delivery) release(onReader bool) {
	if d.refs.Add(-1) != 0 {
		return
	}
	switch {
	case d.in == nil:
	case onReader:
		d.client.engine.AckDeferred(d.in)
	default:
		d.client.engine.Ack(d.in)
	}
	if d.frame != nil {
		d.frame.Release()
		d.frame = nil
	}
}

// fillOwned copies pub's topic, payload and property bytes into one
// exactly-sized allocation owned by m. The copy is never mutated afterwards,
// which is what makes the unsafe.String view of the topic sound; Payload is
// capacity-limited so appending to it cannot overwrite the property bytes.
func fillOwned(m *Message, pub *wire.Publish) {
	props := pub.Properties.Raw()
	t, p := len(pub.Topic), len(pub.Payload)
	buf := make([]byte, t+p+len(props))
	copy(buf, pub.Topic)
	copy(buf[t:], pub.Payload)
	copy(buf[t+p:], props)
	if t > 0 {
		m.Topic = unsafe.String(&buf[0], t)
	}
	m.Payload = buf[t : t+p : t+p]
	if len(props) > 0 {
		m.Properties = Properties{wire.PropertiesFromBytes(buf[t+p:])}
	}
	setHeader(m, pub)
}

// fillZeroCopy points m's fields at the frame.
func fillZeroCopy(m *Message, pub *wire.Publish) {
	m.Topic, m.Payload, m.Properties = pub.Topic, pub.Payload, Properties{pub.Properties}
	setHeader(m, pub)
}

func setHeader(m *Message, pub *wire.Publish) {
	m.QoS, m.Retain, m.Dup, m.PacketID = pub.QoS, pub.Retain, pub.Dup, pub.PacketID
}

// handleFor returns another handle onto the same delivery. Fields are
// assigned one by one: Message holds an atomic and must not be copied.
func handleFor(src *Message) *Message {
	return &Message{
		Topic: src.Topic, Payload: src.Payload, Properties: src.Properties,
		QoS: src.QoS, Retain: src.Retain, Dup: src.Dup, PacketID: src.PacketID,
		d: src.d,
	}
}

// route is one subscription's entry in the topic trie.
type route struct {
	deliver  HandlerFunc
	zeroCopy bool
	// sync routes (SubscribeCallback) run user code on the read goroutine
	// and are not held under mu while doing so.
	sync bool

	mu     sync.RWMutex
	closed bool
}

// dispatch hands m to the subscription unless it has been closed, in which
// case m is acked so the broker isn't left waiting. It runs on the read
// loop. Channel and queue sends happen under the read lock, so close can
// never race a send on a closed channel.
func (r *route) dispatch(m *Message) {
	if r.sync {
		r.mu.RLock()
		closed := r.closed
		r.mu.RUnlock()
		if closed {
			m.ackOnReader()
			return
		}
		r.deliver(m)
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		m.ackOnReader()
		return
	}
	r.deliver(m)
}

// close marks the route closed and runs onClose (which closes the
// subscription's channel or queue) exactly once.
func (r *route) close(onClose func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	if onClose != nil {
		onClose()
	}
}

// Queue is an unbounded MPSC FIFO of T. Enqueue is non-blocking;
// Dequeue blocks until an item arrives, the queue is closed, or ctx
// cancels. After Close, queued items are still returned by Dequeue;
// once drained, Dequeue returns (zero, false).
type Queue[T any] struct {
	mu     sync.Mutex
	head   *queueNode[T]
	tail   *queueNode[T]
	count  int
	notify chan struct{}
	done   chan struct{}
	closed bool
}

type queueNode[T any] struct {
	value T
	next  *queueNode[T]
}

// NewQueue returns a ready-to-use Queue.
func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

// Enqueue appends an item. Returns false if the queue is closed.
func (q *Queue[T]) Enqueue(item T) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	n := &queueNode[T]{value: item}
	if q.tail == nil {
		q.head = n
	} else {
		q.tail.next = n
	}
	q.tail = n
	q.count++
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

// push appends item atomically against a bound of max items (0: none).
// At the bound it refuses item, or with dropOldest removes the head and
// returns it as evicted. accepted is false when item was refused or the
// queue is closed.
func (q *Queue[T]) push(item T, max int, dropOldest bool) (evicted T, wasEvicted, accepted bool) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return evicted, false, false
	}
	if max > 0 && q.count >= max {
		if !dropOldest {
			q.mu.Unlock()
			return evicted, false, false
		}
		evicted, wasEvicted = q.popLocked()
	}
	n := &queueNode[T]{value: item}
	if q.tail == nil {
		q.head = n
	} else {
		q.tail.next = n
	}
	q.tail = n
	q.count++
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return evicted, wasEvicted, true
}

// Dequeue blocks until an item is available, the queue is closed, or
// ctx cancels. ok is false when no item could be retrieved.
func (q *Queue[T]) Dequeue(ctx context.Context) (T, bool) {
	for {
		q.mu.Lock()
		if item, ok := q.popLocked(); ok {
			q.mu.Unlock()
			return item, true
		}
		closed := q.closed
		q.mu.Unlock()

		if closed {
			var zero T
			return zero, false
		}

		select {
		case <-q.notify:
		case <-q.done:
		case <-ctx.Done():
			var zero T
			return zero, false
		}
	}
}

// TryDequeue removes and returns the head without blocking.
func (q *Queue[T]) TryDequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.popLocked()
}

func (q *Queue[T]) popLocked() (T, bool) {
	var zero T
	if q.head == nil {
		return zero, false
	}
	n := q.head
	q.head = n.next
	if q.head == nil {
		q.tail = nil
	}
	q.count--
	n.next = nil // release for GC — important when T is a pointer
	return n.value, true
}

// Len returns the current item count. Mostly for tests and metrics.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// Close signals waiting Dequeue calls to drain. Items already queued
// remain available.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.done)
}
