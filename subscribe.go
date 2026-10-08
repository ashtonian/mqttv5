// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"strings"
)

// ---------------- Subscribe API ----------------

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
	maxQueueSize       int            // queue flavour only (< 0 = unbounded)
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

// SubMaxQueueSize caps [Client.SubscribeQueue]'s length for this
// subscription. [UnboundedQueue] removes the cap; 0 keeps the client
// default from [WithMaxSubscribeQueueSize] ([DefaultMaxSubscribeQueueSize]).
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
// (channel full) or [Client.SubscribeQueue] (cap reached). It runs on the
// read goroutine before the dropped message is acknowledged, so every field
// is readable; it must not block. On a [SubZeroCopy] subscription the
// fields must not be retained past return.
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
// broker already considers the message delivered. Use it for QoS 0 or
// observational consumers; keep manual ack for ledger-style workloads.
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
// Full-buffer messages are dropped + acked (DropNewest); observe drops
// via [SubOnDrop]. Returns [ErrChanDropOldestUnsupported] when called
// with explicit [SubDropPolicy] of [DropOldest].
func (c *Client) Subscribe(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *Message, SubscriptionToken, error) {
	cfg, err := c.chanSubscribeConfig(opts)
	if err != nil {
		return nil, SubscriptionToken{}, err
	}
	ch := make(chan *Message, cfg.bufferSize)
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
// overflow, [SubDropPolicy] decides:
// [DropNewest] acks + drops the inbound; [DropOldest] dequeues +
// acks the head and admits the new one. Closes on
// [Client.Unsubscribe] or [Client.Disconnect]. Refusals and reconnects
// behave as for [Client.Subscribe].
func (c *Client) SubscribeQueue(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*Message], SubscriptionToken, error) {
	cfg := c.subscribeConfigFrom(opts)
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

// chanDeliver hands each message, converted by wrap, to ch without
// blocking the read loop: when ch is full the message is dropped and
// acked. wrap returning false drops and acks the message too (it
// reports why itself).
func chanDeliver[T any](c *Client, cfg subscribeConfig, ch chan<- T, wrap func(*Message) (T, bool)) HandlerFunc {
	return func(m *Message) {
		v, ok := wrap(m)
		if !ok {
			_ = m.Ack()
			return
		}
		if cfg.autoAck {
			_ = m.Ack()
		}
		select {
		case ch <- v:
		default:
			c.dropInbound(cfg, m)
		}
	}
}

// queueDeliver appends each message, converted by wrap, to q within
// cfg's bound and drop policy; message recovers the *Message an evicted
// element carries so it can be acked.
func queueDeliver[T any](c *Client, cfg subscribeConfig, q *Queue[T], wrap func(*Message) (T, bool), message func(T) *Message) HandlerFunc {
	return func(m *Message) {
		v, ok := wrap(m)
		if !ok {
			_ = m.Ack()
			return
		}
		if cfg.autoAck {
			_ = m.Ack()
		}
		evicted, wasEvicted, accepted := q.push(v, cfg.maxQueueSize, cfg.dropPolicy == DropOldest)
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
	_ = m.Ack()
}

// SubscribeCallback registers a subscription that invokes h for every
// matching inbound PUBLISH.
//
// h runs synchronously on the read goroutine and MUST NOT block — a
// slow handler stalls PINGRESP and drops the connection. The runtime
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
		_ = m.Ack()
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
