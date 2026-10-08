// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"fmt"
	"strings"
)

// Codec encodes and decodes payloads of type T. Implementations live
// in separate submodules so the core stays codec-agnostic.
type Codec[T any] interface {
	Encode(v T) ([]byte, error)
	Decode(b []byte) (T, error)
}

// TypedMessage wraps Message with an already-decoded Value. Value and
// Topic are safe to retain past Ack, including on [SubZeroCopy]
// subscriptions (where Topic is cloned off the frame); the embedded
// *Message follows the usual ownership rules for Payload and Properties.
type TypedMessage[T any] struct {
	*Message
	Topic string
	Value T
}

// Typed pairs a *Client with a Codec[T] for typed publish/subscribe.
type Typed[T any] struct {
	client *Client
	codec  Codec[T]
}

// NewTyped wraps c with codec.
func NewTyped[T any](c *Client, codec Codec[T]) *Typed[T] {
	return &Typed[T]{client: c, codec: codec}
}

// Publish encodes v and sends it. opts.Payload, if set, is overwritten.
func (t *Typed[T]) Publish(ctx context.Context, opts PublishOptions, v T) error {
	b, err := t.codec.Encode(v)
	if err != nil {
		return fmt.Errorf("mqttv5/typed: encode: %w", err)
	}
	opts.Payload = b
	return t.client.Publish(ctx, opts)
}

// Subscribe is [Client.Subscribe] with each payload decoded by the
// codec. A payload that fails to decode is logged, acked and dropped —
// for stricter behaviour wrap the codec or use SubscribeCallback. The
// channel's buffer, drop policy and closing are exactly those of
// Client.Subscribe; the caller MUST call Ack on each TypedMessage. A
// [*SubscribeError] comes with the channel and token, as from
// Client.Subscribe.
func (t *Typed[T]) Subscribe(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *TypedMessage[T], SubscriptionToken, error) {
	cfg, err := t.client.chanSubscribeConfig(opts)
	if err != nil {
		return nil, SubscriptionToken{}, err
	}
	ch := make(chan *TypedMessage[T], cfg.bufferSize)
	r := &route{zeroCopy: cfg.zeroCopyDelivery(), deliver: chanDeliver(t.client, cfg, ch, t.decode(cfg))}
	token, err := t.client.subscribe(ctx, filters, r, func() { close(ch) })
	if token.sub == nil {
		return nil, token, err
	}
	return ch, token, err
}

// SubscribeQueue is [Client.SubscribeQueue] with each payload decoded
// by the codec, under the same bound and drop policy.
func (t *Typed[T]) SubscribeQueue(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*TypedMessage[T]], SubscriptionToken, error) {
	cfg := t.client.subscribeConfigFrom(opts)
	q := NewQueue[*TypedMessage[T]]()
	r := &route{zeroCopy: cfg.zeroCopyDelivery(), deliver: queueDeliver(t.client, cfg, q, t.decode(cfg),
		func(m *TypedMessage[T]) *Message { return m.Message })}
	token, err := t.client.subscribe(ctx, filters, r, q.Close)
	if token.sub == nil {
		return nil, token, err
	}
	return q, token, err
}

// decode returns the wrap function that turns a Message into a
// TypedMessage, logging payloads the codec rejects.
func (t *Typed[T]) decode(cfg subscribeConfig) func(*Message) (*TypedMessage[T], bool) {
	zeroCopy := cfg.zeroCopyDelivery()
	return func(m *Message) (*TypedMessage[T], bool) {
		v, err := t.codec.Decode(m.Payload)
		if err != nil {
			t.client.cfg.Logger.Warn("mqttv5/typed: decode failed", "topic", m.Topic, "error", err)
			return nil, false
		}
		return newTypedMessage(m, v, zeroCopy), true
	}
}

func newTypedMessage[T any](m *Message, v T, zeroCopy bool) *TypedMessage[T] {
	topic := m.Topic
	if zeroCopy {
		topic = strings.Clone(topic)
	}
	return &TypedMessage[T]{Message: m, Topic: topic, Value: v}
}
