// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"fmt"
	"strings"

	"github.com/ashtonian/mqttv5/wire"
)

// Codec encodes and decodes payloads of type T. Implementations live
// in separate submodules so the core stays codec-agnostic.
type Codec[T any] interface {
	Encode(v T) ([]byte, error)
	Decode(b []byte) (T, error)
}

// TypedMessage wraps Message with an already-decoded Value and a
// detached Topic. Both Value and Topic are safe to retain past Ack —
// Value via the codec's allocation, Topic via an explicit
// strings.Clone at construction. The embedded *Message still aliases
// the frame for Payload / Properties access; use Message.ClonePayload
// or read those fields before calling Ack if you need them.
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
func (t *Typed[T]) Publish(ctx context.Context, opts wire.PublishOpts, v T) error {
	b, err := t.codec.Encode(v)
	if err != nil {
		return fmt.Errorf("mqttv5/typed: encode: %w", err)
	}
	opts.Payload = b
	return t.client.Publish(ctx, opts)
}

// Subscribe wraps Client.Subscribe with a decode stage. Decode
// failures are logged + acked + dropped — for stricter behaviour
// wrap the codec or use SubscribeCallback. Caller MUST call Ack on
// each TypedMessage.
func (t *Typed[T]) Subscribe(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *TypedMessage[T], SubscriptionToken, error) {
	raw, token, err := t.client.Subscribe(ctx, filters, opts...)
	if err != nil {
		return nil, SubscriptionToken{}, err
	}

	out := make(chan *TypedMessage[T], cap(raw))
	go func() {
		defer close(out)
		for m := range raw {
			v, err := t.codec.Decode(m.Payload)
			if err != nil {
				t.client.cfg.Logger.Warn("mqttv5/typed: decode failed",
					"topic", m.Topic, "error", err)
				_ = m.Ack()
				continue
			}
			out <- &TypedMessage[T]{
				Message: m,
				Topic:   strings.Clone(m.Topic),
				Value:   v,
			}
		}
	}()

	return out, token, nil
}

// SubscribeQueue is the queue-backed variant of Subscribe.
func (t *Typed[T]) SubscribeQueue(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*TypedMessage[T]], SubscriptionToken, error) {
	rawQ, token, err := t.client.SubscribeQueue(ctx, filters, opts...)
	if err != nil {
		return nil, SubscriptionToken{}, err
	}

	out := NewQueue[*TypedMessage[T]]()
	go func() {
		defer out.Close()
		for {
			m, ok := rawQ.Dequeue(context.Background())
			if !ok {
				return
			}
			v, err := t.codec.Decode(m.Payload)
			if err != nil {
				t.client.cfg.Logger.Warn("mqttv5/typed: decode failed",
					"topic", m.Topic, "error", err)
				_ = m.Ack()
				continue
			}
			out.Enqueue(&TypedMessage[T]{
				Message: m,
				Topic:   strings.Clone(m.Topic),
				Value:   v,
			})
		}
	}()

	return out, token, nil
}
