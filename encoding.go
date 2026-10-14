// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// Encoding turns Go values into payloads and back for the typed calls:
// [Client.PublishValue] and [Client.SubscribeAs] with their queue,
// callback, [ClientGroup] and [QueuePublisher] variants. One Encoding
// serves every payload type, so a client sets it once ([WithEncoding])
// and names the type per call. The codec/json and codec/msgpack
// submodules provide one each. Implementations must be safe for
// concurrent use.
type Encoding interface {
	// ContentType is the MQTT 5 Content Type a typed publish declares,
	// such as "application/json". Empty declares none.
	ContentType() string

	// Marshal encodes v.
	Marshal(v any) ([]byte, error)

	// Unmarshal decodes data into v, a non-nil pointer. It must not
	// keep data, or slices of it, once it returns: on a [SubZeroCopy]
	// subscription data is the network frame, reused after the message
	// is acked.
	Unmarshal(data []byte, v any) error
}

// ErrNoEncoding is returned by a typed call on a client without an
// [Encoding]: set one with [WithEncoding], or with [SubEncoding] for one
// subscription.
var ErrNoEncoding = errors.New("mqttv5: no Encoding: set WithEncoding, or SubEncoding for one subscription")

// WithEncoding sets the [Encoding] of the client's typed calls. A
// subscription can use another with [SubEncoding].
func WithEncoding(e Encoding) Option {
	return func(c *Config) error {
		if e == nil {
			return errors.New("mqttv5: WithEncoding(nil)")
		}
		c.Encoding = e
		return nil
	}
}

// SubEncoding decodes one typed subscription with e instead of the
// client's [Encoding]. Ignored by untyped subscriptions.
func SubEncoding(e Encoding) SubscribeOption {
	return func(cfg *subscribeConfig) { cfg.encoding = e }
}

// SubOnDecodeError is called with each message of a typed subscription
// whose payload the [Encoding] rejects, and the error. Such a message is
// not delivered: it is acked, so the broker does not send it again, and
// counted in Stats.DecodeFailures. fn runs on the read goroutine before
// the ack, so it must not block, nor call [Client.Disconnect].
func SubOnDecodeError(fn func(*Message, error)) SubscribeOption {
	return func(cfg *subscribeConfig) { cfg.onDecodeError = fn }
}

// TypedMessage is a delivered message with its payload decoded. Value
// and Topic are safe to retain past Ack, including on [SubZeroCopy]
// subscriptions (where Topic is cloned off the frame); the embedded
// *Message follows the usual ownership rules for Payload and Properties.
type TypedMessage[T any] struct {
	*Message
	Topic string
	Value T
}

// PublishValue encodes v with the client's [Encoding] and publishes it
// as [Client.Publish] does, declaring the Encoding's Content Type unless
// opts sets one. opts.Payload must be nil: v is the payload.
func (c *Client) PublishValue(ctx context.Context, opts PublishOptions, v any) error {
	opts, err := encodePayload(c.cfg.Encoding, opts, v)
	if err != nil {
		return err
	}
	return c.Publish(ctx, opts)
}

// PublishValue encodes v with each member's [Encoding] and publishes it
// as [ClientGroup.Publish] does; members with different Encodings send
// differently encoded copies.
func (g *ClientGroup) PublishValue(ctx context.Context, opts PublishOptions, v any) ([]GroupResult, error) {
	return g.publish(ctx, opts.Topic, func(m *Client) (PublishOptions, error) {
		return encodePayload(m.cfg.Encoding, opts, v)
	})
}

// PublishValue encodes v with the client's [Encoding] and queues it as
// [QueuePublisher.Publish] does. An encoding failure is returned here,
// never dead-lettered.
func (p *QueuePublisher) PublishValue(ctx context.Context, opts PublishOptions, v any) error {
	opts, err := encodePayload(p.client.cfg.Encoding, opts, v)
	if err != nil {
		return err
	}
	return p.Publish(ctx, opts)
}

// encodePayload returns opts carrying v encoded by enc as its payload.
func encodePayload(enc Encoding, opts PublishOptions, v any) (PublishOptions, error) {
	if enc == nil {
		return opts, ErrNoEncoding
	}
	if opts.Payload != nil {
		return opts, fmt.Errorf("%w: a typed publish takes its payload from the value, not opts.Payload", ErrInvalidField)
	}
	b, err := enc.Marshal(v)
	if err != nil {
		return opts, fmt.Errorf("mqttv5: encode %T for %q: %w", v, opts.Topic, err)
	}
	opts.Payload = b
	if opts.ContentType == "" {
		opts.ContentType = enc.ContentType()
	}
	return opts, nil
}

// SubscribeAs is [Client.Subscribe] with each payload decoded into a T
// by the subscription's [Encoding]: [SubEncoding], else the client's.
// Decoding runs on the read goroutine as messages arrive; a payload the
// Encoding rejects is not delivered (see [SubOnDecodeError]). Buffering,
// drops, refusals and closing are those of Client.Subscribe, and each
// message must be acked.
func (c *Client) SubscribeAs[T any](ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *TypedMessage[T], SubscriptionToken, error) {
	return c.subscribeChan(ctx, filters, opts, decoder[T])
}

// SubscribeQueueAs is [Client.SubscribeQueue] with each payload decoded
// into a T, as [Client.SubscribeAs] decodes it.
func (c *Client) SubscribeQueueAs[T any](ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*TypedMessage[T]], SubscriptionToken, error) {
	return c.subscribeQueue(ctx, filters, opts, decoder[T], (*TypedMessage[T]).message)
}

// SubscribeCallbackAs is [Client.SubscribeCallback] with each payload
// decoded into a T, as [Client.SubscribeAs] decodes it. h runs on the
// read goroutine and the message is acked when it returns.
func (c *Client) SubscribeCallbackAs[T any](ctx context.Context, filters []TopicFilter, h func(*TypedMessage[T]), opts ...SubscribeOption) (SubscriptionToken, error) {
	if h == nil {
		return SubscriptionToken{}, ErrNilHandler
	}
	return c.subscribeCallback(ctx, filters, opts, decoder[T], h)
}

// SubscribeAs is [ClientGroup.Subscribe] with each payload decoded into
// a T by the delivering member's [Encoding] (or [SubEncoding]), as
// [Client.SubscribeAs] decodes it.
func (g *ClientGroup) SubscribeAs[T any](ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *TypedMessage[T], map[string]SubscriptionToken, error) {
	return g.subscribeChan(ctx, filters, opts, decoder[T])
}

// SubscribeQueueAs is [ClientGroup.SubscribeQueue] with each payload
// decoded into a T, as [ClientGroup.SubscribeAs] decodes it.
func (g *ClientGroup) SubscribeQueueAs[T any](ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*TypedMessage[T]], map[string]SubscriptionToken, error) {
	return g.subscribeQueue(ctx, filters, opts, decoder[T], (*TypedMessage[T]).message)
}

func (m *TypedMessage[T]) message() *Message { return m.Message }

// decoder returns the wrap function of a typed subscription of c: it
// decodes each payload straight into the delivered TypedMessage, so a
// message costs that one allocation besides what the Encoding makes.
func decoder[T any](c *Client, cfg subscribeConfig) (func(*Message) (*TypedMessage[T], bool), error) {
	enc := cfg.encoding
	if enc == nil {
		enc = c.cfg.Encoding
	}
	if enc == nil {
		return nil, ErrNoEncoding
	}
	zeroCopy := cfg.zeroCopyDelivery()
	return func(m *Message) (*TypedMessage[T], bool) {
		tm := &TypedMessage[T]{Message: m, Topic: m.Topic}
		if zeroCopy {
			tm.Topic = strings.Clone(m.Topic)
		}
		if err := enc.Unmarshal(m.Payload, &tm.Value); err != nil {
			c.stats.addDecodeFailure()
			c.cfg.Logger.Warn("mqttv5: payload decode failed", slog.String("topic", m.Topic), slog.Any("error", err))
			if cfg.onDecodeError != nil {
				cfg.onDecodeError(m, err)
			}
			return nil, false
		}
		return tm, true
	}, nil
}
