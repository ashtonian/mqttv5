// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
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
// code 0x80 or above) is returned as a [*ReasonCodeError].
//
// A call that waits for its packet to be written — QoS 1/2, or QoS 0
// with [PublishWaitForFlush] — writes it on the calling goroutine when
// the connection is a TCP or Unix socket and nothing is queued for its
// writer goroutine, saving the hand-off; otherwise the writer sends it
// behind the queued packets. ctx bounds the call either way: a write it
// interrupts is finished by the writer goroutine, so the packet may
// still reach the broker after Publish returned ctx's error.
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
		if !errors.Is(err, ErrNoHealthyPublishers) {
			return err
		}
		// No member could send it: use the main connection.
		c.stats.addPoolFallback()
	}

	cs := c.cur.Load()
	if cs == nil {
		return ErrNotConnected
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

// publishTracked starts a QoS 1/2 exchange on the current connection
// and returns without waiting for it: settle receives the outcome (see
// [inflight.Message]) and ref is stored with the session record so the
// exchange can be adopted after a restart. The publisher pool is not
// used.
func (c *Client) publishTracked(ctx context.Context, po PublishOptions, ref []byte, settle func(context.Context, error) error) error {
	if po.QoS != 1 && po.QoS != 2 {
		return fmt.Errorf("mqttv5: tracked publish needs QoS 1 or 2, not %d", po.QoS)
	}
	cs := c.cur.Load()
	if cs == nil {
		return ErrNotConnected
	}
	opts := po.wire()
	if err := c.applyServerLimits(cs, &opts); err != nil {
		return err
	}
	_, err := c.startReliable(ctx, opts, ref, settle)
	return err
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
// alias is allocated and the packet written or queued under one lock,
// so a PUBLISH that uses an alias can never be written before the one
// that registers it, and a new alias is kept only once its
// PUBLISH is admitted — written, or queued for the writer — so one the
// broker never saw is never used.
func (c *Client) publishQoS0(ctx context.Context, cs *connState, opts wire.PublishOpts) error {
	if opts.TopicAlias != 0 && opts.TopicAlias > cs.info.TopicAliasMaximum {
		return fmt.Errorf("%w: alias %d, broker maximum %d", ErrTopicAliasInvalid, opts.TopicAlias, cs.info.TopicAliasMaximum)
	}
	if c.cfg.OutboundTopicAliases && opts.TopicAlias == 0 && opts.Topic != "" && cs.info.TopicAliasMaximum > 0 {
		cs.outAliasMu.Lock()
		defer cs.outAliasMu.Unlock()
		if topic, registered := assignOutboundAlias(cs, &opts); registered {
			admitted, err := c.sendQoS0(ctx, cs, &opts)
			if !admitted {
				delete(cs.outAliasMap, topic)
				cs.outAliasNext--
			}
			return err
		}
	}
	_, err := c.sendQoS0(ctx, cs, &opts)
	return err
}

// sendQoS0 writes or queues a QoS 0 PUBLISH. admitted reports whether
// the packet entered the connection's write order: it was written, is
// being finished by the writer, or is queued for it.
func (c *Client) sendQoS0(ctx context.Context, cs *connState, opts *wire.PublishOpts) (admitted bool, err error) {
	waitForFlush := c.cfg.PublishMode == PublishWaitForFlush
	if waitForFlush && cs.acquireIdle() {
		admitted, err = cs.writePublish(ctx, opts)
		cs.wmu.Unlock()
		return admitted, err
	}

	bp, err := wire.EncodePublish(*opts)
	if err != nil {
		return false, err
	}
	if err = checkPacketSize(cs, len(*bp)); err != nil {
		wire.ReleaseBuf(bp)
		return false, err
	}

	req := writeReq{pkt: bp}
	var done chan error
	if waitForFlush {
		done = make(chan error, 1)
		req.done = done
	}
	// WriteDropNewest fails at once when the queue is full; WriteBlock
	// waits for room, ctx or teardown.
	if err = cs.queue(ctx, req, c.cfg.WriteOverflowPolicy != WriteDropNewest); err != nil {
		wire.ReleaseBuf(bp)
		return false, err
	}
	// The writer owns bp now; it releases it after the write.
	if !waitForFlush {
		return true, nil
	}
	return true, awaitWrite(ctx, cs, done)
}

// assignOutboundAlias replaces opts.Topic with an alias registered on cs,
// or registers a new one while the broker's budget lasts, reporting the
// topic it registered. The caller holds cs.outAliasMu until the packet
// is queued, and undoes a registration whose packet was not.
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
// delivered.
func (c *Client) publishQoSReliable(ctx context.Context, opts wire.PublishOpts) error {
	flow, err := c.startReliable(ctx, opts, nil, nil)
	if err != nil {
		return err
	}
	life := c.life.Load()
	select {
	case <-flow.Done():
		return flow.Err()
	case <-ctx.Done():
		c.engine.Withdraw(flow)
		return ctx.Err()
	case <-life.shutdown:
		return life.closedErr()
	}
}

// startReliable allocates a packet identifier, encodes the PUBLISH
// once, registers the flow with the session engine (which stores it
// first when a Store is configured) and queues an ordered marker so the
// writer sends it behind earlier writes. The flow belongs to the session
// from registration on: a dropped connection resends it, and a session
// loss applies the [SessionLossPolicy].
func (c *Client) startReliable(ctx context.Context, opts wire.PublishOpts, ref []byte, settle func(context.Context, error) error) (*inflight.Out, error) {
	// A QoS 1/2 message may be resent on a later connection, where the
	// broker no longer knows any alias, so it always carries its topic.
	if opts.TopicAlias != 0 {
		if opts.Topic == "" {
			return nil, fmt.Errorf("%w: QoS %d needs a topic name, not only an alias", ErrTopicAliasInvalid, opts.QoS)
		}
		opts.TopicAlias = 0
	}
	id, err := c.engine.AllocateID(ctx, inflight.OwnerPublish, c.done())
	if errors.Is(err, inflight.ErrStopped) {
		return nil, ErrClosed
	}
	if err != nil {
		return nil, fmt.Errorf("mqttv5: allocate packet id: %w", err)
	}
	opts.PacketID = id
	opts.Dup = false

	packet, err := wire.MarshalPublish(opts)
	if err != nil {
		c.engine.ReleaseID(id, inflight.OwnerPublish)
		return nil, fmt.Errorf("mqttv5: encode PUBLISH: %w", err)
	}
	if cs := c.cur.Load(); cs != nil {
		if err = checkPacketSize(cs, len(packet)); err != nil {
			c.engine.ReleaseID(id, inflight.OwnerPublish)
			return nil, err
		}
	}
	var expiresAt time.Time
	if opts.MessageExpiryInterval != nil {
		expiresAt = c.cfg.clock.Now().Add(time.Duration(*opts.MessageExpiryInterval) * time.Second)
	}

	flow, link, err := c.engine.Register(ctx, inflight.Message{
		ID: id, QoS: opts.QoS, Packet: packet, ExpiresAt: expiresAt, Ref: ref, Settle: settle,
	})
	if err != nil {
		return nil, fmt.Errorf("mqttv5: store outbound publish: %w", err)
	}
	if link != nil {
		// A failed marker is harmless: the next connection sends every
		// registered flow.
		_ = link.SendOrdered(ctx, flow.Seq())
	}
	return flow, nil
}
