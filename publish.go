// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ashtonian/mqttv5/wire"
)

// Publish sends a PUBLISH packet.
//
//   - QoS 0: returns per [PublishMode] ([PublishFireAndForget] or
//     [PublishWaitForFlush]).
//   - QoS 1: returns after PUBACK.
//   - QoS 2: returns after PUBCOMP.
//
// QoS 1/2 packets are stashed on the session entry so the supervisor
// replays them with DUP=1 across reconnect; callers stay blocked on
// the ack across the drop. With [WithPublisherPool] the call first
// tries the pool per [PoolRoutingPolicy] and falls back to the main
// connection if every pool member is unhealthy.
func (c *Client) Publish(ctx context.Context, opts wire.PublishOpts) error {
	if c.pubPool != nil {
		if err := c.pubPool.publish(ctx, opts); err == nil {
			return nil
		} else if !errors.Is(err, ErrNoHealthyPublishers) {
			// Pool member returned a non-pool-health error (e.g.,
			// invalid QoS, encoding failure). Surface it.
			return err
		}
		// Fall through to main connection.
		c.stats.addPoolFallback()
	}

	if !c.Connected() {
		return ErrNotConnected
	}

	switch opts.QoS {
	case 0:
		err := c.publishQoS0(ctx, opts)
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
	default:
		return fmt.Errorf("mqttv5: invalid QoS %d (must be 0, 1, or 2)", opts.QoS)
	}
}

// publishQoS0 dispatches per PublishMode. Topic alias auto-allocation
// happens here (QoS 0 only — QoS 1/2 must carry the full topic for
// replay across reconnect, since broker alias state resets per
// §3.3.2.3.4).
//
// Both modes pre-encode the packet into a pooled []byte so the writer
// goroutine just calls conn.Write — no closure-capturing-opts
// allocation per call, and the writer can later batch multiple
// pre-encoded packets into one writev.
func (c *Client) publishQoS0(ctx context.Context, opts wire.PublishOpts) error {
	c.applyOutboundAlias(&opts)
	bp, err := wire.EncodePublish(opts)
	if err != nil {
		return err
	}

	if c.cfg.PublishMode == PublishWaitForFlush {
		err := c.enqueueAwaitPkt(ctx, bp)
		// enqueueAwaitPkt owns the buffer once the send succeeds; on
		// every error path (queue full / shutdown / not connected) it
		// has already released.
		return err
	}

	// PublishFireAndForget (default): hand to writer, return.
	cs := c.cur.Load()
	if cs == nil {
		wire.ReleaseBuf(bp)
		return ErrNotConnected
	}

	// WriteDropNewest: don't wait for queue room. The default branch
	// fires only if none of the others are ready immediately, so
	// shutdown/dying still take precedence.
	if c.cfg.WriteOverflowPolicy == WriteDropNewest {
		select {
		case cs.writeQueue <- writeReq{pkt: bp}:
			return nil
		case <-cs.dying:
			wire.ReleaseBuf(bp)
			return ErrNotConnected
		case <-c.shutdown:
			wire.ReleaseBuf(bp)
			return ErrClosed
		default:
			wire.ReleaseBuf(bp)
			return ErrWriteQueueFull
		}
	}

	// WriteBlock: wait for room, ctx, or teardown.
	select {
	case cs.writeQueue <- writeReq{pkt: bp}:
		return nil
	case <-ctx.Done():
		wire.ReleaseBuf(bp)
		return ctx.Err()
	case <-cs.dying:
		wire.ReleaseBuf(bp)
		return ErrNotConnected
	case <-c.shutdown:
		wire.ReleaseBuf(bp)
		return ErrClosed
	}
}

// applyOutboundAlias mutates opts to use a topic alias when one is
// available. No-op if the caller already set TopicAlias, or if the
// broker's TopicAliasMaximum is 0, or if we're out of alias budget.
func (c *Client) applyOutboundAlias(opts *wire.PublishOpts) {
	if opts.TopicAlias != 0 {
		return // caller explicitly set it
	}
	cs := c.cur.Load()
	if cs == nil || cs.outAliasMax == 0 || opts.Topic == "" {
		return
	}
	cs.outAliasMu.Lock()
	defer cs.outAliasMu.Unlock()

	if alias, ok := cs.outAliasMap[opts.Topic]; ok {
		// Re-use: send alias + empty topic (broker substitutes from
		// its cache).
		opts.TopicAlias = alias
		opts.Topic = ""
		return
	}
	if cs.outAliasNext >= cs.outAliasMax {
		return // no budget for a new alias
	}
	cs.outAliasNext++
	cs.outAliasMap[opts.Topic] = cs.outAliasNext
	// Registration: keep Topic, attach Alias. Broker caches it for
	// future use.
	opts.TopicAlias = cs.outAliasNext
}

// publishQoSReliable handles QoS 1 and 2: allocate packet ID,
// serialise once, register on the session, send, wait on the ack
// handshake. A write error doesn't fail the publish — the session
// entry stays and the supervisor replays on reconnect.
func (c *Client) publishQoSReliable(ctx context.Context, opts wire.PublishOpts) error {
	id, err := c.session.AllocateID(ctx)
	if err != nil {
		return fmt.Errorf("mqttv5: allocate packet id: %w", err)
	}
	opts.PacketID = id

	var buf bytes.Buffer
	if _, werr := wire.WritePublish(&buf, opts); werr != nil {
		c.session.ReleaseID(id)
		return fmt.Errorf("mqttv5: encode PUBLISH: %w", werr)
	}
	packet := buf.Bytes()

	entry, err := c.session.Outbound.Register(id, opts.QoS, packet)
	if err != nil {
		c.session.ReleaseID(id)
		return fmt.Errorf("mqttv5: register outbound: %w", err)
	}

	// Send the first attempt. A failure here doesn't fail the publish
	// outright — the supervisor will replay on reconnect. We still
	// surface non-transport errors (e.g., invalid opts) immediately.
	if err := c.enqueueAwait(ctx, func(w io.Writer) (int64, error) {
		n, err := w.Write(packet)
		return int64(n), err
	}); err != nil {
		if errors.Is(err, ErrNotConnected) || errors.Is(err, ErrClosed) {
			// Connection dropped before/during write. Replay will
			// pick it up. Continue to wait below.
			c.cfg.Logger.Debug("mqttv5: publish write deferred to replay",
				"packet_id", id, "error", err)
		} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			// Transport-level failures look like ErrNotConnected /
			// ErrClosed; anything else is more serious.
			c.cfg.Logger.Warn("mqttv5: publish write error",
				"packet_id", id, "error", err)
		}
	}

	select {
	case <-entry.Done:
		if entry.Err != nil {
			return entry.Err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.shutdown:
		return ErrClosed
	}
}
