// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/internal/trie"
	"github.com/ashtonian/mqttv5/wire"
)

// readLoop drives the decoder for one connection.
func (c *Client) readLoop(cs *connState) {
	defer cs.wg.Done()
	for {
		pkt, err := cs.decoder.ReadPacket()
		if perr := decodeViolation(err); perr != nil {
			c.protocolViolation(cs, perr)
			return
		}
		if err != nil {
			c.handleConnError(cs, err)
			return
		}
		cs.lastReadUnixNano.Store(cs.clk.Now().UnixNano())
		cs.reads.Add(1)
		c.dispatch(cs, pkt)
	}
}

// dispatch routes one decoded packet for the given connection.
func (c *Client) dispatch(cs *connState, pkt wire.Packet) {
	switch p := pkt.(type) {
	case *wire.Publish:
		c.handlePublish(cs, p)
	case *wire.PubResp:
		c.handlePubResp(p)
	case *wire.Suback, *wire.Unsuback:
		c.deliverControlAck(cs, pkt)
	case *wire.Pingresp:
		pkt.Release()
	case *wire.Auth:
		c.handleServerAuth(cs, p)
	case *wire.Disconnect:
		// Always retain the reason: the supervisor uses it for the
		// ServerDisconnects stat and OnServerDisconnect, and
		// Reauthenticate uses it to surface ErrReauthRejected with the
		// broker's code.
		info := disconnectInfoOf(p)
		cs.serverDisconnect.Store(&info)
		c.cfg.Logger.Warn("mqttv5: broker DISCONNECT", slog.Int("reason", int(p.ReasonCode)))
		pkt.Release()
		cs.signalDown()
	default:
		// CONNECT, SUBSCRIBE, UNSUBSCRIBE and PINGREQ only travel to the
		// server; CONNACK only during the handshake.
		t := pkt.Type()
		pkt.Release()
		c.protocolViolation(cs, &ProtocolError{Reason: wire.ReasonProtocolError, Packet: t,
			Detail: "a server must not send " + t.String() + " here"})
	}
}

// decodeViolation turns a decoder error that means the broker broke the
// protocol into the ProtocolError to disconnect with; other errors (I/O)
// give nil.
func decodeViolation(err error) *ProtocolError {
	if err == nil {
		return nil
	}
	var pe *wire.PacketError
	if errors.As(err, &pe) {
		return &ProtocolError{Reason: pe.Reason, Packet: pe.Packet, Detail: pe.Detail}
	}
	if errors.Is(err, wire.ErrPacketTooLarge) {
		return &ProtocolError{Reason: wire.ReasonPacketTooLarge, Detail: err.Error()}
	}
	return nil
}

// handlePublish dispatches an inbound PUBLISH to every matching
// subscription. Each subscription gets its own *Message handle onto one
// shared delivery; the broker acknowledgement goes out when the last
// handle is acked. Unless every matched subscription is zero-copy, the
// frame is copied into an owned buffer and released before any handler
// runs. If nothing matches, the PUBLISH is acked here so the broker
// doesn't retry it forever.
func (c *Client) handlePublish(cs *connState, pub *wire.Publish) {
	c.stats.addInboundPublish()
	// Resolve topic aliases per §3.3.2.3.4 before anything else —
	// the trie matcher needs the real topic, and any subscriber
	// reading msg.Topic expects the substituted value.
	if perr := c.resolveTopicAlias(cs, pub); perr != nil {
		pub.Release()
		c.protocolViolation(cs, perr)
		return
	}

	var in *inflight.In
	if pub.QoS > 0 {
		var deliver bool
		var err error
		in, deliver, err = c.engine.Receive(pub.PacketID, pub.QoS)
		if err != nil {
			pub.Release()
			c.protocolViolation(cs, &ProtocolError{
				Reason: wire.ReasonReceiveMaximumExceeded, Packet: wire.PUBLISH, Detail: err.Error(),
			})
			return
		}
		if !deliver {
			// A retransmission of a message still in flight: the
			// application already has it.
			pub.Release()
			return
		}
	}

	// The common single-match case needs no slice.
	var (
		first    *route
		rest     []*route
		zeroCopy = true
	)
	add := func(r *route) {
		if r == first || slices.Contains(rest, r) {
			return
		}
		zeroCopy = zeroCopy && r.zeroCopy
		if first == nil {
			first = r
		} else {
			rest = append(rest, r)
		}
	}
	// A PUBLISH carrying Subscription Identifiers was sent for the
	// subscriptions they name: with overlapping filters the broker may
	// send one copy per subscription, each tagged with its own, and
	// a copy reaches only the subscriptions it was sent for. Without
	// identifiers every matching subscription gets it.
	var tagged [4]uint32
	ids := tagged[:0]
	for id := range pub.Properties.SubscriptionIdentifiers() {
		ids = append(ids, id)
	}
	c.router.Match(pub.Topic, func(h trie.Handler) {
		v := h.(*brokerSub).view.Load()
		if len(ids) > 0 && !v.accepts(ids) {
			return
		}
		for _, r := range v.routes {
			add(r)
		}
	})

	if first == nil {
		if in != nil {
			c.engine.Ack(in)
		}
		pub.Release()
		return
	}

	d := &delivery{client: c, in: in}
	d.refs.Store(int32(1 + len(rest)))
	m := &d.first
	m.d = d
	switch {
	case zeroCopy:
		fillZeroCopy(m, pub)
		d.frame = pub
	case !pub.Pooled():
		// A frame that is never recycled is already the message's own.
		fillZeroCopy(m, pub)
		pub.Release()
	default:
		fillOwned(m, pub)
		pub.Release()
	}

	// Every handle is built before the first dispatch: a synchronous
	// handler may modify its Message or ack it while we are still here.
	var handles []*Message
	if len(rest) > 0 {
		handles = make([]*Message, len(rest))
		for i := range rest {
			handles[i] = handleFor(m)
		}
	}
	first.dispatch(m)
	for i, r := range rest {
		r.dispatch(handles[i])
	}
}

// resolveTopicAlias applies the Topic Alias property (§3.3.2.3.4): a
// PUBLISH with a topic registers or replaces the alias, one with an
// empty topic uses it. An alias of 0 or above the maximum the client
// advertised is a Topic Alias invalid (0x94) violation; using an alias
// that was never registered is a Protocol Error (0x82).
func (c *Client) resolveTopicAlias(cs *connState, pub *wire.Publish) *ProtocolError {
	alias, ok := pub.Properties.Uint16(wire.PropTopicAlias)
	if !ok {
		return nil
	}
	if alias == 0 || alias > c.cfg.InboundTopicAliasMaximum {
		return &ProtocolError{Reason: wire.ReasonTopicAliasInvalid, Packet: wire.PUBLISH,
			Detail: fmt.Sprintf("topic alias %d outside 1..%d", alias, c.cfg.InboundTopicAliasMaximum)}
	}
	if pub.Topic != "" {
		// Clone so the cached string outlives the frame.
		topic := strings.Clone(pub.Topic)
		cs.aliasMu.Lock()
		cs.aliasMap[alias] = topic
		cs.aliasMu.Unlock()
		return nil
	}
	cs.aliasMu.RLock()
	topic, ok := cs.aliasMap[alias]
	cs.aliasMu.RUnlock()
	if !ok {
		return &ProtocolError{Reason: wire.ReasonProtocolError, Packet: wire.PUBLISH,
			Detail: fmt.Sprintf("topic alias %d used before it was registered", alias)}
	}
	pub.Topic = topic
	return nil
}

// protocolViolation handles a broker packet that breaks MQTT v5:
// DISCONNECT with the error's reason code, then close. The supervisor
// reconnects.
func (c *Client) protocolViolation(cs *connState, perr *ProtocolError) {
	c.stats.addProtocolError()
	c.cfg.Logger.Error("mqttv5: broker protocol violation; disconnecting",
		slog.String("packet", perr.Packet.String()),
		slog.Int("reason", int(perr.Reason)),
		slog.String("detail", perr.Detail))
	c.writeDisconnectAndClose(cs, wire.DisconnectOpts{ReasonCode: perr.Reason})
}

// handlePubResp routes PUBACK/PUBREC/PUBREL/PUBCOMP to the session
// engine.
func (c *Client) handlePubResp(p *wire.PubResp) {
	defer p.Release()
	var refused error
	if p.ReasonCode.IsError() {
		refused = newReasonCodeError(p.Type(), p.ReasonCode, p.Properties)
	}
	switch p.Type() {
	case wire.PUBACK:
		c.engine.HandlePuback(p.PacketID, refused)
		c.stats.addPublishAcked()
	case wire.PUBREC:
		c.engine.HandlePubrec(p.PacketID, refused)
	case wire.PUBREL:
		c.engine.HandlePubrel(p.PacketID)
	case wire.PUBCOMP:
		c.engine.HandlePubcomp(p.PacketID)
		c.stats.addPublishAcked()
	}
}
