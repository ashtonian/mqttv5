// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"sync/atomic"
)

// clientStats holds the atomic counters backing Client.Stats. Kept
// unexported so callers cannot reach into the live counters — they
// get a value snapshot via Client.Stats. Every helper method is
// safe to call on a nil receiver: when stats are disabled the
// receiver is nil and the methods compile down to a predicted
// branch with no atomic op.
type clientStats struct {
	connects          atomic.Uint64
	connectFailures   atomic.Uint64
	disconnects       atomic.Uint64
	serverDisconnects atomic.Uint64
	pingTimeouts      atomic.Uint64
	publishesSent     atomic.Uint64
	publishesAcked    atomic.Uint64
	publishesReplayed atomic.Uint64
	inboundPublishes  atomic.Uint64
	inboundDropped    atomic.Uint64
	subscribesSent    atomic.Uint64
	unsubscribesSent  atomic.Uint64
	poolFallbacks     atomic.Uint64
	acksIgnored       atomic.Uint64
	storeErrors       atomic.Uint64
	protocolErrors    atomic.Uint64
}

// Per-counter increment helpers. Each is safe to call on a nil
// receiver (when WithStats was not set) — the nil check compiles to
// a single predicted branch with no atomic op, keeping the hot path
// free of cost when stats are off. The repetition is intentional:
// Go has no method generics and the inline check is cheaper than a
// generic indirection.
func (s *clientStats) addConnect() {
	if s != nil {
		s.connects.Add(1)
	}
}
func (s *clientStats) addConnectFailure() {
	if s != nil {
		s.connectFailures.Add(1)
	}
}
func (s *clientStats) addDisconnect() {
	if s != nil {
		s.disconnects.Add(1)
	}
}
func (s *clientStats) addServerDisconnect() {
	if s != nil {
		s.serverDisconnects.Add(1)
	}
}
func (s *clientStats) addPingTimeout() {
	if s != nil {
		s.pingTimeouts.Add(1)
	}
}
func (s *clientStats) addPublishSent() {
	if s != nil {
		s.publishesSent.Add(1)
	}
}
func (s *clientStats) addPublishAcked() {
	if s != nil {
		s.publishesAcked.Add(1)
	}
}
func (s *clientStats) addPublishReplayed() {
	if s != nil {
		s.publishesReplayed.Add(1)
	}
}
func (s *clientStats) addInboundPublish() {
	if s != nil {
		s.inboundPublishes.Add(1)
	}
}
func (s *clientStats) addInboundDropped() {
	if s != nil {
		s.inboundDropped.Add(1)
	}
}
func (s *clientStats) addSubscribeSent() {
	if s != nil {
		s.subscribesSent.Add(1)
	}
}
func (s *clientStats) addUnsubscribeSent() {
	if s != nil {
		s.unsubscribesSent.Add(1)
	}
}
func (s *clientStats) addPoolFallback() {
	if s != nil {
		s.poolFallbacks.Add(1)
	}
}
func (s *clientStats) addAckIgnored() {
	if s != nil {
		s.acksIgnored.Add(1)
	}
}
func (s *clientStats) addStoreError() {
	if s != nil {
		s.storeErrors.Add(1)
	}
}
func (s *clientStats) addProtocolError() {
	if s != nil {
		s.protocolErrors.Add(1)
	}
}

// Stats is the snapshot of Client counters returned by Client.Stats.
//
// Two flavours of field:
//
//   - Monotonic counters (Connects, ConnectFailures, Disconnects,
//     ServerDisconnects, PingTimeouts, PublishesSent, PublishesAcked,
//     PublishesReplayed, InboundPublishes, InboundDropped,
//     SubscribesSent, UnsubscribesSent, PoolFallbacks, AcksIgnored,
//     StoreErrors, ProtocolErrors). Sampled atomically — no locking on
//     the read side.
//   - Point-in-time gauges (PublishesInflight, SendQuota, SubscriptionsActive)
//     read live state under a mutex (the session state and the
//     subscriptions map respectively). The locks are cheap but a
//     scraper hitting Stats() at very high frequency does contend
//     with Subscribe / Unsubscribe and the ack path. Default to
//     sampling at most a few times per second.
//
// The zero value is returned when WithStats was not set on the
// Config — when stats are disabled the hot path skips every atomic
// increment, so leave WithStats off when you have no scraper.
type Stats struct {
	Connects          uint64 // successful CONNECT/CONNACK handshakes
	ConnectFailures   uint64 // CONNECT attempts that failed (any cause)
	Disconnects       uint64 // connection drops (any cause)
	ServerDisconnects uint64 // broker-sent DISCONNECT packets
	PingTimeouts      uint64 // PINGRESP timeouts that forced a reconnect

	PublishesSent     uint64 // QoS 0/1/2 publishes handed to the writer
	PublishesAcked    uint64 // QoS 1/2 acks received from broker
	PublishesInflight uint64 // QoS 1/2 outbound entries awaiting ack
	SendQuota         uint64 // QoS 1/2 PUBLISHes the broker's Receive Maximum still allows in flight
	PublishesReplayed uint64 // QoS 1/2 replays issued after reconnect

	InboundPublishes uint64 // PUBLISHes received from the broker
	InboundDropped   uint64 // inbound PUBLISHes dropped due to full subscriber buffer

	SubscribesSent      uint64 // SUBSCRIBE packets emitted
	UnsubscribesSent    uint64 // UNSUBSCRIBE packets emitted
	SubscriptionsActive uint64 // count of currently registered subscriptions

	PoolFallbacks uint64 // publishes that fell back from pool to main connection

	AcksIgnored    uint64 // PUBACK/PUBREC/PUBCOMP for an unknown or mismatched packet identifier
	StoreErrors    uint64 // failed session Store operations (see WithStore)
	ProtocolErrors uint64 // broker protocol violations that closed the connection
}

// Stats returns a snapshot of the Client's counters. Cheap to call —
// the returned value is detached. Returns the zero value when stats
// are disabled (WithStats not set on the Config).
func (c *Client) Stats() Stats {
	if c.stats == nil {
		return Stats{}
	}
	c.subsMu.Lock()
	subs := uint64(len(c.activeSubs))
	c.subsMu.Unlock()
	quota, _ := c.engine.SendQuota()

	return Stats{
		Connects:            c.stats.connects.Load(),
		ConnectFailures:     c.stats.connectFailures.Load(),
		Disconnects:         c.stats.disconnects.Load(),
		ServerDisconnects:   c.stats.serverDisconnects.Load(),
		PingTimeouts:        c.stats.pingTimeouts.Load(),
		PublishesSent:       c.stats.publishesSent.Load(),
		PublishesAcked:      c.stats.publishesAcked.Load(),
		PublishesInflight:   uint64(c.engine.OutboundLen()),
		SendQuota:           uint64(quota),
		PublishesReplayed:   c.stats.publishesReplayed.Load(),
		InboundPublishes:    c.stats.inboundPublishes.Load(),
		InboundDropped:      c.stats.inboundDropped.Load(),
		SubscribesSent:      c.stats.subscribesSent.Load(),
		UnsubscribesSent:    c.stats.unsubscribesSent.Load(),
		SubscriptionsActive: subs,
		PoolFallbacks:       c.stats.poolFallbacks.Load(),
		AcksIgnored:         c.stats.acksIgnored.Load(),
		StoreErrors:         c.stats.storeErrors.Load(),
		ProtocolErrors:      c.stats.protocolErrors.Load(),
	}
}
