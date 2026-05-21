// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync/atomic"

	"github.com/ashtonian/mqttv5/wire"
)

// ErrNoHealthyPublishers is returned by publisherPool.publish when
// every pool member is disconnected. The Client falls back to the
// main connection in this case (and increments PoolFallbacks).
var ErrNoHealthyPublishers = errors.New("mqttv5: no healthy publishers in pool")

// publisherPool is a set of N publish-only Clients. Each member has
// its own session and supervisor, so QoS 1/2 acks land on the member
// that emitted the publish.
//
// Spec-compliant but easy to miss:
//
//   - Ordering. MQTT v5 §4.6 in-order delivery is per-ClientID.
//     Round-robin across members can reorder publishes to the same
//     topic. For ordered topics, use
//     WithPublisherPoolRouting(PoolRoutingHashByTopic).
//   - QoS 1/2 across reconnect. Pool members force CleanStart=true
//     and SessionExpiry=0, so a mid-flight drop becomes at-least-once
//     with possible duplicates (the broker has no resumable session
//     when the supervisor replays). Use the main connection for
//     ledger-style work.
//
// What is propagated to pool members from the parent Config:
//
//   - Wire layer: BrokerURLs, Username/Password, TLSConfig, Dialer,
//     DialFunc (required for WebSocket / proxy transports),
//     Authenticator.
//   - CONNECT v5 properties: ReceiveMaximum, MaximumPacketSize,
//     InboundTopicAliasMaximum, RequestResponseInformation,
//     RequestProblemInformation, ConnectUserProperties.
//   - Timing & sizing: KeepAlive (or WithoutKeepAlive),
//     ConnectTimeout, PingTimeout, DisconnectFlushTimeout,
//     WriteQueueSize, ReconnectBackoff, PublishMode.
//   - ConnectPacketBuilder (so per-attempt credential rotation
//     covers pool members too).
//   - StatsEnabled (pool-member Stats() reports independently).
//
// What is NOT propagated (parent is the canonical lifecycle holder):
//
//   - Will. WithWill on the parent is only emitted from the parent's
//     session.
//   - Lifecycle callbacks: OnConnectionUp, OnConnectionDown,
//     OnConnectError, OnReconnectAttempt, OnServerDisconnect.
//     Members manage their own reconnect silently; observe the
//     parent for application-visible lifecycle events.
//   - CleanStartOnReconnect — pool members force CleanStart=true on
//     every CONNECT.
//   - Store. Each pool member uses an in-memory session by default;
//     CleanStart=true makes durable session storage moot.
type publisherPool struct {
	members []*Client
	cursor  atomic.Uint64
	routing PoolRoutingPolicy
	logger  *slog.Logger
}

// newPublisherPool constructs the pool but does not connect it. Pool
// members inherit the parent's broker URL + credentials + TLS config
// + keepalive. Their ClientID is suffixed (-pub-1, -pub-2, ...) for
// per-member uniqueness.
func newPublisherPool(parent *Config, size int, logger *slog.Logger) (*publisherPool, error) {
	clientIDFn := parent.PublisherPoolClientIDFn
	if clientIDFn == nil {
		clientIDFn = defaultPoolClientIDFn
	}
	members := make([]*Client, 0, size)
	for i := 1; i <= size; i++ {
		opts := []Option{
			WithBrokers(parent.BrokerURLs...),
			WithClientID(clientIDFn(parent.ClientID, i)),
			// Pool members force CleanStart=true and SessionExpiry=0 —
			// see the publisherPool type doc. CleanStartOnReconnect is
			// not propagated for the same reason.
			WithCleanStart(true),
			WithSessionExpiry(0),
			WithConnectTimeout(parent.ConnectTimeout),
			WithPingTimeout(parent.PingTimeout),
			WithDisconnectFlushTimeout(parent.DisconnectFlushTimeout),
			WithReconnectBackoff(parent.ReconnectBackoff),
			WithLogger(logger.With("component", "publisher-pool", "member", i)),
			WithPublishMode(parent.PublishMode),
			WithWriteQueueSize(parent.WriteQueueSize),
			WithMaximumPacketSize(parent.MaximumPacketSize),
			WithInboundTopicAliasMaximum(parent.InboundTopicAliasMaximum),
			WithRequestResponseInformation(parent.RequestResponseInformation),
			WithRequestProblemInformation(parent.RequestProblemInformation),
		}
		if parent.keepAliveDisabled {
			opts = append(opts, WithoutKeepAlive())
		} else {
			opts = append(opts, WithKeepAlive(parent.KeepAlive))
		}
		if parent.Username != "" {
			opts = append(opts, WithCredentials(parent.Username, parent.Password))
		}
		if parent.TLSConfig != nil {
			opts = append(opts, WithTLSConfig(parent.TLSConfig))
		}
		if parent.Dialer != nil {
			opts = append(opts, WithDialer(parent.Dialer))
		}
		if parent.DialFunc != nil {
			// Required for WebSocket / proxy / custom transport — the
			// pool members must reach the broker via the same dial
			// path as the parent.
			opts = append(opts, WithDialFunc(parent.DialFunc))
		}
		if parent.Authenticator != nil {
			opts = append(opts, WithAuthenticator(parent.Authenticator))
		}
		if parent.ReceiveMaximum != 0 {
			opts = append(opts, WithReceiveMaximum(parent.ReceiveMaximum))
		}
		if len(parent.ConnectUserProperties) > 0 {
			opts = append(opts, WithConnectUserProperties(parent.ConnectUserProperties))
		}
		if parent.ConnectPacketBuilder != nil {
			opts = append(opts, WithConnectPacketBuilder(parent.ConnectPacketBuilder))
		}
		if parent.StatsEnabled {
			opts = append(opts, WithStats())
		}
		m, err := New(opts...)
		if err != nil {
			return nil, fmt.Errorf("mqttv5: pool member %d: %w", i, err)
		}
		members = append(members, m)
	}
	return &publisherPool{
		members: members,
		routing: parent.PublisherPoolRouting,
		logger:  logger,
	}, nil
}

// connect dials every pool member. Failed members retry on their own
// supervisor; the joined error is diagnostic only — the pool is still
// usable.
func (p *publisherPool) connect(ctx context.Context) error {
	var errs []error
	for i, m := range p.members {
		if err := m.Connect(ctx); err != nil {
			errs = append(errs, fmt.Errorf("pool member %d: %w", i+1, err))
		}
	}
	return errors.Join(errs...)
}

// publish picks a starting member per the routing policy and probes
// linearly from there, trying each member at most once. Returns
// ErrNoHealthyPublishers if no member accepts.
func (p *publisherPool) publish(ctx context.Context, opts wire.PublishOpts) error {
	n := len(p.members)
	if n == 0 {
		return ErrNoHealthyPublishers
	}
	start := p.startIndex(opts)
	var lastErr error
	for i := range n {
		m := p.members[(start+uint64(i))%uint64(n)]
		if !m.Connected() {
			continue
		}
		if err := m.Publish(ctx, opts); err == nil {
			return nil
		} else {
			lastErr = err
			p.logger.Warn("mqttv5: pool member publish failed",
				slog.String("client_id", m.ClientID()),
				slog.Any("error", err),
			)
		}
	}
	if lastErr != nil {
		return fmt.Errorf("%w: %w", ErrNoHealthyPublishers, lastErr)
	}
	return ErrNoHealthyPublishers
}

// startIndex returns the member-index to start the probe loop at,
// chosen per the configured routing policy.
func (p *publisherPool) startIndex(opts wire.PublishOpts) uint64 {
	switch p.routing {
	case PoolRoutingHashByTopic:
		h := fnv.New32a()
		_, _ = h.Write([]byte(opts.Topic))
		return uint64(h.Sum32())
	default:
		return p.cursor.Add(1)
	}
}

// disconnect tears down every pool member, joining any errors.
func (p *publisherPool) disconnect(ctx context.Context) error {
	var errs []error
	for i, m := range p.members {
		if err := m.Disconnect(ctx); err != nil {
			errs = append(errs, fmt.Errorf("pool member %d: %w", i+1, err))
		}
	}
	return errors.Join(errs...)
}
