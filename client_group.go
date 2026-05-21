// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/ashtonian/mqttv5/wire"
)

// GroupMember describes one member of a [ClientGroup]. Each member
// becomes a full [Client] with its own session, supervisor, and
// CONNECT identity.
//
// Use Opts for per-member settings — [WithCredentials], [WithTLSConfig],
// [WithAuthenticator], [WithClientID], [WithConnectPacketBuilder], a
// per-member [WithOnConnectionUp] closing over Name for identity, etc.
// Opts are applied AFTER [WithGroupSharedOpts] so per-member settings
// win.
type GroupMember struct {
	// Broker is the URL this member dials. Required.
	Broker string

	// Name is an optional identifier surfaced in logs, returned in
	// the Subscribe token map, and accepted by ClientGroup.Member.
	// Defaults to "member-N" (1-based) when empty.
	Name string

	// Opts are applied AFTER any WithGroupSharedOpts shared options.
	// Use to override or extend the shared opts per-broker (auth,
	// TLS, ClientID, per-member callbacks, etc.).
	Opts []Option
}

// GroupPublishPolicy selects how [ClientGroup.Publish] dispatches
// across members. For HA failover across interchangeable brokers
// use [WithBrokers] on a single [Client] instead — [ClientGroup]
// keeps N parallel sessions where every member is treated as itself.
type GroupPublishPolicy uint8

const (
	// GroupPublishBroadcast (default) hits every member with the
	// same publish. Use when members carry different data (bridge /
	// mirror semantic). Returns nil if any member's Publish
	// succeeded; otherwise the joined error from every member.
	GroupPublishBroadcast GroupPublishPolicy = iota

	// GroupPublishRoundRobin distributes publishes across the group
	// for throughput against an interchangeable broker fleet. The
	// per-call starting cursor advances by one; unhealthy starting
	// members fall through to the next member regardless of policy.
	// Use when members are equivalent for publish purposes (e.g. a
	// clustered broker fleet) and you want N parallel sessions.
	GroupPublishRoundRobin

	// GroupPublishHashByTopic picks the starting member by FNV-1a
	// of opts.Topic, preserving per-topic ordering across the group
	// while every member is healthy. Recovery (hashed member down)
	// briefly reorders that topic as the probe walks the ring.
	// Empty Topic collapses to member 0.
	GroupPublishHashByTopic
)

func (p GroupPublishPolicy) valid() bool {
	switch p {
	case GroupPublishBroadcast, GroupPublishRoundRobin, GroupPublishHashByTopic:
		return true
	}
	return false
}

// String returns a stable debug name for the policy.
func (p GroupPublishPolicy) String() string {
	switch p {
	case GroupPublishRoundRobin:
		return "round_robin"
	case GroupPublishHashByTopic:
		return "hash_by_topic"
	default:
		return "broadcast"
	}
}

// ClientGroupOption configures a ClientGroup at construct time.
type ClientGroupOption func(*clientGroupConfig)

type clientGroupConfig struct {
	publishPolicy GroupPublishPolicy
	sharedOpts    []Option
	parallel      bool
}

// WithGroupPublishPolicy selects the [ClientGroup.Publish] dispatch
// policy. See [GroupPublishPolicy].
func WithGroupPublishPolicy(p GroupPublishPolicy) ClientGroupOption {
	return func(c *clientGroupConfig) { c.publishPolicy = p }
}

// WithGroupSharedOpts applies opts to every member before that
// member's own [GroupMember.Opts]. Use for shared settings
// (KeepAlive, Logger, ReconnectBackoff). Per-member Opts override.
func WithGroupSharedOpts(opts ...Option) ClientGroupOption {
	return func(c *clientGroupConfig) { c.sharedOpts = opts }
}

// WithGroupSequentialLifecycle disables the default parallel
// Connect / Disconnect / Subscribe across members. Useful in tests
// where deterministic ordering matters more than wall time.
func WithGroupSequentialLifecycle() ClientGroupOption {
	return func(c *clientGroupConfig) { c.parallel = false }
}

// ClientGroup manages N parallel sessions to N independent brokers.
// Each member is a full [Client] with its own session and
// supervisor; the group adds dispatch policy on top. Use for
// bridges across independent brokers, multi-tenant SaaS with
// per-broker credentials, or a clustered broker fleet you want N
// parallel sessions into. For HA failover use [WithBrokers] on a
// single [Client] instead — [ClientGroup] does not failover between
// members.
type ClientGroup struct {
	cfg     clientGroupConfig
	members []*Client
	names   []string // index-aligned with members
	byName  map[string]*Client

	// rrCursor drives GroupPublishRoundRobin starting-index selection.
	rrCursor atomic.Uint64

	// groupCtx governs the bridge goroutines that merge per-member
	// subscriptions into the merged channel / queue. Cancelled on
	// Disconnect.
	groupCtx    context.Context
	groupCancel context.CancelFunc

	bridgesWg sync.WaitGroup
	closed    sync.Once
	done      chan struct{}
}

// NewClientGroup constructs a ClientGroup over the given members.
// Each GroupMember becomes a full *Client; the group applies the
// configured publish policy on top.
//
// Member ClientIDs are deduped: if two members resolve to the same
// ClientID, the second gets a "-N" suffix appended. Set
// GroupMember.Opts with WithClientID for explicit per-member IDs.
//
// Defaults: GroupPublishBroadcast, parallel Connect/Disconnect.
func NewClientGroup(members []GroupMember, opts ...ClientGroupOption) (*ClientGroup, error) {
	if len(members) == 0 {
		return nil, errors.New("mqttv5: ClientGroup requires at least one member")
	}

	cfg := clientGroupConfig{
		publishPolicy: GroupPublishBroadcast,
		parallel:      true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if !cfg.publishPolicy.valid() {
		return nil, fmt.Errorf("mqttv5: invalid GroupPublishPolicy %d", cfg.publishPolicy)
	}

	clients := make([]*Client, 0, len(members))
	names := make([]string, 0, len(members))
	byName := make(map[string]*Client, len(members))
	seenClientID := make(map[string]int, len(members))
	seenName := make(map[string]bool, len(members))

	for i, m := range members {
		if m.Broker == "" {
			return nil, fmt.Errorf("mqttv5: ClientGroup members[%d]: Broker is required", i)
		}
		name := m.Name
		if name == "" {
			name = fmt.Sprintf("member-%d", i+1)
		}
		if seenName[name] {
			return nil, fmt.Errorf("mqttv5: ClientGroup members[%d]: duplicate name %q", i, name)
		}
		seenName[name] = true

		// Probe the resolved ClientID by applying shared+per-member
		// opts to a throwaway Config — cheap, options are pure
		// setters. If two members resolve to the same ID, dedupe
		// with a "-N" suffix.
		probe := &Config{}
		combined := append([]Option(nil), cfg.sharedOpts...)
		combined = append(combined, m.Opts...)
		for _, opt := range combined {
			if err := opt(probe); err != nil {
				return nil, fmt.Errorf("mqttv5: ClientGroup members[%d] (%q): %w", i, name, err)
			}
		}
		intendedID := probe.ClientID
		memberID := intendedID
		if n, dup := seenClientID[intendedID]; dup {
			memberID = fmt.Sprintf("%s-%d", intendedID, n+1)
		}
		seenClientID[intendedID]++

		memberOpts := make([]Option, 0, len(combined)+2)
		memberOpts = append(memberOpts, WithBroker(m.Broker))
		memberOpts = append(memberOpts, combined...)
		memberOpts = append(memberOpts, WithClientID(memberID))

		c, err := New(memberOpts...)
		if err != nil {
			return nil, fmt.Errorf("mqttv5: ClientGroup members[%d] (%q): %w", i, name, err)
		}
		clients = append(clients, c)
		names = append(names, name)
		byName[name] = c
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &ClientGroup{
		cfg:         cfg,
		members:     clients,
		names:       names,
		byName:      byName,
		groupCtx:    ctx,
		groupCancel: cancel,
		done:        make(chan struct{}),
	}, nil
}

// Members returns the underlying Clients in member order. Use for
// per-member operations the group API doesn't expose directly
// (Stats per member, manual Publish to a specific member, etc.).
func (g *ClientGroup) Members() []*Client {
	out := make([]*Client, len(g.members))
	copy(out, g.members)
	return out
}

// Member returns the Client for the named member, or nil when no
// such member exists.
func (g *ClientGroup) Member(name string) *Client { return g.byName[name] }

// Names returns the member names in order.
func (g *ClientGroup) Names() []string {
	out := make([]string, len(g.names))
	copy(out, g.names)
	return out
}

// Connect dials every member. The default is parallel; use
// WithGroupSequentialLifecycle to serialise. The returned error is
// errors.Join of every member that failed — nil on full success.
// If at least one member succeeded the group is still usable for
// the healthy subset.
func (g *ClientGroup) Connect(ctx context.Context) error {
	return g.lifecycleOp(ctx, "Connect", func(_ int, m *Client) error {
		return m.Connect(ctx)
	})
}

// Disconnect tears down every member. Bridge goroutines for
// outstanding Subscribe / SubscribeQueue calls drain first
// (bounded by ctx). The default is parallel; use
// WithGroupSequentialLifecycle to serialise. Returned error is
// errors.Join of every member's Disconnect error.
func (g *ClientGroup) Disconnect(ctx context.Context) error {
	g.closed.Do(func() {
		close(g.done)
		g.groupCancel()
	})

	drained := make(chan struct{})
	go func() {
		g.bridgesWg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-ctx.Done():
	}

	return g.lifecycleOp(ctx, "Disconnect", func(_ int, m *Client) error {
		return m.Disconnect(ctx)
	})
}

// lifecycleOp dispatches op across members per the parallel config.
// Errors join; nil on full success.
func (g *ClientGroup) lifecycleOp(ctx context.Context, label string, op func(idx int, m *Client) error) error {
	n := len(g.members)
	if n == 0 {
		return nil
	}
	if !g.cfg.parallel {
		var errs []error
		for i, m := range g.members {
			if err := op(i, m); err != nil {
				errs = append(errs, fmt.Errorf("%s %s: %w", label, g.names[i], err))
			}
		}
		return errors.Join(errs...)
	}

	type result struct {
		idx int
		err error
	}
	out := make(chan result, n)
	for i, m := range g.members {
		go func(i int, m *Client) {
			out <- result{idx: i, err: op(i, m)}
		}(i, m)
	}

	var errs []error
	for range n {
		select {
		case r := <-out:
			if r.err != nil {
				errs = append(errs, fmt.Errorf("%s %s: %w", label, g.names[r.idx], r.err))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.Join(errs...)
}

// Publish dispatches opts across members per the configured
// GroupPublishPolicy. Returns nil on success (one member acked for
// RoundRobin/HashByTopic; at least one acked for Broadcast); the
// joined per-member errors otherwise.
func (g *ClientGroup) Publish(ctx context.Context, opts wire.PublishOpts) error {
	n := len(g.members)
	if n == 0 {
		return errors.New("mqttv5: ClientGroup has no members")
	}
	switch g.cfg.publishPolicy {
	case GroupPublishBroadcast:
		return g.publishBroadcast(ctx, opts)
	case GroupPublishRoundRobin:
		return g.publishStartingAt(ctx, opts, g.rrCursor.Add(1))
	case GroupPublishHashByTopic:
		var start uint64
		if opts.Topic != "" {
			h := fnv.New32a()
			_, _ = h.Write([]byte(opts.Topic))
			start = uint64(h.Sum32())
		}
		return g.publishStartingAt(ctx, opts, start)
	default:
		return fmt.Errorf("mqttv5: invalid GroupPublishPolicy %d", g.cfg.publishPolicy)
	}
}

func (g *ClientGroup) publishBroadcast(ctx context.Context, opts wire.PublishOpts) error {
	var errs []error
	published := 0
	for i, m := range g.members {
		if err := m.Publish(ctx, opts); err != nil {
			errs = append(errs, fmt.Errorf("member %s: %w", g.names[i], err))
			continue
		}
		published++
	}
	if published == 0 {
		return fmt.Errorf("mqttv5: ClientGroup publish failed on every member: %w",
			errors.Join(errs...))
	}
	return nil
}

// publishStartingAt walks the member ring beginning at start and
// returns on the first member that accepts the publish. Failing
// members are skipped; unhealthy starting members fall through to
// the next. Returns the joined error only when every member fails.
func (g *ClientGroup) publishStartingAt(ctx context.Context, opts wire.PublishOpts, start uint64) error {
	n := uint64(len(g.members))
	var errs []error
	for i := range n {
		idx := (start + i) % n
		m := g.members[idx]
		if err := m.Publish(ctx, opts); err == nil {
			return nil
		} else {
			errs = append(errs, fmt.Errorf("member %s: %w", g.names[idx], err))
		}
	}
	return fmt.Errorf("mqttv5: ClientGroup publish failed on every member: %w", errors.Join(errs...))
}

// Subscribe subscribes on every member and merges all inbound
// messages into one channel. The caller MUST call msg.Ack() on each
// received message; Ack dispatches back to the member that
// delivered it.
//
// The returned token map is keyed by member name — pass an entry to
// Unsubscribe to tear down that member's subscription, or use
// UnsubscribeAll to tear down everything.
//
// Behaviour mirrors Client.Subscribe — Subscribe is "subscribe on
// all" only; for failover across interchangeable brokers use
// WithBrokers on a single Client.
func (g *ClientGroup) Subscribe(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *Message, map[string]SubscriptionToken, error) {
	mergedSize := DefaultSubscribeBuffer
	for _, opt := range opts {
		var tmp subscribeConfig
		opt(&tmp)
		if tmp.bufferSize > 0 {
			mergedSize = tmp.bufferSize
		}
	}
	merged := make(chan *Message, mergedSize)

	tokens, err := g.subscribeAll(ctx, filters, opts, func(m *Client) (any, error) {
		ch, tok, err := m.Subscribe(ctx, filters, opts...)
		if err != nil {
			return nil, err
		}
		g.bridgesWg.Add(1)
		go g.bridgeChan(ch, merged)
		return tok, nil
	})
	if err != nil {
		close(merged)
		return nil, nil, err
	}
	return merged, tokens, nil
}

// SubscribeQueue is the queue-merged variant of Subscribe.
func (g *ClientGroup) SubscribeQueue(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*Message], map[string]SubscriptionToken, error) {
	merged := NewQueue[*Message]()

	tokens, err := g.subscribeAll(ctx, filters, opts, func(m *Client) (any, error) {
		q, tok, err := m.SubscribeQueue(ctx, filters, opts...)
		if err != nil {
			return nil, err
		}
		g.bridgesWg.Add(1)
		go g.bridgeQueue(q, merged)
		return tok, nil
	})
	if err != nil {
		merged.Close()
		return nil, nil, err
	}
	return merged, tokens, nil
}

// subscribeAll runs the per-member subscribe in parallel (or
// sequentially per config), collects tokens into a name-keyed map,
// and returns the joined error when nobody subscribed.
func (g *ClientGroup) subscribeAll(
	ctx context.Context,
	filters []TopicFilter, //nolint:unparam // for symmetry with sibling helpers
	opts []SubscribeOption, //nolint:unparam
	sub func(*Client) (any, error),
) (map[string]SubscriptionToken, error) {
	_ = filters
	_ = opts

	n := len(g.members)
	type result struct {
		idx int
		tok any
		err error
	}
	out := make(chan result, n)

	dispatch := func(i int, m *Client) {
		tok, err := sub(m)
		out <- result{idx: i, tok: tok, err: err}
	}
	if g.cfg.parallel {
		for i, m := range g.members {
			go dispatch(i, m)
		}
	} else {
		for i, m := range g.members {
			dispatch(i, m)
		}
	}

	tokens := make(map[string]SubscriptionToken, n)
	var errs []error
	for range n {
		select {
		case r := <-out:
			if r.err != nil {
				errs = append(errs, fmt.Errorf("member %s: %w", g.names[r.idx], r.err))
				continue
			}
			tokens[g.names[r.idx]] = r.tok.(SubscriptionToken)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("mqttv5: ClientGroup subscribe failed on every member: %w",
			errors.Join(errs...))
	}
	return tokens, nil
}

// Unsubscribe tears down the named member's subscription.
func (g *ClientGroup) Unsubscribe(ctx context.Context, name string, token SubscriptionToken) error {
	m := g.byName[name]
	if m == nil {
		return fmt.Errorf("mqttv5: ClientGroup has no member %q", name)
	}
	return m.Unsubscribe(ctx, token)
}

// UnsubscribeAll tears down every token in the supplied map. Errors
// are joined.
func (g *ClientGroup) UnsubscribeAll(ctx context.Context, tokens map[string]SubscriptionToken) error {
	var errs []error
	for name, tok := range tokens {
		m := g.byName[name]
		if m == nil {
			errs = append(errs, fmt.Errorf("member %q not found", name))
			continue
		}
		if err := m.Unsubscribe(ctx, tok); err != nil {
			errs = append(errs, fmt.Errorf("member %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// bridgeChan drains src into dst until src closes or the group is
// torn down.
func (g *ClientGroup) bridgeChan(src <-chan *Message, dst chan<- *Message) {
	defer g.bridgesWg.Done()
	for {
		select {
		case msg, ok := <-src:
			if !ok {
				return
			}
			select {
			case dst <- msg:
			case <-g.done:
				_ = msg.Ack()
				return
			}
		case <-g.done:
			return
		}
	}
}

// bridgeQueue drains src into dst until src closes or the group is
// torn down.
func (g *ClientGroup) bridgeQueue(src, dst *Queue[*Message]) {
	defer g.bridgesWg.Done()
	for {
		msg, ok := src.Dequeue(g.groupCtx)
		if !ok {
			return
		}
		if !dst.Enqueue(msg) {
			_ = msg.Ack()
			return
		}
	}
}
