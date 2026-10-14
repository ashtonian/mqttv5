// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
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
	// Brokers are the URLs this member dials: one, or several the
	// member fails over between as a [Client] with [WithBrokers] does.
	// Required; neither Opts nor [WithGroupSharedOpts] may set brokers.
	Brokers []string

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
	// GroupPublishBroadcast (default) publishes to every member at
	// once. Use when members carry different data (bridge / mirror
	// semantic). [WithGroupSuccess] decides how many must succeed.
	GroupPublishBroadcast GroupPublishPolicy = iota

	// GroupPublishRoundRobin spreads publishes over the group's
	// brokers, one member per call. The per-call starting member
	// advances by one; a member that cannot send (not connected) passes
	// the message to the next. Use when any of the brokers will do for
	// a message because its consumers subscribe to all of them. For
	// publish throughput into one broker or cluster use
	// [WithPublisherPool] instead.
	GroupPublishRoundRobin

	// GroupPublishHashByTopic picks the member by FNV-1a of
	// opts.Topic, preserving per-topic ordering across the group while
	// every member is connected; a member that cannot send passes the
	// message to the next, briefly reordering that topic. Empty Topic
	// collapses to member 0.
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

// GroupSuccess is how many members must succeed for a broadcast
// [ClientGroup.Publish] or a [ClientGroup.Subscribe] to succeed.
type GroupSuccess int

const (
	// GroupSuccessAll (default) needs every member.
	GroupSuccessAll GroupSuccess = 0
	// GroupSuccessAny needs one member.
	GroupSuccessAny GroupSuccess = 1
)

// GroupSuccessQuorum needs n members.
func GroupSuccessQuorum(n int) GroupSuccess { return GroupSuccess(n) }

// GroupResult is one member's part in a group operation.
type GroupResult struct {
	Member string
	Err    error // nil: the member succeeded
}

// GroupError reports a group operation that fewer members completed
// than [WithGroupSuccess] requires. errors.Is and errors.As see every
// member's error.
type GroupError struct {
	Op      string // "publish" or "subscribe"
	Need    int
	Results []GroupResult
}

func (e *GroupError) Error() string {
	ok := 0
	var failed []string
	for _, r := range e.Results {
		if r.Err == nil {
			ok++
			continue
		}
		failed = append(failed, fmt.Sprintf("%s: %v", r.Member, r.Err))
	}
	return fmt.Sprintf("mqttv5: ClientGroup %s succeeded on %d of %d members, needs %d: %s",
		e.Op, ok, len(e.Results), e.Need, strings.Join(failed, "; "))
}

func (e *GroupError) Unwrap() []error {
	var errs []error
	for _, r := range e.Results {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errs
}

// ClientGroupOption configures a ClientGroup at construct time.
type ClientGroupOption func(*clientGroupConfig)

type clientGroupConfig struct {
	publishPolicy GroupPublishPolicy
	success       GroupSuccess
	sharedOpts    []Option
	parallel      bool
}

// WithGroupSuccess sets how many members must succeed for a broadcast
// Publish or a Subscribe to succeed. Default [GroupSuccessAll].
func WithGroupSuccess(s GroupSuccess) ClientGroupOption {
	return func(c *clientGroupConfig) { c.success = s }
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
// per-broker credentials, or devices split across independent
// clusters. For HA failover use [WithBrokers] on a single [Client]
// instead — [ClientGroup] does not fail over between members, though
// a member with several [GroupMember.Brokers] fails over among them.
// For publish throughput into one broker or cluster use
// [WithPublisherPool]: members of a group over one cluster would each
// receive every message a merged subscription to an unshared filter
// matches. A group is used once: after Disconnect its methods
// return [ErrClosed].
type ClientGroup struct {
	cfg     clientGroupConfig
	members []*Client
	names   []string // index-aligned with members
	byName  map[string]*Client

	// rrCursor drives GroupPublishRoundRobin starting-index selection.
	rrCursor atomic.Uint64

	closed atomic.Bool
}

// NewClientGroup constructs a ClientGroup over the given members.
// Each GroupMember becomes a full *Client; the group applies the
// configured publish policy on top.
//
// Member ClientIDs are kept unique: a member whose ClientID another
// already uses gets the first free "-N" suffix (N from 2); members
// without one get one each from their broker. Set GroupMember.Opts with
// WithClientID for explicit per-member IDs.
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
	if cfg.success < 0 || int(cfg.success) > len(members) {
		return nil, fmt.Errorf("mqttv5: WithGroupSuccess(%d) with %d members", cfg.success, len(members))
	}

	clients := make([]*Client, 0, len(members))
	names := make([]string, 0, len(members))
	byName := make(map[string]*Client, len(members))
	usedClientID := make(map[string]bool, len(members))
	seenName := make(map[string]bool, len(members))

	for i, m := range members {
		if len(m.Brokers) == 0 {
			return nil, fmt.Errorf("mqttv5: ClientGroup members[%d]: Brokers is required", i)
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
		// setters. A ClientID another member uses gets a suffix.
		probe := &Config{}
		combined := append([]Option(nil), cfg.sharedOpts...)
		combined = append(combined, m.Opts...)
		for _, opt := range combined {
			if err := opt(probe); err != nil {
				return nil, fmt.Errorf("mqttv5: ClientGroup members[%d] (%q): %w", i, name, err)
			}
		}
		if len(probe.BrokerURLs) > 0 {
			return nil, fmt.Errorf("mqttv5: ClientGroup members[%d] (%q): set brokers in GroupMember.Brokers, not with WithBroker or WithBrokers", i, name)
		}
		memberID := uniqueClientID(probe.ClientID, usedClientID)

		memberOpts := make([]Option, 0, len(combined)+2)
		memberOpts = append(memberOpts, combined...)
		memberOpts = append(memberOpts, WithBrokers(m.Brokers...), WithClientID(memberID))

		c, err := New(memberOpts...)
		if err != nil {
			return nil, fmt.Errorf("mqttv5: ClientGroup members[%d] (%q): %w", i, name, err)
		}
		clients = append(clients, c)
		names = append(names, name)
		byName[name] = c
	}

	return &ClientGroup{
		cfg:     cfg,
		members: clients,
		names:   names,
		byName:  byName,
	}, nil
}

// need is how many members must succeed.
func (g *ClientGroup) need() int {
	if g.cfg.success == GroupSuccessAll {
		return len(g.members)
	}
	return int(g.cfg.success)
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
	if g.closed.Load() {
		return ErrClosed
	}
	return g.lifecycleOp(ctx, "Connect", func(_ int, m *Client) error {
		return m.Connect(ctx)
	})
}

// Disconnect tears down every member, which closes the merged channels
// and queues of the group's subscriptions. The default is parallel; use
// WithGroupSequentialLifecycle to serialise. Returned error is
// errors.Join of every member's Disconnect error. The group cannot be
// connected again.
func (g *ClientGroup) Disconnect(ctx context.Context) error {
	g.closed.Store(true)
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
// GroupPublishPolicy and returns each member's part. Broadcast publishes
// to every member at once and fails with a [*GroupError] when fewer
// than [WithGroupSuccess] require succeed. RoundRobin and HashByTopic
// send the message once: they move to the next member only when one
// could not send it at all (not connected), so a broker refusal or ctx
// ending is returned as is and never duplicates the message.
func (g *ClientGroup) Publish(ctx context.Context, opts PublishOptions) ([]GroupResult, error) {
	return g.publish(ctx, opts.Topic, func(*Client) (PublishOptions, error) { return opts, nil })
}

// publish sends what optsFor makes for each member it publishes on,
// under the group's publish policy. topic is the message's, for
// GroupPublishHashByTopic.
func (g *ClientGroup) publish(ctx context.Context, topic string, optsFor func(*Client) (PublishOptions, error)) ([]GroupResult, error) {
	if g.closed.Load() {
		return nil, ErrClosed
	}
	switch g.cfg.publishPolicy {
	case GroupPublishRoundRobin:
		return g.publishStartingAt(ctx, optsFor, g.rrCursor.Add(1))
	case GroupPublishHashByTopic:
		var start uint64
		if topic != "" {
			h := fnv.New32a()
			_, _ = h.Write([]byte(topic))
			start = uint64(h.Sum32())
		}
		return g.publishStartingAt(ctx, optsFor, start)
	default:
		return g.publishBroadcast(ctx, optsFor)
	}
}

func (g *ClientGroup) publishBroadcast(ctx context.Context, optsFor func(*Client) (PublishOptions, error)) ([]GroupResult, error) {
	results := make([]GroupResult, len(g.members))
	var wg sync.WaitGroup
	for i, m := range g.members {
		results[i].Member = g.names[i]
		opts, err := optsFor(m)
		if err != nil {
			results[i].Err = err
			continue
		}
		wg.Go(func() { results[i].Err = m.Publish(ctx, opts) })
	}
	wg.Wait()
	ok := 0
	for _, r := range results {
		if r.Err == nil {
			ok++
		}
	}
	if ok < g.need() {
		return results, &GroupError{Op: "publish", Need: g.need(), Results: results}
	}
	return results, nil
}

// publishStartingAt tries members from start until one sends what
// optsFor makes for it.
func (g *ClientGroup) publishStartingAt(ctx context.Context, optsFor func(*Client) (PublishOptions, error), start uint64) ([]GroupResult, error) {
	n := uint64(len(g.members))
	var results []GroupResult
	for i := range n {
		idx := (start + i) % n
		m := g.members[idx]
		opts, err := optsFor(m)
		if err == nil {
			err = m.Publish(ctx, opts)
		}
		results = append(results, GroupResult{Member: g.names[idx], Err: err})
		if !notSent(err) {
			return results, err
		}
	}
	return results, &GroupError{Op: "publish", Need: 1, Results: results}
}

// Subscribe subscribes on every member and merges their messages into
// one channel; Ack on a message goes back to the member that delivered
// it. The channel's buffer and drop rules are those of
// [Client.Subscribe], applied as each member delivers, with room for
// every member's QoS 1 and 2 messages. It closes once
// every member's subscription has ended: after [ClientGroup.UnsubscribeAll],
// an Unsubscribe of each member, or Disconnect.
//
// When fewer members than [WithGroupSuccess] requires subscribe — or
// ctx ends — the members that did are unsubscribed again and a
// [*GroupError] (or ctx's error) is returned. Otherwise the token map,
// keyed by member name, holds the members that subscribed; failures of
// the others are logged. Called before [ClientGroup.Connect], it
// returns at once, as [Client.Subscribe] does, and each member's
// SUBSCRIBE goes out first on its connection.
func (g *ClientGroup) Subscribe(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (<-chan *Message, map[string]SubscriptionToken, error) {
	return g.subscribeChan(ctx, filters, opts, ownMessages)
}

// subscribeChan subscribes every member with one merged channel of the
// elements wrap makes; see [ClientGroup.Subscribe].
func (g *ClientGroup) subscribeChan[T any](ctx context.Context, filters []TopicFilter, opts []SubscribeOption, wrap wrapper[T]) (<-chan T, map[string]SubscriptionToken, error) {
	if g.closed.Load() {
		return nil, nil, ErrClosed
	}
	cfg, err := g.members[0].chanSubscribeConfig(opts)
	if err != nil {
		return nil, nil, err
	}
	convs, err := memberWraps(g.members, cfg, wrap)
	if err != nil {
		return nil, nil, err
	}
	cfg.room = g.qosRoom(cfg, filters)
	if cfg.room > 0 {
		// Members deliver concurrently, so QoS 0 messages can overrun
		// the buffer by one for each other member.
		cfg.room += len(g.members) - 1
	}
	merged := make(chan T, cfg.bufferSize+cfg.room)
	tokens, err := g.subscribeAll(ctx, filters, func() { close(merged) }, func(i int, m *Client) *route {
		return &route{zeroCopy: cfg.zeroCopyDelivery(), deliver: chanDeliver(m, cfg, merged, convs[i])}
	})
	if err != nil {
		return nil, nil, err
	}
	return merged, tokens, nil
}

// memberWraps builds each member's wrap function before anything is
// subscribed, so a member that cannot decode fails the call up front.
func memberWraps[T any](members []*Client, cfg subscribeConfig, wrap wrapper[T]) ([]func(*Message) (T, bool), error) {
	convs := make([]func(*Message) (T, bool), len(members))
	for i, m := range members {
		conv, err := wrap(m, cfg)
		if err != nil {
			return nil, fmt.Errorf("mqttv5: ClientGroup member %d: %w", i+1, err)
		}
		convs[i] = conv
	}
	return convs, nil
}

// SubscribeQueue is the queue-merged variant of Subscribe, with the
// bound and drop policy of [Client.SubscribeQueue] applied to the merged
// queue as a whole.
func (g *ClientGroup) SubscribeQueue(ctx context.Context, filters []TopicFilter, opts ...SubscribeOption) (*Queue[*Message], map[string]SubscriptionToken, error) {
	return g.subscribeQueue(ctx, filters, opts, ownMessages, sameMessage)
}

// subscribeQueue subscribes every member with one merged queue of the
// elements wrap makes; see [ClientGroup.SubscribeQueue].
func (g *ClientGroup) subscribeQueue[T any](ctx context.Context, filters []TopicFilter, opts []SubscribeOption, wrap wrapper[T], message func(T) *Message) (*Queue[T], map[string]SubscriptionToken, error) {
	if g.closed.Load() {
		return nil, nil, ErrClosed
	}
	cfg := g.members[0].subscribeConfigFrom(opts)
	convs, err := memberWraps(g.members, cfg, wrap)
	if err != nil {
		return nil, nil, err
	}
	cfg.room = g.qosRoom(cfg, filters)
	merged := NewQueue[T]()
	tokens, err := g.subscribeAll(ctx, filters, merged.Close, func(i int, m *Client) *route {
		return &route{zeroCopy: cfg.zeroCopyDelivery(), deliver: queueDeliver(m, cfg, merged, convs[i], message)}
	})
	if err != nil {
		return nil, nil, err
	}
	return merged, tokens, nil
}

// qosRoom is the room a merged output keeps for QoS 1 and 2 messages:
// every member's (see [Client.qosRoom]).
func (g *ClientGroup) qosRoom(cfg subscribeConfig, filters []TopicFilter) int {
	room := 0
	for _, m := range g.members {
		room += m.qosRoom(cfg, filters)
	}
	return room
}

// subscribeAll subscribes every member with the route newRoute builds
// for it. closeOutput runs once every member's subscription has ended,
// including those that never started.
func (g *ClientGroup) subscribeAll(ctx context.Context, filters []TopicFilter, closeOutput func(), newRoute func(int, *Client) *route) (map[string]SubscriptionToken, error) {
	n := len(g.members)
	var remaining atomic.Int32
	remaining.Store(int32(n))
	ended := make([]func(), n)
	for i := range ended {
		ended[i] = sync.OnceFunc(func() {
			if remaining.Add(-1) == 0 {
				closeOutput()
			}
		})
	}

	results := make([]GroupResult, n)
	tokens := make([]SubscriptionToken, n)
	run := func(i int) {
		m := g.members[i]
		tokens[i], results[i].Err = m.subscribe(ctx, filters, newRoute(i, m), ended[i])
		results[i].Member = g.names[i]
	}
	if g.cfg.parallel {
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() { run(i) })
		}
		wg.Wait()
	} else {
		for i := range n {
			run(i)
		}
	}

	// A member counts once its subscription is active, even if the
	// broker refused some of its filters.
	active := 0
	for i := range n {
		if tokens[i].sub != nil && tokens[i].Err() == nil {
			active++
		}
	}
	var err error
	switch {
	case active < g.need():
		err = &GroupError{Op: "subscribe", Need: g.need(), Results: results}
	case ctx.Err() != nil:
		err = ctx.Err()
	}
	out := make(map[string]SubscriptionToken, n)
	for i, r := range results {
		switch {
		case tokens[i].sub == nil:
			ended[i]() // never subscribed: nothing else will end it
		case err != nil:
			g.members[i].abandon(tokens[i].sub)
			continue
		case tokens[i].Err() == nil:
			out[r.Member] = tokens[i]
		}
		if r.Err != nil && err == nil {
			g.members[i].cfg.Logger.Warn("mqttv5: ClientGroup member did not subscribe fully",
				slog.String("member", r.Member), slog.Any("error", r.Err))
		}
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// uniqueClientID returns id, or, when another member already uses it,
// the first of id-2, id-3, … that none does, and marks it used. An empty
// id asks each broker to assign one, so it is never suffixed.
func uniqueClientID(id string, used map[string]bool) string {
	if id == "" {
		return ""
	}
	unique := id
	for n := 2; used[unique]; n++ {
		unique = fmt.Sprintf("%s-%d", id, n)
	}
	used[unique] = true
	return unique
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
