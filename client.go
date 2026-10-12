// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	mathrand "math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/internal/trie"
	"github.com/ashtonian/mqttv5/wire"
)

// HandlerFunc runs synchronously on the read goroutine for each
// matching inbound PUBLISH. Handlers MUST be non-blocking — a slow
// handler stalls the read loop, blocks PINGRESP processing, and
// drops the connection. Use Subscribe / SubscribeQueue for async.
type HandlerFunc func(*Message)

// Client is an MQTT v5 client. After New(opts...), call Connect to
// establish the first connection and start the supervisor that
// reconnects on drop.
type Client struct {
	cfg *Config

	// engine holds the session state: packet identifiers and every
	// unfinished QoS 1/2 flow in both directions.
	engine *inflight.Engine

	// restored is set once the engine has loaded the configured Store.
	restored atomic.Bool

	// brokerURLs is the runtime-mutable broker list. SetBrokers
	// replaces the slice (copy-on-write). The supervisor reads it
	// each reconnect attempt.
	brokerURLs atomic.Pointer[[]string]

	// brokerIdx is the round-robin pointer into brokerURLs. Each
	// reconnect attempt advances it by one. The initial value is
	// random per-process so coordinated restarts of many clients
	// don't all hit BrokerURLs[0] simultaneously.
	brokerIdx atomic.Uint32

	// redirectOnce, when set, is the URL the next attempt dials instead
	// of the list (a temporary server redirect).
	redirectOnce atomic.Pointer[string]

	router *trie.Tree

	// Lifecycle. life is the current Connect…Disconnect span; startMu
	// guards started and the switch to a new span.
	startMu sync.Mutex
	started bool
	life    atomic.Pointer[lifecycle]
	// events runs the lifecycle callbacks.
	events events

	// cur is the current live connection. Producers (Publish, Subscribe,
	// etc.) load it atomically; nil means "not connected right now".
	// Changed only through setCur.
	cur atomic.Pointer[connState]
	// curChanged is closed, and replaced, whenever cur changes.
	curMu      sync.Mutex
	curChanged chan struct{}

	// Subscriptions. ctrlSem (capacity 1) orders every decision that
	// sends a SUBSCRIBE or UNSUBSCRIBE together with the send, so the
	// broker sees them in the order the client made them. Lock order:
	// ctrlSem, subsMu, ctrlMu.
	ctrlSem chan struct{}

	subsMu     sync.Mutex
	activeSubs map[uint64]*activeSub
	nextSubID  uint64
	brokerSubs map[string]*brokerSub // by exact filter
	identRefs  map[uint32]int        // Subscription Identifiers in use
	nextIdent  uint32

	ctrlMu      sync.Mutex
	ctrlQueue   []*ctrlOp          // unacknowledged, in send order
	ctrlOps     map[uint16]*ctrlOp // by packet identifier on its connection
	sessionLost bool               // the broker kept no session; flushCtrl re-subscribes

	// pool, if non-nil, spreads Publish across dedicated publish-only
	// connections (WithPublisherPool). Built in New and kept for the
	// Client's lifetime; Connect and Disconnect connect its members.
	pool *publisherPool

	// stats holds the public counters exposed by Client.Stats. Nil
	// when WithStats was not set on the Config — call sites use the
	// safe-on-nil helper methods on *clientStats so the hot path
	// becomes a single predicted nil-check when stats are disabled.
	stats *clientStats
}

// New constructs a Client. Apply WithBroker (required) plus any other
// options. The Client is not connected — call Connect.
func New(opts ...Option) (*Client, error) {
	cfg := &Config{
		CleanStart:                true,
		SessionExpiry:             DefaultSessionExpiry,
		RequestProblemInformation: true,
	}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	cfg.defaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	c := &Client{
		cfg:        cfg,
		router:     trie.NewTree(),
		ctrlSem:    make(chan struct{}, 1),
		ctrlOps:    make(map[uint16]*ctrlOp),
		activeSubs: make(map[uint64]*activeSub),
		brokerSubs: make(map[string]*brokerSub),
		identRefs:  make(map[uint32]int),
		curChanged: make(chan struct{}),
	}
	if cfg.StatsEnabled {
		c.stats = &clientStats{}
	}
	c.engine = inflight.New(inflight.Config{
		Store:          cfg.Store,
		ReceiveMaximum: cfg.ReceiveMaximum,
		SessionLoss:    inflight.SessionLossPolicy(cfg.SessionLossPolicy),
		Logger:         cfg.Logger,
		Now:            cfg.clock.Now,
		OnStoreError:   func(string, error) { c.stats.addStoreError() },
		OnFailure:      c.storeFailed,
		OnStrayAck:     func(wire.PacketType, uint16) { c.stats.addAckIgnored() },
		OnReplay:       c.stats.addPublishReplayed,
		Refusal: func(t wire.PacketType, rc wire.ReasonCode) error {
			return newReasonCodeError(t, rc, wire.Properties{})
		},
	})
	urls := append([]string(nil), cfg.BrokerURLs...)
	c.brokerURLs.Store(&urls)
	// Randomise the starting index so N processes restarting together
	// don't all stampede onto BrokerURLs[0].
	c.brokerIdx.Store(mathrand.Uint32())
	c.life.Store(newLifecycle())
	if cfg.PublisherPoolSize > 1 {
		pool, err := newPublisherPool(cfg, cfg.PublisherPoolSize, cfg.Logger)
		if err != nil {
			return nil, err
		}
		c.pool = pool
	}
	return c, nil
}

// lifecycle is one Connect…Disconnect span of a Client.
type lifecycle struct {
	shutdown    chan struct{} // closed when the span ends
	finished    chan struct{} // closed once its teardown is complete
	endOnce     sync.Once
	disconnOnce sync.Once
	finishOnce  sync.Once
	// err is why the span ended when it was not Disconnect, written
	// before shutdown is closed.
	err error
	// mu orders a connection's activation, and the supervisor's start,
	// against the span's teardown: stopping is set under it, and both
	// happen under it only while stopping is unset.
	mu       sync.Mutex
	stopping bool
	// running counts what may still install a connection in the span:
	// the Connect starting it, and its supervisor. The teardown waits
	// for both.
	running sync.WaitGroup
}

func newLifecycle() *lifecycle {
	return &lifecycle{shutdown: make(chan struct{}), finished: make(chan struct{})}
}

func (l *lifecycle) end() { l.endWith(nil) }

// isStopping reports whether the span's teardown has started.
func (l *lifecycle) isStopping() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stopping
}

// endedErr is why the span ended once it has, and nil before.
func (l *lifecycle) endedErr() error {
	select {
	case <-l.shutdown:
		return l.closedErr()
	default:
		return nil
	}
}

// claimStop marks the span stopping and reports whether this call did,
// in which case the caller owns its teardown.
func (l *lifecycle) claimStop() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopping {
		return false
	}
	l.stopping = true
	return true
}

// context returns a context that ends with parent, or when the span
// ends.
func (l *lifecycle) context(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-l.shutdown:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// endWith ends the span, recording err as the reason when it is the
// first to.
func (l *lifecycle) endWith(err error) {
	l.endOnce.Do(func() {
		l.err = err
		close(l.shutdown)
	})
}

// closedErr is what a call cut short by the end of the span returns:
// ErrClosed, or the failure that ended it. Valid once shutdown is
// closed.
func (l *lifecycle) closedErr() error {
	if l.err != nil {
		return l.err
	}
	return ErrClosed
}

// done is closed when the current Connect…Disconnect span ends. Before
// the first Connect it never closes.
func (c *Client) done() <-chan struct{} { return c.life.Load().shutdown }

// setCur makes cs (nil when disconnected) the current connection and
// wakes everyone waiting in connChanged.
func (c *Client) setCur(cs *connState) {
	c.curMu.Lock()
	c.cur.Store(cs)
	close(c.curChanged)
	c.curChanged = make(chan struct{})
	c.curMu.Unlock()
}

// connChanged returns a channel closed the next time the client
// connects or disconnects.
func (c *Client) connChanged() <-chan struct{} {
	c.curMu.Lock()
	defer c.curMu.Unlock()
	return c.curChanged
}

// currentBrokerURL returns the broker URL the supervisor should dial
// next, using brokerIdx as the round-robin pointer into the URL list.
// Advancing brokerIdx is the supervisor's responsibility (per attempt
// in the reconnect loop); this accessor only reads.
func (c *Client) currentBrokerURL() string {
	urls := *c.brokerURLs.Load()
	return urls[int(c.brokerIdx.Load())%len(urls)]
}

// SetBrokers replaces the broker URL list at runtime; the next
// reconnect uses it. Typical caller: [WithOnServerDisconnect] on a
// ServerMoved / UseAnotherServer redirect. URLs are validated as in
// [New].
func (c *Client) SetBrokers(urls ...string) error {
	if err := validateBrokerURLs(urls, c.cfg.DialFunc != nil); err != nil {
		return err
	}
	cp := append([]string(nil), urls...)
	c.brokerURLs.Store(&cp)
	return nil
}

// Connected reports whether the client currently has a live connection.
func (c *Client) Connected() bool { return c.cur.Load() != nil }

// ClientID returns the ClientID currently in effect: the one configured
// with [WithClientID], or, when that was empty, the identifier the
// broker assigned (AssignedClientIdentifier, §3.2.2.3.7). The assigned
// identifier is reused on every later CONNECT so the session can resume,
// and is kept in the [WithStore] store across restarts.
func (c *Client) ClientID() string {
	if c.cfg.ClientID != "" {
		return c.cfg.ClientID
	}
	return c.engine.Meta().ClientID
}

// ConnackInfo is what the broker granted in CONNACK (§3.2.2.3), with the
// spec's defaults applied to properties it did not send. The client
// enforces these limits on everything it sends: a publish that exceeds
// them fails before it reaches the wire.
type ConnackInfo struct {
	// Broker is the URL of the broker the connection reached: with
	// [WithBrokers], the one the client failed over to.
	Broker string

	// SessionPresent reports whether the broker resumed an existing
	// session.
	SessionPresent bool

	// SessionExpiry is the effective Session Expiry Interval in seconds:
	// the broker's value when it overrode the requested one.
	SessionExpiry uint32

	// KeepAlive is the effective keep-alive in seconds: the broker's
	// Server Keep Alive when sent, otherwise the requested value.
	// Zero disables keep-alive.
	KeepAlive uint16

	// ReceiveMaximum caps the QoS 1/2 PUBLISHes the client may have
	// unacknowledged (default 65,535).
	ReceiveMaximum uint16

	// MaximumQoS is the highest QoS the broker accepts (default 2).
	MaximumQoS byte

	// RetainAvailable reports whether retained messages are accepted
	// (default true).
	RetainAvailable bool

	// MaximumPacketSize is the largest packet the broker accepts, in
	// bytes; zero means no limit beyond the protocol's.
	MaximumPacketSize uint32

	// TopicAliasMaximum is the number of outbound topic aliases the
	// broker accepts (default 0: none).
	TopicAliasMaximum uint16

	// AssignedClientID is the identifier the broker chose when the
	// client connected without one.
	AssignedClientID string

	// Subscription features (default true when not sent).
	WildcardSubscriptionAvailable    bool
	SubscriptionIdentifiersAvailable bool
	SharedSubscriptionAvailable      bool

	ResponseInformation  string
	ServerReference      string
	ReasonString         string
	UserProperties       []wire.UserProperty
	AuthenticationMethod string
	AuthenticationData   []byte
}

// parseConnack reads every CONNACK property once, copying what it keeps
// out of the frame. sent is the CONNECT that produced it.
func parseConnack(p *wire.Connack, sent wire.ConnectOpts) ConnackInfo {
	props := p.Properties
	info := ConnackInfo{
		SessionPresent:                   p.SessionPresent,
		KeepAlive:                        sent.KeepAlive,
		ReceiveMaximum:                   65535,
		MaximumQoS:                       2,
		RetainAvailable:                  true,
		WildcardSubscriptionAvailable:    connackCapability(props, wire.PropWildcardSubAvailable),
		SubscriptionIdentifiersAvailable: connackCapability(props, wire.PropSubscriptionIDAvailable),
		SharedSubscriptionAvailable:      connackCapability(props, wire.PropSharedSubAvailable),
	}
	if sent.SessionExpiryInterval != nil {
		info.SessionExpiry = *sent.SessionExpiryInterval
	}
	if v, ok := props.Uint32(wire.PropSessionExpiryInterval); ok {
		info.SessionExpiry = v
	}
	if v, ok := props.Uint16(wire.PropServerKeepAlive); ok {
		info.KeepAlive = v
	}
	if v, ok := props.Uint16(wire.PropReceiveMaximum); ok && v > 0 {
		info.ReceiveMaximum = v
	}
	if v, ok := props.Byte(wire.PropMaximumQoS); ok && v < 2 {
		info.MaximumQoS = v
	}
	if v, ok := props.Byte(wire.PropRetainAvailable); ok {
		info.RetainAvailable = v != 0
	}
	if v, ok := props.Uint32(wire.PropMaximumPacketSize); ok {
		info.MaximumPacketSize = v
	}
	info.TopicAliasMaximum, _ = props.Uint16(wire.PropTopicAliasMaximum)
	info.AssignedClientID = cloneString(props, wire.PropAssignedClientID)
	info.ResponseInformation = cloneString(props, wire.PropResponseInformation)
	info.ServerReference = cloneString(props, wire.PropServerReference)
	info.ReasonString = cloneString(props, wire.PropReasonString)
	info.AuthenticationMethod = cloneString(props, wire.PropAuthMethod)
	if v, ok := props.Binary(wire.PropAuthData); ok {
		info.AuthenticationData = bytes.Clone(v)
	}
	info.UserProperties = userProperties(props)
	return info
}

func cloneString(props wire.Properties, id byte) string {
	v, _ := props.String(id)
	return strings.Clone(v)
}

// connackCapability reads one of the SubscriptionIdentifiersAvailable
// / WildcardSubscriptionAvailable / SharedSubscriptionAvailable
// CONNACK properties. The MQTT v5 default when the property is absent
// is "supported" (true) for all three.
func connackCapability(props wire.Properties, id byte) bool {
	v, ok := props.Byte(id)
	if !ok {
		return true
	}
	return v != 0
}

// ServerInfo returns what the broker granted on the current connection,
// and false when the client is not connected.
func (c *Client) ServerInfo() (ConnackInfo, bool) {
	cs := c.cur.Load()
	if cs == nil {
		return ConnackInfo{}, false
	}
	return cs.info.clone(), true
}

// clone deep-copies the slices so a caller cannot modify the client's
// copy.
func (i ConnackInfo) clone() ConnackInfo {
	i.UserProperties = slices.Clone(i.UserProperties)
	i.AuthenticationData = bytes.Clone(i.AuthenticationData)
	return i
}

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
