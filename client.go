// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	mathrand "math/rand/v2"
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
	supWg   sync.WaitGroup

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
}

func newLifecycle() *lifecycle {
	return &lifecycle{shutdown: make(chan struct{}), finished: make(chan struct{})}
}

func (l *lifecycle) end() { l.endWith(nil) }

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
