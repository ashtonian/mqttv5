// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mathrand "math/rand/v2"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ashtonian/mqttv5/internal/trie"
	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// Errors returned by the client.
var (
	ErrNotConnected     = errors.New("mqttv5: client not connected")
	ErrAlreadyConnected = errors.New("mqttv5: client already connected")
	ErrClosed           = errors.New("mqttv5: client closed")
	ErrConnectRefused   = errors.New("mqttv5: broker refused CONNECT")
	ErrUnexpectedPacket = errors.New("mqttv5: unexpected packet from broker")

	// Enhanced-authentication errors (§4.12), returned by Reauthenticate.
	// ErrReauthRejected is wrapped with the broker's DISCONNECT reason
	// code (mirroring ErrConnectRefused) — match with errors.Is.
	ErrNoAuthenticator  = errors.New("mqttv5: no Authenticator configured")
	ErrReauthInProgress = errors.New("mqttv5: re-authentication already in progress")
	ErrReauthRejected   = errors.New("mqttv5: broker rejected re-authentication")

	// ErrWriteQueueFull is returned by QoS 0 Publish when the client
	// is configured with WriteDropNewest and the writer queue has no
	// room. The publish never reaches the wire.
	ErrWriteQueueFull = errors.New("mqttv5: write queue full")

	// Subscribe-time errors raised when the broker advertised the
	// feature as unsupported in CONNACK (MQTT v5 §3.2.2.3.{11,12,13}).
	// Returned before any SUBSCRIBE traffic goes on the wire.
	ErrSharedSubsUnsupported      = errors.New("mqttv5: broker does not support shared subscriptions")
	ErrWildcardSubsUnsupported    = errors.New("mqttv5: broker does not support wildcard subscriptions")
	ErrSubscriptionIDsUnsupported = errors.New("mqttv5: broker does not support subscription identifiers")
)

// HandlerFunc runs synchronously on the read goroutine for each
// matching inbound PUBLISH. Handlers MUST be non-blocking — a slow
// handler stalls the read loop, blocks PINGRESP processing, and
// drops the connection. Use Subscribe / SubscribeQueue for async.
type HandlerFunc func(*Message)

// connState bundles everything tied to one TCP/TLS connection. Each
// reconnect allocates a fresh connState; per-connection goroutines
// hold it by pointer so the old one stays valid even after the
// Client has moved on.
type connState struct {
	conn       transport.Conn
	decoder    *wire.Decoder
	writeQueue chan writeReq

	dying     chan struct{}
	dyingOnce sync.Once
	done      chan struct{}
	doneOnce  sync.Once
	wg        sync.WaitGroup

	// Ping liveness for this specific connection (unix nano).
	pingSent atomic.Int64
	pingResp atomic.Int64

	// Last successful network activity timestamps (unix nano) for the
	// idle-aware PINGREQ scheduler. writeLoop updates lastWriteUnixNano
	// after every successful conn.Write; readLoop updates
	// lastReadUnixNano after every successful packet read. The ping
	// scheduler only emits PINGREQ when EITHER window has been quiet
	// for KeepAlive seconds — outbound activity satisfies MQTT v5
	// §3.1.2.10 (the broker won't disconnect us), and inbound activity
	// satisfies broker-liveness detection (we know the broker is alive).
	lastWriteUnixNano atomic.Int64
	lastReadUnixNano  atomic.Int64

	// Inbound topic-alias cache. The server may register an alias by
	// sending PUBLISH (Topic="x/y", TopicAlias=N), then re-use it by
	// sending PUBLISH (Topic="", TopicAlias=N). The cache is fresh
	// per-connection per MQTT v5 §3.3.2.3.4. Stored strings are
	// strings.Clone'd so they outlive the frame they were decoded from.
	aliasMu  sync.RWMutex
	aliasMap map[uint16]string

	// Outbound topic-alias allocation. outAliasMax is the broker's
	// TopicAliasMaximum from CONNACK; 0 means the broker won't accept
	// aliases at all. outAliasMap maps a publish topic to the alias
	// we've registered with the broker on this connection.
	//
	// Only applied to QoS 0 publishes — QoS 1/2 publishes must use
	// the full topic string so the supervisor's replay path works
	// across a reconnect (the broker's alias state resets per
	// §3.3.2.3.4).
	outAliasMu   sync.Mutex
	outAliasMap  map[string]uint16
	outAliasNext uint16
	outAliasMax  uint16

	// serverDisconnect carries the broker-side DISCONNECT (cloned, so
	// safe to retain past the originating frame) when the connection
	// goes down due to a broker-initiated disconnect. Read by the
	// supervisor to invoke OnServerDisconnect before the generic
	// OnConnectionDown, and by Reauthenticate to distinguish a broker
	// rejection (ErrReauthRejected) from a plain drop (ErrNotConnected).
	serverDisconnect atomic.Pointer[wire.Disconnect]

	// reauth is non-nil while a client-initiated re-authentication
	// (Reauthenticate) is in flight on this connection. The read loop
	// routes inbound AUTH to the in-flight call and clears the slot when
	// the exchange resolves on the wire (0x00 Success, or a DISCONNECT /
	// drop that disposes of this connState). It is deliberately NOT
	// cleared when the caller's ctx elapses, so a late terminal AUTH
	// cannot be misattributed to a subsequent Reauthenticate on the same
	// connection.
	reauth atomic.Pointer[reauthCall]

	// Broker capability flags from CONNACK. MQTT v5 default is
	// "supported" when the property is absent. subscribeImpl uses
	// these to fail fast on filters/properties the broker disabled.
	sharedSubsAvail   bool
	wildcardSubsAvail bool
	subIDsAvail       bool
}

// signalDown marks this connection as dying. Idempotent.
func (cs *connState) signalDown() {
	cs.dyingOnce.Do(func() {
		close(cs.dying)
		if cs.conn != nil {
			_ = cs.conn.Close()
		}
	})
}

// Client is an MQTT v5 client. After New(opts...), call Connect to
// establish the first connection and start the supervisor that
// reconnects on drop.
type Client struct {
	cfg     *Config
	session *session.Session

	// brokerURLs is the runtime-mutable broker list. SetBrokers
	// replaces the slice (copy-on-write). The supervisor reads it
	// each reconnect attempt.
	brokerURLs atomic.Pointer[[]string]

	// brokerIdx is the round-robin pointer into brokerURLs. Each
	// reconnect attempt advances it by one. The initial value is
	// random per-process so coordinated restarts of many clients
	// don't all hit BrokerURLs[0] simultaneously.
	brokerIdx atomic.Uint32

	// assignedClientID holds the AssignedClientIdentifier from the
	// last successful CONNACK when the broker chose the ID (caller
	// passed WithClientID("")). Empty string when the caller's
	// configured ID is in effect.
	assignedClientID atomic.Pointer[string]

	router atomic.Pointer[trie.Tree]

	// Lifecycle (permanent — set on first Connect, retired on Disconnect).
	startMu     sync.Mutex
	started     bool
	shutdown    chan struct{} // closed by Disconnect
	supWg       sync.WaitGroup
	closeOnce   sync.Once
	disconnOnce sync.Once

	// cur is the current live connection. Producers (Publish, Subscribe,
	// etc.) load it atomically; nil means "not connected right now".
	cur atomic.Pointer[connState]

	// Subscribe/Unsubscribe inflight (SUBACK/UNSUBACK waiters).
	ctrlMu      sync.Mutex
	ctrlWaiters map[uint16]chan wire.Packet

	// Active subscriptions tracked for resubscribe on reconnect.
	subsMu     sync.Mutex
	activeSubs map[uint64]*activeSub
	nextSubID  atomic.Uint64

	// pubPool, if non-nil, fans Publish round-robin across N
	// dedicated publish-only connections (WithPublisherPool). When
	// every pool member is unhealthy the main connection is used as
	// the fallback.
	pubPool *publisherPool

	// stats holds the public counters exposed by Client.Stats. Nil
	// when WithStats was not set on the Config — call sites use the
	// safe-on-nil helper methods on *clientStats so the hot path
	// becomes a single predicted nil-check when stats are disabled.
	stats *clientStats
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

// Stats is the snapshot of Client counters returned by Client.Stats.
//
// Two flavours of field:
//
//   - Monotonic counters (Connects, ConnectFailures, Disconnects,
//     ServerDisconnects, PingTimeouts, PublishesSent, PublishesAcked,
//     PublishesReplayed, InboundPublishes, InboundDropped,
//     SubscribesSent, UnsubscribesSent, PoolFallbacks). Sampled
//     atomically — no locking on the read side.
//   - Point-in-time gauges (PublishesInflight, SubscriptionsActive)
//     read live state under a mutex (Outbound tracker and the
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
	PublishesReplayed uint64 // QoS 1/2 replays issued after reconnect

	InboundPublishes uint64 // PUBLISHes received from the broker
	InboundDropped   uint64 // inbound PUBLISHes dropped due to full subscriber buffer

	SubscribesSent      uint64 // SUBSCRIBE packets emitted
	UnsubscribesSent    uint64 // UNSUBSCRIBE packets emitted
	SubscriptionsActive uint64 // count of currently registered subscriptions

	PoolFallbacks uint64 // publishes that fell back from pool to main connection
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

	return Stats{
		Connects:            c.stats.connects.Load(),
		ConnectFailures:     c.stats.connectFailures.Load(),
		Disconnects:         c.stats.disconnects.Load(),
		ServerDisconnects:   c.stats.serverDisconnects.Load(),
		PingTimeouts:        c.stats.pingTimeouts.Load(),
		PublishesSent:       c.stats.publishesSent.Load(),
		PublishesAcked:      c.stats.publishesAcked.Load(),
		PublishesInflight:   uint64(c.session.Outbound.Len()),
		PublishesReplayed:   c.stats.publishesReplayed.Load(),
		InboundPublishes:    c.stats.inboundPublishes.Load(),
		InboundDropped:      c.stats.inboundDropped.Load(),
		SubscribesSent:      c.stats.subscribesSent.Load(),
		UnsubscribesSent:    c.stats.unsubscribesSent.Load(),
		SubscriptionsActive: subs,
		PoolFallbacks:       c.stats.poolFallbacks.Load(),
	}
}

// activeSub remembers everything needed to resubscribe a filter set
// and to clean up channel/queue consumers when the sub goes away.
type activeSub struct {
	id      uint64
	filters []wire.SubscribeFilter
	handler HandlerFunc
	trieIDs []trieRegEntry
	// onUnsub fires when the subscription is torn down by either
	// Unsubscribe or Disconnect. Used by Subscribe (channel) and
	// SubscribeQueue to close their consumer-side handle.
	onUnsub func()
}

type trieRegEntry struct {
	filter string
	id     uint64
}

// writeFn produces wire bytes into w. Closures over the caller's
// per-call opts. The writer goroutine invokes them against the transport.
type writeFn func(io.Writer) (int64, error)

// writeReq is a unit of work for the writer goroutine. Exactly one of
// fn or pkt must be set.
//
// The pkt path is the zero-closure-alloc fast path used by QoS 0
// fire-and-forget Publish: the packet bytes are pre-encoded into a
// pooled buffer (wire.EncodePublish) and the writer just calls
// conn.Write on them, then releases the buffer.
//
// The fn path is used by everything else (CONNECT, SUBSCRIBE, AUTH,
// PINGREQ, QoS 1/2 Publish replays, ...) where the encoding is
// stateful or the bytes are already owned by the caller (e.g. the
// session keeps QoS 1/2 PUBLISH bytes for replay).
type writeReq struct {
	fn   writeFn
	pkt  *[]byte      // pre-encoded packet; writer releases after Write
	done chan<- error // nil for fire-and-forget
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
		cfg: cfg,
		session: session.New(session.Config{
			ReceiveMaximum: cfg.ReceiveMaximum,
			Store:          cfg.Store,
		}),
		ctrlWaiters: make(map[uint16]chan wire.Packet),
		activeSubs:  make(map[uint64]*activeSub),
	}
	if cfg.StatsEnabled {
		c.stats = &clientStats{}
	}
	urls := append([]string(nil), cfg.BrokerURLs...)
	c.brokerURLs.Store(&urls)
	// Randomise the starting index so N processes restarting together
	// don't all stampede onto BrokerURLs[0].
	c.brokerIdx.Store(mathrand.Uint32())
	c.router.Store(trie.NewTree())
	return c, nil
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

// Connect performs the initial CONNECT/CONNACK handshake (bounded
// by ctx) and starts the supervisor. Reconnect attempts run on
// internal background contexts until [Client.Disconnect]. Returns
// [ErrAlreadyConnected] when called twice without an intervening
// Disconnect.
func (c *Client) Connect(ctx context.Context) error {
	c.startMu.Lock()
	if c.started {
		c.startMu.Unlock()
		return ErrAlreadyConnected
	}
	c.started = true
	c.shutdown = make(chan struct{})
	c.closeOnce = sync.Once{}
	c.disconnOnce = sync.Once{}
	c.startMu.Unlock()

	result, err := c.connectOnce(ctx, false)
	if err != nil {
		c.startMu.Lock()
		c.started = false
		c.startMu.Unlock()
		if c.cfg.OnConnectError != nil {
			c.cfg.OnConnectError(err)
		}
		c.stats.addConnectFailure()
		return err
	}
	c.runConnection(result)

	if c.cfg.PublisherPoolSize > 1 {
		pool, err := newPublisherPool(c.cfg, c.cfg.PublisherPoolSize, c.cfg.Logger)
		if err != nil {
			c.cfg.Logger.Warn("mqttv5: publisher pool init failed", slog.Any("error", err))
		} else {
			if err := pool.connect(ctx); err != nil {
				c.cfg.Logger.Warn("mqttv5: publisher pool partial connect", slog.Any("error", err))
			}
			c.pubPool = pool
		}
	}

	c.supWg.Add(1)
	go c.supervisor()
	return nil
}

// Disconnect sends a graceful DISCONNECT (reason Normal
// Disconnection), stops the supervisor, and tears down the current
// connection. For a custom reason / properties use
// [Client.DisconnectWith]. Does not fire OnConnectionDown.
// Idempotent.
func (c *Client) Disconnect(ctx context.Context) error {
	return c.DisconnectWith(ctx, wire.DisconnectOpts{
		ReasonCode: wire.ReasonNormalDisconnection,
	})
}

// DisconnectWith sends a DISCONNECT with opts, stops the supervisor,
// and tears down the current connection. Pool-member disconnect
// errors are joined into the returned error. Idempotent.
func (c *Client) DisconnectWith(ctx context.Context, opts wire.DisconnectOpts) error {
	c.startMu.Lock()
	started := c.started
	c.startMu.Unlock()
	if !started {
		return nil
	}

	var poolErr error
	c.disconnOnce.Do(func() {
		c.sendDisconnectWith(ctx, opts)
		c.closeOnce.Do(func() { close(c.shutdown) })
		if cs := c.cur.Load(); cs != nil {
			cs.signalDown()
		}
		if c.pubPool != nil {
			poolErr = c.pubPool.disconnect(ctx)
			c.pubPool = nil
		}
	})
	c.supWg.Wait()

	// Close any per-subscription channels / queues so consumers
	// reading from them unblock cleanly.
	c.subsMu.Lock()
	subs := c.activeSubs
	c.activeSubs = make(map[uint64]*activeSub)
	c.subsMu.Unlock()
	for _, sub := range subs {
		if sub.onUnsub != nil {
			sub.onUnsub()
		}
	}

	c.cur.Store(nil)
	c.cfg.Logger.Info("mqttv5: disconnected", slog.String("client_id", c.cfg.ClientID))

	c.startMu.Lock()
	c.started = false
	c.startMu.Unlock()
	return poolErr
}

// Connected reports whether the client currently has a live connection.
func (c *Client) Connected() bool { return c.cur.Load() != nil }

// ClientID returns the configured ClientID.
// ClientID returns the ClientID currently in effect. When the broker
// assigned an identifier via the AssignedClientIdentifier CONNACK
// property (typical when WithClientID("") was used), the broker-side
// value is returned; otherwise the value configured via WithClientID
// is returned.
func (c *Client) ClientID() string {
	if p := c.assignedClientID.Load(); p != nil && *p != "" {
		return *p
	}
	return c.cfg.ClientID
}

// connectResult bundles everything the CONNACK reveals to the
// supervisor. Returned by connectOnce, consumed by runConnection.
type connectResult struct {
	conn              transport.Conn
	decoder           *wire.Decoder
	brokerAliasMax    uint16
	sharedSubsAvail   bool
	wildcardSubsAvail bool
	subIDsAvail       bool
	// connack is a detached clone of the CONNACK, safe to retain past
	// the originating frame's Release. Handed to OnConnectionUp.
	connack *wire.Connack
}

// dial resolves the broker URL to a live transport.Conn. If a custom
// DialFunc is configured it takes precedence and is given a parsed
// *url.URL so the implementation doesn't re-parse. Otherwise the
// built-in transport.Dial handles TCP and TLS schemes.
func (c *Client) dial(ctx context.Context, brokerURL string) (transport.Conn, error) {
	if c.cfg.DialFunc != nil {
		u, err := url.Parse(brokerURL)
		if err != nil {
			return nil, fmt.Errorf("parse broker URL: %w", err)
		}
		return c.cfg.DialFunc(ctx, u)
	}
	return transport.Dial(ctx, brokerURL, transport.DialOpts{
		TLSConfig: c.cfg.TLSConfig,
		Dialer:    c.cfg.Dialer,
	})
}

// connectOnce performs the dial + CONNECT/CONNACK handshake. Returns
// the live transport.Conn, a wire.Decoder ready to read packets, and
// the broker's TopicAliasMaximum so the outbound alias allocator
// knows its budget for this connection. isReconnect selects between
// CleanStart (initial) and CleanStartOnReconnect (subsequent) and
// drives the ConnectPacketBuilder hook.
func (c *Client) connectOnce(ctx context.Context, isReconnect bool) (*connectResult, error) {
	dialCtx, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
	defer cancel()

	conn, err := c.dial(dialCtx, c.currentBrokerURL())
	if err != nil {
		return nil, fmt.Errorf("mqttv5: dial: %w", err)
	}

	if dl, ok := dialCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	if err := c.writeConnect(dialCtx, conn, isReconnect); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mqttv5: write CONNECT: %w", err)
	}

	dec := wire.NewDecoder(conn)

	// Loop on AUTH packets until CONNACK arrives. Without an
	// Authenticator configured the broker should never send AUTH —
	// if one shows up we error out (the spec says the client must
	// terminate the connection per §3.15.4).
	for {
		pkt, err := dec.ReadPacket()
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("mqttv5: read CONNACK: %w", err)
		}
		switch p := pkt.(type) {
		case *wire.Auth:
			if c.cfg.Authenticator == nil {
				p.Release()
				_ = conn.Close()
				return nil, fmt.Errorf("mqttv5: broker sent AUTH but no Authenticator configured")
			}
			brokerData, _ := p.Properties.Binary(wire.PropAuthData)
			method := c.cfg.Authenticator.Method()
			p.Release()
			response, _, err := c.cfg.Authenticator.Continue(brokerData)
			if err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("mqttv5: Authenticator.Continue: %w", err)
			}
			if _, err := wire.WriteAuth(conn, wire.AuthOpts{
				ReasonCode:           wire.ReasonContinueAuthentication,
				AuthenticationMethod: method,
				AuthenticationData:   response,
			}); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("mqttv5: write AUTH: %w", err)
			}
			// Loop — broker, not us, decides when to send CONNACK.
		case *wire.Connack:
			if p.ReasonCode.IsError() {
				reason := p.ReasonCode
				p.Release()
				_ = conn.Close()
				return nil, fmt.Errorf("%w: reason 0x%02X", ErrConnectRefused, byte(reason))
			}
			// Enhanced-auth mutual verification (§4.12): an Authenticator
			// that implements ServerFinalVerifier checks the server's
			// concluding AuthenticationData (e.g. the SCRAM server
			// signature) carried on the CONNACK. Failure aborts the connect.
			if v, ok := c.cfg.Authenticator.(ServerFinalVerifier); ok {
				serverFinal, _ := p.Properties.Binary(wire.PropAuthData)
				if err := v.VerifyServerFinal(append([]byte(nil), serverFinal...)); err != nil {
					p.Release()
					_ = conn.Close()
					return nil, fmt.Errorf("mqttv5: server authentication verification failed: %w", err)
				}
			}
			aliasMax, _ := p.Properties.Uint16(wire.PropTopicAliasMaximum)
			// CONNACK capability flags. Absent property means feature
			// is supported, per MQTT v5 §3.2.2.3.{11,12,13}.
			sharedAvail := connackCapability(p.Properties, wire.PropSharedSubAvailable)
			wildcardAvail := connackCapability(p.Properties, wire.PropWildcardSubAvailable)
			subIDsAvail := connackCapability(p.Properties, wire.PropSubscriptionIDAvailable)
			// Cache AssignedClientIdentifier (§3.2.2.3.7) so ClientID()
			// reports the broker-assigned value when the caller passed
			// WithClientID("").
			if assigned, ok := p.Properties.String(wire.PropAssignedClientID); ok && assigned != "" {
				s := strings.Clone(assigned)
				c.assignedClientID.Store(&s)
			}
			// Clone before Release — the OnConnectionUp callback may
			// retain the CONNACK indefinitely.
			var connackClone *wire.Connack
			if c.cfg.OnConnectionUp != nil {
				connackClone = p.Clone()
			}
			p.Release()
			_ = conn.SetDeadline(time.Time{})
			return &connectResult{
				conn:              conn,
				decoder:           dec,
				brokerAliasMax:    aliasMax,
				sharedSubsAvail:   sharedAvail,
				wildcardSubsAvail: wildcardAvail,
				subIDsAvail:       subIDsAvail,
				connack:           connackClone,
			}, nil
		default:
			pkt.Release()
			_ = conn.Close()
			return nil, fmt.Errorf("%w: expected CONNACK or AUTH, got %s",
				ErrUnexpectedPacket, pkt.Type())
		}
	}
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

// runConnection installs a freshly-handshaken transport, starts the
// read/write/ping goroutines, and replays + resubscribes any pending
// state. Each call allocates a fresh connState.
func (c *Client) runConnection(r *connectResult) {
	cs := &connState{
		conn:              r.conn,
		decoder:           r.decoder,
		writeQueue:        make(chan writeReq, c.cfg.WriteQueueSize),
		dying:             make(chan struct{}),
		done:              make(chan struct{}),
		aliasMap:          make(map[uint16]string),
		outAliasMap:       make(map[string]uint16),
		outAliasMax:       r.brokerAliasMax,
		sharedSubsAvail:   r.sharedSubsAvail,
		wildcardSubsAvail: r.wildcardSubsAvail,
		subIDsAvail:       r.subIDsAvail,
	}
	// Seed activity timestamps so the first PINGREQ fires KeepAlive
	// seconds after handshake, not immediately.
	now := time.Now().UnixNano()
	cs.lastWriteUnixNano.Store(now)
	cs.lastReadUnixNano.Store(now)
	c.cur.Store(cs)

	cs.wg.Add(2)
	go c.readLoop(cs)
	go c.writeLoop(cs)
	if c.cfg.KeepAlive > 0 {
		cs.wg.Add(1)
		go c.pingLoop(cs)
	}

	go func() {
		cs.wg.Wait()
		cs.doneOnce.Do(func() { close(cs.done) })
	}()

	c.replayPending(cs)
	c.resubscribeAll(cs)

	c.stats.addConnect()
	if c.cfg.OnConnectionUp != nil {
		c.cfg.OnConnectionUp(r.connack)
	}
	c.cfg.Logger.Info("mqttv5: connected",
		slog.String("broker", c.currentBrokerURL()),
		slog.String("client_id", c.cfg.ClientID),
	)
}

// supervisor watches the current connState's done channel and
// reconnects (with backoff) until Disconnect is called. Each reconnect
// attempt advances brokerIdx so multi-broker URL lists rotate through
// healthy endpoints; a successful connect leaves the index parked on
// whichever URL accepted us.
func (c *Client) supervisor() {
	defer c.supWg.Done()
	for {
		cs := c.cur.Load()
		if cs == nil {
			return
		}

		// Wait for the current connection to drop or the client to
		// shut down.
		select {
		case <-cs.done:
		case <-c.shutdown:
			cs.signalDown()
			cs.wg.Wait()
			return
		}

		// Connection has dropped. Clear current pointer + notify.
		c.cur.Store(nil)
		c.stats.addDisconnect()
		if sd := cs.serverDisconnect.Load(); sd != nil {
			c.stats.addServerDisconnect()
			if c.cfg.OnServerDisconnect != nil {
				c.cfg.OnServerDisconnect(sd)
			}
		}
		c.cfg.Logger.Warn("mqttv5: connection lost",
			slog.String("broker", c.currentBrokerURL()),
		)
		// OnConnectionDown's bool return decides whether to keep
		// retrying. Nil callback defaults to "keep going".
		keepRetrying := true
		if c.cfg.OnConnectionDown != nil {
			keepRetrying = c.cfg.OnConnectionDown()
		}
		if !keepRetrying {
			// Caller wants no further reconnect attempts. Tear down
			// cleanly so a subsequent Connect() restarts the
			// lifecycle without dangling state.
			c.closeOnce.Do(func() { close(c.shutdown) })
			c.startMu.Lock()
			c.started = false
			c.startMu.Unlock()
			return
		}

		// Inner retry loop: keeps advancing brokerIdx + sleeping +
		// dialling until we either succeed (break to outer loop and
		// resume waiting on the new cs.done) or are told to shut down.
		attempt := 0
		for {
			select {
			case <-c.shutdown:
				return
			default:
			}

			delay := c.cfg.ReconnectBackoff(attempt)
			select {
			case <-time.After(delay):
			case <-c.shutdown:
				return
			}

			// Advance the URL pointer for this attempt. The first
			// attempt after a drop already moves off the URL that just
			// failed — fail-fast over a known-bad endpoint.
			c.brokerIdx.Add(1)
			attempt++

			brokerURL := c.currentBrokerURL()
			if c.cfg.OnReconnectAttempt != nil {
				c.cfg.OnReconnectAttempt(attempt, brokerURL)
			}

			ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ConnectTimeout)
			result, err := c.connectOnce(ctx, true)
			cancel()
			if err != nil {
				c.stats.addConnectFailure()
				if c.cfg.OnConnectError != nil {
					c.cfg.OnConnectError(err)
				}
				c.cfg.Logger.Warn("mqttv5: reconnect failed",
					slog.String("broker", brokerURL),
					slog.Int("attempt", attempt),
					slog.Any("error", err),
				)
				continue
			}
			c.runConnection(result)
			break
		}
	}
}

// sendDisconnectWith best-effort writes a graceful DISCONNECT carrying
// opts. cs.dying short-circuits the wait if the connection is going
// down before the writer drains; combined with drainWriteQueue in
// writeLoop's deferred exit, req.done is always signalled.
func (c *Client) sendDisconnectWith(ctx context.Context, opts wire.DisconnectOpts) {
	cs := c.cur.Load()
	if cs == nil {
		return
	}
	done := make(chan error, 1)
	select {
	case cs.writeQueue <- writeReq{
		fn: func(w io.Writer) (int64, error) {
			return wire.WriteDisconnect(w, opts)
		},
		done: done,
	}:
		select {
		case <-done:
		case <-ctx.Done():
		case <-cs.dying:
		}
	default:
	}
}

// writeBatchHardCap caps any user-supplied WithWriteBatch(n) to avoid
// runaway iovec sizes. 64 is more than enough for any realistic
// concurrent-publisher workload (writev iovec limit is system-defined
// but typically 1024; we stay well below).
const writeBatchHardCap = 64

// writeBatch accumulates pre-encoded packets to coalesce into one
// net.Buffers.WriteTo (= one writev syscall on *net.TCPConn). Lives
// on the writer goroutine's stack frame and reuses the same backing
// arrays across batches — zero alloc per batch. cap is set at
// construction time from Config.WriteBatchMax (clamped to
// writeBatchHardCap).
type writeBatch struct {
	pkts  []*[]byte
	dones []chan<- error
	bufs  net.Buffers
	n     int
	cap   int
}

// newWriteBatch sizes a batch for cap packets. cap=1 disables
// coalescing (every flush goes straight to conn.Write).
func newWriteBatch(cap int) writeBatch {
	if cap < 1 {
		cap = 1
	} else if cap > writeBatchHardCap {
		cap = writeBatchHardCap
	}
	return writeBatch{
		pkts:  make([]*[]byte, cap),
		dones: make([]chan<- error, cap),
		cap:   cap,
	}
}

// add appends a pkt-mode req to the batch. Caller guarantees req.pkt
// is non-nil and n < writeBatchMax.
func (b *writeBatch) add(req writeReq) {
	b.pkts[b.n] = req.pkt
	b.dones[b.n] = req.done
	b.n++
}

// flush coalesces every queued pkt into one writev. Updates
// lastWriteUnixNano on success, releases every buffer, and signals
// every done channel (with err for failures, nil for success). Resets
// the batch to empty.
//
// Batch-of-1 takes the plain conn.Write path — net.Buffers.WriteTo
// always allocates an iovec slice on darwin/linux (one syscall.Iovec
// per buffer), which is wasted overhead when there's nothing to
// coalesce. Single Write skips it entirely and stays 0-alloc.
func (b *writeBatch) flush(cs *connState) error {
	if b.n == 0 {
		return nil
	}
	var err error
	if b.n == 1 {
		_, err = cs.conn.Write(*b.pkts[0])
	} else {
		b.bufs = b.bufs[:0]
		for i := 0; i < b.n; i++ {
			b.bufs = append(b.bufs, *b.pkts[i])
		}
		_, err = b.bufs.WriteTo(cs.conn)
	}
	if err == nil {
		cs.lastWriteUnixNano.Store(time.Now().UnixNano())
	}
	for i := 0; i < b.n; i++ {
		wire.ReleaseBuf(b.pkts[i])
		if b.dones[i] != nil {
			b.dones[i] <- err
		}
		b.pkts[i] = nil
		b.dones[i] = nil
	}
	b.n = 0
	return err
}

// writeLoop drains the write queue for one connection. Exits on
// shutdown, dying, or write error. On any exit path it drains any
// remaining queued requests with ErrNotConnected so callers waiting
// on req.done never orphan.
//
// Batching: pkt-mode reqs (zero-alloc Publish path) coalesce into
// one writev syscall via writeBatch. fn-mode reqs (CONNECT,
// SUBSCRIBE, QoS 1/2 replays, …) flush any pending batch first and
// then run their own write. Channel FIFO order is preserved across
// the boundary.
func (c *Client) writeLoop(cs *connState) {
	defer cs.wg.Done()
	defer drainWriteQueue(cs.writeQueue)
	if c.cfg.WriteBatchMax > 1 {
		c.writeLoopBatched(cs)
		return
	}
	c.writeLoopDirect(cs)
}

// writeLoopDirect is the default zero-coalescing writer: one
// conn.Write (or req.fn) per dequeued req. Lowest per-call overhead
// for single-producer / small-payload workloads.
func (c *Client) writeLoopDirect(cs *connState) {
	for {
		select {
		case req, ok := <-cs.writeQueue:
			if !ok {
				return
			}
			var err error
			if req.pkt != nil {
				_, err = cs.conn.Write(*req.pkt)
				wire.ReleaseBuf(req.pkt)
			} else if req.fn != nil {
				_, err = req.fn(cs.conn)
			}
			if err == nil {
				cs.lastWriteUnixNano.Store(time.Now().UnixNano())
			}
			if req.done != nil {
				req.done <- err
			}
			if err != nil {
				c.handleConnError(cs, err)
				return
			}
		case <-cs.dying:
			return
		case <-c.shutdown:
			return
		}
	}
}

// writeLoopBatched coalesces up to Config.WriteBatchMax pre-encoded
// packets per writev. Used when WithWriteBatch(n) has been set.
func (c *Client) writeLoopBatched(cs *connState) {
	batch := newWriteBatch(c.cfg.WriteBatchMax)
	for {
		select {
		case req, ok := <-cs.writeQueue:
			if !ok {
				return
			}
			if err := c.processWrite(cs, &batch, req); err != nil {
				c.handleConnError(cs, err)
				return
			}
		case <-cs.dying:
			return
		case <-c.shutdown:
			return
		}
	}
}

// processWrite handles one writeReq received from the queue. For
// pkt-mode reqs it drains any further pkt-mode reqs into the batch
// (bounded by writeBatchMax) and flushes once via writev. For
// fn-mode reqs it flushes any pending batch first, then invokes
// req.fn directly.
func (c *Client) processWrite(cs *connState, batch *writeBatch, req writeReq) error {
	if req.fn != nil {
		if err := batch.flush(cs); err != nil {
			if req.done != nil {
				req.done <- err
			}
			return err
		}
		_, err := req.fn(cs.conn)
		if err == nil {
			cs.lastWriteUnixNano.Store(time.Now().UnixNano())
		}
		if req.done != nil {
			req.done <- err
		}
		return err
	}

	// pkt-mode: add + try to drain more without blocking.
	batch.add(req)
	for batch.n < batch.cap {
		select {
		case more, ok := <-cs.writeQueue:
			if !ok {
				return batch.flush(cs)
			}
			if more.fn != nil {
				if err := batch.flush(cs); err != nil {
					if more.done != nil {
						more.done <- err
					}
					return err
				}
				_, err := more.fn(cs.conn)
				if err == nil {
					cs.lastWriteUnixNano.Store(time.Now().UnixNano())
				}
				if more.done != nil {
					more.done <- err
				}
				return err
			}
			batch.add(more)
		default:
			return batch.flush(cs)
		}
	}
	return batch.flush(cs)
}

// drainWriteQueue empties any buffered writeReqs without writing them
// to the network and signals each req.done with ErrNotConnected so
// blocked senders unblock. Called only from writeLoop's deferred exit
// path, so no further producers race with the drain.
func drainWriteQueue(q chan writeReq) {
	for {
		select {
		case req := <-q:
			if req.pkt != nil {
				wire.ReleaseBuf(req.pkt)
			}
			if req.done != nil {
				select {
				case req.done <- ErrNotConnected:
				default:
				}
			}
		default:
			return
		}
	}
}

// readLoop drives the decoder for one connection.
func (c *Client) readLoop(cs *connState) {
	defer cs.wg.Done()
	for {
		pkt, err := cs.decoder.ReadPacket()
		if err != nil {
			c.handleConnError(cs, err)
			return
		}
		cs.lastReadUnixNano.Store(time.Now().UnixNano())
		c.dispatch(cs, pkt)
	}
}

// pingLoop emits PINGREQ to satisfy MQTT v5 §3.1.2.10 and to detect a
// dead broker. Two windows are tracked: outbound (lastWriteUnixNano,
// resets the broker's disconnect timer) and inbound (lastReadUnixNano,
// confirms broker liveness). PINGREQ fires only when one of the
// windows has been quiet for KeepAlive. Busy bi-directional
// connections skip PINGREQ entirely; pure-QoS-0 publishers still pay
// the keepalive cost (no inbound traffic to confirm broker liveness).
func (c *Client) pingLoop(cs *connState) {
	defer cs.wg.Done()
	keep := time.Duration(c.cfg.KeepAlive) * time.Second

	for {
		// Sleep until at least one window would expire, then re-check.
		wait := nextPingWait(cs, keep, time.Now())
		select {
		case <-time.After(wait):
		case <-cs.dying:
			return
		case <-c.shutdown:
			return
		}

		now := time.Now()
		sendRecent := now.Sub(time.Unix(0, cs.lastWriteUnixNano.Load())) < keep
		recvRecent := now.Sub(time.Unix(0, cs.lastReadUnixNano.Load())) < keep
		if sendRecent && recvRecent {
			// Both windows fresh — no PINGREQ needed this cycle.
			continue
		}

		c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
			return wire.WritePingreq(w)
		})
		cs.pingSent.Store(time.Now().UnixNano())

		select {
		case <-time.After(c.cfg.PingTimeout):
			// Liveness = "did the broker send us ANYTHING since we
			// PINGREQ'd?", not "did the PINGRESP specifically land?".
			// The PINGRESP shares the single ordered stream with inbound
			// PUBLISHes, and the readLoop dispatches serially, so under a
			// sustained inbound flood a PINGRESP sits behind a deep read
			// backlog and pingResp lags past PingTimeout — even though
			// bytes are actively arriving. A fresh inbound read window
			// (lastRead advanced past pingSent) is DEFINITIVE proof the
			// broker is alive, so treat it as a successful liveness check
			// and skip the reconnect. Without this a busy subscriber
			// tears down its own healthy connection under load and drops
			// its subscription window (estavelle RFC 0009 forward-ceiling
			// forensics: this presented as ~20% cross-node QoS 0 "loss").
			// A genuinely half-open broker sends no bytes at all, so
			// lastRead stays stale and the timeout still fires.
			pingSent := cs.pingSent.Load()
			if cs.pingResp.Load() < pingSent && cs.lastReadUnixNano.Load() < pingSent {
				c.cfg.Logger.Warn("mqttv5: PINGRESP timeout",
					slog.Duration("after", c.cfg.PingTimeout),
				)
				c.stats.addPingTimeout()
				c.handleConnError(cs, errors.New("PINGRESP timeout"))
				return
			}
		case <-cs.dying:
			return
		case <-c.shutdown:
			return
		}
	}
}

// nextPingWait returns min(remainingSendWindow, remainingRecvWindow),
// clamped to [1ms, keep]. Snapshot-and-poll, not timer-reset:
// activity during the sleep only extends deadlines, never shortens
// them, so a stale snapshot can wake us early (re-check + re-sleep)
// but never miss a needed PINGREQ. We pay one wasted goroutine wake
// per KeepAlive on a busy connection in exchange for zero
// coordination on the hot write/read path.
func nextPingWait(cs *connState, keep time.Duration, now time.Time) time.Duration {
	lastWrite := time.Unix(0, cs.lastWriteUnixNano.Load())
	lastRead := time.Unix(0, cs.lastReadUnixNano.Load())
	sendDeadline := lastWrite.Add(keep)
	recvDeadline := lastRead.Add(keep)
	next := sendDeadline
	if recvDeadline.Before(next) {
		next = recvDeadline
	}
	wait := next.Sub(now)
	if wait < 0 {
		// Both windows already expired — fire immediately, but yield
		// at least one scheduling tick so this doesn't hot-loop if
		// the writer is wedged.
		return time.Millisecond
	}
	if wait > keep {
		wait = keep
	}
	return wait
}

// handleConnError is the per-connection error handler. The first
// caller wins via cs.signalDown's Once.
func (c *Client) handleConnError(cs *connState, err error) {
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
		c.cfg.Logger.Warn("mqttv5: connection error", slog.Any("error", err))
	}
	cs.signalDown()
}

// dispatch routes one decoded packet for the given connection.
func (c *Client) dispatch(cs *connState, pkt wire.Packet) {
	switch p := pkt.(type) {
	case *wire.Publish:
		c.handlePublish(cs, p)
	case *wire.PubResp:
		c.handlePubResp(cs, p)
	case *wire.Suback:
		c.deliverControlAck(p.PacketID, pkt)
	case *wire.Unsuback:
		c.deliverControlAck(p.PacketID, pkt)
	case *wire.Pingresp:
		cs.pingResp.Store(time.Now().UnixNano())
		pkt.Release()
	case *wire.Auth:
		c.handleServerAuth(cs, p)
	case *wire.Disconnect:
		// Always retain the reason: the supervisor uses it for the
		// ServerDisconnects stat and OnServerDisconnect, and
		// Reauthenticate uses it to surface ErrReauthRejected with the
		// broker's code. Cheap clone on a rare event.
		cs.serverDisconnect.Store(p.Clone())
		c.cfg.Logger.Warn("mqttv5: broker DISCONNECT", slog.Int("reason", int(p.ReasonCode)))
		pkt.Release()
		cs.signalDown()
	default:
		c.cfg.Logger.Warn("mqttv5: unexpected packet type", slog.String("type", pkt.Type().String()))
		pkt.Release()
	}
}

// writeDisconnectAndClose synchronously writes a DISCONNECT with the
// given opts, waits briefly for the write to flush, then signals the
// connection down. Used by error paths that need the broker to see
// our reason code before the socket disappears (e.g. mid-session
// auth failure).
func (c *Client) writeDisconnectAndClose(cs *connState, opts wire.DisconnectOpts) {
	done := make(chan error, 1)
	req := writeReq{
		fn: func(w io.Writer) (int64, error) {
			return wire.WriteDisconnect(w, opts)
		},
		done: done,
	}
	select {
	case cs.writeQueue <- req:
		select {
		case <-done:
		case <-time.After(c.cfg.DisconnectFlushTimeout):
		case <-cs.dying:
		}
	default:
		// Write queue full / writer dead — best-effort only.
		c.cfg.Logger.Debug("mqttv5: DISCONNECT dropped (writer wedged)",
			slog.Int("reason", int(opts.ReasonCode)))
	}
	cs.signalDown()
}

// handleServerAuth processes an AUTH packet that arrived outside the
// CONNECT handshake (MQTT v5 §4.12 enhanced authentication).
//
// Per §3.15.2.1 the AUTH reason codes have fixed senders: 0x00 Success
// is server-only, 0x19 Re-authentication is client-only, 0x18 Continue
// is either. A conformant broker therefore never spontaneously starts a
// mid-session exchange — it only replies within one the client started
// (0x18 / 0x00) or sends DISCONNECT. This handler services that reply
// and, defensively, any unsolicited mid-session AUTH a broker sends.
//
// Behaviour:
//   - inbound 0x00 Success: terminal — the client does not reply and
//     does not call Continue (only the server concludes the exchange).
//   - otherwise (0x18 Continue): feed brokerData to Authenticator.Continue
//     and reply 0x18. The Continue done flag is informational; the client
//     never emits 0x00. This mirrors the in-CONNECT loop in connectOnce.
//
// Failure modes (all log + close the connection so the supervisor
// reconnects through the normal CONNECT path with fresh credentials):
//   - no Authenticator configured: emit DISCONNECT 0x87
//   - Authenticator.Continue returns error: emit DISCONNECT 0x87
//   - write of the AUTH response fails: signalDown
func (c *Client) handleServerAuth(cs *connState, p *wire.Auth) {
	// p.Properties is a view over the frame buffer; copy out before
	// Release recycles the frame, otherwise the Authenticator could
	// read bytes the broker has since overwritten.
	rawData, _ := p.Properties.Binary(wire.PropAuthData)
	brokerData := append([]byte(nil), rawData...)
	pktReason := p.ReasonCode
	p.Release()

	// A client-initiated re-authentication (Reauthenticate) takes
	// precedence: route the broker's reply to the in-flight waiter
	// rather than the defensive broker-driven path below.
	if call := cs.reauth.Load(); call != nil {
		c.handleReauthInbound(cs, call, pktReason, brokerData)
		return
	}

	if c.cfg.Authenticator == nil {
		c.cfg.Logger.Warn("mqttv5: broker sent AUTH mid-session but no Authenticator configured",
			slog.Int("reason", int(pktReason)),
		)
		c.writeDisconnectAndClose(cs, wire.DisconnectOpts{
			ReasonCode:   wire.ReasonNotAuthorized,
			ReasonString: "no Authenticator configured for mid-session re-auth",
		})
		return
	}

	// 0x00 Success concludes the exchange. The client must not respond
	// and must not re-enter the Authenticator — replying here would emit
	// an unsolicited extra AUTH (and a client-sent 0x00 is itself a spec
	// violation, §3.15.2.1).
	if pktReason == wire.ReasonSuccess {
		if v, ok := c.cfg.Authenticator.(ServerFinalVerifier); ok {
			if err := v.VerifyServerFinal(brokerData); err != nil {
				c.cfg.Logger.Warn("mqttv5: server re-authentication verification failed",
					slog.Any("error", err),
				)
				c.writeDisconnectAndClose(cs, wire.DisconnectOpts{ReasonCode: wire.ReasonNotAuthorized})
				return
			}
		}
		c.cfg.Logger.Debug("mqttv5: broker concluded mid-session re-auth (AUTH 0x00 Success)")
		c.fireReauthenticated()
		return
	}

	// The broker is continuing the exchange (0x18). Run the Authenticator
	// and always reply 0x18 Continue — only the server concludes (0x00),
	// so done is discarded here just as in connectOnce.
	response, _, err := c.cfg.Authenticator.Continue(brokerData)
	if err != nil {
		c.cfg.Logger.Warn("mqttv5: Authenticator.Continue failed during mid-session re-auth",
			slog.Any("error", err),
		)
		c.writeDisconnectAndClose(cs, wire.DisconnectOpts{
			ReasonCode: wire.ReasonNotAuthorized,
		})
		return
	}

	c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
		return wire.WriteAuth(w, wire.AuthOpts{
			ReasonCode:           wire.ReasonContinueAuthentication,
			AuthenticationMethod: c.cfg.Authenticator.Method(),
			AuthenticationData:   response,
		})
	})
}

// reauthCall tracks one in-flight client-initiated re-authentication
// (§4.12). result delivers the terminal outcome to the Reauthenticate
// caller: nil on AUTH 0x00 Success, or a non-nil error if the
// Authenticator fails mid-exchange. It is buffered (cap 1) so the read
// loop never blocks delivering the outcome even when the caller has
// stopped waiting (ctx elapsed). Broker rejection (DISCONNECT) and plain
// drops are reported via cs.dying, not result.
type reauthCall struct {
	result chan error
}

// Reauthenticate initiates MQTT v5 re-authentication (§4.12) on the live
// connection: it sends an AUTH 0x19 carrying the configured
// Authenticator's Method() and a fresh Begin(ctx) payload, services any
// broker 0x18 Continue challenges via Authenticator.Continue, and returns
// when the broker concludes the exchange — without dropping the
// connection or disturbing in-flight QoS state.
//
// It returns nil when the broker accepts re-authentication (AUTH 0x00
// Success), or:
//   - ErrNoAuthenticator if the client was not built WithAuthenticator;
//   - ErrNotConnected if there is no live connection (before the first
//     CONNACK, or after a drop);
//   - ErrReauthInProgress if another re-auth is already in flight on this
//     connection (calls are single-flighted per connection);
//   - ErrReauthRejected (wrapped with the DISCONNECT reason code) if the
//     broker rejects re-auth and tears the connection down;
//   - ctx.Err() if ctx is cancelled or its deadline elapses first.
//
// The AuthenticationMethod established at CONNECT is reused; Method() MUST
// be stable for the client's lifetime (§4.12 requires it to match across
// the exchange). Reauthenticate is safe for concurrent use.
//
// ctx bounds the whole operation including credential resolution via
// Begin(ctx). On ctx cancellation after the AUTH 0x19 has been sent the
// connection is left intact and the exchange resolves on the wire — re-auth
// stays single-flighted until the broker concludes, so a subsequent call
// may observe ErrReauthInProgress until then. On a broker rejection the
// connection is torn down and the supervisor reconnects through the normal
// CONNECT path (which re-presents credentials).
func (c *Client) Reauthenticate(ctx context.Context) error {
	if c.cfg.Authenticator == nil {
		return ErrNoAuthenticator
	}
	cs := c.cur.Load()
	if cs == nil {
		return ErrNotConnected
	}

	call := &reauthCall{result: make(chan error, 1)}
	if !cs.reauth.CompareAndSwap(nil, call) {
		return ErrReauthInProgress
	}

	// Resolve a fresh credential. ctx bounds any I/O Begin performs.
	// Nothing is on the wire yet, so a failure here releases the slot.
	data, err := c.cfg.Authenticator.Begin(ctx)
	if err != nil {
		cs.reauth.CompareAndSwap(call, nil)
		return fmt.Errorf("mqttv5: Authenticator.Begin: %w", err)
	}
	method := c.cfg.Authenticator.Method()

	// Send AUTH 0x19 to initiate. Until it is enqueued nothing is on the
	// wire, so the ctx/dying/shutdown exits may still release the slot.
	authFn := func(w io.Writer) (int64, error) {
		return wire.WriteAuth(w, wire.AuthOpts{
			ReasonCode:           wire.ReasonReAuthenticate,
			AuthenticationMethod: method,
			AuthenticationData:   data,
		})
	}
	select {
	case cs.writeQueue <- writeReq{fn: authFn}:
	case <-ctx.Done():
		cs.reauth.CompareAndSwap(call, nil)
		return ctx.Err()
	case <-cs.dying:
		cs.reauth.CompareAndSwap(call, nil)
		return ErrNotConnected
	case <-c.shutdown:
		cs.reauth.CompareAndSwap(call, nil)
		return ErrClosed
	}

	// The 0x19 is queued and will flush. The read loop now owns the
	// exchange: it answers 0x18 challenges and delivers the terminal
	// outcome on call.result. From here the slot is released only by the
	// read loop (on 0x00) or by this connState being disposed on teardown
	// — never on ctx cancel — so a late terminal AUTH cannot land on a
	// subsequent Reauthenticate.
	select {
	case err := <-call.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		// A delivered result (e.g. a server-verification failure that
		// also tore the connection down) takes precedence over the
		// generic down error when both are ready.
		select {
		case err := <-call.result:
			return err
		default:
			return c.reauthDownError(cs)
		}
	case <-c.shutdown:
		return ErrClosed
	}
}

// reauthDownError maps a connection teardown observed mid-re-auth to the
// right error: a broker DISCONNECT (rejection) surfaces as
// ErrReauthRejected with its reason code; a plain socket drop surfaces as
// ErrNotConnected.
func (c *Client) reauthDownError(cs *connState) error {
	if sd := cs.serverDisconnect.Load(); sd != nil {
		return fmt.Errorf("%w: reason 0x%02X", ErrReauthRejected, byte(sd.ReasonCode))
	}
	return ErrNotConnected
}

// fireReauthenticated invokes the OnReauthenticated observability hook (if
// configured) on the read-loop goroutine. The callback must not block.
func (c *Client) fireReauthenticated() {
	if c.cfg.OnReauthenticated != nil {
		c.cfg.OnReauthenticated()
	}
}

// handleReauthInbound services a broker AUTH that arrived while a
// client-initiated re-auth is in flight (slot held by call). Runs on the
// read-loop goroutine.
//
//   - 0x00 Success: terminal — release the slot and signal the waiter.
//   - otherwise (0x18 Continue): feed the Authenticator and reply 0x18,
//     keeping the slot so the exchange continues. A Continue error tears
//     the connection down; the waiter then observes the drop via cs.dying.
func (c *Client) handleReauthInbound(cs *connState, call *reauthCall, pktReason wire.ReasonCode, brokerData []byte) {
	if pktReason == wire.ReasonSuccess {
		cs.reauth.CompareAndSwap(call, nil)
		// Mutual verification (§4.12): if the Authenticator verifies the
		// server's final data, a failure means the broker is not trusted
		// — report it to the caller and tear the connection down.
		if v, ok := c.cfg.Authenticator.(ServerFinalVerifier); ok {
			if err := v.VerifyServerFinal(brokerData); err != nil {
				call.result <- fmt.Errorf("mqttv5: server re-authentication verification failed: %w", err)
				c.writeDisconnectAndClose(cs, wire.DisconnectOpts{ReasonCode: wire.ReasonNotAuthorized})
				return
			}
		}
		c.fireReauthenticated()
		call.result <- nil
		return
	}

	response, _, err := c.cfg.Authenticator.Continue(brokerData)
	if err != nil {
		c.cfg.Logger.Warn("mqttv5: Authenticator.Continue failed during re-authentication",
			slog.Any("error", err),
		)
		// Tear down — the waiter wakes on cs.dying and reports the drop.
		// The slot rides down with this disposed connState.
		c.writeDisconnectAndClose(cs, wire.DisconnectOpts{
			ReasonCode: wire.ReasonNotAuthorized,
		})
		return
	}

	c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
		return wire.WriteAuth(w, wire.AuthOpts{
			ReasonCode:           wire.ReasonContinueAuthentication,
			AuthenticationMethod: c.cfg.Authenticator.Method(),
			AuthenticationData:   response,
		})
	})
}

// handlePublish dispatches an inbound PUBLISH to every matching
// handler. Handlers share one *Message; Message.Ack refcounts down
// to the final caller, which dispatches the broker-side ack and
// releases the underlying wire.Publish. If nothing matches, we ack
// here so the broker doesn't retry forever.
func (c *Client) handlePublish(cs *connState, pub *wire.Publish) {
	c.stats.addInboundPublish()
	// Resolve topic aliases per §3.3.2.3.4 before anything else —
	// the trie matcher needs the real topic, and any subscriber
	// reading msg.Topic expects the substituted value.
	if !c.resolveTopicAlias(cs, pub) {
		pub.Release()
		return
	}

	if pub.QoS > 0 {
		if !c.session.Inbound.RegisterPublish(pub.PacketID, pub.QoS) {
			// Duplicate retransmit; re-emit any pending acks and drop.
			c.flushPendingAcks(cs, pub.PacketID)
			pub.Release()
			return
		}
	}

	msg := acquireMessage(pub, c, cs)

	// Single-match fast path: stack-local handler, no slice. Multi-match
	// falls through to overflow. refs MUST be set before any handler
	// runs — a sync handler that Acks immediately could otherwise
	// drive refs below zero while we're still iterating.
	var (
		only     HandlerFunc
		overflow []HandlerFunc
		matched  int
	)
	c.router.Load().Match(pub.Topic, func(h trie.Handler) {
		fn := h.(HandlerFunc)
		matched++
		if matched == 1 {
			only = fn
		} else {
			overflow = append(overflow, fn)
		}
	})

	switch matched {
	case 0:
		// No subscribers — ack + release so we don't pin frame memory.
		msg.refs.Store(1)
		_ = msg.Ack()
	case 1:
		msg.refs.Store(1)
		only(msg)
	default:
		msg.refs.Store(int32(matched))
		only(msg)
		for _, h := range overflow {
			h(msg)
		}
	}
}

// resolveTopicAlias inspects the Topic Alias property and either
// registers a new alias (Topic non-empty) or substitutes Topic from
// the cache (Topic empty). Returns false to indicate the publish
// should be dropped (unknown alias — protocol violation by the
// broker, but we don't kill the connection over it).
func (c *Client) resolveTopicAlias(cs *connState, pub *wire.Publish) bool {
	alias, ok := pub.Properties.Uint16(wire.PropTopicAlias)
	if !ok || alias == 0 {
		return true
	}
	if pub.Topic != "" {
		// Server is registering / updating the alias. Clone so the
		// cached string outlives the frame.
		topic := strings.Clone(pub.Topic)
		cs.aliasMu.Lock()
		cs.aliasMap[alias] = topic
		cs.aliasMu.Unlock()
		return true
	}
	// Empty Topic + Alias = look up cached value.
	cs.aliasMu.RLock()
	topic, ok := cs.aliasMap[alias]
	cs.aliasMu.RUnlock()
	if !ok {
		c.cfg.Logger.Warn("mqttv5: PUBLISH with unknown topic alias",
			slog.Int("alias", int(alias)),
		)
		return false
	}
	pub.Topic = topic
	return true
}

func (c *Client) flushPendingAcks(cs *connState, id uint16) {
	for _, send := range c.session.Inbound.AckPublish(id) {
		c.emitAck(cs, send)
	}
}

func (c *Client) emitAck(cs *connState, send session.AckSend) {
	switch send.Type {
	case wire.PUBACK:
		c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
			return wire.WritePuback(w, wire.PubRespOpts{PacketID: send.PacketID})
		})
	case wire.PUBREC:
		c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
			return wire.WritePubrec(w, wire.PubRespOpts{PacketID: send.PacketID})
		})
	}
}

// handlePubResp routes PUBACK/PUBREC/PUBREL/PUBCOMP to the session.
func (c *Client) handlePubResp(cs *connState, p *wire.PubResp) {
	defer p.Release()
	switch p.Type() {
	case wire.PUBACK:
		_ = c.session.Outbound.HandlePubAck(p.PacketID, p.ReasonCode)
		c.session.ReleaseID(p.PacketID)
		c.stats.addPublishAcked()
	case wire.PUBREC:
		sendPubrel, err := c.session.Outbound.HandlePubRec(p.PacketID, p.ReasonCode)
		if err != nil || !sendPubrel {
			c.session.ReleaseID(p.PacketID)
			return
		}
		id := p.PacketID
		c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
			return wire.WritePubrel(w, wire.PubRespOpts{PacketID: id})
		})
	case wire.PUBREL:
		if c.session.Inbound.HandlePubRel(p.PacketID) {
			id := p.PacketID
			c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
				return wire.WritePubcomp(w, wire.PubRespOpts{PacketID: id})
			})
		}
	case wire.PUBCOMP:
		_ = c.session.Outbound.HandlePubComp(p.PacketID, p.ReasonCode)
		c.session.ReleaseID(p.PacketID)
		c.stats.addPublishAcked()
	}
}

// deliverControlAck hands a SUBACK / UNSUBACK to whichever caller is
// blocked on it.
func (c *Client) deliverControlAck(id uint16, pkt wire.Packet) {
	c.ctrlMu.Lock()
	ch, ok := c.ctrlWaiters[id]
	if ok {
		delete(c.ctrlWaiters, id)
	}
	c.ctrlMu.Unlock()
	if !ok {
		pkt.Release()
		return
	}
	ch <- pkt
	// Receiver Releases the packet.
}

// enqueueFireAndForget pushes a write without waiting. Drops the write
// if the connection is dying.
func (c *Client) enqueueFireAndForget(cs *connState, fn writeFn) {
	select {
	case cs.writeQueue <- writeReq{fn: fn}:
	case <-cs.dying:
	case <-c.shutdown:
	}
}

// enqueueAwait pushes a write op and blocks until completion (or
// ctx/shutdown/dying fires).
//
// The "cs" arg can be nil — Publish callers want to use the current
// connection rather than capture one at call-site. If cs is nil we
// load c.cur.
func (c *Client) enqueueAwait(ctx context.Context, fn writeFn) error {
	cs := c.cur.Load()
	if cs == nil {
		return ErrNotConnected
	}
	done := make(chan error, 1)
	select {
	case cs.writeQueue <- writeReq{fn: fn, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		return ErrNotConnected
	case <-c.shutdown:
		return ErrClosed
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		return ErrNotConnected
	case <-c.shutdown:
		return ErrClosed
	}
}

// enqueueAwaitPkt is the pre-encoded-bytes variant of enqueueAwait.
// On every exit path (success or error) the buffer is released; the
// caller must NOT touch it after this returns.
func (c *Client) enqueueAwaitPkt(ctx context.Context, pkt *[]byte) error {
	cs := c.cur.Load()
	if cs == nil {
		wire.ReleaseBuf(pkt)
		return ErrNotConnected
	}
	done := make(chan error, 1)
	select {
	case cs.writeQueue <- writeReq{pkt: pkt, done: done}:
		// Writer owns pkt now; it will ReleaseBuf after Write.
	case <-ctx.Done():
		wire.ReleaseBuf(pkt)
		return ctx.Err()
	case <-cs.dying:
		wire.ReleaseBuf(pkt)
		return ErrNotConnected
	case <-c.shutdown:
		wire.ReleaseBuf(pkt)
		return ErrClosed
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		return ErrNotConnected
	case <-c.shutdown:
		return ErrClosed
	}
}

// enqueueAwaitOn writes on a specific connection (used by resubscribe
// which captures the connState explicitly).
func (c *Client) enqueueAwaitOn(ctx context.Context, cs *connState, fn writeFn) error {
	done := make(chan error, 1)
	select {
	case cs.writeQueue <- writeReq{fn: fn, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		return ErrNotConnected
	case <-c.shutdown:
		return ErrClosed
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		return ErrNotConnected
	case <-c.shutdown:
		return ErrClosed
	}
}

// replayPending re-emits every in-flight outbound publish from the
// session, setting DUP=1 per §3.3.1.1.
func (c *Client) replayPending(cs *connState) {
	c.session.PendingOutbound(func(e *session.OutboundEntry) bool {
		if e.Packet == nil {
			c.cfg.Logger.Warn("mqttv5: outbound entry without retransmit packet",
				slog.Int("packet_id", int(e.PacketID)),
			)
			return true
		}
		bytes := withDupSet(e.Packet)
		c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
			n, err := w.Write(bytes)
			return int64(n), err
		})
		c.stats.addPublishReplayed()
		return true
	})
}

// withDupSet flips the DUP bit (bit 3 of the first byte) on a stored
// PUBLISH packet. Returns a copy so the original stored bytes remain
// unchanged for any future replay.
func withDupSet(orig []byte) []byte {
	if len(orig) == 0 {
		return orig
	}
	out := make([]byte, len(orig))
	copy(out, orig)
	out[0] |= 0x08
	return out
}

// resubscribeAll re-issues SUBSCRIBE for every tracked active
// subscription.
func (c *Client) resubscribeAll(cs *connState) {
	c.subsMu.Lock()
	subs := make([]*activeSub, 0, len(c.activeSubs))
	for _, s := range c.activeSubs {
		subs = append(subs, s)
	}
	c.subsMu.Unlock()

	for _, sub := range subs {
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ConnectTimeout)
		id, err := c.session.AllocateID(ctx)
		if err != nil {
			cancel()
			c.cfg.Logger.Error("mqttv5: resubscribe allocate id", slog.Any("error", err))
			continue
		}

		wait := c.installCtrlWaiter(id)
		filters := sub.filters
		err = c.enqueueAwaitOn(ctx, cs, func(w io.Writer) (int64, error) {
			return wire.WriteSubscribe(w, wire.SubscribeOpts{PacketID: id, Filters: filters})
		})
		if err != nil {
			c.removeCtrlWaiter(id)
			c.session.ReleaseID(id)
			cancel()
			c.cfg.Logger.Error("mqttv5: resubscribe write", slog.Any("error", err))
			continue
		}
		select {
		case pkt := <-wait:
			pkt.Release()
		case <-ctx.Done():
			c.removeCtrlWaiter(id)
		case <-c.shutdown:
			c.removeCtrlWaiter(id)
		}
		c.session.ReleaseID(id)
		cancel()
	}
}

// writeConnect emits the CONNECT packet directly against w (used during
// the handshake, before the writer goroutine has started).
//
// CleanStart resolution: the initial handshake uses cfg.CleanStart; every
// subsequent attempt uses cfg.CleanStartOnReconnect (default false —
// preserve session for QoS 1/2 resume).
//
// If cfg.Authenticator is set, the CONNECT carries
// AuthenticationMethod + the initial AuthenticationData from
// Authenticator.Begin(ctx). connectOnce then drives the AUTH loop.
//
// If cfg.ConnectPacketBuilder is set, it runs after this method has
// populated opts and may mutate any field — useful for per-attempt
// credential rotation. A non-nil error from the builder fails the
// attempt.
func (c *Client) writeConnect(ctx context.Context, w io.Writer, isReconnect bool) error {
	cfg := c.cfg
	cleanStart := cfg.CleanStart
	if isReconnect {
		cleanStart = cfg.CleanStartOnReconnect
	}

	opts := wire.ConnectOpts{
		ClientID:          cfg.ClientID,
		CleanStart:        cleanStart,
		KeepAlive:         cfg.KeepAlive,
		Username:          cfg.Username,
		Password:          cfg.Password,
		Will:              cfg.WillMessage,
		TopicAliasMaximum: cfg.InboundTopicAliasMaximum,
		UserProperties:    cfg.ConnectUserProperties,
	}
	if cfg.SessionExpiry > 0 {
		opts.SessionExpiryInterval = &cfg.SessionExpiry
	}
	if cfg.ReceiveMaximum > 0 && cfg.ReceiveMaximum < 65535 {
		opts.ReceiveMaximum = &cfg.ReceiveMaximum
	}
	if cfg.MaximumPacketSize > 0 {
		opts.MaximumPacketSize = &cfg.MaximumPacketSize
	}
	if cfg.RequestResponseInformation {
		v := byte(1)
		opts.RequestResponseInformation = &v
	}
	if cfg.RequestProblemInformation {
		v := byte(1)
		opts.RequestProblemInformation = &v
	}
	if cfg.Authenticator != nil {
		opts.AuthenticationMethod = cfg.Authenticator.Method()
		data, err := cfg.Authenticator.Begin(ctx)
		if err != nil {
			return fmt.Errorf("mqttv5: Authenticator.Begin: %w", err)
		}
		opts.AuthenticationData = data
	}
	if cfg.ConnectPacketBuilder != nil {
		if err := cfg.ConnectPacketBuilder(ctx, &opts); err != nil {
			return fmt.Errorf("mqttv5: ConnectPacketBuilder: %w", err)
		}
	}
	_, err := wire.WriteConnect(w, opts)
	return err
}
