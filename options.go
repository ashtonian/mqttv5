// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package mqttv5 is a high-performance MQTT v5 client.
//
// The client speaks raw bytes by default. Callers that want typed
// payloads can wrap with Typed[T] via a Codec[T] from one of the
// sibling codec submodules. See the package doc in doc.go for an
// architecture overview.
package mqttv5

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"time"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// Default values used by Config.defaults. Exported so callers can
// reference them when overriding selectively.
const (
	// DefaultKeepAlive is the keepalive interval used when
	// WithKeepAlive is not called. MQTT v5 spec range is 1..65535
	// seconds.
	DefaultKeepAlive uint16 = 30

	// DefaultConnectTimeout bounds the CONNECT handshake (dial +
	// CONNECT/CONNACK) when WithConnectTimeout is not called.
	DefaultConnectTimeout = 10 * time.Second

	// DefaultPingTimeoutFloor is the minimum PingTimeout the defaults
	// will produce — KeepAlive*2 is used unless it is below this
	// floor, in which case the floor wins.
	DefaultPingTimeoutFloor = 10 * time.Second

	// DefaultWriteQueueSize sizes the internal MPSC write channel.
	DefaultWriteQueueSize = 256

	// DefaultDisconnectFlushTimeout caps how long writeDisconnectAndClose
	// will wait for the DISCONNECT packet to flush before the
	// connection is forcibly torn down.
	DefaultDisconnectFlushTimeout = 500 * time.Millisecond
)

// DefaultReconnectBackoff is used when WithReconnectBackoff is not
// called: 1s -> 2s -> 4s -> ... up to 30s with 200ms jitter.
var DefaultReconnectBackoff = ExponentialBackoff(
	time.Second, 30*time.Second, 200*time.Millisecond,
)

// Config carries the resolved configuration for a [Client]. Construct
// via [New].
type Config struct {
	// BrokerURLs is the ordered list of broker URLs. The supervisor
	// rotates through it on reconnect; [Client.SetBrokers] replaces
	// it at runtime.
	BrokerURLs []string
	ClientID   string
	Username   string
	Password   []byte
	KeepAlive  uint16

	// CleanStart sets the CleanStart flag on the initial CONNECT
	// (§3.1.2.4). Default true.
	CleanStart bool

	// CleanStartOnReconnect sets CleanStart on every CONNECT after
	// the first. Default false (preserve broker session for QoS 1/2
	// resume).
	CleanStartOnReconnect bool

	// keepAliveDisabled distinguishes "user passed WithoutKeepAlive"
	// from "KeepAlive zero, fill in default" inside Config.defaults.
	keepAliveDisabled bool

	SessionExpiry  uint32
	ReceiveMaximum uint16

	// MaximumPacketSize caps the largest packet the broker may send
	// to us (§3.1.2.11.4). Zero advertises no limit.
	MaximumPacketSize uint32

	// InboundTopicAliasMaximum is the inbound TopicAliasMaximum
	// (§3.1.2.11.5). Zero (default) tells the broker not to alias
	// inbound PUBLISHes. The outbound budget is taken automatically
	// from the broker's CONNACK.
	InboundTopicAliasMaximum uint16

	// RequestResponseInformation asks the broker for
	// ResponseInformation in CONNACK (§3.1.2.11.6).
	RequestResponseInformation bool

	// RequestProblemInformation asks the broker for ReasonString /
	// UserProperties on error responses (§3.1.2.11.7).
	RequestProblemInformation bool

	// ConnectUserProperties are sent verbatim as CONNECT user
	// properties.
	ConnectUserProperties []wire.UserProperty

	ConnectTimeout time.Duration
	PingTimeout    time.Duration

	// DisconnectFlushTimeout bounds the wait for a graceful
	// DISCONNECT write before the socket is forcibly closed.
	// Default 500ms.
	DisconnectFlushTimeout time.Duration

	WriteQueueSize int

	// WriteBatchMax caps how many pre-encoded packets the writer
	// goroutine coalesces into one writev. 0 disables batching.
	// Worth enabling only for sustained concurrent publishers; see
	// [WithWriteBatch].
	WriteBatchMax int

	WillMessage *wire.WillOpts

	// MaxSubscribeQueueSize caps the per-subscription buffer.
	// Zero = unbounded.
	MaxSubscribeQueueSize int

	// DropPolicy sets the default policy for full subscription
	// buffers. [Client.Subscribe] (chan) supports DropNewest only;
	// [Client.SubscribeQueue] honors both.
	DropPolicy DropPolicy

	// PublisherPoolSize > 1 enables a publish-only connection pool
	// that falls back to the main connection if every member is
	// unhealthy.
	PublisherPoolSize int

	// PublisherPoolRouting selects the pool member-picking strategy.
	PublisherPoolRouting PoolRoutingPolicy

	// PublisherPoolClientIDFn derives each pool member's ClientID
	// from the parent ClientID and a 1-based index. Default is
	// fmt.Sprintf("%s-pub-%d", parent, idx).
	PublisherPoolClientIDFn func(parent string, idx int) string

	// PublishMode selects QoS 0 write-completion semantics. QoS 1/2
	// always wait for the broker's ack.
	PublishMode PublishMode

	// OnConnectionUp fires on each successful CONNECT/CONNACK after
	// pending replays and resubscribes. The supplied *wire.Connack
	// is a detached clone safe to retain. Must not block.
	OnConnectionUp func(*wire.Connack)

	// OnConnectionDown fires on unexpected connection loss (not on
	// user-initiated Disconnect). Return false to stop the
	// supervisor; a subsequent Connect restarts it. Must not block.
	OnConnectionDown func() bool

	// OnConnectError fires per failed CONNECT attempt (dial failure,
	// CONNACK refusal, AUTH-loop error). Observability only; the
	// supervisor retries regardless. Must not block.
	OnConnectError func(err error)

	// OnReconnectAttempt fires immediately before each reconnect
	// dial (not the initial Connect). attempt starts at 1. Must not
	// block.
	OnReconnectAttempt func(attempt int, brokerURL string)

	// StatsEnabled toggles the [Client.Stats] counter set. Default
	// false — when disabled the hot path skips every atomic
	// increment and Stats returns the zero value.
	StatsEnabled bool

	// OnServerDisconnect fires when the broker sends a DISCONNECT
	// (§3.14) — the supplied *wire.Disconnect is a detached clone.
	// Fires after the connection is marked down and before
	// OnConnectionDown. May call [Client.SetBrokers] to honour a
	// ServerMoved / UseAnotherServer redirect on the next attempt.
	OnServerDisconnect func(*wire.Disconnect)

	// Authenticator drives MQTT v5 enhanced authentication when set.
	Authenticator Authenticator

	// ConnectPacketBuilder is invoked immediately before each CONNECT
	// is serialised. The callback may mutate any field of opts —
	// typical use is rotating Username/Password per attempt for
	// OAuth refresh. ctx is the per-attempt context bounded by
	// ConnectTimeout. A non-nil error fails the attempt; the
	// supervisor retries after backoff and fires OnConnectError.
	ConnectPacketBuilder func(ctx context.Context, opts *wire.ConnectOpts) error

	// ReconnectBackoff returns the delay before retry attempt N.
	// Defaults to [DefaultReconnectBackoff].
	ReconnectBackoff Backoff

	Logger *slog.Logger
	Store  session.Store

	TLSConfig *tls.Config
	Dialer    *net.Dialer

	// DialFunc replaces the built-in TCP/TLS dial path. Takes
	// precedence over TLSConfig and Dialer. Use for SOCKS / mTLS /
	// WebSocket (see transport/ws) / in-memory test transports.
	// When set, broker URL scheme validation is skipped.
	DialFunc transport.DialFunc
}

// Option mutates a [Config] during [New]; a non-nil return aborts
// construction.
type Option func(*Config) error

// WithBroker sets a single broker URL. Supported schemes: mqtt://,
// tcp://, mqtts://, tls://, ssl://; default ports 1883 / 8883 fill
// in when missing. For ws:// or wss:// pair with [WithDialFunc].
func WithBroker(url string) Option {
	return func(c *Config) error {
		c.BrokerURLs = []string{url}
		return nil
	}
}

// WithBrokers sets the ordered broker URL list. The supervisor
// rotates through it across reconnect attempts; [Client.SetBrokers]
// replaces it at runtime.
func WithBrokers(urls ...string) Option {
	return func(c *Config) error {
		if len(urls) == 0 {
			return ErrMissingBroker
		}
		c.BrokerURLs = append(c.BrokerURLs[:0], urls...)
		return nil
	}
}

// WithClientID sets the MQTT ClientID. Empty string asks the broker to
// assign one via the AssignedClientIdentifier CONNACK property.
func WithClientID(id string) Option {
	return func(c *Config) error {
		c.ClientID = id
		return nil
	}
}

// WithCredentials sets username and password sent in the CONNECT packet.
func WithCredentials(user string, pass []byte) Option {
	return func(c *Config) error {
		c.Username = user
		c.Password = pass
		return nil
	}
}

// WithKeepAlive sets the keepalive interval (seconds, range 1..65535).
// Pass 0 to [WithoutKeepAlive] instead.
func WithKeepAlive(s uint16) Option {
	return func(c *Config) error {
		if s == 0 {
			return errors.New("mqttv5: WithKeepAlive(0) is invalid; use WithoutKeepAlive to disable")
		}
		c.KeepAlive = s
		c.keepAliveDisabled = false
		return nil
	}
}

// WithoutKeepAlive disables MQTT keepalive entirely (no PINGREQ
// goroutine). Rarely correct in production — without PINGREQ the
// broker cannot detect a half-open connection from this side.
func WithoutKeepAlive() Option {
	return func(c *Config) error {
		c.KeepAlive = 0
		c.keepAliveDisabled = true
		return nil
	}
}

// WithCleanStart sets the CleanStart flag on the initial CONNECT.
// Default true. Reconnects use [WithCleanStartOnReconnect].
func WithCleanStart(b bool) Option {
	return func(c *Config) error {
		c.CleanStart = b
		return nil
	}
}

// WithCleanStartOnReconnect sets CleanStart on every CONNECT after
// the first. Default false — preserve broker session for QoS 1/2
// resume.
func WithCleanStartOnReconnect(b bool) Option {
	return func(c *Config) error {
		c.CleanStartOnReconnect = b
		return nil
	}
}

// WithSessionExpiry sets the Session Expiry Interval (seconds). Zero
// means the session ends with the network connection.
func WithSessionExpiry(s uint32) Option {
	return func(c *Config) error {
		c.SessionExpiry = s
		return nil
	}
}

// WithReceiveMaximum bounds concurrent inbound QoS 1/2 publishes
// (§3.1.2.11.3). Also sizes the packet-ID pool.
func WithReceiveMaximum(n uint16) Option {
	return func(c *Config) error {
		c.ReceiveMaximum = n
		return nil
	}
}

// WithMaximumPacketSize caps the largest packet the broker may send
// to this client (§3.1.2.11.4). Zero advertises no limit.
func WithMaximumPacketSize(n uint32) Option {
	return func(c *Config) error {
		c.MaximumPacketSize = n
		return nil
	}
}

// WithInboundTopicAliasMaximum advertises how many inbound topic
// aliases this client accepts (§3.1.2.11.5). Zero (default)
// disables inbound aliasing.
func WithInboundTopicAliasMaximum(n uint16) Option {
	return func(c *Config) error {
		c.InboundTopicAliasMaximum = n
		return nil
	}
}

// WithRequestResponseInformation asks the broker for
// ResponseInformation in CONNACK (§3.1.2.11.6) — used by the
// request/response pattern (§4.10).
func WithRequestResponseInformation(b bool) Option {
	return func(c *Config) error {
		c.RequestResponseInformation = b
		return nil
	}
}

// WithRequestProblemInformation asks the broker for ReasonString /
// UserProperties on error responses (§3.1.2.11.7).
func WithRequestProblemInformation(b bool) Option {
	return func(c *Config) error {
		c.RequestProblemInformation = b
		return nil
	}
}

// WithConnectUserProperty appends one CONNECT user property. May be
// called multiple times; for bulk replacement see
// [WithConnectUserProperties].
func WithConnectUserProperty(key, value string) Option {
	return func(c *Config) error {
		c.ConnectUserProperties = append(c.ConnectUserProperties,
			wire.UserProperty{Key: key, Value: value})
		return nil
	}
}

// WithConnectUserProperties replaces the CONNECT user-property list.
// The slice is referenced as-is; do not mutate after passing.
func WithConnectUserProperties(p []wire.UserProperty) Option {
	return func(c *Config) error {
		c.ConnectUserProperties = p
		return nil
	}
}

// WithConnectTimeout caps the CONNECT handshake (dial + CONNECT/CONNACK).
func WithConnectTimeout(d time.Duration) Option {
	return func(c *Config) error {
		c.ConnectTimeout = d
		return nil
	}
}

// WithPingTimeout sets the budget for a PINGRESP after a PINGREQ.
// Exceeding it forces a reconnect. Defaults clamp to at least
// [DefaultPingTimeoutFloor].
func WithPingTimeout(d time.Duration) Option {
	return func(c *Config) error {
		c.PingTimeout = d
		return nil
	}
}

// WithDisconnectFlushTimeout caps the wait for a graceful DISCONNECT
// write before the socket is forcibly closed. Default
// [DefaultDisconnectFlushTimeout].
func WithDisconnectFlushTimeout(d time.Duration) Option {
	return func(c *Config) error {
		c.DisconnectFlushTimeout = d
		return nil
	}
}

// WithWriteQueueSize sets the internal MPSC write-channel buffer.
// Larger absorbs publish bursts; costs memory and queue latency.
func WithWriteQueueSize(n int) Option {
	return func(c *Config) error {
		c.WriteQueueSize = n
		return nil
	}
}

// WithWriteBatch coalesces up to n pre-encoded packets per writev
// syscall. Off by default (n=0 or 1). Wins under sustained concurrent
// [Client.Publish] (~40% at 256B with n=16 on loopback); regresses
// for single-publisher or large-payload workloads. Measure first.
func WithWriteBatch(n int) Option {
	return func(c *Config) error {
		if n < 0 {
			return fmt.Errorf("mqttv5: WriteBatchMax must be >= 0, got %d", n)
		}
		c.WriteBatchMax = n
		return nil
	}
}

// WithWill attaches a will message to be published by the broker if
// this client disconnects ungracefully.
func WithWill(w *wire.WillOpts) Option {
	return func(c *Config) error {
		c.WillMessage = w
		return nil
	}
}

// WithOnConnectionUp fires on each successful CONNECT/CONNACK after
// pending replays and resubscribes. fn receives a detached
// *wire.Connack clone safe to retain (assigned ClientID, server
// keepalive, MaximumQoS, ResponseInformation, ...). Must not block.
func WithOnConnectionUp(fn func(*wire.Connack)) Option {
	return func(c *Config) error {
		c.OnConnectionUp = fn
		return nil
	}
}

// WithOnConnectionDown registers a callback fired when the connection
// is lost. Does NOT fire on user-initiated Disconnect. Return false
// to terminate the supervisor — no further reconnect attempts. A
// subsequent Connect on the same Client re-starts the lifecycle.
// Must not block.
func WithOnConnectionDown(fn func() bool) Option {
	return func(c *Config) error {
		c.OnConnectionDown = fn
		return nil
	}
}

// WithOnConnectError fires per failed CONNECT attempt (dial err,
// CONNACK refusal, AUTH-loop err). Observability only — the
// supervisor retries regardless. Must not block.
func WithOnConnectError(fn func(error)) Option {
	return func(c *Config) error {
		c.OnConnectError = fn
		return nil
	}
}

// WithOnReconnectAttempt fires immediately before each reconnect
// dial (not the initial [Client.Connect]); attempt starts at 1.
// Must not block.
func WithOnReconnectAttempt(fn func(attempt int, brokerURL string)) Option {
	return func(c *Config) error {
		c.OnReconnectAttempt = fn
		return nil
	}
}

// WithOnServerDisconnect fires when the broker sends a DISCONNECT
// (§3.14). fn receives a detached *wire.Disconnect clone safe to
// retain. Fires after the connection is marked down and before
// OnConnectionDown; may call [Client.SetBrokers] to honour a
// ServerMoved / UseAnotherServer redirect on the next attempt. Not
// fired for socket-level errors or client-initiated Disconnect.
// Must not block.
func WithOnServerDisconnect(fn func(*wire.Disconnect)) Option {
	return func(c *Config) error {
		c.OnServerDisconnect = fn
		return nil
	}
}

// WithConnectPacketBuilder mutates *wire.ConnectOpts immediately
// before each CONNECT is serialised — canonical OAuth token-refresh
// hook. ctx is the per-attempt context bounded by ConnectTimeout.
// A non-nil error fails the attempt; the supervisor retries after
// backoff and fires OnConnectError.
func WithConnectPacketBuilder(fn func(ctx context.Context, opts *wire.ConnectOpts) error) Option {
	return func(c *Config) error {
		c.ConnectPacketBuilder = fn
		return nil
	}
}

// WithReconnectBackoff sets the supervisor's reconnect backoff. See
// [ConstantBackoff] and [ExponentialBackoff]; default
// [DefaultReconnectBackoff].
func WithReconnectBackoff(b Backoff) Option {
	return func(c *Config) error {
		c.ReconnectBackoff = b
		return nil
	}
}

// WithMaxSubscribeQueueSize caps each subscription's buffer.
// 0 = unbounded. Per-call override via [SubMaxQueueSize].
func WithMaxSubscribeQueueSize(n int) Option {
	return func(c *Config) error {
		c.MaxSubscribeQueueSize = n
		return nil
	}
}

// WithDropPolicy sets the default per-subscription drop policy.
// Per-call override via [SubDropPolicy]; see [DropPolicy].
func WithDropPolicy(p DropPolicy) Option {
	return func(c *Config) error {
		if !p.valid() {
			return fmt.Errorf("mqttv5: invalid DropPolicy %d", p)
		}
		c.DropPolicy = p
		return nil
	}
}

// WithPublisherPool enables N publish-only connections to the
// configured broker. 0/1 disables. Each member is a full [Client]
// with its own session; QoS 1/2 acks land on the emitting member.
// Members force CleanStart=true + SessionExpiry=0 — a mid-flight
// drop becomes at-least-once with possible duplicates. Use the
// main connection for ledger-style work.
func WithPublisherPool(size int) Option {
	return func(c *Config) error {
		c.PublisherPoolSize = size
		return nil
	}
}

// PoolRoutingPolicy selects how the publisher pool picks the
// starting member per [Client.Publish]. Unhealthy starting members
// fall through to the next member regardless of policy.
type PoolRoutingPolicy uint8

const (
	// PoolRoutingRoundRobin (default) cycles through members; same
	// topic may arrive at the broker out of order.
	PoolRoutingRoundRobin PoolRoutingPolicy = iota

	// PoolRoutingHashByTopic picks the starting member by FNV-1a of
	// opts.Topic, preserving per-topic ordering while the pool is
	// healthy. Empty Topic collapses to member 0.
	PoolRoutingHashByTopic
)

func (p PoolRoutingPolicy) valid() bool {
	return p == PoolRoutingRoundRobin || p == PoolRoutingHashByTopic
}

// String returns a stable debug name for the policy.
func (p PoolRoutingPolicy) String() string {
	switch p {
	case PoolRoutingHashByTopic:
		return "hash_by_topic"
	default:
		return "round_robin"
	}
}

// WithPublisherPoolRouting selects the publisher-pool member-picking
// strategy. See [PoolRoutingPolicy]. No-op when PublisherPoolSize ≤ 1.
func WithPublisherPoolRouting(p PoolRoutingPolicy) Option {
	return func(c *Config) error {
		if !p.valid() {
			return fmt.Errorf("mqttv5: invalid PoolRoutingPolicy %d", p)
		}
		c.PublisherPoolRouting = p
		return nil
	}
}

// DefaultPublisherPoolClientIDFormat is the format string used by the
// default pool-member ClientID function: parent ClientID, then "-pub-",
// then the 1-based member index.
const DefaultPublisherPoolClientIDFormat = "%s-pub-%d"

// defaultPoolClientIDFn implements the default ClientID derivation
// for pool members.
func defaultPoolClientIDFn(parent string, idx int) string {
	return fmt.Sprintf(DefaultPublisherPoolClientIDFormat, parent, idx)
}

// WithPublisherPoolClientIDFn derives each pool member's ClientID
// from the parent ClientID and a 1-based index. Override when the
// parent ClientID is empty — the default ("-pub-N") collides across
// processes.
func WithPublisherPoolClientIDFn(fn func(parent string, idx int) string) Option {
	return func(c *Config) error {
		c.PublisherPoolClientIDFn = fn
		return nil
	}
}

// PublishMode controls QoS 0 [Client.Publish] write-completion
// semantics. QoS 1/2 always wait for the broker's ack.
type PublishMode uint8

const (
	// PublishFireAndForget (default): Publish returns once the
	// message is queued for the writer goroutine. Transport errors
	// are not surfaced.
	PublishFireAndForget PublishMode = iota

	// PublishWaitForFlush: Publish blocks until conn.Write returns
	// and surfaces transport errors. Useful as a connection-health
	// probe.
	PublishWaitForFlush
)

func (m PublishMode) valid() bool {
	return m == PublishFireAndForget || m == PublishWaitForFlush
}

// String returns a stable name for the mode.
func (m PublishMode) String() string {
	switch m {
	case PublishWaitForFlush:
		return "wait_for_flush"
	default:
		return "fire_and_forget"
	}
}

// WithPublishMode selects QoS 0 [Client.Publish] semantics. See
// [PublishMode]. QoS 1/2 unaffected.
func WithPublishMode(m PublishMode) Option {
	return func(c *Config) error {
		if !m.valid() {
			return fmt.Errorf("mqttv5: invalid PublishMode %d", m)
		}
		c.PublishMode = m
		return nil
	}
}

// DropPolicy controls behaviour when a bounded subscription buffer
// fills.
type DropPolicy uint8

const (
	// DropNewest discards the inbound message + acks it so the
	// broker doesn't retry.
	DropNewest DropPolicy = iota
	// DropOldest evicts the buffer head to admit the inbound
	// message. SubscribeQueue only — chan-based [Client.Subscribe]
	// rejects this with [ErrChanDropOldestUnsupported].
	DropOldest
)

func (p DropPolicy) valid() bool {
	return p == DropNewest || p == DropOldest
}

// String returns a stable debug name for the policy.
func (p DropPolicy) String() string {
	switch p {
	case DropOldest:
		return "drop_oldest"
	default:
		return "drop_newest"
	}
}

// WithLogger sets the [*slog.Logger]. Default [slog.Default].
func WithLogger(l *slog.Logger) Option {
	return func(c *Config) error {
		c.Logger = l
		return nil
	}
}

// WithStats enables the [Client.Stats] counter set. When disabled
// the hot path skips every atomic increment and Stats returns the
// zero value. Off by default; enable to scrape counters into
// Prometheus / OpenTelemetry / expvar.
func WithStats() Option {
	return func(c *Config) error {
		c.StatsEnabled = true
		return nil
	}
}

// WithStore overrides the [session.Store]. Default is in-memory; use
// the store/file submodule for crash safety.
func WithStore(s session.Store) Option {
	return func(c *Config) error {
		c.Store = s
		return nil
	}
}

// WithTLSConfig supplies a [*tls.Config] for mqtts:// / tls:// dials.
// Ignored for plain TCP schemes.
func WithTLSConfig(t *tls.Config) Option {
	return func(c *Config) error {
		c.TLSConfig = t
		return nil
	}
}

// WithDialer overrides the default [*net.Dialer].
func WithDialer(d *net.Dialer) Option {
	return func(c *Config) error {
		c.Dialer = d
		return nil
	}
}

// WithDialFunc replaces the built-in TCP/TLS dial path. Takes
// precedence over [WithDialer] / [WithTLSConfig]. Use for SOCKS /
// HTTP proxies, per-dial mTLS, WebSocket (transport/ws), or
// in-memory test transports. ctx is bounded by ConnectTimeout.
// When set, broker URL scheme validation is skipped. Nil rejected.
//
// The transport/ws sibling submodule exposes a [ws.DialFunc] helper
// that bakes in DialOpts (TLS config, custom headers, etc.) and
// returns a value directly usable here:
//
//	mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg}))
func WithDialFunc(fn transport.DialFunc) Option {
	return func(c *Config) error {
		if fn == nil {
			return errors.New("mqttv5: WithDialFunc(nil) is invalid")
		}
		c.DialFunc = fn
		return nil
	}
}

// defaults fills in unset fields. Called by New after options are applied.
func (c *Config) defaults() {
	if c.KeepAlive == 0 && !c.keepAliveDisabled {
		c.KeepAlive = DefaultKeepAlive
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.PingTimeout == 0 {
		c.PingTimeout = time.Duration(c.KeepAlive) * time.Second * 2
		if c.PingTimeout < DefaultPingTimeoutFloor {
			c.PingTimeout = DefaultPingTimeoutFloor
		}
	}
	if c.DisconnectFlushTimeout == 0 {
		c.DisconnectFlushTimeout = DefaultDisconnectFlushTimeout
	}
	if c.WriteQueueSize == 0 {
		c.WriteQueueSize = DefaultWriteQueueSize
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Store == nil {
		c.Store = session.NewMemoryStore()
	}
	if c.ReconnectBackoff == nil {
		c.ReconnectBackoff = DefaultReconnectBackoff
	}
	if c.PublisherPoolClientIDFn == nil {
		c.PublisherPoolClientIDFn = defaultPoolClientIDFn
	}
	// CleanStart defaults to true for safety; sessions are explicit.
	// (Zero-value bool is false, so we cannot detect "user set it
	// false" vs "user left it default". Document the default as true
	// in the option godoc and pin the default here.)
	//
	// To override, WithCleanStart(false) must be called explicitly.
}

// ErrMissingBroker is returned by New when no broker URL was provided.
var ErrMissingBroker = errors.New("mqttv5: missing broker URL (use WithBroker)")

// ErrInvalidBrokerURL is returned when a broker URL fails to parse or
// has an unsupported scheme. WithDialFunc relaxes scheme validation.
var ErrInvalidBrokerURL = errors.New("mqttv5: invalid broker URL")

// builtinSchemes is the set of URL schemes handled by transport.Dial.
// ws:// and wss:// require WithDialFunc(ws.DialFunc(...)) — they're
// listed here so SetBrokers / validateBrokerURLs accept them at
// construct time when a DialFunc is also configured.
var builtinSchemes = map[string]bool{
	"mqtt":  true,
	"tcp":   true,
	"mqtts": true,
	"tls":   true,
	"ssl":   true,
	"ws":    true,
	"wss":   true,
}

// validateBrokerURLs sanity-checks urls. When dialFuncSet is true the
// scheme list is not enforced — the caller owns the contract via
// WithDialFunc. Otherwise the scheme must be one of the built-in
// transports.
func validateBrokerURLs(urls []string, dialFuncSet bool) error {
	if len(urls) == 0 {
		return ErrMissingBroker
	}
	for i, raw := range urls {
		if raw == "" {
			return fmt.Errorf("%w: urls[%d] is empty", ErrInvalidBrokerURL, i)
		}
		u, err := url.Parse(raw)
		if err != nil {
			return fmt.Errorf("%w: urls[%d] = %q: %w", ErrInvalidBrokerURL, i, raw, err)
		}
		if u.Host == "" {
			return fmt.Errorf("%w: urls[%d] = %q: no host", ErrInvalidBrokerURL, i, raw)
		}
		if dialFuncSet {
			continue
		}
		if !builtinSchemes[u.Scheme] {
			return fmt.Errorf("%w: urls[%d] = %q: unsupported scheme %q",
				ErrInvalidBrokerURL, i, raw, u.Scheme)
		}
	}
	return nil
}

// validate sanity-checks the config after defaults are applied.
func (c *Config) validate() error {
	if err := validateBrokerURLs(c.BrokerURLs, c.DialFunc != nil); err != nil {
		return err
	}
	if !c.DropPolicy.valid() {
		return fmt.Errorf("mqttv5: invalid DropPolicy %d", c.DropPolicy)
	}
	if !c.PublishMode.valid() {
		return fmt.Errorf("mqttv5: invalid PublishMode %d", c.PublishMode)
	}
	if !c.PublisherPoolRouting.valid() {
		return fmt.Errorf("mqttv5: invalid PoolRoutingPolicy %d", c.PublisherPoolRouting)
	}
	return nil
}
