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
	"io"
	"log/slog"
	"net"
	"net/url"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/inflight"
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

	// DefaultPingTimeout caps how long the client waits for any packet
	// after a PINGREQ before declaring the connection dead. The default
	// is the smaller of this and half the keep-alive, so a dead
	// connection is always detected before the next PINGREQ would be
	// due. With DefaultKeepAlive (30 s) the dead-detect time is 40 s,
	// inside a broker's 1.5×KeepAlive (45 s) disconnect window.
	DefaultPingTimeout = 10 * time.Second

	// DefaultSessionExpiry is the Session Expiry Interval (§3.1.2.11.2)
	// advertised on every CONNECT. 5 minutes is long enough to ride out
	// the typical reconnect blip / container redeploy window so QoS 1/2
	// resume actually works out of the box; short enough that the
	// broker isn't asked to hold idle sessions indefinitely.
	DefaultSessionExpiry uint32 = 300

	// DefaultWriteQueueSize sizes the internal MPSC write channel.
	DefaultWriteQueueSize = 256

	// DefaultDisconnectFlushTimeout caps how long writeDisconnectAndClose
	// will wait for the DISCONNECT packet to flush before the
	// connection is forcibly torn down.
	DefaultDisconnectFlushTimeout = 500 * time.Millisecond

	// DefaultReadBufferSize is the per-connection read window used when
	// [WithReadBufferSize] is not called. 16 KiB cuts read syscalls for
	// streams of small messages fourfold against 4 KiB; larger windows
	// gain little (BenchmarkReceiveWindow).
	DefaultReadBufferSize = 16 << 10

	// DefaultMaxSubscribeQueueSize caps each [Client.SubscribeQueue]
	// when no cap is configured. At roughly 200 B of overhead plus the
	// payload per queued message, a full default queue of small
	// messages holds about 13 MiB plus payloads.
	DefaultMaxSubscribeQueueSize = 65536

	// UnboundedQueue, passed to [WithMaxSubscribeQueueSize] or
	// [SubMaxQueueSize], removes the queue cap. A consumer that falls
	// behind then grows memory without limit.
	UnboundedQueue = -1
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
	// (§3.1.2.4). When not set explicitly it is true unless there is
	// session state to resume (see [WithStore]).
	CleanStart bool

	// cleanStartSet records an explicit WithCleanStart.
	cleanStartSet bool

	// SessionLossPolicy decides what happens to unacknowledged QoS 1/2
	// publishes when a connection starts without the old session.
	// Default [SessionLossRepublish].
	SessionLossPolicy SessionLossPolicy

	// OutboundTopicAliases replaces repeated QoS 0 topics with topic
	// aliases. Default false.
	OutboundTopicAliases bool

	// LenientDecoding tolerates the harmless broker violations listed at
	// [WithLenientDecoding] instead of disconnecting. Default false.
	LenientDecoding bool

	// OnResubscribeError fires when the broker refuses filters the client
	// re-subscribes after a session loss. Must not block.
	OnResubscribeError func(SubscriptionToken, error)

	// QoSDowngrade lowers a publish's QoS to the broker's Maximum QoS
	// instead of failing it with [ErrQoSNotSupported]. Default false.
	QoSDowngrade bool

	// OnServerRedirect fires when the broker redirects the client (§4.11).
	OnServerRedirect func(ServerRedirect)
	// FollowServerRedirects makes the client connect where a redirect
	// points.
	FollowServerRedirects bool

	// RetryInitialConnect makes Connect hand a failed first handshake
	// to the supervisor's reconnect loop instead of returning its error.
	RetryInitialConnect bool

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
	// to this client (§3.1.2.11.4). Zero advertises no limit. A larger
	// packet closes the connection with DISCONNECT 0x95.
	MaximumPacketSize uint32

	// ReadBufferSize is the per-connection read window in bytes: how
	// much one read syscall can bring in. Default
	// [DefaultReadBufferSize].
	ReadBufferSize int

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

	// WriteOverflowPolicy controls what QoS 0 Publish does when the
	// writer queue is full. WriteBlock (default) waits for room or
	// ctx; WriteDropNewest returns ErrWriteQueueFull immediately.
	// QoS 1/2 unaffected.
	WriteOverflowPolicy WriteOverflowPolicy

	// WriteBatchMax caps how many pre-encoded packets the writer
	// goroutine coalesces into one writev. 0 disables batching. See
	// [WithWriteBatch].
	WriteBatchMax int

	WillMessage *WillOptions

	// MaxSubscribeQueueSize caps each SubscribeQueue's length.
	// 0 means [DefaultMaxSubscribeQueueSize]; [UnboundedQueue] (any
	// negative value) removes the cap.
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

	// OnConnectionUp fires on each successful CONNECT/CONNACK with
	// what the broker granted. Must not block.
	OnConnectionUp func(ConnackInfo)

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
	// (§3.14).
	// Fires after the connection is marked down and before
	// OnConnectionDown. May call [Client.SetBrokers] to honour a
	// ServerMoved / UseAnotherServer redirect on the next attempt.
	OnServerDisconnect func(DisconnectInfo)

	// Authenticator drives MQTT v5 enhanced authentication when set.
	Authenticator Authenticator

	// OnReauthenticated fires when a re-authentication (§4.12) the
	// client started with Reauthenticate concludes successfully (broker
	// AUTH 0x00 Success). Runs on the read loop; must not block.
	// Observability only: the success is also Reauthenticate's return
	// value.
	OnReauthenticated func()

	// ConnectPacketBuilder is invoked immediately before each CONNECT
	// is serialised and may change its credentials and user
	// properties — typically rotating Username/Password per attempt
	// for OAuth refresh. ctx is the per-attempt context bounded by
	// ConnectTimeout. A non-nil error fails the attempt; the
	// supervisor retries after backoff and fires OnConnectError.
	ConnectPacketBuilder func(ctx context.Context, opts *ConnectOptions) error

	// ReconnectBackoff returns the delay before retry attempt N.
	// Defaults to [DefaultReconnectBackoff].
	ReconnectBackoff Backoff

	Logger *slog.Logger

	// Store persists session state across process restarts. Nil (the
	// default) keeps it in memory: in-flight QoS 1/2 messages survive
	// reconnects but not a restart.
	Store session.Store

	// OnStoreFailure fires once when a Store write fails and the client
	// has stopped because of it (see [WithStore]). Must not block.
	OnStoreFailure func(err error)

	TLSConfig *tls.Config
	Dialer    *net.Dialer

	// DialFunc replaces the built-in TCP/TLS dial path. Takes
	// precedence over TLSConfig and Dialer. Use for SOCKS / mTLS /
	// WebSocket (see transport/ws) / in-memory test transports.
	// When set, broker URL scheme validation is skipped.
	DialFunc transport.DialFunc

	// clock is the time source for keep-alive, backoff and queue draining.
	// Tests inject a fake through withClock; production uses clock.Real.
	clock clock.Clock
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
//
// Brokers that do not share session state answer a failover CONNECT
// with Session Present = 0: unacknowledged QoS 1/2 publishes then
// follow [WithSessionLossPolicy] and subscriptions are re-issued.
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
// Without it the first CONNECT starts clean unless there is session
// state to resume — state left by an earlier Connect on this Client or
// loaded from [WithStore] — in which case it resumes (CleanStart=0).
// WithCleanStart(true) always starts a new session and discards that
// state. Reconnects use [WithCleanStartOnReconnect].
func WithCleanStart(b bool) Option {
	return func(c *Config) error {
		c.CleanStart = b
		c.cleanStartSet = true
		return nil
	}
}

// SessionLossPolicy decides what happens to unacknowledged QoS 1/2
// publishes when a connection starts without the old session: the
// broker answered CONNACK with Session Present = 0 (restart without
// persistence, session expiry, failover to another broker in
// [WithBrokers]) or the client connected with CleanStart=1.
type SessionLossPolicy uint8

const (
	// SessionLossRepublish sends them again as new messages (DUP=0) in
	// their original order; their Publish calls keep waiting for the new
	// acknowledgement. Delivery stays at-least-once, and a message the
	// old broker had already forwarded is delivered twice — including
	// QoS 2, whose exactly-once guarantee cannot survive a session loss.
	// Messages whose Message Expiry Interval ran out complete with
	// [ErrMessageExpired]; the rest are sent with the remaining interval.
	SessionLossRepublish SessionLossPolicy = SessionLossPolicy(inflight.Republish)

	// SessionLossFail completes them with [ErrSessionLost] and leaves
	// any retry to the caller.
	SessionLossFail SessionLossPolicy = SessionLossPolicy(inflight.Fail)
)

func (p SessionLossPolicy) valid() bool { return p == SessionLossRepublish || p == SessionLossFail }

// WithOutboundTopicAliases makes QoS 0 publishes replace a repeated
// topic with a topic alias (§3.3.2.3.4) while the broker's Topic Alias
// Maximum lasts: the first publish to a topic registers the alias, later
// ones send only the alias. It saves bandwidth for long topics. QoS 1/2
// publishes always carry their topic, since they may be resent on a
// connection where the alias is unknown. Off by default.
func WithOutboundTopicAliases() Option {
	return func(c *Config) error {
		c.OutboundTopicAliases = true
		return nil
	}
}

// WithLenientDecoding tolerates two harmless protocol violations seen
// from real brokers — a non-minimally encoded Remaining Length and
// reserved flag bits on PINGRESP — logging each at Warn instead of
// disconnecting. Every other violation still closes the connection with
// the spec's reason code (0x81 Malformed Packet, 0x82 Protocol Error, or
// a more specific one) and counts in [Stats].ProtocolErrors.
func WithLenientDecoding() Option {
	return func(c *Config) error {
		c.LenientDecoding = true
		return nil
	}
}

// WithOnResubscribeError observes filters the broker refuses when the
// client re-subscribes after a session loss (Session Present = 0). A
// subscription that loses every filter closes, and its token's Err
// reports why; one that keeps some carries on with those. Must not
// block.
func WithOnResubscribeError(fn func(SubscriptionToken, error)) Option {
	return func(c *Config) error {
		c.OnResubscribeError = fn
		return nil
	}
}

// WithQoSDowngrade sends a publish whose QoS is above the broker's
// Maximum QoS (CONNACK, §3.2.2.3.4) at that maximum instead of failing
// it with [ErrQoSNotSupported]. It weakens the delivery guarantee the
// caller asked for — QoS 2 becomes at-least-once — so it is off by
// default. Brokers such as AWS IoT Core advertise Maximum QoS 1.
func WithQoSDowngrade() Option {
	return func(c *Config) error {
		c.QoSDowngrade = true
		return nil
	}
}

// WithSessionLossPolicy sets the [SessionLossPolicy]. Default
// [SessionLossRepublish].
func WithSessionLossPolicy(p SessionLossPolicy) Option {
	return func(c *Config) error {
		if !p.valid() {
			return fmt.Errorf("mqttv5: invalid SessionLossPolicy %d", p)
		}
		c.SessionLossPolicy = p
		return nil
	}
}

// WithOnServerRedirect calls fn when the broker refuses a CONNECT, or
// ends the connection, with reason 0x9C Use another server or 0x9D
// Server moved and a Server Reference (§4.11). Must not block.
func WithOnServerRedirect(fn func(ServerRedirect)) Option {
	return func(c *Config) error {
		c.OnServerRedirect = fn
		return nil
	}
}

// WithFollowServerRedirects makes the client connect where the broker
// redirects it: after 0x9D Server moved the reference replaces the
// broker list ([Client.SetBrokers]); after 0x9C Use another server only
// the next connection attempt uses it. A reference that is a host or
// host:port keeps the current URL's scheme and path. Off by default:
// a redirect is reported ([WithOnServerRedirect]) and otherwise
// ignored.
func WithFollowServerRedirects() Option {
	return func(c *Config) error {
		c.FollowServerRedirects = true
		return nil
	}
}

// WithRetryInitialConnect lets the client start while the broker is
// unreachable: when the first CONNECT fails, Connect reports the error to
// [WithOnConnectError], returns nil, and the supervisor retries with
// [WithReconnectBackoff] as after a lost connection. Publish and
// Subscribe return ErrNotConnected until then ([QueuePublisher] queues);
// [Client.AwaitConnection] waits for the connection. Without it,
// Connect returns the first failure.
func WithRetryInitialConnect() Option {
	return func(c *Config) error {
		c.RetryInitialConnect = true
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

// WithSessionExpiry sets the Session Expiry Interval (§3.1.2.11.2) in
// seconds. Default [DefaultSessionExpiry] (300 s / 5 min) — enough
// for QoS 1/2 resume across a typical reconnect blip. Zero ends the
// session with the network connection (no broker-side retention).
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
// to this client (§3.1.2.11.4). Zero advertises no limit. A broker that
// sends a larger packet is disconnected with reason 0x95.
func WithMaximumPacketSize(n uint32) Option {
	return func(c *Config) error {
		c.MaximumPacketSize = n
		return nil
	}
}

// WithReadBufferSize sets the per-connection read window in bytes: how
// much one read syscall can bring in. A larger window cuts syscalls for
// streams of small messages at the cost of that much memory per
// connection. Default [DefaultReadBufferSize].
func WithReadBufferSize(n int) Option {
	return func(c *Config) error {
		if n < 512 {
			return fmt.Errorf("mqttv5: read buffer size %d below 512 bytes", n)
		}
		c.ReadBufferSize = n
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
// UserProperties on error responses (§3.1.2.11.7). Default true —
// debugging a CONNECT or PUBLISH refusal without these is "why was
// this rejected?" with no answer; the wire cost is a handful of
// bytes on the error path only. Pass false to opt out: the CONNECT
// then carries Request Problem Information = 0.
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

// WithPingTimeout sets how long the client waits for any packet after a
// PINGREQ before declaring the connection dead and reconnecting. It
// must be shorter than the keep-alive ([New] rejects it otherwise);
// when the broker grants a shorter keep-alive than requested, half the
// granted keep-alive is used instead. Default: the smaller of
// [DefaultPingTimeout] and half the keep-alive. Raise it only on links
// whose round trip exceeds a few seconds (satellite, congested
// cellular).
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

// WithWriteBatch coalesces up to n queued packets into one writev
// syscall. Off by default (n=0 or 1). It saves syscalls when many
// goroutines publish concurrently and packets queue behind the writer;
// with one publisher or large payloads there is little to coalesce.
// Measure with your workload before enabling it.
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
func WithWill(w *WillOptions) Option {
	return func(c *Config) error {
		c.WillMessage = w
		return nil
	}
}

// WithOnConnectionUp fires on each successful CONNECT/CONNACK with what
// the broker granted (session present, limits, assigned ClientID,
// effective keep-alive, ...); [Client.ServerInfo] returns the same for
// the current connection. Must not block.
func WithOnConnectionUp(fn func(ConnackInfo)) Option {
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
// dial (not the initial [Client.Connect]). attempt starts at 1 after a
// connection that lasted, and keeps counting across connections that
// dropped sooner than the reconnect delay before them (see
// [WithReconnectBackoff]). Must not block.
func WithOnReconnectAttempt(fn func(attempt int, brokerURL string)) Option {
	return func(c *Config) error {
		c.OnReconnectAttempt = fn
		return nil
	}
}

// WithOnServerDisconnect fires when the broker sends a DISCONNECT
// (§3.14), with its reason, reason string, server reference and user
// properties. Fires after the connection is marked down and before
// OnConnectionDown; may call [Client.SetBrokers] to honour a
// ServerMoved / UseAnotherServer redirect on the next attempt. Not
// fired for socket-level errors or client-initiated Disconnect.
// Must not block.
func WithOnServerDisconnect(fn func(DisconnectInfo)) Option {
	return func(c *Config) error {
		c.OnServerDisconnect = fn
		return nil
	}
}

// WithOnReauthenticated registers a callback fired when a
// re-authentication (§4.12) the client started with Reauthenticate
// concludes successfully (broker AUTH 0x00 Success). Observability only
// — runs on the read loop and must not block.
func WithOnReauthenticated(fn func()) Option {
	return func(c *Config) error {
		c.OnReauthenticated = fn
		return nil
	}
}

// WithConnectPacketBuilder lets fn change the CONNECT's credentials
// and user properties immediately before each attempt — canonical
// OAuth token-refresh hook. ctx is the per-attempt context bounded by
// ConnectTimeout. A non-nil error fails the attempt; the supervisor
// retries after backoff and fires OnConnectError.
func WithConnectPacketBuilder(fn func(ctx context.Context, opts *ConnectOptions) error) Option {
	return func(c *Config) error {
		c.ConnectPacketBuilder = fn
		return nil
	}
}

// WithReconnectBackoff sets the supervisor's reconnect backoff: the
// delay before each reconnect attempt, by attempt number. The count
// starts over only once a connection has outlived the delay before it,
// so a broker that drops connections right after accepting them is
// retried at the growing interval rather than at the first one. See
// [ConstantBackoff] and [ExponentialBackoff]; default
// [DefaultReconnectBackoff].
func WithReconnectBackoff(b Backoff) Option {
	return func(c *Config) error {
		c.ReconnectBackoff = b
		return nil
	}
}

// WithMaxSubscribeQueueSize caps each [Client.SubscribeQueue]'s length.
// Default [DefaultMaxSubscribeQueueSize]; [UnboundedQueue] removes the
// cap. When a queue is full its [DropPolicy] applies. Per-call override
// via [SubMaxQueueSize].
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

	// PublishWaitForFlush makes Publish block until the packet is
	// written to the connection and surfaces transport errors. On a TCP
	// or Unix connection with nothing queued for the writer goroutine the
	// caller writes the packet itself: the header and the caller's
	// payload go out in one writev, and the payload is not copied. ctx
	// bounds the call either way. A write it interrupts is finished by
	// the writer goroutine, which copies what is left, so the packet may
	// still reach the broker after Publish returned ctx's error; one
	// interrupted before its first byte is not sent. Useful as a
	// connection-health probe.
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

// WriteOverflowPolicy controls what QoS 0 [Client.Publish] does when
// the internal writer queue (sized by [WithWriteQueueSize]) is full.
// QoS 1/2 are unaffected — they always block on the broker ack.
type WriteOverflowPolicy uint8

const (
	// WriteBlock (default) blocks Publish until the writer queue has
	// room, the call's ctx fires, or the connection goes down. The
	// caller surfaces overload as latency.
	WriteBlock WriteOverflowPolicy = iota

	// WriteDropNewest returns [ErrWriteQueueFull] immediately when
	// the queue has no room. Use for high-rate fire-and-forget
	// telemetry where the producer goroutine must not block.
	WriteDropNewest
)

func (p WriteOverflowPolicy) valid() bool {
	return p == WriteBlock || p == WriteDropNewest
}

// String returns a stable name for the policy.
func (p WriteOverflowPolicy) String() string {
	switch p {
	case WriteDropNewest:
		return "drop_newest"
	default:
		return "block"
	}
}

// WithWriteOverflowPolicy selects the QoS 0 writer-queue overflow
// policy. Only meaningful for [PublishFireAndForget] — QoS 1/2 and
// [PublishWaitForFlush] always wait for either the broker or the
// writer to be ready.
func WithWriteOverflowPolicy(p WriteOverflowPolicy) Option {
	return func(c *Config) error {
		if !p.valid() {
			return fmt.Errorf("mqttv5: invalid WriteOverflowPolicy %d", p)
		}
		c.WriteOverflowPolicy = p
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

// WithStore persists session state in s so a restarted process resumes
// its QoS 1/2 flows: unacknowledged publishes are resent, QoS 2
// exchanges continue at the phase they reached, and a broker-assigned
// ClientID is reused. Each state change is written to s before the
// packet that depends on it is sent (see package session). Without a
// store the same state is kept in memory and survives reconnects only.
//
// A failed write of a new message's record fails that Publish. Any
// other failed write means the store no longer holds what the session
// has done, so the client stops rather than go on without the guarantee
// the store exists for: it sends DISCONNECT 0x80 (the broker publishes
// the Will), ends as [Client.Disconnect] would, and fires
// [WithOnStoreFailure]. QoS 1/2 Publish calls still waiting return the
// failure, a [*StoreError] matching [ErrStoreFailed]; their messages stay
// in the store. The next [Client.Connect] reloads the session from s, as
// a restarted process would.
//
// The client does not close s. Use the store/file submodule for a
// file-backed store, or session.MemoryStore in tests.
func WithStore(s session.Store) Option {
	return func(c *Config) error {
		c.Store = s
		return nil
	}
}

// WithOnStoreFailure registers a callback fired once when a session
// store write failed and the client has stopped because of it (see
// [WithStore]); err is a [*StoreError]. Call [Client.Connect] to start
// again from what the store holds. Must not block.
func WithOnStoreFailure(fn func(err error)) Option {
	return func(c *Config) error {
		c.OnStoreFailure = fn
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
		c.PingTimeout = DefaultPingTimeout
		if half := time.Duration(c.KeepAlive) * time.Second / 2; c.KeepAlive > 0 && half < c.PingTimeout {
			c.PingTimeout = half
		}
	}
	if c.DisconnectFlushTimeout == 0 {
		c.DisconnectFlushTimeout = DefaultDisconnectFlushTimeout
	}
	if c.WriteQueueSize == 0 {
		c.WriteQueueSize = DefaultWriteQueueSize
	}
	if c.ReadBufferSize == 0 {
		c.ReadBufferSize = DefaultReadBufferSize
	}
	if c.MaxSubscribeQueueSize == 0 {
		c.MaxSubscribeQueueSize = DefaultMaxSubscribeQueueSize
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.ReconnectBackoff == nil {
		c.ReconnectBackoff = DefaultReconnectBackoff
	}
	if c.PublisherPoolClientIDFn == nil {
		c.PublisherPoolClientIDFn = defaultPoolClientIDFn
	}
	if c.clock == nil {
		c.clock = clock.Real{}
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
// ws:// and wss:// need WithDialFunc(ws.DialFunc(...)).
var builtinSchemes = map[string]bool{
	"mqtt":  true,
	"tcp":   true,
	"mqtts": true,
	"tls":   true,
	"ssl":   true,
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
		if u.Scheme == "ws" || u.Scheme == "wss" {
			return fmt.Errorf("%w: urls[%d] = %q: WebSocket needs WithDialFunc (see the transport/ws submodule)",
				ErrInvalidBrokerURL, i, raw)
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
	if !c.SessionLossPolicy.valid() {
		return fmt.Errorf("mqttv5: invalid SessionLossPolicy %d", c.SessionLossPolicy)
	}
	if keep := time.Duration(c.KeepAlive) * time.Second; c.KeepAlive > 0 && c.PingTimeout >= keep {
		return fmt.Errorf("mqttv5: PingTimeout %v must be shorter than KeepAlive %v", c.PingTimeout, keep)
	}
	if c.PingTimeout < 0 {
		return fmt.Errorf("mqttv5: negative PingTimeout %v", c.PingTimeout)
	}
	if !c.PublishMode.valid() {
		return fmt.Errorf("mqttv5: invalid PublishMode %d", c.PublishMode)
	}
	if !c.PublisherPoolRouting.valid() {
		return fmt.Errorf("mqttv5: invalid PoolRoutingPolicy %d", c.PublisherPoolRouting)
	}
	for _, d := range []struct {
		name string
		v    time.Duration
	}{{"ConnectTimeout", c.ConnectTimeout}, {"DisconnectFlushTimeout", c.DisconnectFlushTimeout}} {
		if d.v < 0 {
			return fmt.Errorf("mqttv5: negative %s %v", d.name, d.v)
		}
	}
	for _, n := range []struct {
		name string
		v    int
	}{{"WriteQueueSize", c.WriteQueueSize}, {"PublisherPoolSize", c.PublisherPoolSize}} {
		if n.v < 0 {
			return fmt.Errorf("mqttv5: negative %s %d", n.name, n.v)
		}
	}
	// Encode the CONNECT this configuration produces, so an invalid
	// client ID, user name, user property or Will fails here rather than
	// on every connection attempt.
	probe := wire.ConnectOpts{
		ClientID: c.ClientID, KeepAlive: c.KeepAlive, Username: c.Username, Password: c.Password,
		Will: c.WillMessage.wire(), UserProperties: c.ConnectUserProperties,
	}
	if _, err := wire.WriteConnect(io.Discard, probe); err != nil {
		return fmt.Errorf("mqttv5: CONNECT from these options: %w", err)
	}
	return nil
}
