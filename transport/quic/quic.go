// Package quic is the MQTT over QUIC transport for mqttv5, in Single
// Stream mode: one client-initiated bidirectional QUIC stream carries the
// unchanged MQTT byte stream, ALPN "mqtt", as "MQTT Over QUIC — Single
// Stream Mode Version 1.0" (OASIS MQTT TC Committee Note Draft 04)
// describes. It is a sibling submodule with its own go.mod so the core
// mqttv5 module stays standard-library only. Wire it up through
// [DialFunc]:
//
//	cli, _ := mqttv5.New(
//	    mqttv5.WithBroker("mqtt-quic://broker.example:14567"),
//	    mqttv5.WithDialFunc(quic.DialFunc(quic.DialOpts{})),
//	)
//
// Without DialOpts.TLSConfig the broker is verified against the system
// roots with the URL's host as the server name, as for mqtts://.
//
// External dependency: github.com/quic-go/quic-go.
package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/transport"
)

const (
	scheme      = "mqtt-quic"
	defaultPort = "14567"
	alpn        = "mqtt"

	// defaultFallbackPort is the TLS port a Fallback without a URL dials.
	defaultFallbackPort = "8883"

	// maxQUICPhase caps the QUIC attempts a dial with a Fallback makes
	// before it falls back (draft §6.6.1).
	maxQUICPhase = 10 * time.Second

	// codeBase is added to an MQTT reason code to form the QUIC
	// application error code that carries it (draft §6.3.3).
	codeBase = 0x300

	// alpnFailure is CRYPTO_ERROR no_application_protocol, the transport
	// error a failed ALPN negotiation closes the handshake with (RFC 9001
	// §8.1).
	alpnFailure quicgo.TransportErrorCode = 0x100 + 120
)

// networkRetryBackoff is the wait before each QUIC retry over a network
// failure when a Fallback is configured (draft §6.6.1).
var networkRetryBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

// Defaults for the zero values of DialOpts.
const (
	// DefaultHandshakeTimeout bounds one QUIC handshake (draft §6.5.1).
	DefaultHandshakeTimeout = 5 * time.Second

	// DefaultMaxIdleTimeout is the QUIC idle timeout. It exceeds the MQTT
	// keep-alive the client uses by default (30 s), as the draft requires
	// (§6.2).
	DefaultMaxIdleTimeout = 120 * time.Second

	// DefaultCloseTimeout bounds the wait for the broker to finish a
	// graceful close, and for it to retire a stream aborted over a
	// protocol error (draft §6.3.1, §6.7.2.3).
	DefaultCloseTimeout = 10 * time.Second

	// DefaultTakeoverBackoff is the wait before a Dialer dials again
	// after the broker reported Session Taken Over (draft §6.6.4).
	DefaultTakeoverBackoff = 5 * time.Second
)

// DialOpts customises a Dial. The zero value is valid.
type DialOpts struct {
	// TLSConfig configures the QUIC handshake, and the Fallback's TLS
	// connection. Nil verifies against the system roots; an empty
	// ServerName is filled in from the URL's host. For QUIC, NextProtos
	// is replaced by ["mqtt"] and TLS 1.3 is required; the Fallback uses
	// the config as given. The config is never modified. At most one of
	// TLSConfig and TLSConfigFunc is set.
	TLSConfig *tls.Config

	// TLSConfigFunc returns the TLS config for each Dial, before any
	// packet is sent and under the dial's ctx: rotate trusted roots or
	// client certificates without rebuilding the DialFunc. Its result is
	// used as TLSConfig would be, for the QUIC attempts and the Fallback
	// of that Dial.
	TLSConfigFunc transport.TLSConfigFunc

	// HandshakeTimeout bounds each QUIC handshake. Default
	// DefaultHandshakeTimeout.
	HandshakeTimeout time.Duration

	// MaxIdleTimeout closes a connection on which nothing has been
	// received for this long; the broker may negotiate it lower. It must
	// exceed the MQTT keep-alive. Default DefaultMaxIdleTimeout.
	MaxIdleTimeout time.Duration

	// KeepAlivePeriod is how often a QUIC PING is sent on an idle
	// connection, at most half the negotiated idle timeout. Default
	// MaxIdleTimeout / 4.
	KeepAlivePeriod time.Duration

	// CloseTimeout bounds the wait for the broker's FIN after Close, and
	// for the broker to close the connection after Abort. The wait runs
	// in the background; Dialer.Wait waits for it. Default
	// DefaultCloseTimeout.
	CloseTimeout time.Duration

	// Allow0RTT sends the MQTT CONNECT as 0-RTT early data when a
	// session ticket from an earlier connection to the broker is cached
	// (see Dialer). Only the first MQTT packet, which must be a CONNECT,
	// rides early data. Early data can be replayed; see the README
	// before enabling it.
	Allow0RTT bool

	// Fallback, when set, dials the broker over TCP/TLS when QUIC cannot
	// reach it (draft §6.6).
	Fallback *Fallback

	// TakeoverBackoff is how long a Dialer waits before dialing again
	// after the broker reported Session Taken Over on one of its
	// connections. Default DefaultTakeoverBackoff.
	TakeoverBackoff time.Duration

	// PacketConn, when set, is the UDP socket every connection uses
	// instead of a socket of its own: bind it to choose the local address
	// or interface. Dials on the same PacketConn, from any Dialer, share
	// it; nothing else may read it while their connections are open.
	// The caller owns it: closing a connection leaves it open.
	PacketConn net.PacketConn

	// Logger receives fallback and takeover events. Default
	// slog.Default().
	Logger *slog.Logger

	// clock paces retries, the takeover wait and the close timeouts;
	// tests inject a fake.
	clock clock.Clock
}

// Fallback is the TCP/TLS transport a dial falls back to when QUIC
// cannot reach the broker (draft §6.6). The fallback is always TLS: a
// plain TCP URL is refused with ErrFallbackNotTLS.
type Fallback struct {
	// URL is the broker's TLS endpoint: an mqtts://, tls:// or ssl://
	// URL. Empty means mqtts:// at the host of the mqtt-quic:// URL,
	// port 8883.
	URL string

	// Dialer overrides the TCP dial, e.g. to set a local address.
	Dialer *net.Dialer
}

// Errors callers can branch on.
var (
	// ErrALPN reports that the broker did not select the "mqtt"
	// application protocol: it does not speak MQTT over QUIC Single
	// Stream mode on that port.
	ErrALPN = errors.New("mqttv5/transport/quic: the broker did not select ALPN \"mqtt\"")

	// ErrConflictingTLSConfig is returned when DialOpts sets both
	// TLSConfig and TLSConfigFunc.
	ErrConflictingTLSConfig = errors.New("mqttv5/transport/quic: DialOpts sets both TLSConfig and TLSConfigFunc")

	// ErrFallbackNotTLS is returned when Fallback.URL is not a TLS URL:
	// the fallback never sends MQTT in the clear (draft §6.6.2).
	ErrFallbackNotTLS = errors.New("mqttv5/transport/quic: Fallback.URL must be mqtts://, tls:// or ssl://")
)

// Dialer dials mqtt-quic:// URLs. Its dials share a TLS session cache, so
// reconnects resume the TLS session and, with Allow0RTT, send the CONNECT
// as early data; an address-validation token store; and state across
// dials: whether QUIC has been failing over to the Fallback, and when the
// broker last reported Session Taken Over. Create one with NewDialer;
// it is safe for concurrent use.
type Dialer struct {
	opts   DialOpts
	clock  clock.Clock
	logger *slog.Logger

	// sessions resumes QUIC TLS sessions, fallbackSessions the
	// Fallback's TLS-over-TCP ones; both are nil for a one-shot Dial.
	sessions         tls.ClientSessionCache
	fallbackSessions tls.ClientSessionCache
	tokens           quicgo.TokenStore

	mu sync.Mutex
	// degraded records that the last dial fell back over network
	// failures, so the next makes one QUIC attempt before falling back.
	degraded bool
	// takeoverAt is when the broker last reported Session Taken Over.
	takeoverAt time.Time

	closing tracker
}

// NewDialer returns a Dialer for opts.
func NewDialer(opts DialOpts) *Dialer {
	d := newDialer(opts)
	// Zero capacity: the standard library's default.
	d.sessions = tls.NewLRUClientSessionCache(0)
	d.fallbackSessions = tls.NewLRUClientSessionCache(0)
	d.tokens = quicgo.NewLRUTokenStore(4, 4)
	return d
}

func newDialer(opts DialOpts) *Dialer {
	d := &Dialer{opts: opts, clock: opts.clock, logger: opts.Logger}
	if d.clock == nil {
		d.clock = clock.Real{}
	}
	if d.logger == nil {
		d.logger = slog.Default()
	}
	d.closing.init()
	return d
}

// DialFunc returns NewDialer(opts).Dial, for mqttv5.WithDialFunc:
//
//	mqttv5.WithDialFunc(quic.DialFunc(quic.DialOpts{TLSConfig: tlsCfg}))
func DialFunc(opts DialOpts) transport.DialFunc {
	return NewDialer(opts).Dial
}

// Dial makes one connection to brokerURL outside a Dialer: it resumes a
// TLS session only through the config's own ClientSessionCache, and keeps
// no state for later dials. Prefer DialFunc when wiring into
// mqttv5.WithDialFunc.
func Dial(ctx context.Context, brokerURL *url.URL, opts DialOpts) (transport.Conn, error) {
	return newDialer(opts).Dial(ctx, brokerURL)
}

// Dial opens a QUIC connection to brokerURL, an mqtt-quic:// URL, and its
// MQTT stream, falling back to TLS as DialOpts.Fallback says. ctx bounds
// the whole dial, the fallback included.
func (d *Dialer) Dial(ctx context.Context, brokerURL *url.URL) (transport.Conn, error) {
	target, err := parseTarget(brokerURL)
	if err != nil {
		return nil, err
	}
	fallback, err := parseFallback(d.opts.Fallback, target.host)
	if err != nil {
		return nil, err
	}
	tlsBase, err := d.tlsConfig(ctx, brokerURL)
	if err != nil {
		return nil, err
	}
	if err = d.awaitTakeoverBackoff(ctx, brokerURL); err != nil {
		return nil, err
	}

	conn, quicErr := d.dialQUICPhase(ctx, target, tlsBase, fallback != nil)
	if quicErr == nil {
		return conn, nil
	}
	if fallback == nil || ctx.Err() != nil || !fallsBack(quicErr) {
		return nil, fmt.Errorf("mqttv5/transport/quic: dial %s: %w", brokerURL, quicErr)
	}
	tlsConn, err := d.dialFallback(ctx, fallback, tlsBase)
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport/quic: dial %s: %w; TLS fallback to %s: %w", brokerURL, quicErr, fallback.url, err)
	}
	d.logger.Warn("mqttv5/transport/quic: QUIC unavailable, connected over TLS",
		slog.String("broker", brokerURL.String()),
		slog.String("fallback", fallback.url),
		slog.Any("error", quicErr))
	return tlsConn, nil
}

// Wait blocks until every connection the Dialer made has finished
// closing, including the graceful close that runs after Conn.Close
// returns, or until ctx ends. Call it on shutdown, after the client's
// Disconnect, so the broker sees each connection closed cleanly.
func (d *Dialer) Wait(ctx context.Context) error {
	return d.closing.wait(ctx)
}

// target is a parsed mqtt-quic:// URL.
type target struct {
	host string
	port uint16
}

func parseTarget(u *url.URL) (target, error) {
	if u == nil {
		return target{}, errors.New("mqttv5/transport/quic: nil URL")
	}
	if u.Scheme != scheme {
		return target{}, fmt.Errorf("mqttv5/transport/quic: unsupported scheme %q, want %s://", u.Scheme, scheme)
	}
	host := u.Hostname()
	if host == "" {
		return target{}, fmt.Errorf("%w: %s", transport.ErrMissingHost, u)
	}
	portStr := u.Port()
	if portStr == "" {
		portStr = defaultPort
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return target{}, fmt.Errorf("mqttv5/transport/quic: invalid port %q in %s", portStr, u)
	}
	return target{host: host, port: uint16(port)}, nil
}

// fallbackTarget is a validated Fallback.
type fallbackTarget struct {
	url        string
	addr       string
	serverName string
	dialer     *net.Dialer
}

func parseFallback(fb *Fallback, quicHost string) (*fallbackTarget, error) {
	if fb == nil {
		return nil, nil
	}
	raw := fb.URL
	if raw == "" {
		raw = "mqtts://" + net.JoinHostPort(quicHost, defaultFallbackPort)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport/quic: Fallback.URL %q: %w", raw, err)
	}
	if !transport.IsTLSScheme(u.Scheme) {
		return nil, fmt.Errorf("%w, got %q", ErrFallbackNotTLS, raw)
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("%w: Fallback.URL %q", transport.ErrMissingHost, raw)
	}
	port := u.Port()
	if port == "" {
		port = defaultFallbackPort
	}
	return &fallbackTarget{
		url:        raw,
		addr:       net.JoinHostPort(u.Hostname(), port),
		serverName: u.Hostname(),
		dialer:     fb.Dialer,
	}, nil
}

// tlsConfig resolves the TLS config for one Dial of brokerURL: the
// caller's, before the QUIC- or fallback-specific adjustments.
func (d *Dialer) tlsConfig(ctx context.Context, brokerURL *url.URL) (*tls.Config, error) {
	if d.opts.TLSConfigFunc == nil {
		return d.opts.TLSConfig, nil
	}
	if d.opts.TLSConfig != nil {
		return nil, ErrConflictingTLSConfig
	}
	cfg, err := d.opts.TLSConfigFunc(ctx, brokerURL)
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport/quic: TLS config: %w", err)
	}
	if cfg == nil {
		return nil, fmt.Errorf("mqttv5/transport/quic: %w", transport.ErrNilTLSConfig)
	}
	return cfg, nil
}

// awaitTakeoverBackoff waits out what is left of the takeover backoff,
// bounded by ctx (draft §6.3.3, §6.6.4).
func (d *Dialer) awaitTakeoverBackoff(ctx context.Context, brokerURL *url.URL) error {
	d.mu.Lock()
	at := d.takeoverAt
	d.mu.Unlock()
	if at.IsZero() {
		return nil
	}
	wait := at.Add(d.takeoverBackoff()).Sub(d.clock.Now())
	if wait <= 0 {
		return nil
	}
	d.logger.Info("mqttv5/transport/quic: waiting before reconnecting after a session takeover",
		slog.String("broker", brokerURL.String()), slog.Duration("wait", wait))
	return d.sleep(ctx, wait)
}

// noteTakeover records that the broker reported Session Taken Over.
func (d *Dialer) noteTakeover() {
	d.mu.Lock()
	d.takeoverAt = d.clock.Now()
	d.mu.Unlock()
}

func (d *Dialer) takeoverBackoff() time.Duration {
	if d.opts.TakeoverBackoff > 0 {
		return d.opts.TakeoverBackoff
	}
	return DefaultTakeoverBackoff
}

func (d *Dialer) sleep(ctx context.Context, wait time.Duration) error {
	timer := d.clock.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C():
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// dialQUICPhase makes the QUIC attempts of one Dial. Without a fallback
// that is one attempt: the client's reconnect loop does the retrying.
// With one, a network failure is retried after 1, 2 and 4 s, within at
// most 10 s and half of what is left of ctx, so the fallback keeps the
// other half (draft §6.6.1). After a dial that fell back, the next makes
// a single QUIC attempt before falling back, until QUIC connects again;
// it still tries QUIC first (draft §6.6.3).
func (d *Dialer) dialQUICPhase(ctx context.Context, t target, tlsBase *tls.Config, hasFallback bool) (*Conn, error) {
	if !hasFallback {
		return d.dialQUIC(ctx, t, tlsBase, d.handshakeTimeout())
	}
	start := d.clock.Now()
	budget := maxQUICPhase
	// ctx's deadline is wall-clock time; the budget it leaves is spent on
	// d.clock, which paces the retries.
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/2)
	}
	d.mu.Lock()
	retries := len(networkRetryBackoff)
	if d.degraded {
		retries = 0
	}
	d.mu.Unlock()

	var err error
	for attempt := 0; ; attempt++ {
		left := budget - d.clock.Now().Sub(start)
		if left <= 0 {
			break
		}
		var conn *Conn
		conn, err = d.dialQUIC(ctx, t, tlsBase, min(d.handshakeTimeout(), left))
		if err == nil {
			d.setDegraded(false)
			return conn, nil
		}
		if ctx.Err() != nil || !isNetworkFailure(err) || attempt >= retries {
			break
		}
		wait := networkRetryBackoff[attempt]
		if d.clock.Now().Sub(start)+wait >= budget {
			break
		}
		if serr := d.sleep(ctx, wait); serr != nil {
			return nil, serr
		}
	}
	if err == nil {
		err = context.DeadlineExceeded
	}
	if isNetworkFailure(err) {
		d.setDegraded(true)
	}
	return nil, err
}

func (d *Dialer) setDegraded(v bool) {
	d.mu.Lock()
	d.degraded = v
	d.mu.Unlock()
}

func (d *Dialer) handshakeTimeout() time.Duration {
	if d.opts.HandshakeTimeout > 0 {
		return d.opts.HandshakeTimeout
	}
	return DefaultHandshakeTimeout
}

func (d *Dialer) closeTimeout() time.Duration {
	if d.opts.CloseTimeout > 0 {
		return d.opts.CloseTimeout
	}
	return DefaultCloseTimeout
}

// quicConfig is the quic-go configuration every connection uses.
func (d *Dialer) quicConfig(handshakeTimeout time.Duration) *quicgo.Config {
	idle := d.opts.MaxIdleTimeout
	if idle <= 0 {
		idle = DefaultMaxIdleTimeout
	}
	keepAlive := d.opts.KeepAlivePeriod
	if keepAlive <= 0 {
		keepAlive = idle / 4
	}
	return &quicgo.Config{
		HandshakeIdleTimeout: handshakeTimeout,
		MaxIdleTimeout:       idle,
		KeepAlivePeriod:      keepAlive,
		// The broker must not open streams in Single Stream mode (draft
		// §5.2): refuse them in the transport parameters.
		MaxIncomingStreams:    -1,
		MaxIncomingUniStreams: -1,
		// Lets Abort deliver the DISCONNECT written before the reset,
		// when the broker supports it as well.
		EnableStreamResetPartialDelivery: true,
		TokenStore:                       d.tokens,
	}
}

// dialQUIC makes one QUIC attempt: the handshake within timeout, ALPN
// "mqtt", and the MQTT stream.
func (d *Dialer) dialQUIC(ctx context.Context, t target, tlsBase *tls.Config, timeout time.Duration) (*Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tlsCfg := transport.TLSClientConfig(tlsBase, t.host, d.sessions).Clone()
	tlsCfg.NextProtos = []string{alpn}
	qcfg := d.quicConfig(timeout)

	addr, err := resolve(ctx, t)
	if err != nil {
		return nil, err
	}
	qc, release, err := d.handshake(ctx, addr, tlsCfg, qcfg)
	if err != nil {
		return nil, handshakeError(err)
	}
	early := isEarly(qc)
	if !early && qc.ConnectionState().TLS.NegotiatedProtocol != alpn {
		_ = qc.CloseWithError(0, "")
		release()
		return nil, ErrALPN
	}
	stream, err := qc.OpenStreamSync(ctx)
	if err != nil {
		_ = qc.CloseWithError(0, "")
		release()
		return nil, fmt.Errorf("open stream: %w", err)
	}
	return newConn(d, qc, stream, early, release), nil
}

// handshake dials addr. release is to run once the connection has
// closed: it closes the connection's socket, or gives up its share of
// the caller's.
func (d *Dialer) handshake(ctx context.Context, addr *net.UDPAddr, tlsCfg *tls.Config, qcfg *quicgo.Config) (*quicgo.Conn, func(), error) {
	tr, release, err := d.transportFor(addr)
	if err != nil {
		return nil, nil, err
	}
	var qc *quicgo.Conn
	if d.opts.Allow0RTT {
		qc, err = tr.DialEarly(ctx, addr, tlsCfg, qcfg)
	} else {
		qc, err = tr.Dial(ctx, addr, tlsCfg, qcfg)
	}
	if err != nil {
		release()
		return nil, nil, err
	}
	return qc, release, nil
}

// transportFor returns the Transport to dial addr on: the caller's
// PacketConn's, shared with its other connections, or else one over a
// socket of the connection's own. release undoes it.
func (d *Dialer) transportFor(addr *net.UDPAddr) (tr *quicgo.Transport, release func(), err error) {
	if pc := d.opts.PacketConn; pc != nil {
		return sockets.acquire(pc)
	}

	// Not quic.DialAddr: it binds "udp" at 0.0.0.0, a dual-stack socket
	// that macOS and the BSDs let take a port an IPv4 socket bound to a
	// specific address already holds. That socket then receives the
	// broker's replies and the handshake stalls. A socket of the broker's
	// own address family cannot share the port.
	network := "udp4"
	if addr.IP.To4() == nil {
		network = "udp6"
	}
	sock, err := net.ListenUDP(network, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open UDP socket: %w", err)
	}
	tr = &quicgo.Transport{Conn: sock}
	release = func() {
		_ = tr.Close()
		_ = sock.Close()
	}
	return tr, release, nil
}

// isEarly reports whether qc was returned before its handshake
// completed, to send 0-RTT data.
func isEarly(qc *quicgo.Conn) bool {
	select {
	case <-qc.HandshakeComplete():
		return false
	default:
		return true
	}
}

// resolve looks up t's host within ctx, preferring IPv4 as
// net.ResolveUDPAddr does: a UDP dial has no fallback across address
// families.
func resolve(ctx context.Context, t target) (*net.UDPAddr, error) {
	if ip, err := netip.ParseAddr(t.host); err == nil {
		return net.UDPAddrFromAddrPort(netip.AddrPortFrom(ip, t.port)), nil
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", t.host)
	if err != nil {
		return nil, err
	}
	pick := ips[0]
	for _, ip := range ips {
		if ip.Unmap().Is4() {
			pick = ip
			break
		}
	}
	return net.UDPAddrFromAddrPort(netip.AddrPortFrom(pick.Unmap(), t.port)), nil
}

// handshakeError reports a failed ALPN negotiation as ErrALPN.
func handshakeError(err error) error {
	if te, ok := errors.AsType[*quicgo.TransportError](err); ok && te.ErrorCode == alpnFailure {
		return fmt.Errorf("%w: %w", ErrALPN, err)
	}
	return err
}

// isNetworkFailure reports whether err is a QUIC failure the draft calls
// a network failure (§6.6.1): the broker could not be reached, so a
// retry or the fallback may succeed. A TLS verification error is not
// one: it is not retried and does not fall back.
func isNetworkFailure(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if _, ok := errors.AsType[*quicgo.HandshakeTimeoutError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*quicgo.IdleTimeoutError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*quicgo.StatelessResetError](err); ok {
		return true
	}
	if te, ok := errors.AsType[*quicgo.TransportError](err); ok {
		return te.Remote && te.ErrorCode == quicgo.ConnectionRefused
	}
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return true
	}
	_, ok := errors.AsType[*net.OpError](err)
	return ok
}

// fallsBack reports whether a failed QUIC phase falls back to TLS: on a
// network failure, and at once when the endpoint is reachable but does
// not speak MQTT over QUIC (draft §6.6.1).
func fallsBack(err error) bool {
	if errors.Is(err, ErrALPN) {
		return true
	}
	if _, ok := errors.AsType[*quicgo.VersionNegotiationError](err); ok {
		return true
	}
	return isNetworkFailure(err)
}

// dialFallback connects to the Fallback over TLS.
func (d *Dialer) dialFallback(ctx context.Context, fb *fallbackTarget, tlsBase *tls.Config) (transport.Conn, error) {
	conn, err := transport.DialTLS(ctx, fb.addr, fb.serverName, transport.DialOpts{
		TLSConfig:    tlsBase,
		Dialer:       fb.dialer,
		SessionCache: d.fallbackSessions,
	})
	if err != nil {
		return nil, err
	}
	return &fallbackConn{Conn: conn, dialer: d}, nil
}

// fallbackConn is a Fallback's TLS connection. It reports a Session
// Taken Over DISCONNECT to the Dialer, as a QUIC connection does.
type fallbackConn struct {
	transport.Conn
	dialer *Dialer
}

var (
	_ transport.ConnectionStater   = (*fallbackConn)(nil)
	_ transport.DisconnectObserver = (*fallbackConn)(nil)
)

// ConnectionState returns the TLS handshake of the connection.
func (c *fallbackConn) ConnectionState() tls.ConnectionState {
	state, _ := transport.TLSState(c.Conn)
	return state
}

// ObserveDisconnect records a Session Taken Over from the broker.
func (c *fallbackConn) ObserveDisconnect(reason byte) {
	if reason == reasonSessionTakenOver {
		c.dialer.noteTakeover()
	}
}

// tracker counts the connections and close procedures that have not
// finished.
type tracker struct {
	mu   sync.Mutex
	n    int
	idle chan struct{} // closed while n == 0
}

func (t *tracker) init() {
	t.idle = make(chan struct{})
	close(t.idle)
}

func (t *tracker) add() {
	t.mu.Lock()
	if t.n == 0 {
		t.idle = make(chan struct{})
	}
	t.n++
	t.mu.Unlock()
}

func (t *tracker) done() {
	t.mu.Lock()
	t.n--
	if t.n == 0 {
		close(t.idle)
	}
	t.mu.Unlock()
}

func (t *tracker) wait(ctx context.Context) error {
	t.mu.Lock()
	idle := t.idle
	t.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
