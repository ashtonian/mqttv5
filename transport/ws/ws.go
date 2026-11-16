// Package ws is the WebSocket transport for mqttv5. It is a sibling
// submodule with its own go.mod so the core mqttv5 module stays
// stdlib-only. Wire it up via [DialFunc]:
//
//	cli, _ := mqttv5.New(
//	    mqttv5.WithBroker("wss://broker.example/mqtt"),
//	    mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{
//	        TLSConfig: tlsCfg,
//	    })),
//	)
//
// Both ws:// and wss:// are first-class. Like mqtts://, wss:// without
// a DialOpts.TLSConfig verifies the broker against the system roots
// with the URL's host as the server name.
//
// External dependency: github.com/gobwas/ws — chosen for its low
// per-frame allocation profile, in line with mqttv5's allocation-free
// decoder.
package ws

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/transport"
)

// DefaultHandshakeTimeout is the WebSocket upgrade-handshake budget
// used when DialOpts.HandshakeTimeout is zero.
const DefaultHandshakeTimeout = 10 * time.Second

// DefaultCloseFrameTimeout bounds the closing handshake Conn.Close
// starts, when DialOpts.CloseFrameTimeout is zero.
const DefaultCloseFrameTimeout = time.Second

// readBufferSize is the read window over the socket: frame headers and
// small frames come out of it, not one read each.
const readBufferSize = 4096

// DialOpts customises a Dial call. Zero value is valid.
type DialOpts struct {
	// TLSConfig configures TLS for wss:// URLs and is ignored for
	// ws://. Nil verifies against the system roots; an empty ServerName
	// is filled in from the URL's host. At most one of TLSConfig and
	// TLSConfigFunc is set.
	TLSConfig *tls.Config

	// TLSConfigFunc returns the TLS config for each wss:// dial, before
	// the TCP dial and under the dial's ctx: rotate trusted roots or
	// client certificates without rebuilding the DialFunc. Its result
	// is used as TLSConfig would be. Not called for ws://.
	TLSConfigFunc transport.TLSConfigFunc

	// HTTPHeaders are added to the HTTP upgrade request — useful for
	// auth tokens, custom routing headers, etc.
	HTTPHeaders http.Header

	// Subprotocols sent in Sec-WebSocket-Protocol. Defaults to
	// []string{"mqtt"} per MQTT v5 §6.
	Subprotocols []string

	// HandshakeTimeout bounds the upgrade handshake. Defaults to
	// DefaultHandshakeTimeout.
	HandshakeTimeout time.Duration

	// CloseFrameTimeout bounds the closing handshake: writing the Close
	// frame, then waiting for the broker's before the TCP connection is
	// closed. Defaults to DefaultCloseFrameTimeout.
	CloseFrameTimeout time.Duration

	// NetDial overrides the underlying TCP dial. Use this to inject a
	// SOCKS / HTTP CONNECT proxy or a custom net.Dialer.
	NetDial func(ctx context.Context, network, addr string) (net.Conn, error)

	// clock paces the closing handshake; nil is the wall clock.
	clock clock.Clock
}

// DialFunc binds opts and returns a [transport.DialFunc] that's
// directly compatible with mqttv5.WithDialFunc — typical usage:
//
//	mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg}))
//
// Its wss:// dials share one in-memory TLS session cache, so the
// client's reconnects resume the TLS session, for configs without a
// ClientSessionCache of their own that present no client certificate
// (see [transport.TLSClientConfig]). Otherwise it is [Dial].
func DialFunc(opts DialOpts) transport.DialFunc {
	// Zero capacity: the standard library's default.
	sessions := tls.NewLRUClientSessionCache(0)
	return func(ctx context.Context, brokerURL *url.URL) (transport.Conn, error) {
		return dial(ctx, brokerURL, opts, sessions)
	}
}

// Dial performs the WebSocket upgrade against brokerURL and returns a
// transport.Conn that frames every Read/Write as a single MQTT
// control packet (per MQTT-over-WS §6 — "single WebSocket data
// frame"). Prefer [DialFunc] when wiring into mqttv5.WithDialFunc;
// call Dial directly when you need a one-shot upgrade outside the
// client. Dial resumes a TLS session only through the config's own
// ClientSessionCache.
func Dial(ctx context.Context, brokerURL *url.URL, opts DialOpts) (transport.Conn, error) {
	return dial(ctx, brokerURL, opts, nil)
}

// dial is Dial with sessions as the TLS session cache for configs that
// have none (see transport.TLSClientConfig).
func dial(ctx context.Context, brokerURL *url.URL, opts DialOpts, sessions tls.ClientSessionCache) (transport.Conn, error) {
	if brokerURL == nil {
		return nil, errors.New("mqttv5/transport/ws: nil URL")
	}
	switch brokerURL.Scheme {
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("mqttv5/transport/ws: unsupported scheme %q", brokerURL.Scheme)
	}

	tlsCfg, err := tlsConfig(ctx, brokerURL, opts)
	if err != nil {
		return nil, err
	}
	if brokerURL.Scheme == "wss" {
		tlsCfg = transport.TLSClientConfig(tlsCfg, brokerURL.Hostname(), sessions)
	}

	subprotocols := opts.Subprotocols
	if len(subprotocols) == 0 {
		subprotocols = []string{"mqtt"}
	}
	timeout := opts.HandshakeTimeout
	if timeout == 0 {
		timeout = DefaultHandshakeTimeout
	}

	dialer := gws.Dialer{
		Protocols: subprotocols,
		Timeout:   timeout,
		TLSConfig: tlsCfg,
		NetDial:   opts.NetDial,
	}
	if len(opts.HTTPHeaders) > 0 {
		dialer.Header = gws.HandshakeHeaderHTTP(opts.HTTPHeaders)
	}

	conn, buffered, hs, err := dialer.Dial(ctx, brokerURL.String())
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport/ws: dial %s: %w", brokerURL.String(), err)
	}
	// The dialer has checked that a selected subprotocol is one offered;
	// one must be selected [MQTT-6.0.0-4].
	if hs.Protocol == "" {
		_ = conn.Close()
		return nil, fmt.Errorf("mqttv5/transport/ws: dial %s: %w", brokerURL.String(), ErrNoSubprotocol)
	}
	closeTimeout := opts.CloseFrameTimeout
	if closeTimeout == 0 {
		closeTimeout = DefaultCloseFrameTimeout
	}
	clk := opts.clock
	if clk == nil {
		clk = clock.Real{}
	}
	return newConn(conn, buffered, closeTimeout, clk), nil
}

// tlsConfig resolves the TLS config for one dial of brokerURL.
func tlsConfig(ctx context.Context, brokerURL *url.URL, opts DialOpts) (*tls.Config, error) {
	if opts.TLSConfigFunc == nil {
		return opts.TLSConfig, nil
	}
	if opts.TLSConfig != nil {
		return nil, ErrConflictingTLSConfig
	}
	if brokerURL.Scheme != "wss" {
		return nil, nil
	}
	cfg, err := opts.TLSConfigFunc(ctx, brokerURL)
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport/ws: TLS config: %w", err)
	}
	if cfg == nil {
		return nil, fmt.Errorf("mqttv5/transport/ws: %w", transport.ErrNilTLSConfig)
	}
	return cfg, nil
}

// ErrConflictingTLSConfig is returned by [Dial] when DialOpts sets both
// TLSConfig and TLSConfigFunc.
var ErrConflictingTLSConfig = errors.New("mqttv5/transport/ws: DialOpts sets both TLSConfig and TLSConfigFunc")

// ErrTextFrame reports a WebSocket text frame from the broker. MQTT
// travels only in binary frames; any other data frame must close the
// connection [MQTT-6.0.0-1], which the client does with status 1003.
var ErrTextFrame = errors.New("mqttv5/transport/ws: text frame from the broker")

// ErrNoSubprotocol is returned by [Dial] when the broker's handshake
// response selects no WebSocket subprotocol: an MQTT broker selects
// "mqtt" [MQTT-6.0.0-4], so the endpoint is not one.
var ErrNoSubprotocol = errors.New("mqttv5/transport/ws: the broker selected no WebSocket subprotocol")

// CloseError is the status of a Close frame the broker ended the
// connection with, when that is not Normal Closure (1000) or no status.
// Read returns io.EOF for those.
type CloseError struct {
	Code   uint16
	Reason string
}

func (e *CloseError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("mqttv5/transport/ws: broker closed the WebSocket with status %d", e.Code)
	}
	return fmt.Sprintf("mqttv5/transport/ws: broker closed the WebSocket with status %d: %s", e.Code, e.Reason)
}

// Conn wraps a websocket connection so every Write is one WebSocket
// frame holding one MQTT control packet. It is created by Dial; callers
// should treat the returned transport.Conn as opaque.
type Conn struct {
	raw    net.Conn
	reader *wsutil.Reader

	// rmu serialises Read with the wait for the broker's Close after
	// Close: both read frames.
	rmu sync.Mutex

	// wmu keeps frames whole: the client's writer and the pongs and
	// Close frames Read sends write to the same connection. closeSent,
	// which it guards, ends writing: no frame follows a Close (RFC 6455
	// §5.5.1).
	wmu       sync.Mutex
	closeSent bool

	// closing is set by Close or Abort; peerClosed once the broker's
	// Close frame has arrived.
	closing    atomic.Bool
	peerClosed atomic.Bool

	closeTimeout time.Duration
	clock        clock.Clock
}

var (
	_ transport.ConnectionStater = (*Conn)(nil)
	_ transport.Aborter          = (*Conn)(nil)
)

// ConnectionState returns the TLS state of a wss:// connection. For
// ws:// it returns the zero state, which [transport.TLSState] reports as
// no TLS.
func (c *Conn) ConnectionState() tls.ConnectionState {
	if tc, ok := c.raw.(*tls.Conn); ok {
		return tc.ConnectionState()
	}
	return tls.ConnectionState{}
}

// newConn wraps raw. buffered, when not nil, holds frames the server
// sent right behind its handshake response, read along with it; they
// come first.
func newConn(raw net.Conn, buffered *bufio.Reader, closeTimeout time.Duration, clk clock.Clock) *Conn {
	c := &Conn{raw: raw, closeTimeout: closeTimeout, clock: clk}
	src := io.Reader(raw)
	if buffered != nil {
		src = io.MultiReader(buffered, raw)
	}
	c.reader = &wsutil.Reader{
		Source: bufio.NewReaderSize(src, readBufferSize),
		State:  gws.StateClientSide,
		// Control frames between the fragments of a message.
		OnIntermediate: c.control,
	}
	return c
}

// frameWriter writes what the control-frame handler produces — one
// Write per frame — under the connection's write lock, and nothing once
// a Close frame has gone.
type frameWriter struct{ c *Conn }

func (w frameWriter) Write(p []byte) (int, error) {
	w.c.wmu.Lock()
	defer w.c.wmu.Unlock()
	if w.c.closeSent {
		return len(p), nil
	}
	return w.c.raw.Write(p)
}

// Read returns the next available bytes from the MQTT byte stream.
// Each underlying WebSocket binary frame is one MQTT control packet,
// but the MQTT decoder reads field-at-a-time — so Read may be called
// many times per frame. Between frames it answers Ping with Pong and a
// Close with Close, then reports io.EOF, or a [*CloseError] when the
// broker's status is not Normal Closure. A text frame is
// [ErrTextFrame]. After Close or Abort, Read reports net.ErrClosed.
func (c *Conn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if c.closing.Load() {
		return 0, net.ErrClosed
	}
	return c.read(p)
}

// read is Read under rmu.
func (c *Conn) read(p []byte) (int, error) {
	for {
		n, err := c.reader.Read(p)
		if n > 0 {
			return n, nil
		}
		// wsutil.Reader signals "no frame currently positioned" via
		// ErrNoFrameAdvance, and "current frame payload consumed"
		// via io.EOF. Both mean "fetch the next frame".
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, wsutil.ErrNoFrameAdvance) {
			return 0, err
		}
		h, err := c.reader.NextFrame()
		if err != nil {
			if perr, ok := errors.AsType[gws.ProtocolError](err); ok {
				c.sendClose(gws.StatusProtocolError, perr.Error())
			}
			return 0, err
		}
		switch {
		case h.OpCode.IsControl():
			if err := c.control(h, c.reader); err != nil {
				return 0, err
			}
		case h.OpCode == gws.OpText:
			c.sendClose(gws.StatusUnsupportedData, "MQTT travels in binary frames")
			return 0, ErrTextFrame
		}
	}
}

// control handles the control frame h, whose payload r holds.
func (c *Conn) control(h gws.Header, r io.Reader) error {
	if h.OpCode == gws.OpClose {
		return c.brokerClose(h, r)
	}
	return wsutil.ControlHandler{Src: r, Dst: frameWriter{c}, State: gws.StateClientSide}.Handle(h)
}

// brokerClose handles the broker's Close frame: it answers one the
// client did not start, echoing its status (RFC 6455 §5.5.1), and ends
// the stream.
func (c *Conn) brokerClose(h gws.Header, r io.Reader) error {
	payload := make([]byte, h.Length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	c.peerClosed.Store(true)
	if len(payload) == 0 {
		c.sendClose(gws.StatusNoStatusRcvd, "")
		return io.EOF
	}
	code, reason := gws.ParseCloseFrameData(payload)
	if err := gws.CheckCloseFrameData(code, reason); err != nil {
		c.sendClose(gws.StatusProtocolError, err.Error())
		return fmt.Errorf("mqttv5/transport/ws: the broker's Close frame: %w", err)
	}
	c.sendClose(code, "")
	if code == gws.StatusNormalClosure {
		return io.EOF
	}
	return &CloseError{Code: uint16(code), Reason: reason}
}

// sendClose writes a Close frame with code, unless one has gone.
func (c *Conn) sendClose(code gws.StatusCode, reason string) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.writeCloseLocked(code, reason)
}

// writeCloseLocked is sendClose under wmu. StatusNoStatusRcvd sends a
// Close without a body. reason must fit a control frame (123 bytes).
func (c *Conn) writeCloseLocked(code gws.StatusCode, reason string) {
	if c.closeSent {
		return
	}
	c.closeSent = true
	var body []byte
	if code != gws.StatusNoStatusRcvd {
		body = gws.NewCloseFrameBody(code, reason)
	}
	frame, err := gws.CompileFrame(gws.MaskFrameInPlace(gws.NewCloseFrame(body)))
	if err != nil {
		return
	}
	_, _ = c.raw.Write(frame)
}

// framePool recycles frame buffers up to maxPooledFrame bytes.
var framePool = sync.Pool{New: func() any { return new([]byte) }}

const maxPooledFrame = 64 << 10

// Write emits p as one masked WebSocket binary frame in a single write
// to the connection. p is not modified. After Close or Abort, Write
// reports net.ErrClosed.
func (c *Conn) Write(p []byte) (int, error) {
	h := gws.Header{Fin: true, OpCode: gws.OpBinary, Masked: true, Mask: gws.NewMask(), Length: int64(len(p))}
	size := gws.HeaderSize(h) + len(p)
	bp := framePool.Get().(*[]byte)
	if cap(*bp) < size {
		*bp = make([]byte, 0, size)
	}
	b := bytes.NewBuffer((*bp)[:0])
	if err := gws.WriteHeader(b, h); err != nil {
		framePool.Put(bp)
		return 0, err
	}
	frame := append(b.Bytes(), p...)
	gws.Cipher(frame[len(frame)-len(p):], h.Mask, 0)

	c.wmu.Lock()
	err := net.ErrClosed
	if !c.closeSent {
		_, err = c.raw.Write(frame)
	}
	c.wmu.Unlock()

	*bp = frame[:0]
	if cap(frame) <= maxPooledFrame {
		framePool.Put(bp)
	}
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close starts the closing handshake (RFC 6455 §7.1.2) and returns: it
// sends a Close frame with Normal Closure, and the TCP connection closes
// once the broker's Close arrives, the broker closes it, or
// DialOpts.CloseFrameTimeout passes, whichever is first. A write in
// progress is not waited for: it may be stalled behind a broker that
// stopped reading, and closing the TCP connection at once is what ends
// it.
func (c *Conn) Close() error {
	if !c.closing.CompareAndSwap(false, true) {
		return net.ErrClosed
	}
	if !c.wmu.TryLock() {
		return c.raw.Close()
	}
	_ = c.raw.SetWriteDeadline(time.Now().Add(c.closeTimeout))
	c.writeCloseLocked(gws.StatusNormalClosure, "")
	c.wmu.Unlock()
	if c.peerClosed.Load() {
		return c.raw.Close()
	}
	go c.awaitBrokerClose()
	return nil
}

// awaitBrokerClose reads, discarding data frames, until the broker's
// Close frame or the end of the connection, for at most closeTimeout;
// then it closes the TCP connection. The broker closes its side first,
// as RFC 6455 §7.1.1 has it, unless it is late.
func (c *Conn) awaitBrokerClose() {
	timer := c.clock.NewTimer(c.closeTimeout)
	defer timer.Stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.rmu.Lock()
		defer c.rmu.Unlock()
		buf := make([]byte, 512)
		for !c.peerClosed.Load() {
			if _, err := c.read(buf); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-timer.C():
		// A deadline in the past ends the read in progress, this one's or
		// a Read that started before Close.
		_ = c.raw.SetReadDeadline(time.Unix(1, 0))
		<-done
	}
	_ = c.raw.Close()
}

// Abort ends the connection over a protocol violation, after the
// client's DISCONNECT: a Close frame with Protocol Error (1002) naming
// the MQTT reason code, then the TCP connection closes at once.
func (c *Conn) Abort(reason byte) error {
	if !c.closing.CompareAndSwap(false, true) {
		return net.ErrClosed
	}
	if c.wmu.TryLock() {
		_ = c.raw.SetWriteDeadline(time.Now().Add(c.closeTimeout))
		c.writeCloseLocked(gws.StatusProtocolError, fmt.Sprintf("MQTT reason code 0x%02X", reason))
		c.wmu.Unlock()
	}
	return c.raw.Close()
}

// SetDeadline forwards to the underlying connection.
func (c *Conn) SetDeadline(t time.Time) error { return c.raw.SetDeadline(t) }

// SetReadDeadline forwards to the underlying connection.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.raw.SetReadDeadline(t) }

// SetWriteDeadline forwards to the underlying connection.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }

// LocalAddr forwards to the underlying connection.
func (c *Conn) LocalAddr() net.Addr { return c.raw.LocalAddr() }

// RemoteAddr forwards to the underlying connection.
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }
