// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

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
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/ashtonian/mqttv5/transport"
)

// DefaultHandshakeTimeout is the WebSocket upgrade-handshake budget
// used when DialOpts.HandshakeTimeout is zero.
const DefaultHandshakeTimeout = 10 * time.Second

// DefaultCloseFrameTimeout bounds the Close frame Conn.Close sends, when
// DialOpts.CloseFrameTimeout is zero.
const DefaultCloseFrameTimeout = time.Second

// DialOpts customises a Dial call. Zero value is valid.
type DialOpts struct {
	// TLSConfig configures TLS for wss:// URLs and is ignored for
	// ws://. Nil verifies against the system roots; an empty ServerName
	// is filled in from the URL's host.
	TLSConfig *tls.Config

	// HTTPHeaders are added to the HTTP upgrade request — useful for
	// auth tokens, custom routing headers, etc.
	HTTPHeaders http.Header

	// Subprotocols sent in Sec-WebSocket-Protocol. Defaults to
	// []string{"mqtt"} per MQTT v5 §6.
	Subprotocols []string

	// HandshakeTimeout bounds the upgrade handshake. Defaults to
	// DefaultHandshakeTimeout.
	HandshakeTimeout time.Duration

	// CloseFrameTimeout bounds the Close frame sent when the connection
	// is closed. Defaults to DefaultCloseFrameTimeout.
	CloseFrameTimeout time.Duration

	// NetDial overrides the underlying TCP dial. Use this to inject a
	// SOCKS / HTTP CONNECT proxy or a custom net.Dialer.
	NetDial func(ctx context.Context, network, addr string) (net.Conn, error)
}

// DialFunc binds opts and returns a [transport.DialFunc] that's
// directly compatible with mqttv5.WithDialFunc — typical usage:
//
//	mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg}))
//
// Equivalent to writing the closure by hand around [Dial]; provided
// because the closure form reads poorly when several broker options
// are stacked together.
func DialFunc(opts DialOpts) transport.DialFunc {
	return func(ctx context.Context, brokerURL *url.URL) (transport.Conn, error) {
		return Dial(ctx, brokerURL, opts)
	}
}

// Dial performs the WebSocket upgrade against brokerURL and returns a
// transport.Conn that frames every Read/Write as a single MQTT
// control packet (per MQTT-over-WS §6 — "single WebSocket data
// frame"). Prefer [DialFunc] when wiring into mqttv5.WithDialFunc;
// call Dial directly when you need a one-shot upgrade outside the
// client.
func Dial(ctx context.Context, brokerURL *url.URL, opts DialOpts) (transport.Conn, error) {
	if brokerURL == nil {
		return nil, errors.New("mqttv5/transport/ws: nil URL")
	}
	switch brokerURL.Scheme {
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("mqttv5/transport/ws: unsupported scheme %q", brokerURL.Scheme)
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
		TLSConfig: opts.TLSConfig,
		NetDial:   opts.NetDial,
	}
	if len(opts.HTTPHeaders) > 0 {
		dialer.Header = gws.HandshakeHeaderHTTP(opts.HTTPHeaders)
	}

	conn, buffered, _, err := dialer.Dial(ctx, brokerURL.String())
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport/ws: dial %s: %w", brokerURL.String(), err)
	}
	closeTimeout := opts.CloseFrameTimeout
	if closeTimeout == 0 {
		closeTimeout = DefaultCloseFrameTimeout
	}
	return newConn(conn, buffered, closeTimeout), nil
}

// ErrTextFrame reports a WebSocket text frame from the broker. MQTT
// travels only in binary frames; any other data frame must close the
// connection [MQTT-6.0.0-1].
var ErrTextFrame = errors.New("mqttv5/transport/ws: text frame from the broker")

// Conn wraps a websocket connection so every Write is one WebSocket
// frame holding one MQTT control packet. It is created by Dial; callers
// should treat the returned transport.Conn as opaque.
type Conn struct {
	raw     net.Conn
	reader  *wsutil.Reader
	control wsutil.FrameHandlerFunc

	// wmu keeps frames whole: the client's writer and the pongs and
	// close replies Read sends write to the same connection.
	wmu sync.Mutex

	closeTimeout time.Duration
}

// newConn wraps raw. buffered, when not nil, holds frames the server
// sent right behind its handshake response, read along with it; they
// come first.
func newConn(raw net.Conn, buffered *bufio.Reader, closeTimeout time.Duration) *Conn {
	c := &Conn{raw: raw, closeTimeout: closeTimeout}
	c.control = wsutil.ControlFrameHandler(frameWriter{c}, gws.StateClientSide)
	src := io.Reader(raw)
	if buffered != nil {
		src = io.MultiReader(buffered, raw)
	}
	c.reader = &wsutil.Reader{
		Source: src,
		State:  gws.StateClientSide,
		// Control frames between the fragments of a message.
		OnIntermediate: c.control,
	}
	return c
}

// frameWriter writes what the control-frame handler produces — one
// Write per frame — under the connection's write lock.
type frameWriter struct{ c *Conn }

func (w frameWriter) Write(p []byte) (int, error) {
	w.c.wmu.Lock()
	defer w.c.wmu.Unlock()
	return w.c.raw.Write(p)
}

// Read returns the next available bytes from the MQTT byte stream.
// Each underlying WebSocket binary frame is one MQTT control packet,
// but the MQTT decoder reads field-at-a-time — so Read may be called
// many times per frame. Between frames it answers Ping with Pong and
// Close with Close (then reports io.EOF); a text frame is an error.
func (c *Conn) Read(p []byte) (int, error) {
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
			return 0, err
		}
		switch {
		case h.OpCode.IsControl():
			if err := c.control(h, c.reader); err != nil {
				var closed wsutil.ClosedError
				if errors.As(err, &closed) {
					return 0, io.EOF
				}
				return 0, err
			}
		case h.OpCode == gws.OpText:
			return 0, ErrTextFrame
		}
	}
}

// framePool recycles frame buffers up to maxPooledFrame bytes.
var framePool = sync.Pool{New: func() any { return new([]byte) }}

const maxPooledFrame = 64 << 10

// Write emits p as one masked WebSocket binary frame in a single write
// to the connection. p is not modified.
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
	_, err := c.raw.Write(frame)
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

// Close closes the underlying connection, first sending a Close frame
// when no write is in progress, within DialOpts.CloseFrameTimeout. A
// write in progress is not waited for: it may be stalled behind a peer
// that stopped reading, and closing the connection is what ends it.
func (c *Conn) Close() error {
	if c.wmu.TryLock() {
		_ = c.raw.SetWriteDeadline(time.Now().Add(c.closeTimeout))
		_ = gws.WriteHeader(c.raw, gws.Header{
			Fin:    true,
			OpCode: gws.OpClose,
			Masked: true,
			Mask:   gws.NewMask(),
		})
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
