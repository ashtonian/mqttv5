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
// Both ws:// and wss:// are first-class. wss:// requires DialOpts
// to carry a *tls.Config — DialFunc / Dial fail before opening the
// socket if one is not supplied, so there is no silent downgrade.
//
// External dependency: github.com/gobwas/ws — chosen for its low
// per-frame allocation profile, in line with mqttv5's zero-alloc
// receive-path contract.
package ws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/ashtonian/mqttv5/transport"
	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// DefaultHandshakeTimeout is the WebSocket upgrade-handshake budget
// used when DialOpts.HandshakeTimeout is zero.
const DefaultHandshakeTimeout = 10 * time.Second

// DialOpts customises a Dial call. Zero value is valid.
type DialOpts struct {
	// TLSConfig is required for wss:// URLs and ignored otherwise.
	// Dial fails before opening the socket if TLSConfig is nil for a
	// wss:// scheme — no implicit downgrade to ws://.
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

	// NetDial overrides the underlying TCP dial. Use this to inject a
	// SOCKS / HTTP CONNECT proxy or a custom net.Dialer.
	NetDial func(ctx context.Context, network, addr string) (net.Conn, error)
}

// ErrMissingTLSConfig is returned by Dial when the URL scheme is
// wss:// but no TLSConfig was supplied. We never downgrade.
var ErrMissingTLSConfig = errors.New("mqttv5/transport/ws: wss:// requires DialOpts.TLSConfig")

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
	case "ws":
	case "wss":
		if opts.TLSConfig == nil {
			return nil, ErrMissingTLSConfig
		}
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

	conn, _, _, err := dialer.Dial(ctx, brokerURL.String())
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport/ws: dial %s: %w", brokerURL.String(), err)
	}
	return newConn(conn), nil
}

// Conn wraps a websocket connection so every Read/Write maps to one
// MQTT control packet. It is created by Dial; callers should treat
// the returned transport.Conn as opaque.
type Conn struct {
	raw    net.Conn
	reader *wsutil.Reader
}

func newConn(raw net.Conn) *Conn {
	c := &Conn{raw: raw}
	c.reader = &wsutil.Reader{
		Source: raw,
		State:  gws.StateClientSide,
		// Auto-handle pings: reply with Pong on the same conn.
		OnIntermediate: wsutil.ControlFrameHandler(raw, gws.StateClientSide),
	}
	return c
}

// Read returns the next available bytes from the MQTT byte stream.
// Each underlying WebSocket binary frame is one MQTT control packet,
// but the MQTT decoder reads field-at-a-time — so Read may be called
// many times per frame. At frame boundaries we transparently advance
// to the next data frame; control frames (Ping/Pong/Close) are
// handled inline so the caller never sees them.
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
			return n, err
		}
		h, err := c.reader.NextFrame()
		if err != nil {
			return 0, err
		}
		switch h.OpCode {
		case gws.OpClose:
			return 0, io.EOF
		case gws.OpBinary, gws.OpContinuation:
			// fall through and Read again
		default:
			// Unknown / non-data frame — skip its payload.
			if err := c.reader.Discard(); err != nil {
				return 0, err
			}
		}
	}
}

// Write emits p as one WebSocket binary frame (Fin=true, masked).
// The caller's buffer is not modified — wsutil copies before masking.
func (c *Conn) Write(p []byte) (int, error) {
	if err := wsutil.WriteClientBinary(c.raw, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close sends a Close frame (best effort) and closes the underlying
// TCP connection.
func (c *Conn) Close() error {
	// Best-effort Close frame; ignore errors — the next operation
	// will close the conn anyway.
	_ = gws.WriteHeader(c.raw, gws.Header{
		Fin:    true,
		OpCode: gws.OpClose,
		Masked: true,
		Mask:   gws.NewMask(),
	})
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
