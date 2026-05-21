// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package transport abstracts the network layer underneath the MQTT v5
// client. The Conn interface is io.ReadWriteCloser + the three deadline
// methods that net.Conn exposes, which is everything the client needs
// to drive a reader, a writer, and per-call timeouts.
//
// The Dial function dispatches on URL scheme:
//
//	mqtt://, tcp://         → TCP (DialTCP)
//	mqtts://, tls://, ssl:// → TLS  (DialTLS)
//
// WebSocket transports (ws://, wss://) are intentionally not in this
// package — they require a non-stdlib dependency and belong in a
// sibling extension package.
package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"
)

// Conn is the connection contract the MQTT client consumes. Every
// implementation in this package satisfies net.Conn, so callers that
// need the full net.Conn surface can type-assert when appropriate.
type Conn interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error

	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error

	// LocalAddr / RemoteAddr are kept for logging and metrics — every
	// concrete transport here can answer.
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// Compile-time assurance that net.Conn is a superset of our Conn.
var _ Conn = (net.Conn)(nil)

// DialFunc is the signature the mqttv5 client uses to open a new
// broker connection. Named here so extension transports (e.g.
// transport/ws) can return a value of this type from a helper
// constructor and have it satisfy mqttv5.WithDialFunc without the
// caller writing the signature out longhand.
type DialFunc func(ctx context.Context, brokerURL *url.URL) (Conn, error)

// DialOpts customises a Dial call. Zero value is valid (uses system
// defaults).
type DialOpts struct {
	// TLSConfig is required when dialing tls:// / mqtts:// / ssl://
	// and ignored otherwise. If nil for a TLS scheme, an empty
	// tls.Config is used (which means "verify against system roots,
	// SNI = host portion of the URL").
	TLSConfig *tls.Config

	// Dialer overrides the default net.Dialer if non-nil. Use this to
	// set custom KeepAlive, LocalAddr, or DualStack behaviour.
	Dialer *net.Dialer
}

// Errors that callers might want to branch on.
var (
	// ErrUnknownScheme is returned by Dial for an unrecognised URL
	// scheme.
	ErrUnknownScheme = errors.New("mqttv5/transport: unknown URL scheme")

	// ErrMissingHost is returned when the URL parses but lacks a host
	// component.
	ErrMissingHost = errors.New("mqttv5/transport: URL has no host")
)

// Dial parses rawURL and dispatches to the matching transport. ctx
// controls the dial deadline.
func Dial(ctx context.Context, rawURL string, opts DialOpts) (Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport: parse %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("%w: %s", ErrMissingHost, rawURL)
	}

	addr := defaultPort(u)

	switch u.Scheme {
	case "mqtt", "tcp", "":
		return DialTCP(ctx, addr, opts)
	case "mqtts", "tls", "ssl":
		return DialTLS(ctx, addr, hostname(u), opts)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownScheme, u.Scheme)
	}
}

// defaultPort fills in 1883 (mqtt) or 8883 (mqtts) when the URL omits
// the port.
func defaultPort(u *url.URL) string {
	if u.Port() != "" {
		return u.Host
	}
	switch u.Scheme {
	case "mqtts", "tls", "ssl":
		return net.JoinHostPort(u.Hostname(), "8883")
	default:
		return net.JoinHostPort(u.Hostname(), "1883")
	}
}

// hostname returns the host without any port, suitable for use as the
// TLS ServerName.
func hostname(u *url.URL) string { return u.Hostname() }
