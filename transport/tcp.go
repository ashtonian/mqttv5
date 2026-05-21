// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"fmt"
	"net"
)

// DialTCP dials addr (host:port) using opts.Dialer (or a default
// net.Dialer) honouring ctx as the dial deadline.
//
// Returns the raw *net.TCPConn — callers that want to assert
// (*net.TCPConn).WriteBuffers (zero-copy vectored write) can do so via
// type assertion after the fact.
func DialTCP(ctx context.Context, addr string, opts DialOpts) (Conn, error) {
	dialer := opts.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}
	c, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport: dial tcp %s: %w", addr, err)
	}
	return c, nil
}
