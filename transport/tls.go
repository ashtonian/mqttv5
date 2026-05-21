// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
)

// DialTLS dials addr (host:port) over TCP, then performs a TLS
// handshake using opts.TLSConfig.
//
// serverName is set as ServerName on a cloned TLS config when the
// caller's config has none — this is what enables proper SAN
// validation when the URL hostname differs from the connect address
// (rare but valid: e.g., a load-balancer fronting a cert with a SAN
// list).
//
// ctx bounds both the dial and the handshake.
func DialTLS(ctx context.Context, addr, serverName string, opts DialOpts) (Conn, error) {
	rawDialer := opts.Dialer
	if rawDialer == nil {
		rawDialer = &net.Dialer{}
	}
	rawConn, err := rawDialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mqttv5/transport: dial tcp %s: %w", addr, err)
	}

	cfg := opts.TLSConfig
	if cfg == nil {
		cfg = &tls.Config{}
	}
	if cfg.ServerName == "" && serverName != "" {
		cfg = cfg.Clone()
		cfg.ServerName = serverName
	}

	tlsConn := tls.Client(rawConn, cfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("mqttv5/transport: tls handshake %s: %w", addr, err)
	}
	return tlsConn, nil
}
