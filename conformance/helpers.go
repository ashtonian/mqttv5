// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

// Package conformance houses tests that run mqttv5 against a real
// broker (mosquitto by default; emqx also supported via env). The
// suite is gated behind the `conformance` build tag so the default
// `go test ./...` does not require a broker to be running.
//
// To run:
//
//	docker compose -f conformance/docker-compose.yml up -d
//	go -C conformance test -tags conformance -race -v -timeout 120s
//	docker compose -f conformance/docker-compose.yml down
//
// MQTT_BROKER env var overrides the default mqtt://127.0.0.1:1883.
package conformance

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
)

const defaultBroker = "mqtt://127.0.0.1:1883"

// brokerURL returns the configured broker, defaulting to localhost
// mosquitto on 1883.
func brokerURL() string {
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		return v
	}
	return defaultBroker
}

// secondaryBrokerURL is used by multi-broker tests (e.g., ClientGroup).
// Defaults to the emqx port in our docker-compose.
func secondaryBrokerURL() string {
	if v := os.Getenv("MQTT_BROKER_2"); v != "" {
		return v
	}
	return "mqtt://127.0.0.1:1884"
}

// requireBroker skips the test when the broker is unreachable. Run the
// compose file before invoking the suite — see this package's docs.
func requireBroker(t *testing.T, url string) {
	t.Helper()
	host := stripScheme(url)
	c, err := net.DialTimeout("tcp", host, 500*time.Millisecond)
	if err != nil {
		t.Skipf("conformance: broker %s unreachable (%v) — start docker-compose first", url, err)
	}
	_ = c.Close()
}

func stripScheme(u string) string {
	// Cheap parse — these are well-known shapes (mqtt://host:port).
	for _, prefix := range []string{"mqtt://", "tcp://", "mqtts://", "tls://", "ssl://"} {
		if len(u) > len(prefix) && u[:len(prefix)] == prefix {
			return u[len(prefix):]
		}
	}
	return u
}

// connect builds a fresh, connected Client with sensible defaults.
// Caller defers cli.Disconnect.
func connect(t *testing.T, opts ...mqttv5.Option) *mqttv5.Client {
	t.Helper()
	requireBroker(t, brokerURL())

	all := []mqttv5.Option{
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(t.Name() + "-" + randSuffix()),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithConnectTimeout(5 * time.Second),
	}
	all = append(all, opts...)

	cli, err := mqttv5.New(all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = cli.Disconnect(shutdown)
	})
	return cli
}

// tcpProxy is a controllable TCP forwarder sitting between a test
// client and the real broker. Tests use it to simulate a network
// drop on a single client (Pause closes every live conn and refuses
// new ones; Resume re-enables forwarding) without killing the broker
// or reaching into client internals — the supervisor sees a real
// transport-level disconnect, enters its reconnect loop, and resumes
// once the proxy comes back up.
type tcpProxy struct {
	ln     net.Listener
	target string

	mu      sync.Mutex
	paused  bool
	conns   []net.Conn
}

// newTCPProxy listens on 127.0.0.1:0 and forwards every accepted
// connection to target (host:port). Cleanup is registered with t.
func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tcpProxy listen: %v", err)
	}
	p := &tcpProxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(func() {
		_ = p.ln.Close()
		p.Pause()
	})
	return p
}

// URL returns the mqtt:// URL clients should dial to reach the proxy.
func (p *tcpProxy) URL() string { return "mqtt://" + p.ln.Addr().String() }

// Pause closes every live forwarded connection and refuses new ones
// until Resume is called.
func (p *tcpProxy) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// Resume re-enables forwarding so the next accept opens a new
// upstream connection.
func (p *tcpProxy) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.paused {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		p.conns = append(p.conns, c, upstream)
		p.mu.Unlock()
		go func() { _, _ = io.Copy(upstream, c); _ = upstream.Close() }()
		go func() { _, _ = io.Copy(c, upstream); _ = c.Close() }()
	}
}

// randSuffix returns a short random string so tests don't collide on
// ClientID when running concurrently.
func randSuffix() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	now := time.Now().UnixNano()
	for i := range b {
		b[i] = charset[int(now)%len(charset)]
		now /= int64(len(charset))
	}
	return string(b)
}
