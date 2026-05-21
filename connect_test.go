// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// connectInspector returns a fakeBroker handler that captures the first
// CONNECT packet's decoded form into the returned channel. After
// CONNACK, the connection is held open until the broker is closed so
// the test can run normal lifecycle teardown.
func connectInspector(t *testing.T, sink chan<- *wire.Connect, connack wire.ConnackOpts) func(*fakeBroker, net.Conn) {
	return func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			t.Errorf("CONNECT read: %v", err)
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			t.Errorf("got %s, want CONNECT", pkt.Type())
			pkt.Release()
			return
		}
		sink <- conn
		if _, err := wire.WriteConnack(c, connack); err != nil {
			t.Errorf("CONNACK write: %v", err)
			return
		}
		<-fb.Done()
	}
}

func TestConnectExposesV5Properties(t *testing.T) {
	sink := make(chan *wire.Connect, 1)
	fb := newFakeBroker(t, connectInspector(t, sink, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}))

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("connect-props-test"),
		WithMaximumPacketSize(1024*1024),
		WithInboundTopicAliasMaximum(32),
		WithRequestResponseInformation(true),
		WithRequestProblemInformation(true),
		WithConnectUserProperty("tenant", "acme"),
		WithConnectUserProperty("region", "us-east-1"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	conn := <-sink
	defer conn.Release()

	maxSize, ok := conn.Properties.Uint32(wire.PropMaximumPacketSize)
	if !ok || maxSize != 1024*1024 {
		t.Errorf("MaximumPacketSize = (%d, %v), want (1048576, true)", maxSize, ok)
	}
	aliasMax, ok := conn.Properties.Uint16(wire.PropTopicAliasMaximum)
	if !ok || aliasMax != 32 {
		t.Errorf("TopicAliasMaximum = (%d, %v), want (32, true)", aliasMax, ok)
	}
	rri, ok := conn.Properties.Byte(wire.PropRequestResponseInfo)
	if !ok || rri != 1 {
		t.Errorf("RequestResponseInformation = (%d, %v), want (1, true)", rri, ok)
	}
	rpi, ok := conn.Properties.Byte(wire.PropRequestProblemInfo)
	if !ok || rpi != 1 {
		t.Errorf("RequestProblemInformation = (%d, %v), want (1, true)", rpi, ok)
	}
	props := map[string]string{}
	for k, v := range conn.Properties.UserProperties() {
		props[k] = v
	}
	if props["tenant"] != "acme" || props["region"] != "us-east-1" {
		t.Errorf("UserProperties = %+v, want tenant=acme region=us-east-1", props)
	}
}

func TestConnectPacketBuilderMutates(t *testing.T) {
	sink := make(chan *wire.Connect, 1)
	fb := newFakeBroker(t, connectInspector(t, sink, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}))

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("from-config"),
		WithConnectPacketBuilder(func(ctx context.Context, opts *wire.ConnectOpts) error {
			opts.Username = "from-builder"
			opts.Password = []byte("rotated-token")
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	conn := <-sink
	defer conn.Release()
	if conn.Username != "from-builder" {
		t.Errorf("Username = %q, want from-builder", conn.Username)
	}
	if string(conn.Password) != "rotated-token" {
		t.Errorf("Password = %q, want rotated-token", conn.Password)
	}
}

func TestConnectPacketBuilderErrorFailsAttempt(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		_, _ = c.Read(make([]byte, 64))
	})
	wantErr := errors.New("builder rejected the attempt")
	cli, err := New(
		WithBroker(fb.URL()),
		WithConnectPacketBuilder(func(ctx context.Context, opts *wire.ConnectOpts) error {
			return wantErr
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = cli.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect succeeded but ConnectPacketBuilder returned an error")
	}
	if !strings.Contains(err.Error(), wantErr.Error()) {
		t.Errorf("Connect err = %v, want one wrapping %v", err, wantErr)
	}
}

// TestConnectPacketBuilderRotatesCredentialsAcrossAttempts verifies the
// headline use case the godoc advertises: a builder that returns a
// fresh credential per attempt should produce distinct CONNECT
// Username values across reconnects.
func TestConnectPacketBuilderRotatesCredentialsAcrossAttempts(t *testing.T) {
	captured := make(chan string, 8)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			pkt.Release()
			return
		}
		// conn.Username aliases the pooled frame buffer; clone so the
		// string survives Release + frame reuse on the next CONNECT.
		username := strings.Clone(conn.Username)
		conn.Release()
		captured <- username
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		// Drop quickly so the supervisor reconnects.
		time.Sleep(10 * time.Millisecond)
	})

	var counter atomic.Int32
	cli, err := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
		WithConnectTimeout(200*time.Millisecond),
		WithConnectPacketBuilder(func(ctx context.Context, opts *wire.ConnectOpts) error {
			n := counter.Add(1)
			opts.Username = fmt.Sprintf("token-%d", n)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	seen := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(seen) < 2 {
		select {
		case u := <-captured:
			seen[u] = true
		case <-deadline:
			t.Fatalf("captured %d distinct usernames in 3s, want >= 2; seen=%v",
				len(seen), seen)
		}
	}
	if !seen["token-1"] {
		keys := make([]string, 0, len(seen))
		for k := range seen {
			keys = append(keys, fmt.Sprintf("%q", k))
		}
		t.Errorf("first attempt didn't carry token-1; keys=%v", keys)
	}
}

func TestValidateBrokerURLsRejectsBad(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"empty", ""},
		{"no scheme", "broker.example:1883"},
		{"unsupported scheme", "http://broker.example:1883"},
		{"missing host", "mqtt://"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(WithBroker(tc.url))
			if err == nil {
				t.Fatalf("New(WithBroker(%q)) succeeded; want error", tc.url)
			}
		})
	}
}

func TestValidateBrokerURLsAllowsDialFuncScheme(t *testing.T) {
	dialFn := func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		return nil, errors.New("not used")
	}
	_, err := New(
		WithBroker("custom://anything"),
		WithDialFunc(dialFn),
	)
	if err != nil {
		t.Fatalf("New with custom scheme + DialFunc: %v", err)
	}
}

func TestWithDialFuncNilRejected(t *testing.T) {
	_, err := New(
		WithBroker("mqtt://127.0.0.1:1"),
		WithDialFunc(nil),
	)
	if err == nil {
		t.Fatal("WithDialFunc(nil) accepted; want error")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Errorf("err = %v, want mention of nil", err)
	}
}

// TestClientIDReportsAssignedID verifies that Client.ClientID()
// returns the broker-assigned identifier from CONNACK when the
// caller passed WithClientID("").
func TestClientIDReportsAssignedID(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode:               wire.ReasonSuccess,
			AssignedClientIdentifier: "broker-assigned-xyz",
		})
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()), WithClientID(""))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cli.ClientID() == "broker-assigned-xyz" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("ClientID() = %q, want broker-assigned-xyz", cli.ClientID())
}
