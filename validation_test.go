// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// Invalid publishes and subscriptions fail at the call and
// never reach the broker.
func TestInvalidRequestsNeverSent(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, tt := range []struct {
		name string
		opts PublishOptions
		want error
	}{
		{"wildcard topic qos0", PublishOptions{Topic: "a/#"}, ErrInvalidTopic},
		{"wildcard topic qos1", PublishOptions{Topic: "a/+", QoS: 1}, ErrInvalidTopic},
		{"NUL in topic", PublishOptions{Topic: "a\x00"}, ErrInvalidTopic},
		{"alias without topic at QoS 1", PublishOptions{TopicAlias: 1, QoS: 1}, ErrTopicAliasInvalid},
	} {
		if err := cli.Publish(ctx, tt.opts); !errors.Is(err, tt.want) {
			t.Errorf("%s: Publish = %v, want %v", tt.name, err, tt.want)
		}
	}
	for _, f := range []TopicFilter{{Topic: "$share/g/a", NoLocal: true}, {Topic: "a/#/b"}, {Topic: "$share//x"}} {
		if _, _, err := cli.Subscribe(ctx, []TopicFilter{f}); err == nil {
			t.Errorf("Subscribe(%+v) succeeded", f)
		}
	}
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.PUBLISH || p.Type == wire.SUBSCRIBE {
			t.Fatalf("invalid request reached the broker: %+v", p)
		}
	}
}

// A malformed packet from the broker closes the connection with the
// spec's reason code and is counted.
func TestMalformedBrokerPacketDisconnects(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Raw([]byte{0x30, 4, 0, 1, 0xff, 0}) // PUBLISH with invalid UTF-8 topic
		if p := c.Expect(wire.DISCONNECT, 0); p.Reason != wire.ReasonMalformedPacket {
			c.T.Errorf("DISCONNECT reason %#x, want 0x81", byte(p.Reason))
		}
	})
	cli := tbClient(t, b, WithStats())
	waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
	if got := cli.Stats().ProtocolErrors; got != 1 {
		t.Fatalf("ProtocolErrors = %d", got)
	}
}

// A packet type only clients send is a protocol error from a broker.
func TestServerOnlyPacketFromBrokerDisconnects(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Raw([]byte{0xc0, 0}) // PINGREQ
		if p := c.Expect(wire.DISCONNECT, 0); p.Reason != wire.ReasonProtocolError {
			c.T.Errorf("DISCONNECT reason %#x, want 0x82", byte(p.Reason))
		}
	})
	tbClient(t, b)
	waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
}

// Lenient decoding tolerates a non-minimal Remaining Length.
func TestLenientDecodingOption(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Raw([]byte{0xd0, 0x80, 0x00}) // PINGRESP, Remaining Length in two bytes
		c.Hold(0)
	})
	cli := tbClient(t, b, WithLenientDecoding())
	cs := cli.cur.Load()
	for deadline := time.Now().Add(3 * time.Second); cs.reads.Load() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("client never read the PINGRESP")
		}
	}
	if !cli.Connected() {
		t.Fatal("lenient client disconnected on a tolerated violation")
	}
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.DISCONNECT {
			t.Fatal("lenient client sent DISCONNECT")
		}
	}
}

// New rejects options that could only fail later.
func TestNewValidatesOptions(t *testing.T) {
	bad := "a\x00b"
	for _, tt := range []struct {
		name string
		opts []Option
	}{
		{"ws without a dial function", []Option{WithBroker("ws://broker:80/mqtt")}},
		{"wss without a dial function", []Option{WithBroker("wss://broker:443/mqtt")}},
		{"negative write queue", []Option{WithWriteQueueSize(-1)}},
		{"negative pool size", []Option{WithPublisherPool(-2)}},
		{"negative connect timeout", []Option{WithConnectTimeout(-time.Second)}},
		{"negative disconnect flush timeout", []Option{WithDisconnectFlushTimeout(-time.Second)}},
		{"invalid will topic", []Option{WithWill(&WillOptions{Topic: "w/#"})}},
		{"NUL in client ID", []Option{WithClientID(bad)}},
		{"invalid user property", []Option{WithConnectUserProperties([]UserProperty{{Key: bad, Value: "v"}})}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := append([]Option{WithBroker("mqtt://127.0.0.1:1883")}, tt.opts...)
			if _, err := New(opts...); err == nil {
				t.Fatal("New accepted it")
			}
		})
	}
	if _, err := New(WithBroker("ws://broker:80/mqtt"), WithDialFunc(func(context.Context, *url.URL) (transport.Conn, error) { return nil, nil })); err != nil {
		t.Fatalf("ws with a dial function: %v", err)
	}
}
