// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// Publishes the broker's CONNACK rules out fail before the wire.
func TestPublishRespectsConnackLimits(t *testing.T) {
	maxQoS, noRetain, maxSize := byte(1), byte(0), uint32(64)
	limited := wire.ConnackOpts{MaximumQoS: &maxQoS, RetainAvailable: &noRetain, MaximumPacketSize: &maxSize}
	tests := []struct {
		name string
		opts PublishOptions
		err  error
	}{
		{"qos above maximum", PublishOptions{Topic: "t", QoS: 2}, ErrQoSNotSupported},
		{"retain unavailable", PublishOptions{Topic: "t", Retain: true}, ErrRetainNotSupported},
		{"qos0 too large", PublishOptions{Topic: "t", Payload: make([]byte, 100)}, ErrPacketTooLarge},
		{"qos1 too large", PublishOptions{Topic: "t", Payload: make([]byte, 100), QoS: 1}, ErrPacketTooLarge},
	}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(limited)
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := cli.Publish(ctx, tt.opts); !errors.Is(err, tt.err) {
				t.Fatalf("Publish = %v, want %v", err, tt.err)
			}
		})
	}
	if err := cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 1}); err != nil {
		t.Fatalf("publish within limits: %v", err)
	}
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.PUBLISH && (p.QoS == 2 || p.Retain || len(p.Payload) > 0) {
			t.Fatalf("a refused publish reached the broker: %+v", p)
		}
	}
	if _, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: strings.Repeat("f", 80)}}); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("oversized SUBSCRIBE = %v", err)
	}
}

// WithQoSDowngrade sends at the broker's maximum instead.
func TestQoSDowngrade(t *testing.T) {
	maxQoS := byte(1)
	got := make(chan byte, 1)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{MaximumQoS: &maxQoS})
		p := c.Expect(wire.PUBLISH, 0)
		got <- p.QoS
		c.Puback(p.PacketID, wire.ReasonSuccess)
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithQoSDowngrade())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 2}); err != nil {
		t.Fatal(err)
	}
	if q := <-got; q != 1 {
		t.Fatalf("broker received QoS %d, want 1", q)
	}
}

// ServerInfo reports the effective limits with spec defaults.
func TestServerInfo(t *testing.T) {
	ka, rm := uint16(7), uint16(9)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{ServerKeepAlive: &ka, ReceiveMaximum: &rm, ResponseInformation: "resp/"})
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	info, ok := cli.ServerInfo()
	if !ok {
		t.Fatal("not connected")
	}
	if info.KeepAlive != 7 || info.ReceiveMaximum != 9 || info.MaximumQoS != 2 || !info.RetainAvailable ||
		info.ResponseInformation != "resp/" || info.SessionExpiry != DefaultSessionExpiry {
		t.Fatalf("ServerInfo = %+v", info)
	}
}

// A packet above the client's advertised Maximum Packet Size closes
// the connection with DISCONNECT 0x95.
func TestInboundMaximumPacketSizeEnforced(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		ci := c.AcceptConnect(wire.ConnackOpts{})
		if v, ok := ci.Properties().Uint32(wire.PropMaximumPacketSize); !ok || v != 256 {
			c.T.Errorf("CONNECT Maximum Packet Size = %d, %v", v, ok)
		}
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "big", Payload: make([]byte, 1024)})
		if p := c.Expect(wire.DISCONNECT, 0); p.Reason != wire.ReasonPacketTooLarge {
			c.T.Errorf("DISCONNECT reason = %#x, want 0x95", byte(p.Reason))
		}
	})
	cli := tbClient(t, b, WithMaximumPacketSize(256))
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "big"}})
	if err != nil {
		t.Fatal(err)
	}
	waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
	select {
	case m := <-ch:
		t.Fatalf("oversized message delivered (%d bytes)", len(m.Payload))
	default:
	}
}

// Opting out of problem information sends the property as 0; the
// default leaves it absent (meaning 1).
func TestRequestProblemInformation(t *testing.T) {
	for _, tt := range []struct {
		opts    []Option
		present bool
		value   byte
	}{{nil, false, 0}, {[]Option{WithRequestProblemInformation(false)}, true, 0}} {
		got := make(chan *testbroker.ConnectInfo, 1)
		b := testbroker.New(t, func(c *testbroker.Conn) {
			got <- c.AcceptConnect(wire.ConnackOpts{})
			c.ServeAuto()
		})
		tbClient(t, b, tt.opts...)
		v, ok := (<-got).Properties().Byte(wire.PropRequestProblemInfo)
		if ok != tt.present || v != tt.value {
			t.Fatalf("Request Problem Information = %d, %v; want %d, %v", v, ok, tt.value, tt.present)
		}
	}
}

// DISCONNECT may not set a non-zero Session Expiry when CONNECT
// sent 0 [MQTT-3.14.2-2].
func TestDisconnectCannotRaiseZeroSessionExpiry(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithSessionExpiry(0))
	ten := uint32(10)
	err := cli.DisconnectWith(context.Background(), DisconnectOptions{SessionExpiryInterval: &ten})
	if !errors.Is(err, ErrInvalidSessionExpiry) {
		t.Fatalf("DisconnectWith = %v", err)
	}
	if !cli.Connected() {
		t.Fatal("refused DisconnectWith still disconnected")
	}
}
