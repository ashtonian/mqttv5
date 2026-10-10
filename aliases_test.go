// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func aliasOf(p testbroker.Packet) uint16 {
	v, _ := p.Properties().Uint16(wire.PropTopicAlias)
	return v
}

// Outbound aliases are opt-in.
func TestOutboundAliasesOffByDefault(t *testing.T) {
	max := uint16(10)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{TopicAliasMaximum: max})
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	for i := 0; i < 3; i++ {
		if err := cli.Publish(context.Background(), PublishOptions{Topic: "long/topic/name", QoS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.PUBLISH && (p.Topic == "" || aliasOf(p) != 0) {
			t.Fatalf("aliased PUBLISH without WithOutboundTopicAliases: %+v", p)
		}
	}
}

// With concurrent publishers and connections dropping mid-stream,
// a PUBLISH that uses an alias never reaches the broker before the
// PUBLISH that registered the alias on the same connection.
func TestOutboundAliasNeverPrecedesRegistration(t *testing.T) {
	max := uint16(4)
	accept := func(c *testbroker.Conn) { c.AcceptConnect(wire.ConnackOpts{TopicAliasMaximum: max}) }
	dropAfter := func(n int) testbroker.Script {
		return func(c *testbroker.Conn) {
			accept(c)
			for got := 0; got < n; {
				if _, ok := c.Await(wire.PUBLISH, 3*time.Second); !ok {
					return
				}
				got++
			}
		}
	}
	b := testbroker.New(t, dropAfter(150), dropAfter(150), dropAfter(150))
	b.SetFallback(func(c *testbroker.Conn) { accept(c); c.ServeAuto() })
	cli := tbClient(t, b, WithOutboundTopicAliases())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Go(func() {
			// Keep publishing across the three dropped connections.
			for i := 0; b.Accepted() < 4 || i < 100; i++ {
				err := cli.Publish(ctx, PublishOptions{Topic: fmt.Sprintf("alias/%d", (w+i)%6), Payload: []byte{byte(i)}})
				if errors.Is(err, ErrNotConnected) {
					time.Sleep(time.Millisecond)
				}
				if ctx.Err() != nil {
					t.Error("publishers did not see four connections")
					return
				}
			}
		})
	}
	wg.Wait()

	for i := 0; i < b.Accepted(); i++ {
		known := map[uint16]string{}
		for _, p := range b.Conn(i, 0).Log() {
			if p.Type != wire.PUBLISH {
				continue
			}
			alias := aliasOf(p)
			switch {
			case alias == 0:
			case alias > max:
				t.Fatalf("conn %d: alias %d above the broker's maximum %d", i, alias, max)
			case p.Topic != "":
				known[alias] = p.Topic
			case known[alias] == "":
				t.Fatalf("conn %d: alias %d used before it was registered on this connection", i, alias)
			}
		}
	}
	if b.Accepted() < 2 {
		t.Fatalf("only %d connections; the reconnect path was not exercised", b.Accepted())
	}
}

// Inbound alias violations close the connection with the spec's
// reason codes.
func TestInboundAliasViolations(t *testing.T) {
	tests := []struct {
		name   string
		send   func(c *testbroker.Conn)
		reason wire.ReasonCode
	}{
		{"above advertised maximum", func(c *testbroker.Conn) {
			c.Publish(wire.PublishOpts{Topic: "a", TopicAlias: 3})
		}, wire.ReasonTopicAliasInvalid},
		{"alias zero", func(c *testbroker.Conn) {
			c.Raw([]byte{0x30, 7, 0, 1, 'a', 3, 0x23, 0, 0})
		}, wire.ReasonTopicAliasInvalid},
		{"never registered", func(c *testbroker.Conn) {
			c.Publish(wire.PublishOpts{TopicAlias: 1})
		}, wire.ReasonProtocolError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.AcceptConnect(wire.ConnackOpts{})
				tt.send(c)
				if p := c.Expect(wire.DISCONNECT, 0); p.Reason != tt.reason {
					c.T.Errorf("DISCONNECT reason %#x, want %#x", byte(p.Reason), byte(tt.reason))
				}
			})
			tbClient(t, b, WithInboundTopicAliasMaximum(2))
			waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
		})
	}
}
