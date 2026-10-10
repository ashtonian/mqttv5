// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func TestResolveServerReference(t *testing.T) {
	for _, tt := range []struct{ current, ref, want string }{
		{"mqtts://a.example:8883", "b.example:9883", "mqtts://b.example:9883"},
		{"mqtt://a.example", "b.example", "mqtt://b.example"},
		{"ws://a.example/mqtt", "b.example:80", "ws://b.example:80/mqtt"},
		{"mqtt://a.example", "mqtts://c.example:8883 d.example", "mqtts://c.example:8883"},
		{"mqtt://a.example", " b.example c.example ", "mqtt://b.example"},
	} {
		if got, err := resolveServerReference(tt.current, tt.ref); err != nil || got != tt.want {
			t.Errorf("resolve(%q, %q) = %q, %v; want %q", tt.current, tt.ref, got, err, tt.want)
		}
	}
	for _, ref := range []string{"", "b.example/path", "user@b.example"} {
		if got, err := resolveServerReference("mqtt://a.example", ref); err == nil {
			t.Errorf("resolve(%q) = %q, want an error", ref, got)
		}
	}
}

func hostOf(b *testbroker.Broker) string { return strings.TrimPrefix(b.URL(), "mqtt://") }

// 0x9D Server moved on CONNACK: the client follows and keeps the new
// broker for later reconnects.
func TestFollowServerMoved(t *testing.T) {
	target := testbroker.New(t)
	target.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	origin := testbroker.New(t)
	origin.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{ReasonCode: wire.ReasonServerMoved, ServerReference: hostOf(target)})
	})
	var got []ServerRedirect
	cli, err := New(WithBroker(origin.URL()), WithClientID("moved"), WithLogger(quietLogger()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)), WithRetryInitialConnect(),
		WithFollowServerRedirects(), WithOnServerRedirect(func(r ServerRedirect) { got = append(got, r) }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	if err := cli.AwaitConnection(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Permanent() || got[0].Packet != CONNACK {
		t.Fatalf("redirects reported %+v", got)
	}
	target.Conn(0, time.Second).Close()
	if c := target.Conn(1, 3*time.Second); c == nil {
		t.Fatal("the client did not reconnect to the broker it moved to")
	}
	if origin.Accepted() != 1 {
		t.Fatalf("the client went back to the old broker (%d connections)", origin.Accepted())
	}
}

// 0x9C Use another server on DISCONNECT: the next attempt only goes to
// the reference.
func TestFollowUseAnotherServerOnce(t *testing.T) {
	other := testbroker.New(t)
	other.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	origin := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonUseAnotherServer, ServerReference: hostOf(other)})
	})
	origin.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, origin, WithFollowServerRedirects())
	if c := other.Conn(0, 3*time.Second); c == nil {
		t.Fatal("the client did not use the other server")
	}
	if err := cli.AwaitConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	other.Conn(0, 0).Close()
	if c := origin.Conn(1, 3*time.Second); c == nil {
		t.Fatal("after the temporary redirect the client did not return to its broker")
	}
}

// Without WithFollowServerRedirects a redirect is reported, not followed.
func TestServerRedirectReportedNotFollowed(t *testing.T) {
	other := testbroker.New(t)
	origin := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonServerMoved, ServerReference: hostOf(other)})
	})
	origin.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	reported := make(chan ServerRedirect, 1)
	tbClient(t, origin, WithOnServerRedirect(func(r ServerRedirect) { reported <- r }))
	select {
	case r := <-reported:
		if r.Packet != DISCONNECT || r.Reference != hostOf(other) {
			t.Fatalf("reported %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("redirect not reported")
	}
	if c := origin.Conn(1, 3*time.Second); c == nil {
		t.Fatal("the client did not reconnect to its own broker")
	}
	if other.Accepted() != 0 {
		t.Fatal("the client followed the redirect")
	}
}
