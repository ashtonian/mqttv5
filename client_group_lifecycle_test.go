// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// groupBroker serves one member: it answers SUBSCRIBE with subscribe
// (or not at all when holdSubscribe), PUBLISH with publish, and counts
// PUBLISHes and UNSUBSCRIBEs.
type groupBroker struct {
	b             *testbroker.Broker
	subscribe     wire.ReasonCode
	holdSubscribe bool
	publish       wire.ReasonCode
	publishes     atomic.Int32
	unsubscribes  atomic.Int32
	conn          atomic.Pointer[testbroker.Conn]
}

func newGroupBroker(t *testing.T, subscribe, publish wire.ReasonCode) *groupBroker {
	gb := &groupBroker{subscribe: subscribe, publish: publish}
	gb.b = testbroker.New(t)
	gb.b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		gb.conn.Store(c)
		for {
			p, ok, err := c.Next(time.Minute)
			if !ok {
				if errors.Is(err, testbroker.ErrTimeout) {
					continue
				}
				return
			}
			switch p.Type {
			case wire.SUBSCRIBE:
				if gb.holdSubscribe {
					continue
				}
				codes := make([]wire.ReasonCode, len(p.Filters))
				for i := range codes {
					codes[i] = gb.subscribe
				}
				c.Suback(p.PacketID, codes...)
			case wire.UNSUBSCRIBE:
				gb.unsubscribes.Add(1)
				codes := make([]wire.ReasonCode, len(p.Topics))
				c.Write(func(w io.Writer) (int64, error) {
					return wire.WriteUnsuback(w, wire.UnsubackOpts{PacketID: p.PacketID, ReasonCodes: codes})
				})
			case wire.PUBLISH:
				gb.publishes.Add(1)
				if p.QoS == 1 {
					c.Puback(p.PacketID, gb.publish)
				}
			case wire.PINGREQ:
				c.Write(wire.WritePingresp)
			case wire.DISCONNECT:
				return
			}
		}
	})
	return gb
}

func newTestGroup(t *testing.T, brokers []*groupBroker, opts ...ClientGroupOption) *ClientGroup {
	t.Helper()
	members := make([]GroupMember, len(brokers))
	for i, gb := range brokers {
		members[i] = GroupMember{Broker: gb.b.URL()}
	}
	opts = append([]ClientGroupOption{WithGroupSharedOpts(WithLogger(quietLogger()), WithClientID(t.Name()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))}, opts...)
	g, err := NewClientGroup(members, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = g.Disconnect(ctx)
	})
	return g
}

func waitCount(t *testing.T, n *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for n.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("count %d, want %d", n.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// The merged channel closes when every member's subscription
// ends, by UnsubscribeAll or by Disconnect.
func TestClientGroupMergedOutputsClose(t *testing.T) {
	t.Run("UnsubscribeAll", func(t *testing.T) {
		g := newTestGroup(t, []*groupBroker{newGroupBroker(t, 0, 0), newGroupBroker(t, 0, 0)})
		ch, tokens, err := g.Subscribe(context.Background(), []TopicFilter{{Topic: "g/#"}})
		if err != nil || len(tokens) != 2 {
			t.Fatalf("Subscribe = %v, %v", tokens, err)
		}
		if err := g.UnsubscribeAll(context.Background(), tokens); err != nil {
			t.Fatal(err)
		}
		expectClosed(t, ch)
	})
	t.Run("Disconnect queue", func(t *testing.T) {
		g := newTestGroup(t, []*groupBroker{newGroupBroker(t, 0, 0), newGroupBroker(t, 0, 0)})
		q, _, err := g.SubscribeQueue(context.Background(), []TopicFilter{{Topic: "g/#"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Disconnect(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, ok := q.Dequeue(ctx); ok || ctx.Err() != nil {
			t.Fatal("merged queue not closed by Disconnect")
		}
		if _, err := g.Publish(context.Background(), PublishOptions{Topic: "t"}); !errors.Is(err, ErrClosed) {
			t.Fatalf("Publish after Disconnect = %v, want ErrClosed", err)
		}
	})
}

// A group subscribe that does not meet its success policy, or whose ctx
// ends, takes back the members that subscribed.
func TestClientGroupSubscribeUnwinds(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		ok, refusing := newGroupBroker(t, 0, 0), newGroupBroker(t, wire.ReasonNotAuthorized, 0)
		g := newTestGroup(t, []*groupBroker{ok, refusing})
		ch, tokens, err := g.Subscribe(context.Background(), []TopicFilter{{Topic: "g/#"}})
		var gerr *GroupError
		if !errors.As(err, &gerr) || !errors.Is(err, ErrNotAuthorized) || ch != nil || tokens != nil {
			t.Fatalf("Subscribe = %v, %v, %v; want a GroupError", ch, tokens, err)
		}
		waitCount(t, &ok.unsubscribes, 1)
	})
	t.Run("any is enough", func(t *testing.T) {
		ok, refusing := newGroupBroker(t, 0, 0), newGroupBroker(t, wire.ReasonNotAuthorized, 0)
		g := newTestGroup(t, []*groupBroker{ok, refusing}, WithGroupSuccess(GroupSuccessAny))
		ch, tokens, err := g.Subscribe(context.Background(), []TopicFilter{{Topic: "g/#"}})
		if err != nil || ch == nil || len(tokens) != 1 || tokens["member-1"].sub == nil {
			t.Fatalf("Subscribe = %v, %v, %v", ch, tokens, err)
		}
	})
	t.Run("ctx", func(t *testing.T) {
		ok, slow := newGroupBroker(t, 0, 0), newGroupBroker(t, 0, 0)
		slow.holdSubscribe = true
		g := newTestGroup(t, []*groupBroker{ok, slow})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if _, _, err := g.Subscribe(ctx, []TopicFilter{{Topic: "g/#"}}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Subscribe = %v", err)
		}
		waitCount(t, &ok.unsubscribes, 1)
	})
}

// A broadcast publish reports every member; the success policy
// decides the error. No policy duplicates a refused message.
func TestClientGroupPublishResults(t *testing.T) {
	for _, tt := range []struct {
		name    string
		success GroupSuccess
		fail    bool
	}{{"all", GroupSuccessAll, true}, {"any", GroupSuccessAny, false}} {
		t.Run(tt.name, func(t *testing.T) {
			ok, refusing := newGroupBroker(t, 0, 0), newGroupBroker(t, 0, wire.ReasonNotAuthorized)
			g := newTestGroup(t, []*groupBroker{ok, refusing}, WithGroupSuccess(tt.success))
			results, err := g.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1})
			if (err != nil) != tt.fail || len(results) != 2 || results[0].Err != nil || !errors.Is(results[1].Err, ErrNotAuthorized) {
				t.Fatalf("Publish = %+v, %v", results, err)
			}
			if ok.publishes.Load() != 1 || refusing.publishes.Load() != 1 {
				t.Fatalf("PUBLISHes %d and %d, want one each", ok.publishes.Load(), refusing.publishes.Load())
			}
		})
	}
	t.Run("round robin", func(t *testing.T) {
		a, b := newGroupBroker(t, 0, wire.ReasonQuotaExceeded), newGroupBroker(t, 0, wire.ReasonQuotaExceeded)
		g := newTestGroup(t, []*groupBroker{a, b}, WithGroupPublishPolicy(GroupPublishRoundRobin))
		_, err := g.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1})
		if !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("Publish = %v", err)
		}
		if n := a.publishes.Load() + b.publishes.Load(); n != 1 {
			t.Fatalf("one refused Publish became %d PUBLISHes", n)
		}
	})
}

// A typed subscription needs no goroutine of its own, so one whose
// consumer stopped reading leaves nothing behind once it ends.
func TestTypedSubscriptionLeavesNothingBehind(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		for i := range 8 {
			c.Publish(wire.PublishOpts{Topic: "t", Payload: []byte(`"` + string(rune('a'+i)) + `"`)})
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithStats())
	typed := NewTyped(cli, testJSONCodec[string]{})
	before := runtime.NumGoroutine()
	ch, tok, err := typed.Subscribe(context.Background(), []TopicFilter{{Topic: "t"}}, SubBuffer(2))
	if err != nil {
		t.Fatal(err)
	}
	// The broker's 8 messages fill the 2-slot buffer; the rest drop.
	deadline := time.Now().Add(3 * time.Second)
	for cli.Stats().InboundDropped < 6 {
		if time.Now().After(deadline) {
			t.Fatalf("%d dropped, want 6", cli.Stats().InboundDropped)
		}
		time.Sleep(time.Millisecond)
	}
	if err := cli.Unsubscribe(context.Background(), tok); err != nil {
		t.Fatal(err)
	}
	got := 0
	for range ch {
		got++
	}
	if got != 2 {
		t.Fatalf("%d messages through a 2-slot buffer the consumer never read", got)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("%d goroutines before the subscription, %d after it ended", before, after)
	}
}
