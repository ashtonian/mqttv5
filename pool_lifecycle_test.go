// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// countingBroker answers every connection, refusing PUBLISHes with
// refuse (0 accepts) and counting them; connections from client IDs
// in dead are dropped.
func countingBroker(t *testing.T, refuse wire.ReasonCode, dead func(clientID string) bool) (*testbroker.Broker, *atomic.Int32) {
	var publishes atomic.Int32
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		ci := c.AcceptConnect(wire.ConnackOpts{})
		if ci == nil || (dead != nil && dead(ci.ClientID)) {
			c.Close()
			return
		}
		for {
			p, ok, err := c.Next(time.Minute)
			if !ok {
				if errors.Is(err, testbroker.ErrTimeout) {
					continue
				}
				return
			}
			switch p.Type {
			case wire.PUBLISH:
				publishes.Add(1)
				if p.QoS == 1 {
					c.Puback(p.PacketID, refuse)
				}
			case wire.PINGREQ:
				c.Write(wire.WritePingresp)
			case wire.DISCONNECT:
				return
			}
		}
	})
	return b, &publishes
}

// A broker refusal is the caller's answer; the pool does not try
// the message on its other members or the main connection.
func TestPoolPublishesOnceOnRefusal(t *testing.T) {
	b, publishes := countingBroker(t, wire.ReasonQuotaExceeded, nil)
	cli := tbClient(t, b, WithPublisherPool(3), WithStats())
	waitForPoolReady(t, cli, 3, 3*time.Second)
	err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1, Payload: []byte("x")})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("Publish = %v, want the broker's refusal", err)
	}
	if n := publishes.Load(); n != 1 {
		t.Fatalf("one Publish put %d PUBLISHes on the wire", n)
	}
	if cli.Stats().PoolFallbacks != 0 {
		t.Fatal("refusal fell back to the main connection")
	}
}

// A member that cannot send passes the message on, once.
func TestPoolFailsOverWhenAMemberIsDown(t *testing.T) {
	b, publishes := countingBroker(t, wire.ReasonSuccess, func(id string) bool { return strings.HasSuffix(id, "-pub-1") })
	cli := tbClient(t, b, WithPublisherPool(2))
	waitForPoolReady(t, cli, 1, 3*time.Second)
	const n = 6
	for range n {
		if err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if got := publishes.Load(); got != n {
		t.Fatalf("%d publishes put %d PUBLISHes on the wire", n, got)
	}
}

// Publish may run while the client connects and disconnects.
func TestPoolLifecycleRace(t *testing.T) {
	b, _ := countingBroker(t, wire.ReasonSuccess, nil)
	cli, err := New(WithBroker(b.URL()), WithClientID("race"), WithPublisherPool(2), WithLogger(quietLogger()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_ = cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 1})
				cancel()
			}
		})
	}
	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := cli.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		if err := cli.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	close(stop)
	wg.Wait()
}

// When OnConnectionDown stops the reconnects, the client is torn down
// as by Disconnect: subscriptions close, and Connect starts again.
func TestStopReconnectingClosesSubscriptions(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-c.Gone()
	})
	b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithOnConnectionDown(func() bool { return false }))
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "s"}})
	if err != nil {
		t.Fatal(err)
	}
	b.Conn(0, 0).Close()
	expectClosed(t, ch)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect after the supervisor stopped: %v", err)
	}
}

// A pooled publish follows the parent's publish options: with
// WithQoSDowngrade a QoS 2 publish to a Maximum QoS 1 broker goes out
// from a member at QoS 1 instead of failing there.
func TestPoolMemberDowngradesQoS(t *testing.T) {
	maxQoS := byte(1)
	type sent struct {
		clientID string
		qos      byte
	}
	got := make(chan sent, 1)
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		ci := c.AcceptConnect(wire.ConnackOpts{MaximumQoS: &maxQoS})
		if ci == nil {
			return
		}
		for {
			p, ok, err := c.Next(time.Minute)
			if !ok {
				if errors.Is(err, testbroker.ErrTimeout) {
					continue
				}
				return
			}
			switch p.Type {
			case wire.PUBLISH:
				got <- sent{ci.ClientID, p.QoS}
				if p.QoS == 1 {
					c.Puback(p.PacketID, wire.ReasonSuccess)
				}
			case wire.DISCONNECT:
				return
			}
		}
	})
	cli := tbClient(t, b, WithPublisherPool(2), WithQoSDowngrade())
	waitForPoolReady(t, cli, 2, 3*time.Second)
	if err := cli.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 2}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	s := <-got
	if !strings.Contains(s.clientID, "-pub-") || s.qos != 1 {
		t.Fatalf("PUBLISH from %q at QoS %d, want a pool member at QoS 1", s.clientID, s.qos)
	}
}

func TestPoolMemberInheritsPublishBehaviour(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_000, 0))
	cli, err := New(
		WithBroker("tcp://127.0.0.1:1"),
		WithClientID("inherit"),
		WithPublisherPool(2),
		WithQoSDowngrade(),
		WithSessionLossPolicy(SessionLossFail),
		WithOutboundTopicAliases(),
		WithLenientDecoding(),
		WithFollowServerRedirects(),
		WithReadBufferSize(4096),
		withClock(fake),
	)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range cli.pool.members {
		c := m.cfg
		if !c.QoSDowngrade || c.SessionLossPolicy != SessionLossFail || !c.OutboundTopicAliases ||
			!c.LenientDecoding || !c.FollowServerRedirects || c.ReadBufferSize != 4096 || c.clock != fake {
			t.Errorf("member %d config = %+v, want the parent's publish behaviour", i+1, *c)
		}
	}
}
