// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/soak"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func subIDOf(p testbroker.Packet) uint32 {
	v, _ := p.Properties().Varint(wire.PropSubscriptionIdentifier)
	return v
}

// Refusals are visible; a full refusal closes the outputs,
// a partial one keeps the granted filters.
func TestSubscribeRefusals(t *testing.T) {
	t.Run("all refused", func(t *testing.T) {
		b := testbroker.New(t, func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			p := c.Expect(wire.SUBSCRIBE, 0)
			c.Suback(p.PacketID, wire.ReasonNotAuthorized)
			c.ServeAuto()
		})
		cli := tbClient(t, b)
		ch, tok, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "secret/#", QoS: 1}})
		var serr *SubscribeError
		if !errors.As(err, &serr) || !errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("Subscribe = %v, want *SubscribeError matching ErrNotAuthorized", err)
		}
		if tok.Err() == nil || len(tok.Results()) != 1 || tok.Results()[0].Granted() {
			t.Fatalf("token: err %v results %+v", tok.Err(), tok.Results())
		}
		expectClosed(t, ch)
	})
	t.Run("partly refused", func(t *testing.T) {
		b := testbroker.New(t, func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			p := c.Expect(wire.SUBSCRIBE, 0)
			c.Suback(p.PacketID, wire.ReasonGrantedQoS1, wire.ReasonNotAuthorized)
			c.Publish(wire.PublishOpts{Topic: "open/x", Payload: []byte("ok")})
			c.ServeAuto()
		})
		cli := tbClient(t, b)
		ch, tok, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "open/#", QoS: 2}, {Topic: "secret/#"}})
		var serr *SubscribeError
		if !errors.As(err, &serr) || len(serr.Results) != 2 {
			t.Fatalf("Subscribe = %v", err)
		}
		if r := tok.Results(); !r[0].Granted() || r[0].QoS() != 1 || r[1].Granted() {
			t.Fatalf("results %+v", r)
		}
		if tok.Err() != nil {
			t.Fatalf("partly refused subscription ended: %v", tok.Err())
		}
		if m := recvMsg(t, ch, 2*time.Second); string(m.Payload) != "ok" {
			t.Fatalf("payload %q", m.Payload)
		}
	})
}

// A SUBSCRIBE whose SUBACK is lost with the connection is sent
// again on the next one, and Subscribe returns that answer.
func TestSubscribeSurvivesReconnect(t *testing.T) {
	for _, sp := range []bool{false, true} {
		t.Run(map[bool]string{false: "session lost", true: "session resumed"}[sp], func(t *testing.T) {
			b := testbroker.New(t,
				func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{})
					c.Expect(wire.SUBSCRIBE, 0) // dropped before SUBACK
				},
				func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{SessionPresent: sp})
					p := c.Expect(wire.SUBSCRIBE, 0)
					if len(p.Filters) != 1 || p.Filters[0].Topic != "r/#" {
						c.T.Errorf("re-sent SUBSCRIBE %+v", p.Filters)
					}
					c.Suback(p.PacketID, wire.ReasonGrantedQoS1)
					c.ExpectNone(wire.SUBSCRIBE, 200*time.Millisecond)
					c.ServeAuto()
				},
			)
			cli := tbClient(t, b)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, tok, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "r/#", QoS: 1}})
			if err != nil {
				t.Fatalf("Subscribe across a reconnect: %v", err)
			}
			if r := tok.Results(); len(r) != 1 || !r[0].Granted() {
				t.Fatalf("results %+v", r)
			}
		})
	}
}

// An UNSUBSCRIBE whose UNSUBACK is lost is re-sent when the session
// survives, and needs no re-send when it does not; either way the
// subscription closes.
func TestUnsubscribeSurvivesReconnect(t *testing.T) {
	for _, sp := range []bool{false, true} {
		t.Run(map[bool]string{false: "session lost", true: "session resumed"}[sp], func(t *testing.T) {
			b := testbroker.New(t,
				func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{})
					c.ServeSubscribe(-1)
					c.Expect(wire.UNSUBSCRIBE, 0) // dropped before UNSUBACK
				},
				func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{SessionPresent: sp})
					if sp {
						p := c.Expect(wire.UNSUBSCRIBE, 0)
						_ = c.Write(func(w io.Writer) (int64, error) {
							return wire.WriteUnsuback(w, wire.UnsubackOpts{PacketID: p.PacketID, ReasonCodes: []wire.ReasonCode{0}})
						})
					} else {
						c.ExpectNone(wire.UNSUBSCRIBE, 200*time.Millisecond)
					}
					c.ServeAuto()
				},
			)
			cli := tbClient(t, b)
			ch, tok, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "u/#"}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := cli.Unsubscribe(ctx, tok); err != nil {
				t.Fatalf("Unsubscribe across a reconnect: %v", err)
			}
			expectClosed(t, ch)
		})
	}
}

// A Subscribe whose caller gave up is taken back when the broker grants
// it after all.
func TestAbandonedSubscribeIsUnsubscribed(t *testing.T) {
	gaveUp := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		p := c.Expect(wire.SUBSCRIBE, 0)
		<-gaveUp
		c.Suback(p.PacketID, wire.ReasonGrantedQoS0)
		u := c.Expect(wire.UNSUBSCRIBE, 0)
		if len(u.Topics) != 1 || u.Topics[0] != "late/#" {
			c.T.Errorf("cleanup UNSUBSCRIBE %v", u.Topics)
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "late/#"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Subscribe = %v", err)
	}
	close(gaveUp)
	waitLog(t, b.Conn(0, 0), wire.UNSUBSCRIBE, 1)
}

// Refusals when re-subscribing after a session loss end the
// subscription and are reported.
func TestResubscribeRefusalEndsSubscription(t *testing.T) {
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			<-c.Gone() // the test drops this connection
		},
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: false})
			p := c.Expect(wire.SUBSCRIBE, 0)
			c.Suback(p.PacketID, wire.ReasonNotAuthorized)
			c.ServeAuto()
		},
	)
	reported := make(chan error, 1)
	cli := tbClient(t, b, WithOnResubscribeError(func(_ SubscriptionToken, err error) { reported <- err }))
	ch, tok, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "acl/#"}})
	if err != nil {
		t.Fatal(err)
	}
	b.Conn(0, 0).Close()
	select {
	case err := <-reported:
		if !errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("reported %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("refused resubscribe not reported")
	}
	expectClosed(t, ch)
	if !errors.Is(tok.Err(), ErrNotAuthorized) {
		t.Fatalf("token Err = %v", tok.Err())
	}
}

// With Subscription Identifiers each subscription receives
// exactly the copies addressed to it; without them, overlapping filters
// each receive every copy.
func TestOverlappingSubscriptions(t *testing.T) {
	for _, ids := range []bool{true, false} {
		t.Run(map[bool]string{true: "identifiers", false: "no identifiers"}[ids], func(t *testing.T) {
			avail := byte(0)
			if ids {
				avail = 1
			}
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.AcceptConnect(wire.ConnackOpts{SubscriptionIdentifierAvailable: &avail})
				var subIDs []uint32
				for i := 0; i < 2; i++ {
					p := c.Expect(wire.SUBSCRIBE, 0)
					if got := subIDOf(p); (got != 0) != ids {
						c.T.Errorf("SUBSCRIBE identifier %d with identifiers available=%v", got, ids)
					}
					subIDs = append(subIDs, subIDOf(p))
					c.Suback(p.PacketID, wire.ReasonGrantedQoS0)
				}
				// A broker sends one copy per matching subscription.
				for _, id := range subIDs {
					opts := wire.PublishOpts{Topic: "o/x", Payload: []byte("m")}
					if id != 0 {
						c.PublishTagged(opts, id)
						continue
					}
					c.Publish(opts)
				}
				c.ServeAuto()
			})
			cli := tbClient(t, b)
			wide, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "o/#"}}, SubBuffer(8))
			if err != nil {
				t.Fatal(err)
			}
			narrow, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "o/x"}}, SubBuffer(8))
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if !ids {
				want = 2
			}
			for name, ch := range map[string]<-chan *Message{"o/#": wide, "o/x": narrow} {
				got := 0
				for done := false; !done; {
					select {
					case <-ch:
						got++
					case <-time.After(300 * time.Millisecond):
						done = true
					}
				}
				if got != want {
					t.Errorf("subscription %s received %d copies, want %d", name, got, want)
				}
			}
		})
	}
}

// Acknowledgements that match no operation are ignored and counted; one
// with the wrong number of reason codes is a protocol error.
func TestControlAckValidation(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Suback(999, wire.ReasonGrantedQoS0) // nothing waits for 999
		p := c.Expect(wire.SUBSCRIBE, 0)
		c.Suback(p.PacketID, wire.ReasonGrantedQoS0, wire.ReasonGrantedQoS0) // one filter, two codes
		if d := c.Expect(wire.DISCONNECT, 0); d.Reason != wire.ReasonProtocolError {
			c.T.Errorf("DISCONNECT reason %#x", byte(d.Reason))
		}
	})
	b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithStats())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "x"}}); err != nil {
		t.Fatalf("Subscribe after a malformed SUBACK should complete on the next connection: %v", err)
	}
	if got := cli.Stats().AcksIgnored; got < 1 {
		t.Fatalf("AcksIgnored = %d", got)
	}
}

// A refused UNSUBSCRIBE is reported; the subscription closes anyway.
func TestUnsubscribeRefusal(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		p := c.Expect(wire.UNSUBSCRIBE, 0)
		_ = c.Write(func(w io.Writer) (int64, error) {
			return wire.WriteUnsuback(w, wire.UnsubackOpts{PacketID: p.PacketID, ReasonCodes: []wire.ReasonCode{wire.ReasonNotAuthorized}})
		})
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ch, tok, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "z"}})
	if err != nil {
		t.Fatal(err)
	}
	var uerr *UnsubscribeError
	if err := cli.Unsubscribe(context.Background(), tok); !errors.As(err, &uerr) || uerr.Results[0].Topic != "z" {
		t.Fatalf("Unsubscribe = %v", err)
	}
	expectClosed(t, ch)
}

// Disconnect fails a Subscribe still waiting for its SUBACK.
func TestDisconnectFailsPendingSubscribe(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	result := make(chan error, 1)
	go func() {
		_, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "p"}})
		result <- err
	}()
	waitLog(t, b.Conn(0, time.Second), wire.SUBSCRIBE, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = cli.Disconnect(ctx)
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Subscribe = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe still blocked after Disconnect")
	}
}

// The broker holds one subscription per filter and a second SUBSCRIBE for
// it replaces the first (§3.8.4): two local subscriptions with the same
// filter share it, both receive what it is sent, and the UNSUBSCRIBE goes
// out only when the last of them unsubscribes.
func TestSameFilterSharesBrokerSubscription(t *testing.T) {
	soloSent := make(chan struct{})
	firstGone := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		avail := byte(1)
		c.AcceptConnect(wire.ConnackOpts{SubscriptionIdentifierAvailable: &avail})
		first := c.Expect(wire.SUBSCRIBE, 0)
		c.Suback(first.PacketID, wire.ReasonGrantedQoS0)
		second := c.Expect(wire.SUBSCRIBE, 0)
		if subIDOf(second) == subIDOf(first) {
			c.T.Errorf("both SUBSCRIBEs carry identifier %d", subIDOf(first))
		}
		pub := func(payload string, id uint32) {
			c.PublishTagged(wire.PublishOpts{Topic: "s/x", Payload: []byte(payload)}, id)
		}
		pub("before", subIDOf(first)) // sent before the broker saw the second SUBSCRIBE
		c.Suback(second.PacketID, wire.ReasonGrantedQoS0)
		pub("stale", subIDOf(first)) // the replaced subscription no longer exists
		pub("after", subIDOf(second))
		select {
		case <-firstGone:
		case <-c.Gone():
			return
		}
		pub("solo", subIDOf(second))
		close(soloSent)
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	a, tokA, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "s/#"}})
	if err != nil {
		t.Fatal(err)
	}
	bch, tokB, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "s/#"}})
	if err != nil {
		t.Fatal(err)
	}
	payloads := func(ch <-chan *Message) []string {
		var got []string
		for m := range ch {
			got = append(got, string(m.Payload))
			_ = m.Ack()
		}
		return got
	}
	if m := recvMsg(t, bch, 2*time.Second); string(m.Payload) != "before" {
		t.Fatalf("second subscription got %q first", m.Payload)
	}
	if m := recvMsg(t, bch, 2*time.Second); string(m.Payload) != "after" {
		t.Fatalf("second subscription got %q after the SUBACK", m.Payload)
	}
	if err := cli.Unsubscribe(context.Background(), tokA); err != nil {
		t.Fatal(err)
	}
	if got := payloads(a); !slices.Equal(got, []string{"before", "after"}) {
		t.Fatalf("first subscription received %q", got)
	}
	close(firstGone)
	select {
	case <-soloSent:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not send the last message")
	}
	if m := recvMsg(t, bch, 2*time.Second); string(m.Payload) != "solo" {
		t.Fatalf("second subscription got %q once alone", m.Payload)
	}
	if err := cli.Unsubscribe(context.Background(), tokB); err != nil {
		t.Fatal(err)
	}
	if got := payloads(bch); len(got) != 0 {
		t.Fatalf("second subscription received %q more", got)
	}
	var unsubs []testbroker.Packet
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.UNSUBSCRIBE {
			unsubs = append(unsubs, p)
		}
	}
	if len(unsubs) != 1 || !slices.Equal(unsubs[0].Topics, []string{"s/#"}) {
		t.Fatalf("UNSUBSCRIBEs %+v, want one for s/# after the last subscription", unsubs)
	}
}

// Typed subscriptions hand back the channel and token with a partial
// refusal, as Client.Subscribe does.
func TestTypedSubscribePartialRefusal(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		p := c.Expect(wire.SUBSCRIBE, 0)
		c.Suback(p.PacketID, wire.ReasonGrantedQoS0, wire.ReasonNotAuthorized)
		c.Publish(wire.PublishOpts{Topic: "open/x", Payload: []byte(`"ok"`)})
		c.ServeAuto()
	})
	typed := NewTyped(tbClient(t, b), testJSONCodec[string]{})
	ch, tok, err := typed.Subscribe(context.Background(), []TopicFilter{{Topic: "open/#"}, {Topic: "secret/#"}})
	var serr *SubscribeError
	if !errors.As(err, &serr) || ch == nil || tok.Results() == nil {
		t.Fatalf("Subscribe = %v, %v, %+v", ch, err, tok.Results())
	}
	select {
	case m := <-ch:
		if m.Value != "ok" {
			t.Fatalf("value %q", m.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("granted filter delivered nothing")
	}
}

// modelBroker keeps subscriptions the way §3.8.4 says a broker must: one
// per exact filter, replaced by a later SUBSCRIBE for the same filter and
// tagged with that SUBSCRIBE's identifier. Its session survives a
// reconnect when the next CONNACK says so.
type modelBroker struct {
	t *testing.T

	mu         sync.Mutex
	subs       map[string]uint32
	keep       bool // next CONNACK reports Session Present when CleanStart allows
	singleCopy bool // one PUBLISH tagged with every matching subscription, else one per subscription
	conn       *testbroker.Conn
}

func (m *modelBroker) serve(c *testbroker.Conn) {
	p, ok := c.Await(wire.CONNECT, 0)
	if !ok {
		return
	}
	avail := byte(1)
	m.mu.Lock()
	sp := m.keep && !p.Connect.CleanStart
	if !sp {
		m.subs = map[string]uint32{}
	}
	m.conn = c
	m.mu.Unlock()
	if err := c.Write(func(w io.Writer) (int64, error) {
		return wire.WriteConnack(w, wire.ConnackOpts{SessionPresent: sp, SubscriptionIdentifierAvailable: &avail})
	}); err != nil {
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
		case wire.SUBSCRIBE:
			codes := make([]wire.ReasonCode, len(p.Filters))
			m.mu.Lock()
			for _, f := range p.Filters {
				m.subs[f.Topic] = subIDOf(p)
			}
			m.mu.Unlock()
			c.Suback(p.PacketID, codes...)
		case wire.UNSUBSCRIBE:
			codes := make([]wire.ReasonCode, len(p.Topics))
			m.mu.Lock()
			for i, f := range p.Topics {
				if _, ok := m.subs[f]; !ok {
					codes[i] = wire.ReasonNoSubscriptionExisted
				}
				delete(m.subs, f)
			}
			m.mu.Unlock()
			c.Write(func(w io.Writer) (int64, error) {
				return wire.WriteUnsuback(w, wire.UnsubackOpts{PacketID: p.PacketID, ReasonCodes: codes})
			})
		case wire.PINGREQ:
			c.Write(wire.WritePingresp)
		case wire.DISCONNECT:
			return
		}
	}
}

func (m *modelBroker) filters() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var fs []string
	for f := range m.subs {
		fs = append(fs, f)
	}
	slices.Sort(fs)
	return fs
}

// publish sends topic to every matching subscription.
func (m *modelBroker) publish(topic, payload string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []uint32
	for f, id := range m.subs {
		if filterMatches(f, topic) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	opts := wire.PublishOpts{Topic: topic, Payload: []byte(payload)}
	if m.singleCopy && len(ids) > 0 {
		m.conn.PublishTagged(opts, ids...)
		return
	}
	for _, id := range ids {
		m.conn.PublishTagged(opts, id)
	}
}

func (m *modelBroker) subsSnapshot() map[string]uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.subs)
}

func (c *Client) subsDump() string {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	var b strings.Builder
	for f, bs := range c.brokerSubs {
		fmt.Fprintf(&b, "%s ids=%v holders=%d view=%v; ", f, bs.ids, len(bs.holders), bs.view.Load().ids)
	}
	for id, sub := range c.activeSubs {
		fmt.Fprintf(&b, "\nsub %d state %d filters %v", id, sub.state, sub.filters)
	}
	return b.String()
}

// live reports whether the broker has a connection the test has not
// dropped; once the client is connected, it is connected to that one.
func (m *modelBroker) live() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conn != nil
}

func (m *modelBroker) drop(keep bool) {
	m.mu.Lock()
	m.keep = keep
	c := m.conn
	m.conn = nil
	m.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

// filterMatches is §4.7 matching, written independently of the trie.
func filterMatches(filter, topic string) bool {
	if strings.HasPrefix(filter, "$share/") {
		_, filter, _ = strings.Cut(strings.TrimPrefix(filter, "$share/"), "/")
	}
	f, tp := strings.Split(filter, "/"), strings.Split(topic, "/")
	for i, level := range f {
		if level == "#" {
			return true
		}
		if i >= len(tp) || (level != "+" && level != tp[i]) {
			return false
		}
	}
	return len(f) == len(tp)
}

type modelSub struct {
	filter string
	ch     <-chan *Message
	token  SubscriptionToken
}

// TestSubscriptionModel runs random rounds of concurrent Subscribe,
// Unsubscribe and abandoned Subscribe calls, with connection drops that
// keep or lose the session, against modelBroker. After each round the
// broker must hold exactly the filters of the open subscriptions, and a
// message to each probe topic must reach every open subscription whose
// filter matches it exactly once and no other.
func TestSubscriptionModel(t *testing.T) {
	const rounds = 25
	base, n := soak.Seeds(t, 20, 4)
	for seed := base; seed < base+n; seed++ {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) {
			runSubscriptionModel(t, seed, rounds)
		})
	}
}

func runSubscriptionModel(t *testing.T, seed uint64, rounds int) {
	rng := rand.New(rand.NewPCG(seed, 0x5eed))
	// Every filter matches the final probe, a/b, so each round ends with
	// one known message per open subscription.
	filterPool := []string{"a/b", "a/+", "a/#", "#", "+/b", "$share/g/a/b"}
	probes := []string{"a/c", "b/b", "a/b/c", "a/b"}

	m := &modelBroker{t: t, singleCopy: seed%2 == 1}
	b := testbroker.New(t)
	b.SetFallback(m.serve)
	cli := tbClient(t, b)

	var open []*modelSub
	for round := range rounds {
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			added   []*modelSub
			removed []*modelSub
		)
		for range 1 + rng.IntN(4) {
			switch op := rng.IntN(10); {
			case op < 4: // subscribe
				filter := filterPool[rng.IntN(len(filterPool))]
				wg.Go(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					ch, tok, err := cli.Subscribe(ctx, []TopicFilter{{Topic: filter}}, SubBuffer(64))
					if errors.Is(err, ErrNotConnected) {
						return
					}
					if err != nil {
						t.Errorf("round %d: Subscribe(%s): %v", round, filter, err)
						return
					}
					mu.Lock()
					added = append(added, &modelSub{filter: filter, ch: ch, token: tok})
					mu.Unlock()
				})
			case op < 6: // subscribe, then give up at a random point
				filter := filterPool[rng.IntN(len(filterPool))]
				wait := time.Duration(rng.IntN(300)) * time.Microsecond
				wg.Go(func() {
					ctx, cancel := context.WithTimeout(context.Background(), wait)
					defer cancel()
					ch, tok, err := cli.Subscribe(ctx, []TopicFilter{{Topic: filter}}, SubBuffer(64))
					if err != nil {
						return
					}
					mu.Lock()
					added = append(added, &modelSub{filter: filter, ch: ch, token: tok})
					mu.Unlock()
				})
			case op < 9: // unsubscribe one opened in an earlier round
				if len(open) == 0 {
					continue
				}
				i := rng.IntN(len(open))
				s := open[i]
				open = slices.Delete(open, i, i+1)
				removed = append(removed, s)
				wg.Go(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := cli.Unsubscribe(ctx, s.token); err != nil {
						t.Errorf("round %d: Unsubscribe(%s): %v", round, s.filter, err)
					}
				})
			default:
				m.drop(rng.IntN(2) == 0)
			}
		}
		wg.Wait()
		if t.Failed() {
			return
		}
		open = append(open, added...)
		for _, s := range removed {
			for msg := range s.ch {
				t.Fatalf("round %d: unsubscribed %s still held %q", round, s.filter, msg.Payload)
			}
		}

		want := make([]string, 0, len(open))
		for _, s := range open {
			if !slices.Contains(want, s.filter) {
				want = append(want, s.filter)
			}
		}
		slices.Sort(want)
		deadline := time.Now().Add(5 * time.Second)
		for !slices.Equal(m.filters(), want) || !cli.ctrlIdle() || !m.live() || cli.cur.Load() == nil {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: broker holds %q, open subscriptions %q", round, m.filters(), want)
			}
			time.Sleep(time.Millisecond)
		}

		for _, p := range probes {
			m.publish(p, fmt.Sprintf("%d:%s", round, p))
		}
		for _, s := range open {
			var expect []string
			for _, p := range probes {
				if filterMatches(s.filter, p) {
					expect = append(expect, fmt.Sprintf("%d:%s", round, p))
				}
			}
			for _, e := range expect {
				var msg *Message
				select {
				case msg = <-s.ch:
				case <-time.After(5 * time.Second):
					var tracked []uint64
					for _, o := range open {
						tracked = append(tracked, o.token.sub.id)
					}
					t.Fatalf("round %d: %s (sub %d) did not receive %q\nbroker %v\nclient %s\ntracked %v", round, s.filter, s.token.sub.id, e, m.subsSnapshot(), cli.subsDump(), tracked)
				}
				if msg == nil {
					t.Fatalf("round %d: %s closed before %q", round, s.filter, e)
				}
				if string(msg.Payload) != e {
					t.Fatalf("round %d: %s received %q, want %q (expected %q)", round, s.filter, msg.Payload, e, expect)
				}
			}
		}
	}
	for _, s := range open {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := cli.Unsubscribe(ctx, s.token)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		for msg := range s.ch {
			t.Fatalf("%s received an extra %q", s.filter, msg.Payload)
		}
	}
}

// ctrlIdle reports whether no SUBSCRIBE or UNSUBSCRIBE awaits an answer.
func (c *Client) ctrlIdle() bool {
	c.ctrlMu.Lock()
	defer c.ctrlMu.Unlock()
	return len(c.ctrlQueue) == 0
}

// poisonBroker serves every connection like the auto-responder, but
// answers a SUBSCRIBE for filter with DISCONNECT 0x81 and a close, as
// mosquitto does for a filter deeper than its limit. It counts those.
func poisonBroker(t *testing.T, filter string) (*testbroker.Broker, *atomic.Int32) {
	var poisoned atomic.Int32
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		if c.AcceptResume(wire.ConnackOpts{}) == nil {
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
			case wire.SUBSCRIBE:
				if slices.ContainsFunc(p.Filters, func(f wire.SubscribeFilter) bool { return f.Topic == filter }) {
					poisoned.Add(1)
					_ = c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonMalformedPacket})
					c.Close()
					return
				}
				codes := make([]wire.ReasonCode, len(p.Filters))
				for i, f := range p.Filters {
					codes[i] = wire.ReasonCode(f.QoS)
				}
				_ = c.Suback(p.PacketID, codes...)
			case wire.PINGREQ:
				_ = c.Write(wire.WritePingresp)
			case wire.DISCONNECT:
				return
			}
		}
	})
	return b, &poisoned
}

// A SUBSCRIBE the broker answers by disconnecting is sent
// maxCtrlAttempts times, then fails with RejectedByDisconnectError; the
// client carries on, and other subscriptions work.
func TestPoisonedSubscribeGivesUp(t *testing.T) {
	b, poisoned := poisonBroker(t, "deep/filter")
	cli := tbClient(t, b, WithReconnectBackoff(ConstantBackoff(time.Millisecond)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "deep/filter", QoS: 1}})
	var rej *RejectedByDisconnectError
	if !errors.As(err, &rej) || rej.Attempts != maxCtrlAttempts || rej.Disconnect.ReasonCode != wire.ReasonMalformedPacket {
		t.Fatalf("Subscribe: %v, want RejectedByDisconnectError after %d attempts", err, maxCtrlAttempts)
	}
	if n := poisoned.Load(); n != maxCtrlAttempts {
		t.Fatalf("broker received the SUBSCRIBE %d times", n)
	}
	if err := cli.AwaitConnection(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "fine", QoS: 1}}); err != nil {
		t.Fatalf("a later Subscribe: %v", err)
	}
	if n := poisoned.Load(); n != maxCtrlAttempts {
		t.Fatalf("the abandoned SUBSCRIBE was sent again: %d", n)
	}
}

// A caller that stops waiting does not keep the poisoned SUBSCRIBE
// alive: it still stops after maxCtrlAttempts.
func TestAbandonedPoisonedSubscribeStops(t *testing.T) {
	b, poisoned := poisonBroker(t, "deep/filter")
	cli := tbClient(t, b, WithReconnectBackoff(ConstantBackoff(time.Millisecond)))
	short, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, _, err := cli.Subscribe(short, []TopicFilter{{Topic: "deep/filter"}}); err == nil {
		t.Fatal("Subscribe succeeded")
	}
	deadline := time.Now().Add(5 * time.Second)
	for poisoned.Load() < maxCtrlAttempts && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if n := poisoned.Load(); n != maxCtrlAttempts {
		t.Fatalf("broker received the SUBSCRIBE %d times, want %d", n, maxCtrlAttempts)
	}
}

// A connection that ends as soon as it is up does not restart the
// reconnect backoff; one that lasted longer than the delay before it
// does.
func TestReconnectBackoffGrowsWhileConnectionsDieAtOnce(t *testing.T) {
	clk := clock.NewFake(time.Unix(1_000, 0))
	drop := func(c *testbroker.Conn) { c.AcceptResume(wire.ConnackOpts{}); c.Close() }
	hold := func(c *testbroker.Conn) { c.AcceptResume(wire.ConnackOpts{}); c.ServeAuto() }
	b := testbroker.New(t, drop, drop, drop, hold)
	b.SetFallback(hold)

	var mu sync.Mutex
	var attempts []int
	reconnected := make(chan struct{}, 8)
	cli, err := New(WithBroker(b.URL()), WithClientID("backoff"), WithLogger(quietLogger()), WithoutKeepAlive(),
		withClock(clk), WithReconnectBackoff(ExponentialBackoff(time.Second, 30*time.Second, 0)),
		WithOnReconnectAttempt(func(n int, _ string) {
			mu.Lock()
			attempts = append(attempts, n)
			mu.Unlock()
		}),
		WithOnConnectionUp(func(ConnackInfo) { reconnected <- struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	<-reconnected

	// Three connections die at once: the next delay is still pending
	// until the fake clock passes it.
	for range 3 {
		if !clk.WaitPending(1, 5*time.Second) {
			t.Fatal("no reconnect timer")
		}
		clk.Advance(30 * time.Second)
		<-reconnected
	}
	// The fourth stays up longer than the delay before the next attempt.
	clk.Advance(time.Minute)
	b.DropAll()
	if !clk.WaitPending(1, 5*time.Second) {
		t.Fatal("no reconnect timer")
	}
	clk.Advance(30 * time.Second)
	<-reconnected

	mu.Lock()
	defer mu.Unlock()
	if want := []int{1, 2, 3, 1}; !slices.Equal(attempts, want) {
		t.Fatalf("reconnect attempts %v, want %v", attempts, want)
	}
}
