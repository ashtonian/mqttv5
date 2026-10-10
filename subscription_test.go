// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func subIDOf(p testbroker.Packet) uint32 {
	v, _ := p.Properties().Varint(wire.PropSubscriptionIdentifier)
	return v
}

func expectClosed(t *testing.T, ch <-chan *Message) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("received a message on a subscription that should be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed")
	}
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
