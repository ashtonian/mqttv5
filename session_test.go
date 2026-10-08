// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// A DUP resend of a message the application still holds must not be
// acknowledged on its behalf, nor delivered twice.
func TestDupRedeliveryIsNotAckedForTheApplication(t *testing.T) {
	held := make(chan struct{})
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			c.Publish(wire.PublishOpts{Topic: "t/1", Payload: []byte("m"), QoS: 1, PacketID: 1})
			<-held
		},
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			c.Publish(wire.PublishOpts{Topic: "t/1", Payload: []byte("m"), QoS: 1, PacketID: 1, Dup: true})
			c.Hold(0)
		},
	)
	cli := tbClient(t, b, WithStats())
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "t/#", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	m := recvMsg(t, ch, 2*time.Second)
	close(held)
	// The script reads the second connection; this goroutine only looks
	// at its log, so the two never compete for a packet.
	deadline := time.Now().Add(3 * time.Second)
	for cli.Stats().InboundPublishes < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the retransmission never reached the client")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // a wrong PUBACK would be written by now
	conn := b.Conn(1, 0)
	if countType(conn, wire.PUBACK) != 0 {
		t.Fatal("the retransmission was acknowledged before the application acked")
	}
	select {
	case dup := <-ch:
		t.Fatalf("retransmission delivered again: %q", dup.Payload)
	default:
	}
	_ = m.Ack()
	waitLog(t, conn, wire.PUBACK, 1)
	for _, p := range conn.Log() {
		if p.Type == wire.PUBACK && p.PacketID != 1 {
			t.Fatalf("PUBACK id = %d", p.PacketID)
		}
	}
}

func countType(conn *testbroker.Conn, pt wire.PacketType) int {
	n := 0
	for _, p := range conn.Log() {
		if p.Type == pt {
			n++
		}
	}
	return n
}

// An Ack made while disconnected is sent once the session resumes.
func TestAckWhileDisconnectedFlushedOnResume(t *testing.T) {
	held, acked := make(chan struct{}), make(chan struct{})
	puback := make(chan uint16, 1)
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			c.Publish(wire.PublishOpts{Topic: "t/1", QoS: 1, PacketID: 4})
			<-held
		},
		func(c *testbroker.Conn) {
			<-acked
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			puback <- c.Expect(wire.PUBACK, 0).PacketID
			c.Hold(0)
		},
	)
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "t/#", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	m := recvMsg(t, ch, 2*time.Second)
	close(held)
	if b.Conn(1, 3*time.Second) == nil {
		t.Fatal("client did not redial")
	}
	_ = m.Ack() // the reconnect is still waiting for CONNACK
	close(acked)
	select {
	case id := <-puback:
		if id != 4 {
			t.Fatalf("PUBACK id = %d", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no PUBACK after resume")
	}
}

// A QoS 2 PUBLISH resent after our PUBREC was lost gets PUBREC again
// and is not delivered twice.
func TestQoS2DuplicateGetsPubrecAgain(t *testing.T) {
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			c.Publish(wire.PublishOpts{Topic: "q2/a", Payload: []byte("x"), QoS: 2, PacketID: 7})
			c.Expect(wire.PUBREC, 0)
		},
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			c.Publish(wire.PublishOpts{Topic: "q2/a", Payload: []byte("x"), QoS: 2, PacketID: 7, Dup: true})
			if p := c.Expect(wire.PUBREC, 0); p.PacketID != 7 {
				c.T.Errorf("PUBREC id = %d", p.PacketID)
			}
			c.Pubrel(7, wire.ReasonSuccess)
			if p := c.Expect(wire.PUBCOMP, 0); p.Reason != wire.ReasonSuccess {
				c.T.Errorf("PUBCOMP reason = %#x", byte(p.Reason))
			}
			c.Hold(0)
		},
	)
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "q2/#", QoS: 2}})
	if err != nil {
		t.Fatal(err)
	}
	_ = recvMsg(t, ch, 2*time.Second).Ack()
	conn := b.Conn(1, 3*time.Second)
	if conn == nil {
		t.Fatal("no reconnect")
	}
	waitLog(t, conn, wire.PUBCOMP, 1)
	select {
	case m := <-ch:
		t.Fatalf("QoS 2 duplicate delivered: %q", m.Payload)
	default:
	}
}

// PUBREL for an unknown identifier is answered with PUBCOMP 0x92.
func TestUnknownPubrelGetsPubcomp92(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Pubrel(99, wire.ReasonSuccess)
		if p := c.Expect(wire.PUBCOMP, 0); p.PacketID != 99 || p.Reason != wire.ReasonPacketIdentifierNotFound {
			c.T.Errorf("PUBCOMP = id %d reason %#x", p.PacketID, byte(p.Reason))
		}
		c.Hold(0)
	})
	tbClient(t, b)
	conn := b.Conn(0, time.Second)
	waitLog(t, conn, wire.PUBCOMP, 1)
}

// After PUBREC a resumed session resends PUBREL, never the PUBLISH.
func TestQoS2ResumeAfterPubrecSendsPubrel(t *testing.T) {
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			p := c.Expect(wire.PUBLISH, 0)
			c.Pubrec(p.PacketID, wire.ReasonSuccess)
			c.Expect(wire.PUBREL, 0) // PUBCOMP is lost with the connection
		},
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			p, ok, _ := c.Next(0)
			if !ok || p.Type != wire.PUBREL {
				c.T.Errorf("first packet on resume = %v %s, want PUBREL", ok, p.Type)
				return
			}
			c.Pubcomp(p.PacketID, wire.ReasonSuccess)
			c.Hold(0)
		},
	)
	cli := tbClient(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, PublishOptions{Topic: "x", Payload: []byte("once"), QoS: 2}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// A resumed session resends unacknowledged PUBLISHes in their
// original send order, with DUP=1, and every Publish completes.
func TestResumeKeepsSendOrder(t *testing.T) {
	const n = 32
	first := make(chan []uint16, 1)
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			first <- readPublishIDs(c, n, false)
		},
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			ids := readPublishIDs(c, n, true)
			want := <-first
			if !slices.Equal(ids, want) {
				c.T.Errorf("resend order differs [MQTT-4.6.0-1]\n original: %v\n resent:   %v", want, ids)
			}
			for _, id := range ids {
				c.Puback(id, wire.ReasonSuccess)
			}
			c.Hold(0)
		},
	)
	cli := tbClient(t, b)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			if err := cli.Publish(ctx, PublishOptions{Topic: "o", Payload: []byte(fmt.Sprint(i)), QoS: 1}); err != nil {
				t.Errorf("Publish %d: %v", i, err)
			}
		})
	}
	wg.Wait()
}

func readPublishIDs(c *testbroker.Conn, n int, wantDup bool) []uint16 {
	var ids []uint16
	for len(ids) < n {
		p, ok := c.Await(wire.PUBLISH, 3*time.Second)
		if !ok {
			c.T.Errorf("got %d of %d PUBLISHes", len(ids), n)
			break
		}
		if p.Dup != wantDup {
			c.T.Errorf("PUBLISH %d dup=%v, want %v", p.PacketID, p.Dup, wantDup)
		}
		ids = append(ids, p.PacketID)
	}
	return ids
}

// With Session Present = 0 the stale inbound state is discarded, so
// the new session's PUBLISH reusing an identifier is delivered.
func TestSessionNotPresentClearsInboundState(t *testing.T) {
	newPubrec := make(chan struct{})
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			c.Publish(wire.PublishOpts{Topic: "s/a", Payload: []byte("old"), QoS: 2, PacketID: 5})
			c.Expect(wire.PUBREC, 0)
		},
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: false})
			c.ServeSubscribe(-1) // the client re-subscribes into the new session
			c.Publish(wire.PublishOpts{Topic: "s/a", Payload: []byte("new"), QoS: 2, PacketID: 5})
			c.Expect(wire.PUBREC, 0)
			close(newPubrec)
			c.Hold(0)
		},
	)
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "s/#", QoS: 2}})
	if err != nil {
		t.Fatal(err)
	}
	_ = recvMsg(t, ch, 2*time.Second).Ack()
	m := recvMsg(t, ch, 3*time.Second)
	if string(m.Payload) != "new" {
		t.Fatalf("payload = %q", m.Payload)
	}
	_ = m.Ack()
	select {
	case <-newPubrec:
	case <-time.After(3 * time.Second):
		t.Fatal("broker never received the PUBREC for the new session's message")
	}
}

// With Session Present = 0 unacknowledged publishes are sent again
// as new messages (DUP=0) by default, and fail under SessionLossFail.
func TestSessionLossPolicies(t *testing.T) {
	tests := []struct {
		name   string
		opts   []Option
		resend bool
		err    error
	}{
		{"republish", nil, true, nil},
		{"fail", []Option{WithSessionLossPolicy(SessionLossFail)}, false, ErrSessionLost},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testbroker.New(t,
				func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{})
					c.Expect(wire.PUBLISH, 0)
				},
				func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{SessionPresent: false})
					p, ok := c.Await(wire.PUBLISH, time.Second)
					if ok != tt.resend {
						c.T.Errorf("PUBLISH after session loss: got %v, want %v", ok, tt.resend)
					}
					if ok {
						if p.Dup {
							c.T.Error("republished message has DUP=1 in a new session")
						}
						c.Puback(p.PacketID, wire.ReasonSuccess)
					}
					c.Hold(0)
				},
			)
			cli := tbClient(t, b, tt.opts...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := cli.Publish(ctx, PublishOptions{Topic: "x", QoS: 1})
			if !errors.Is(err, tt.err) {
				t.Fatalf("Publish = %v, want %v", err, tt.err)
			}
		})
	}
}

// [MQTT-3.2.2-2]: Session Present = 1 in reply to CleanStart = 1 is a
// protocol error.
func TestCleanStartWithSessionPresentIsProtocolError(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
		if p := c.Expect(wire.DISCONNECT, 0); p.Reason != wire.ReasonProtocolError {
			c.T.Errorf("DISCONNECT reason = %#x", byte(p.Reason))
		}
	})
	cli, err := New(WithBroker(b.URL()), WithClientID("cs-sp"), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = cli.Connect(ctx)
	var perr *ProtocolError
	if !errors.As(err, &perr) || perr.Reason != wire.ReasonProtocolError {
		t.Fatalf("Connect = %v, want ProtocolError 0x82", err)
	}
	waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
}

// A broker-assigned ClientID is reused on reconnect.
func TestAssignedClientIDReused(t *testing.T) {
	ids := make(chan string, 2)
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			ci := c.AcceptConnect(wire.ConnackOpts{AssignedClientIdentifier: "assigned-1"})
			ids <- ci.ClientID
		},
		func(c *testbroker.Conn) {
			ci := c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			ids <- ci.ClientID
			c.Hold(0)
		},
	)
	cli, err := New(WithBroker(b.URL()), WithClientID(""), WithLogger(quietLogger()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	if got := <-ids; got != "" {
		t.Fatalf("first CONNECT ClientID = %q", got)
	}
	if cli.ClientID() != "assigned-1" {
		t.Fatalf("ClientID() = %q", cli.ClientID())
	}
	select {
	case got := <-ids:
		if got != "assigned-1" {
			t.Fatalf("reconnect CONNECT ClientID = %q, want assigned-1", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect")
	}
}

// A PUBACK for an identifier a SUBSCRIBE owns is ignored and counted;
// the SUBSCRIBE still completes on its SUBACK.
func TestStrayPubackIgnored(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		p := c.Expect(wire.SUBSCRIBE, 0)
		c.Puback(p.PacketID, wire.ReasonSuccess)
		c.Suback(p.PacketID, wire.ReasonGrantedQoS1)
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithStats())
	if _, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "x", QoS: 1}}); err != nil {
		t.Fatal(err)
	}
	if got := cli.Stats().AcksIgnored; got != 1 {
		t.Fatalf("AcksIgnored = %d", got)
	}
}

// A QoS 1/2 publish never overtakes a QoS 0 publish queued before it by
// the same caller.
func TestQoS1DoesNotOvertakeEarlierQoS0(t *testing.T) {
	const n = 50
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if err := cli.Publish(ctx, PublishOptions{Topic: "o", Payload: []byte(fmt.Sprintf("a%d", i))}); err != nil {
			t.Fatal(err)
		}
		if err := cli.Publish(ctx, PublishOptions{Topic: "o", Payload: []byte(fmt.Sprintf("b%d", i)), QoS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.PUBLISH {
			got = append(got, string(p.Payload))
		}
	}
	for i := 0; i < n; i++ {
		if got[2*i] != fmt.Sprintf("a%d", i) || got[2*i+1] != fmt.Sprintf("b%d", i) {
			t.Fatalf("write order broken at %d: %v", i, got[max(0, 2*i-2):min(len(got), 2*i+2)])
		}
	}
}

// Server Receive Maximum caps the QoS > 0 PUBLISHes in flight (§4.9).
func TestServerReceiveMaximumHonoured(t *testing.T) {
	const max = 3
	limit := uint16(max)
	release := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{ReceiveMaximum: &limit})
		var held []uint16
		for len(held) < max {
			p := c.Expect(wire.PUBLISH, 0)
			held = append(held, p.PacketID)
		}
		c.ExpectNone(wire.PUBLISH, 300*time.Millisecond)
		<-release
		for _, id := range held {
			c.Puback(id, wire.ReasonSuccess)
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	var wg sync.WaitGroup
	for i := 0; i < 2*max; i++ {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := cli.Publish(ctx, PublishOptions{Topic: "rm", QoS: 1}); err != nil {
				t.Error(err)
			}
		})
	}
	time.Sleep(500 * time.Millisecond)
	close(release)
	wg.Wait()
}

// waitLog waits until conn has recorded n packets of type pt.
func waitLog(t *testing.T, conn *testbroker.Conn, pt wire.PacketType, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for _, p := range conn.Log() {
			if p.Type == pt {
				count++
			}
		}
		if count >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("fewer than %d %s recorded", n, pt)
}

// §3.3.4: a broker that sends more unacknowledged QoS > 0
// PUBLISHes than the client's Receive Maximum is disconnected with 0x93.
func TestInboundReceiveMaximumEnforced(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		for id := uint16(1); id <= 3; id++ {
			c.Publish(wire.PublishOpts{Topic: "rm/x", QoS: 1, PacketID: id})
		}
		if p := c.Expect(wire.DISCONNECT, 0); p.Reason != wire.ReasonReceiveMaximumExceeded {
			c.T.Errorf("DISCONNECT reason = %#x, want 0x93", byte(p.Reason))
		}
	})
	cli := tbClient(t, b, WithReceiveMaximum(2), WithStats())
	if _, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "rm/#", QoS: 1}}); err != nil {
		t.Fatal(err)
	}
	waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
	if got := cli.Stats().ProtocolErrors; got != 1 {
		t.Fatalf("ProtocolErrors = %d", got)
	}
}
