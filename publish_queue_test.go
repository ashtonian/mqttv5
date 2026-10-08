// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

// queueBroker answers PUBLISHes and records them. answer picks the
// PUBACK/PUBREC reason for each; gate, when set, holds every answer
// until it is closed.
type queueBroker struct {
	mu     sync.Mutex
	got    []testbroker.Packet
	held   []testbroker.Packet
	open   bool
	answer func(p testbroker.Packet) wire.ReasonCode
	gate   chan struct{}
	seen   chan testbroker.Packet
}

func newQueueBroker() *queueBroker {
	return &queueBroker{seen: make(chan testbroker.Packet, 256)}
}

func (qb *queueBroker) serve(c *testbroker.Conn) {
	if qb.gate != nil {
		go func() {
			select {
			case <-qb.gate:
			case <-c.Gone():
				return
			}
			qb.mu.Lock()
			qb.open = true
			held := qb.held
			qb.held = nil
			qb.mu.Unlock()
			for _, p := range held {
				qb.reply(c, p)
			}
		}()
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
			qb.mu.Lock()
			qb.got = append(qb.got, p)
			hold := qb.gate != nil && !qb.open
			if hold {
				qb.held = append(qb.held, p)
			}
			qb.mu.Unlock()
			qb.seen <- p
			if !hold {
				qb.reply(c, p)
			}
		case wire.PUBREL:
			c.Pubcomp(p.PacketID, wire.ReasonSuccess)
		case wire.PINGREQ:
			c.Write(wire.WritePingresp)
		case wire.DISCONNECT:
			return
		}
	}
}

func (qb *queueBroker) reply(c *testbroker.Conn, p testbroker.Packet) {
	rc := wire.ReasonSuccess
	if qb.answer != nil {
		rc = qb.answer(p)
	}
	if p.QoS == 1 {
		c.Puback(p.PacketID, rc)
	} else {
		c.Pubrec(p.PacketID, rc)
	}
}

func (qb *queueBroker) payloads() []string {
	qb.mu.Lock()
	defer qb.mu.Unlock()
	var out []string
	for _, p := range qb.got {
		out = append(out, string(p.Payload))
	}
	return out
}

func (qb *queueBroker) next(t *testing.T) testbroker.Packet {
	t.Helper()
	select {
	case p := <-qb.seen:
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("broker received no PUBLISH")
		return testbroker.Packet{}
	}
}

func newQueuePublisher(t *testing.T, cli *Client, q PublisherQueue, opts ...QueueOption) *QueuePublisher {
	t.Helper()
	p, err := NewQueuePublisher(cli, q, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})
	return p
}

func enqueueAll(t *testing.T, p *QueuePublisher, qos byte, payloads ...string) {
	t.Helper()
	for _, s := range payloads {
		if err := p.Publish(context.Background(), PublishOptions{Topic: "q/t", QoS: qos, Payload: []byte(s)}); err != nil {
			t.Fatalf("Publish(%s): %v", s, err)
		}
	}
}

func waitQueueLen(t *testing.T, q PublisherQueue, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got, err := q.Len(context.Background())
		if err == nil && got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue length %d (%v), want %d", got, err, n)
		}
		time.Sleep(time.Millisecond)
	}
}

type deadLetters struct {
	mu   sync.Mutex
	list []string
	errs []error
}

func (d *deadLetters) record(e QueueEntry, err error) {
	d.mu.Lock()
	d.list = append(d.list, string(e.Publish.Payload))
	d.errs = append(d.errs, err)
	d.mu.Unlock()
}

func (d *deadLetters) get() ([]string, []error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.list), slices.Clone(d.errs)
}

func TestQueuePublisherRejectsInvalidMessages(t *testing.T) {
	cli, _ := New(WithBroker("mqtt://127.0.0.1:1"), WithLogger(quietLogger()))
	p := newQueuePublisher(t, cli, NewMemoryPublisherQueue())
	for _, tt := range []struct {
		opts PublishOptions
		want error
	}{
		{PublishOptions{Topic: "t", QoS: 0}, ErrQoS0NotQueueable},
		{PublishOptions{Topic: "t/#", QoS: 1}, ErrInvalidTopic},
		{PublishOptions{Topic: "t", QoS: 1, TopicAlias: 2}, ErrTopicAliasInvalid},
	} {
		if err := p.Publish(context.Background(), tt.opts); !errors.Is(err, tt.want) {
			t.Errorf("Publish(%+v) = %v, want %v", tt.opts, err, tt.want)
		}
	}
	if n, _ := p.queue.Len(context.Background()); n != 0 {
		t.Fatalf("%d invalid messages queued", n)
	}
}

// Up to the window is in flight at once, sent in queue order.
func TestQueuePublisherPipelines(t *testing.T) {
	qb := newQueueBroker()
	qb.gate = make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	cli := tbClient(t, b)
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, cli, q, WithQueueWindow(8))
	var want []string
	for i := range 20 {
		want = append(want, fmt.Sprint(i))
	}
	enqueueAll(t, p, 1, want...)

	for range 8 {
		qb.next(t)
	}
	select {
	case extra := <-qb.seen:
		t.Fatalf("PUBLISH %q beyond the window of 8 before any PUBACK", extra.Payload)
	case <-time.After(100 * time.Millisecond):
	}
	close(qb.gate)
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, want) {
		t.Fatalf("broker received %q, want %q", got, want)
	}
}

// A refusal that cannot pass is dead-lettered once and does not
// hold up the messages behind it.
func TestQueuePublisherDeadLettersPermanentRefusal(t *testing.T) {
	qb := newQueueBroker()
	qb.answer = func(p testbroker.Packet) wire.ReasonCode {
		if string(p.Payload) == "bad" {
			return wire.ReasonNotAuthorized
		}
		return wire.ReasonSuccess
	}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	var dl deadLetters
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, tbClient(t, b), q, WithDeadLetter(dl.record))
	enqueueAll(t, p, 1, "bad", "good")
	waitQueueLen(t, q, 0)
	got, errs := dl.get()
	if !slices.Equal(got, []string{"bad"}) || !errors.Is(errs[0], ErrNotAuthorized) {
		t.Fatalf("dead letters %q %v", got, errs)
	}
	if payloads := qb.payloads(); !slices.Equal(payloads, []string{"bad", "good"}) {
		t.Fatalf("broker received %q", payloads)
	}
}

// A refusal that may pass is retried after the backoff.
func TestQueuePublisherRetriesPassingRefusal(t *testing.T) {
	qb := newQueueBroker()
	var attempts int
	qb.answer = func(p testbroker.Packet) wire.ReasonCode {
		if string(p.Payload) == "busy" {
			attempts++
			if attempts == 1 {
				return wire.ReasonQuotaExceeded
			}
		}
		return wire.ReasonSuccess
	}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	var dl deadLetters
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, tbClient(t, b), q, WithDeadLetter(dl.record), WithQueueRetryBackoff(ConstantBackoff(time.Millisecond)))
	enqueueAll(t, p, 2, "busy", "next")
	waitQueueLen(t, q, 0)
	if got, _ := dl.get(); len(got) != 0 {
		t.Fatalf("dead-lettered %q", got)
	}
	if got := qb.payloads(); len(got) != 3 || slices.Index(got, "next") < 0 || got[0] != "busy" {
		t.Fatalf("broker received %q, want busy twice and next once", got)
	}
}

// A slow broker never makes the publisher start a second exchange
// for the same message.
func TestQueuePublisherOneExchangePerMessage(t *testing.T) {
	qb := newQueueBroker()
	qb.gate = make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, tbClient(t, b), q)
	enqueueAll(t, p, 2, "slow")
	qb.next(t)
	select {
	case again := <-qb.seen:
		t.Fatalf("second PUBLISH %+v while the first was unanswered", again)
	case <-time.After(300 * time.Millisecond):
	}
	close(qb.gate)
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); len(got) != 1 {
		t.Fatalf("broker received %q", got)
	}
}

// Constructing a publisher never removes queued entries.
func TestQueuePublisherConstructionKeepsEntries(t *testing.T) {
	q := NewMemoryPublisherQueue()
	if _, _, err := q.Enqueue(context.Background(), QueueEntry{ID: "kept", Publish: PublishOptions{Topic: "t", QoS: 1}}, QueueLimit{}); err != nil {
		t.Fatal(err)
	}
	cli, _ := New(WithBroker("mqtt://127.0.0.1:1"), WithLogger(quietLogger()))
	newQueuePublisher(t, cli, q, WithQueueDropPolicy(DropOldest), WithQueueMaxSize(1))
	if n, _ := q.Len(context.Background()); n != 1 {
		t.Fatalf("queue length %d after construction, want 1", n)
	}
}

// DropOldest evicts only messages not yet being published.
func TestQueuePublisherDropOldestSparesInFlight(t *testing.T) {
	qb := newQueueBroker()
	qb.gate = make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	var dl deadLetters
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, tbClient(t, b), q, WithQueueWindow(2), WithQueueMaxSize(3),
		WithQueueDropPolicy(DropOldest), WithDeadLetter(dl.record))
	enqueueAll(t, p, 1, "0", "1")
	qb.next(t)
	qb.next(t)
	enqueueAll(t, p, 1, "2", "3", "4", "5")
	got, errs := dl.get()
	if !slices.Equal(got, []string{"2", "3", "4"}) || !errors.Is(errs[0], ErrQueueFull) {
		t.Fatalf("evicted %q %v, want 2, 3 and 4 with ErrQueueFull", got, errs)
	}
	close(qb.gate)
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, []string{"0", "1", "5"}) {
		t.Fatalf("broker received %q", got)
	}
}

// A message carries the lifetime it has left, and one that ran out
// while queued is dead-lettered instead of sent.
func TestQueuePublisherSendsRemainingLifetime(t *testing.T) {
	qb := newQueueBroker()
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	clk := clock.NewFake(time.Unix(1_000, 0))
	cli, err := New(WithBroker(b.URL()), WithClientID("ttl"), WithLogger(quietLogger()), withClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	var dl deadLetters
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, cli, q, WithQueueTTL(time.Minute), WithDeadLetter(dl.record))
	ctx := context.Background()
	own, short := uint32(30), uint32(10)
	for _, m := range []struct {
		payload string
		expiry  *uint32
	}{{"ttl", nil}, {"own", &own}, {"short", &short}} {
		if err := p.Publish(ctx, PublishOptions{Topic: "t", QoS: 1, Payload: []byte(m.payload), MessageExpiryInterval: m.expiry}); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(25 * time.Second)
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	waitQueueLen(t, q, 0)

	expiry := map[string]uint32{}
	qb.mu.Lock()
	for _, pk := range qb.got {
		v, _ := pk.Properties().Uint32(wire.PropMessageExpiryInterval)
		expiry[string(pk.Payload)] = v
	}
	qb.mu.Unlock()
	if len(expiry) != 2 || expiry["ttl"] != 35 || expiry["own"] != 5 {
		t.Fatalf("Message Expiry sent %v, want ttl 35 and own 5", expiry)
	}
	if got, errs := dl.get(); !slices.Equal(got, []string{"short"}) || !errors.Is(errs[0], ErrMessageExpired) {
		t.Fatalf("dead letters %q %v", got, errs)
	}
}

// A restarted process continues the exchange its predecessor started
// instead of publishing the message again.
func TestQueuePublisherContinuesExchangeAfterRestart(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprint("qos", qos), func(t *testing.T) {
			first := make(chan testbroker.Packet, 1)
			resumed := make(chan []testbroker.Packet, 1)
			b := testbroker.New(t,
				func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{})
					p := c.Expect(wire.PUBLISH, 0)
					if qos == 2 {
						c.Pubrec(p.PacketID, wire.ReasonSuccess) // the restart finds it at AwaitPubcomp
						c.Expect(wire.PUBREL, 0)
					}
					first <- p
					<-c.Gone()
				},
				func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
					var got []testbroker.Packet
					p, _ := c.Await(map[byte]wire.PacketType{1: wire.PUBLISH, 2: wire.PUBREL}[qos], 0)
					got = append(got, p)
					if qos == 1 {
						c.Puback(p.PacketID, wire.ReasonSuccess)
					} else {
						c.Pubcomp(p.PacketID, wire.ReasonSuccess)
					}
					if extra, ok := c.Await(wire.PUBLISH, 300*time.Millisecond); ok {
						got = append(got, extra)
					}
					resumed <- got
					c.ServeAuto()
				},
			)
			st := session.NewMemoryStore()
			q := NewMemoryPublisherQueue()
			var dl deadLetters

			cli1 := tbClient(t, b, WithStore(st))
			p1, err := NewQueuePublisher(cli1, q)
			if err != nil {
				t.Fatal(err)
			}
			enqueueAll(t, p1, qos, "once")
			sent := <-first
			// The process dies: its client and publisher stop; the store and
			// queue keep what they hold.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := cli1.Disconnect(ctx); err != nil {
				t.Fatal(err)
			}

			cli2 := tbClient(t, b, WithStore(st))
			newQueuePublisher(t, cli2, q, WithDeadLetter(dl.record))
			got := <-resumed
			waitQueueLen(t, q, 0)
			if len(got) != 1 || got[0].PacketID != sent.PacketID || (qos == 1 && !got[0].Dup) {
				t.Fatalf("after the restart the broker received %+v; want only the resent %s for packet %d",
					got, map[byte]string{1: "PUBLISH (DUP)", 2: "PUBREL"}[qos], sent.PacketID)
			}
			if letters, _ := dl.get(); len(letters) != 0 {
				t.Fatalf("dead letters %q", letters)
			}
		})
	}
}

// When the broker lost the session the message is published again, once.
func TestQueuePublisherRepublishesAfterSessionLoss(t *testing.T) {
	again := make(chan []testbroker.Packet, 1)
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.Expect(wire.PUBLISH, 0)
			<-c.Gone()
		},
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			var got []testbroker.Packet
			for {
				p, ok := c.Await(wire.PUBLISH, 300*time.Millisecond)
				if !ok {
					break
				}
				got = append(got, p)
				c.Puback(p.PacketID, wire.ReasonSuccess)
			}
			again <- got
			c.ServeAuto()
		},
	)
	q := NewMemoryPublisherQueue()
	cli := tbClient(t, b, WithSessionLossPolicy(SessionLossFail))
	p := newQueuePublisher(t, cli, q)
	enqueueAll(t, p, 1, "m")
	waitLog(t, b.Conn(0, time.Second), wire.PUBLISH, 1)
	b.Conn(0, 0).Close()
	got := <-again
	waitQueueLen(t, q, 0)
	if len(got) != 1 || got[0].Dup || string(got[0].Payload) != "m" {
		t.Fatalf("after the session loss the broker received %+v; want one new PUBLISH", got)
	}
}

// A message the broker's limits rule out is dead-lettered, not retried.
// With WithQoSDowngrade too: a QoS 0 message could never be
// acknowledged, so the publisher does not send one.
func TestQueuePublisherDeadLettersWhatTheBrokerCannotTake(t *testing.T) {
	for _, downgrade := range []bool{false, true} {
		t.Run(fmt.Sprintf("downgrade=%v", downgrade), func(t *testing.T) {
			zero := byte(0)
			published := make(chan testbroker.Packet, 1)
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.AcceptConnect(wire.ConnackOpts{MaximumQoS: &zero})
				if p, ok := c.Await(wire.PUBLISH, 300*time.Millisecond); ok {
					published <- p
				}
				c.ServeAuto()
			})
			var opts []Option
			if downgrade {
				opts = append(opts, WithQoSDowngrade())
			}
			cli := tbClient(t, b, opts...)
			var dl deadLetters
			q := NewMemoryPublisherQueue()
			p := newQueuePublisher(t, cli, q, WithDeadLetter(dl.record))
			enqueueAll(t, p, 1, "qos1")
			waitQueueLen(t, q, 0)
			if got, errs := dl.get(); !slices.Equal(got, []string{"qos1"}) || !errors.Is(errs[0], ErrQoSNotSupported) {
				t.Fatalf("dead letters %q %v", got, errs)
			}
			select {
			case pkt := <-published:
				t.Fatalf("sent a QoS %d PUBLISH that no acknowledgement could settle", pkt.QoS)
			case <-time.After(400 * time.Millisecond):
			}
			if n := cli.engine.OutboundLen(); n != 0 {
				t.Fatalf("%d outbound flows left", n)
			}
		})
	}
}

// The idempotency key carries the entry's ID; the queued copy is the
// publisher's own.
func TestQueuePublisherIdempotencyKeyAndOwnership(t *testing.T) {
	qb := newQueueBroker()
	qb.gate = make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, tbClient(t, b), q, WithQueueIdempotencyKey())
	payload := []byte("orig")
	if err := p.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	got := qb.next(t)
	close(qb.gate)
	if string(got.Payload) != "orig" {
		t.Fatalf("broker received %q after the caller reused its buffer", got.Payload)
	}
	var key string
	for k, v := range got.Properties().UserProperties() {
		if k == QueueIDProperty {
			key = v
		}
	}
	if b, err := hex.DecodeString(key); err != nil || len(b) != 16 {
		t.Fatalf("%s = %q, want 32 hex digits", QueueIDProperty, key)
	}
}

// BenchmarkQueuePublisherRTT drains a backlog through a broker that
// answers each PUBLISH one round trip after receiving it, and reports
// messages per second. With a window of W the drain approaches W/RTT.
func BenchmarkQueuePublisherRTT(b *testing.B) {
	for _, rtt := range []time.Duration{25 * time.Millisecond, 300 * time.Millisecond} {
		for _, window := range []int{1, 16, 32} {
			b.Run(fmt.Sprintf("rtt=%v/window=%d", rtt, window), func(b *testing.B) {
				broker := testbroker.New(b, func(c *testbroker.Conn) {
					c.AcceptConnect(wire.ConnackOpts{})
					for {
						p, ok, err := c.Next(time.Minute)
						if !ok {
							if errors.Is(err, testbroker.ErrTimeout) {
								continue
							}
							return
						}
						if p.Type == wire.PUBLISH {
							time.AfterFunc(rtt, func() { c.Puback(p.PacketID, wire.ReasonSuccess) })
						}
					}
				})
				cli, err := New(WithBroker(broker.URL()), WithClientID("rtt"), WithLogger(quietLogger()))
				if err != nil {
					b.Fatal(err)
				}
				if err := cli.Connect(context.Background()); err != nil {
					b.Fatal(err)
				}
				defer cli.Disconnect(context.Background())
				q := NewMemoryPublisherQueue()
				p, err := NewQueuePublisher(cli, q, WithQueueWindow(window))
				if err != nil {
					b.Fatal(err)
				}
				defer p.Close(context.Background())
				n := max(b.N, 2*window)
				b.ResetTimer()
				start := time.Now()
				for i := range n {
					if err := p.Publish(context.Background(), PublishOptions{Topic: "rtt", QoS: 1, Payload: []byte{byte(i)}}); err != nil {
						b.Fatal(err)
					}
				}
				for {
					if l, _ := q.Len(context.Background()); l == 0 {
						break
					}
					time.Sleep(time.Millisecond)
				}
				b.ReportMetric(float64(n)/time.Since(start).Seconds(), "msg/s")
			})
		}
	}
}

// A bounded queue whose head stays unacknowledged keeps memory for what
// it holds, not for every message acknowledged behind the head.
func TestMemoryQueueReclaimsAckedEntriesBehindTheHead(t *testing.T) {
	q := NewMemoryPublisherQueue()
	ctx := context.Background()
	limit := QueueLimit{Max: 2}
	if _, _, err := q.Enqueue(ctx, QueueEntry{ID: "head", Publish: PublishOptions{Topic: "q", QoS: 1}}, limit); err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	payload := make([]byte, 64<<10)
	for range 400 {
		seq, _, err := q.Enqueue(ctx, QueueEntry{ID: "tail", Publish: PublishOptions{Topic: "q", QoS: 1, Payload: payload}}, limit)
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
		if err := q.Ack(ctx, seq); err != nil {
			t.Fatal(err)
		}
	}
	q.mu.Lock()
	kept, referenced := len(q.entries)-q.head, 0
	for _, e := range q.entries[q.head:] {
		referenced += len(e.e.Publish.Payload)
	}
	q.mu.Unlock()
	if kept > 70 || referenced != 0 {
		t.Fatalf("after acking 400 entries behind the head the queue keeps %d slots referencing %d payload bytes", kept, referenced)
	}
	// Order and lookups survive the reclaiming.
	if _, _, err := q.Enqueue(ctx, QueueEntry{ID: "last", Publish: PublishOptions{Topic: "q", QoS: 1}}, limit); err != nil {
		t.Fatal(err)
	}
	got, err := q.Peek(ctx, 0, 10)
	if err != nil || len(got) != 2 || got[0].ID != "head" || got[1].ID != "last" {
		t.Fatalf("Peek after reclaiming: %+v, %v", got, err)
	}
	if err := q.Ack(ctx, seqs[0]); err != nil {
		t.Fatal(err)
	}
	if n, _ := q.Len(ctx); n != 2 {
		t.Fatalf("Len %d, want 2", n)
	}
}
