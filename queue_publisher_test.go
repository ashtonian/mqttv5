package mqttv5

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/inflight"
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

// A window larger than the packet identifiers left free still drains:
// the drain waits for an identifier while the outcomes that free one are
// recorded apart from it.
func TestQueuePublisherWindowBeyondFreeIdentifiers(t *testing.T) {
	qb := newQueueBroker()
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	cli := tbClient(t, b)
	const free = 2
	for range 65535 - free {
		if _, err := cli.engine.AllocateID(t.Context(), inflight.OwnerSubscribe, nil); err != nil {
			t.Fatal(err)
		}
	}
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, cli, q, WithQueueWindow(8))
	var payloads []string
	for i := range 32 {
		payloads = append(payloads, strconv.Itoa(i))
	}
	enqueueAll(t, p, 1, payloads...)
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, payloads) {
		t.Fatalf("broker received %q, want %q", got, payloads)
	}
}

// heldEnqueue holds the Enqueue of the entry whose payload is hold,
// before it stores it, until release is closed.
type heldEnqueue struct {
	*MemoryPublisherQueue
	hold          string
	held, release chan struct{}
}

func (q *heldEnqueue) Enqueue(ctx context.Context, e QueueEntry, limit QueueLimit) (uint64, []QueueEntry, error) {
	if string(e.Publish.Payload) == q.hold {
		close(q.held)
		<-q.release
	}
	return q.MemoryPublisherQueue.Enqueue(ctx, e, limit)
}

// A queue that cannot evict does not make the drain wait for a Publish
// whose Enqueue is still writing: the messages already stored go out
// meanwhile.
func TestQueuePublisherDrainsDuringEnqueue(t *testing.T) {
	qb := newQueueBroker()
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	cli := tbClient(t, b)
	q := &heldEnqueue{MemoryPublisherQueue: NewMemoryPublisherQueue(), hold: "held", held: make(chan struct{}), release: make(chan struct{})}
	p := newQueuePublisher(t, cli, q)
	release := sync.OnceFunc(func() { close(q.release) })
	t.Cleanup(release)
	published := make(chan error, 1)
	go func() {
		published <- p.Publish(context.Background(), PublishOptions{Topic: "q/t", QoS: 1, Payload: []byte("held")})
	}()
	<-q.held
	enqueueAll(t, p, 1, "next")
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, []string{"next"}) {
		t.Fatalf("broker received %q while an Enqueue was held, want [next]", got)
	}
	release()
	if err := <-published; err != nil {
		t.Fatalf("Publish(held): %v", err)
	}
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, []string{"next", "held"}) {
		t.Fatalf("broker received %q, want [next held]", got)
	}
}

// ackLog holds the first Ack until release is closed and records the
// seqs of every Ack.
type ackLog struct {
	*MemoryPublisherQueue
	held, release chan struct{}
	once          sync.Once
	mu            sync.Mutex
	acks          [][]uint64
}

func (q *ackLog) Ack(ctx context.Context, seqs ...uint64) error {
	q.mu.Lock()
	q.acks = append(q.acks, slices.Clone(seqs))
	q.mu.Unlock()
	q.once.Do(func() {
		close(q.held)
		<-q.release
	})
	return q.MemoryPublisherQueue.Ack(ctx, seqs...)
}

func (q *ackLog) get() [][]uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return slices.Clone(q.acks)
}

// The messages whose outcomes came in while the queue was busy leave it
// together, in one Ack.
func TestQueuePublisherRemovesFinishedMessagesTogether(t *testing.T) {
	qb := newQueueBroker()
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	q := &ackLog{MemoryPublisherQueue: NewMemoryPublisherQueue(), held: make(chan struct{}), release: make(chan struct{})}
	const n = 16
	p := newQueuePublisher(t, tbClient(t, b), q, WithQueueWindow(n))
	var payloads []string
	for i := range n {
		payloads = append(payloads, strconv.Itoa(i))
	}
	// The first message's Ack is held while the others are published, so
	// their outcomes arrive while the queue is busy.
	enqueueAll(t, p, 1, payloads[0])
	<-q.held
	enqueueAll(t, p, 1, payloads[1:]...)
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(time.Millisecond) {
		p.mu.Lock()
		waiting := len(p.outcomes)
		p.mu.Unlock()
		if waiting == n-1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d outcomes waiting behind the first Ack, want %d", waiting, n-1)
		}
	}
	close(q.release)
	waitQueueLen(t, q, 0)
	acks := q.get()
	if len(acks) != 2 || !slices.Equal(acks[0], []uint64{1}) || len(acks[1]) != n-1 {
		t.Fatalf("Acks %v; want [1] and then the %d outcomes that waited, in one", acks, n-1)
	}
	slices.Sort(acks[1])
	for i, seq := range acks[1] {
		if seq != uint64(i+2) {
			t.Fatalf("second Ack %v; want seqs 2 to %d", acks[1], n)
		}
	}
}

// keptQueue survives Close, as a durable queue's contents do.
// failingPeek fails the first Peek that would return entries.
type failingPeek struct {
	*MemoryPublisherQueue
	peeks  atomic.Int32
	once   sync.Once
	failed chan struct{}
}

func (q *failingPeek) Peek(ctx context.Context, after uint64, limit PeekLimit) ([]QueueEntry, error) {
	q.peeks.Add(1)
	entries, err := q.MemoryPublisherQueue.Peek(ctx, after, limit)
	if err != nil || len(entries) == 0 {
		return entries, err
	}
	fail := false
	q.once.Do(func() {
		fail = true
		close(q.failed)
	})
	if fail {
		return nil, errors.New("queue unavailable")
	}
	return entries, nil
}

// A failed queue read is retried after the retry backoff, though no
// exchange is running whose outcome would wake the drain.
func TestQueuePublisherRetriesFailedQueueRead(t *testing.T) {
	qb := newQueueBroker()
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	clk := clock.NewFake(time.Unix(1_000, 0))
	cli := tbClient(t, b, withClock(clk))
	q := &failingPeek{MemoryPublisherQueue: NewMemoryPublisherQueue(), failed: make(chan struct{})}
	p := newQueuePublisher(t, cli, q, WithQueueRetryBackoff(ConstantBackoff(time.Second)))
	// While connected and idle, the keep-alive is the clock's one timer.
	if !clk.WaitPending(1, 3*time.Second) {
		t.Fatal("keep-alive timer not armed")
	}
	// The drain has read the empty queue once, so the read that fails is
	// the one Publish wakes it for, and nothing wakes it after.
	for deadline := time.Now().Add(3 * time.Second); q.peeks.Load() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the drain never read the queue")
		}
	}
	enqueueAll(t, p, 1, "m")
	select {
	case <-q.failed:
	case <-time.After(3 * time.Second):
		t.Fatal("the drain never read the published message")
	}
	if !clk.WaitPending(2, 3*time.Second) {
		t.Fatal("no retry scheduled after the failed read")
	}
	clk.Advance(time.Second)
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, []string{"m"}) {
		t.Fatalf("broker received %q, want [m]", got)
	}
}

// A queued message whose record cannot be stored is not sent; it is
// tried again, as a new exchange, after the retry backoff.
func TestQueuePublisherRetriesAnUnstorableRecord(t *testing.T) {
	qb := newQueueBroker()
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	st := &phaseFailStore{MemoryStore: session.NewMemoryStore(), phase: session.AwaitPuback}
	st.fail.Store(true)
	clk := clock.NewFake(time.Unix(1_000, 0))
	cli := tbClient(t, b, WithStore(st), withClock(clk))
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, cli, q, WithQueueRetryBackoff(ConstantBackoff(time.Second)))
	// While connected and idle, the keep-alive is the clock's one timer.
	if !clk.WaitPending(1, 3*time.Second) {
		t.Fatal("keep-alive timer not armed")
	}
	enqueueAll(t, p, 1, "m")
	if !clk.WaitPending(2, 3*time.Second) {
		t.Fatal("no retry scheduled after the failed write")
	}
	if got := qb.payloads(); len(got) != 0 {
		t.Fatalf("broker received %q before the record was stored", got)
	}
	st.fail.Store(false)
	clk.Advance(time.Second)
	waitQueueLen(t, q, 0)
	waitEmpty(t, st)
	if got := qb.payloads(); !slices.Equal(got, []string{"m"}) {
		t.Fatalf("broker received %q, want [m]", got)
	}
	if !cli.Connected() {
		t.Fatal("the failed write disconnected the client")
	}
}

// Each message waiting for a retry starts when its own retry is due, not
// when the latest one waiting is.
func TestQueuePublisherRetriesEachMessageWhenDue(t *testing.T) {
	qb := newQueueBroker()
	refused := make(map[string]bool)
	qb.answer = func(p testbroker.Packet) wire.ReasonCode {
		if !refused[string(p.Payload)] {
			refused[string(p.Payload)] = true
			return wire.ReasonQuotaExceeded
		}
		return wire.ReasonSuccess
	}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	clk := clock.NewFake(time.Unix(1_000, 0))
	cli := tbClient(t, b, withClock(clk))
	q := NewMemoryPublisherQueue()
	// The first refusal recorded waits 20 s, the second 10 s.
	var decided atomic.Int32
	backoff := BackoffFunc(func(int) time.Duration {
		if decided.Add(1) == 1 {
			return 20 * time.Second
		}
		return 10 * time.Second
	})
	p := newQueuePublisher(t, cli, q, WithQueueRetryBackoff(backoff))
	enqueueAll(t, p, 2, "a", "b")
	for deadline := time.Now().Add(3 * time.Second); decided.Load() < 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("both refusals were not recorded")
		}
	}
	clk.Advance(10 * time.Second)
	waitQueueLen(t, q, 1)
	if got := qb.payloads(); len(got) != 3 {
		t.Fatalf("broker received %q, want each message once and the one due again", got)
	}
	clk.Advance(10 * time.Second)
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); len(got) != 4 {
		t.Fatalf("broker received %q, want each message twice", got)
	}
}

type keptQueue struct{ *MemoryPublisherQueue }

func (keptQueue) Close() error { return nil }

// An exchange that finishes after Close gave up waiting for it keeps its
// outcome in the session; the next QueuePublisher on the queue records
// it without publishing the message again.
func TestQueuePublisherKeepsOutcomeAfterClose(t *testing.T) {
	qb := newQueueBroker()
	qb.gate = make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	st := session.NewMemoryStore()
	cli := tbClient(t, b, WithStore(st), WithStats())
	q := keptQueue{NewMemoryPublisherQueue()}
	p1, err := NewQueuePublisher(cli, q)
	if err != nil {
		t.Fatal(err)
	}
	enqueueAll(t, p1, 1, "late")
	qb.next(t)
	gaveUp, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p1.Close(gaveUp); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close with the exchange unanswered = %v", err)
	}

	close(qb.gate)
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(time.Millisecond) {
		_, recs, err := st.Load(context.Background())
		if err == nil && len(recs) == 1 && recs[0].Phase == session.Completed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session records %+v (%v), want the finished exchange kept", recs, err)
		}
	}

	newQueuePublisher(t, cli, q)
	waitQueueLen(t, q, 0)
	waitEmpty(t, st)
	if got := qb.payloads(); len(got) != 1 {
		t.Fatalf("broker received %q, want the message once", got)
	}
	if sent := cli.Stats().PublishesSent; sent != 1 {
		t.Fatalf("PublishesSent = %d, want 1", sent)
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

func TestQueueDefaultWindow(t *testing.T) {
	tests := []struct {
		name string
		cfg  queueConfig
		want int
	}{
		{"unbounded", queueConfig{}, DefaultQueueWindow},
		{"bounded, DropNewest", queueConfig{maxSize: 10, dropPolicy: DropNewest}, DefaultQueueWindow},
		{"DropOldest, large bound", queueConfig{maxSize: 1_000_000, dropPolicy: DropOldest}, DefaultQueueWindow},
		{"DropOldest, small bound", queueConfig{maxSize: 100, dropPolicy: DropOldest}, 50},
		{"DropOldest, bound of one", queueConfig{maxSize: 1, dropPolicy: DropOldest}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.defaultWindow(); got != tt.want {
				t.Fatalf("default window %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNewQueuePublisherRejectsInvalidOptions(t *testing.T) {
	cli, err := New(WithBroker("mqtt://127.0.0.1:1883"), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		opt  QueueOption
	}{
		{"window of 0", WithQueueWindow(0)},
		{"negative window bytes", WithQueueWindowBytes(-1)},
		{"negative max size", WithQueueMaxSize(-1)},
		{"negative TTL", WithQueueTTL(-time.Second)},
		{"unknown drop policy", WithQueueDropPolicy(DropPolicy(9))},
		{"nil classifier", WithQueueClassifier(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if p, err := NewQueuePublisher(cli, NewMemoryPublisherQueue(), tt.opt); err == nil {
				_ = p.Close(context.Background())
				t.Fatal("NewQueuePublisher accepted it")
			}
		})
	}
}

func TestQueuePublisherDefaults(t *testing.T) {
	cli, err := New(WithBroker("mqtt://127.0.0.1:1883"), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewQueuePublisher(cli, NewMemoryPublisherQueue())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(context.Background())
	if p.cfg.window != DefaultQueueWindow || p.cfg.windowBytes != DefaultQueueWindowBytes {
		t.Fatalf("window %d messages, %d bytes; want %d and %d", p.cfg.window, p.cfg.windowBytes, DefaultQueueWindow, DefaultQueueWindowBytes)
	}
}

func TestQueueEntrySize(t *testing.T) {
	tests := []struct {
		name string
		e    QueueEntry
		want int
	}{
		{"empty", QueueEntry{}, 0},
		{"ID, topic and payload", QueueEntry{ID: "id", Publish: PublishOptions{Topic: "a/b", Payload: make([]byte, 10)}}, 15},
		{"properties", QueueEntry{Publish: PublishOptions{
			ContentType: "json", ResponseTopic: "r", CorrelationData: []byte{1, 2},
			UserProperties: []UserProperty{{Key: "k", Value: "vv"}, {Key: "kk", Value: ""}},
		}}, 12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Size(); got != tt.want {
				t.Fatalf("Size() = %d, want %d", got, tt.want)
			}
		})
	}
}

// enqueueSized stores messages of one size directly in q, before a
// QueuePublisher starts on it, so its first claim takes as many as its
// window allows at once. It returns their Size.
func enqueueSized(t *testing.T, q PublisherQueue, payloads ...string) int {
	t.Helper()
	size := 0
	for _, s := range payloads {
		e := QueueEntry{ID: "id-" + s, Publish: PublishOptions{Topic: "q/t", QoS: 1, Payload: []byte(s)}}
		if _, _, err := q.Enqueue(context.Background(), e, QueueLimit{}); err != nil {
			t.Fatal(err)
		}
		size = e.Size()
	}
	return size
}

// The drain holds no more messages than WithQueueWindowBytes allows, and
// takes more as they leave.
func TestQueuePublisherWindowBytes(t *testing.T) {
	qb := newQueueBroker()
	qb.gate = make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	q := NewMemoryPublisherQueue()
	size := enqueueSized(t, q, "0", "1", "2", "3", "4", "5")
	p := newQueuePublisher(t, tbClient(t, b), q, WithQueueWindowBytes(3*size))
	for range 3 {
		qb.next(t)
	}
	p.mu.Lock()
	held, bytes := len(p.pending), p.pendingBytes
	p.mu.Unlock()
	if held != 3 || bytes != 3*size {
		t.Fatalf("drain holds %d messages of %d bytes, want 3 of %d", held, bytes, 3*size)
	}
	close(qb.gate)
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, []string{"0", "1", "2", "3", "4", "5"}) {
		t.Fatalf("broker received %q", got)
	}
}

// A message larger than the byte bound is published on its own.
func TestQueuePublisherMessageLargerThanWindowBytes(t *testing.T) {
	qb := newQueueBroker()
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	q := NewMemoryPublisherQueue()
	enqueueSized(t, q, "0", "1", "2")
	newQueuePublisher(t, tbClient(t, b), q, WithQueueWindowBytes(1))
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, []string{"0", "1", "2"}) {
		t.Fatalf("broker received %q", got)
	}
}

// Under DropOldest the default window leaves part of a small queue
// outside it, so a full queue still evicts rather than refusing.
func TestQueuePublisherDefaultWindowLeavesRoomToEvict(t *testing.T) {
	qb := newQueueBroker()
	qb.gate = make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		qb.serve(c)
	})
	q := NewMemoryPublisherQueue()
	enqueueSized(t, q, "0", "1", "2", "3")
	var dl deadLetters
	p := newQueuePublisher(t, tbClient(t, b), q, WithQueueMaxSize(4),
		WithQueueDropPolicy(DropOldest), WithDeadLetter(dl.record))
	qb.next(t)
	qb.next(t)
	enqueueAll(t, p, 1, "4", "5")
	if got, errs := dl.get(); !slices.Equal(got, []string{"2", "3"}) || !errors.Is(errs[0], ErrQueueFull) {
		t.Fatalf("evicted %q %v, want 2 and 3 with ErrQueueFull", got, errs)
	}
	close(qb.gate)
	waitQueueLen(t, q, 0)
	if got := qb.payloads(); !slices.Equal(got, []string{"0", "1", "4", "5"}) {
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
		for _, window := range []int{1, 16, 32, 256, DefaultQueueWindow} {
			b.Run(fmt.Sprintf("rtt=%v/window=%d", rtt, window), func(b *testing.B) {
				broker := rttBroker(b, rtt)
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

// rttBroker acknowledges each QoS 1 PUBLISH one round trip after
// receiving it.
func rttBroker(b *testing.B, rtt time.Duration) *testbroker.Broker {
	return testbroker.New(b, func(c *testbroker.Conn) {
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
	got, err := q.Peek(ctx, 0, PeekLimit{Entries: 10})
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
