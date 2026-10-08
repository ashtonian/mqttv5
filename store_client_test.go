// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

func storedPublish(t *testing.T, st session.Store, id uint16, seq uint64, topic string) {
	t.Helper()
	pkt, err := wire.MarshalPublish(wire.PublishOpts{Topic: topic, Payload: []byte("stored"), QoS: 1, PacketID: id})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), session.Record{
		Key: session.RecordKey{Dir: session.Outbound, PacketID: id}, Seq: seq, QoS: 1, Phase: session.AwaitPuback, Packet: pkt,
	}); err != nil {
		t.Fatal(err)
	}
}

func waitEmpty(t *testing.T, st session.Store) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if _, recs, _ := st.Load(context.Background()); len(recs) == 0 {
			return
		}
	}
	_, recs, _ := st.Load(context.Background())
	t.Fatalf("store still holds %+v", recs)
}

// A client started on a store with session state resumes
// it (CleanStart=0) and resends the stored publishes.
func TestRestartResumesFromStore(t *testing.T) {
	st := session.NewMemoryStore()
	storedPublish(t, st, 55, 1, "stored/a")
	storedPublish(t, st, 12, 2, "stored/b")
	b := testbroker.New(t, func(c *testbroker.Conn) {
		ci := c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
		if ci.CleanStart {
			c.T.Error("CleanStart=1 although the store holds a session")
		}
		for _, want := range []uint16{55, 12} {
			p := c.Expect(wire.PUBLISH, 0)
			if p.PacketID != want || !p.Dup || p.Topic == "" {
				c.T.Errorf("resent PUBLISH id=%d dup=%v topic=%q, want id %d dup", p.PacketID, p.Dup, p.Topic, want)
			}
			c.Puback(p.PacketID, wire.ReasonSuccess)
		}
		c.ServeAuto()
	})
	tbClient(t, b, WithStore(st))
	waitEmpty(t, st)
}

// recordingStore notes whether a record existed when the broker saw the
// matching PUBLISH.
type recordingStore struct {
	*session.MemoryStore
	mu   sync.Mutex
	puts []session.RecordKey
}

func (r *recordingStore) Put(ctx context.Context, rec session.Record) error {
	r.mu.Lock()
	r.puts = append(r.puts, rec.Key)
	r.mu.Unlock()
	return r.MemoryStore.Put(ctx, rec)
}

func (r *recordingStore) stored(k session.RecordKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.puts {
		if p == k {
			return true
		}
	}
	return false
}

// A QoS 1/2 publish is written to the store before it is sent.
func TestPublishIsStoredBeforeItIsSent(t *testing.T) {
	st := &recordingStore{MemoryStore: session.NewMemoryStore()}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		for i := 0; i < 2; i++ {
			p := c.Expect(wire.PUBLISH, 0)
			if !st.stored(session.RecordKey{Dir: session.Outbound, PacketID: p.PacketID}) {
				c.T.Errorf("PUBLISH id=%d reached the broker before its record was stored", p.PacketID)
			}
			if p.QoS == 1 {
				c.Puback(p.PacketID, wire.ReasonSuccess)
			} else {
				c.Pubrec(p.PacketID, wire.ReasonSuccess)
				c.Expect(wire.PUBREL, 0)
				c.Pubcomp(p.PacketID, wire.ReasonSuccess)
			}
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithStore(st))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, qos := range []byte{1, 2} {
		if err := cli.Publish(ctx, PublishOptions{Topic: "s", QoS: qos}); err != nil {
			t.Fatal(err)
		}
	}
	waitEmpty(t, st)
}

// A broker-assigned ClientID kept in the store is used by
// the next process.
func TestAssignedClientIDRestoredFromStore(t *testing.T) {
	st := session.NewMemoryStore()
	if err := st.SetMeta(context.Background(), session.Meta{ClientID: "assigned-x", SessionExpiry: 60}); err != nil {
		t.Fatal(err)
	}
	ids := make(chan string, 1)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		ids <- c.AcceptConnect(wire.ConnackOpts{}).ClientID
		c.ServeAuto()
	})
	cli, err := New(WithBroker(b.URL()), WithClientID(""), WithStore(st), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(ctx)
	if got := <-ids; got != "assigned-x" {
		t.Fatalf("CONNECT ClientID = %q", got)
	}
}

// An explicit WithCleanStart(true) starts a new session and drops
// the stored one instead of resending it.
func TestExplicitCleanStartDiscardsStore(t *testing.T) {
	st := session.NewMemoryStore()
	storedPublish(t, st, 7, 1, "old")
	b := testbroker.New(t, func(c *testbroker.Conn) {
		if ci := c.AcceptConnect(wire.ConnackOpts{}); !ci.CleanStart {
			c.T.Error("CleanStart=0 despite WithCleanStart(true)")
		}
		c.ExpectNone(wire.PUBLISH, 300*time.Millisecond)
		c.ServeAuto()
	})
	tbClient(t, b, WithStore(st), WithCleanStart(true))
	waitLog(t, b.Conn(0, time.Second), wire.CONNECT, 1)
	waitEmpty(t, st)
}

// phaseFailStore fails writes of one outbound phase while fail is set.
type phaseFailStore struct {
	*session.MemoryStore
	phase session.Phase
	fail  atomic.Bool
}

func (s *phaseFailStore) Put(ctx context.Context, r session.Record) error {
	if s.fail.Load() && r.Phase == s.phase {
		return errors.New("disk full")
	}
	return s.MemoryStore.Put(ctx, r)
}

// A store write that fails after a message was accepted stops the
// client: no PUBREL goes out without its stored phase, the broker gets
// DISCONNECT 0x80, the waiting Publish returns the failure, and the next
// Connect continues the exchange from what the store holds.
func TestStoreFailureStopsTheClientAndConnectReloads(t *testing.T) {
	st := &phaseFailStore{MemoryStore: session.NewMemoryStore(), phase: session.AwaitPubcomp}
	st.fail.Store(true)
	disconnect := make(chan wire.ReasonCode, 1)
	resumed := make(chan testbroker.Packet, 1)
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			p := c.Expect(wire.PUBLISH, 0)
			c.Pubrec(p.PacketID, wire.ReasonSuccess)
			for {
				q, ok, _ := c.Next(3 * time.Second)
				if !ok {
					c.T.Error("connection closed without DISCONNECT")
					return
				}
				switch q.Type {
				case wire.PUBREL:
					c.T.Error("PUBREL sent although its phase was never stored")
				case wire.DISCONNECT:
					disconnect <- q.Reason
					return
				}
			}
		},
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			p := c.Expect(wire.PUBLISH, 0)
			resumed <- p
			c.Pubrec(p.PacketID, wire.ReasonSuccess)
			c.Expect(wire.PUBREL, 0)
			c.Pubcomp(p.PacketID, wire.ReasonSuccess)
			c.ServeAuto()
		})
	failures := make(chan error, 2)
	cli := tbClient(t, b, WithStore(st), WithOnStoreFailure(func(err error) { failures <- err }))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := cli.Publish(ctx, PublishOptions{Topic: "s", QoS: 2, Payload: []byte("exactly once")})
	var se *StoreError
	if !errors.Is(err, ErrStoreFailed) || !errors.As(err, &se) || se.Op != "put outbound" {
		t.Fatalf("Publish returned %v, want the store failure", err)
	}
	select {
	case rc := <-disconnect:
		if rc != wire.ReasonUnspecifiedError {
			t.Fatalf("DISCONNECT reason %#x, want 0x80", byte(rc))
		}
	case <-ctx.Done():
		t.Fatal("no DISCONNECT")
	}
	select {
	case err := <-failures:
		if !errors.Is(err, ErrStoreFailed) {
			t.Fatalf("OnStoreFailure got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("OnStoreFailure did not fire")
	}
	if cli.Connected() {
		t.Fatal("client still connected after the store failed")
	}
	if err := cli.Publish(ctx, PublishOptions{Topic: "s", QoS: 1}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Publish after the stop: %v", err)
	}

	st.fail.Store(false)
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-resumed:
		if !p.Dup || string(p.Payload) != "exactly once" {
			t.Fatalf("resumed with %+v, want the stored PUBLISH resent with DUP", p)
		}
	case <-ctx.Done():
		t.Fatal("the stored exchange was not continued")
	}
	waitEmpty(t, st)
	select {
	case err := <-failures:
		t.Fatalf("OnStoreFailure fired again: %v", err)
	default:
	}
}

// failingAckQueue fails Ack until healed.
type failingAckQueue struct {
	*MemoryPublisherQueue
	healed atomic.Bool
	acks   atomic.Int32
}

func (q *failingAckQueue) Ack(ctx context.Context, seq uint64) error {
	q.acks.Add(1)
	if !q.healed.Load() {
		return errors.New("queue unavailable")
	}
	return q.MemoryPublisherQueue.Ack(ctx, seq)
}

// A message the broker accepted but the queue could not remove keeps
// its session record, so it is never published again; the removal is
// retried until it succeeds.
func TestQueueAckFailureKeepsTheExchange(t *testing.T) {
	qb := newQueueBroker()
	b := testbroker.New(t, func(c *testbroker.Conn) { c.AcceptConnect(wire.ConnackOpts{}); qb.serve(c) })
	st := session.NewMemoryStore()
	cli := tbClient(t, b, WithStore(st))
	q := &failingAckQueue{MemoryPublisherQueue: NewMemoryPublisherQueue()}
	p := newQueuePublisher(t, cli, q, WithQueueRetryBackoff(ConstantBackoff(10*time.Millisecond)))
	enqueueAll(t, p, 1, "once")
	qb.next(t)
	for deadline := time.Now().Add(3 * time.Second); q.acks.Load() < 3; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("removal attempted %d times, want retries", q.acks.Load())
		}
	}
	if _, recs, _ := st.Load(context.Background()); len(recs) != 1 {
		t.Fatalf("session records while the removal fails: %+v", recs)
	}
	q.healed.Store(true)
	waitQueueLen(t, q, 0)
	waitEmpty(t, st)
	if got := qb.payloads(); len(got) != 1 {
		t.Fatalf("broker received %v, want the message once", got)
	}
}
