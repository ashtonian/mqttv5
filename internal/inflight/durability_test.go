// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

// faultStore fails or holds chosen writes.
type faultStore struct {
	*session.MemoryStore
	// failPut and failDelete fail writes whose record matches.
	failPut    func(session.Record) bool
	failDelete atomic.Bool
	failMeta   atomic.Bool
	// hold, when set, blocks Puts it matches until release is closed;
	// held is closed when the first one starts.
	hold     func(session.Record) bool
	held     chan struct{}
	heldOnce sync.Once
	release  chan struct{}
}

var errDiskFull = errors.New("disk full")

func newFaultStore() *faultStore {
	return &faultStore{MemoryStore: session.NewMemoryStore(), held: make(chan struct{}), release: make(chan struct{})}
}

func (s *faultStore) Put(ctx context.Context, r session.Record) error {
	if s.hold != nil && s.hold(r) {
		s.heldOnce.Do(func() { close(s.held) })
		<-s.release
	}
	if s.failPut != nil && s.failPut(r) {
		return errDiskFull
	}
	return s.MemoryStore.Put(ctx, r)
}

func (s *faultStore) Delete(ctx context.Context, k session.RecordKey) error {
	if s.failDelete.Load() {
		return errDiskFull
	}
	return s.MemoryStore.Delete(ctx, k)
}

func (s *faultStore) SetMeta(ctx context.Context, m session.Meta) error {
	if s.failMeta.Load() {
		return errDiskFull
	}
	return s.MemoryStore.SetMeta(ctx, m)
}

func phaseIs(p session.Phase) func(session.Record) bool {
	return func(r session.Record) bool { return r.Phase == p }
}

// failures records OnFailure calls.
type failures struct {
	mu   sync.Mutex
	errs []error
}

func (f *failures) add(err error) {
	f.mu.Lock()
	f.errs = append(f.errs, err)
	f.mu.Unlock()
}

func (f *failures) list() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]error(nil), f.errs...)
}

func (h *harness) drain() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.e.Drain(ctx); err != nil {
		h.t.Fatal(err)
	}
}

func records(t *testing.T, st session.Store) []session.Record {
	t.Helper()
	_, recs, err := st.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

// A PUBREL never goes out unless the AwaitPubcomp phase it depends on is
// stored: when that write fails the session ends instead, and the next
// process (or Restore) continues from the phase the store holds.
func TestFailedPhaseWriteEndsTheSession(t *testing.T) {
	st := newFaultStore()
	var failed failures
	h := newHarness(t, Config{Store: st, OnFailure: failed.add})
	h.connect(false, 10)
	o := h.publish(2, "durable")
	waiting := h.publish(1, "waiting")
	h.collect()

	st.failPut = phaseIs(session.AwaitPubcomp)
	h.e.HandlePubrec(o.PacketID(), 0, nil)
	h.drain()
	want(t, h.collect())

	errs := failed.list()
	if len(errs) != 1 || !errors.Is(errs[0], ErrStoreFailed) || !errors.Is(errs[0], errDiskFull) {
		t.Fatalf("OnFailure calls: %v", errs)
	}
	for _, f := range []*Out{o, waiting} {
		if err := done(t, f); !errors.Is(err, ErrStoreFailed) {
			t.Fatalf("flow %d completed with %v, want the store failure", f.PacketID(), err)
		}
	}
	if _, _, err := h.e.Register(context.Background(), Message{ID: 999, QoS: 1, Packet: []byte{0x32, 0}}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Register after the failure: %v", err)
	}
	if _, _, err := h.e.Connected(ConnInfo{Link: &fakeLink{}}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Connected after the failure: %v", err)
	}

	// Restore reloads what is durable: the QoS 2 flow at AwaitPubrec, so
	// the resumed session resends its PUBLISH, never a PUBREL.
	st.failPut = nil
	if err := h.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.connect(true, 10)
	want(t, h.collect(), fmt.Sprintf("PUBLISH#%d+dup", o.PacketID()), fmt.Sprintf("PUBLISH#%d+dup", waiting.PacketID()))
}

// A failed write of anything but a new flow's record ends the session:
// deleting a finished flow's record, storing an inbound QoS 2 phase, and
// the session meta.
func TestEveryFailedWriteEndsTheSession(t *testing.T) {
	cases := []struct {
		name  string
		setup func(h *harness, st *faultStore)
	}{
		{"delete outbound", func(h *harness, st *faultStore) {
			o := h.publish(1, "a")
			h.collect()
			st.failDelete.Store(true)
			h.e.HandlePuback(o.PacketID(), 0, nil)
		}},
		{"put inbound", func(h *harness, st *faultStore) {
			st.failPut = phaseIs(session.AwaitPubrel)
			in, _, _ := h.e.Receive(7, 2)
			h.e.Ack(in)
		}},
		{"delete inbound", func(h *harness, st *faultStore) {
			in, _, _ := h.e.Receive(7, 2)
			h.e.Ack(in)
			h.drain()
			h.collect()
			st.failDelete.Store(true)
			h.e.HandlePubrel(7)
		}},
		{"set meta", func(h *harness, st *faultStore) {
			st.failMeta.Store(true)
			if err := h.e.SetMeta(context.Background(), session.Meta{ClientID: "assigned"}); !errors.Is(err, ErrStoreFailed) {
				h.t.Fatalf("SetMeta: %v", err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newFaultStore()
			var failed failures
			h := newHarness(t, Config{Store: st, OnFailure: failed.add})
			h.connect(false, 10)
			tc.setup(h, st)
			h.drain()
			if errs := failed.list(); len(errs) != 1 || !errors.Is(errs[0], ErrStoreFailed) {
				t.Fatalf("OnFailure calls: %v", errs)
			}
			want(t, h.collect())
		})
	}
}

// SetMeta keeps the old value until the write succeeds, so the same
// value is written again on the next attempt.
func TestSetMetaRetriesAfterAFailure(t *testing.T) {
	st := newFaultStore()
	h := newHarness(t, Config{Store: st, OnFailure: func(error) {}})
	st.failMeta.Store(true)
	m := session.Meta{ClientID: "assigned", SessionExpiry: 60}
	if err := h.e.SetMeta(context.Background(), m); err == nil {
		t.Fatal("SetMeta succeeded although the store failed")
	}
	st.failMeta.Store(false)
	if err := h.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.e.SetMeta(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := st.Load(context.Background()); got != m {
		t.Fatalf("stored meta %+v, want %+v", got, m)
	}
}

// A resumed session regenerates the PUBRELs it owes, and each still
// waits for the write of its flow's AwaitPubcomp phase.
func TestResumeWaitsForPendingPhaseWrite(t *testing.T) {
	st := newFaultStore()
	st.hold = phaseIs(session.AwaitPubcomp)
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	o := h.publish(2, "durable")
	h.collect()
	h.e.HandlePubrec(o.PacketID(), 0, nil)
	<-st.held
	h.disconnect()
	h.connect(true, 10)
	want(t, h.collect())
	close(st.release)
	h.drain()
	want(t, h.collect(), fmt.Sprintf("PUBREL#%d", o.PacketID()))
}

// A write from before a reconnect never lands after the write the new
// connection made for the same flow.
func TestSessionLossWriteWaitsForEarlierWrite(t *testing.T) {
	st := newFaultStore()
	st.hold = phaseIs(session.AwaitPubcomp)
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	o := h.publish(2, "old session")
	h.collect()
	h.e.HandlePubrec(o.PacketID(), 0, nil)
	<-st.held
	h.disconnect()
	h.connect(false, 10)
	want(t, h.collect()) // republished only once its new record is stored
	close(st.release)
	h.drain()
	recs := records(t, st)
	if len(recs) != 1 || recs[0].Phase != session.AwaitPubrec || recs[0].PubrecSeq != 0 {
		t.Fatalf("records after the reconnect: %+v", recs)
	}
	want(t, h.collect(), fmt.Sprintf("PUBLISH#%d", o.PacketID()))
}

// The original publication order survives a restart for flows that had
// reached AwaitPubcomp, so a lost session republishes in that order.
func TestRestartKeepsPublishOrder(t *testing.T) {
	st := session.NewMemoryStore()
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	first := h.publish(2, "first")
	second := h.publish(2, "second")
	third := h.publish(2, "third")
	h.collect()
	// PUBRECs arrive out of order.
	h.e.HandlePubrec(third.PacketID(), 0, nil)
	h.e.HandlePubrec(first.PacketID(), 0, nil)
	h.collect()

	resumed := newHarness(t, Config{Store: st})
	if err := resumed.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	resumed.connect(true, 10)
	want(t, resumed.collect(),
		fmt.Sprintf("PUBREL#%d", third.PacketID()), fmt.Sprintf("PUBREL#%d", first.PacketID()),
		fmt.Sprintf("PUBLISH#%d+dup", second.PacketID()))

	lost := newHarness(t, Config{Store: st})
	if err := lost.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	lost.connect(false, 10)
	got := lost.collect()
	var pays []string
	for _, f := range got {
		pays = append(pays, f.pay)
	}
	if fmt.Sprint(pays) != "[first second third]" {
		t.Fatalf("republished %v after a restart and a lost session", pays)
	}
}

// A message that waited for send quota past its expiry is not sent, and
// one that waited less goes out with the time it has left.
func TestUnsentMessageExpiresWhileWaiting(t *testing.T) {
	for _, store := range []bool{false, true} {
		t.Run(fmt.Sprintf("store=%v", store), func(t *testing.T) {
			cfg := Config{}
			var st *session.MemoryStore
			if store {
				st = session.NewMemoryStore()
				cfg.Store = st
			}
			h := newHarness(t, cfg)
			h.connect(false, 1)
			holder := h.publish(1, "quota holder")
			h.collect()
			expiry := func(s uint32) func(*wire.PublishOpts) {
				return func(o *wire.PublishOpts) { o.MessageExpiryInterval = &s }
			}
			stale := h.publish(1, "stale", expiry(1))
			fresh := h.publish(1, "fresh", expiry(10))
			h.now = h.now.Add(3 * time.Second)
			h.e.HandlePuback(holder.PacketID(), 0, nil)
			wakes := h.link.wakes.Load()
			got := h.collect()
			if store {
				// The shortened packet goes out once its record holds it;
				// the write wakes the writer.
				want(t, got)
				h.drain()
				if h.link.wakes.Load() == wakes {
					t.Fatal("the record write did not wake the writer")
				}
				got = h.collect()
			}
			if err := done(t, stale); !errors.Is(err, ErrMessageExpired) {
				t.Fatalf("stale message: %v", err)
			}
			if len(got) != 1 || got[0].id != fresh.PacketID() || got[0].mei != 7 {
				t.Fatalf("sent %+v, want only the fresh message with 7 s left", got)
			}
			if store {
				recs := records(t, st)
				if len(recs) != 1 {
					t.Fatalf("records: %+v", recs)
				}
				dec := decodeAll(t, recs[0].Packet)
				if len(dec) != 1 || dec[0].mei != 7 {
					t.Fatalf("stored packet %+v, want the interval as sent", dec)
				}
			}
		})
	}
}

// A producer that cannot record the outcome keeps the record and the
// identifier; Adopt offers the outcome again and only then is the
// record deleted.
func TestFailedSettleKeepsTheRecord(t *testing.T) {
	st := session.NewMemoryStore()
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 1, PacketID: id})
	attempts := make(chan error, 1)
	o, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: 1, Packet: pkt, Ref: []byte("entry"),
		Settle: func(_ context.Context, outcome error) error {
			attempts <- outcome
			return errors.New("queue unavailable")
		}})
	if err != nil {
		t.Fatal(err)
	}
	h.collect()
	h.e.HandlePuback(o.PacketID(), 0, nil)
	if err := recvSettle(t, attempts); err != nil {
		t.Fatalf("outcome %v", err)
	}
	h.drain()
	if recs := records(t, st); len(recs) != 1 {
		t.Fatalf("record not kept after a failed settle: %+v", recs)
	}
	if h.e.ids.owner[id] != OwnerPublish || !h.e.HasState() {
		t.Fatal("identifier or state released before the outcome was recorded")
	}

	settled := make(chan error, 1)
	if !h.e.Adopt([]byte("entry"), func(_ context.Context, outcome error) error { settled <- outcome; return nil }) {
		t.Fatal("the unsettled flow was not offered again")
	}
	if err := recvSettle(t, settled); err != nil {
		t.Fatalf("outcome %v", err)
	}
	h.drain()
	if recs := records(t, st); len(recs) != 0 {
		t.Fatalf("record left after the outcome was recorded: %+v", recs)
	}
	if h.e.ids.owner[id] != OwnerNone {
		t.Fatal("identifier not released")
	}
}

// A flow restored without its producer keeps its record until the
// producer adopts it, also when it finished first.
func TestOrphanKeepsRecordUntilAdopted(t *testing.T) {
	st := session.NewMemoryStore()
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	o, _ := h.register(nil, 1, "entry")
	h.collect()

	restarted := newHarness(t, Config{Store: st})
	if err := restarted.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted.connect(true, 10)
	restarted.collect()
	restarted.e.HandlePuback(o.PacketID(), 0, nil)
	restarted.drain()
	if recs := records(t, st); len(recs) != 1 {
		t.Fatalf("finished orphan lost its record before adoption: %+v", recs)
	}
	if !restarted.e.HasState() {
		t.Fatal("an unadopted orphan is session state worth resuming")
	}
	settled := make(chan error, 1)
	if !restarted.e.Adopt([]byte("entry"), func(_ context.Context, err error) error { settled <- err; return nil }) {
		t.Fatal("orphan not found")
	}
	if err := recvSettle(t, settled); err != nil {
		t.Fatal(err)
	}
	restarted.drain()
	if recs := records(t, st); len(recs) != 0 {
		t.Fatalf("record left after adoption: %+v", recs)
	}
}

// The store writes of one flow run in the order they were decided even
// when the store is slow and the flow changes meanwhile.
func TestInboundDeleteWaitsForItsPut(t *testing.T) {
	st := newFaultStore()
	st.hold = phaseIs(session.AwaitPubrel)
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	in, _, _ := h.e.Receive(5, 2)
	h.e.Ack(in)
	<-st.held
	h.disconnect()
	h.connect(false, 10) // session lost: the inbound record is deleted
	close(st.release)
	h.drain()
	if recs := records(t, st); len(recs) != 0 {
		t.Fatalf("the delete ran before the put it follows: %+v", recs)
	}
}

// A producer that adopts a flow again while its Settle is still running
// — a retry scheduled by the failing Settle itself — gets the outcome
// once that Settle has failed; the flow is never invisible to Adopt.
func TestAdoptWhileSettling(t *testing.T) {
	st := session.NewMemoryStore()
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 1, PacketID: id})
	retried := make(chan error, 1)
	o, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: 1, Packet: pkt, Ref: []byte("entry"),
		Settle: func(context.Context, error) error {
			if !h.e.Adopt([]byte("entry"), func(_ context.Context, outcome error) error { retried <- outcome; return nil }) {
				t.Error("the flow was not adoptable while its Settle ran")
			}
			return errors.New("queue unavailable")
		}})
	if err != nil {
		t.Fatal(err)
	}
	h.collect()
	h.e.HandlePuback(o.PacketID(), 0, nil)
	if err := recvSettle(t, retried); err != nil {
		t.Fatalf("outcome %v", err)
	}
	h.drain()
	if recs := records(t, st); len(recs) != 0 {
		t.Fatalf("record left after the second Settle recorded the outcome: %+v", recs)
	}
	if h.e.ids.owner[id] != OwnerNone || h.e.HasState() {
		t.Fatal("identifier or orphan kept after the outcome was recorded")
	}
}

// deleteGate holds the first Delete until release is closed.
type deleteGate struct {
	*session.MemoryStore
	held, release chan struct{}
	once          sync.Once
}

func (s *deleteGate) Delete(ctx context.Context, k session.RecordKey) error {
	s.once.Do(func() {
		close(s.held)
		<-s.release
	})
	return s.MemoryStore.Delete(ctx, k)
}

// A new inbound flow that reuses a packet identifier stores its record
// only after the old session's delete of that record: the old write can
// never erase the new one.
func TestReusedInboundIDWaitsForOldDelete(t *testing.T) {
	st := &deleteGate{MemoryStore: session.NewMemoryStore(), held: make(chan struct{}), release: make(chan struct{})}
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	in, _, _ := h.e.Receive(5, 2)
	h.e.Ack(in)
	h.drain()
	want(t, h.collect(), "PUBREC#5")

	h.disconnect()
	h.connect(false, 10) // the session is lost; its record is deleted
	<-st.held
	next, deliver, err := h.e.Receive(5, 2)
	if !deliver || err != nil {
		t.Fatalf("Receive: deliver %v, err %v", deliver, err)
	}
	h.e.Ack(next)
	want(t, h.collect()) // its PUBREC waits for its record, behind the delete
	close(st.release)
	h.drain()
	want(t, h.collect(), "PUBREC#5")
	if recs := records(t, st); len(recs) != 1 {
		t.Fatalf("records: %+v, want the new flow's", recs)
	}
}

// A message the broker accepted but whose producer could not record it is
// stored as Completed: a restart, with or without the session, never
// sends it again, and the producer still gets the outcome.
func TestAcceptedOutcomeSurvivesRestart(t *testing.T) {
	for _, tc := range []struct {
		qos     byte
		present bool
	}{{1, true}, {1, false}, {2, true}, {2, false}} {
		t.Run(fmt.Sprintf("qos=%d/session-present=%v", tc.qos, tc.present), func(t *testing.T) {
			st := session.NewMemoryStore()
			h := newHarness(t, Config{Store: st})
			h.connect(false, 10)
			id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
			pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: tc.qos, PacketID: id})
			if _, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: tc.qos, Packet: pkt, Ref: []byte("entry"),
				Settle: func(context.Context, error) error { return errors.New("queue unavailable") }}); err != nil {
				t.Fatal(err)
			}
			h.collect()
			if tc.qos == 1 {
				h.e.HandlePuback(id, 0, nil)
			} else {
				h.e.HandlePubrec(id, 0, nil)
				h.collect()
				h.e.HandlePubcomp(id)
			}
			h.drain()
			recs := records(t, st)
			if len(recs) != 1 || recs[0].Phase != session.Completed {
				t.Fatalf("records after a failed settle: %+v", recs)
			}

			restarted := newHarness(t, Config{Store: st})
			if err := restarted.e.Restore(context.Background()); err != nil {
				t.Fatal(err)
			}
			restarted.connect(tc.present, 10)
			want(t, restarted.collect())
			settled := make(chan error, 1)
			if !restarted.e.Adopt([]byte("entry"), func(_ context.Context, err error) error { settled <- err; return nil }) {
				t.Fatal("the completed message was not adoptable after the restart")
			}
			if err := recvSettle(t, settled); err != nil {
				t.Fatalf("outcome %v, want accepted", err)
			}
			restarted.drain()
			if recs := records(t, st); len(recs) != 0 {
				t.Fatalf("records after the producer recorded the outcome: %+v", recs)
			}
		})
	}
}

// A session that fails while a message's record is written completes the
// message once, through its Settle; Register does not also fail it.
func TestFailureDuringRegisterCompletesOnce(t *testing.T) {
	st := newFaultStore()
	st.hold = phaseIs(session.AwaitPuback)
	st.failMeta.Store(true)
	h := newHarness(t, Config{Store: st, OnFailure: func(error) {}})
	h.connect(false, 10)
	id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 1, PacketID: id})
	settles := make(chan error, 2)
	registered := make(chan error, 1)
	go func() {
		_, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: 1, Packet: pkt, Ref: []byte("entry"),
			Settle: func(_ context.Context, err error) error { settles <- err; return nil }})
		registered <- err
	}()
	<-st.held
	if err := h.e.SetMeta(context.Background(), session.Meta{ClientID: "x"}); err == nil {
		t.Fatal("SetMeta succeeded although the store failed")
	}
	if err := recvSettle(t, settles); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("settled with %v", err)
	}
	close(st.release)
	if err := <-registered; err != nil {
		t.Fatalf("Register returned %v after the flow had settled", err)
	}
	h.drain()
	select {
	case err := <-settles:
		t.Fatalf("settled twice, the second time with %v", err)
	default:
	}
}

// putGate fails the first Put after holding it until release is closed.
type putGate struct {
	*session.MemoryStore
	held, release chan struct{}
	first         sync.Once
}

func (s *putGate) Put(ctx context.Context, r session.Record) error {
	failed := false
	s.first.Do(func() {
		close(s.held)
		<-s.release
		failed = true
	})
	if failed {
		return errDiskFull
	}
	return s.MemoryStore.Put(ctx, r)
}

// A message whose first record write fails leaves nothing behind, also
// when a reconnect queued another write for it meanwhile.
func TestFailedFirstPutCancelsLaterWrites(t *testing.T) {
	st := &putGate{MemoryStore: session.NewMemoryStore(), held: make(chan struct{}), release: make(chan struct{})}
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 1, PacketID: id})
	returned := make(chan error, 1)
	go func() {
		_, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: 1, Packet: pkt})
		returned <- err
	}()
	<-st.held
	h.disconnect()
	h.connect(false, 10) // queues a rewrite of the flow's record
	close(st.release)
	if err := <-returned; !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Register: %v", err)
	}
	h.drain()
	if recs := records(t, st); len(recs) != 0 {
		t.Fatalf("a message never accepted left a record: %+v", recs)
	}
	want(t, h.collect())
}

// refusalErr is a test Config.Refusal that keeps the reason code.
type refusalErr struct {
	t  wire.PacketType
	rc wire.ReasonCode
}

func (e refusalErr) Error() string { return fmt.Sprintf("%s %#x", e.t, byte(e.rc)) }

// Every way a tracked message's exchange can end is kept as a Completed
// record while its producer cannot record it: after a restart nothing is
// sent again — a refused PUBLISH must never be ([MQTT-4.4.0-2]) — and
// Adopt gives the producer the same outcome.
func TestEveryOutcomeSurvivesRestart(t *testing.T) {
	notAuthorized := errors.New("refused")
	for _, tc := range []struct {
		name string
		qos  byte
		end  func(h *harness, id uint16)
		want func(t *testing.T, err error)
	}{
		{"accepted QoS 1", 1, func(h *harness, id uint16) { h.e.HandlePuback(id, 0, nil) },
			func(t *testing.T, err error) {
				if err != nil {
					t.Fatalf("outcome %v, want accepted", err)
				}
			}},
		{"refused QoS 1", 1, func(h *harness, id uint16) { h.e.HandlePuback(id, wire.ReasonNotAuthorized, notAuthorized) },
			func(t *testing.T, err error) {
				if r, ok := err.(refusalErr); !ok || r.t != wire.PUBACK || r.rc != wire.ReasonNotAuthorized {
					t.Fatalf("outcome %v, want the PUBACK refusal 0x87", err)
				}
			}},
		{"refused QoS 2", 2, func(h *harness, id uint16) { h.e.HandlePubrec(id, wire.ReasonQuotaExceeded, notAuthorized) },
			func(t *testing.T, err error) {
				if r, ok := err.(refusalErr); !ok || r.t != wire.PUBREC || r.rc != wire.ReasonQuotaExceeded {
					t.Fatalf("outcome %v, want the PUBREC refusal 0x97", err)
				}
			}},
	} {
		for _, present := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/session-present=%v", tc.name, present), func(t *testing.T) {
				st := session.NewMemoryStore()
				cfg := Config{Store: st, Refusal: func(t wire.PacketType, rc wire.ReasonCode) error { return refusalErr{t, rc} }}
				h := newHarness(t, cfg)
				h.connect(false, 10)
				id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
				pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: tc.qos, PacketID: id})
				if _, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: tc.qos, Packet: pkt, Ref: []byte("entry"),
					Settle: func(context.Context, error) error { return errors.New("queue unavailable") }}); err != nil {
					t.Fatal(err)
				}
				h.collect()
				tc.end(h, id)
				h.drain()

				restarted := newHarness(t, cfg)
				if err := restarted.e.Restore(context.Background()); err != nil {
					t.Fatal(err)
				}
				restarted.connect(present, 10)
				want(t, restarted.collect())
				settled := make(chan error, 1)
				if !restarted.e.Adopt([]byte("entry"), func(_ context.Context, err error) error { settled <- err; return nil }) {
					t.Fatal("the ended exchange was not adoptable after the restart")
				}
				tc.want(t, recvSettle(t, settled))
				restarted.drain()
				if recs := records(t, st); len(recs) != 0 {
					t.Fatalf("records after the producer recorded the outcome: %+v", recs)
				}
			})
		}
	}
}

// A restored message that ends before its producer adopts it keeps its
// outcome through another restart, instead of its phase from before.
func TestRestoredOutcomeKeptUntilAdopted(t *testing.T) {
	st := session.NewMemoryStore()
	h := newHarness(t, Config{Store: st})
	h.connect(false, 10)
	o, _ := h.register(nil, 1, "entry")
	h.collect()

	second := newHarness(t, Config{Store: st})
	if err := second.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	second.connect(true, 10)
	second.collect()
	second.e.HandlePuback(o.PacketID(), 0, nil) // before anyone adopts it
	second.drain()

	third := newHarness(t, Config{Store: st})
	if err := third.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	third.connect(true, 10)
	want(t, third.collect())
	settled := make(chan error, 1)
	if !third.e.Adopt([]byte("entry"), func(_ context.Context, err error) error { settled <- err; return nil }) {
		t.Fatal("not adoptable")
	}
	if err := recvSettle(t, settled); err != nil {
		t.Fatalf("outcome %v, want accepted", err)
	}
}

// A registration rejected because its record could not be written leaves
// nothing to adopt, also when a session loss ended it meanwhile.
func TestRejectedRegistrationLeavesNoOrphan(t *testing.T) {
	st := &putGate{MemoryStore: session.NewMemoryStore(), held: make(chan struct{}), release: make(chan struct{})}
	h := newHarness(t, Config{Store: st, SessionLoss: Fail})
	h.connect(false, 10)
	id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 1, PacketID: id})
	settled := make(chan error, 2)
	returned := make(chan error, 1)
	go func() {
		_, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: 1, Packet: pkt, Ref: []byte("entry"),
			Settle: func(_ context.Context, err error) error { settled <- err; return nil }})
		returned <- err
	}()
	<-st.held
	h.disconnect()
	h.connect(false, 10) // the Fail policy ends the flow while its Put runs
	close(st.release)
	if err := <-returned; err == nil {
		t.Fatal("Register accepted a message whose record was never written")
	}
	h.drain()
	if h.e.Adopt([]byte("entry"), func(_ context.Context, err error) error { settled <- err; return nil }) {
		t.Fatal("a rejected registration is adoptable")
	}
	select {
	case err := <-settled:
		t.Fatalf("a rejected registration settled with %v", err)
	default:
	}
	if h.e.HasState() || h.e.ids.owner[id] != OwnerNone {
		t.Fatal("a rejected registration left state behind")
	}
}
