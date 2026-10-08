// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

// fakeLink records wake-ups and ordered markers.
type fakeLink struct {
	wakes atomic.Int32
	mu    sync.Mutex
	marks []uint64
}

func (l *fakeLink) Wake() { l.wakes.Add(1) }
func (l *fakeLink) SendOrdered(_ context.Context, seq uint64) error {
	l.mu.Lock()
	l.marks = append(l.marks, seq)
	l.mu.Unlock()
	return nil
}

// frame is a decoded packet from a Collect batch.
type frame struct {
	typ wire.PacketType
	id  uint16
	dup bool
	rc  wire.ReasonCode
	pay string
	mei uint32
}

func (f frame) String() string {
	s := fmt.Sprintf("%s#%d", f.typ, f.id)
	if f.dup {
		s += "+dup"
	}
	if f.rc != 0 {
		s += fmt.Sprintf("(%#x)", byte(f.rc))
	}
	return s
}

type harness struct {
	t    *testing.T
	e    *Engine
	link *fakeLink
	gen  uint64
	now  time.Time
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{t: t, now: time.Unix(1_700_000_000, 0)}
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg.Now = func() time.Time { return h.now }
	h.e = New(cfg)
	return h
}

// connect binds the engine to a new fake connection.
func (h *harness) connect(sp bool, recvMax uint16) bool {
	h.t.Helper()
	h.link = &fakeLink{}
	gen, resub, err := h.e.Connected(ConnInfo{SessionPresent: sp, ReceiveMaximum: recvMax, Link: h.link})
	if err != nil {
		h.t.Fatalf("Connected: %v", err)
	}
	h.gen = gen
	return resub
}

func (h *harness) disconnect() { h.e.Disconnected(h.link) }

// collect drains everything the writer would send now, releasing all
// registered flows.
func (h *harness) collect() []frame {
	h.t.Helper()
	var out []frame
	var f Frames
	for {
		f.Reset()
		if h.e.Collect(h.gen, ^uint64(0)>>1, &f) == 0 {
			return out
		}
		var buf bytes.Buffer
		if _, err := f.WriteTo(&buf); err != nil {
			h.t.Fatal(err)
		}
		out = append(out, decodeAll(h.t, buf.Bytes())...)
	}
}

func decodeAll(t *testing.T, b []byte) []frame {
	t.Helper()
	dec := wire.NewDecoder(bytes.NewReader(b))
	var out []frame
	for {
		p, err := dec.ReadPacket()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("decode batch: %v", err)
		}
		switch x := p.(type) {
		case *wire.Publish:
			mei, _ := x.Properties.Uint32(wire.PropMessageExpiryInterval)
			out = append(out, frame{typ: wire.PUBLISH, id: x.PacketID, dup: x.Dup, pay: string(x.Payload), mei: mei})
		case *wire.PubResp:
			out = append(out, frame{typ: x.Type(), id: x.PacketID, rc: x.ReasonCode})
		default:
			t.Fatalf("unexpected %s in batch", p.Type())
		}
		p.Release()
	}
}

// publish allocates an identifier and registers a flow.
func (h *harness) publish(qos byte, payload string, opts ...func(*wire.PublishOpts)) *Out {
	h.t.Helper()
	id, err := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	po := wire.PublishOpts{Topic: "t", Payload: []byte(payload), QoS: qos, PacketID: id}
	for _, o := range opts {
		o(&po)
	}
	pkt, err := wire.MarshalPublish(po)
	if err != nil {
		h.t.Fatal(err)
	}
	var exp time.Time
	if po.MessageExpiryInterval != nil {
		exp = h.now.Add(time.Duration(*po.MessageExpiryInterval) * time.Second)
	}
	o, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: qos, Packet: pkt, ExpiresAt: exp})
	if err != nil {
		h.t.Fatal(err)
	}
	return o
}

func want(t *testing.T, got []frame, wantStr ...string) {
	t.Helper()
	var s []string
	for _, f := range got {
		s = append(s, f.String())
	}
	if !slices.Equal(s, wantStr) {
		t.Fatalf("frames = %v, want %v", s, wantStr)
	}
}

func done(t *testing.T, o *Out) error {
	t.Helper()
	select {
	case <-o.Done():
		return o.Err()
	default:
		t.Fatalf("flow %d not complete", o.PacketID())
		return nil
	}
}

func notDone(t *testing.T, o *Out) {
	t.Helper()
	select {
	case <-o.Done():
		t.Fatalf("flow %d completed early (err %v)", o.PacketID(), o.Err())
	default:
	}
}

func TestQoS1PublishAndAck(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	o := h.publish(1, "a")
	want(t, h.collect(), fmt.Sprintf("PUBLISH#%d", o.PacketID()))
	notDone(t, o)
	h.e.HandlePuback(o.PacketID(), nil)
	if err := done(t, o); err != nil {
		t.Fatal(err)
	}
	if h.e.OutboundLen() != 0 || h.e.ids.owner[o.PacketID()] != OwnerNone {
		t.Fatal("flow or identifier left behind after PUBACK")
	}
}

func TestPublishRefusedReturnsError(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	o := h.publish(1, "a")
	h.collect()
	refused := errors.New("not authorized")
	h.e.HandlePuback(o.PacketID(), refused)
	if err := done(t, o); err != refused {
		t.Fatalf("err = %v", err)
	}
}

func TestPublishesWaitForTheirMarker(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	a := h.publish(1, "a")
	b := h.publish(1, "b")
	// Registered after Connected, so nothing goes out before a marker.
	var f Frames
	if n := h.e.Collect(h.gen, 0, &f); n != 0 {
		t.Fatalf("collected %d before any marker", n)
	}
	f.Reset()
	if n := h.e.Collect(h.gen, a.Seq(), &f); n != 1 {
		t.Fatalf("marker for a released %d packets, want 1", n)
	}
	f.Reset()
	if n := h.e.Collect(h.gen, b.Seq(), &f); n != 1 {
		t.Fatalf("marker for b released %d packets, want 1", n)
	}
}

func TestStaleWriterGetsNothing(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	old := h.gen
	h.disconnect()
	h.publish(1, "a")
	var f Frames
	if n := h.e.Collect(old, 1<<62, &f); n != 0 {
		t.Fatalf("disconnected writer collected %d", n)
	}
	h.connect(true, 0)
	if n := h.e.Collect(old, 1<<62, &f); n != 0 {
		t.Fatalf("previous connection's writer collected %d", n)
	}
}

func TestResumeResendsInOriginalOrderWithDup(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	var flows []*Out
	for i := 0; i < 5; i++ {
		flows = append(flows, h.publish(1, fmt.Sprint(i)))
	}
	first := h.collect()
	h.e.HandlePuback(flows[2].PacketID(), nil)
	h.disconnect()
	late := h.publish(1, "late") // registered while disconnected: never sent
	h.connect(true, 0)
	got := h.collect()
	var wantFrames []string
	for _, f := range first {
		if f.id != flows[2].PacketID() {
			wantFrames = append(wantFrames, fmt.Sprintf("PUBLISH#%d+dup", f.id))
		}
	}
	wantFrames = append(wantFrames, fmt.Sprintf("PUBLISH#%d", late.PacketID()))
	want(t, got, wantFrames...)
}

func TestQoS2PubrelAfterPubrecAndOnResume(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	o := h.publish(2, "x")
	id := o.PacketID()
	want(t, h.collect(), fmt.Sprintf("PUBLISH#%d", id))
	h.e.HandlePubrec(id, nil)
	want(t, h.collect(), fmt.Sprintf("PUBREL#%d", id))
	h.disconnect()
	h.connect(true, 0)
	// §4.4: after PUBREC the resend is PUBREL, never PUBLISH.
	want(t, h.collect(), fmt.Sprintf("PUBREL#%d", id))
	h.e.HandlePubrec(id, nil) // duplicate PUBREC: answer again
	want(t, h.collect(), fmt.Sprintf("PUBREL#%d", id))
	h.e.HandlePubcomp(id)
	if err := done(t, o); err != nil {
		t.Fatal(err)
	}
}

func TestPubrelOrderFollowsPubrecOrder(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	a, b := h.publish(2, "a"), h.publish(2, "b")
	h.collect()
	h.e.HandlePubrec(b.PacketID(), nil)
	h.e.HandlePubrec(a.PacketID(), nil)
	h.collect()
	h.disconnect()
	h.connect(true, 0)
	want(t, h.collect(), fmt.Sprintf("PUBREL#%d", b.PacketID()), fmt.Sprintf("PUBREL#%d", a.PacketID()))
}

func TestUnknownPubrecGetsPubrel92(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	h.e.HandlePubrec(4242, nil)
	want(t, h.collect(), "PUBREL#4242(0x92)")
}

func TestStrayAckNeverFreesAnotherOwnersID(t *testing.T) {
	var strays []string
	h := newHarness(t, Config{OnStrayAck: func(t wire.PacketType, id uint16) { strays = append(strays, fmt.Sprintf("%s#%d", t, id)) }})
	h.connect(false, 0)
	sub, err := h.e.AllocateID(context.Background(), OwnerSubscribe, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.e.HandlePuback(sub, nil)
	h.e.HandlePubcomp(sub)
	if h.e.ids.owner[sub] != OwnerSubscribe {
		t.Fatal("stray ack freed a SUBSCRIBE identifier")
	}
	if h.e.ReleaseID(sub, OwnerPublish) {
		t.Fatal("ReleaseID with the wrong owner succeeded")
	}
	if !h.e.ReleaseID(sub, OwnerSubscribe) {
		t.Fatal("ReleaseID with the right owner failed")
	}
	if len(strays) != 2 {
		t.Fatalf("strays = %v", strays)
	}
}

func TestSendQuotaFollowsServerReceiveMaximum(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 2)
	a, b, c := h.publish(1, "a"), h.publish(2, "b"), h.publish(1, "c")
	want(t, h.collect(), fmt.Sprintf("PUBLISH#%d", a.PacketID()), fmt.Sprintf("PUBLISH#%d", b.PacketID()))
	h.e.HandlePubrec(b.PacketID(), nil) // QoS 2 keeps its quota until PUBCOMP
	want(t, h.collect(), fmt.Sprintf("PUBREL#%d", b.PacketID()))
	h.e.HandlePuback(a.PacketID(), nil)
	want(t, h.collect(), fmt.Sprintf("PUBLISH#%d", c.PacketID()))
	// Quota never exceeds the maximum, even after a reconnect resends a
	// PUBREL whose PUBCOMP then arrives.
	h.disconnect()
	h.connect(true, 2)
	got := h.collect()
	h.e.HandlePubcomp(b.PacketID())
	if h.e.quota > 2 {
		t.Fatalf("quota %d above Receive Maximum 2 after %v", h.e.quota, got)
	}
}

func TestSessionLossRepublishesInOrderAsNew(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	a, b, c := h.publish(2, "a"), h.publish(1, "b"), h.publish(2, "c")
	h.collect()
	h.e.HandlePubrec(a.PacketID(), nil) // a reaches AwaitPubcomp
	h.collect()
	h.disconnect()
	if !h.connect(false, 0) {
		t.Fatal("session loss must request resubscription")
	}
	want(t, h.collect(),
		fmt.Sprintf("PUBLISH#%d", a.PacketID()),
		fmt.Sprintf("PUBLISH#%d", b.PacketID()),
		fmt.Sprintf("PUBLISH#%d", c.PacketID()))
	for _, o := range []*Out{a, b, c} {
		notDone(t, o)
	}
	h.e.HandlePubrec(a.PacketID(), nil)
	want(t, h.collect(), fmt.Sprintf("PUBREL#%d", a.PacketID()))
}

func TestSessionLossFailPolicy(t *testing.T) {
	h := newHarness(t, Config{SessionLoss: Fail})
	h.connect(false, 0)
	a := h.publish(1, "a")
	h.collect()
	h.disconnect()
	h.connect(false, 0)
	if err := done(t, a); !errors.Is(err, ErrSessionLost) {
		t.Fatalf("err = %v", err)
	}
	if len(h.collect()) != 0 || h.e.ids.owner[a.PacketID()] != OwnerNone {
		t.Fatal("failed flow still sent or still owns its identifier")
	}
}

func TestRepublishHonoursMessageExpiry(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	ten := uint32(10)
	hundred := uint32(100)
	short := h.publish(1, "short", func(o *wire.PublishOpts) { o.MessageExpiryInterval = &ten })
	long := h.publish(1, "long", func(o *wire.PublishOpts) { o.MessageExpiryInterval = &hundred })
	h.collect()
	h.disconnect()
	h.now = h.now.Add(30 * time.Second)
	h.connect(false, 0)
	if err := done(t, short); !errors.Is(err, ErrMessageExpired) {
		t.Fatalf("expired message: err = %v", err)
	}
	got := h.collect()
	if len(got) != 1 || got[0].id != long.PacketID() || got[0].mei != 70 {
		t.Fatalf("republished = %+v, want long with 70 s left", got)
	}
}

func TestInboundAcksLeaveInArrivalOrder(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	var ins []*In
	for id := uint16(1); id <= 3; id++ {
		in, deliver, err := h.e.Receive(id, 1)
		if err != nil || !deliver {
			t.Fatal(deliver, err)
		}
		ins = append(ins, in)
	}
	h.e.Ack(ins[2])
	h.e.Ack(ins[1])
	want(t, h.collect())
	h.e.Ack(ins[0])
	want(t, h.collect(), "PUBACK#1", "PUBACK#2", "PUBACK#3")
}

func TestInboundDuplicatesAreNotRedelivered(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	in, deliver, _ := h.e.Receive(1, 1)
	if !deliver {
		t.Fatal("first PUBLISH not delivered")
	}
	if _, deliver, _ := h.e.Receive(1, 1); deliver {
		t.Fatal("duplicate delivered while the application holds the message")
	}
	h.e.Ack(in)
	if _, deliver, _ := h.e.Receive(1, 1); deliver {
		t.Fatal("duplicate delivered before its PUBACK was written")
	}
	want(t, h.collect(), "PUBACK#1")
	if _, deliver, _ := h.e.Receive(1, 1); !deliver {
		t.Fatal("a new PUBLISH reusing the identifier after PUBACK was not delivered")
	}
}

func TestInboundDuplicateNeverAcksForTheApplication(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	h.e.Receive(1, 1)
	h.disconnect()
	h.connect(true, 0)
	h.e.Receive(1, 1) // DUP resend on resume
	want(t, h.collect())
}

func TestInboundQoS2(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	in, _, _ := h.e.Receive(7, 2)
	h.e.Ack(in)
	want(t, h.collect(), "PUBREC#7")
	if _, deliver, _ := h.e.Receive(7, 2); deliver {
		t.Fatal("QoS 2 duplicate delivered")
	}
	want(t, h.collect(), "PUBREC#7")
	h.e.HandlePubrel(7)
	h.e.HandlePubrel(99)
	want(t, h.collect(), "PUBCOMP#7", "PUBCOMP#99(0x92)")
	if _, deliver, _ := h.e.Receive(7, 2); !deliver {
		t.Fatal("new QoS 2 PUBLISH after PUBCOMP not delivered")
	}
}

func TestSessionLossDiscardsInboundState(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	old, _, _ := h.e.Receive(5, 2)
	acked, _, _ := h.e.Receive(6, 1)
	h.e.Ack(acked) // PUBACK queued but never written
	h.disconnect()
	h.connect(false, 0)
	h.e.Ack(old)
	want(t, h.collect())
	if _, deliver, _ := h.e.Receive(5, 2); !deliver {
		t.Fatal("new session's PUBLISH dropped as a duplicate of discarded state")
	}
}

func TestReceiveMaximumEnforced(t *testing.T) {
	h := newHarness(t, Config{ReceiveMaximum: 2})
	h.connect(false, 0)
	h.e.Receive(1, 1)
	h.e.Receive(2, 2)
	if _, _, err := h.e.Receive(3, 1); !errors.Is(err, ErrReceiveMaximumExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestWithdrawOnlyUnsentFlows(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 0)
	sent := h.publish(1, "sent")
	h.collect()
	unsent := h.publish(1, "unsent")
	if h.e.Withdraw(sent) {
		t.Fatal("withdrew a flow already handed to the connection")
	}
	if !h.e.Withdraw(unsent) {
		t.Fatal("could not withdraw an unsent flow")
	}
	want(t, h.collect())
	if h.e.ids.owner[unsent.PacketID()] != OwnerNone {
		t.Fatal("withdrawn flow kept its identifier")
	}
}

func TestIdentifiersBlockWhenExhausted(t *testing.T) {
	h := newHarness(t, Config{})
	for i := 0; i < 65535; i++ {
		if _, err := h.e.AllocateID(context.Background(), OwnerPublish, nil); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := h.e.AllocateID(ctx, OwnerPublish, nil); !errors.Is(err, ErrIDsExhausted) {
		t.Fatalf("err = %v", err)
	}
	stop := make(chan struct{})
	close(stop)
	if _, err := h.e.AllocateID(context.Background(), OwnerPublish, stop); !errors.Is(err, ErrStopped) {
		t.Fatalf("stopped wait: err = %v", err)
	}
	h.e.ReleaseID(4000, OwnerPublish)
	id, err := h.e.AllocateID(context.Background(), OwnerSubscribe, nil)
	if err != nil || id != 4000 {
		t.Fatalf("after release: id %d err %v", id, err)
	}
}

// blockingStore lets a test hold store writes to check what waits on them.
type blockingStore struct {
	*session.MemoryStore
	gate chan struct{}
	fail atomic.Bool
}

func (s *blockingStore) Put(ctx context.Context, r session.Record) error {
	<-s.gate
	if s.fail.Load() {
		return errors.New("disk full")
	}
	return s.MemoryStore.Put(ctx, r)
}

func (s *blockingStore) Delete(ctx context.Context, k session.RecordKey) error {
	<-s.gate
	return s.MemoryStore.Delete(ctx, k)
}

// headCtrlReady reports whether the next acknowledgement can be written.
func (h *harness) headCtrlReady() bool {
	h.e.mu.Lock()
	defer h.e.mu.Unlock()
	return len(h.e.ctrl) > 0 && h.e.readyLocked(&h.e.ctrl[0])
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func TestPacketsWaitForTheirRecords(t *testing.T) {
	st := &blockingStore{MemoryStore: session.NewMemoryStore(), gate: make(chan struct{})}
	h := newHarness(t, Config{Store: st})
	h.connect(false, 0)

	registered := make(chan *Out)
	go func() {
		id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
		pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 2, PacketID: id})
		o, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: 2, Packet: pkt})
		if err != nil {
			t.Error(err)
		}
		registered <- o
	}()
	waitFor(t, func() bool { return h.e.OutboundLen() == 1 })
	want(t, h.collect()) // PUBLISH waits for its record
	st.gate <- struct{}{}
	o := <-registered
	want(t, h.collect(), fmt.Sprintf("PUBLISH#%d", o.PacketID()))

	h.e.HandlePubrec(o.PacketID(), nil)
	want(t, h.collect()) // PUBREL waits for the AwaitPubcomp record
	st.gate <- struct{}{}
	waitFor(t, h.headCtrlReady)
	want(t, h.collect(), fmt.Sprintf("PUBREL#%d", o.PacketID()))

	in, _, _ := h.e.Receive(9, 2)
	h.e.Ack(in)
	want(t, h.collect()) // PUBREC waits for the inbound record
	st.gate <- struct{}{}
	waitFor(t, h.headCtrlReady)
	want(t, h.collect(), "PUBREC#9")

	h.e.HandlePubrel(9)
	want(t, h.collect()) // PUBCOMP waits for the delete
	st.gate <- struct{}{}
	waitFor(t, h.headCtrlReady)
	want(t, h.collect(), "PUBCOMP#9")

	h.e.HandlePubcomp(o.PacketID())
	st.gate <- struct{}{} // outbound delete
	if err := h.e.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, recs, _ := st.Load(context.Background()); len(recs) != 0 {
		t.Fatalf("records left after completion: %+v", recs)
	}
}

func TestRegisterFailsWhenStoreFails(t *testing.T) {
	st := &blockingStore{MemoryStore: session.NewMemoryStore(), gate: make(chan struct{})}
	close(st.gate)
	st.fail.Store(true)
	h := newHarness(t, Config{Store: st})
	h.connect(false, 0)
	id, _ := h.e.AllocateID(context.Background(), OwnerPublish, nil)
	pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 1, PacketID: id})
	if _, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: 1, Packet: pkt}); err == nil {
		t.Fatal("Register succeeded although the store failed")
	}
	if h.e.OutboundLen() != 0 || h.e.ids.owner[id] != OwnerNone {
		t.Fatal("failed registration left state behind")
	}
	want(t, h.collect())
}

func TestRestoreResumesStoredFlows(t *testing.T) {
	st := session.NewMemoryStore()
	h := newHarness(t, Config{Store: st})
	h.connect(false, 0)
	q1 := h.publish(1, "one")
	q2 := h.publish(2, "two")
	h.collect()
	h.e.HandlePubrec(q2.PacketID(), nil)
	h.collect()
	in, _, _ := h.e.Receive(11, 2)
	h.e.Ack(in)
	h.collect()
	if err := h.e.SetMeta(context.Background(), session.Meta{ClientID: "assigned"}); err != nil {
		t.Fatal(err)
	}

	// A new process with the same store.
	h2 := newHarness(t, Config{Store: st})
	if err := h2.e.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !h2.e.HasState() || h2.e.Meta().ClientID != "assigned" {
		t.Fatal("restored engine has no state")
	}
	h2.connect(true, 0)
	want(t, h2.collect(), fmt.Sprintf("PUBREL#%d", q2.PacketID()), fmt.Sprintf("PUBLISH#%d+dup", q1.PacketID()))
	if _, deliver, _ := h2.e.Receive(11, 2); deliver {
		t.Fatal("restored AwaitPubrel message delivered again")
	}
	h2.collect()
	h2.e.HandlePubrel(11)
	want(t, h2.collect(), "PUBCOMP#11")
	if _, err := h2.e.AllocateID(context.Background(), OwnerPublish, nil); err != nil {
		t.Fatal(err)
	}
	if h2.e.ids.owner[q1.PacketID()] != OwnerPublish {
		t.Fatal("restored flow's identifier not reserved")
	}
}

// Concurrent publishers, acks and a writer must keep every invariant
// under -race.
func TestConcurrentUse(t *testing.T) {
	h := newHarness(t, Config{})
	h.connect(false, 16)
	const n = 2000
	var wg sync.WaitGroup
	ids := make(chan uint16, n)
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Go(func() {
		var f Frames
		for {
			select {
			case <-stop:
				return
			default:
			}
			f.Reset()
			if h.e.Collect(h.gen, ^uint64(0)>>1, &f) == 0 {
				time.Sleep(10 * time.Microsecond)
				continue
			}
			var buf bytes.Buffer
			_, _ = f.WriteTo(&buf)
			for _, fr := range decodeAll(t, buf.Bytes()) {
				if fr.typ == wire.PUBLISH {
					ids <- fr.id
				}
			}
		}
	})
	acker := sync.WaitGroup{}
	acker.Go(func() {
		for i := 0; i < n; i++ {
			h.e.HandlePuback(<-ids, nil)
		}
	})
	for w := 0; w < 8; w++ {
		wg.Go(func() {
			for i := 0; i < n/8; i++ {
				id, err := h.e.AllocateID(context.Background(), OwnerPublish, nil)
				if err != nil {
					t.Error(err)
					return
				}
				pkt, _ := wire.MarshalPublish(wire.PublishOpts{Topic: "t", QoS: 1, PacketID: id})
				o, _, err := h.e.Register(context.Background(), Message{ID: id, QoS: 1, Packet: pkt})
				if err != nil {
					t.Error(err)
					return
				}
				<-o.Done()
			}
		})
	}
	wg.Wait()
	acker.Wait()
	close(stop)
	writer.Wait()
	if h.e.OutboundLen() != 0 {
		t.Fatalf("%d flows left", h.e.OutboundLen())
	}
}

func TestDiscardDropsEverything(t *testing.T) {
	st := session.NewMemoryStore()
	h := newHarness(t, Config{Store: st})
	h.connect(false, 0)
	o := h.publish(1, "a")
	h.collect()
	in, _, _ := h.e.Receive(3, 2)
	if err := h.e.SetMeta(context.Background(), session.Meta{ClientID: "x"}); err != nil {
		t.Fatal(err)
	}
	h.disconnect()
	if err := h.e.Discard(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := done(t, o); !errors.Is(err, ErrSessionLost) {
		t.Fatalf("err = %v", err)
	}
	h.e.Ack(in)
	if h.e.HasState() || h.e.Meta() != (session.Meta{}) {
		t.Fatal("state survived Discard")
	}
	if m, recs, _ := st.Load(context.Background()); len(recs) != 0 || m != (session.Meta{}) {
		t.Fatalf("store not reset: %+v %+v", m, recs)
	}
	h.connect(false, 0)
	want(t, h.collect())
	if h.e.ids.owner[o.PacketID()] != OwnerNone {
		t.Fatal("discarded flow kept its identifier")
	}
}

func TestConnectedEnforcesBrokerLimits(t *testing.T) {
	for _, downgrade := range []bool{false, true} {
		t.Run(fmt.Sprintf("downgrade=%v", downgrade), func(t *testing.T) {
			h := newHarness(t, Config{})
			big := h.publish(1, string(make([]byte, 200)))
			retained := h.publish(1, "r", func(o *wire.PublishOpts) { o.Retain = true })
			q2 := h.publish(2, "q2")
			ok := h.publish(1, "ok")
			h.link = &fakeLink{}
			gen, _, err := h.e.Connected(ConnInfo{Link: h.link, Limits: &Limits{
				MaximumQoS: 1, RetainAvailable: false, MaximumPacketSize: 100, Downgrade: downgrade,
			}})
			if err != nil {
				t.Fatal(err)
			}
			h.gen = gen
			if err := done(t, big); !errors.Is(err, ErrPacketTooLarge) {
				t.Errorf("oversized: %v", err)
			}
			if err := done(t, retained); !errors.Is(err, ErrRetainNotSupported) {
				t.Errorf("retained: %v", err)
			}
			got := h.collect()
			if downgrade {
				notDone(t, q2)
				want(t, got, fmt.Sprintf("PUBLISH#%d", q2.PacketID()), fmt.Sprintf("PUBLISH#%d", ok.PacketID()))
				h.e.HandlePuback(q2.PacketID(), nil)
				if err := done(t, q2); err != nil {
					t.Errorf("downgraded QoS 2 after PUBACK: %v", err)
				}
			} else {
				if err := done(t, q2); !errors.Is(err, ErrQoSNotSupported) {
					t.Errorf("QoS 2: %v", err)
				}
				want(t, got, fmt.Sprintf("PUBLISH#%d", ok.PacketID()))
			}
		})
	}
}
