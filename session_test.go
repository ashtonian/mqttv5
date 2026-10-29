package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/session"
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

// Unless told otherwise the client advertises DefaultReceiveMaximum,
// which bounds what a subscription whose consumer falls behind holds.
func TestDefaultReceiveMaximumAdvertised(t *testing.T) {
	got := make(chan uint16, 1)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		info := c.AcceptConnect(wire.ConnackOpts{})
		if info == nil {
			return
		}
		v, _ := info.Properties().Uint16(wire.PropReceiveMaximum)
		got <- v
		c.Hold(0)
	})
	tbClient(t, b)
	if v := <-got; v != DefaultReceiveMaximum {
		t.Fatalf("Receive Maximum = %d, want %d", v, DefaultReceiveMaximum)
	}
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

// registerGate holds the first AwaitPuback Put until release is closed
// and fails every AwaitPubcomp Put.
type registerGate struct {
	*session.MemoryStore
	held, release chan struct{}
	once          sync.Once
}

func (s *registerGate) Put(ctx context.Context, r session.Record) error {
	if r.Phase == session.AwaitPuback {
		s.once.Do(func() {
			close(s.held)
			<-s.release
		})
	}
	if r.Phase == session.AwaitPubcomp {
		return errors.New("disk full")
	}
	return s.MemoryStore.Put(ctx, r)
}

// A store failure while a queued message's record is being written ends
// that message's exchange once: the QueuePublisher sees one outcome, so
// its accounting holds (no negative WaitGroup) and it closes cleanly.
func TestQueuePublisherSurvivesStoreFailureDuringRegister(t *testing.T) {
	st := &registerGate{MemoryStore: session.NewMemoryStore(), held: make(chan struct{}), release: make(chan struct{})}
	triggered, pubrec := make(chan struct{}), make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		p := c.Expect(wire.PUBLISH, 0)
		close(triggered)
		<-pubrec
		c.Pubrec(p.PacketID, wire.ReasonSuccess)
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithStore(st), WithOnStoreFailure(func(error) {}))
	failed := make(chan error, 1)
	go func() { failed <- cli.Publish(context.Background(), PublishOptions{Topic: "trigger", QoS: 2}) }()
	// PUBLISHes go out in order: the queued message, held below, must
	// come after the trigger.
	<-triggered
	q := NewMemoryPublisherQueue()
	p := newQueuePublisher(t, cli, q)
	enqueueAll(t, p, 1, "registering")
	<-st.held
	close(pubrec) // the QoS 2 phase write fails and ends the session
	select {
	case err := <-failed:
		if !errors.Is(err, ErrStoreFailed) {
			t.Fatalf("Publish: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the store failure did not end the session")
	}
	close(st.release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.engine.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := q.Len(context.Background()); n != 1 {
		t.Fatalf("queue length %d, want the message kept", n)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// pausingLog holds the client between the session's reconciliation and
// the connection's activation, at the log line that falls between them.
type pausingLog struct {
	slog.Handler
	entered, release chan struct{}
	once             sync.Once
}

func (h *pausingLog) Handle(ctx context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, "mqttv5: connected without a session") {
		h.once.Do(func() {
			close(h.entered)
			<-h.release
		})
	}
	return nil
}

// A store write that Connected started and that fails before the
// connection is installed stops the client for good: Connect returns the
// failure and OnConnectionUp never fires.
func TestStoreFailureDuringActivation(t *testing.T) {
	st := &phaseFailStore{MemoryStore: session.NewMemoryStore(), phase: session.AwaitPuback}
	storedPublish(t, st.MemoryStore, 1, 1, "stored")
	st.fail.Store(true) // the session-loss rewrite of the stored message fails
	log := &pausingLog{Handler: slog.Default().Handler(), entered: make(chan struct{}), release: make(chan struct{})}
	failures := make(chan error, 1)
	up := make(chan struct{}, 1)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli, err := New(WithBroker(b.URL()), WithClientID("activation"), WithStore(st), WithLogger(slog.New(log)),
		WithoutKeepAlive(), WithOnStoreFailure(func(err error) { failures <- err }),
		WithOnConnectionUp(func(ConnackInfo) { up <- struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	connected := make(chan error, 1)
	go func() { connected <- cli.Connect(context.Background()) }()
	<-log.entered
	// The write fails while Connect is held before activating the
	// connection; the client stops once Connect gives up.
	deadline := time.Now().Add(5 * time.Second)
	for cli.engine.Failure() == nil {
		if time.Now().After(deadline) {
			close(log.release)
			t.Fatal("the session-loss rewrite did not fail")
		}
		time.Sleep(time.Millisecond)
	}
	close(log.release)
	if err := <-connected; !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Connect returned %v after the client stopped", err)
	}
	select {
	case <-failures:
	case <-time.After(5 * time.Second):
		t.Fatal("the failed write did not stop the client")
	}
	select {
	case <-up:
		t.Fatal("OnConnectionUp fired for a connection of a stopped client")
	default:
	}
	if cli.Connected() {
		t.Fatal("a stopped client is connected")
	}
}

// failDeleteStore fails every Delete.
type failDeleteStore struct{ *session.MemoryStore }

func (s *failDeleteStore) Delete(context.Context, session.RecordKey) error {
	return errors.New("disk full")
}

// A store failure while Connect is still connecting the publisher pool
// stops the client for good: Connect returns the failure and starts no
// supervisor for the ended span.
func TestStoreFailureWhilePoolConnects(t *testing.T) {
	st := &failDeleteStore{session.NewMemoryStore()}
	storedPublish(t, st.MemoryStore, 1, 1, "replayed")
	poolConnecting, stopped := make(chan struct{}), make(chan struct{})
	b := testbroker.New(t,
		func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
			p := c.Expect(wire.PUBLISH, 0)
			select {
			case <-poolConnecting:
			case <-c.Gone():
				return
			}
			c.Puback(p.PacketID, wire.ReasonSuccess) // its record's delete fails
			c.ServeAuto()
		},
		func(c *testbroker.Conn) { // the pool member
			c.Expect(wire.CONNECT, 0)
			close(poolConnecting)
			select {
			case <-stopped:
			case <-c.Gone():
				return
			}
			_ = c.Write(func(w io.Writer) (int64, error) { return wire.WriteConnack(w, wire.ConnackOpts{}) })
			c.ServeAuto()
		})
	cli, err := New(WithBroker(b.URL()), WithClientID("pool-failure"), WithPublisherPool(2), WithStore(st),
		WithoutKeepAlive(), WithLogger(quietLogger()), WithOnStoreFailure(func(error) { close(stopped) }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Connect returned %v after the client stopped", err)
	}
	if cli.Connected() {
		t.Fatal("a stopped client is connected")
	}
	// No supervisor of the ended span may be left running.
	done := make(chan struct{})
	go func() {
		cli.life.Load().running.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a supervisor runs after the client stopped")
	}
}
