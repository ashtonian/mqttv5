// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

func TestOnConnectionUpReceivesConnack(t *testing.T) {
	maxQoS := byte(1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode:               wire.ReasonSuccess,
			AssignedClientIdentifier: "broker-assigned-id",
			MaximumQoS:               &maxQoS,
		})
		<-fb.Done()
	})

	received := make(chan *wire.Connack, 1)
	cli, err := New(
		WithBroker(fb.URL()),
		WithOnConnectionUp(func(ack *wire.Connack) {
			received <- ack
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case ack := <-received:
		if ack == nil {
			t.Fatal("OnConnectionUp received nil Connack")
		}
		assigned, _ := ack.Properties.String(wire.PropAssignedClientID)
		if assigned != "broker-assigned-id" {
			t.Errorf("AssignedClientIdentifier = %q, want broker-assigned-id", assigned)
		}
		qos, ok := ack.Properties.Byte(wire.PropMaximumQoS)
		if !ok || qos != 1 {
			t.Errorf("MaximumQoS = (%d, %v), want (1, true)", qos, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnectionUp not fired within 2s")
	}
}

func TestOnConnectErrorFiresOnFailedReconnect(t *testing.T) {
	first := atomic.Bool{}
	var fbMu sync.Mutex
	var fbRef *fakeBroker
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		if first.CompareAndSwap(false, true) {
			fbMu.Lock()
			r := fbRef
			fbMu.Unlock()
			if r != nil {
				r.Close()
			}
		}
		_ = c.Close()
	})
	fbMu.Lock()
	fbRef = fb
	fbMu.Unlock()

	errCount := atomic.Int32{}
	cli, err := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(50*time.Millisecond)),
		WithConnectTimeout(200*time.Millisecond),
		WithOnConnectError(func(err error) {
			errCount.Add(1)
		}),
		WithOnConnectionDown(func() bool { return true }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if errCount.Load() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := errCount.Load(); got < 2 {
		t.Fatalf("OnConnectError fired %d times, want >= 2", got)
	}
}

func TestOnConnectionDownReturnFalseStopsSupervisor(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		_ = c.Close()
	})

	downCount := atomic.Int32{}
	cli, err := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
		WithConnectTimeout(200*time.Millisecond),
		WithOnConnectionDown(func() bool {
			downCount.Add(1)
			return false
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if downCount.Load() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if downCount.Load() < 1 {
		t.Fatal("OnConnectionDown never fired")
	}

	cli.supWg.Wait()
	if cli.Connected() {
		t.Fatal("Client still reports Connected after supervisor exit")
	}

	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Second Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())
}

func TestCleanStartOnReconnect(t *testing.T) {
	cleanStarts := make(chan bool, 4)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			pkt.Release()
			return
		}
		cleanStarts <- conn.CleanStart
		conn.Release()
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		time.Sleep(10 * time.Millisecond)
	})

	cli, err := New(
		WithBroker(fb.URL()),
		WithCleanStart(true),
		WithCleanStartOnReconnect(false),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
		WithConnectTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	got := make([]bool, 0, 3)
	timeout := time.After(3 * time.Second)
	for len(got) < 3 {
		select {
		case v := <-cleanStarts:
			got = append(got, v)
		case <-timeout:
			t.Fatalf("only captured %d CONNECTs in 3s: %v", len(got), got)
		}
	}
	if !got[0] {
		t.Errorf("first CONNECT.CleanStart = false, want true (initial)")
	}
	for i := 1; i < len(got); i++ {
		if got[i] {
			t.Errorf("reconnect[%d] CONNECT.CleanStart = true, want false", i)
		}
	}
}

func TestWithoutKeepAliveDisablesPingLoop(t *testing.T) {
	sink := make(chan *wire.Connect, 1)
	fb := newFakeBroker(t, connectInspector(t, sink, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}))

	cli, err := New(WithBroker(fb.URL()), WithoutKeepAlive())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	conn := <-sink
	defer conn.Release()
	if conn.KeepAlive != 0 {
		t.Errorf("CONNECT KeepAlive = %d, want 0", conn.KeepAlive)
	}
}

func TestWithKeepAliveZeroRejected(t *testing.T) {
	_, err := New(WithBroker("mqtt://127.0.0.1:1"), WithKeepAlive(0))
	if err == nil {
		t.Fatal("New(WithKeepAlive(0)) succeeded; want error")
	}
	if !strings.Contains(err.Error(), "WithKeepAlive(0)") {
		t.Errorf("err = %v, want mention of WithKeepAlive(0)", err)
	}
}

// TestDisconnectThenConnectCleanCycle verifies a Client can be torn
// down via Disconnect and then reconnected via Connect without
// dangling state. Catches lifecycle bugs in started / shutdown /
// supervisor reset.
func TestDisconnectThenConnectCleanCycle(t *testing.T) {
	connCount := atomic.Int32{}
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		connCount.Add(1)
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	if !cli.Connected() {
		t.Fatal("not connected after first Connect")
	}
	if err := cli.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if cli.Connected() {
		t.Fatal("still reports connected after Disconnect")
	}

	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())
	if !cli.Connected() {
		t.Fatal("not connected after second Connect")
	}
	if got := connCount.Load(); got != 2 {
		t.Errorf("broker accepted %d CONNECTs, want 2", got)
	}
}

// TestClientGroupCallbacksFirePerMember verifies that lifecycle
// callbacks fire per member (cardinality only — we don't carry
// member identity through the callback today).
func TestClientGroupCallbacksFirePerMember(t *testing.T) {
	const memberCount = 3
	brokers := make([]*fakeBroker, memberCount)
	urls := make([]string, memberCount)
	for i := range memberCount {
		brokers[i] = newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
			defer c.Close()
			dec := wire.NewDecoder(c)
			pkt, _ := dec.ReadPacket()
			if pkt != nil {
				pkt.Release()
			}
			_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
			<-fb.Done()
		})
		urls[i] = brokers[i].URL()
	}

	var upCount atomic.Int32
	members := make([]GroupMember, memberCount)
	for i, u := range urls {
		members[i] = GroupMember{Broker: u}
	}
	group, err := NewClientGroup(members,
		WithGroupSharedOpts(
			WithClientID("group-callback-test"),
			WithOnConnectionUp(func(_ *wire.Connack) { upCount.Add(1) }),
		),
	)
	if err != nil {
		t.Fatalf("NewClientGroup: %v", err)
	}
	if err := group.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer group.Disconnect(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if upCount.Load() == memberCount {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("OnConnectionUp fired %d times, want %d", upCount.Load(), memberCount)
}

// TestSetBrokersAfterServerMoved verifies the canonical broker-
// redirect flow: server sends DISCONNECT with ServerReference,
// OnServerDisconnect calls SetBrokers, supervisor reconnects to the
// new broker on the next attempt.
func TestSetBrokersAfterServerMoved(t *testing.T) {
	moved := atomic.Bool{}
	target := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		moved.Store(true)
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		<-fb.Done()
	})

	origin := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		// Send a server DISCONNECT with ServerReference pointing at target.
		_, _ = wire.WriteDisconnect(c, wire.DisconnectOpts{
			ReasonCode:      wire.ReasonServerMoved,
			ServerReference: target.URL(),
		})
		<-fb.Done()
	})

	var cli *Client
	cli, err := New(
		WithBroker(origin.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
		WithConnectTimeout(200*time.Millisecond),
		WithOnServerDisconnect(func(d *wire.Disconnect) {
			ref, ok := d.Properties.String(wire.PropServerReference)
			if !ok {
				return
			}
			_ = cli.SetBrokers(ref)
		}),
		WithOnConnectionDown(func() bool { return true }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if moved.Load() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("client never reconnected to ServerReference target")
}

