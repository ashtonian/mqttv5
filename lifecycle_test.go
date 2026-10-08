// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
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

	received := make(chan ConnackInfo, 1)
	cli, err := New(
		WithBroker(fb.URL()),
		WithOnConnectionUp(func(info ConnackInfo) {
			received <- info
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
	case info := <-received:
		if info.AssignedClientID != "broker-assigned-id" {
			t.Errorf("AssignedClientID = %q, want broker-assigned-id", info.AssignedClientID)
		}
		if info.MaximumQoS != 1 {
			t.Errorf("MaximumQoS = %d, want 1", info.MaximumQoS)
		}
		if info.ReceiveMaximum != 65535 || !info.RetainAvailable || !info.SharedSubscriptionAvailable {
			t.Errorf("defaults not applied: %+v", info)
		}
		if got, ok := cli.ServerInfo(); !ok || got.AssignedClientID != info.AssignedClientID {
			t.Errorf("ServerInfo() = %+v, %v", got, ok)
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

	cli.life.Load().supervisor.Wait()
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
			WithOnConnectionUp(func(ConnackInfo) { upCount.Add(1) }),
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
		WithOnServerDisconnect(func(d DisconnectInfo) {
			if d.ServerReference != "" {
				_ = cli.SetBrokers(d.ServerReference)
			}
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

// A Disconnect racing Connect, started the moment the connection is up,
// either lets the supervisor start first and waits for it, or keeps it
// from starting: nothing of the span is left running either way.
func TestDisconnectRacingConnectLeavesNothingRunning(t *testing.T) {
	check := noLeaks(t)
	b := testbroker.New(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				if !connectAndDisconnectAtOnce(t, b.URL()) {
					return
				}
			}
		})
	}
	wg.Wait()
	b.Close()
	check()
}

func connectAndDisconnectAtOnce(t *testing.T, url string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stopped := make(chan error, 1)
	var cli *Client
	cli, err := New(WithBroker(url), WithoutKeepAlive(), WithLogger(quietLogger()),
		WithOnConnectionUp(func(ConnackInfo) {
			go func() { stopped <- cli.Disconnect(ctx) }()
		}))
	if err != nil {
		t.Error(err)
		return false
	}
	if err := cli.Connect(ctx); err != nil && !errors.Is(err, ErrClosed) {
		t.Errorf("Connect: %v", err)
		return false
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("Disconnect: %v", err)
			return false
		}
	case <-ctx.Done():
		t.Error("Disconnect did not return")
		return false
	}
	select {
	case <-cli.life.Load().finished:
	default:
		t.Error("Disconnect returned before the teardown finished")
		return false
	}
	if cli.Connected() {
		t.Error("connected after Disconnect")
		return false
	}
	return true
}

// Disconnect does not wait out a reconnect's handshake: the attempt
// ends with the span, and its failure is not reported as a connect error.
func TestDisconnectEndsAReconnectAwaitingConnack(t *testing.T) {
	stalled := make(chan struct{})
	var once sync.Once
	b := testbroker.New(t, func(c *testbroker.Conn) {
		// Returning drops the connection.
		c.AcceptConnect(wire.ConnackOpts{})
	})
	b.SetFallback(func(c *testbroker.Conn) {
		c.Expect(wire.CONNECT, 0)
		once.Do(func() { close(stalled) })
		c.Hold(0)
	})
	var connectErrors atomic.Int32
	cli, err := New(WithBroker(b.URL()), WithoutKeepAlive(), WithLogger(quietLogger()),
		WithConnectTimeout(time.Minute), WithReconnectBackoff(ConstantBackoff(time.Millisecond)),
		WithOnConnectError(func(error) { connectErrors.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stalled:
	case <-ctx.Done():
		t.Fatal("no reconnect attempt")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- cli.Disconnect(ctx) }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Disconnect: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Disconnect waited for the reconnect handshake")
	}
	if n := connectErrors.Load(); n != 0 {
		t.Fatalf("OnConnectError fired %d times for the attempt Disconnect ended", n)
	}
}

// Cancelling Connect's ctx while it waits for the CONNACK ends the
// handshake with ctx's error, well within the connect timeout.
func TestConnectCancelledAwaitingConnack(t *testing.T) {
	sent := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.Expect(wire.CONNECT, 0)
		close(sent)
		c.Hold(0)
	})
	cli, err := New(WithBroker(b.URL()), WithoutKeepAlive(), WithLogger(quietLogger()),
		WithConnectTimeout(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- cli.Connect(ctx) }()
	select {
	case <-sent:
	case <-time.After(10 * time.Second):
		t.Fatal("no CONNECT")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Connect = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Connect ignored the cancelled ctx")
	}
	if cli.Connected() {
		t.Fatal("connected after a cancelled Connect")
	}
}
