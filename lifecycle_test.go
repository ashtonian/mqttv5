// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

func TestClientConnect(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		// Hold the connection open so the read loop is happy.
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()), WithClientID("test-client"))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !cli.Connected() {
		t.Fatal("Connected() = false after Connect")
	}
	if err := cli.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if cli.Connected() {
		t.Fatal("Connected() = true after Disconnect")
	}
}

func TestClientConnectRefused(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		pkt.Release()
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode: wire.ReasonBanned,
		})
	})

	cli, _ := New(WithBroker(fb.URL()))
	err := cli.Connect(context.Background())
	if !errors.Is(err, ErrConnectRefused) {
		t.Fatalf("got %v, want ErrConnectRefused", err)
	}
}

func TestClientDoubleConnect(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	err := cli.Connect(context.Background())
	if !errors.Is(err, ErrAlreadyConnected) {
		t.Fatalf("got %v, want ErrAlreadyConnected", err)
	}
}

func TestDialFailure(t *testing.T) {
	cli, _ := New(WithBroker("mqtt://127.0.0.1:1"), WithConnectTimeout(200*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := cli.Connect(ctx); err == nil {
		t.Fatal("expected dial failure")
	}
}

func TestOnServerDisconnect(t *testing.T) {
	// Broker accepts the CONNECT, then immediately sends a
	// DISCONNECT with ServerMoved + ServerReference so the test can
	// verify both the callback firing and the ability to redirect
	// via SetBrokers.
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		_, _ = wire.WriteDisconnect(c, wire.DisconnectOpts{
			ReasonCode:      wire.ReasonServerMoved,
			ServerReference: "mqtt://elsewhere.example:1883",
			ReasonString:    "moved",
		})
		<-fb.Done()
	})

	var (
		got      atomic.Pointer[DisconnectInfo]
		fired    atomic.Int32
		callDone = make(chan struct{}, 1)
	)
	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("server-disco-test"),
		WithOnServerDisconnect(func(d DisconnectInfo) {
			got.Store(&d)
			fired.Add(1)
			select {
			case callDone <- struct{}{}:
			default:
			}
		}),
		// Tight backoff so the supervisor races to reconnect, which
		// we don't want — fail any further dial since we redirected
		// to a bogus address. The test only cares about the first
		// callback firing.
		WithReconnectBackoff(ConstantBackoff(50*time.Millisecond)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case <-callDone:
	case <-time.After(2 * time.Second):
		t.Fatal("OnServerDisconnect callback not fired")
	}

	d := got.Load()
	if d == nil {
		t.Fatal("callback received nil")
	}
	if d.ReasonCode != wire.ReasonServerMoved {
		t.Fatalf("ReasonCode = 0x%02X, want ReasonServerMoved", byte(d.ReasonCode))
	}
	if d.ServerReference != "mqtt://elsewhere.example:1883" {
		t.Fatalf("ServerReference = %q, want elsewhere", d.ServerReference)
	}
	if fired.Load() != 1 {
		t.Fatalf("callback fired %d times, want 1", fired.Load())
	}
}

func TestOnConnectionUpReceivesConnack(t *testing.T) {
	maxQoS := byte(1)
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
	var fbRef *testbroker.Broker
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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

	cli.life.Load().running.Wait()
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
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
	fb := testbroker.NewRaw(t, connectInspector(t, sink, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}))

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
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
	brokers := make([]*testbroker.Broker, memberCount)
	urls := make([]string, memberCount)
	for i := range memberCount {
		brokers[i] = testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
		members[i] = GroupMember{Brokers: []string{u}}
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
	target := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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

	origin := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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

// A Disconnect during Connect's handshake cancels it: Connect returns
// ErrClosed, and a CONNACK arriving later binds nothing.
func TestDisconnectCancelsAConnectAwaitingConnack(t *testing.T) {
	sent, held := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(held) })
	first := testbroker.New(t, func(c *testbroker.Conn) {
		c.Expect(wire.CONNECT, 0)
		close(sent)
		<-held
		_ = c.Write(func(w io.Writer) (int64, error) { return wire.WriteConnack(w, wire.ConnackOpts{}) })
		c.Hold(0)
	})
	t.Cleanup(release)
	next := testbroker.New(t)
	cli, err := New(WithBroker(first.URL()), WithClientID("span"), WithoutKeepAlive(), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	old := make(chan error, 1)
	go func() { old <- cli.Connect(ctx) }()
	<-sent
	if err := cli.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	// The broker still holds the CONNACK: only the cancellation can
	// have ended Connect.
	select {
	case err := <-old:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("the cancelled Connect returned %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect kept waiting for the CONNACK after Disconnect")
	}
	release()
	if err := cli.SetBrokers(next.URL()); err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(ctx)
	if err := cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 1, Payload: []byte("after")}); err != nil {
		t.Fatalf("QoS 1 on the next span: %v", err)
	}
}

// A connection Connect activated is joined by Disconnect even when the
// span ends before its supervisor starts: Disconnect returns only once
// its read loop, which runs SubscribeCallback handlers, has exited.
func TestDisconnectBeforeSupervisorJoinsTheConnection(t *testing.T) {
	poolSent, poolHeld := make(chan struct{}), make(chan struct{})
	entered, held := make(chan struct{}), make(chan struct{})
	releasePool := sync.OnceFunc(func() { close(poolHeld) })
	release := sync.OnceFunc(func() { close(held) })
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		_ = c.Publish(wire.PublishOpts{Topic: "t", Payload: []byte("held")})
		c.Hold(0)
	}, func(c *testbroker.Conn) {
		// The publisher pool's first member: its handshake keeps
		// Connect from starting the supervisor.
		c.Expect(wire.CONNECT, 0)
		close(poolSent)
		<-poolHeld
	})
	t.Cleanup(releasePool)
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cli *Client
	cli, err := New(WithBroker(b.URL()), WithClientID("joined"), WithoutKeepAlive(), WithPublisherPool(2), WithLogger(quietLogger()),
		WithOnConnectionUp(func(ConnackInfo) {
			go func() {
				_, err := cli.SubscribeCallback(ctx, []TopicFilter{{Topic: "t"}}, func(*Message) {
					close(entered)
					<-held
				})
				if err != nil {
					t.Error(err)
				}
			}()
		}))
	if err != nil {
		t.Fatal(err)
	}
	connected := make(chan error, 1)
	go func() { connected <- cli.Connect(ctx) }()
	<-poolSent
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- cli.Disconnect(ctx) }()
	go func() {
		time.Sleep(100 * time.Millisecond)
		release()
	}()
	select {
	case <-stopped:
		select {
		case <-held:
		default:
			t.Fatal("Disconnect returned while a SubscribeCallback handler was running")
		}
	case <-ctx.Done():
		t.Fatal("Disconnect did not return")
	}
	releasePool()
	if err := <-connected; !errors.Is(err, ErrClosed) {
		t.Fatalf("Connect = %v, want ErrClosed", err)
	}
}

// heldMetaStore holds its first SetMeta until release.
type heldMetaStore struct {
	*session.MemoryStore
	entered chan struct{}
	held    chan struct{}
	once    sync.Once
}

func (s *heldMetaStore) SetMeta(ctx context.Context, m session.Meta) error {
	s.once.Do(func() {
		close(s.entered)
		<-s.held
	})
	return s.MemoryStore.SetMeta(ctx, m)
}

// A Disconnect while Connect binds the session engine to its new
// connection waits for Connect, which then gives the connection up: the
// next span's connection keeps the engine.
func TestDisconnectWaitsForAConnectBindingItsConnection(t *testing.T) {
	st := &heldMetaStore{MemoryStore: session.NewMemoryStore(), entered: make(chan struct{}), held: make(chan struct{})}
	release := sync.OnceFunc(func() { close(st.held) })
	t.Cleanup(release)
	first := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Hold(0)
	})
	next := testbroker.New(t)
	cli, err := New(WithBroker(first.URL()), WithClientID("binding"), WithStore(st), WithoutKeepAlive(), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	old := make(chan error, 1)
	go func() { old <- cli.Connect(ctx) }()
	<-st.entered
	stopped := make(chan error, 1)
	go func() { stopped <- cli.Disconnect(ctx) }()
	select {
	case <-stopped:
		t.Fatal("Disconnect returned while Connect was binding its connection")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if err := <-old; !errors.Is(err, ErrClosed) {
		t.Fatalf("the stopped Connect returned %v, want ErrClosed", err)
	}
	if err := cli.SetBrokers(next.URL()); err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(ctx)
	if err := cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 1, Payload: []byte("after")}); err != nil {
		t.Fatalf("QoS 1 on the next span: %v", err)
	}
}

// connectInspector returns a raw testbroker handler that captures the first
// CONNECT packet's decoded form into the returned channel. After
// CONNACK, the connection is held open until the broker is closed so
// the test can run normal lifecycle teardown.
func connectInspector(t *testing.T, sink chan<- *wire.Connect, connack wire.ConnackOpts) func(*testbroker.Broker, net.Conn) {
	return func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			t.Errorf("CONNECT read: %v", err)
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			t.Errorf("got %s, want CONNECT", pkt.Type())
			pkt.Release()
			return
		}
		sink <- conn
		if _, err := wire.WriteConnack(c, connack); err != nil {
			t.Errorf("CONNACK write: %v", err)
			return
		}
		<-fb.Done()
	}
}

func TestConnectExposesV5Properties(t *testing.T) {
	sink := make(chan *wire.Connect, 1)
	fb := testbroker.NewRaw(t, connectInspector(t, sink, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}))

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("connect-props-test"),
		WithMaximumPacketSize(1024*1024),
		WithInboundTopicAliasMaximum(32),
		WithRequestResponseInformation(true),
		WithRequestProblemInformation(true),
		WithConnectUserProperty("tenant", "acme"),
		WithConnectUserProperty("region", "us-east-1"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	conn := <-sink
	defer conn.Release()

	maxSize, ok := conn.Properties.Uint32(wire.PropMaximumPacketSize)
	if !ok || maxSize != 1024*1024 {
		t.Errorf("MaximumPacketSize = (%d, %v), want (1048576, true)", maxSize, ok)
	}
	aliasMax, ok := conn.Properties.Uint16(wire.PropTopicAliasMaximum)
	if !ok || aliasMax != 32 {
		t.Errorf("TopicAliasMaximum = (%d, %v), want (32, true)", aliasMax, ok)
	}
	rri, ok := conn.Properties.Byte(wire.PropRequestResponseInfo)
	if !ok || rri != 1 {
		t.Errorf("RequestResponseInformation = (%d, %v), want (1, true)", rri, ok)
	}
	// Requested (the default): the property is omitted, which means 1.
	if rpi, ok := conn.Properties.Byte(wire.PropRequestProblemInfo); ok && rpi != 1 {
		t.Errorf("RequestProblemInformation = %d, want absent or 1", rpi)
	}
	props := map[string]string{}
	for k, v := range conn.Properties.UserProperties() {
		props[k] = v
	}
	if props["tenant"] != "acme" || props["region"] != "us-east-1" {
		t.Errorf("UserProperties = %+v, want tenant=acme region=us-east-1", props)
	}
}

func TestConnectPacketBuilderMutates(t *testing.T) {
	sink := make(chan *wire.Connect, 1)
	fb := testbroker.NewRaw(t, connectInspector(t, sink, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}))

	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("from-config"),
		WithConnectPacketBuilder(func(ctx context.Context, opts *ConnectOptions) error {
			opts.Username = "from-builder"
			opts.Password = []byte("rotated-token")
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	conn := <-sink
	defer conn.Release()
	if conn.Username != "from-builder" {
		t.Errorf("Username = %q, want from-builder", conn.Username)
	}
	if string(conn.Password) != "rotated-token" {
		t.Errorf("Password = %q, want rotated-token", conn.Password)
	}
}

func TestConnectPacketBuilderErrorFailsAttempt(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		_, _ = c.Read(make([]byte, 64))
	})
	wantErr := errors.New("builder rejected the attempt")
	cli, err := New(
		WithBroker(fb.URL()),
		WithConnectPacketBuilder(func(ctx context.Context, opts *ConnectOptions) error {
			return wantErr
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = cli.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect succeeded but ConnectPacketBuilder returned an error")
	}
	if !strings.Contains(err.Error(), wantErr.Error()) {
		t.Errorf("Connect err = %v, want one wrapping %v", err, wantErr)
	}
}

// TestConnectPacketBuilderRotatesCredentialsAcrossAttempts verifies the
// headline use case the godoc advertises: a builder that returns a
// fresh credential per attempt should produce distinct CONNECT
// Username values across reconnects.
func TestConnectPacketBuilderRotatesCredentialsAcrossAttempts(t *testing.T) {
	captured := make(chan string, 8)
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
		// conn.Username aliases the pooled frame buffer; clone so the
		// string survives Release + frame reuse on the next CONNECT.
		username := strings.Clone(conn.Username)
		conn.Release()
		captured <- username
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		// Drop quickly so the supervisor reconnects.
		time.Sleep(10 * time.Millisecond)
	})

	var counter atomic.Int32
	cli, err := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
		WithConnectTimeout(200*time.Millisecond),
		WithConnectPacketBuilder(func(ctx context.Context, opts *ConnectOptions) error {
			n := counter.Add(1)
			opts.Username = fmt.Sprintf("token-%d", n)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	seen := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(seen) < 2 {
		select {
		case u := <-captured:
			seen[u] = true
		case <-deadline:
			t.Fatalf("captured %d distinct usernames in 3s, want >= 2; seen=%v",
				len(seen), seen)
		}
	}
	if !seen["token-1"] {
		keys := make([]string, 0, len(seen))
		for k := range seen {
			keys = append(keys, fmt.Sprintf("%q", k))
		}
		t.Errorf("first attempt didn't carry token-1; keys=%v", keys)
	}
}

func TestValidateBrokerURLsRejectsBad(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"empty", ""},
		{"no scheme", "broker.example:1883"},
		{"unsupported scheme", "http://broker.example:1883"},
		{"missing host", "mqtt://"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(WithBroker(tc.url))
			if err == nil {
				t.Fatalf("New(WithBroker(%q)) succeeded; want error", tc.url)
			}
		})
	}
}

func TestValidateBrokerURLsAllowsDialFuncScheme(t *testing.T) {
	dialFn := func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		return nil, errors.New("not used")
	}
	_, err := New(
		WithBroker("custom://anything"),
		WithDialFunc(dialFn),
	)
	if err != nil {
		t.Fatalf("New with custom scheme + DialFunc: %v", err)
	}
}

func TestWithDialFuncNilRejected(t *testing.T) {
	_, err := New(
		WithBroker("mqtt://127.0.0.1:1"),
		WithDialFunc(nil),
	)
	if err == nil {
		t.Fatal("WithDialFunc(nil) accepted; want error")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Errorf("err = %v, want mention of nil", err)
	}
}

// TestClientIDReportsAssignedID verifies that Client.ClientID()
// returns the broker-assigned identifier from CONNACK when the
// caller passed WithClientID("").
func TestClientIDReportsAssignedID(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode:               wire.ReasonSuccess,
			AssignedClientIdentifier: "broker-assigned-xyz",
		})
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()), WithClientID(""))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cli.ClientID() == "broker-assigned-xyz" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("ClientID() = %q, want broker-assigned-xyz", cli.ClientID())
}

// With WithRetryInitialConnect a client starts while the
// broker refuses it and connects once the broker accepts.
func TestRetryInitialConnect(t *testing.T) {
	refuse := func(c *testbroker.Conn) { c.Close() }
	b := testbroker.New(t, refuse, refuse, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	var failures int
	cli, err := New(WithBroker(b.URL()), WithClientID("retry"), WithLogger(quietLogger()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)), WithRetryInitialConnect(),
		WithOnConnectError(func(error) { failures++ }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect with the broker refusing: %v", err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	if err := cli.AwaitConnection(ctx); err != nil {
		t.Fatalf("AwaitConnection: %v", err)
	}
	if failures != 2 {
		t.Fatalf("%d connect errors reported, want 2", failures)
	}
	if _, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "up"}}); err != nil {
		t.Fatalf("Subscribe once connected: %v", err)
	}
}

// Without the option Connect fails fast, as before.
func TestInitialConnectFailsFast(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) { c.Close() })
	cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded against a refusing broker")
	}
	if cli.Connected() {
		t.Fatal("client reports connected")
	}
}

// AwaitConnection ends with ErrClosed when the client is disconnected
// while it waits, and with ctx.
func TestAwaitConnectionEnds(t *testing.T) {
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) { c.Close() })
	cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()), WithRetryInitialConnect(),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := cli.AwaitConnection(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AwaitConnection = %v, want the deadline", err)
	}
	done := make(chan error, 1)
	go func() { done <- cli.AwaitConnection(context.Background()) }()
	if err := cli.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("AwaitConnection = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AwaitConnection still waiting after Disconnect")
	}
}

// TestDisconnectWithCarriesOpts verifies the DisconnectWith path sends
// the supplied DisconnectOpts on the wire (reason code + ReasonString
// + SessionExpiryInterval override).
func TestDisconnectWithCarriesOpts(t *testing.T) {
	got := make(chan *wire.Disconnect, 1)
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			if d, ok := pkt.(*wire.Disconnect); ok {
				got <- d.Clone()
				pkt.Release()
				return
			}
			pkt.Release()
		}
	})

	cli, err := New(WithBroker(fb.URL()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	expiry := uint32(0)
	if err := cli.DisconnectWith(context.Background(), DisconnectOptions{
		ReasonCode:            wire.ReasonAdministrativeAction,
		ReasonString:          "test teardown",
		SessionExpiryInterval: &expiry,
	}); err != nil {
		t.Fatalf("DisconnectWith: %v", err)
	}

	select {
	case d := <-got:
		if d.ReasonCode != wire.ReasonAdministrativeAction {
			t.Errorf("ReasonCode = 0x%02X, want 0x%02X",
				byte(d.ReasonCode), byte(wire.ReasonAdministrativeAction))
		}
		rs, _ := d.Properties.String(wire.PropReasonString)
		if rs != "test teardown" {
			t.Errorf("ReasonString = %q, want test teardown", rs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker never received DISCONNECT")
	}
}

// Callbacks run one at a time in the order posted, and flush waits for
// those posted before it unless stop closes first.
func TestEventsRunInOrder(t *testing.T) {
	var q events
	var got []int
	held := make(chan struct{})
	q.post(func() { <-held })
	for i := range 100 {
		q.post(func() { got = append(got, i) })
	}
	stop := make(chan struct{})
	close(stop)
	if q.flush(stop) {
		t.Fatal("flush reported the queue flushed while a callback was held")
	}
	close(held)
	if !q.flush(nil) {
		t.Fatal("flush did not complete")
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("callback %d ran as %d", v, i)
		}
	}
	if len(got) != 100 {
		t.Fatalf("%d of 100 callbacks ran", len(got))
	}
}

// Every lifecycle callback may call Disconnect: it returns, and the
// client ends disconnected.
func TestDisconnectFromALifecycleCallback(t *testing.T) {
	refuse := func(rc wire.ReasonCode, ref string) testbroker.Script {
		return func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{ReasonCode: rc, ServerReference: ref})
		}
	}
	held := func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	}
	drop := func(_ *testing.T, _ *Client, b *testbroker.Broker) { b.Conn(0, time.Second).Close() }
	for _, tc := range []struct {
		name     string
		first    testbroker.Script // nil: the broker's default
		fallback testbroker.Script
		opts     func(stop func()) []Option
		trigger  func(*testing.T, *Client, *testbroker.Broker)
	}{
		{name: "OnConnectionUp", opts: func(stop func()) []Option {
			return []Option{WithOnConnectionUp(func(ConnackInfo) { stop() })}
		}},
		{name: "OnConnectionUp after a reconnect", first: held, trigger: drop, opts: func(stop func()) []Option {
			var ups int
			return []Option{WithOnConnectionUp(func(ConnackInfo) {
				if ups++; ups == 2 {
					stop()
				}
			})}
		}},
		{name: "OnConnectionDown", first: held, trigger: drop, opts: func(stop func()) []Option {
			return []Option{WithOnConnectionDown(func() bool { stop(); return true })}
		}},
		{name: "OnServerDisconnect", first: func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			_ = c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonServerShuttingDown})
		}, opts: func(stop func()) []Option {
			return []Option{WithOnServerDisconnect(func(DisconnectInfo) { stop() })}
		}},
		{name: "OnReconnectAttempt", first: held, trigger: drop, opts: func(stop func()) []Option {
			return []Option{WithOnReconnectAttempt(func(int, string) { stop() })}
		}},
		{name: "OnConnectError", fallback: refuse(wire.ReasonNotAuthorized, ""), opts: func(stop func()) []Option {
			return []Option{WithRetryInitialConnect(), WithOnConnectError(func(error) { stop() })}
		}},
		{name: "OnServerRedirect", fallback: refuse(wire.ReasonServerMoved, "127.0.0.1:1"), opts: func(stop func()) []Option {
			return []Option{WithRetryInitialConnect(), WithOnServerRedirect(func(ServerRedirect) { stop() })}
		}},
		{name: "OnResubscribeError", first: func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			<-c.Gone()
		}, fallback: func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			p := c.Expect(wire.SUBSCRIBE, 0)
			_ = c.Suback(p.PacketID, wire.ReasonNotAuthorized)
			c.ServeAuto()
		}, trigger: func(t *testing.T, cli *Client, b *testbroker.Broker) {
			if _, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "acl/#"}}); err != nil {
				t.Fatal(err)
			}
			b.Conn(0, time.Second).Close()
		}, opts: func(stop func()) []Option {
			return []Option{WithOnResubscribeError(func(SubscriptionToken, error) { stop() })}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scripts []testbroker.Script
			if tc.first != nil {
				scripts = append(scripts, tc.first)
			}
			b := testbroker.New(t, scripts...)
			if tc.fallback != nil {
				b.SetFallback(tc.fallback)
			}
			var cli *Client
			var once sync.Once
			stopped := make(chan error, 1)
			stop := func() {
				once.Do(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					stopped <- cli.Disconnect(ctx)
				})
			}
			opts := append([]Option{WithBroker(b.URL()), WithClientID("callback-" + t.Name()), WithoutKeepAlive(),
				WithLogger(quietLogger()), WithReconnectBackoff(ConstantBackoff(10 * time.Millisecond))}, tc.opts(stop)...)
			var err error
			cli, err = New(opts...)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := cli.Connect(ctx); err != nil && !errors.Is(err, ErrClosed) {
				t.Fatalf("Connect: %v", err)
			}
			if tc.trigger != nil {
				tc.trigger(t, cli, b)
			}
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatalf("Disconnect: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("Disconnect called from the callback did not return")
			}
			if cli.Connected() {
				t.Fatal("connected after Disconnect")
			}
		})
	}
}

// OnStoreFailure may start the client again from what the store holds.
func TestConnectFromOnStoreFailure(t *testing.T) {
	st := &phaseFailStore{MemoryStore: session.NewMemoryStore(), phase: session.AwaitPubcomp}
	st.fail.Store(true)
	b := testbroker.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cli *Client
	restarted := make(chan error, 1)
	cli = tbClient(t, b, WithStore(st), WithOnStoreFailure(func(error) {
		st.fail.Store(false)
		restarted <- cli.Connect(ctx)
	}))
	if err := cli.Publish(ctx, PublishOptions{Topic: "s", QoS: 2, Payload: []byte("x")}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Publish returned %v, want the store failure", err)
	}
	select {
	case err := <-restarted:
		if err != nil {
			t.Fatalf("Connect from OnStoreFailure: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Connect from OnStoreFailure did not return")
	}
	if err := cli.AwaitConnection(ctx); err != nil {
		t.Fatal(err)
	}
}

// recordingLog keeps the messages logged at Warn and above.
type recordingLog struct {
	slog.Handler
	mu   sync.Mutex
	msgs []string
}

func (h *recordingLog) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (h *recordingLog) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *recordingLog) logged(prefix string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if strings.HasPrefix(m, prefix) {
			return true
		}
	}
	return false
}

// A teardown held up by a SubscribeCallback handler says so in the log,
// and completes once the handler returns.
func TestTeardownReportsAHandlerThatDoesNotReturn(t *testing.T) {
	clk := clock.NewFake(time.Now())
	log := &recordingLog{Handler: slog.Default().Handler()}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		_ = c.Publish(wire.PublishOpts{Topic: "t", Payload: []byte("held")})
		c.Hold(0)
	})
	cli, err := New(WithBroker(b.URL()), WithClientID("stall"), WithoutKeepAlive(), WithLogger(slog.New(log)), withClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	entered, held := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(held) })
	t.Cleanup(release)
	if _, err := cli.SubscribeCallback(ctx, []TopicFilter{{Topic: "t"}}, func(*Message) {
		close(entered)
		<-held
	}); err != nil {
		t.Fatal(err)
	}
	<-entered
	timers := clk.Pending()
	stopped := make(chan error, 1)
	go func() { stopped <- cli.Disconnect(ctx) }()
	if !clk.WaitPending(timers+1, 5*time.Second) {
		t.Fatal("the teardown did not start waiting for the handler")
	}
	clk.Advance(joinStallWarning)
	const warning = "mqttv5: disconnect is waiting for a SubscribeCallback handler"
	for deadline := time.Now().Add(5 * time.Second); !log.logged(warning); {
		if time.Now().After(deadline) {
			t.Fatal("no warning about the handler holding the teardown")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-stopped:
		t.Fatal("Disconnect returned while the handler was running")
	default:
	}
	release()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}

// TestReconnectAfterDrop simulates a broker that drops the connection
// after the handshake on the first attempt and accepts the second.
// Verifies the supervisor reconnects and Connected() returns true.
// TestMultiBrokerFailover verifies the supervisor rotates through the
// BrokerURLs list on reconnect. The first broker is dialled, drops the
// connection, the supervisor advances to the second broker for the
// next attempt.
func TestMultiBrokerFailover(t *testing.T) {
	connsA := atomic.Int32{}
	connsB := atomic.Int32{}
	acceptedOnB := make(chan struct{}, 1)

	fbA := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		connsA.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		// Drop right after the first CONNACK so the supervisor
		// rotates to broker B for the next attempt.
	})
	fbB := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		connsB.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		select {
		case acceptedOnB <- struct{}{}:
		default:
		}
		<-fb.Done()
	})

	cli, err := New(
		WithBrokers(fbA.URL(), fbB.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Force initial pick to be A. brokerIdx.Load() % 2 must equal 0.
	// Set to a known value then verify currentBrokerURL.
	cli.brokerIdx.Store(0)
	if cli.currentBrokerURL() != fbA.URL() {
		t.Fatalf("currentBrokerURL = %q, want %q", cli.currentBrokerURL(), fbA.URL())
	}

	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case <-acceptedOnB:
	case <-time.After(2 * time.Second):
		t.Fatalf("did not failover to broker B within 2s (connsA=%d connsB=%d)",
			connsA.Load(), connsB.Load())
	}
	// ServerInfo names the broker the client failed over to.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if info, ok := cli.ServerInfo(); ok && info.Broker == fbB.URL() {
			break
		}
		if time.Now().After(deadline) {
			info, _ := cli.ServerInfo()
			t.Fatalf("ServerInfo().Broker = %q, want %q", info.Broker, fbB.URL())
		}
		time.Sleep(5 * time.Millisecond)
	}

	if cli.currentBrokerURL() != fbB.URL() {
		t.Fatalf("after failover, currentBrokerURL = %q, want %q",
			cli.currentBrokerURL(), fbB.URL())
	}
}

// Connect moves past a broker that is down to the next one in the list.
func TestConnectTriesEachBroker(t *testing.T) {
	const down = "mqtt://127.0.0.1:1"
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		acceptConnect(t, c, wire.NewDecoder(c))
		<-fb.Done()
	})
	failures := make(chan error, 4)
	cli, err := New(
		WithBrokers(down, fb.URL()),
		WithStats(),
		WithLogger(quietLogger()),
		WithOnConnectError(func(err error) { failures <- err }),
	)
	if err != nil {
		t.Fatal(err)
	}
	cli.brokerIdx.Store(0)

	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if info, _ := cli.ServerInfo(); info.Broker != fb.URL() {
		t.Fatalf("ServerInfo().Broker = %q, want %q", info.Broker, fb.URL())
	}
	if n := cli.Stats().ConnectFailures; n != 1 {
		t.Fatalf("ConnectFailures = %d, want 1", n)
	}
	select {
	case err := <-failures:
		if !isDialError(err) {
			t.Fatalf("OnConnectError got %v, want a dial error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnectError not called for the broker that was down")
	}
}

// When no broker accepts, Connect returns every broker's error.
func TestConnectEveryBrokerFails(t *testing.T) {
	const down = "mqtt://127.0.0.1:1"
	refusing := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		acceptConnectWith(t, c, wire.NewDecoder(c), wire.ConnackOpts{ReasonCode: wire.ReasonNotAuthorized})
	})
	cli, err := New(WithBrokers(down, refusing.URL()), WithStats(), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	cli.brokerIdx.Store(0)

	err = cli.Connect(context.Background())
	if !isDialError(err) || !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("Connect = %v, want both brokers' errors", err)
	}
	for _, u := range []string{down, refusing.URL()} {
		if !strings.Contains(err.Error(), u) {
			t.Fatalf("Connect error %q does not name %s", err, u)
		}
	}
	if n := cli.Stats().ConnectFailures; n != 2 {
		t.Fatalf("ConnectFailures = %d, want 2", n)
	}
}

// Under FailoverPriority Connect starts at the first broker, wherever
// the list pointer was left.
func TestFailoverPriorityConnectStartsAtFirst(t *testing.T) {
	hold := func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		acceptConnect(t, c, wire.NewDecoder(c))
		<-fb.Done()
	}
	primary := testbroker.NewRaw(t, hold)
	standby := testbroker.NewRaw(t, hold)
	cli, err := New(WithBrokers(primary.URL(), standby.URL()), WithFailoverOrder(FailoverPriority))
	if err != nil {
		t.Fatal(err)
	}
	cli.brokerIdx.Store(1)

	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())
	if info, _ := cli.ServerInfo(); info.Broker != primary.URL() {
		t.Fatalf("ServerInfo().Broker = %q, want the primary %q", info.Broker, primary.URL())
	}
}

// Under FailoverPriority a reconnect goes back to the primary, where
// FailoverRotate moves on to the next broker.
func TestFailoverPriorityReconnectReturnsToPrimary(t *testing.T) {
	var primaryConns atomic.Int32
	primary := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		acceptConnect(t, c, wire.NewDecoder(c))
		if primaryConns.Add(1) == 1 {
			return // drop the first connection
		}
		<-fb.Done()
	})
	standby := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		acceptConnect(t, c, wire.NewDecoder(c))
		<-fb.Done()
	})
	ups := make(chan string, 4)
	cli, err := New(
		WithBrokers(primary.URL(), standby.URL()),
		WithFailoverOrder(FailoverPriority),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
		WithLogger(quietLogger()),
		WithOnConnectionUp(func(info ConnackInfo) { ups <- info.Broker }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	for i, want := range []string{primary.URL(), primary.URL()} {
		select {
		case got := <-ups:
			if got != want {
				t.Fatalf("connection %d went to %q, want the primary %q", i+1, got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("connection %d did not come up within 2s", i+1)
		}
	}
}

// Under FailoverPriority one reconnect attempt is a pass over the list:
// a primary that refuses the CONNECT and a broker that is down are tried
// back to back, without the backoff between them, before the standby
// takes the client. FailoverRotate would wait the backoff after the
// broker that is down.
func TestFailoverPriorityPassFallsThroughWithoutBackoff(t *testing.T) {
	const down = "mqtt://127.0.0.1:1"
	var primaryConns atomic.Int32
	primary := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		if primaryConns.Add(1) == 1 {
			acceptConnect(t, c, dec)
			return // drop the first connection
		}
		acceptConnectWith(t, c, dec, wire.ConnackOpts{ReasonCode: wire.ReasonServerUnavailable})
	})
	onStandby := make(chan struct{}, 1)
	standby := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		acceptConnect(t, c, wire.NewDecoder(c))
		select {
		case onStandby <- struct{}{}:
		default:
		}
		<-fb.Done()
	})

	type dial struct {
		attempt int
		broker  string
	}
	var mu sync.Mutex
	var dials []dial
	cli, err := New(
		WithBrokers(primary.URL(), down, standby.URL()),
		WithFailoverOrder(FailoverPriority),
		// No delay before the first reconnect attempt, an hour before any
		// later one: reaching the standby in time takes a single pass.
		WithReconnectBackoff(func(attempt int) time.Duration {
			if attempt == 0 {
				return 0
			}
			return time.Hour
		}),
		WithLogger(quietLogger()),
		WithOnReconnectAttempt(func(attempt int, broker string) {
			mu.Lock()
			dials = append(dials, dial{attempt, broker})
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case <-onStandby:
	case <-time.After(2 * time.Second):
		t.Fatal("the standby did not take the client within one pass")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []dial{{1, primary.URL()}, {1, down}, {1, standby.URL()}}
	if !slices.Equal(dials, want) {
		t.Fatalf("reconnect dials = %v, want %v", dials, want)
	}
}

func TestWithFailoverOrderRejectsUnknown(t *testing.T) {
	if _, err := New(WithBroker("mqtt://127.0.0.1:1883"), WithFailoverOrder(FailoverOrder(7))); err == nil {
		t.Fatal("New accepted FailoverOrder(7)")
	}
}

// A publisher pool's members fail over in the parent's order.
func TestPublisherPoolInheritsFailoverOrder(t *testing.T) {
	cli, err := New(
		WithBrokers("mqtt://127.0.0.1:1883", "mqtt://127.0.0.1:1884"),
		WithFailoverOrder(FailoverPriority),
		WithPublisherPool(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range cli.pool.members {
		if m.cfg.FailoverOrder != FailoverPriority {
			t.Fatalf("pool member %d FailoverOrder = %d, want FailoverPriority", i+1, m.cfg.FailoverOrder)
		}
	}
}

func isDialError(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

func TestReconnectAfterDrop(t *testing.T) {
	connNum := atomic.Int32{}
	secondAccepted := make(chan struct{}, 1)

	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		n := connNum.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		if n == 1 {
			// Drop right after CONNACK.
			return
		}
		select {
		case secondAccepted <- struct{}{}:
		default:
		}
		<-fb.Done()
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case <-secondAccepted:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not reconnect within 2s")
	}

	// Give the runConnection a moment to update Connected().
	deadline := time.Now().Add(time.Second)
	for !cli.Connected() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !cli.Connected() {
		t.Fatal("Connected() = false after reconnect")
	}
}

// TestResubscribeOnReconnect verifies the supervisor re-issues
// SUBSCRIBE after a reconnect so prior subscriptions remain active.
func TestResubscribeOnReconnect(t *testing.T) {
	subsSeen := make(chan uint16, 8)
	connNum := atomic.Int32{}

	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		n := connNum.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			if sub, ok := pkt.(*wire.Subscribe); ok {
				subsSeen <- sub.PacketID
				_, _ = wire.WriteSuback(c, wire.SubackOpts{
					PacketID:    sub.PacketID,
					ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
				})
				pkt.Release()
				if n == 1 {
					return // drop after first SUBSCRIBE
				}
				continue
			}
			pkt.Release()
		}
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if _, err := cli.SubscribeCallback(context.Background(),
		[]TopicFilter{{Topic: "topic/x", QoS: 1}},
		func(*Message) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// First subscribe.
	select {
	case <-subsSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not see initial SUBSCRIBE")
	}

	// After the drop + reconnect, resubscribeAll should re-issue the SUBSCRIBE.
	select {
	case <-subsSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not resubscribe after reconnect")
	}
}

// TestReplayQoS1OnReconnect verifies that a QoS 1 publish in-flight
// when the connection drops is resent with DUP=1 when the broker resumes
// the session, and the original Publish() call completes successfully.
func TestReplayQoS1OnReconnect(t *testing.T) {
	connNum := atomic.Int32{}
	publishesSeen := make(chan *publishSeen, 8)

	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		n := connNum.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnectWith(t, c, dec, wire.ConnackOpts{SessionPresent: n > 1})

		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			if pub, ok := pkt.(*wire.Publish); ok {
				publishesSeen <- &publishSeen{
					connNum: int(n),
					id:      pub.PacketID,
					dup:     pub.Dup,
					topic:   string([]byte(pub.Topic)),
				}
				id := pub.PacketID
				pkt.Release()
				if n == 1 {
					// First connection: read PUBLISH but DROP before PUBACK.
					return
				}
				// Second connection: ACK the replay.
				_, _ = wire.WritePuback(c, wire.PubRespOpts{PacketID: id})
				continue
			}
			pkt.Release()
		}
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	pubDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pubDone <- cli.Publish(ctx, PublishOptions{
			Topic:   "events/important",
			Payload: []byte("hello"),
			QoS:     1,
		})
	}()

	// First broker connection should see the PUBLISH (DUP=false).
	first, err := waitForPublish(publishesSeen, 2*time.Second)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if first.connNum != 1 || first.dup {
		t.Errorf("first publish: connNum=%d dup=%v (want 1, false)", first.connNum, first.dup)
	}

	// After reconnect, supervisor replays — DUP must be set.
	second, err := waitForPublish(publishesSeen, 3*time.Second)
	if err != nil {
		t.Fatalf("replay publish: %v", err)
	}
	if second.connNum < 2 || !second.dup {
		t.Errorf("replay: connNum=%d dup=%v (want >=2, true)", second.connNum, second.dup)
	}
	if second.id != first.id {
		t.Errorf("replay packet id changed: first=%d replay=%d", first.id, second.id)
	}

	// The original Publish() call must complete with no error.
	select {
	case err := <-pubDone:
		if err != nil {
			t.Errorf("Publish() returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish() did not complete after replay+PUBACK")
	}
}

// publishSeen is a per-connection record of a PUBLISH the broker saw.
type publishSeen struct {
	connNum int
	id      uint16
	dup     bool
	topic   string
}

func waitForPublish(ch <-chan *publishSeen, d time.Duration) (*publishSeen, error) {
	select {
	case p := <-ch:
		return p, nil
	case <-time.After(d):
		return nil, errors.New("timeout waiting for publish")
	}
}

// silence unused-import linter warnings on future test additions
var _ = io.EOF

func TestResolveServerReference(t *testing.T) {
	for _, tt := range []struct{ current, ref, want string }{
		{"mqtts://a.example:8883", "b.example:9883", "mqtts://b.example:9883"},
		{"mqtt://a.example", "b.example", "mqtt://b.example"},
		{"ws://a.example/mqtt", "b.example:80", "ws://b.example:80/mqtt"},
		{"mqtt://a.example", "mqtts://c.example:8883 d.example", "mqtts://c.example:8883"},
		{"mqtt://a.example", " b.example c.example ", "mqtt://b.example"},
	} {
		if got, err := resolveServerReference(tt.current, tt.ref); err != nil || got != tt.want {
			t.Errorf("resolve(%q, %q) = %q, %v; want %q", tt.current, tt.ref, got, err, tt.want)
		}
	}
	for _, ref := range []string{"", "b.example/path", "user@b.example"} {
		if got, err := resolveServerReference("mqtt://a.example", ref); err == nil {
			t.Errorf("resolve(%q) = %q, want an error", ref, got)
		}
	}
}

func hostOf(b *testbroker.Broker) string { return strings.TrimPrefix(b.URL(), "mqtt://") }

// 0x9D Server moved on CONNACK: the client follows and keeps the new
// broker for later reconnects.
func TestFollowServerMoved(t *testing.T) {
	target := testbroker.New(t)
	target.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	origin := testbroker.New(t)
	origin.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{ReasonCode: wire.ReasonServerMoved, ServerReference: hostOf(target)})
	})
	var got []ServerRedirect
	cli, err := New(WithBroker(origin.URL()), WithClientID("moved"), WithLogger(quietLogger()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)), WithRetryInitialConnect(),
		WithFollowServerRedirects(), WithOnServerRedirect(func(r ServerRedirect) { got = append(got, r) }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	if err := cli.AwaitConnection(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Permanent() || got[0].Packet != CONNACK {
		t.Fatalf("redirects reported %+v", got)
	}
	target.Conn(0, time.Second).Close()
	if c := target.Conn(1, 3*time.Second); c == nil {
		t.Fatal("the client did not reconnect to the broker it moved to")
	}
	if origin.Accepted() != 1 {
		t.Fatalf("the client went back to the old broker (%d connections)", origin.Accepted())
	}
}

// 0x9C Use another server on DISCONNECT: the next attempt only goes to
// the reference.
func TestFollowUseAnotherServerOnce(t *testing.T) {
	other := testbroker.New(t)
	other.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	origin := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonUseAnotherServer, ServerReference: hostOf(other)})
	})
	origin.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, origin, WithFollowServerRedirects())
	if c := other.Conn(0, 3*time.Second); c == nil {
		t.Fatal("the client did not use the other server")
	}
	if err := cli.AwaitConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	other.Conn(0, 0).Close()
	if c := origin.Conn(1, 3*time.Second); c == nil {
		t.Fatal("after the temporary redirect the client did not return to its broker")
	}
}

// Without WithFollowServerRedirects a redirect is reported, not followed.
func TestServerRedirectReportedNotFollowed(t *testing.T) {
	other := testbroker.New(t)
	origin := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonServerMoved, ServerReference: hostOf(other)})
	})
	origin.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	reported := make(chan ServerRedirect, 1)
	tbClient(t, origin, WithOnServerRedirect(func(r ServerRedirect) { reported <- r }))
	select {
	case r := <-reported:
		if r.Packet != DISCONNECT || r.Reference != hostOf(other) {
			t.Fatalf("reported %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("redirect not reported")
	}
	if c := origin.Conn(1, 3*time.Second); c == nil {
		t.Fatal("the client did not reconnect to its own broker")
	}
	if other.Accepted() != 0 {
		t.Fatal("the client followed the redirect")
	}
}

// A client used every way there is — every subscription kind, every
// QoS, a publisher pool, a QueuePublisher, Typed, a reconnect, and a
// second Connect — leaves no goroutine behind once it is disconnected.
func TestLifecycleLeavesNoGoroutines(t *testing.T) {
	check := noLeaks(t)
	b := testbroker.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := New(WithBroker(b.URL()), WithClientID("leak"), WithLogger(quietLogger()),
		WithPublisherPool(2), WithStats(), WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	for cycle := range 2 {
		if err := cli.Connect(ctx); err != nil {
			t.Fatalf("cycle %d: Connect: %v", cycle, err)
		}
		ch, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "leak/chan", QoS: 1}})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for m := range ch {
				_ = m.Ack()
			}
		}()
		if _, _, err := cli.SubscribeQueue(ctx, []TopicFilter{{Topic: "leak/queue", QoS: 2}}); err != nil {
			t.Fatal(err)
		}
		if _, err := cli.SubscribeCallback(ctx, []TopicFilter{{Topic: "leak/cb"}}, func(*Message) {}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := cli.SubscribeAs[string](ctx, []TopicFilter{{Topic: "leak/typed"}}, SubEncoding(testJSON{})); err != nil {
			t.Fatal(err)
		}
		for qos := byte(0); qos <= 2; qos++ {
			if err := cli.Publish(ctx, PublishOptions{Topic: "leak/pub", QoS: qos, Payload: []byte("x")}); err != nil {
				t.Fatalf("publish QoS %d: %v", qos, err)
			}
		}
		qp, err := NewQueuePublisher(cli, NewMemoryPublisherQueue())
		if err != nil {
			t.Fatal(err)
		}
		if err := qp.Publish(ctx, PublishOptions{Topic: "leak/queued", QoS: 1}); err != nil {
			t.Fatal(err)
		}
		if err := qp.Close(ctx); err != nil {
			t.Fatal(err)
		}
		// Drop the connections; the supervisors reconnect.
		connects := cli.Stats().Connects
		b.DropAll()
		for cli.Stats().Connects == connects {
			if ctx.Err() != nil {
				t.Fatal("no reconnect after the drop")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err := cli.Disconnect(ctx); err != nil {
			t.Fatalf("cycle %d: Disconnect: %v", cycle, err)
		}
	}
	b.Close()
	check()
}

// A client that never reaches its broker, and one whose supervisor
// stops on OnConnectionDown, leave nothing behind either.
func TestStoppedSupervisorsLeaveNoGoroutines(t *testing.T) {
	t.Run("retrying initial connect", func(t *testing.T) {
		check := noLeaks(t)
		b := testbroker.New(t)
		b.SetFallback(func(c *testbroker.Conn) { c.Close() })
		cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()), WithRetryInitialConnect(),
			WithReconnectBackoff(ConstantBackoff(5*time.Millisecond)))
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond) // a few failed attempts
		if err := cli.Disconnect(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := cli.AwaitConnection(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("AwaitConnection after Disconnect: %v", err)
		}
		b.Close()
		check()
	})
	t.Run("OnConnectionDown returns false", func(t *testing.T) {
		check := noLeaks(t)
		b := testbroker.New(t)
		down := make(chan struct{})
		cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()),
			WithOnConnectionDown(func() bool { close(down); return false }))
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "t"}}); err != nil {
			t.Fatal(err)
		}
		b.DropAll()
		<-down
		b.Close()
		check()
	})
}

// resumeOrderBroker serves a client holding subs subscriptions. The first
// connection acknowledges them and is dropped: once they are all
// acknowledged, or, with dropOnPublish, once it has read a PUBLISH it
// leaves unacknowledged. The next connection keeps no session; it
// acknowledges what it reads and reports each SUBSCRIBE and PUBLISH on
// order, in the order it read them.
func resumeOrderBroker(t *testing.T, subs int, dropOnPublish bool, order chan<- string) *testbroker.Broker {
	t.Helper()
	var conns atomic.Int32
	return testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		first := conns.Add(1) == 1
		acceptConnect(t, c, dec)
		acked := 0
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			switch p := pkt.(type) {
			case *wire.Subscribe:
				codes := make([]wire.ReasonCode, len(p.Filters))
				for i := range codes {
					codes[i] = wire.ReasonGrantedQoS1
				}
				_, _ = wire.WriteSuback(c, wire.SubackOpts{PacketID: p.PacketID, ReasonCodes: codes})
				acked++
				if !first {
					order <- "SUBSCRIBE"
				}
			case *wire.Publish:
				if first {
					pkt.Release()
					return
				}
				order <- "PUBLISH " + strings.Clone(p.Topic)
				if p.QoS == 1 {
					_, _ = wire.WritePuback(c, wire.PubRespOpts{PacketID: p.PacketID})
				}
			}
			pkt.Release()
			if first && !dropOnPublish && acked == subs {
				return
			}
		}
	})
}

// subscribeBeforeConnect gives cli n subscriptions, each its own SUBSCRIBE.
func subscribeBeforeConnect(t *testing.T, cli *Client, n int) {
	t.Helper()
	for i := range n {
		if _, err := cli.SubscribeCallback(context.Background(),
			[]TopicFilter{{Topic: fmt.Sprintf("resume/%d", i), QoS: 1}}, func(*Message) {}); err != nil {
			t.Fatal(err)
		}
	}
}

// awaitPublishAfterSubscribes reads order until the PUBLISH of topic and
// fails unless every one of the subs re-issued SUBSCRIBEs came first.
func awaitPublishAfterSubscribes(t *testing.T, order <-chan string, subs int, topic string) {
	t.Helper()
	before := 0
	for {
		select {
		case got := <-order:
			if got == "SUBSCRIBE" {
				before++
				continue
			}
			if got != "PUBLISH "+topic {
				t.Fatalf("broker read %q, want PUBLISH %s", got, topic)
			}
			if before != subs {
				t.Fatalf("PUBLISH %s reached a broker that kept no session after %d of the %d re-issued SUBSCRIBEs", topic, before, subs)
			}
			return
		case <-time.After(5 * time.Second):
			t.Fatalf("PUBLISH %s never reached the broker (%d SUBSCRIBEs did)", topic, before)
		}
	}
}

// A connection whose broker kept no session re-issues every subscription
// before it sends a PUBLISH made on it, from Publish at either QoS or a
// QueuePublisher.
func TestPublishAfterSessionLossFollowsResubscribe(t *testing.T) {
	const subs = 64
	direct := func(_ *testing.T, cli *Client) func(context.Context, PublishOptions) error {
		return cli.Publish
	}
	for _, tt := range []struct {
		name string
		qos  byte
		// publisher returns what sends a message for cli.
		publisher func(t *testing.T, cli *Client) func(context.Context, PublishOptions) error
	}{
		{"Publish QoS 0", 0, direct},
		{"Publish QoS 1", 1, direct},
		{"QueuePublisher", 1, func(t *testing.T, cli *Client) func(context.Context, PublishOptions) error {
			qp, err := NewQueuePublisher(cli, NewMemoryPublisherQueue())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := qp.Close(ctx); err != nil {
					t.Errorf("QueuePublisher.Close: %v", err)
				}
			})
			return qp.Publish
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			order := make(chan string, subs+4)
			fb := resumeOrderBroker(t, subs, false, order)
			published := make(chan error, 1)
			var publish func(context.Context, PublishOptions) error
			var ups atomic.Int32
			cli, err := New(
				WithBroker(fb.URL()),
				WithReconnectBackoff(ConstantBackoff(time.Millisecond)),
				WithLogger(quietLogger()),
				WithOnConnectionUp(func(ConnackInfo) {
					if ups.Add(1) == 2 {
						go func() { published <- publish(context.Background(), PublishOptions{Topic: "fresh", QoS: tt.qos}) }()
					}
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			// Cleanups run last first: the publisher closes, settling its
			// exchange, before the client disconnects.
			t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
			publish = tt.publisher(t, cli)
			subscribeBeforeConnect(t, cli, subs)
			if err := cli.Connect(context.Background()); err != nil {
				t.Fatal(err)
			}

			awaitPublishAfterSubscribes(t, order, subs, "fresh")
			if err := <-published; err != nil {
				t.Fatalf("publish = %v", err)
			}
		})
	}
}

// A QoS 1 message a lost session sends again goes out behind the
// re-issued subscriptions too.
func TestRepublishAfterSessionLossFollowsResubscribe(t *testing.T) {
	const subs = 16
	order := make(chan string, subs+4)
	fb := resumeOrderBroker(t, subs, true, order)
	cli, err := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(time.Millisecond)),
		WithLogger(quietLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	subscribeBeforeConnect(t, cli, subs)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	published := make(chan error, 1)
	go func() { published <- cli.Publish(ctx, PublishOptions{Topic: "in-flight", QoS: 1}) }()
	awaitPublishAfterSubscribes(t, order, subs, "in-flight")
	if err := <-published; err != nil {
		t.Fatalf("Publish = %v, want the republished message acknowledged", err)
	}
}
