// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// tbClient connects a client to a testbroker with fast reconnects and a
// silent logger.
func tbClient(t *testing.T, b *testbroker.Broker, opts ...Option) *Client {
	t.Helper()
	base := []Option{
		WithBroker(b.URL()),
		WithClientID("tb-" + t.Name()),
		WithReconnectBackoff(ConstantBackoff(10 * time.Millisecond)),
		WithLogger(quietLogger()),
		WithKeepAlive(60),
	}
	c, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Disconnect(ctx)
	})
	return c
}

func recvMsg(t *testing.T, ch <-chan *Message, d time.Duration) *Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("channel closed")
		}
		return m
	case <-time.After(d):
		t.Fatalf("no message within %v", d)
		return nil
	}
}

// The drop hook must receive a readable message (it used to receive a
// Message whose frame had already been released, crashing on m.Topic).
func TestSubOnDropReceivesReadableMessage(t *testing.T) {
	ready := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-ready
		for i := 0; i < 5; i++ {
			c.Publish(wire.PublishOpts{Topic: fmt.Sprintf("d/%d", i), Payload: []byte(fmt.Sprintf("p%d", i))})
		}
		c.Hold(0)
	})
	cli := tbClient(t, b)
	var mu sync.Mutex
	var dropped []string
	_, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "d/#"}}, SubBuffer(1),
		SubOnDrop(func(m *Message) {
			mu.Lock()
			dropped = append(dropped, m.Topic+"="+string(m.Payload))
			mu.Unlock()
		}))
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(dropped)
		mu.Unlock()
		if n == 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dropped) != 4 {
		t.Fatalf("dropped = %v, want 4 drops", dropped)
	}
	for i, s := range dropped {
		if want := fmt.Sprintf("d/%d=p%d", i+1, i+1); s != want {
			t.Errorf("drop %d = %q, want %q", i, s, want)
		}
	}
}

// A second Ack on an already-acked message is a no-op; it must never
// acknowledge or invalidate a later delivery.
func TestDoubleAckNeverTouchesAnotherMessage(t *testing.T) {
	step := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "a/1", Payload: []byte("A"), QoS: 1, PacketID: 1})
		<-step
		c.Publish(wire.PublishOpts{Topic: "a/2", Payload: []byte("B"), QoS: 1, PacketID: 2})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "a/#", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	mA := recvMsg(t, ch, 2*time.Second)
	if err := mA.Ack(); err != nil {
		t.Fatal(err)
	}
	close(step)
	mB := recvMsg(t, ch, 2*time.Second)
	_ = mA.Ack() // second Ack: must be a no-op
	_ = mA.Ack()

	conn := b.Conn(0, time.Second)
	if _, ok := conn.Await(wire.PUBACK, 300*time.Millisecond); !ok {
		t.Fatal("no PUBACK for A")
	}
	if p, ok := conn.Await(wire.PUBACK, 300*time.Millisecond); ok {
		t.Fatalf("PUBACK id=%d sent before the application acked B", p.PacketID)
	}
	if mB.Topic != "a/2" || string(mB.Payload) != "B" {
		t.Fatalf("B corrupted: topic=%q payload=%q", mB.Topic, mB.Payload)
	}
	_ = mB.Ack()
	if p, ok := conn.Await(wire.PUBACK, time.Second); !ok || p.PacketID != 2 {
		t.Fatalf("PUBACK for B: ok=%v id=%d", ok, p.PacketID)
	}
}

// With overlapping subscriptions each handler owns its own handle; one
// handler acking twice must not release or acknowledge for the other.
func TestDoubleAckOnSharedDeliveryKeepsOtherHandlerValid(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "m/x", Payload: []byte("PAYLOAD"), QoS: 1, PacketID: 1})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	chA, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "m/#", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	chB, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "m/x", QoS: 1}})
	if err != nil {
		t.Fatal(err)
	}
	mA := recvMsg(t, chA, 2*time.Second)
	mB := recvMsg(t, chB, 2*time.Second)
	_ = mA.Ack()
	_ = mA.Ack()
	conn := b.Conn(0, time.Second)
	if p, ok := conn.Await(wire.PUBACK, 300*time.Millisecond); ok {
		t.Fatalf("PUBACK id=%d sent while handler B still holds the message", p.PacketID)
	}
	if mB.Topic != "m/x" || string(mB.Payload) != "PAYLOAD" {
		t.Fatalf("B invalidated: topic=%q payload=%q", mB.Topic, mB.Payload)
	}
	_ = mB.Ack()
	if _, ok := conn.Await(wire.PUBACK, time.Second); !ok {
		t.Fatal("no PUBACK after both handlers acked")
	}
}

// Zero-copy subscriptions keep the same per-handle guarantees.
func TestZeroCopyDoubleAckIsNoOp(t *testing.T) {
	step := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "z/1", Payload: []byte("A"), QoS: 1, PacketID: 1})
		<-step
		c.Publish(wire.PublishOpts{Topic: "z/2", Payload: []byte("B"), QoS: 1, PacketID: 2})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "z/#", QoS: 1}}, SubZeroCopy())
	if err != nil {
		t.Fatal(err)
	}
	mA := recvMsg(t, ch, 2*time.Second)
	_ = mA.Ack()
	close(step)
	mB := recvMsg(t, ch, 2*time.Second)
	_ = mA.Ack()
	if mB.Topic != "z/2" || string(mB.Payload) != "B" {
		t.Fatalf("B corrupted: topic=%q payload=%q", mB.Topic, mB.Payload)
	}
	conn := b.Conn(0, time.Second)
	conn.Expect(wire.PUBACK, time.Second)
	conn.ExpectNone(wire.PUBACK, 300*time.Millisecond)
	_ = mB.Ack()
	conn.Expect(wire.PUBACK, time.Second)
}

// Disconnect must unregister handlers; reconnecting and subscribing to
// the same filter must not dispatch into the closed channel.
func TestDisconnectConnectResubscribeSameFilter(t *testing.T) {
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		if _, ok := c.ServeSubscribe(-1); !ok {
			return
		}
		if c.Index == 1 {
			c.Publish(wire.PublishOpts{Topic: "r/x", Payload: []byte("after-reconnect")})
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ctx := context.Background()
	old, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "r/x"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-old; ok {
		t.Fatal("old channel should be closed by Disconnect")
	}
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "r/x"}})
	if err != nil {
		t.Fatal(err)
	}
	m := recvMsg(t, ch, 2*time.Second)
	if string(m.Payload) != "after-reconnect" {
		t.Fatalf("payload = %q", m.Payload)
	}
}

// The reader may dispatch through a trie snapshot taken before
// Unsubscribe removed the route. A closed route must ack the message
// instead of sending on its closed channel.
func TestRouteDispatchAfterCloseAcks(t *testing.T) {
	ch := make(chan *Message, 1)
	r := &route{deliver: func(m *Message) { ch <- m }}
	r.close(func() { close(ch) })

	d := &delivery{}
	d.refs.Store(1)
	m := &d.first
	m.d = d
	r.dispatch(m)
	if !m.acked.Load() || d.refs.Load() != 0 {
		t.Fatalf("message on a closed route not acked: acked=%v refs=%d", m.acked.Load(), d.refs.Load())
	}
}

func TestRouteCloseRacingDispatch(t *testing.T) {
	for i := 0; i < 10000; i++ {
		ch := make(chan *Message, 1)
		r := &route{deliver: func(m *Message) {
			select {
			case ch <- m:
			default:
				_ = m.Ack()
			}
		}}
		var wg sync.WaitGroup
		wg.Go(func() {
			for j := 0; j < 4; j++ {
				d := &delivery{}
				d.refs.Store(1)
				d.first.d = d
				r.dispatch(&d.first)
			}
		})
		wg.Go(func() { r.close(func() { close(ch) }) })
		wg.Wait()
	}
}

// End to end: Unsubscribe racing a QoS 0 stream on an overlapping filter.
// The stream is paced so SUBACK/UNSUBACK are not queued behind a
// saturated socket. Run with -race.
func TestUnsubscribeRacingDispatch(t *testing.T) {
	stop := make(chan struct{})
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		go func() {
			tick := time.NewTicker(100 * time.Microsecond)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-c.Gone():
					return
				case <-tick.C:
				}
				for k := 0; k < 8; k++ {
					if c.Publish(wire.PublishOpts{Topic: "flood/x", Payload: []byte("f")}) != nil {
						return
					}
				}
			}
		}()
		c.ServeAuto()
	})
	defer close(stop)
	cli := tbClient(t, b)
	ctx := context.Background()
	if _, err := cli.SubscribeCallback(ctx, []TopicFilter{{Topic: "flood/#"}}, func(*Message) {}); err != nil {
		t.Fatal(err)
	}
	const workers = 4
	iterations := 1000
	if testing.Short() {
		iterations = 200
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for i := 0; i < iterations/workers; i++ {
				ch, tok, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "flood/x"}}, SubBuffer(1))
				if err != nil {
					t.Error(err)
					return
				}
				go func() {
					for range ch {
					}
				}()
				if err := cli.Unsubscribe(ctx, tok); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// A queued small message costs roughly its own size, not a pinned
// 4 KiB frame.
func TestQueuedSmallMessagesStaySmall(t *testing.T) {
	const n = 20000
	payload := make([]byte, 64)
	ready := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-ready
		for i := 0; i < n; i++ {
			c.Publish(wire.PublishOpts{Topic: "q/small", Payload: payload})
		}
		c.Hold(0)
	})
	cli := tbClient(t, b)
	q, _, err := cli.SubscribeQueue(context.Background(), []TopicFilter{{Topic: "q/#"}})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	close(ready)
	deadline := time.Now().Add(10 * time.Second)
	for q.Len() < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if q.Len() != n {
		t.Fatalf("queued %d of %d", q.Len(), n)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	per := (int64(after.HeapInuse) - int64(before.HeapInuse)) / n
	t.Logf("heap per queued 64 B message: %d B", per)
	if per > 400 {
		t.Errorf("heap per queued 64 B message = %d B, want ≤ 400", per)
	}
}

// SubAutoAck acks on the read goroutine before the consumer receives the
// message, and the consumer still gets an owned, readable copy.
func TestSubAutoAckDeliversOwnedMessage(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "aa/1", Payload: []byte("v"), QoS: 1, PacketID: 9})
		c.Hold(0)
	})
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "aa/#", QoS: 1}}, SubAutoAck())
	if err != nil {
		t.Fatal(err)
	}
	conn := b.Conn(0, time.Second)
	if p := conn.Expect(wire.PUBACK, time.Second); p.PacketID != 9 {
		t.Fatalf("PUBACK id = %d before the consumer read the message", p.PacketID)
	}
	m := recvMsg(t, ch, 2*time.Second)
	_ = m.Ack()
	_ = m.Ack()
	conn.ExpectNone(wire.PUBACK, 200*time.Millisecond)
	if m.Topic != "aa/1" || string(m.Payload) != "v" {
		t.Fatalf("message = %q %q", m.Topic, m.Payload)
	}
}

func TestSubscribeQueueCapDefaults(t *testing.T) {
	tests := []struct {
		name   string
		client []Option
		sub    []SubscribeOption
		want   int
	}{
		{"default", nil, nil, DefaultMaxSubscribeQueueSize},
		{"client cap", []Option{WithMaxSubscribeQueueSize(10)}, nil, 10},
		{"client unbounded", []Option{WithMaxSubscribeQueueSize(UnboundedQueue)}, nil, UnboundedQueue},
		{"per-sub cap", nil, []SubscribeOption{SubMaxQueueSize(5)}, 5},
		{"per-sub zero keeps client value", []Option{WithMaxSubscribeQueueSize(10)}, []SubscribeOption{SubMaxQueueSize(0)}, 10},
		{"per-sub unbounded", nil, []SubscribeOption{SubMaxQueueSize(UnboundedQueue)}, UnboundedQueue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(append([]Option{WithBroker("mqtt://127.0.0.1:1")}, tt.client...)...)
			if err != nil {
				t.Fatal(err)
			}
			cfg := newSubscribeConfig(c)
			for _, o := range tt.sub {
				o(&cfg)
			}
			if cfg.maxQueueSize != tt.want {
				t.Fatalf("maxQueueSize = %d, want %d", cfg.maxQueueSize, tt.want)
			}
		})
	}
}

// A callback that retains m.Topic / m.Payload must keep seeing its own
// bytes after later frames reuse the pooled buffers.
func TestCallbackRetainedFieldsStayValid(t *testing.T) {
	const n = 200
	ready := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-ready
		for i := 0; i < n; i++ {
			c.Publish(wire.PublishOpts{Topic: fmt.Sprintf("cb/%d", i), Payload: []byte(fmt.Sprintf("payload-%d", i))})
		}
		c.Hold(0)
	})
	cli := tbClient(t, b)
	var (
		mu       sync.Mutex
		topics   []string
		payloads [][]byte
		done     = make(chan struct{})
	)
	_, err := cli.SubscribeCallback(context.Background(), []TopicFilter{{Topic: "cb/#"}}, func(m *Message) {
		mu.Lock()
		defer mu.Unlock()
		topics = append(topics, m.Topic)
		payloads = append(payloads, m.Payload)
		if len(topics) == n {
			close(done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("not all messages delivered")
	}
	mu.Lock()
	defer mu.Unlock()
	for i := range topics {
		if want := fmt.Sprintf("cb/%d", i); topics[i] != want {
			t.Fatalf("retained topic %d = %q, want %q", i, topics[i], want)
		}
		if want := fmt.Sprintf("payload-%d", i); string(payloads[i]) != want {
			t.Fatalf("retained payload %d = %q, want %q", i, payloads[i], want)
		}
	}
}

// A large message is delivered in the frame it was read into, which is
// never recycled: retained messages stay intact, and receiving one costs
// about its size rather than one copy per stage.
func TestLargeMessagesKeepTheirFrame(t *testing.T) {
	const n, size = 8, 1 << 20
	payloads := make([][]byte, n)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte(i)}, size)
	}
	ready := make(chan struct{})
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		<-ready
		for i := 0; i < n; i++ {
			c.Publish(wire.PublishOpts{Topic: fmt.Sprintf("big/%d", i), Payload: payloads[i]})
		}
		c.Hold(0)
	})
	cli := tbClient(t, b)
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "big/#"}}, SubBuffer(n))
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	close(ready)
	msgs := make([]*Message, n)
	for i := range msgs {
		msgs[i] = recvMsg(t, ch, 5*time.Second)
	}
	runtime.ReadMemStats(&after)
	for i, m := range msgs {
		if m.Topic != fmt.Sprintf("big/%d", i) || !bytes.Equal(m.Payload, payloads[i]) {
			t.Fatalf("message %d changed after later deliveries", i)
		}
		_ = m.Ack()
	}
	if per := (after.TotalAlloc - before.TotalAlloc) / n; per > 3*size/2 {
		t.Fatalf("receiving a %d-byte message allocated %d bytes", size, per)
	}
}
