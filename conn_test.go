// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// Publishers writing their own packets and publishers queued behind the
// writer goroutine never interleave bytes: every packet arrives whole,
// at every payload size.
func TestDirectAndQueuedWritesKeepPacketsWhole(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithPublishMode(PublishWaitForFlush))
	sizes := []int{0, 10, 1000, 70 << 10}
	const workers, per = 8, 120
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			for i := range per {
				payload := append([]byte(fmt.Sprintf("%d/%d/", w, i)), bytes.Repeat([]byte{byte(w)}, sizes[i%len(sizes)])...)
				qos := byte(0)
				if i%5 == 0 {
					qos = 1
				}
				if err := cli.Publish(ctx, PublishOptions{Topic: "whole", Payload: payload, QoS: qos}); err != nil {
					t.Errorf("worker %d publish %d: %v", w, i, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	// A QoS 0 publish returns once written; the broker may not have read
	// the last ones yet.
	got := map[string]bool{}
	for deadline := time.Now().Add(5 * time.Second); len(got) < workers*per && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		clear(got)
		for _, p := range b.Conn(0, 0).Log() {
			if p.Type == wire.PUBLISH {
				got[string(p.Payload)] = true
			}
		}
	}
	for w := range workers {
		for i := range per {
			want := append([]byte(fmt.Sprintf("%d/%d/", w, i)), bytes.Repeat([]byte{byte(w)}, sizes[i%len(sizes)])...)
			if !got[string(want)] {
				t.Fatalf("publish %d/%d never arrived intact", w, i)
			}
		}
	}
}

// sinkBroker accepts one MQTT connection, answers its CONNECT and then
// reads and discards everything, so a test can measure what the client
// alone allocates.
func sinkBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		p, err := wire.NewDecoder(conn).ReadPacket()
		if err != nil || p.Type() != wire.CONNECT {
			return
		}
		p.Release()
		if _, err := wire.WriteConnack(conn, wire.ConnackOpts{}); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	}()
	return "mqtt://" + ln.Addr().String()
}

// With PublishWaitForFlush on an idle TCP connection a QoS 0 publish
// writes the caller's payload as it is, without copying it.
func TestWaitForFlushDoesNotCopyThePayload(t *testing.T) {
	cli, err := New(WithBroker(sinkBroker(t)), WithClientID("nocopy"), WithLogger(quietLogger()),
		WithKeepAlive(60), WithPublishMode(PublishWaitForFlush))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })

	payload := make([]byte, 256<<10)
	publish := func() {
		if err := cli.Publish(ctx, PublishOptions{Topic: "big", Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	publish()
	// A publish that finds the writer goroutine busy is queued, and
	// copied, by design; the best of several rounds is the direct path.
	const rounds, n = 5, 20
	best := uint64(math.MaxUint64)
	for range rounds {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range n {
			publish()
		}
		runtime.ReadMemStats(&after)
		best = min(best, (after.TotalAlloc-before.TotalAlloc)/n)
	}
	if best > 8<<10 {
		t.Fatalf("a %d-byte publish allocated %d bytes", len(payload), best)
	}
}

// A direct write to a connection the broker has closed returns an
// error rather than hanging.
func TestWaitForFlushReportsAFailedWrite(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Close()
	})
	cli := tbClient(t, b, WithPublishMode(PublishWaitForFlush), WithOnConnectionDown(func() bool { return false }))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	payload := make([]byte, 64<<10)
	for {
		err := cli.Publish(ctx, PublishOptions{Topic: "gone", Payload: payload})
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("publishing to a closed connection never failed")
		}
		if err != nil {
			return
		}
	}
}

// stalledBroker accepts one connection, answers its CONNECT and then
// stops reading until the test reads from the returned connection.
func stalledBroker(t *testing.T) (url string, accepted <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	ch := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		p, err := wire.NewDecoder(conn).ReadPacket()
		if err != nil || p.Type() != wire.CONNECT {
			return
		}
		p.Release()
		if _, err := wire.WriteConnack(conn, wire.ConnackOpts{}); err != nil {
			return
		}
		ch <- conn
	}()
	return "mqtt://" + ln.Addr().String(), ch
}

// A Publish writing on its own goroutine returns when ctx ends, although
// the broker has stopped reading; the writer goroutine finishes the
// interrupted packet, so the stream stays whole and the next packet
// arrives intact behind it. The caller may reuse its payload at once.
func TestDirectWriteReturnsWhenCtxEnds(t *testing.T) {
	for _, qos := range []byte{0, 1} {
		t.Run(fmt.Sprintf("QoS%d", qos), func(t *testing.T) {
			url, accepted := stalledBroker(t)
			cli, err := New(WithBroker(url), WithClientID("stalled"), WithLogger(quietLogger()),
				WithoutKeepAlive(), WithPublishMode(PublishWaitForFlush))
			if err != nil {
				t.Fatal(err)
			}
			if err := cli.Connect(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = cli.Disconnect(ctx)
			})
			conn := <-accepted

			// Larger than the socket buffers on both ends, so the write
			// blocks while the broker does not read.
			payload := bytes.Repeat([]byte{'a'}, 32<<20)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			returned := make(chan error, 1)
			go func() { returned <- cli.Publish(ctx, PublishOptions{Topic: "big", QoS: qos, Payload: payload}) }()
			select {
			case err := <-returned:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Publish returned %v, want the context's deadline", err)
				}
			case <-time.After(5 * time.Second):
				_ = conn.Close()
				t.Fatal("Publish still blocked 5s after a 100ms deadline")
			}
			clear(payload) // the caller owns its payload again

			next := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				next <- cli.Publish(ctx, PublishOptions{Topic: "next", Payload: []byte("behind it")})
			}()

			dec := wire.NewDecoderSize(conn, 64<<10)
			first, err := dec.ReadPacket()
			if err != nil {
				t.Fatalf("interrupted packet: %v", err)
			}
			pub, ok := first.(*wire.Publish)
			if !ok || pub.Topic != "big" || !bytes.Equal(pub.Payload, bytes.Repeat([]byte{'a'}, 32<<20)) {
				t.Fatalf("interrupted packet arrived as %s, not whole", first.Type())
			}
			if qos == 1 {
				if _, err := wire.WritePuback(conn, wire.PubRespOpts{PacketID: pub.PacketID}); err != nil {
					t.Fatal(err)
				}
			}
			first.Release()
			second, err := dec.ReadPacket()
			if err != nil {
				t.Fatalf("packet behind it: %v", err)
			}
			if p, ok := second.(*wire.Publish); !ok || p.Topic != "next" || string(p.Payload) != "behind it" {
				t.Fatalf("packet behind the interrupted one: %s", second.Type())
			}
			second.Release()
			if err := <-next; err != nil {
				t.Fatalf("next Publish: %v", err)
			}
		})
	}
}

// A first PUBLISH that registers a topic alias and is refused before it
// is sent leaves no alias behind: the next PUBLISH on the topic
// registers it again rather than using one the broker never saw.
func TestRefusedPublishDoesNotRegisterAlias(t *testing.T) {
	for _, mode := range []PublishMode{PublishWaitForFlush, PublishFireAndForget} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			maxSize := uint32(100)
			received := make(chan testbroker.Packet, 1)
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.AcceptConnect(wire.ConnackOpts{TopicAliasMaximum: 10, MaximumPacketSize: &maxSize})
				received <- c.Expect(wire.PUBLISH, 0)
				c.ServeAuto()
			})
			cli := tbClient(t, b, WithOutboundTopicAliases(), WithPublishMode(mode))
			ctx := context.Background()
			if err := cli.Publish(ctx, PublishOptions{Topic: "alias/topic", Payload: make([]byte, 200)}); !errors.Is(err, ErrPacketTooLarge) {
				t.Fatalf("oversized publish: %v", err)
			}
			if err := cli.Publish(ctx, PublishOptions{Topic: "alias/topic", Payload: []byte("small")}); err != nil {
				t.Fatal(err)
			}
			p := <-received
			alias, _ := p.Properties().Uint16(wire.PropTopicAlias)
			if p.Topic != "alias/topic" || alias != 1 {
				t.Fatalf("first PUBLISH on the wire: topic %q alias %d, want the topic registering alias 1", p.Topic, alias)
			}
		})
	}
}

// Requests racing a connection's teardown are all answered: none is
// admitted after the writer has drained its queue.
func TestTeardownAnswersEveryQueuedRequest(t *testing.T) {
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptResume(wire.ConnackOpts{})
		c.Hold(20 * time.Millisecond)
	})
	cli := tbClient(t, b, WithPublishMode(PublishWaitForFlush), WithWriteQueueSize(4))
	running, stop := context.WithCancel(context.Background())
	defer stop()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			payload := make([]byte, 512)
			for running.Err() == nil {
				ctx, cancel := context.WithTimeout(running, 5*time.Second)
				err := cli.Publish(ctx, PublishOptions{Topic: fmt.Sprint("race/", w), Payload: payload})
				cancel()
				switch {
				case errors.Is(err, context.DeadlineExceeded):
					t.Error("a publish racing the teardown was never answered")
					return
				case errors.Is(err, ErrNotConnected):
					// Between connections: wait for the next instead of
					// spinning, which starves the reconnect on one CPU.
					_ = cli.AwaitConnection(running)
				}
			}
		})
	}
	// The broker drops each connection after 20 ms; watch six go by.
	var dead []*connState
	for deadline := time.Now().Add(30 * time.Second); len(dead) < 6; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("only %d connections in 30s; the broker drops them every 20ms", len(dead))
		}
		if cs := cli.cur.Load(); cs != nil && (len(dead) == 0 || dead[len(dead)-1] != cs) {
			dead = append(dead, cs)
		}
	}
	stop()
	wg.Wait()
	for _, cs := range dead[:len(dead)-1] {
		<-cs.writerDone
		if n := cs.queued.Load(); n != 0 {
			t.Fatalf("a closed connection still counts %d queued writes", n)
		}
		if len(cs.writeQueue) != 0 {
			t.Fatalf("a closed connection's queue holds %d unanswered requests", len(cs.writeQueue))
		}
	}
}

// writeWatch reports the first write after it is armed.
type writeWatch struct {
	net.Conn
	armed   atomic.Bool
	writing chan struct{}
	once    sync.Once
}

func (c *writeWatch) Write(p []byte) (int, error) {
	if c.armed.Load() {
		c.once.Do(func() { close(c.writing) })
	}
	return c.Conn.Write(p)
}

// A publish waiting its turn for the topic-alias table stops waiting
// when its ctx ends, even while an earlier publish is stuck writing.
func TestAliasWaitEndsWithContext(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	conn := &writeWatch{Conn: client, writing: make(chan struct{})}
	go func() {
		p, err := wire.NewDecoder(server).ReadPacket()
		if err != nil {
			return
		}
		p.Release()
		_, _ = wire.WriteConnack(server, wire.ConnackOpts{TopicAliasMaximum: 10})
		// Then stop reading.
	}()
	cli, err := New(WithBroker("mqtt://pipe"), WithoutKeepAlive(), WithPublishMode(PublishWaitForFlush),
		WithOutboundTopicAliases(), WithLogger(quietLogger()),
		WithDialFunc(func(context.Context, *url.URL) (transport.Conn, error) { return conn, nil }))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = cli.Disconnect(ctx)
	})
	conn.armed.Store(true)
	first := make(chan error, 1)
	go func() { first <- cli.Publish(context.Background(), PublishOptions{Topic: "first"}) }()
	<-conn.writing
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- cli.Publish(ctx, PublishOptions{Topic: "second"}) }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second publish: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the second publish ignored its ctx while waiting for the alias table")
	}
	_ = server.Close()
	<-first
}

// Once a connection's writer has exited, its queue admits nothing: a
// request is refused at once instead of waiting for an answer that no
// writer would give.
func TestClosedWriteQueueAdmitsNothing(t *testing.T) {
	held := make(chan struct{})
	drop := sync.OnceFunc(func() { close(held) })
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		<-held
		c.Close()
	})
	t.Cleanup(drop)
	cli := tbClient(t, b, WithOnConnectionDown(func() bool { return false }))
	// Taken while the broker holds the connection open.
	cs := cli.cur.Load()
	if cs == nil {
		t.Fatal("not connected")
	}
	drop()
	<-cs.writerDone
	done := make(chan error, 1)
	if err := cs.queue(context.Background(), writeReq{fn: writeBytes(nil), done: done}, true); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("queue after the writer exited: %v", err)
	}
	if n := cs.queued.Load(); n != 0 {
		t.Fatalf("queued count %d after the writer exited", n)
	}
}

func TestPublish_WriteDropNewestSurfacesErrWriteQueueFull(t *testing.T) {
	// The broker accepts CONNECT + CONNACK, then never reads from
	// the socket. Kernel TCP send buffer fills, the writer goroutine
	// blocks in conn.Write, writeQueue stops draining. With
	// WriteDropNewest + a small WriteQueueSize, the next
	// Publish call must return ErrWriteQueueFull rather than block.
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		<-fb.Done()
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithWriteQueueSize(1),
		WithWriteOverflowPolicy(WriteDropNewest),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	// 16 KiB payload * 500 iter = 8 MiB pushed. Even a generously
	// autotuned TCP send buffer (typically 4 MiB max on linux/darwin)
	// fills before we exhaust the loop, so the writer ends up
	// blocked in conn.Write and writeQueue cannot drain.
	payload := make([]byte, 16*1024)
	opts := PublishOptions{Topic: "x", QoS: 0, Payload: payload}

	var sawDrop bool
	for range 500 {
		if errors.Is(cli.Publish(context.Background(), opts), ErrWriteQueueFull) {
			sawDrop = true
			break
		}
	}
	if !sawDrop {
		t.Fatal("expected at least one ErrWriteQueueFull, got none")
	}
}

func TestWithWriteOverflowPolicy_RejectsInvalid(t *testing.T) {
	_, err := New(
		WithBroker("mqtt://127.0.0.1:1883"),
		WithWriteOverflowPolicy(WriteOverflowPolicy(99)),
	)
	if err == nil {
		t.Fatal("expected New to reject invalid WriteOverflowPolicy, got nil")
	}
}

func TestKeepAliveDecide(t *testing.T) {
	base := time.Unix(1_000, 0)
	keep, timeout := 10*time.Second, 2*time.Second
	tests := []struct {
		name       string
		k          keepAlive
		send, dead bool
		wait       time.Duration
	}{
		{"fresh both ways", keepAlive{now: base, lastWrite: base, lastRead: base}, false, false, keep},
		{"outbound quiet", keepAlive{now: base, lastWrite: base.Add(-keep), lastRead: base}, true, false, 0},
		{"inbound quiet", keepAlive{now: base, lastWrite: base, lastRead: base.Add(-keep)}, true, false, 0},
		{"earlier window wins", keepAlive{now: base, lastWrite: base.Add(-4 * time.Second), lastRead: base}, false, false, 6 * time.Second},
		{"ping outstanding", keepAlive{now: base, lastWrite: base, lastRead: base.Add(-keep), pingSent: base.Add(-time.Second)}, false, false, time.Second},
		{"ping timed out", keepAlive{now: base, lastWrite: base, lastRead: base.Add(-keep), pingSent: base.Add(-timeout)}, false, true, 0},
		{"ping timed out while application code held the read loop", keepAlive{now: base, lastWrite: base, lastRead: base.Add(-keep), pingSent: base.Add(-timeout), appRan: true}, true, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.k.keep, tt.k.timeout = keep, timeout
			send, dead, wait := tt.k.decide()
			if send != tt.send || dead != tt.dead || wait != tt.wait {
				t.Fatalf("decide() = send %v dead %v wait %v, want %v %v %v", send, dead, wait, tt.send, tt.dead, tt.wait)
			}
		})
	}
}

func TestEffectivePingTimeout(t *testing.T) {
	if got := effectivePingTimeout(3*time.Second, 10*time.Second); got != 3*time.Second {
		t.Fatalf("within keep-alive: %v", got)
	}
	// The broker granted a keep-alive shorter than the configured timeout.
	if got := effectivePingTimeout(10*time.Second, 4*time.Second); got != 2*time.Second {
		t.Fatalf("above granted keep-alive: %v", got)
	}
}

// PingTimeout defaults below the keep-alive and is validated.
func TestPingTimeoutDefaultsAndValidation(t *testing.T) {
	for _, tt := range []struct {
		keep uint16
		want time.Duration
	}{{30, 10 * time.Second}, {8, 4 * time.Second}, {1, 500 * time.Millisecond}} {
		c, err := New(WithBroker("mqtt://127.0.0.1:1"), WithKeepAlive(tt.keep))
		if err != nil {
			t.Fatal(err)
		}
		if c.cfg.PingTimeout != tt.want {
			t.Errorf("keep-alive %ds: default PingTimeout %v, want %v", tt.keep, c.cfg.PingTimeout, tt.want)
		}
	}
	_, err := New(WithBroker("mqtt://127.0.0.1:1"), WithKeepAlive(5), WithPingTimeout(5*time.Second))
	if err == nil || !strings.Contains(err.Error(), "PingTimeout") {
		t.Fatalf("PingTimeout equal to KeepAlive accepted: %v", err)
	}
	if _, err := New(WithBroker("mqtt://127.0.0.1:1"), WithoutKeepAlive(), WithPingTimeout(time.Minute)); err != nil {
		t.Fatalf("PingTimeout with keep-alive disabled: %v", err)
	}
}

// pingHarness runs pingLoop against a fake clock, playing writer and
// broker: it reads the write queue, records each PINGREQ, and optionally
// answers it.
type pingHarness struct {
	t     *testing.T
	clk   *clock.Fake
	c     *Client
	cs    *connState
	pings []time.Time
	died  time.Time

	answer  bool                                // reply to PINGREQ
	traffic func(now time.Time, h *pingHarness) // application traffic per step
}

func newPingHarness(t *testing.T, keep time.Duration, timeout time.Duration) *pingHarness {
	clk := clock.NewFake(time.Unix(1_000, 0))
	c := &Client{cfg: &Config{PingTimeout: timeout, Logger: quietLogger(), clock: clk}}
	life := newLifecycle()
	c.life.Store(life)
	cs := &connState{
		clk:        clk,
		writeQueue: make(chan writeReq, 64),
		life:       life,
		dying:      make(chan struct{}),
		info:       ConnackInfo{KeepAlive: uint16(keep / time.Second)},
	}
	now := clk.Now().UnixNano()
	cs.lastWriteUnixNano.Store(now)
	cs.lastReadUnixNano.Store(now)
	cs.wg.Add(1)
	go c.pingLoop(cs)
	t.Cleanup(func() {
		life.endWith(nil)
		cs.wg.Wait()
	})
	return &pingHarness{t: t, clk: clk, c: c, cs: cs, answer: true}
}

func (h *pingHarness) write(now time.Time) { h.cs.lastWriteUnixNano.Store(now.UnixNano()) }

func (h *pingHarness) read(now time.Time) {
	h.cs.lastReadUnixNano.Store(now.UnixNano())
	h.cs.reads.Add(1)
}

// run advances the clock by total in steps, servicing the write queue
// whenever the loop has parked on its timer.
func (h *pingHarness) run(total, step time.Duration) {
	h.t.Helper()
	for elapsed := time.Duration(0); elapsed <= total; elapsed += step {
		if !h.parked() {
			return
		}
		now := h.clk.Now()
		for drained := false; !drained; {
			select {
			case req := <-h.cs.writeQueue:
				if req.fn == nil {
					continue
				}
				var buf strings.Builder
				if _, err := req.fn(&writerTo{&buf}); err != nil {
					h.t.Fatal(err)
				}
				h.pings = append(h.pings, now)
				h.write(now)
				if h.answer {
					h.read(now)
				}
			default:
				drained = true
			}
		}
		if h.traffic != nil {
			h.traffic(now, h)
		}
		h.clk.Advance(step)
	}
}

// parked waits until the loop is blocked on its timer, or reports false
// once the connection was declared dead.
func (h *pingHarness) parked() bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-h.cs.dying:
			if h.died.IsZero() {
				h.died = h.clk.Now()
			}
			return false
		default:
		}
		if h.clk.Pending() > 0 {
			return true
		}
		time.Sleep(50 * time.Microsecond)
	}
	h.t.Fatal("ping loop neither parked nor died")
	return false
}

type writerTo struct{ b *strings.Builder }

func (w *writerTo) Write(p []byte) (int, error) { return w.b.Write(p) }

// On an idle connection PINGREQs are never further apart than the
// keep-alive, whatever the ping timeout.
func TestKeepAliveIdleGapsWithinKeepAlive(t *testing.T) {
	keep := 10 * time.Second
	for _, timeout := range []time.Duration{100 * time.Millisecond, keep / 4, keep / 2, keep - time.Millisecond} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			h := newPingHarness(t, keep, timeout)
			h.run(2*time.Minute, 100*time.Millisecond)
			if !h.died.IsZero() {
				t.Fatalf("answered pings, yet declared dead at %v", h.died)
			}
			if len(h.pings) < 11 {
				t.Fatalf("%d PINGREQs in 2 minutes", len(h.pings))
			}
			prev := time.Unix(1_000, 0)
			for _, p := range h.pings {
				if gap := p.Sub(prev); gap > keep {
					t.Fatalf("PINGREQ gap %v exceeds keep-alive %v; pings %v", gap, keep, h.pings)
				}
				prev = p
			}
		})
	}
}

// Traffic in both directions makes PINGREQ unnecessary.
func TestKeepAliveBusyConnectionSendsNoPing(t *testing.T) {
	h := newPingHarness(t, 10*time.Second, 2*time.Second)
	h.traffic = func(now time.Time, h *pingHarness) {
		h.write(now)
		h.read(now)
	}
	h.run(time.Minute, time.Second)
	if len(h.pings) != 0 {
		t.Fatalf("%d PINGREQs on a busy connection", len(h.pings))
	}
}

// Any inbound packet after a PINGREQ proves the broker alive: a stream of
// PUBLISHes whose PINGRESPs are stuck behind the backlog must not tear
// the connection down.
func TestKeepAliveInboundTrafficCountsAsAnswer(t *testing.T) {
	h := newPingHarness(t, 2*time.Second, time.Second)
	h.answer = false
	h.traffic = func(now time.Time, h *pingHarness) { h.read(now) }
	h.run(30*time.Second, 100*time.Millisecond)
	if !h.died.IsZero() {
		t.Fatalf("declared dead at %v despite inbound traffic", h.died)
	}
	if len(h.pings) == 0 {
		t.Fatal("outbound-idle connection sent no PINGREQ")
	}
}

// A broker that answers nothing is detected one ping timeout after the
// PINGREQ.
func TestKeepAliveDetectsHalfOpen(t *testing.T) {
	keep, timeout := 10*time.Second, 3*time.Second
	h := newPingHarness(t, keep, timeout)
	h.answer = false
	h.run(time.Minute, 100*time.Millisecond)
	if h.died.IsZero() {
		t.Fatal("silent broker not detected")
	}
	if len(h.pings) != 1 {
		t.Fatalf("%d PINGREQs before detection, want 1", len(h.pings))
	}
	if got := h.died.Sub(h.pings[0]); got < timeout || got > timeout+100*time.Millisecond {
		t.Fatalf("detected %v after the PINGREQ, want %v", got, timeout)
	}
}

// hold runs a SubscribeCallback handler through the real route, as the
// read loop would, until the returned function releases it.
func (h *pingHarness) hold() (release func()) {
	gate, entered, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r := &route{sync: true, deliver: func(*Message) { close(entered); <-gate }}
	d := &delivery{client: h.c}
	d.refs.Store(1)
	d.first.d = d
	go func() {
		defer close(done)
		r.dispatch(&d.first)
	}()
	<-entered
	return func() { close(gate); <-done }
}

// blockingAuth is an Authenticator whose Continue, and VerifyServerFinal,
// wait for release once entered is closed.
type blockingAuth struct {
	entered, release chan struct{}
}

func (*blockingAuth) Method() string                        { return "ECHO" }
func (*blockingAuth) Begin(context.Context) ([]byte, error) { return nil, nil }
func (a *blockingAuth) Continue([]byte) ([]byte, bool, error) {
	close(a.entered)
	<-a.release
	return nil, false, nil
}
func (a *blockingAuth) VerifyServerFinal([]byte) error {
	close(a.entered)
	<-a.release
	return nil
}

// An Authenticator answering a re-authentication runs on the read loop
// too; the keep-alive waits it out as it waits out a handler.
func TestKeepAliveWaitsOutAnAuthenticator(t *testing.T) {
	for _, reason := range []wire.ReasonCode{wire.ReasonContinueAuthentication, wire.ReasonSuccess} {
		t.Run(fmt.Sprintf("reason=0x%02X", byte(reason)), func(t *testing.T) {
			h := newPingHarness(t, 2*time.Second, time.Second)
			h.answer = false // nothing is read while the Authenticator holds the loop
			auth := &blockingAuth{entered: make(chan struct{}), release: make(chan struct{})}
			h.c.cfg.Authenticator = auth
			done := make(chan struct{})
			go func() {
				defer close(done)
				h.c.handleReauthInbound(h.cs, &reauthCall{result: make(chan error, 1)}, reason, nil)
			}()
			<-auth.entered
			h.read(h.clk.Now()) // the AUTH that started it was read
			h.run(10*time.Second, 100*time.Millisecond)
			close(auth.release)
			<-done
			if !h.died.IsZero() {
				t.Fatalf("declared dead at %v while the Authenticator held the read loop", h.died)
			}
		})
	}
}

// While application code holds the read loop the broker's PINGRESP waits
// unread; that is no proof the broker is gone. Once the handler returns
// the loop reads it, and the connection stays up.
func TestKeepAliveWaitsOutApplicationCode(t *testing.T) {
	keep, timeout := 2*time.Second, time.Second
	h := newPingHarness(t, keep, timeout)
	h.answer = false // what the broker answers is not read while the loop is busy
	release := h.hold()
	h.run(20*time.Second, 100*time.Millisecond)
	if !h.died.IsZero() {
		t.Fatalf("declared dead at %v while a handler held the read loop", h.died)
	}
	// The broker drops a client silent for 1.5 keep-alives (§3.1.2.10),
	// so PINGREQs keep going out while the answers wait unread.
	prev := time.Unix(1_000, 0)
	for _, p := range h.pings {
		if gap := p.Sub(prev); gap > keep {
			t.Fatalf("PINGREQ gap %v exceeds keep-alive %v; pings %v", gap, keep, h.pings)
		}
		prev = p
	}
	if h.clk.Now().Sub(prev) > keep {
		t.Fatalf("no PINGREQ since %v", prev)
	}
	release()
	h.read(h.clk.Now()) // the PINGRESP waiting in the socket
	h.answer = true
	h.run(20*time.Second, 100*time.Millisecond)
	if !h.died.IsZero() {
		t.Fatalf("declared dead at %v after the handler returned", h.died)
	}
}

// A broker that answers nothing is still detected once the handler
// returns: within two ping timeouts.
func TestKeepAliveDetectsSilenceAfterApplicationCode(t *testing.T) {
	keep, timeout := 2*time.Second, time.Second
	h := newPingHarness(t, keep, timeout)
	h.answer = false
	release := h.hold()
	h.run(10*time.Second, 100*time.Millisecond)
	if !h.died.IsZero() {
		t.Fatalf("declared dead at %v while a handler held the read loop", h.died)
	}
	freed := h.clk.Now()
	release()
	h.run(time.Minute, 100*time.Millisecond)
	if h.died.IsZero() {
		t.Fatal("silent broker not detected after the handler returned")
	}
	if got := h.died.Sub(freed); got > 2*timeout+100*time.Millisecond {
		t.Fatalf("detected %v after the handler returned, want at most %v", got, 2*timeout)
	}
}

// The broker's Server Keep Alive replaces the requested one.
func TestServerKeepAliveOverridesRequested(t *testing.T) {
	ka := uint16(2)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		if ci := c.AcceptConnect(wire.ConnackOpts{ServerKeepAlive: &ka}); ci.KeepAlive != 60 {
			c.T.Errorf("CONNECT keep-alive %d", ci.KeepAlive)
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	if info, _ := cli.ServerInfo(); info.KeepAlive != 2 {
		t.Fatalf("effective keep-alive %d, want the broker's 2", info.KeepAlive)
	}
	// With a 2 s keep-alive an idle connection must ping within ~2 s.
	waitLogFor(t, b.Conn(0, time.Second), wire.PINGREQ, 1, 4*time.Second)
}

func waitLogFor(t *testing.T, conn *testbroker.Conn, pt wire.PacketType, n int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		count := 0
		for _, p := range conn.Log() {
			if p.Type == pt {
				count++
			}
		}
		if count >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fewer than %d %s within %v", n, pt, d)
}
