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
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			payload := make([]byte, 512)
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := cli.Publish(ctx, PublishOptions{Topic: fmt.Sprint("race/", w), Payload: payload})
				cancel()
				if errors.Is(err, context.DeadlineExceeded) {
					t.Error("a publish racing the teardown was never answered")
					return
				}
			}
		})
	}
	var dead []*connState
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if cs := cli.cur.Load(); cs != nil && (len(dead) == 0 || dead[len(dead)-1] != cs) {
			dead = append(dead, cs)
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
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
	if len(dead) < 5 {
		t.Fatalf("only %d connections in 2s; the broker should drop them every 20ms", len(dead))
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
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Close()
	})
	cli := tbClient(t, b, WithOnConnectionDown(func() bool { return false }))
	cs := cli.cur.Load()
	if cs == nil {
		t.Fatal("not connected")
	}
	<-cs.writerDone
	done := make(chan error, 1)
	if err := cs.queue(context.Background(), writeReq{fn: writeBytes(nil), done: done}, true); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("queue after the writer exited: %v", err)
	}
	if n := cs.queued.Load(); n != 0 {
		t.Fatalf("queued count %d after the writer exited", n)
	}
}
