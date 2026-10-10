// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// BenchmarkReceive measures the client's inbound path in isolation: decode,
// route, message construction, delivery and acknowledgement. A local feed
// replaces the broker and writes pre-encoded PUBLISH frames in batches, so
// ns/op and allocs/op are the client's cost per delivered message rather
// than a broker's.
//
// QoS 1 rows hold at most receiveWindow messages unacknowledged, as a broker
// honouring Receive Maximum would, so the channel and queue consumers never
// overflow. The QoS 0 row uses the callback API, whose read goroutine
// applies TCP backpressure itself.
func BenchmarkReceive(b *testing.B) {
	type consumer struct {
		name string
		qos  byte
		run  func(b *testing.B, c *Client, f *receiveFeed, opts []SubscribeOption)
	}
	consumers := []consumer{
		{"callback", 1, recvCallback},
		{"chan", 1, recvChan},
		{"queue", 1, recvQueue},
		{"callback", 0, recvCallback},
	}
	modes := []struct {
		name string
		opts []SubscribeOption
	}{
		{"owned", nil},
		{"zerocopy", []SubscribeOption{SubZeroCopy()}},
	}
	for _, cons := range consumers {
		for _, mode := range modes {
			for _, size := range []int{64, 4096} {
				name := fmt.Sprintf("qos=%d/consumer=%s/delivery=%s/size=%dB", cons.qos, cons.name, mode.name, size)
				b.Run(name, func(b *testing.B) {
					runReceive(b, cons.qos, size, mode.opts, cons.run)
				})
			}
		}
	}
}

// BenchmarkReceiveWindow sweeps the read window for streams of small
// messages. reads/msg — read syscalls per delivered message — does not
// depend on host load, unlike ns/op.
func BenchmarkReceiveWindow(b *testing.B) {
	for _, qos := range []byte{0, 1} {
		run := recvCallback
		if qos == 1 {
			run = recvChan
		}
		for _, window := range []int{4 << 10, 16 << 10, 32 << 10, 64 << 10} {
			b.Run(fmt.Sprintf("qos=%d/size=64B/window=%dKiB", qos, window>>10), func(b *testing.B) {
				runReceive(b, qos, 64, nil, run, WithReadBufferSize(window))
			})
		}
	}
}

// BenchmarkReceiveFilters measures delivery while the client holds many
// subscriptions that do not match the message: routing cost depends on
// the topic's depth, not on how many filters exist.
func BenchmarkReceiveFilters(b *testing.B) {
	for _, n := range []int{0, 10000} {
		b.Run(fmt.Sprintf("filters=%d/qos=0/consumer=callback/size=64B", n), func(b *testing.B) {
			feed := newReceiveFeed(b, 0, 64)
			if n > 0 {
				feed.subscribes = 2
			}
			c := feed.client(b)
			if n > 0 {
				others := make([]TopicFilter, n)
				for i := range others {
					others[i] = TopicFilter{Topic: fmt.Sprintf("devices/%d/telemetry", i)}
				}
				if _, err := c.SubscribeCallback(context.Background(), others, func(*Message) {}); err != nil {
					b.Fatal(err)
				}
			}
			b.SetBytes(64)
			b.ReportAllocs()
			recvCallback(b, c, feed, nil)
		})
	}
}

func runReceive(b *testing.B, qos byte, size int, subOpts []SubscribeOption,
	run func(*testing.B, *Client, *receiveFeed, []SubscribeOption), opts ...Option) {
	feed := newReceiveFeed(b, qos, size)
	c := feed.client(b, opts...)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	feed.reads.Store(0)
	run(b, c, feed, subOpts)
	b.ReportMetric(float64(feed.reads.Load())/float64(b.N), "reads/msg")
}

const (
	receiveTopic  = "bench/receive"
	receiveWindow = 256
	// receiveSlots distinct packet IDs are cycled; more than the window,
	// so an ID is always acknowledged before it is reused.
	receiveSlots = 1024
)

func recvCallback(b *testing.B, c *Client, f *receiveFeed, opts []SubscribeOption) {
	n := b.N
	var got atomic.Int64
	done := make(chan struct{})
	h := func(m *Message) {
		if got.Add(1) == int64(n) {
			close(done)
		}
	}
	if _, err := c.SubscribeCallback(context.Background(), []TopicFilter{{Topic: receiveTopic, QoS: 1}}, h, opts...); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	f.start(n)
	select {
	case <-done:
	case <-time.After(time.Minute):
		b.Fatalf("received %d of %d", got.Load(), n)
	}
	b.StopTimer()
}

func recvChan(b *testing.B, c *Client, f *receiveFeed, opts []SubscribeOption) {
	n := b.N
	ch, _, err := c.Subscribe(context.Background(), []TopicFilter{{Topic: receiveTopic, QoS: 1}},
		append([]SubscribeOption{SubBuffer(receiveWindow)}, opts...)...)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	f.start(n)
	timeout := time.After(time.Minute)
	for i := 0; i < n; i++ {
		select {
		case m := <-ch:
			_ = m.Ack()
		case <-timeout:
			b.Fatalf("received %d of %d", i, n)
		}
	}
	b.StopTimer()
}

func recvQueue(b *testing.B, c *Client, f *receiveFeed, opts []SubscribeOption) {
	n := b.N
	q, _, err := c.SubscribeQueue(context.Background(), []TopicFilter{{Topic: receiveTopic, QoS: 1}}, opts...)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b.ResetTimer()
	f.start(n)
	for i := 0; i < n; i++ {
		m, ok := q.Dequeue(ctx)
		if !ok {
			b.Fatalf("received %d of %d", i, n)
		}
		_ = m.Ack()
	}
	b.StopTimer()
}

// receiveFeed is a single-connection stand-in for a broker.
type receiveFeed struct {
	reads  atomic.Int64 // client-side Read calls on the connection
	ln     net.Listener
	qos    byte
	frames []byte // receiveSlots (QoS 1) or 1 (QoS 0) encoded PUBLISH frames
	size   int    // bytes per frame
	conn   chan net.Conn
	begin  chan int
	// subscribes is how many SUBSCRIBEs the client sends before the
	// messages start.
	subscribes int
}

func newReceiveFeed(b *testing.B, qos byte, payloadSize int) *receiveFeed {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	f := &receiveFeed{ln: ln, qos: qos, subscribes: 1, conn: make(chan net.Conn, 1), begin: make(chan int, 1)}
	payload := make([]byte, payloadSize)
	slots := 1
	if qos > 0 {
		slots = receiveSlots
	}
	for i := 0; i < slots; i++ {
		opts := wire.PublishOpts{Topic: receiveTopic, Payload: payload, QoS: qos}
		if qos > 0 {
			opts.PacketID = uint16(i + 1)
		}
		bp, err := wire.EncodePublish(opts)
		if err != nil {
			b.Fatal(err)
		}
		f.size = len(*bp)
		f.frames = append(f.frames, *bp...)
		wire.ReleaseBuf(bp)
	}
	go f.serve()
	b.Cleanup(func() {
		_ = ln.Close()
		select {
		case c := <-f.conn:
			_ = c.Close()
		default:
		}
	})
	return f
}

func (f *receiveFeed) client(b *testing.B, opts ...Option) *Client {
	base := []Option{
		WithBroker("mqtt://" + f.ln.Addr().String()),
		WithClientID("bench-receive"),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		WithKeepAlive(60),
		WithDialFunc(func(ctx context.Context, u *url.URL) (transport.Conn, error) {
			var d net.Dialer
			nc, err := d.DialContext(ctx, "tcp", u.Host)
			if err != nil {
				return nil, err
			}
			return &countingConn{Conn: nc, reads: &f.reads}, nil
		}),
	}
	c, err := New(append(base, opts...)...)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Disconnect(ctx)
	})
	return c
}

// countingConn counts Read calls, i.e. read syscalls.
type countingConn struct {
	net.Conn
	reads *atomic.Int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(p)
}

// start releases n messages to the client.
func (f *receiveFeed) start(n int) { f.begin <- n }

func (f *receiveFeed) serve() {
	nc, err := f.ln.Accept()
	if err != nil {
		return
	}
	f.conn <- nc
	dec := wire.NewDecoder(nc)
	if err := f.handshake(nc, dec); err != nil {
		return
	}
	n := <-f.begin
	if f.qos == 0 {
		go func() { _, _ = io.Copy(io.Discard, nc) }()
		f.stream(nc, n)
		return
	}
	credit := make(chan struct{}, receiveWindow)
	for i := 0; i < receiveWindow; i++ {
		credit <- struct{}{}
	}
	go func() {
		for {
			p, err := dec.ReadPacket()
			if err != nil {
				return
			}
			if p.Type() == wire.PUBACK {
				credit <- struct{}{}
			}
			p.Release()
		}
	}()
	f.windowed(nc, n, credit)
}

// handshake answers CONNECT and f.subscribes SUBSCRIBEs, granting every
// filter.
func (f *receiveFeed) handshake(nc net.Conn, dec *wire.Decoder) error {
	for answered := 0; answered < f.subscribes; {
		p, err := dec.ReadPacket()
		if err != nil {
			return err
		}
		switch x := p.(type) {
		case *wire.Connect:
			x.Release()
			if _, err := wire.WriteConnack(nc, wire.ConnackOpts{}); err != nil {
				return err
			}
		case *wire.Subscribe:
			codes := make([]wire.ReasonCode, len(x.Filters))
			for i := range codes {
				codes[i] = wire.ReasonGrantedQoS1
			}
			id := x.PacketID
			x.Release()
			if _, err := wire.WriteSuback(nc, wire.SubackOpts{PacketID: id, ReasonCodes: codes}); err != nil {
				return err
			}
			answered++
		default:
			p.Release()
		}
	}
	return nil
}

// stream writes n QoS 0 frames in 64 KiB batches.
func (f *receiveFeed) stream(nc net.Conn, n int) {
	per := max(1, (64<<10)/f.size)
	batch := make([]byte, 0, per*f.size)
	for i := 0; i < per; i++ {
		batch = append(batch, f.frames...)
	}
	for n > 0 {
		k := min(n, per)
		if _, err := nc.Write(batch[:k*f.size]); err != nil {
			return
		}
		n -= k
	}
}

// windowed writes n QoS 1 frames, never more than receiveWindow ahead of
// the client's PUBACKs, batching whatever credit is available.
func (f *receiveFeed) windowed(nc net.Conn, n int, credit chan struct{}) {
	slot := 0
	for n > 0 {
		<-credit
		k := 1
		for k < n && k < 64 && slot+k < receiveSlots {
			select {
			case <-credit:
				k++
				continue
			default:
			}
			break
		}
		if _, err := nc.Write(f.frames[slot*f.size : (slot+k)*f.size]); err != nil {
			return
		}
		slot = (slot + k) % receiveSlots
		n -= k
	}
}
