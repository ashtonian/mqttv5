//go:build e2e

package benchmarks

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// BenchmarkE2E_Publish measures one publisher calling Publish in a loop
// against the broker: ns/op is the time per call, which at QoS 1 and 2
// includes the broker's acknowledgements. A QoS 0 call returns once the
// packet is written to the connection in every library (mqttv5 with
// PublishWaitForFlush), so the libraries complete the same work.
func BenchmarkE2E_Publish(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, qos := range []byte{0, 1, 2} {
			for _, sz := range []size{size64B, size1KiB, size1MiB} {
				b.Run(fmt.Sprintf("lib=%s/qos=%d/size=%s", l.name, qos, sz.name), func(b *testing.B) {
					p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pub"), written: true})
					topic := "bench/publish/" + uniqueID("")
					payload := Payload(sz.bytes)
					ctx := context.Background()

					b.SetBytes(int64(sz.bytes))
					b.ReportAllocs()
					b.ResetTimer()
					cpu := startCPU(b)
					for range b.N {
						if err := p.publish(ctx, topic, qos, payload); err != nil {
							b.Fatalf("publish: %v", err)
						}
					}
					b.StopTimer()
					cpu.stop()
				})
			}
		}
	}
}

// BenchmarkE2E_PublishConcurrent runs a fixed number of goroutines
// publishing QoS 1 on one client, each waiting for its PUBACK before the
// next publish. ns/op is aggregate: elapsed time divided by messages
// acknowledged across all workers. Every library keeps within the
// broker's Receive Maximum (20 on the benchmark broker), so with 64
// workers the MQTT 5 clients queue for send quota; MQTT 3.1.1 has no
// such limit.
func BenchmarkE2E_PublishConcurrent(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, workers := range []int{8, 64} {
			b.Run(fmt.Sprintf("lib=%s/workers=%d/size=%s", l.name, workers, size256B.name), func(b *testing.B) {
				p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pubc")})
				topic := "bench/concurrent/" + uniqueID("")
				payload := Payload(size256B.bytes)

				var failed atomic.Pointer[error]
				start := make(chan struct{})
				var wg sync.WaitGroup
				for w := range workers {
					n := b.N / workers
					if w < b.N%workers {
						n++
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						labels := pprof.Labels("bench-worker", strconv.Itoa(w))
						pprof.Do(context.Background(), labels, func(ctx context.Context) {
							<-start
							for range n {
								if err := p.publish(ctx, topic, 1, payload); err != nil {
									failed.CompareAndSwap(nil, &err)
									return
								}
							}
						})
					}()
				}

				b.SetBytes(int64(size256B.bytes))
				b.ReportAllocs()
				b.ResetTimer()
				cpu := startCPU(b)
				close(start)
				wg.Wait()
				b.StopTimer()
				cpu.stop()
				if err := failed.Load(); err != nil {
					b.Fatalf("publish: %v", *err)
				}
			})
		}
	}
}

// receiveWindow is the Receive Maximum the MQTT 5 subscribers advertise:
// the benchmark broker's in-flight limit towards MQTT 3.1.1 clients, so
// every library receives QoS 1 with the same window.
const receiveWindow = 20

// BenchmarkE2E_Receive measures a subscriber of the library under test
// fed by the library-neutral raw publisher at the rate the broker
// sustains. ns/op is the time per delivered message; allocs/op and
// cpu-ns/op are the subscriber's cost (the raw publisher allocates
// nothing per message and spends little CPU). Every message is checked
// for size and counted; a lost or dropped message fails the run.
//
// QoS 0 is measured with the callback mode only: a library's read loop
// is then held back by the handler, so nothing is dropped. Channel and
// queue consumers run at QoS 1, where the receive window bounds how far
// the broker gets ahead of them.
func BenchmarkE2E_Receive(b *testing.B) {
	requireBroker(b)
	type variant struct {
		mode      mode
		consumers int
		qos       byte
	}
	variants := []variant{
		{modeCallback, 1, 0},
		{modeCallback, 1, 1},
		{modeChan, 1, 1}, {modeChan, 4, 1}, {modeChan, 8, 1},
		{modeQueue, 1, 1}, {modeQueue, 4, 1}, {modeQueue, 8, 1},
	}
	for _, l := range libs {
		for _, v := range variants {
			if !l.supports(v.mode) {
				continue
			}
			for _, sz := range []size{size64B, size1KiB} {
				name := fmt.Sprintf("lib=%s/mode=%s/consumers=%d/qos=%d/size=%s", l.name, v.mode, v.consumers, v.qos, sz.name)
				b.Run(name, func(b *testing.B) {
					topic := "bench/receive/" + uniqueID("")
					s := newSink(sz.bytes, b.N)
					sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-sub"), receiveMaximum: receiveWindow},
						topic, v.qos, v.mode, v.consumers, s.onMsg)
					pub := dialRaw(b, uniqueID("raw-pub"))
					waitLive(b, s, func() error { return pub.publish(topic, 0, nil, 1) })
					payload := Payload(sz.bytes)

					b.SetBytes(int64(sz.bytes))
					b.ReportAllocs()
					b.ResetTimer()
					cpu := startCPU(b)
					published := pub.publishAsync(topic, v.qos, payload, b.N)
					s.wait(b, 2*time.Minute)
					b.StopTimer()
					cpu.stop()
					if err := published(); err != nil {
						b.Fatalf("raw publisher: %v", err)
					}
					s.check(b, sub)
				})
			}
		}
	}
}

// BenchmarkE2E_RoundTrip publishes one message at a time from a client
// of the library under test to a subscriber of the same library and
// waits for it to arrive before the next: ns/op is the closed-loop
// publish-to-delivery latency, allocations are both clients'.
func BenchmarkE2E_RoundTrip(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, qos := range []byte{0, 1, 2} {
			for _, sz := range []size{size64B, size1KiB, size1MiB} {
				b.Run(fmt.Sprintf("lib=%s/qos=%d/size=%s", l.name, qos, sz.name), func(b *testing.B) {
					runRoundTrip(b, l, qos, sz.bytes)
				})
			}
		}
	}
}

func runRoundTrip(b *testing.B, l lib, qos byte, size int) {
	topic := "bench/roundtrip/" + uniqueID("")
	s := newSink(size, b.N)
	arrived := make(chan uint64, 1)
	sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-sub")}, topic, qos, modeCallback, 1, func(p []byte) {
		if len(p) > 0 {
			// Torn stamps are the sink's to report, and so is a delivery
			// the loop is not waiting for.
			seq, _ := readSeq(p)
			select {
			case arrived <- seq:
			default:
			}
		}
		s.onMsg(p)
	})
	p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pub")})
	ctx := context.Background()
	waitLive(b, s, func() error { return p.publish(ctx, topic, 0, nil) })
	// One buffer, stamped for each message: the previous one has
	// arrived, so no library still holds it.
	payload := Payload(size)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		stampSeq(payload, uint64(i))
		if err := p.publish(ctx, topic, qos, payload); err != nil {
			b.Fatalf("publish %d: %v", i, err)
		}
		select {
		case seq := <-arrived:
			if seq != uint64(i) {
				b.Fatalf("waiting for message %d, message %d arrived; %s", i, seq, s)
			}
		case <-time.After(10 * time.Second):
			b.Fatalf("message %d never arrived", i)
		}
	}
	b.StopTimer()
	s.check(b, sub)
}

// latencyWorkers publish on behalf of the open-loop pacer, so a slow
// acknowledgement delays later sends only once every worker is busy.
const latencyWorkers = 32

// BenchmarkE2E_Latency is an open-loop latency test: messages are
// published on a fixed schedule (rate per second) regardless of how fast
// earlier ones complete, and each message's latency is measured from the
// time it was scheduled to the time the subscriber receives it. A
// stalled client therefore shows up as latency instead of as a lower
// send rate (no coordinated omission). The p50/p90/p99/p99.9/max metrics
// are in nanoseconds; ns/op, the schedule plus the wait for the last
// deliveries over N, is about the schedule interval. Run with a fixed
// iteration count, e.g. -benchtime=20000x.
func BenchmarkE2E_Latency(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, qos := range []byte{0, 1} {
			for _, rate := range []int{1000, 10000} {
				b.Run(fmt.Sprintf("lib=%s/qos=%d/rate=%d/size=%s", l.name, qos, rate, size256B.name), func(b *testing.B) {
					runLatency(b, l, qos, rate, size256B.bytes)
				})
			}
		}
	}
}

func runLatency(b *testing.B, l lib, qos byte, rate, size int) {
	topic := "bench/latency/" + uniqueID("")
	interval := time.Second / time.Duration(rate)
	n := b.N
	s := newSink(size, n)
	// Written by the subscriber's one callback goroutine, read once the
	// subscription has stopped.
	latency := make([]time.Duration, n)
	var start atomic.Pointer[time.Time]
	sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-sub")}, topic, qos, modeCallback, 1, func(p []byte) {
		if t0 := start.Load(); t0 != nil && len(p) > 0 {
			// The first delivery of each message counts; the sink fails
			// the run on any other.
			if seq, ok := readSeq(p); ok && seq < uint64(n) && latency[seq] == 0 {
				latency[seq] = time.Since(t0.Add(time.Duration(seq) * interval))
			}
		}
		s.onMsg(p)
	})
	p := l.connect(b, clientConfig{id: uniqueID(l.name + "-pub")})
	ctx := context.Background()
	waitLive(b, s, func() error { return p.publish(ctx, topic, 0, nil) })

	payloads := make([][]byte, n)
	slab := make([]byte, n*size)
	for i := range payloads {
		payloads[i] = slab[i*size : (i+1)*size : (i+1)*size]
		copy(payloads[i], Payload(size))
		stampSeq(payloads[i], uint64(i))
	}

	jobs := make(chan int, n)
	var failed atomic.Pointer[error]
	var wg sync.WaitGroup
	for range latencyWorkers {
		wg.Go(func() {
			for i := range jobs {
				if err := p.publish(ctx, topic, qos, payloads[i]); err != nil {
					failed.CompareAndSwap(nil, &err)
				}
			}
		})
	}

	b.ResetTimer()
	t0 := time.Now()
	start.Store(&t0)
	for i := range n {
		sleepUntil(t0.Add(time.Duration(i) * interval))
		jobs <- i
	}
	close(jobs)
	s.wait(b, time.Minute)
	b.StopTimer()
	wg.Wait()
	if err := failed.Load(); err != nil {
		b.Fatalf("publish: %v", *err)
	}
	s.check(b, sub)

	slices.Sort(latency)
	at := func(q float64) float64 { return float64(latency[int(q*float64(n-1))].Nanoseconds()) }
	b.ReportMetric(at(0.50), "p50-ns")
	b.ReportMetric(at(0.90), "p90-ns")
	b.ReportMetric(at(0.99), "p99-ns")
	b.ReportMetric(at(0.999), "p99.9-ns")
	b.ReportMetric(float64(latency[n-1].Nanoseconds()), "max-ns")
}

// sleepUntil sleeps until t, spinning for the last stretch because a
// timer can wake late by more than a schedule interval.
func sleepUntil(t time.Time) {
	const spin = 200 * time.Microsecond
	if d := time.Until(t) - spin; d > 0 {
		time.Sleep(d)
	}
	for time.Now().Before(t) {
	}
}

// BenchmarkE2E_ReceiveStream measures how fast each library consumes a
// stream fed without a broker. Through the broker (BenchmarkE2E_Receive)
// a QoS 0 stream runs at the broker's pace, and the broker shares the
// machine with the client; here rawServer, a minimal MQTT server in the
// benchmark process, writes pre-encoded PUBLISH packets over loopback
// TCP as fast as the client reads them, keeping the client's Receive
// Maximum of QoS 1 messages unacknowledged (20, the window the broker
// scenarios use). ns/op is the time per delivered message and cpu-ns/op
// the whole process's CPU per message, rawServer included. The floor
// column is the cheapest subscriber the harness can feed (see
// floorLib), a reference point for the harness's own cost.
func BenchmarkE2E_ReceiveStream(b *testing.B) {
	type variant struct {
		mode mode
		qos  byte
	}
	variants := []variant{{modeCallback, 0}, {modeCallback, 1}, {modeChan, 1}, {modeQueue, 1}}
	for _, l := range append(slices.Clone(libs), floorLib) {
		for _, v := range variants {
			if !l.supports(v.mode) {
				continue
			}
			for _, sz := range []size{size64B, size1KiB} {
				name := fmt.Sprintf("lib=%s/mode=%s/qos=%d/size=%s", l.name, v.mode, v.qos, sz.name)
				b.Run(name, func(b *testing.B) {
					srv := newRawServer(b, l.v5)
					topic := "bench/stream/" + uniqueID("")
					s := newSink(sz.bytes, b.N)
					sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-stream"), addr: srv.addr(), receiveMaximum: receiveWindow},
						topic, v.qos, v.mode, 1, s.onMsg)
					srv.awaitSubscribed(b)
					waitLive(b, s, func() error { return srv.publish(topic, 0, nil, 1) })
					payload := Payload(sz.bytes)

					b.SetBytes(int64(sz.bytes))
					b.ReportAllocs()
					b.ResetTimer()
					cpu := startCPU(b)
					published := srv.publishAsync(topic, v.qos, payload, b.N)
					s.wait(b, 2*time.Minute)
					b.StopTimer()
					cpu.stop()
					if err := published(); err != nil {
						b.Fatalf("raw server: %v", err)
					}
					s.check(b, sub)
				})
			}
		}
	}
}

// rawServer is a minimal MQTT server for one subscriber, the same for
// every library under test. It completes the handshake and the
// subscription for MQTT 5 and MQTT 3.1.1 clients, answers PINGREQ, and
// writes pre-encoded PUBLISH packets in batches, stamped with their
// sequence numbers, without allocating per message.
type rawServer struct {
	b  *testing.B
	ln net.Listener
	v5 bool

	mu   sync.Mutex
	conn net.Conn
	// wmu serializes writes to conn between the publisher and the
	// reader's PINGRESP.
	wmu sync.Mutex
	// credits holds one token per QoS 1 publish the client may still
	// accept: its Receive Maximum, or receiveWindow for MQTT 3.1.1.
	credits    chan struct{}
	stall      *time.Timer
	subscribed chan struct{}
	once       sync.Once
	readErr    atomic.Pointer[error]
	done       chan struct{}
}

func newRawServer(b *testing.B, v5 bool) *rawServer {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	s := &rawServer{b: b, ln: ln, v5: v5, stall: stoppedTimer(), subscribed: make(chan struct{}), done: make(chan struct{})}
	go s.serve()
	b.Cleanup(func() {
		_ = ln.Close()
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
		}
		s.mu.Unlock()
		<-s.done
	})
	return s
}

func (s *rawServer) addr() string { return s.ln.Addr().String() }

// serve accepts the subscriber, completes its handshake, then answers
// SUBSCRIBE and PINGREQ and counts acknowledgements until the
// connection ends.
func (s *rawServer) serve() {
	defer close(s.done)
	conn, err := s.ln.Accept()
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	r := bufio.NewReaderSize(conn, 64<<10)
	if err := s.handshake(conn, r); err != nil {
		s.readErr.Store(&err)
		return
	}
	var frame []byte
	for {
		frame, err = readRawFrame(r, frame)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.readErr.Store(&err)
			}
			return
		}
		switch wire.PacketType(frame[0] >> 4) {
		case wire.SUBSCRIBE:
			if err = s.suback(conn, frame); err == nil {
				s.once.Do(func() { close(s.subscribed) })
			}
		case wire.PUBACK:
			s.credits <- struct{}{}
		case wire.PINGREQ:
			s.wmu.Lock()
			_, err = conn.Write([]byte{byte(wire.PINGRESP) << 4, 0})
			s.wmu.Unlock()
		case wire.DISCONNECT:
			return
		}
		if err != nil {
			s.readErr.Store(&err)
			return
		}
	}
}

// err reports why the reader stopped, if it did.
func (s *rawServer) err() error {
	if p := s.readErr.Load(); p != nil {
		return *p
	}
	return nil
}

// handshake answers CONNECT.
func (s *rawServer) handshake(conn net.Conn, r *bufio.Reader) error {
	frame, err := readRawFrame(r, nil)
	if err != nil {
		return err
	}
	window := receiveWindow
	if s.v5 {
		p, err := wire.NewDecoder(bytes.NewReader(frame)).ReadPacket()
		if err != nil {
			return fmt.Errorf("CONNECT: %w", err)
		}
		if c, ok := p.(*wire.Connect); ok {
			if v, ok := c.Properties.Uint16(wire.PropReceiveMaximum); ok {
				window = int(v)
			}
		}
		p.Release()
		if _, err := wire.WriteConnack(conn, wire.ConnackOpts{}); err != nil {
			return err
		}
	} else if _, err := conn.Write([]byte{byte(wire.CONNACK) << 4, 2, 0, 0}); err != nil {
		return err
	}
	s.credits = make(chan struct{}, window)
	for range window {
		s.credits <- struct{}{}
	}
	return nil
}

// suback answers a SUBSCRIBE, granting the QoS of its (single) filter.
func (s *rawServer) suback(conn net.Conn, frame []byte) error {
	body := frame[len(frame)-int(rawRemaining(frame)):]
	id := binary.BigEndian.Uint16(body)
	qos := body[len(body)-1] & 0x03 // the filter's options byte comes last
	s.wmu.Lock()
	defer s.wmu.Unlock()
	var err error
	if s.v5 {
		_, err = wire.WriteSuback(conn, wire.SubackOpts{PacketID: id, ReasonCodes: []wire.ReasonCode{wire.ReasonCode(qos)}})
	} else {
		_, err = conn.Write([]byte{byte(wire.SUBACK) << 4, 3, byte(id >> 8), byte(id), qos})
	}
	return err
}

func (s *rawServer) awaitSubscribed(b *testing.B) {
	b.Helper()
	select {
	case <-s.subscribed:
	case <-s.done:
		b.Fatalf("raw server: subscriber gone before subscribing: %v", s.err())
	case <-time.After(5 * time.Second):
		b.Fatal("raw server: no SUBSCRIBE")
	}
}

// publish writes n messages of payload to topic at qos as fast as the
// client takes them, keeping at most its window of QoS 1 messages
// unacknowledged. Message i carries sequence number i.
func (s *rawServer) publish(topic string, qos byte, payload []byte, n int) error {
	ids := 1
	if qos > 0 {
		ids = min(4*cap(s.credits), 65535)
	}
	frames := make([][]byte, ids)
	for i := range frames {
		var id uint16
		if qos > 0 {
			id = uint16(i + 1)
		}
		frames[i] = s.encodePublish(topic, qos, id, payload)
	}
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	body := len(frames[0]) - len(payload)
	w := bufio.NewWriterSize(conn, rawBatch*len(frames[0]))
	next := 0
	for sent := 0; sent < n; {
		k := min(rawBatch, n-sent)
		if qos > 0 {
			k = takeCredits(s.credits, s.stall, k)
			if k == 0 {
				return fmt.Errorf("stalled after %d of %d: %v", sent, n, s.err())
			}
		}
		for i := range k {
			stampSeq(frames[next][body:], uint64(sent+i))
			_, _ = w.Write(frames[next])
			next = (next + 1) % ids
		}
		s.wmu.Lock()
		err := w.Flush()
		s.wmu.Unlock()
		if err != nil {
			return err
		}
		sent += k
	}
	return nil
}

func (s *rawServer) publishAsync(topic string, qos byte, payload []byte, n int) (wait func() error) {
	var wg sync.WaitGroup
	var err error
	wg.Go(func() { err = s.publish(topic, qos, payload, n) })
	return func() error {
		wg.Wait()
		return err
	}
}

// encodePublish encodes a PUBLISH for the subscriber's protocol: MQTT 5
// through the wire package, MQTT 3.1.1 (no properties) by hand.
func (s *rawServer) encodePublish(topic string, qos byte, id uint16, payload []byte) []byte {
	if s.v5 {
		out, err := wire.MarshalPublish(wire.PublishOpts{Topic: topic, QoS: qos, PacketID: id, Payload: payload})
		if err != nil {
			s.b.Fatal(err)
		}
		return out
	}
	var body []byte
	body = binary.BigEndian.AppendUint16(body, uint16(len(topic)))
	body = append(body, topic...)
	if qos > 0 {
		body = binary.BigEndian.AppendUint16(body, id)
	}
	body = append(body, payload...)
	out := []byte{byte(wire.PUBLISH)<<4 | qos<<1}
	var vbi [4]byte
	n, err := wire.EncodeVarint(vbi[:], uint32(len(body)))
	if err != nil {
		s.b.Fatal(err)
	}
	return append(append(out, vbi[:n]...), body...)
}

// readRawFrame reads one whole packet — fixed header, Remaining Length
// and body — into buf, growing it when it is too small, and returns it.
func readRawFrame(r *bufio.Reader, buf []byte) ([]byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	frame := append(buf[:0], first)
	n, mul := 0, 1
	for i := 0; ; i++ {
		if i == 4 {
			return nil, errors.New("malformed Remaining Length")
		}
		c, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		frame = append(frame, c)
		n += int(c&0x7f) * mul
		if c&0x80 == 0 {
			break
		}
		mul *= 128
	}
	start := len(frame)
	frame = slices.Grow(frame, n)[:start+n]
	if _, err := io.ReadFull(r, frame[start:]); err != nil {
		return nil, err
	}
	return frame, nil
}

// rawRemaining is the Remaining Length of a frame from readRawFrame.
func rawRemaining(frame []byte) uint32 {
	v, _, _ := wire.DecodeVarint(frame[1:])
	return v
}

// floorLib is the cheapest subscriber the harness can feed: MQTT 5 on
// the wire package, taking each PUBLISH's payload straight from the read
// buffer and acknowledging QoS 1 in one write per read. Its column in
// BenchmarkE2E_ReceiveStream shows what rawServer, the gated sink and the
// loopback socket cost with almost no subscriber: a reference point, not
// an amount to subtract, since how often rawServer wakes depends on the
// subscriber.
var floorLib = lib{name: "floor", v5: true, modes: []mode{modeCallback}, subscribe: subscribeFloor}

func subscribeFloor(b *testing.B, cfg clientConfig, filter string, qos byte, _ mode, _ int, onMsg func([]byte)) subscriber {
	b.Helper()
	conn, err := net.DialTimeout("tcp", cfg.host(b), 5*time.Second)
	if err != nil {
		b.Fatalf("floor dial: %v", err)
	}
	co := wire.ConnectOpts{ClientID: cfg.id, CleanStart: true}
	if cfg.receiveMaximum > 0 {
		co.ReceiveMaximum = &cfg.receiveMaximum
	}
	if _, err := wire.WriteConnect(conn, co); err != nil {
		b.Fatalf("floor CONNECT: %v", err)
	}
	r := bufio.NewReaderSize(conn, 64<<10)
	if _, err := readRawFrame(r, nil); err != nil {
		b.Fatalf("floor CONNACK: %v", err)
	}
	if _, err := wire.WriteSubscribe(conn, wire.SubscribeOpts{
		PacketID: 1, Filters: []wire.SubscribeFilter{{Topic: filter, QoS: qos}},
	}); err != nil {
		b.Fatalf("floor SUBSCRIBE: %v", err)
	}
	if _, err := readRawFrame(r, nil); err != nil {
		b.Fatalf("floor SUBACK: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		w := bufio.NewWriterSize(conn, 4<<10)
		var frame, ack []byte
		for {
			var err error
			if frame, err = readRawFrame(r, frame); err != nil {
				return
			}
			if wire.PacketType(frame[0]>>4) == wire.PUBLISH {
				payload, id := floorPublish(frame)
				onMsg(payload)
				if id != 0 {
					ack = wire.AppendPubResp(ack[:0], wire.PUBACK, id, wire.ReasonSuccess)
					_, _ = w.Write(ack)
				}
			}
			if r.Buffered() == 0 && w.Flush() != nil {
				return
			}
		}
	}()
	return subscriber{dropped: noDrops, stop: func(ctx context.Context) error {
		_ = conn.Close()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
}

// floorPublish returns an MQTT 5 PUBLISH frame's payload and, for QoS 1
// and 2, its packet identifier.
func floorPublish(frame []byte) (payload []byte, id uint16) {
	qos := frame[0] >> 1 & 0x03
	_, n, _ := wire.DecodeVarint(frame[1:])
	body := frame[1+n:]
	body = body[2+int(binary.BigEndian.Uint16(body)):]
	if qos > 0 {
		id = binary.BigEndian.Uint16(body)
		body = body[2:]
	}
	props, n, _ := wire.DecodeVarint(body)
	return body[n+int(props):], id
}

// replayWindow is how many QoS 1 publishes are in flight when the
// connection is cut: the benchmark broker's Receive Maximum.
const replayWindow = 20

// BenchmarkE2E_Reconnect measures session resumption. Each iteration
// lets replayWindow QoS 1 publishes leave the client and vanish (a proxy
// between client and broker swallows them), then cuts the connection.
// ns/op is the time from the cut until a subscriber has received every
// one of them: detecting the loss, reconnecting with the session
// (CleanStart=0) and resending the publishes with DUP set.
// publish-errors/op counts Publish calls that reported an error although
// the message was delivered after the reconnect.
func BenchmarkE2E_Reconnect(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		if !l.v5 {
			continue // the MQTT 3.1.1 client's session resumption is not compared
		}
		b.Run(fmt.Sprintf("lib=%s/inflight=%d/size=%s", l.name, replayWindow, size256B.name), func(b *testing.B) {
			runReconnect(b, l)
		})
	}
}

func runReconnect(b *testing.B, l lib) {
	topic := "bench/reconnect/" + uniqueID("")
	frame, err := wire.EncodePublish(wire.PublishOpts{Topic: topic, QoS: 1, PacketID: 1, Payload: Payload(size256B.bytes)})
	if err != nil {
		b.Fatal(err)
	}
	frameLen := len(*frame)
	wire.ReleaseBuf(frame)

	// Each payload carries its sequence number, so the sink can tell a
	// redelivery, which QoS 1 allows after the reconnect, from a first
	// delivery.
	total := b.N * replayWindow
	payloads := make([][]byte, total)
	for i := range payloads {
		payloads[i] = Payload(size256B.bytes)
		stampSeq(payloads[i], uint64(i))
	}
	s := newSink(size256B.bytes, total)
	s.allowDups = true
	roundDone := make(chan struct{}, 1)
	sub := dialRaw(b, uniqueID("raw-sub"))
	sub.subscribe(topic, 0, func(p *wire.Publish) {
		if s.accept(p.Payload) && s.got.Load()%replayWindow == 0 {
			roundDone <- struct{}{}
		}
	})

	px := newProxy(b, brokerAddr(b))
	p := l.connect(b, clientConfig{id: uniqueID(l.name + "-resume"), addr: px.addr(), session: true})
	ctx := context.Background()
	waitLive(b, s, func() error { return p.publish(ctx, topic, 0, nil) })

	var publishErrors atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for round := range b.N {
		px.swallow()
		var wg sync.WaitGroup
		for k := range replayWindow {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := p.publish(ctx, topic, 1, payloads[round*replayWindow+k]); err != nil {
					publishErrors.Add(1)
				}
			}()
		}
		px.awaitSwallowed(b, replayWindow*frameLen)

		b.StartTimer()
		px.cut()
		select {
		case <-roundDone:
		case <-s.invalid:
			b.Fatalf("round %d: invalid delivery; %s", round, s)
		case <-time.After(30 * time.Second):
			b.Fatalf("round %d: want %d messages; %s", round, (round+1)*replayWindow, s)
		}
		b.StopTimer()
		wg.Wait()
	}
	// The reader stops before the counts are read.
	sub.close()
	if err := s.verify(noDrops); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(publishErrors.Load())/float64(b.N), "publish-errors/op")
	b.ReportMetric(float64(s.dup.Load())/float64(b.N), "duplicates/op")
}

// proxy relays a client's connections to the broker. swallow makes the
// current connection discard what the client sends, so publishes leave
// the client but never reach the broker; cut closes it.
type proxy struct {
	b        *testing.B
	ln       net.Listener
	upstream string

	mu    sync.Mutex
	links []*link
}

// link is one relayed connection pair.
type link struct {
	client, broker net.Conn
	swallowing     atomic.Bool
	swallowed      atomic.Int64
}

func newProxy(b *testing.B, upstream string) *proxy {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	px := &proxy{b: b, ln: ln, upstream: upstream}
	go px.accept()
	b.Cleanup(func() {
		_ = ln.Close()
		px.cut()
	})
	return px
}

func (px *proxy) addr() string { return px.ln.Addr().String() }

func (px *proxy) accept() {
	for {
		client, err := px.ln.Accept()
		if err != nil {
			return
		}
		broker, err := net.Dial("tcp", px.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		lk := &link{client: client, broker: broker}
		px.mu.Lock()
		px.links = append(px.links, lk)
		px.mu.Unlock()
		go lk.pump(client, broker, nil)
		go lk.pump(broker, client, lk)
	}
}

// pump copies src to dst. Traffic from the client (swallow != nil) is
// discarded while the link swallows.
func (lk *link) pump(dst, src net.Conn, swallow *link) {
	defer dst.Close()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if swallow != nil && swallow.swallowing.Load() {
				swallow.swallowed.Add(int64(n))
			} else if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (px *proxy) current() *link {
	px.mu.Lock()
	defer px.mu.Unlock()
	if len(px.links) == 0 {
		px.b.Fatal("proxy: no connection")
	}
	return px.links[len(px.links)-1]
}

func (px *proxy) swallow() { px.current().swallowing.Store(true) }

// awaitSwallowed waits until the current link discarded n bytes.
func (px *proxy) awaitSwallowed(b *testing.B, n int) {
	b.Helper()
	lk := px.current()
	deadline := time.Now().Add(10 * time.Second)
	for lk.swallowed.Load() < int64(n) {
		if time.Now().After(deadline) {
			b.Fatalf("the client sent %d of %d bytes", lk.swallowed.Load(), n)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// cut closes every relayed connection.
func (px *proxy) cut() {
	px.mu.Lock()
	links := px.links
	px.links = nil
	px.mu.Unlock()
	for _, lk := range links {
		_ = lk.client.Close()
		_ = lk.broker.Close()
	}
}

// BenchmarkE2E_SlowConsumer shows what a library holds in memory when
// its consumer stops. The consumer blocks on its first message while the
// raw publisher sends b.N messages at QoS 0 or 1; once the client has
// stopped taking in data the consumer is released and drains. held-B
// is what the library holds at that point: how far the process's live
// heap, measured after a collection, has grown over its level before
// the burst. delivered-% is how many messages reached the consumer
// intact; the library dropped, and counted, the rest.
// A library whose consumer holds back its read loop stops reading, and
// the backlog stays in the broker; so does one that leaves QoS 1
// messages unacknowledged until its consumer takes them. Run with a
// fixed iteration count, e.g. -benchtime=200000x; ns/op is not
// meaningful here.
func BenchmarkE2E_SlowConsumer(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		for _, m := range l.modes {
			for _, qos := range []byte{0, 1} {
				b.Run(fmt.Sprintf("lib=%s/mode=%s/qos=%d/size=%s", l.name, m, qos, size1KiB.name), func(b *testing.B) {
					runSlowConsumer(b, l, m, qos, size1KiB.bytes)
				})
			}
		}
	}
}

func runSlowConsumer(b *testing.B, l lib, m mode, qos byte, size int) {
	topic := "bench/slow/" + uniqueID("")
	release := make(chan struct{})
	// The library may drop what does not fit; the rest must arrive once
	// and intact.
	s := newSink(size, b.N)
	s.allowDrops = true
	var delivered atomic.Int64
	sub := subscribe(b, l, clientConfig{id: uniqueID(l.name + "-slow")}, topic, qos, m, 1, func(p []byte) {
		if len(p) > 0 {
			<-release
			delivered.Add(1)
		}
		s.onMsg(p)
	})
	pub := dialRaw(b, uniqueID("raw-pub"))
	waitLive(b, s, func() error { return pub.publish(topic, 0, nil, 1) })

	runtime.GC()
	base := heapBytes()
	b.ResetTimer()
	if err := pub.publish(topic, qos, Payload(size), b.N); err != nil {
		b.Fatalf("raw publisher: %v", err)
	}
	// The client has stopped taking in data once neither its heap nor
	// its drop count has grown for a while.
	settled := func() (uint64, int64) { return heapBytes(), sub.dropped() }
	h, d := settled()
	for still := 0; still < 5; {
		time.Sleep(100 * time.Millisecond)
		h2, d2 := settled()
		if h2 <= h+h/100 && d2 == d {
			still++
		} else {
			still = 0
		}
		h, d = h2, d2
	}
	// Garbage is collected first: what is left is what the library
	// keeps, not how far the collector let the heap run.
	runtime.GC()
	held := max(int64(heapBytes())-int64(base), 0)
	close(release)
	deadline := time.Now().Add(2 * time.Minute)
	for delivered.Load()+sub.dropped() < int64(b.N) {
		select {
		case <-s.invalid:
			b.Fatalf("invalid delivery: %s", s)
		default:
		}
		if time.Now().After(deadline) {
			b.Fatalf("delivered %d and dropped %d of %d", delivered.Load(), sub.dropped(), b.N)
		}
		time.Sleep(time.Millisecond)
	}
	b.StopTimer()
	s.check(b, sub)
	b.ReportMetric(float64(held), "held-B")
	b.ReportMetric(100*float64(s.got.Load())/float64(b.N), "delivered-%")
}

const heapMetric = "/memory/classes/heap/objects:bytes"

func heapBytes() uint64 {
	s := []metrics.Sample{{Name: heapMetric}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}
