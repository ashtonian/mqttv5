// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

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
					sub := l.subscribe(b, clientConfig{id: uniqueID(l.name + "-stream"), addr: srv.addr(), receiveMaximum: receiveWindow},
						topic, v.qos, v.mode, 1, s.onMsg)
					srv.awaitSubscribed(b)
					waitLive(b, s, func() error { return srv.publish(topic, 0, nil, 1) })
					payload := Payload(sz.bytes)

					b.SetBytes(int64(sz.bytes))
					b.ReportAllocs()
					b.ResetTimer()
					cpu := startCPU(b)
					published := srv.publishAsync(topic, v.qos, payload, b.N)
					await(b, s.done, 2*time.Minute, s.String)
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
// BenchmarkE2E_ReceiveStream shows what rawServer, the sink and the
// loopback socket cost with almost no subscriber: a reference point, not
// an amount to subtract, since how often rawServer wakes depends on the
// subscriber.
var floorLib = lib{name: "floor", v5: true, modes: []mode{modeCallback}, subscribe: subscribeFloor}

func subscribeFloor(b *testing.B, cfg clientConfig, filter string, qos byte, _ mode, _ int, onMsg func([]byte)) *subscription {
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
	return newSubscription(b, func() int64 { return 0 }, func() {
		_ = conn.Close()
		<-done
	})
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
