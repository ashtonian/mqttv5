// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// faultProxy sits between one client and the broker and relays MQTT
// packet by packet. A test can cut every connection, refuse new ones
// for a while (so the broker's session expires), or drop packets of
// chosen types that the client sends.
type faultProxy struct {
	ln       net.Listener
	upstream string
	trace    *packetTrace

	drop    atomic.Pointer[func(wire.PacketType) bool]
	dropped atomic.Int64

	mu          sync.Mutex
	conns       []net.Conn
	accepted    int
	refuseUntil time.Time
}

// newFaultProxy relays to upstream; with traced it also records the
// packets it relays (see packetTrace).
func newFaultProxy(t *testing.T, upstream string, traced bool) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	px := &faultProxy{ln: ln, upstream: upstream}
	if traced {
		px.trace = newPacketTrace()
	}
	go px.accept()
	t.Cleanup(func() {
		_ = ln.Close()
		px.cut()
	})
	return px
}

// url is the broker URL clients use to go through the proxy.
func (px *faultProxy) url() string { return "mqtt://" + px.ln.Addr().String() }

// dropFromClient discards every packet the client sends whose type
// matches; nil relays everything again.
func (px *faultProxy) dropFromClient(match func(wire.PacketType) bool) {
	if match == nil {
		px.drop.Store(nil)
		return
	}
	px.drop.Store(&match)
}

// refuse closes connections accepted during the next d.
func (px *faultProxy) refuse(d time.Duration) {
	px.mu.Lock()
	px.refuseUntil = time.Now().Add(d)
	px.mu.Unlock()
}

// cut closes every relayed connection and reports how many it closed.
func (px *faultProxy) cut() int {
	px.mu.Lock()
	conns := px.conns
	px.conns = nil
	px.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return len(conns) / 2
}

// awaitDropped waits until n packets have been dropped.
func (px *faultProxy) awaitDropped(t *testing.T, n int64) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); px.dropped.Load() < n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d packets dropped", px.dropped.Load(), n)
		}
	}
}

func (px *faultProxy) accept() {
	for {
		client, err := px.ln.Accept()
		if err != nil {
			return
		}
		px.mu.Lock()
		refused := time.Now().Before(px.refuseUntil)
		px.mu.Unlock()
		if refused {
			_ = client.Close()
			continue
		}
		broker, err := net.Dial("tcp", px.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		px.mu.Lock()
		px.conns = append(px.conns, client, broker)
		px.accepted++
		n := px.accepted
		px.mu.Unlock()
		go px.relay(broker, client, fmt.Sprintf("c%d>", n), true)
		go px.relay(client, broker, fmt.Sprintf("c%d<", n), false)
	}
}

func (px *faultProxy) relay(dst, src net.Conn, dir string, fromClient bool) {
	defer dst.Close()
	r := bufio.NewReaderSize(src, 64<<10)
	for {
		frame, err := readFrame(r)
		if err != nil {
			return
		}
		if fromClient {
			if match := px.drop.Load(); match != nil && (*match)(wire.PacketType(frame[0]>>4)) {
				px.dropped.Add(1)
				continue
			}
		}
		if px.trace != nil {
			px.trace.recordFrame(dir, frame)
		}
		if _, err := dst.Write(frame); err != nil {
			return
		}
	}
}

// readFrame reads one whole MQTT packet: fixed header, Remaining
// Length and body.
func readFrame(r *bufio.Reader) ([]byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	hdr := []byte{first}
	n, mul := 0, 1
	for i := 0; ; i++ {
		if i == 4 {
			return nil, errors.New("malformed Remaining Length")
		}
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		hdr = append(hdr, b)
		n += int(b&0x7f) * mul
		if b&0x80 == 0 {
			break
		}
		mul *= 128
	}
	frame := make([]byte, len(hdr)+n)
	copy(frame, hdr)
	if _, err := io.ReadFull(r, frame[len(hdr):]); err != nil {
		return nil, err
	}
	return frame, nil
}

// packetTrace keeps, for each message payload, the PUBLISH and
// acknowledgements that carried it, plus every CONNACK.
type packetTrace struct {
	start time.Time
	mu    sync.Mutex
	// byID maps a packet identifier of the client — one session across
	// all its connections — to the payload of the PUBLISH it currently
	// belongs to.
	byID     map[uint16]string
	events   map[string][]string
	connacks []string
}

func newPacketTrace() *packetTrace {
	return &packetTrace{start: time.Now(), byID: map[uint16]string{}, events: map[string][]string{}}
}

func (pt *packetTrace) recordFrame(dir string, frame []byte) {
	p, err := wire.NewDecoder(bytes.NewReader(frame)).ReadPacket()
	if err != nil {
		return
	}
	defer p.Release()
	at := time.Since(pt.start).Round(time.Millisecond)
	pt.mu.Lock()
	defer pt.mu.Unlock()
	switch x := p.(type) {
	case *wire.Publish:
		if x.QoS == 0 {
			return
		}
		payload := string(x.Payload)
		pt.byID[x.PacketID] = payload
		pt.events[payload] = append(pt.events[payload], fmt.Sprintf("%v %s PUBLISH#%d qos=%d dup=%v topic=%q",
			at, dir, x.PacketID, x.QoS, x.Dup, x.Topic))
	case *wire.PubResp:
		if payload, ok := pt.byID[x.PacketID]; ok {
			pt.events[payload] = append(pt.events[payload], fmt.Sprintf("%v %s %s#%d rc=0x%02x",
				at, dir, x.Type(), x.PacketID, byte(x.ReasonCode)))
		}
	case *wire.Connack:
		pt.connacks = append(pt.connacks, fmt.Sprintf("%v %s CONNACK sp=%v rc=0x%02x",
			at, dir, x.SessionPresent, byte(x.ReasonCode)))
	}
}

func (pt *packetTrace) history(payload string) []string {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return pt.events[payload]
}

func (pt *packetTrace) sessions() []string {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return append([]string(nil), pt.connacks...)
}
