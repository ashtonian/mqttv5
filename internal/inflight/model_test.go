// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/soak"
	"github.com/ashtonian/mqttv5/wire"
)

// TestSessionFaultInjection drives the engine against a reference broker
// model through randomly interleaved publishes in both directions,
// application acks, packet loss at connection drops, and resumptions
// with and without the session. Every run checks:
//
//   - a QoS 1/2 message from the broker is acknowledged only after the
//     application acked it;
//   - a QoS 2 message reaches the application, and the broker's
//     subscribers, at most once while the session survives;
//   - the client sends PUBLISH packets in their original order on every
//     connection, including resends;
//   - a new PUBLISH never reuses a packet identifier the broker still
//     holds for a live flow;
//   - the client never exceeds the broker's Receive Maximum;
//   - once the network is stable every flow completes.
func TestSessionFaultInjection(t *testing.T) {
	base, n := soak.Seeds(t, 10000, 500)
	for seed := base; seed < base+n; seed++ {
		m := newModel(t, seed)
		if err := m.run(); err != nil {
			t.Fatalf("seed %d: %v\n%s", seed, err, m.trace.String())
		}
	}
}

type msgKey struct {
	epoch int
	msg   int
}

type rxFlow struct { // a client PUBLISH the broker is tracking
	qos     byte
	msg     int
	counted bool // counts toward this connection's Receive Maximum
}

type txFlow struct { // a broker PUBLISH to the client
	qos   byte
	msg   int
	phase wire.PacketType // the ack the broker waits for
	sent  bool
}

type pkt struct {
	typ wire.PacketType
	id  uint16
	qos byte
	dup bool
	rc  wire.ReasonCode
	msg int
}

type appMsg struct {
	in  *In
	msg int
}

type model struct {
	t   *testing.T
	rng *rand.Rand
	e   *Engine

	link      *fakeLink
	gen       uint64
	connected bool
	epoch     int // incremented when the broker loses the session

	// client application
	nextMsg  int
	pending  map[*Out]int
	unacked  []appMsg
	appAcked map[int]bool
	appGot   map[msgKey]int

	// broker
	rm       int
	inflight int // client PUBLISHes counted against Receive Maximum on this connection
	rx       map[uint16]*rxFlow
	onward   map[msgKey]int
	lastPub  int // highest message number received on this connection
	tx       map[uint16]*txFlow
	txOrder  []uint16
	txNextID uint16
	toClient []pkt

	trace bytes.Buffer
}

func newModel(t *testing.T, seed uint64) *model {
	m := &model{
		t:        t,
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		pending:  make(map[*Out]int),
		appAcked: make(map[int]bool),
		appGot:   make(map[msgKey]int),
		rx:       make(map[uint16]*rxFlow),
		onward:   make(map[msgKey]int),
		tx:       make(map[uint16]*txFlow),
		txNextID: 1,
	}
	m.e = New(Config{Logger: discardLogger(), Now: func() time.Time { return time.Unix(1, 0) }})
	// A small identifier space makes reuse, and exhaustion, routine.
	m.e.ids = newIDTable(uint16(4 + m.rng.IntN(28)))
	return m
}

func (m *model) logf(format string, args ...any) {
	fmt.Fprintf(&m.trace, format+"\n", args...)
}

func (m *model) run() error {
	if err := m.connect(false); err != nil {
		return err
	}
	for step := 0; step < 300; step++ {
		var err error
		switch r := m.rng.IntN(100); {
		case r < 14:
			err = m.appPublish()
		case r < 28:
			m.appAck()
		case r < 34:
			m.brokerPublish()
		case r < 40:
			m.brokerAck(false)
		case r < 66:
			err = m.clientToBroker(true)
		case r < 94:
			err = m.brokerToClient(true)
		case m.connected:
			m.drop()
		default:
			err = m.connect(m.rng.IntN(10) < 7)
		}
		if err != nil {
			return err
		}
		if !m.connected && m.rng.IntN(4) == 0 {
			if err := m.connect(m.rng.IntN(10) < 7); err != nil {
				return err
			}
		}
	}
	return m.settle()
}

// connect brings up a connection; keep says whether the broker still has
// the session.
func (m *model) connect(keep bool) error {
	if m.connected {
		return nil
	}
	if !keep {
		m.epoch++
		clear(m.rx)
		clear(m.tx)
		m.txOrder = nil
	}
	m.toClient = nil
	m.rm = 1 + m.rng.IntN(8)
	m.inflight = 0
	m.lastPub = -1
	for _, f := range m.rx {
		f.counted = false
	}
	m.link = &fakeLink{}
	gen, _, err := m.e.Connected(ConnInfo{SessionPresent: keep, ReceiveMaximum: uint16(m.rm), Link: m.link})
	if err != nil {
		return err
	}
	m.gen, m.connected = gen, true
	m.logf("connect keep=%v rm=%d epoch=%d", keep, m.rm, m.epoch)
	if keep {
		// §4.4: the broker resends its unacknowledged flows in order.
		for _, id := range m.txOrder {
			f := m.tx[id]
			switch f.phase {
			case wire.PUBACK, wire.PUBREC:
				m.toClient = append(m.toClient, pkt{typ: wire.PUBLISH, id: id, qos: f.qos, dup: f.sent, msg: f.msg})
				f.sent = true
			case wire.PUBCOMP:
				m.toClient = append(m.toClient, pkt{typ: wire.PUBREL, id: id})
			}
		}
	}
	return nil
}

func (m *model) drop() {
	m.e.Disconnected(m.link)
	m.connected = false
	m.toClient = nil
	m.logf("drop")
}

func (m *model) appPublish() error {
	exhausted, cancel := context.WithCancel(context.Background())
	cancel()
	id, err := m.e.AllocateID(exhausted, OwnerPublish, nil)
	if errors.Is(err, ErrIDsExhausted) {
		return nil
	}
	if err != nil {
		return err
	}
	msg := m.nextMsg
	m.nextMsg++
	qos := byte(1 + m.rng.IntN(2))
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(msg))
	packet, err := wire.MarshalPublish(wire.PublishOpts{Topic: "m", Payload: payload, QoS: qos, PacketID: id})
	if err != nil {
		return err
	}
	o, _, err := m.e.Register(context.Background(), Message{ID: id, QoS: qos, Packet: packet})
	if err != nil {
		return err
	}
	m.pending[o] = msg
	m.logf("app publish msg=%d id=%d qos=%d", msg, id, qos)
	return nil
}

func (m *model) appAck() {
	if len(m.unacked) == 0 {
		return
	}
	i := m.rng.IntN(len(m.unacked))
	a := m.unacked[i]
	m.unacked = append(m.unacked[:i], m.unacked[i+1:]...)
	m.appAcked[a.msg] = true
	m.e.Ack(a.in)
	m.logf("app ack msg=%d", a.msg)
}

func (m *model) brokerPublish() {
	if len(m.tx) > 200 {
		return
	}
	id := m.txNextID
	for m.tx[id] != nil || id == 0 {
		id++
	}
	m.txNextID = id + 1
	f := &txFlow{qos: byte(1 + m.rng.IntN(2)), msg: 1_000_000 + m.nextMsg}
	m.nextMsg++
	f.phase = wire.PUBACK
	if f.qos == 2 {
		f.phase = wire.PUBREC
	}
	m.tx[id] = f
	m.txOrder = append(m.txOrder, id)
	if m.connected {
		m.toClient = append(m.toClient, pkt{typ: wire.PUBLISH, id: id, qos: f.qos, msg: f.msg})
		f.sent = true
	}
	m.logf("broker publish msg=%d id=%d qos=%d", f.msg, id, f.qos)
}

// clientToBroker moves one batch from the client to the broker; with
// lossy set, the connection may drop part way through it.
func (m *model) clientToBroker(lossy bool) error {
	if !m.connected {
		return nil
	}
	var f Frames
	if m.e.Collect(m.gen, ^uint64(0)>>1, &f) == 0 {
		return nil
	}
	var buf bytes.Buffer
	if _, err := f.WriteTo(&buf); err != nil {
		return err
	}
	pkts, err := decodeModel(buf.Bytes())
	if err != nil {
		return err
	}
	cut := len(pkts)
	dropAfter := lossy && m.rng.IntN(10) == 0
	if dropAfter {
		cut = m.rng.IntN(len(pkts) + 1)
	}
	for _, p := range pkts[:cut] {
		if err := m.brokerHandle(p); err != nil {
			return err
		}
	}
	if dropAfter {
		m.drop()
	}
	return nil
}

func (m *model) brokerHandle(p pkt) error {
	m.logf("broker <- %s id=%d dup=%v msg=%d rc=%#x inflight=%d", p.typ, p.id, p.dup, p.msg, byte(p.rc), m.inflight)
	switch p.typ {
	case wire.PUBLISH:
		if p.msg <= m.lastPub {
			return fmt.Errorf("PUBLISH msg=%d after msg=%d on one connection: send order broken", p.msg, m.lastPub)
		}
		m.lastPub = p.msg
		live := m.rx[p.id]
		if live != nil && !p.dup {
			return fmt.Errorf("new PUBLISH msg=%d reuses packet id %d still live for msg=%d", p.msg, p.id, live.msg)
		}
		if live != nil && live.msg != p.msg {
			return fmt.Errorf("DUP PUBLISH id=%d carries msg=%d, broker holds msg=%d", p.id, p.msg, live.msg)
		}
		if live == nil || !live.counted {
			m.inflight++
			if m.inflight > m.rm {
				return fmt.Errorf("client has %d PUBLISHes in flight, Receive Maximum is %d", m.inflight, m.rm)
			}
		}
		if p.qos == 1 {
			if live == nil {
				m.onward[msgKey{m.epoch, p.msg}]++
				live = &rxFlow{qos: 1, msg: p.msg}
				m.rx[p.id] = live
			}
			live.counted = true // PUBACK follows in brokerAck
			return nil
		}
		if live == nil {
			k := msgKey{m.epoch, p.msg}
			m.onward[k]++
			if m.onward[k] > 1 {
				return fmt.Errorf("QoS 2 msg=%d delivered onward %d times in one session", p.msg, m.onward[k])
			}
			live = &rxFlow{qos: 2, msg: p.msg}
			m.rx[p.id] = live
		}
		live.counted = true
		m.toClient = append(m.toClient, pkt{typ: wire.PUBREC, id: p.id})
	case wire.PUBREL:
		if f := m.rx[p.id]; f != nil {
			if f.counted {
				m.inflight--
			}
			delete(m.rx, p.id)
			m.toClient = append(m.toClient, pkt{typ: wire.PUBCOMP, id: p.id})
		} else {
			m.toClient = append(m.toClient, pkt{typ: wire.PUBCOMP, id: p.id, rc: wire.ReasonPacketIdentifierNotFound})
		}
	case wire.PUBACK, wire.PUBREC:
		f := m.tx[p.id]
		if f == nil || f.phase != p.typ {
			if f != nil && f.phase == wire.PUBCOMP && p.typ == wire.PUBREC {
				m.toClient = append(m.toClient, pkt{typ: wire.PUBREL, id: p.id})
			}
			return nil
		}
		if !m.appAcked[f.msg] {
			return fmt.Errorf("%s for msg=%d before the application acked it", p.typ, f.msg)
		}
		if p.typ == wire.PUBACK {
			m.forgetTx(p.id)
			return nil
		}
		f.phase = wire.PUBCOMP
		m.toClient = append(m.toClient, pkt{typ: wire.PUBREL, id: p.id})
	case wire.PUBCOMP:
		if f := m.tx[p.id]; f != nil && f.phase == wire.PUBCOMP {
			m.forgetTx(p.id)
		}
	}
	return nil
}

// brokerAck acknowledges some of the QoS 1 PUBLISHes the broker holds.
func (m *model) brokerAck(all bool) {
	if !m.connected {
		return
	}
	for _, id := range slices.Sorted(maps.Keys(m.rx)) {
		f := m.rx[id]
		if f.qos != 1 || !f.counted || (!all && m.rng.IntN(2) == 0) {
			continue
		}
		delete(m.rx, id)
		m.inflight--
		m.toClient = append(m.toClient, pkt{typ: wire.PUBACK, id: id})
		m.logf("broker acks id=%d inflight=%d", id, m.inflight)
	}
}

func (m *model) forgetTx(id uint16) {
	delete(m.tx, id)
	for i, x := range m.txOrder {
		if x == id {
			m.txOrder = append(m.txOrder[:i], m.txOrder[i+1:]...)
			return
		}
	}
}

// brokerToClient delivers queued broker packets; with lossy set, the
// connection may drop part way through.
func (m *model) brokerToClient(lossy bool) error {
	if !m.connected || len(m.toClient) == 0 {
		return nil
	}
	batch := m.toClient
	m.toClient = nil
	cut := len(batch)
	dropAfter := lossy && m.rng.IntN(10) == 0
	if dropAfter {
		cut = m.rng.IntN(len(batch) + 1)
	}
	for _, p := range batch[:cut] {
		m.logf("client <- %s id=%d dup=%v msg=%d", p.typ, p.id, p.dup, p.msg)
		switch p.typ {
		case wire.PUBACK:
			m.e.HandlePuback(p.id, 0, nil)
		case wire.PUBREC:
			m.e.HandlePubrec(p.id, 0, nil)
		case wire.PUBREL:
			m.e.HandlePubrel(p.id)
		case wire.PUBCOMP:
			m.e.HandlePubcomp(p.id)
		case wire.PUBLISH:
			in, deliver, err := m.e.Receive(p.id, p.qos)
			if err != nil {
				return err
			}
			if !deliver {
				continue
			}
			k := msgKey{m.epoch, p.msg}
			m.appGot[k]++
			if p.qos == 2 && m.appGot[k] > 1 {
				return fmt.Errorf("QoS 2 msg=%d delivered to the application %d times in one session", p.msg, m.appGot[k])
			}
			m.unacked = append(m.unacked, appMsg{in: in, msg: p.msg})
		}
	}
	if dropAfter {
		m.drop()
	}
	return nil
}

// settle runs a loss-free network until everything completes.
func (m *model) settle() error {
	if err := m.connect(true); err != nil {
		return err
	}
	for range 10000 {
		for len(m.unacked) > 0 {
			m.appAck()
		}
		m.brokerAck(true)
		if err := m.clientToBroker(false); err != nil {
			return err
		}
		if err := m.brokerToClient(false); err != nil {
			return err
		}
		for o := range m.pending {
			select {
			case <-o.Done():
				if o.Err() != nil {
					return fmt.Errorf("publish msg=%d failed: %v", m.pending[o], o.Err())
				}
				delete(m.pending, o)
			default:
			}
		}
		if len(m.pending) == 0 && len(m.tx) == 0 && len(m.rx) == 0 && len(m.toClient) == 0 && m.e.OutboundLen() == 0 {
			return nil
		}
	}
	return fmt.Errorf("did not settle: %d publishes pending, %d broker flows, %d client-originated flows at broker",
		len(m.pending), len(m.tx), len(m.rx))
}

func decodeModel(b []byte) ([]pkt, error) {
	dec := wire.NewDecoder(bytes.NewReader(b))
	var out []pkt
	for {
		p, err := dec.ReadPacket()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		switch x := p.(type) {
		case *wire.Publish:
			out = append(out, pkt{typ: wire.PUBLISH, id: x.PacketID, qos: x.QoS, dup: x.Dup, msg: int(binary.BigEndian.Uint64(x.Payload))})
		case *wire.PubResp:
			out = append(out, pkt{typ: x.Type(), id: x.PacketID, rc: x.ReasonCode})
		}
		p.Release()
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
