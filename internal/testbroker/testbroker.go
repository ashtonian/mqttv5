// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package testbroker is a scripted MQTT v5 peer for tests. Each accepted
// connection runs the next Script in order; connections beyond the scripted
// ones run the fallback (by default an auto-responder that accepts CONNECT and
// answers SUBSCRIBE, UNSUBSCRIBE, PINGREQ, PUBLISH and PUBREL).
//
// Scripts speak raw MQTT through the wire package, so a test controls the
// exact packet sequence the client sees: drop a connection mid-flow, resend a
// PUBLISH with DUP set, withhold an acknowledgement, advertise limits in
// CONNACK. Every packet the client sends is recorded and can be asserted on.
//
// Each connection has one reader that records packets as they arrive. Next,
// Await and Expect consume that stream, so a script may Hold the connection
// open while the test goroutine asserts on what the client sent.
package testbroker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// DefaultWait bounds Expect/Await calls that are given a zero timeout.
const DefaultWait = 3 * time.Second

var (
	// ErrTimeout is returned by Next when no packet arrives in time.
	ErrTimeout = errors.New("testbroker: timed out waiting for a packet")
	// ErrBrokerClosed is returned by Next once the broker shuts down.
	ErrBrokerClosed = errors.New("testbroker: broker closed")
)

// Script drives one accepted connection. When it returns the connection is
// closed, which the client observes as a network drop.
type Script func(c *Conn)

// Broker accepts TCP connections on 127.0.0.1 and hands each to a Script.
type Broker struct {
	t        testing.TB
	ln       net.Listener
	scripts  []Script
	fallback atomic.Pointer[Script]
	accepted atomic.Int32
	done     chan struct{}
	once     sync.Once
	wg       sync.WaitGroup

	mu    sync.Mutex
	conns []*Conn
}

// New starts a broker that runs scripts[i] for the i-th accepted connection
// and the fallback for every later one. The broker closes on test cleanup.
func New(t testing.TB, scripts ...Script) *Broker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("testbroker: listen: %v", err)
	}
	b := &Broker{t: t, ln: ln, scripts: scripts, done: make(chan struct{})}
	auto := Script(func(c *Conn) { c.AcceptResume(wire.ConnackOpts{}); c.ServeAuto() })
	b.fallback.Store(&auto)
	b.wg.Add(1)
	go b.serve()
	t.Cleanup(b.Close)
	return b
}

// SetFallback replaces the script used once the scripted connections run out.
func (b *Broker) SetFallback(s Script) { b.fallback.Store(&s) }

// URL is the mqtt:// URL clients dial.
func (b *Broker) URL() string { return "mqtt://" + b.ln.Addr().String() }

// Done is closed when the broker shuts down; long-running scripts select on it.
func (b *Broker) Done() <-chan struct{} { return b.done }

// Accepted reports how many connections the broker has accepted.
func (b *Broker) Accepted() int { return int(b.accepted.Load()) }

// Conn returns the i-th accepted connection (0-based), waiting up to d for it.
func (b *Broker) Conn(i int, d time.Duration) *Conn {
	deadline := time.Now().Add(orDefault(d))
	for {
		b.mu.Lock()
		if i < len(b.conns) {
			c := b.conns[i]
			b.mu.Unlock()
			return c
		}
		b.mu.Unlock()
		if time.Now().After(deadline) {
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// DropAll closes every connection accepted so far, as a network failure
// would; the broker keeps accepting new ones.
func (b *Broker) DropAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.conns {
		_ = c.net.Close()
	}
}

// Close stops accepting, closes every connection and waits for scripts.
func (b *Broker) Close() {
	b.once.Do(func() {
		close(b.done)
		_ = b.ln.Close()
		b.mu.Lock()
		for _, c := range b.conns {
			_ = c.net.Close()
		}
		b.mu.Unlock()
	})
	b.wg.Wait()
}

func (b *Broker) serve() {
	defer b.wg.Done()
	for {
		nc, err := b.ln.Accept()
		if err != nil {
			return
		}
		i := int(b.accepted.Add(1)) - 1
		c := &Conn{
			T: b.t, Index: i, net: nc, broker: b,
			arrived: make(chan struct{}, 1),
			gone:    make(chan struct{}),
		}
		b.mu.Lock()
		b.conns = append(b.conns, c)
		b.mu.Unlock()
		script := *b.fallback.Load()
		if i < len(b.scripts) {
			script = b.scripts[i]
		}
		b.wg.Add(2)
		go func() {
			defer b.wg.Done()
			c.pump(wire.NewDecoder(nc))
		}()
		go func() {
			defer b.wg.Done()
			defer func() { _ = nc.Close() }()
			script(c)
		}()
	}
}

// Conn is one broker-side connection.
type Conn struct {
	T     testing.TB
	Index int

	net    net.Conn
	broker *Broker
	wmu    sync.Mutex

	mu      sync.Mutex
	log     []Packet
	unread  []Packet
	readErr error
	arrived chan struct{} // signalled after each recorded packet
	gone    chan struct{} // closed when the reader stops
}

// pump records every packet the client sends until the connection fails.
func (c *Conn) pump(dec *wire.Decoder) {
	defer close(c.gone)
	for {
		raw, err := dec.ReadPacket()
		if err != nil {
			c.mu.Lock()
			c.readErr = err
			c.mu.Unlock()
			return
		}
		p := snapshot(raw)
		raw.Release()
		c.mu.Lock()
		c.log = append(c.log, p)
		c.unread = append(c.unread, p)
		c.mu.Unlock()
		select {
		case c.arrived <- struct{}{}:
		default:
		}
	}
}

// Packet is an owned copy of a packet the client sent, safe to keep.
type Packet struct {
	Type     wire.PacketType
	PacketID uint16
	Reason   wire.ReasonCode
	QoS      byte
	Dup      bool
	Retain   bool
	Topic    string
	Payload  []byte
	Props    []byte // raw property bytes
	Filters  []wire.SubscribeFilter
	Topics   []string // UNSUBSCRIBE
	Connect  *ConnectInfo
	At       time.Time
}

// Properties returns a lazy view over the packet's copied property bytes.
func (p Packet) Properties() wire.Properties { return wire.PropertiesFromBytes(p.Props) }

// ConnectInfo is the part of a CONNECT tests usually assert on.
type ConnectInfo struct {
	ClientID   string
	CleanStart bool
	KeepAlive  uint16
	Username   string
	Password   []byte
	Props      []byte
	HasWill    bool
}

// Properties returns a lazy view over the CONNECT's property bytes.
func (ci *ConnectInfo) Properties() wire.Properties { return wire.PropertiesFromBytes(ci.Props) }

// Log returns a copy of every packet received on this connection so far,
// consumed or not.
func (c *Conn) Log() []Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Packet(nil), c.log...)
}

// Gone is closed once the connection has stopped reading: the client
// disconnected, the script returned, or the broker closed.
func (c *Conn) Gone() <-chan struct{} { return c.gone }

// Next consumes the next unread packet, waiting up to d. ok is false on
// timeout ([ErrTimeout]), broker shutdown ([ErrBrokerClosed]), or when the
// connection has failed (the read error, typically io.EOF).
func (c *Conn) Next(d time.Duration) (p Packet, ok bool, err error) {
	timer := time.NewTimer(orDefault(d))
	defer timer.Stop()
	for {
		c.mu.Lock()
		if len(c.unread) > 0 {
			p = c.unread[0]
			c.unread = c.unread[1:]
			c.mu.Unlock()
			return p, true, nil
		}
		err = c.readErr
		c.mu.Unlock()
		if err != nil {
			return Packet{}, false, err
		}
		select {
		case <-c.arrived:
		case <-c.gone:
		case <-c.broker.done:
			return Packet{}, false, ErrBrokerClosed
		case <-timer.C:
			return Packet{}, false, ErrTimeout
		}
	}
}

// Await reads packets until one of type pt arrives, discarding others.
func (c *Conn) Await(pt wire.PacketType, d time.Duration) (Packet, bool) {
	deadline := time.Now().Add(orDefault(d))
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return Packet{}, false
		}
		p, ok, _ := c.Next(left)
		if !ok {
			return Packet{}, false
		}
		if p.Type == pt {
			return p, true
		}
	}
}

// Expect is Await that fails the test when the packet does not arrive.
func (c *Conn) Expect(pt wire.PacketType, d time.Duration) Packet {
	c.T.Helper()
	p, ok := c.Await(pt, d)
	if !ok {
		c.T.Errorf("testbroker conn %d: no %s within %v", c.Index, pt, orDefault(d))
	}
	return p
}

// ExpectNone fails the test if a packet of type pt arrives within d.
func (c *Conn) ExpectNone(pt wire.PacketType, d time.Duration) {
	c.T.Helper()
	if p, ok := c.Await(pt, d); ok {
		c.T.Errorf("testbroker conn %d: unexpected %s (id=%d)", c.Index, pt, p.PacketID)
	}
}

// AcceptConnect reads CONNECT and replies with CONNACK opts. It returns
// nil when the client leaves without sending CONNECT, as one whose
// attempt was cancelled does; a client that stays and sends none, or
// sends something else first, fails the test.
func (c *Conn) AcceptConnect(opts wire.ConnackOpts) *ConnectInfo {
	c.T.Helper()
	p, ok := c.awaitConnect()
	if !ok {
		return nil
	}
	// A client gone before the CONNACK fails the script's next step.
	_ = c.Write(func(w io.Writer) (int64, error) { return wire.WriteConnack(w, opts) })
	return p.Connect
}

// AcceptResume is AcceptConnect for a broker that keeps every session:
// Session Present is set unless the CONNECT asked for a clean start.
func (c *Conn) AcceptResume(opts wire.ConnackOpts) *ConnectInfo {
	c.T.Helper()
	p, ok := c.awaitConnect()
	if !ok {
		return nil
	}
	opts.SessionPresent = !p.Connect.CleanStart
	// A client gone before the CONNACK fails the script's next step.
	_ = c.Write(func(w io.Writer) (int64, error) { return wire.WriteConnack(w, opts) })
	return p.Connect
}

// awaitConnect reads the connection's first packet, which must be
// CONNECT. ok is false when the client left first.
func (c *Conn) awaitConnect() (Packet, bool) {
	c.T.Helper()
	p, ok, err := c.Next(0)
	switch {
	case ok && p.Type == wire.CONNECT:
		return p, true
	case ok:
		c.T.Errorf("testbroker conn %d: first packet %s, want CONNECT", c.Index, p.Type)
	case errors.Is(err, ErrTimeout):
		c.T.Errorf("testbroker conn %d: no CONNECT within %v", c.Index, DefaultWait)
	}
	return Packet{}, false
}

// ServeSubscribe answers one SUBSCRIBE: each filter gets reason rc, or its
// requested QoS when rc < 0.
func (c *Conn) ServeSubscribe(rc int) (Packet, bool) {
	p, ok := c.Await(wire.SUBSCRIBE, 0)
	if !ok {
		return Packet{}, false
	}
	codes := make([]wire.ReasonCode, len(p.Filters))
	for i, f := range p.Filters {
		if rc < 0 {
			codes[i] = wire.ReasonCode(f.QoS)
		} else {
			codes[i] = wire.ReasonCode(rc)
		}
	}
	if err := c.Suback(p.PacketID, codes...); err != nil {
		return p, false
	}
	return p, true
}

// ServeAuto answers protocol traffic until the connection or broker closes:
// SUBSCRIBE (granted QoS), UNSUBSCRIBE (success), PINGREQ, PUBLISH QoS 1
// (PUBACK) and QoS 2 (PUBREC), PUBREL (PUBCOMP). It returns on DISCONNECT.
func (c *Conn) ServeAuto() {
	for {
		p, ok, err := c.Next(time.Minute)
		if !ok {
			if errors.Is(err, ErrTimeout) {
				continue
			}
			return
		}
		switch p.Type {
		case wire.SUBSCRIBE:
			codes := make([]wire.ReasonCode, len(p.Filters))
			for i, f := range p.Filters {
				codes[i] = wire.ReasonCode(f.QoS)
			}
			err = c.Suback(p.PacketID, codes...)
		case wire.UNSUBSCRIBE:
			codes := make([]wire.ReasonCode, len(p.Topics))
			err = c.Write(func(w io.Writer) (int64, error) {
				return wire.WriteUnsuback(w, wire.UnsubackOpts{PacketID: p.PacketID, ReasonCodes: codes})
			})
		case wire.PINGREQ:
			err = c.Write(wire.WritePingresp)
		case wire.PUBLISH:
			switch p.QoS {
			case 1:
				err = c.Puback(p.PacketID, wire.ReasonSuccess)
			case 2:
				err = c.Pubrec(p.PacketID, wire.ReasonSuccess)
			}
		case wire.PUBREL:
			err = c.Pubcomp(p.PacketID, wire.ReasonSuccess)
		case wire.DISCONNECT:
			return
		}
		if err != nil {
			return
		}
	}
}

// Hold keeps the connection open until the broker closes, the client drops
// the connection, or d elapses (d == 0 means no time limit). Packets keep
// being recorded and can be consumed from another goroutine meanwhile.
func (c *Conn) Hold(d time.Duration) {
	var timeout <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case <-c.broker.done:
	case <-c.gone:
	case <-timeout:
	}
}

// Write serialises one packet to the client. It returns the write error
// (typically because the client closed the connection) rather than logging
// it, so scripts and their goroutines may outlive the test safely.
func (c *Conn) Write(fn func(io.Writer) (int64, error)) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := fn(c.net)
	return err
}

// Raw writes bytes verbatim.
func (c *Conn) Raw(b []byte) error {
	return c.Write(func(w io.Writer) (int64, error) { n, err := w.Write(b); return int64(n), err })
}

// Publish sends a PUBLISH.
func (c *Conn) Publish(opts wire.PublishOpts) error {
	return c.Write(func(w io.Writer) (int64, error) { return wire.WritePublish(w, opts) })
}

// PublishTagged sends a PUBLISH carrying Subscription Identifiers ids, as
// a broker tags a message with the subscriptions it matched (§3.3.4).
func (c *Conn) PublishTagged(opts wire.PublishOpts, ids ...uint32) error {
	frame, err := wire.MarshalPublish(opts)
	if err != nil {
		return err
	}
	return c.Raw(withSubscriptionIDs(frame, ids))
}

// withSubscriptionIDs appends Subscription Identifier properties to an
// encoded PUBLISH.
func withSubscriptionIDs(frame []byte, ids []uint32) []byte {
	_, n, _ := wire.DecodeVarint(frame[1:])
	body := frame[1+n:]
	off := 2 + int(binary.BigEndian.Uint16(body))
	if qos := frame[0] >> 1 & 3; qos > 0 {
		off += 2
	}
	propsLen, m, _ := wire.DecodeVarint(body[off:])
	props := slices.Clone(body[off+m : off+m+int(propsLen)])
	payload := body[off+m+int(propsLen):]
	var vbi [4]byte
	for _, id := range ids {
		k, _ := wire.EncodeVarint(vbi[:], id)
		props = append(append(props, wire.PropSubscriptionIdentifier), vbi[:k]...)
	}
	k, _ := wire.EncodeVarint(vbi[:], uint32(len(props)))
	newBody := append(slices.Clone(body[:off]), vbi[:k]...)
	newBody = append(append(newBody, props...), payload...)
	k, _ = wire.EncodeVarint(vbi[:], uint32(len(newBody)))
	out := append([]byte{frame[0]}, vbi[:k]...)
	return append(out, newBody...)
}

// Puback sends a PUBACK.
func (c *Conn) Puback(id uint16, rc wire.ReasonCode) error {
	return c.Write(func(w io.Writer) (int64, error) {
		return wire.WritePuback(w, wire.PubRespOpts{PacketID: id, ReasonCode: rc})
	})
}

// Pubrec sends a PUBREC.
func (c *Conn) Pubrec(id uint16, rc wire.ReasonCode) error {
	return c.Write(func(w io.Writer) (int64, error) {
		return wire.WritePubrec(w, wire.PubRespOpts{PacketID: id, ReasonCode: rc})
	})
}

// Pubrel sends a PUBREL.
func (c *Conn) Pubrel(id uint16, rc wire.ReasonCode) error {
	return c.Write(func(w io.Writer) (int64, error) {
		return wire.WritePubrel(w, wire.PubRespOpts{PacketID: id, ReasonCode: rc})
	})
}

// Pubcomp sends a PUBCOMP.
func (c *Conn) Pubcomp(id uint16, rc wire.ReasonCode) error {
	return c.Write(func(w io.Writer) (int64, error) {
		return wire.WritePubcomp(w, wire.PubRespOpts{PacketID: id, ReasonCode: rc})
	})
}

// Suback sends a SUBACK with one reason code per filter.
func (c *Conn) Suback(id uint16, codes ...wire.ReasonCode) error {
	return c.Write(func(w io.Writer) (int64, error) {
		return wire.WriteSuback(w, wire.SubackOpts{PacketID: id, ReasonCodes: codes})
	})
}

// Disconnect sends a DISCONNECT.
func (c *Conn) Disconnect(opts wire.DisconnectOpts) error {
	return c.Write(func(w io.Writer) (int64, error) { return wire.WriteDisconnect(w, opts) })
}

// Close drops the connection.
func (c *Conn) Close() { _ = c.net.Close() }

func snapshot(raw wire.Packet) Packet {
	p := Packet{Type: raw.Type(), At: time.Now()}
	switch x := raw.(type) {
	case *wire.Publish:
		p.PacketID, p.QoS, p.Dup, p.Retain = x.PacketID, x.QoS, x.Dup, x.Retain
		p.Topic = strings.Clone(x.Topic)
		p.Payload = bytes.Clone(x.Payload)
		p.Props = bytes.Clone(x.Properties.Raw())
	case *wire.PubResp:
		p.PacketID, p.Reason = x.PacketID, x.ReasonCode
		p.Props = bytes.Clone(x.Properties.Raw())
	case *wire.Subscribe:
		p.PacketID = x.PacketID
		p.Props = bytes.Clone(x.Properties.Raw())
		for _, f := range x.Filters {
			f.Topic = strings.Clone(f.Topic)
			p.Filters = append(p.Filters, f)
		}
	case *wire.Unsubscribe:
		p.PacketID = x.PacketID
		p.Props = bytes.Clone(x.Properties.Raw())
		for _, t := range x.Topics {
			p.Topics = append(p.Topics, strings.Clone(t))
		}
	case *wire.Disconnect:
		p.Reason = x.ReasonCode
		p.Props = bytes.Clone(x.Properties.Raw())
	case *wire.Auth:
		p.Reason = x.ReasonCode
		p.Props = bytes.Clone(x.Properties.Raw())
	case *wire.Connect:
		p.Props = bytes.Clone(x.Properties.Raw())
		p.Connect = &ConnectInfo{
			ClientID:   strings.Clone(x.ClientID),
			CleanStart: x.CleanStart,
			KeepAlive:  x.KeepAlive,
			Username:   strings.Clone(x.Username),
			Password:   bytes.Clone(x.Password),
			Props:      p.Props,
			HasWill:    x.Will != nil,
		}
	}
	return p
}

func orDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultWait
	}
	return d
}
