// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/wire"
)

// Message is one inbound PUBLISH as seen by one subscription handler.
//
// By default a Message owns its memory: Topic, Payload and Properties are
// copied out of the network frame before any handler runs, stay valid for
// as long as the Message is reachable, and may be retained freely. When a
// PUBLISH matches several subscriptions, each handler receives its own
// *Message; the handles share the same read-only bytes.
//
// Subscriptions created with [SubZeroCopy] instead receive Messages whose
// Topic, Payload and Properties alias the pooled network frame. Those fields
// are valid only until every handler's Ack has returned.
//
// Ack acknowledges the message to the broker (PUBACK for QoS 1, PUBREC for
// QoS 2, emitted in arrival order per §4.6) once every handler that received
// the PUBLISH has acked. Ack is idempotent per Message: later calls are
// no-ops and never affect any other message.
type Message struct {
	Topic      string
	Payload    []byte
	Properties Properties
	QoS        byte
	Retain     bool
	Dup        bool
	PacketID   uint16

	d     *delivery
	acked atomic.Bool
}

// Ack acknowledges the message for this handler. It is safe to call more
// than once and from any goroutine; only the first call counts. For QoS 0
// messages Ack is only needed on zero-copy subscriptions, where it returns
// the network frame to the pool.
func (m *Message) Ack() error {
	if m.d == nil || !m.acked.CompareAndSwap(false, true) {
		return nil
	}
	m.d.release(false)
	return nil
}

// ackOnReader acks m on the read loop, which sends the acknowledgement
// itself before it next waits for the broker (see Client.sendReady).
func (m *Message) ackOnReader() {
	if m.d == nil || !m.acked.CompareAndSwap(false, true) {
		return
	}
	m.d.release(true)
}

// CloneTopic returns a copy of Topic. Only needed on zero-copy
// subscriptions; owned messages may simply keep m.Topic.
func (m *Message) CloneTopic() string { return strings.Clone(m.Topic) }

// ClonePayload returns a copy of Payload. Only needed on zero-copy
// subscriptions; owned messages may simply keep m.Payload.
func (m *Message) ClonePayload() []byte { return bytes.Clone(m.Payload) }

// delivery is one inbound PUBLISH shared by every handler it matched. The
// broker acknowledgement is queued, and a zero-copy frame released, when
// the last handler's Message is acked.
type delivery struct {
	client *Client
	refs   atomic.Int32
	in     *inflight.In  // QoS 1/2 flow; nil for QoS 0
	frame  *wire.Publish // zero-copy deliveries only
	first  Message       // the first handler's handle, saving one allocation
}

// release drops one handle; the last queues the broker acknowledgement.
// onReader means the read loop is releasing it and sends the
// acknowledgement itself.
func (d *delivery) release(onReader bool) {
	if d.refs.Add(-1) != 0 {
		return
	}
	switch {
	case d.in == nil:
	case onReader:
		d.client.engine.AckDeferred(d.in)
	default:
		d.client.engine.Ack(d.in)
	}
	if d.frame != nil {
		d.frame.Release()
		d.frame = nil
	}
}

// fillOwned copies pub's topic, payload and property bytes into one
// exactly-sized allocation owned by m. The copy is never mutated afterwards,
// which is what makes the unsafe.String view of the topic sound; Payload is
// capacity-limited so appending to it cannot overwrite the property bytes.
func fillOwned(m *Message, pub *wire.Publish) {
	props := pub.Properties.Raw()
	t, p := len(pub.Topic), len(pub.Payload)
	buf := make([]byte, t+p+len(props))
	copy(buf, pub.Topic)
	copy(buf[t:], pub.Payload)
	copy(buf[t+p:], props)
	if t > 0 {
		m.Topic = unsafe.String(&buf[0], t)
	}
	m.Payload = buf[t : t+p : t+p]
	if len(props) > 0 {
		m.Properties = Properties{wire.PropertiesFromBytes(buf[t+p:])}
	}
	setHeader(m, pub)
}

// fillZeroCopy points m's fields at the frame.
func fillZeroCopy(m *Message, pub *wire.Publish) {
	m.Topic, m.Payload, m.Properties = pub.Topic, pub.Payload, Properties{pub.Properties}
	setHeader(m, pub)
}

func setHeader(m *Message, pub *wire.Publish) {
	m.QoS, m.Retain, m.Dup, m.PacketID = pub.QoS, pub.Retain, pub.Dup, pub.PacketID
}

// handleFor returns another handle onto the same delivery. Fields are
// assigned one by one: Message holds an atomic and must not be copied.
func handleFor(src *Message) *Message {
	return &Message{
		Topic: src.Topic, Payload: src.Payload, Properties: src.Properties,
		QoS: src.QoS, Retain: src.Retain, Dup: src.Dup, PacketID: src.PacketID,
		d: src.d,
	}
}

// route is one subscription's entry in the topic trie.
type route struct {
	deliver  HandlerFunc
	zeroCopy bool
	// sync routes (SubscribeCallback) run user code on the read goroutine
	// and are not held under mu while doing so.
	sync bool

	mu     sync.RWMutex
	closed bool
}

// dispatch hands m to the subscription unless it has been closed, in which
// case m is acked so the broker isn't left waiting. It runs on the read
// loop. Channel and queue sends happen under the read lock, so close can
// never race a send on a closed channel.
func (r *route) dispatch(m *Message) {
	if r.sync {
		r.mu.RLock()
		closed := r.closed
		r.mu.RUnlock()
		if closed {
			m.ackOnReader()
			return
		}
		r.deliver(m)
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		m.ackOnReader()
		return
	}
	r.deliver(m)
}

// close marks the route closed and runs onClose (which closes the
// subscription's channel or queue) exactly once.
func (r *route) close(onClose func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	if onClose != nil {
		onClose()
	}
}
