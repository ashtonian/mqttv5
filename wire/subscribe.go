// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// SubscribeFilter is one entry in the SUBSCRIBE payload: a topic filter
// plus its subscription options byte (§3.8.3.1).
type SubscribeFilter struct {
	Topic             string
	QoS               byte // 0, 1, 2
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte // 0, 1, 2 — see MQTT v5 §3.8.3.1
}

// Subscribe is a decoded SUBSCRIBE packet (§3.8).
//
// Filters slice references the decoded list (one allocation per packet
// decode — SUBSCRIBE is infrequent, lazy parsing would be premature).
type Subscribe struct {
	PacketID   uint16
	Properties Properties
	Filters    []SubscribeFilter

	frame *[]byte
}

// Type implements Packet.
func (*Subscribe) Type() PacketType { return SUBSCRIBE }

// Release returns the packet and its frame to their pools.
func (s *Subscribe) Release() {
	if s.frame != nil {
		releaseBuf(s.frame)
		s.frame = nil
	}
	s.PacketID = 0
	s.Properties = Properties{}
	for i := range s.Filters {
		s.Filters[i] = SubscribeFilter{}
	}
	s.Filters = s.Filters[:0]
	subscribePool.Put(s)
}

var subscribePool = sync.Pool{
	New: func() any { return &Subscribe{Filters: make([]SubscribeFilter, 0, 4)} },
}

// SubscribeOpts is the input for WriteSubscribe.
type SubscribeOpts struct {
	PacketID               uint16
	Filters                []SubscribeFilter
	SubscriptionIdentifier *uint32
	UserProperties         []UserProperty
}

// ErrEmptyFilterList is returned when WriteSubscribe / WriteUnsubscribe
// is called with no filters; the protocol requires at least one.
var ErrEmptyFilterList = fmt.Errorf("mqttv5: subscribe/unsubscribe requires at least one filter")

// WriteSubscribe emits a SUBSCRIBE packet (flags = 0x02).
func WriteSubscribe(w io.Writer, opts SubscribeOpts) (int64, error) {
	if len(opts.Filters) == 0 {
		return 0, ErrEmptyFilterList
	}

	propsLen := 0
	if opts.SubscriptionIdentifier != nil {
		propsLen += 1 + VarintSize(*opts.SubscriptionIdentifier)
	}
	propsLen += userPropertiesLen(opts.UserProperties)

	payloadLen := 0
	for _, f := range opts.Filters {
		payloadLen += 2 + len(f.Topic) + 1
	}

	varHdrLen := 2 + VarintSize(uint32(propsLen)) + propsLen
	bodyLen := varHdrLen + payloadLen

	var fixedHdr [5]byte
	fixedHdr[0] = byte(SUBSCRIBE)<<4 | 0x02
	vbiN, err := EncodeVarint(fixedHdr[1:], uint32(bodyLen))
	if err != nil {
		return 0, err
	}

	bp := acquireBuf(bodyLen)
	defer releaseBuf(bp)
	buf := *bp

	binary.BigEndian.PutUint16(buf, opts.PacketID)
	off := 2
	off = writePropertiesPrefix(buf, off, propsLen)
	if opts.SubscriptionIdentifier != nil {
		buf[off] = PropSubscriptionIdentifier
		off++
		n, _ := EncodeVarint(buf[off:], *opts.SubscriptionIdentifier)
		off += n
	}
	off = writePropertyUserProps(buf, off, opts.UserProperties)

	for _, f := range opts.Filters {
		off = writeUTF8String(buf, off, f.Topic)
		buf[off] = encodeSubscribeOptions(f)
		off++
	}

	bufs := net.Buffers{fixedHdr[:1+vbiN], buf}
	return bufs.WriteTo(w)
}

// decodeSubscribe parses a SUBSCRIBE body. flags must be 0x02.
func decodeSubscribe(frame *[]byte, flags byte) (*Subscribe, error) {
	if flags != 0x02 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: SUBSCRIBE reserved flags must be 0x02", ErrInvalidPacket)
	}

	buf := *frame
	if len(buf) < 2 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: SUBSCRIBE truncated packet id", ErrInvalidPacket)
	}
	packetID := binary.BigEndian.Uint16(buf)
	buf = buf[2:]

	props, n, err := readProperties(buf)
	if err != nil {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: SUBSCRIBE properties: %w", ErrInvalidPacket, err)
	}
	buf = buf[n:]

	s := subscribePool.Get().(*Subscribe)
	s.Filters = s.Filters[:0]

	for len(buf) > 0 {
		topic, tn, err := readUTF8String(buf)
		if err != nil {
			s.Filters = s.Filters[:0]
			subscribePool.Put(s)
			releaseBuf(frame)
			return nil, fmt.Errorf("%w: SUBSCRIBE filter topic: %w", ErrInvalidPacket, err)
		}
		buf = buf[tn:]
		if len(buf) < 1 {
			s.Filters = s.Filters[:0]
			subscribePool.Put(s)
			releaseBuf(frame)
			return nil, fmt.Errorf("%w: SUBSCRIBE filter options byte missing", ErrInvalidPacket)
		}
		s.Filters = append(s.Filters, decodeSubscribeFilter(topic, buf[0]))
		buf = buf[1:]
	}

	if len(s.Filters) == 0 {
		s.Filters = s.Filters[:0]
		subscribePool.Put(s)
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: SUBSCRIBE has no filters", ErrInvalidPacket)
	}

	s.PacketID = packetID
	s.Properties = props
	s.frame = frame
	return s, nil
}

func decodeSubscribeFilter(topic string, optsByte byte) SubscribeFilter {
	return SubscribeFilter{
		Topic:             topic,
		QoS:               optsByte & 0x03,
		NoLocal:           optsByte&0x04 != 0,
		RetainAsPublished: optsByte&0x08 != 0,
		RetainHandling:    (optsByte >> 4) & 0x03,
	}
}

func encodeSubscribeOptions(f SubscribeFilter) byte {
	b := f.QoS & 0x03
	if f.NoLocal {
		b |= 0x04
	}
	if f.RetainAsPublished {
		b |= 0x08
	}
	b |= (f.RetainHandling & 0x03) << 4
	return b
}
