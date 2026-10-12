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
	if err := validateSubscribeOpts(&opts); err != nil {
		return 0, err
	}
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

// Suback is a decoded SUBACK packet (§3.9). One Reason per filter in
// the SUBSCRIBE this SUBACK responds to.
type Suback struct {
	PacketID    uint16
	Properties  Properties
	ReasonCodes []ReasonCode

	frame *[]byte
}

// Type implements Packet.
func (*Suback) Type() PacketType { return SUBACK }

// Release returns the packet and its frame to their pools.
func (s *Suback) Release() {
	if s.frame != nil {
		releaseBuf(s.frame)
		s.frame = nil
	}
	s.PacketID = 0
	s.Properties = Properties{}
	s.ReasonCodes = s.ReasonCodes[:0]
	subackPool.Put(s)
}

var subackPool = sync.Pool{
	New: func() any { return &Suback{ReasonCodes: make([]ReasonCode, 0, 4)} },
}

// SubackOpts is the input for WriteSuback.
type SubackOpts struct {
	PacketID       uint16
	ReasonCodes    []ReasonCode
	ReasonString   string
	UserProperties []UserProperty
}

// WriteSuback emits a SUBACK packet.
func WriteSuback(w io.Writer, opts SubackOpts) (int64, error) {
	if err := validateAckList(SUBACK, opts.PacketID, opts.ReasonCodes, opts.ReasonString, opts.UserProperties); err != nil {
		return 0, err
	}
	return writeSubAckLike(w, SUBACK, opts.PacketID, opts.ReasonCodes, opts.ReasonString, opts.UserProperties)
}

// decodeSuback parses a SUBACK body. flags must be 0x00.
func decodeSuback(frame *[]byte, flags byte) (*Suback, error) {
	if flags != 0x00 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: SUBACK reserved flags must be 0", ErrInvalidPacket)
	}

	packetID, props, codes, err := readSubAckLike(*frame, SUBACK)
	if err != nil {
		releaseBuf(frame)
		return nil, err
	}

	s := subackPool.Get().(*Suback)
	s.PacketID = packetID
	s.Properties = props
	s.ReasonCodes = append(s.ReasonCodes[:0], codes...)
	s.frame = frame
	return s, nil
}

// writeSubAckLike emits a SUBACK or UNSUBACK. Shared because the two
// have identical wire structure beyond the type byte.
func writeSubAckLike(w io.Writer, pktType PacketType, packetID uint16, codes []ReasonCode, reasonString string, userProps []UserProperty) (int64, error) {
	if len(codes) == 0 {
		return 0, fmt.Errorf("%w: %s requires at least one reason code", ErrInvalidPacket, pktType)
	}

	propsLen := 0
	if reasonString != "" {
		propsLen += 1 + 2 + len(reasonString)
	}
	propsLen += userPropertiesLen(userProps)

	varHdrLen := 2 + VarintSize(uint32(propsLen)) + propsLen
	payloadLen := len(codes)
	bodyLen := varHdrLen + payloadLen

	var fixedHdr [5]byte
	fixedHdr[0] = byte(pktType) << 4
	vbiN, err := EncodeVarint(fixedHdr[1:], uint32(bodyLen))
	if err != nil {
		return 0, err
	}

	bp := acquireBuf(bodyLen)
	defer releaseBuf(bp)
	buf := *bp

	binary.BigEndian.PutUint16(buf, packetID)
	off := 2
	off = writePropertiesPrefix(buf, off, propsLen)
	off = writePropertyString(buf, off, PropReasonString, reasonString)
	off = writePropertyUserProps(buf, off, userProps)

	for _, c := range codes {
		buf[off] = byte(c)
		off++
	}

	bufs := net.Buffers{fixedHdr[:1+vbiN], buf}
	return bufs.WriteTo(w)
}

// readSubAckLike parses the PacketID + Properties + ReasonCodes payload
// common to SUBACK and UNSUBACK. The returned codes slice aliases the
// frame buffer; the caller (a per-type decoder) copies into a pooled
// slice before storing in the packet struct.
func readSubAckLike(buf []byte, pktType PacketType) (uint16, Properties, []ReasonCode, error) {
	if len(buf) < 2 {
		return 0, Properties{}, nil, fmt.Errorf("%w: %s truncated packet id", ErrInvalidPacket, pktType)
	}
	packetID := binary.BigEndian.Uint16(buf)
	buf = buf[2:]

	props, n, err := readProperties(buf)
	if err != nil {
		return 0, Properties{}, nil, fmt.Errorf("%w: %s properties: %w", ErrInvalidPacket, pktType, err)
	}
	buf = buf[n:]

	if len(buf) == 0 {
		return 0, Properties{}, nil, fmt.Errorf("%w: %s has no reason codes", ErrInvalidPacket, pktType)
	}

	// codes is a []ReasonCode view over the frame bytes — same length,
	// same memory. The caller is expected to copy into a pooled slice.
	codes := make([]ReasonCode, len(buf))
	for i, b := range buf {
		codes[i] = ReasonCode(b)
	}
	return packetID, props, codes, nil
}

// Unsubscribe is a decoded UNSUBSCRIBE packet (§3.10). The Topics slice
// references the decoded list — one allocation per packet decode.
type Unsubscribe struct {
	PacketID   uint16
	Properties Properties
	Topics     []string

	frame *[]byte
}

// Type implements Packet.
func (*Unsubscribe) Type() PacketType { return UNSUBSCRIBE }

// Release returns the packet and its frame to their pools.
func (u *Unsubscribe) Release() {
	if u.frame != nil {
		releaseBuf(u.frame)
		u.frame = nil
	}
	u.PacketID = 0
	u.Properties = Properties{}
	for i := range u.Topics {
		u.Topics[i] = ""
	}
	u.Topics = u.Topics[:0]
	unsubscribePool.Put(u)
}

var unsubscribePool = sync.Pool{
	New: func() any { return &Unsubscribe{Topics: make([]string, 0, 4)} },
}

// UnsubscribeOpts is the input for WriteUnsubscribe.
type UnsubscribeOpts struct {
	PacketID       uint16
	Topics         []string
	UserProperties []UserProperty
}

// WriteUnsubscribe emits an UNSUBSCRIBE packet (flags = 0x02).
func WriteUnsubscribe(w io.Writer, opts UnsubscribeOpts) (int64, error) {
	if err := validateUnsubscribeOpts(&opts); err != nil {
		return 0, err
	}
	if len(opts.Topics) == 0 {
		return 0, ErrEmptyFilterList
	}

	propsLen := userPropertiesLen(opts.UserProperties)

	payloadLen := 0
	for _, t := range opts.Topics {
		payloadLen += 2 + len(t)
	}

	varHdrLen := 2 + VarintSize(uint32(propsLen)) + propsLen
	bodyLen := varHdrLen + payloadLen

	var fixedHdr [5]byte
	fixedHdr[0] = byte(UNSUBSCRIBE)<<4 | 0x02
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
	off = writePropertyUserProps(buf, off, opts.UserProperties)

	for _, t := range opts.Topics {
		off = writeUTF8String(buf, off, t)
	}

	bufs := net.Buffers{fixedHdr[:1+vbiN], buf}
	return bufs.WriteTo(w)
}

// decodeUnsubscribe parses an UNSUBSCRIBE body. flags must be 0x02.
func decodeUnsubscribe(frame *[]byte, flags byte) (*Unsubscribe, error) {
	if flags != 0x02 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: UNSUBSCRIBE reserved flags must be 0x02", ErrInvalidPacket)
	}

	buf := *frame
	if len(buf) < 2 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: UNSUBSCRIBE truncated packet id", ErrInvalidPacket)
	}
	packetID := binary.BigEndian.Uint16(buf)
	buf = buf[2:]

	props, n, err := readProperties(buf)
	if err != nil {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: UNSUBSCRIBE properties: %w", ErrInvalidPacket, err)
	}
	buf = buf[n:]

	u := unsubscribePool.Get().(*Unsubscribe)
	u.Topics = u.Topics[:0]

	for len(buf) > 0 {
		topic, tn, err := readUTF8String(buf)
		if err != nil {
			u.Topics = u.Topics[:0]
			unsubscribePool.Put(u)
			releaseBuf(frame)
			return nil, fmt.Errorf("%w: UNSUBSCRIBE topic: %w", ErrInvalidPacket, err)
		}
		u.Topics = append(u.Topics, topic)
		buf = buf[tn:]
	}

	if len(u.Topics) == 0 {
		u.Topics = u.Topics[:0]
		unsubscribePool.Put(u)
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: UNSUBSCRIBE has no topics", ErrInvalidPacket)
	}

	u.PacketID = packetID
	u.Properties = props
	u.frame = frame
	return u, nil
}

// Unsuback is a decoded UNSUBACK packet (§3.11). One Reason per topic
// in the UNSUBSCRIBE this UNSUBACK responds to.
type Unsuback struct {
	PacketID    uint16
	Properties  Properties
	ReasonCodes []ReasonCode

	frame *[]byte
}

// Type implements Packet.
func (*Unsuback) Type() PacketType { return UNSUBACK }

// Release returns the packet and its frame to their pools.
func (u *Unsuback) Release() {
	if u.frame != nil {
		releaseBuf(u.frame)
		u.frame = nil
	}
	u.PacketID = 0
	u.Properties = Properties{}
	u.ReasonCodes = u.ReasonCodes[:0]
	unsubackPool.Put(u)
}

var unsubackPool = sync.Pool{
	New: func() any { return &Unsuback{ReasonCodes: make([]ReasonCode, 0, 4)} },
}

// UnsubackOpts is the input for WriteUnsuback.
type UnsubackOpts struct {
	PacketID       uint16
	ReasonCodes    []ReasonCode
	ReasonString   string
	UserProperties []UserProperty
}

// WriteUnsuback emits an UNSUBACK packet.
func WriteUnsuback(w io.Writer, opts UnsubackOpts) (int64, error) {
	if err := validateAckList(UNSUBACK, opts.PacketID, opts.ReasonCodes, opts.ReasonString, opts.UserProperties); err != nil {
		return 0, err
	}
	return writeSubAckLike(w, UNSUBACK, opts.PacketID, opts.ReasonCodes, opts.ReasonString, opts.UserProperties)
}

// decodeUnsuback parses an UNSUBACK body. flags must be 0x00.
func decodeUnsuback(frame *[]byte, flags byte) (*Unsuback, error) {
	if flags != 0x00 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: UNSUBACK reserved flags must be 0", ErrInvalidPacket)
	}

	packetID, props, codes, err := readSubAckLike(*frame, UNSUBACK)
	if err != nil {
		releaseBuf(frame)
		return nil, err
	}

	u := unsubackPool.Get().(*Unsuback)
	u.PacketID = packetID
	u.Properties = props
	u.ReasonCodes = append(u.ReasonCodes[:0], codes...)
	u.frame = frame
	return u, nil
}
