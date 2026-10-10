// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
)

// Publish is a decoded PUBLISH packet. Topic, Payload, and Properties
// reference the underlying frame buffer; they become invalid once
// Release is called.
//
// For an outbound publish, build a PublishOpts and call WritePublish —
// this type is for receivers only.
type Publish struct {
	// Topic aliases the frame. Copy via strings.Clone to retain past Release.
	Topic string
	// Payload aliases the frame. Copy via bytes.Clone to retain past Release.
	Payload []byte
	// Properties is a lazy view into the frame.
	Properties Properties

	QoS      byte
	Retain   bool
	Dup      bool
	PacketID uint16

	frame *[]byte
}

// Type implements Packet.
func (*Publish) Type() PacketType { return PUBLISH }

// Release returns the packet and its frame buffer to their pools. Every
// []byte and string field becomes invalid after this returns, unless
// Pooled reports false.
func (p *Publish) Release() {
	if p.frame != nil {
		releaseBuf(p.frame)
		p.frame = nil
	}
	p.Topic = ""
	p.Payload = nil
	p.Properties = Properties{}
	p.QoS = 0
	p.Retain = false
	p.Dup = false
	p.PacketID = 0
	publishPool.Put(p)
}

// Pooled reports whether Release recycles the packet's frame, which
// invalidates the Topic, Payload and Properties views. A frame larger
// than the biggest pool class is never recycled: those views stay valid
// after Release for as long as they are referenced.
func (p *Publish) Pooled() bool { return p.frame != nil && pooled(*p.frame) }

// Clone returns a deep copy of o: no slice or pointed-to value is
// shared with o, and nil stays nil.
func (o PublishOpts) Clone() PublishOpts {
	c := o
	c.Payload = bytes.Clone(o.Payload)
	c.CorrelationData = bytes.Clone(o.CorrelationData)
	c.UserProperties = slices.Clone(o.UserProperties)
	if o.PayloadFormatIndicator != nil {
		v := *o.PayloadFormatIndicator
		c.PayloadFormatIndicator = &v
	}
	if o.MessageExpiryInterval != nil {
		v := *o.MessageExpiryInterval
		c.MessageExpiryInterval = &v
	}
	return c
}

// Opts returns options that encode this PUBLISH again: topic, payload,
// QoS, flags, packet identifier and every property a client may send,
// with optional properties present exactly when they are here (an
// empty Correlation Data stays non-nil; an empty payload is nil). Subscription Identifiers, which
// only a server sends, are left out. The result owns copies of its
// fields and stays valid after Release.
func (p *Publish) Opts() PublishOpts {
	o := PublishOpts{
		Topic:    strings.Clone(p.Topic),
		QoS:      p.QoS,
		Retain:   p.Retain,
		Dup:      p.Dup,
		PacketID: p.PacketID,
	}
	if len(p.Payload) > 0 {
		o.Payload = bytes.Clone(p.Payload)
	}
	if v, ok := p.Properties.Byte(PropPayloadFormat); ok {
		o.PayloadFormatIndicator = &v
	}
	if v, ok := p.Properties.Uint32(PropMessageExpiryInterval); ok {
		o.MessageExpiryInterval = &v
	}
	if v, ok := p.Properties.String(PropContentType); ok {
		o.ContentType = strings.Clone(v)
	}
	if v, ok := p.Properties.String(PropResponseTopic); ok {
		o.ResponseTopic = strings.Clone(v)
	}
	if v, ok := p.Properties.Binary(PropCorrelationData); ok {
		o.CorrelationData = append([]byte{}, v...)
	}
	if v, ok := p.Properties.Uint16(PropTopicAlias); ok {
		o.TopicAlias = v
	}
	for k, v := range p.Properties.UserProperties() {
		o.UserProperties = append(o.UserProperties, UserProperty{Key: strings.Clone(k), Value: strings.Clone(v)})
	}
	return o
}

var publishPool = sync.Pool{
	New: func() any { return new(Publish) },
}

// UserProperty is a single key/value entry. Multiple may appear in one
// PUBLISH.
type UserProperty struct {
	Key, Value string
}

// PublishOpts is the input for WritePublish. All pointer-typed property
// fields are skipped when nil; string fields are skipped when empty;
// TopicAlias is skipped when 0 (per MQTT v5, alias 0 is reserved).
type PublishOpts struct {
	Topic    string
	Payload  []byte
	QoS      byte // 0, 1, or 2
	Retain   bool
	Dup      bool   // must be false for QoS 0
	PacketID uint16 // required when QoS > 0

	// Properties (all optional). Only set what you mean to send.
	PayloadFormatIndicator *byte
	MessageExpiryInterval  *uint32
	ContentType            string
	ResponseTopic          string
	CorrelationData        []byte
	TopicAlias             uint16 // 0 = unset (alias 0 is reserved by spec)
	UserProperties         []UserProperty
}

// ErrInvalidQoS is returned by WritePublish when opts.QoS is not 0, 1, or 2.
var ErrInvalidQoS = errors.New("mqttv5: QoS must be 0, 1, or 2")

// ErrPacketIDRequired is returned by WritePublish when QoS > 0 and
// PacketID is 0 (packet ID 0 is reserved).
var ErrPacketIDRequired = errors.New("mqttv5: PacketID required for QoS > 0")

// decodePublish parses a PUBLISH packet body. The frame buffer is taken
// over by the returned Publish — caller must not release it directly,
// must call p.Release() instead.
func decodePublish(frame *[]byte, flags byte) (*Publish, error) {
	buf := *frame

	qos := (flags >> 1) & 0x03
	if qos == 3 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: PUBLISH QoS=3", ErrInvalidPacket)
	}
	dup := flags&0x08 != 0
	if dup && qos == 0 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: PUBLISH DUP set with QoS=0", ErrInvalidPacket)
	}
	retain := flags&0x01 != 0

	// Topic Name: 2-byte length + UTF-8 bytes.
	if len(buf) < 2 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: truncated topic length", ErrInvalidPacket)
	}
	topicLen := int(binary.BigEndian.Uint16(buf))
	buf = buf[2:]
	if len(buf) < topicLen {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: truncated topic", ErrInvalidPacket)
	}
	topic := bytesToString(buf[:topicLen])
	buf = buf[topicLen:]

	// Packet ID: present only when QoS > 0.
	var packetID uint16
	if qos > 0 {
		if len(buf) < 2 {
			releaseBuf(frame)
			return nil, fmt.Errorf("%w: truncated packet id", ErrInvalidPacket)
		}
		packetID = binary.BigEndian.Uint16(buf)
		buf = buf[2:]
	}

	// Properties: VBI length + N bytes.
	propsLen, n, err := DecodeVarint(buf)
	if err != nil {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: properties length: %w", ErrInvalidPacket, err)
	}
	buf = buf[n:]
	if uint32(len(buf)) < propsLen {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: truncated properties", ErrInvalidPacket)
	}
	props := PropertiesFromBytes(buf[:propsLen])
	buf = buf[propsLen:]

	// Remainder is payload.
	payload := buf

	p := publishPool.Get().(*Publish)
	p.Topic = topic
	p.Payload = payload
	p.Properties = props
	p.QoS = qos
	p.Retain = retain
	p.Dup = dup
	p.PacketID = packetID
	p.frame = frame
	return p, nil
}

// WritePublish encodes a PUBLISH packet and writes it to w as its
// header (AppendPublishHeader) followed by the payload, which is not
// copied. Writers that support vectored I/O (*net.TCPConn,
// *net.UnixConn) send both in one writev.
func WritePublish(w io.Writer, opts PublishOpts) (int64, error) {
	bp := acquireBuf(0)
	defer releaseBuf(bp)
	hdr, err := AppendPublishHeader((*bp)[:0], opts)
	if err != nil {
		return 0, err
	}
	*bp = hdr
	if len(opts.Payload) == 0 {
		n, err := w.Write(hdr)
		return int64(n), err
	}
	bufs := net.Buffers{hdr, opts.Payload}
	return bufs.WriteTo(w)
}

// AppendPublishHeader appends to dst the PUBLISH packet opts describes,
// up to its payload: the fixed header, whose Remaining Length counts the
// payload, the variable header and the properties. The header followed
// by opts.Payload is the whole packet, so a caller that writes the two
// together — one writev through net.Buffers — sends the packet without
// copying the payload.
func AppendPublishHeader(dst []byte, opts PublishOpts) ([]byte, error) {
	if err := validatePublishOpts(&opts); err != nil {
		return dst, err
	}
	l := publishLayoutOf(&opts)
	start := len(dst)
	dst = slices.Grow(dst, l.headerSize())[:start+l.headerSize()]
	if err := l.encodeHeader(dst[start:], &opts); err != nil {
		return dst[:start], err
	}
	return dst, nil
}

// EncodePublish encodes a PUBLISH into a pooled []byte and returns a
// pointer to it. The caller MUST call ReleaseBuf on the returned pointer
// after sending the bytes (or on any error path that discards them).
//
// This is the zero-closure-alloc path used by the client's QoS 0
// fire-and-forget Publish — pre-encoding lets the call avoid capturing
// PublishOpts into a writer closure, and the bytes can later be
// batched with other reqs into a single writev.
//
// Returned slice contents are valid until ReleaseBuf is called.
func EncodePublish(opts PublishOpts) (*[]byte, error) {
	if err := validatePublishOpts(&opts); err != nil {
		return nil, err
	}
	l := publishLayoutOf(&opts)
	bp := acquireBuf(l.total)
	if err := l.encode(*bp, &opts); err != nil {
		releaseBuf(bp)
		return nil, err
	}
	return bp, nil
}

// MarshalPublish encodes a PUBLISH into a new, exactly-sized slice owned
// by the caller. Use it for packets that outlive a write, such as QoS 1/2
// publishes kept for retransmission; EncodePublish is the pooled variant
// for packets released right after the write.
func MarshalPublish(opts PublishOpts) ([]byte, error) {
	if err := validatePublishOpts(&opts); err != nil {
		return nil, err
	}
	l := publishLayoutOf(&opts)
	out := make([]byte, l.total)
	if err := l.encode(out, &opts); err != nil {
		return nil, err
	}
	return out, nil
}

// publishLayout is the size of each part of an encoded PUBLISH.
type publishLayout struct {
	propsLen, propsLenVBI int
	varHdrSize            int
	remaining, vbiSize    int
	total                 int
}

func publishLayoutOf(o *PublishOpts) publishLayout {
	var l publishLayout
	l.propsLen = publishPropsLen(o)
	l.propsLenVBI = VarintSize(uint32(l.propsLen))
	l.varHdrSize = 2 + len(o.Topic) + l.propsLenVBI + l.propsLen
	if o.QoS > 0 {
		l.varHdrSize += 2
	}
	l.remaining = l.varHdrSize + len(o.Payload)
	l.vbiSize = VarintSize(uint32(l.remaining))
	l.total = 1 + l.vbiSize + l.remaining
	return l
}

// headerSize is the length of the packet without its payload.
func (l publishLayout) headerSize() int { return 1 + l.vbiSize + l.varHdrSize }

// encodeHeader writes everything but the payload into buf, which holds
// at least l.headerSize() bytes.
func (l publishLayout) encodeHeader(buf []byte, o *PublishOpts) error {
	buf[0] = byte(PUBLISH)<<4 | publishFlags(o)
	if _, err := EncodeVarint(buf[1:1+l.vbiSize], uint32(l.remaining)); err != nil {
		return err
	}
	off := 1 + l.vbiSize
	encodePublishVarHdr(buf[off:off+l.varHdrSize], o, l.propsLen, l.propsLenVBI)
	return nil
}

// encode writes the packet into buf, which holds exactly l.total bytes.
func (l publishLayout) encode(buf []byte, o *PublishOpts) error {
	if err := l.encodeHeader(buf, o); err != nil {
		return err
	}
	copy(buf[l.headerSize():], o.Payload)
	return nil
}

// ReleaseBuf returns a buffer obtained from EncodePublish (or any other
// caller-facing acquireBuf variant we expose) back to the pool. Safe
// against nil.
func ReleaseBuf(bp *[]byte) {
	if bp == nil {
		return
	}
	releaseBuf(bp)
}

// publishFlags packs DUP, QoS, Retain into the low nibble of byte 1.
func publishFlags(o *PublishOpts) byte {
	var f byte
	if o.Dup {
		f |= 0x08
	}
	f |= (o.QoS & 0x03) << 1
	if o.Retain {
		f |= 0x01
	}
	return f
}

// publishPropsLen computes the byte length of the property section
// (excluding the leading length VBI).
func publishPropsLen(o *PublishOpts) int {
	n := 0
	if o.PayloadFormatIndicator != nil {
		n += 2
	}
	if o.MessageExpiryInterval != nil {
		n += 5
	}
	if o.ContentType != "" {
		n += 1 + 2 + len(o.ContentType)
	}
	if o.ResponseTopic != "" {
		n += 1 + 2 + len(o.ResponseTopic)
	}
	if o.CorrelationData != nil {
		n += 1 + 2 + len(o.CorrelationData)
	}
	if o.TopicAlias != 0 {
		n += 3
	}
	return n + userPropertiesLen(o.UserProperties)
}

// encodePublishVarHdr writes the topic, packet ID (if QoS>0), property
// length VBI, and property bytes into buf, which must be sized exactly
// for the combined variable header + properties.
func encodePublishVarHdr(buf []byte, o *PublishOpts, propsLen, _ int) {
	off := writeUTF8String(buf, 0, o.Topic)
	if o.QoS > 0 {
		binary.BigEndian.PutUint16(buf[off:], o.PacketID)
		off += 2
	}
	off = writePropertiesPrefix(buf, off, propsLen)

	off = writeProperty1(buf, off, PropPayloadFormat, o.PayloadFormatIndicator)
	off = writeProperty4(buf, off, PropMessageExpiryInterval, o.MessageExpiryInterval)
	off = writePropertyString(buf, off, PropContentType, o.ContentType)
	off = writePropertyString(buf, off, PropResponseTopic, o.ResponseTopic)
	off = writePropertyBinary(buf, off, PropCorrelationData, o.CorrelationData)
	off = writeProperty2(buf, off, PropTopicAlias, o.TopicAlias)
	_ = writePropertyUserProps(buf, off, o.UserProperties)
}

// WithMessageExpiry returns a copy of the encoded PUBLISH frame with its
// Message Expiry Interval set to seconds. Frames that carry no expiry
// property, or that cannot be parsed, are returned unchanged. Used when a
// stored message is sent again later than it was first encoded
// (§3.3.2.3.3).
func WithMessageExpiry(frame []byte, seconds uint32) []byte {
	off, ok := messageExpiryOffset(frame)
	if !ok {
		return frame
	}
	out := bytes.Clone(frame)
	binary.BigEndian.PutUint32(out[off:], seconds)
	return out
}

// messageExpiryOffset locates the 4-byte Message Expiry Interval value
// inside an encoded PUBLISH frame.
func messageExpiryOffset(frame []byte) (int, bool) {
	if len(frame) < 2 || PacketType(frame[0]>>4) != PUBLISH {
		return 0, false
	}
	qos := (frame[0] >> 1) & 0x03
	_, n, err := DecodeVarint(frame[1:])
	if err != nil {
		return 0, false
	}
	off := 1 + n
	if len(frame) < off+2 {
		return 0, false
	}
	off += 2 + int(binary.BigEndian.Uint16(frame[off:]))
	if qos > 0 {
		off += 2
	}
	if len(frame) < off {
		return 0, false
	}
	propsLen, n, err := DecodeVarint(frame[off:])
	if err != nil {
		return 0, false
	}
	off += n
	end := off + int(propsLen)
	if len(frame) < end {
		return 0, false
	}
	for off < end {
		id := frame[off]
		off++
		if id == PropMessageExpiryInterval {
			if end-off < 4 {
				return 0, false
			}
			return off, true
		}
		size, err := propValueSize(id, frame[off:end])
		if err != nil {
			return 0, false
		}
		off += size
	}
	return 0, false
}
