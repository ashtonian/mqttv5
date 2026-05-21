// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
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
// []byte and string field becomes invalid after this returns.
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

// WritePublish encodes a PUBLISH packet and writes it to w. Uses
// net.Buffers so writers that support vectored I/O (*net.TCPConn,
// *net.UnixConn) coalesce the fixed header, variable header, and payload
// into one writev syscall.
//
// Allocations: one allocation for the variable-header buffer (from
// bufpool, so amortized away after the first call) plus the unavoidable
// 3-element [][]byte literal that escapes into net.Buffers.WriteTo.
//
// On the client's hot path, prefer EncodePublish + a direct Write —
// it pools the entire packet as a single contiguous buffer and avoids
// the net.Buffers escape.
func WritePublish(w io.Writer, opts PublishOpts) (int64, error) {
	if opts.QoS > 2 {
		return 0, ErrInvalidQoS
	}
	if opts.QoS > 0 && opts.PacketID == 0 {
		return 0, ErrPacketIDRequired
	}

	// Compute sizes top-down.
	propsLen := publishPropsLen(&opts)
	propsLenVBI := VarintSize(uint32(propsLen))
	varHdrSize := 2 + len(opts.Topic) + propsLenVBI + propsLen
	if opts.QoS > 0 {
		varHdrSize += 2
	}
	remaining := varHdrSize + len(opts.Payload)

	// Fixed header: stack-allocated, max 1 + 4 bytes.
	var fixedHdr [5]byte
	fixedHdr[0] = byte(PUBLISH)<<4 | publishFlags(&opts)
	vbiN, err := EncodeVarint(fixedHdr[1:], uint32(remaining))
	if err != nil {
		return 0, err
	}

	// Variable header + properties: pooled buffer.
	bp := acquireBuf(varHdrSize)
	defer releaseBuf(bp)
	encodePublishVarHdr(*bp, &opts, propsLen, propsLenVBI)

	bufs := net.Buffers{
		fixedHdr[:1+vbiN],
		*bp,
		opts.Payload,
	}
	return bufs.WriteTo(w)
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
	if opts.QoS > 2 {
		return nil, ErrInvalidQoS
	}
	if opts.QoS > 0 && opts.PacketID == 0 {
		return nil, ErrPacketIDRequired
	}

	propsLen := publishPropsLen(&opts)
	propsLenVBI := VarintSize(uint32(propsLen))
	varHdrSize := 2 + len(opts.Topic) + propsLenVBI + propsLen
	if opts.QoS > 0 {
		varHdrSize += 2
	}
	remaining := varHdrSize + len(opts.Payload)
	vbiSize := VarintSize(uint32(remaining))
	total := 1 + vbiSize + varHdrSize + len(opts.Payload)

	bp := acquireBuf(total)
	buf := *bp
	buf[0] = byte(PUBLISH)<<4 | publishFlags(&opts)
	if _, err := EncodeVarint(buf[1:1+vbiSize], uint32(remaining)); err != nil {
		releaseBuf(bp)
		return nil, err
	}
	off := 1 + vbiSize
	encodePublishVarHdr(buf[off:off+varHdrSize], &opts, propsLen, propsLenVBI)
	off += varHdrSize
	copy(buf[off:], opts.Payload)
	return bp, nil
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
