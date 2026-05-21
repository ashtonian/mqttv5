// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// PubResp is the shared decoded form for the four publish-response
// packets: PUBACK, PUBREC, PUBREL, PUBCOMP. They have identical wire
// shape (PacketID + optional ReasonCode + optional Properties) and
// differ only in their type byte and PUBREL's reserved-flag value.
//
// Inspect Type() to know which of the four arrived.
type PubResp struct {
	PacketID   uint16
	ReasonCode ReasonCode
	Properties Properties

	pktType PacketType
	frame   *[]byte
}

// Type returns the concrete packet type — one of PUBACK, PUBREC, PUBREL,
// PUBCOMP.
func (p *PubResp) Type() PacketType { return p.pktType }

// Release returns the packet and its frame to their pools.
func (p *PubResp) Release() {
	if p.frame != nil {
		releaseBuf(p.frame)
		p.frame = nil
	}
	p.PacketID = 0
	p.ReasonCode = 0
	p.Properties = Properties{}
	p.pktType = 0
	pubRespPool.Put(p)
}

var pubRespPool = sync.Pool{
	New: func() any { return new(PubResp) },
}

// PubRespOpts is the input for WritePuback / Pubrec / Pubrel / Pubcomp.
// Per §3.4.2.2.1, when ReasonCode is Success AND there are no
// properties, the encoded packet omits both — yielding a minimal 4-byte
// frame (fixed header + 2-byte PacketID).
type PubRespOpts struct {
	PacketID       uint16
	ReasonCode     ReasonCode
	ReasonString   string
	UserProperties []UserProperty
}

// WritePuback emits a PUBACK packet.
func WritePuback(w io.Writer, opts PubRespOpts) (int64, error) {
	return writePubResp(w, PUBACK, 0x00, &opts)
}

// WritePubrec emits a PUBREC packet.
func WritePubrec(w io.Writer, opts PubRespOpts) (int64, error) {
	return writePubResp(w, PUBREC, 0x00, &opts)
}

// WritePubrel emits a PUBREL packet. The fixed-header flag nibble is
// 0x02 per §3.6.1.
func WritePubrel(w io.Writer, opts PubRespOpts) (int64, error) {
	return writePubResp(w, PUBREL, 0x02, &opts)
}

// WritePubcomp emits a PUBCOMP packet.
func WritePubcomp(w io.Writer, opts PubRespOpts) (int64, error) {
	return writePubResp(w, PUBCOMP, 0x00, &opts)
}

// decodePubResp parses a body whose shape is shared by PUBACK/REC/REL/
// COMP. The caller passes the packet type (to validate flags and to
// stamp p.pktType) and the flags byte from the fixed header.
func decodePubResp(frame *[]byte, pktType PacketType, flags byte) (*PubResp, error) {
	expected := byte(0x00)
	if pktType == PUBREL {
		expected = 0x02
	}
	if flags != expected {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: %s reserved flags must be %#02x", ErrInvalidPacket, pktType, expected)
	}

	buf := *frame
	if len(buf) < 2 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: %s truncated packet id", ErrInvalidPacket, pktType)
	}
	packetID := binary.BigEndian.Uint16(buf)
	buf = buf[2:]

	var (
		reason ReasonCode = ReasonSuccess
		props  Properties
	)

	// Per §3.4.2.2.1 the reason code is implied 0x00 if absent.
	if len(buf) > 0 {
		reason = ReasonCode(buf[0])
		buf = buf[1:]
		// Per §3.4.2.2.2 properties are implied empty if absent.
		if len(buf) > 0 {
			p, _, err := readProperties(buf)
			if err != nil {
				releaseBuf(frame)
				return nil, fmt.Errorf("%w: %s properties: %w", ErrInvalidPacket, pktType, err)
			}
			props = p
		}
	}

	p := pubRespPool.Get().(*PubResp)
	p.PacketID = packetID
	p.ReasonCode = reason
	p.Properties = props
	p.pktType = pktType
	p.frame = frame
	return p, nil
}

// writePubResp is the shared encoder for the four ack-shape packets.
func writePubResp(w io.Writer, pktType PacketType, flags byte, opts *PubRespOpts) (int64, error) {
	propsLen := 0
	if opts.ReasonString != "" {
		propsLen += 1 + 2 + len(opts.ReasonString)
	}
	propsLen += userPropertiesLen(opts.UserProperties)

	// Trim trailing zero fields per spec §3.4.2.2.1 / 2.2.2.
	hasProps := propsLen > 0
	hasReason := hasProps || opts.ReasonCode != ReasonSuccess

	bodyLen := 2 // PacketID
	if hasReason {
		bodyLen++ // ReasonCode
		if hasProps {
			bodyLen += VarintSize(uint32(propsLen)) + propsLen
		}
	}

	var fixedHdr [5]byte
	fixedHdr[0] = byte(pktType)<<4 | flags
	vbiN, err := EncodeVarint(fixedHdr[1:], uint32(bodyLen))
	if err != nil {
		return 0, err
	}

	bp := acquireBuf(bodyLen)
	defer releaseBuf(bp)
	buf := *bp

	binary.BigEndian.PutUint16(buf, opts.PacketID)
	off := 2
	if hasReason {
		buf[off] = byte(opts.ReasonCode)
		off++
		if hasProps {
			off = writePropertiesPrefix(buf, off, propsLen)
			off = writePropertyString(buf, off, PropReasonString, opts.ReasonString)
			_ = writePropertyUserProps(buf, off, opts.UserProperties)
		}
	}

	bufs := net.Buffers{fixedHdr[:1+vbiN], buf}
	return bufs.WriteTo(w)
}
