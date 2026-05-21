// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

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
