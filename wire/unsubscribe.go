// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

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
