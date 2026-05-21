// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"fmt"
	"io"
	"sync"
)

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
