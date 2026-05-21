// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"fmt"
	"io"
	"net"
	"sync"
)

// Disconnect is a decoded DISCONNECT packet (MQTT v5 §3.14).
//
// Per §3.14.2.2.1, the Reason Code defaults to 0x00 (Normal
// Disconnection) when RemainingLength == 0; the Properties section is
// implied empty when RemainingLength <= 1.
type Disconnect struct {
	ReasonCode ReasonCode
	Properties Properties

	frame *[]byte
}

// Type implements Packet.
func (*Disconnect) Type() PacketType { return DISCONNECT }

// Clone returns a deep copy of d that is safe to retain past the
// originating frame's lifetime. The clone is not pooled — let it be
// garbage-collected; do not call Release on it.
func (d *Disconnect) Clone() *Disconnect {
	var props Properties
	if raw := d.Properties.Raw(); len(raw) > 0 {
		props = PropertiesFromBytes(append([]byte(nil), raw...))
	}
	return &Disconnect{
		ReasonCode: d.ReasonCode,
		Properties: props,
	}
}

// Release returns the packet and its frame to their pools.
func (d *Disconnect) Release() {
	if d.frame != nil {
		releaseBuf(d.frame)
		d.frame = nil
	}
	d.ReasonCode = 0
	d.Properties = Properties{}
	disconnectPool.Put(d)
}

var disconnectPool = sync.Pool{
	New: func() any { return new(Disconnect) },
}

// DisconnectOpts is the input for WriteDisconnect. ReasonCode default
// (zero = NormalDisconnection) plus no properties yields a minimal
// 2-byte packet.
type DisconnectOpts struct {
	ReasonCode            ReasonCode
	SessionExpiryInterval *uint32
	ReasonString          string
	ServerReference       string
	UserProperties        []UserProperty
}

// WriteDisconnect emits a DISCONNECT packet.
func WriteDisconnect(w io.Writer, opts DisconnectOpts) (int64, error) {
	propsLen := 0
	if opts.SessionExpiryInterval != nil {
		propsLen += 5
	}
	if opts.ReasonString != "" {
		propsLen += 1 + 2 + len(opts.ReasonString)
	}
	if opts.ServerReference != "" {
		propsLen += 1 + 2 + len(opts.ServerReference)
	}
	propsLen += userPropertiesLen(opts.UserProperties)

	hasProps := propsLen > 0
	hasReason := hasProps || opts.ReasonCode != ReasonNormalDisconnection

	bodyLen := 0
	if hasReason {
		bodyLen++
		if hasProps {
			bodyLen += VarintSize(uint32(propsLen)) + propsLen
		}
	}

	var fixedHdr [5]byte
	fixedHdr[0] = byte(DISCONNECT) << 4
	vbiN, err := EncodeVarint(fixedHdr[1:], uint32(bodyLen))
	if err != nil {
		return 0, err
	}

	if bodyLen == 0 {
		bufs := net.Buffers{fixedHdr[:1+vbiN]}
		return bufs.WriteTo(w)
	}

	bp := acquireBuf(bodyLen)
	defer releaseBuf(bp)
	buf := *bp

	buf[0] = byte(opts.ReasonCode)
	off := 1
	if hasProps {
		off = writePropertiesPrefix(buf, off, propsLen)
		off = writeProperty4(buf, off, PropSessionExpiryInterval, opts.SessionExpiryInterval)
		off = writePropertyString(buf, off, PropReasonString, opts.ReasonString)
		off = writePropertyString(buf, off, PropServerReference, opts.ServerReference)
		_ = writePropertyUserProps(buf, off, opts.UserProperties)
	}

	bufs := net.Buffers{fixedHdr[:1+vbiN], buf}
	return bufs.WriteTo(w)
}

// decodeDisconnect parses a DISCONNECT body. flags must be 0x00.
func decodeDisconnect(frame *[]byte, flags byte) (*Disconnect, error) {
	if flags != 0x00 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: DISCONNECT reserved flags must be 0", ErrInvalidPacket)
	}

	buf := *frame
	var (
		reason ReasonCode = ReasonNormalDisconnection
		props  Properties
	)
	if len(buf) > 0 {
		reason = ReasonCode(buf[0])
		buf = buf[1:]
		if len(buf) > 0 {
			p, _, err := readProperties(buf)
			if err != nil {
				releaseBuf(frame)
				return nil, fmt.Errorf("%w: DISCONNECT properties: %w", ErrInvalidPacket, err)
			}
			props = p
		}
	}

	d := disconnectPool.Get().(*Disconnect)
	d.ReasonCode = reason
	d.Properties = props
	d.frame = frame
	return d, nil
}
