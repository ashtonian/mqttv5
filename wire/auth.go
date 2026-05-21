// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"fmt"
	"io"
	"net"
	"sync"
)

// Auth is a decoded AUTH packet (MQTT v5 §3.15).
//
// Per §3.15.2.2.1, when RemainingLength == 0 the Reason Code is implied
// Success and no properties are present.
type Auth struct {
	ReasonCode ReasonCode
	Properties Properties

	frame *[]byte
}

// Type implements Packet.
func (*Auth) Type() PacketType { return AUTH }

// Release returns the packet and its frame to their pools.
func (a *Auth) Release() {
	if a.frame != nil {
		releaseBuf(a.frame)
		a.frame = nil
	}
	a.ReasonCode = 0
	a.Properties = Properties{}
	authPool.Put(a)
}

var authPool = sync.Pool{
	New: func() any { return new(Auth) },
}

// AuthOpts is the input for WriteAuth.
type AuthOpts struct {
	ReasonCode           ReasonCode
	AuthenticationMethod string
	AuthenticationData   []byte
	ReasonString         string
	UserProperties       []UserProperty
}

// WriteAuth emits an AUTH packet.
func WriteAuth(w io.Writer, opts AuthOpts) (int64, error) {
	propsLen := 0
	if opts.AuthenticationMethod != "" {
		propsLen += 1 + 2 + len(opts.AuthenticationMethod)
	}
	if opts.AuthenticationData != nil {
		propsLen += 1 + 2 + len(opts.AuthenticationData)
	}
	if opts.ReasonString != "" {
		propsLen += 1 + 2 + len(opts.ReasonString)
	}
	propsLen += userPropertiesLen(opts.UserProperties)

	hasProps := propsLen > 0
	hasReason := hasProps || opts.ReasonCode != ReasonSuccess

	bodyLen := 0
	if hasReason {
		bodyLen++
		if hasProps {
			bodyLen += VarintSize(uint32(propsLen)) + propsLen
		}
	}

	var fixedHdr [5]byte
	fixedHdr[0] = byte(AUTH) << 4
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
		off = writePropertyString(buf, off, PropAuthMethod, opts.AuthenticationMethod)
		off = writePropertyBinary(buf, off, PropAuthData, opts.AuthenticationData)
		off = writePropertyString(buf, off, PropReasonString, opts.ReasonString)
		_ = writePropertyUserProps(buf, off, opts.UserProperties)
	}

	bufs := net.Buffers{fixedHdr[:1+vbiN], buf}
	return bufs.WriteTo(w)
}

// decodeAuth parses an AUTH body. flags must be 0x00.
func decodeAuth(frame *[]byte, flags byte) (*Auth, error) {
	if flags != 0x00 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: AUTH reserved flags must be 0", ErrInvalidPacket)
	}

	buf := *frame
	var (
		reason = ReasonSuccess
		props  Properties
	)
	if len(buf) > 0 {
		reason = ReasonCode(buf[0])
		buf = buf[1:]
		if len(buf) > 0 {
			p, _, err := readProperties(buf)
			if err != nil {
				releaseBuf(frame)
				return nil, fmt.Errorf("%w: AUTH properties: %w", ErrInvalidPacket, err)
			}
			props = p
		}
	}

	a := authPool.Get().(*Auth)
	a.ReasonCode = reason
	a.Properties = props
	a.frame = frame
	return a, nil
}
