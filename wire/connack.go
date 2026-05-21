// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"fmt"
	"io"
	"net"
	"sync"
)

// Connack is a decoded CONNACK packet (§3.2). Properties is a lazy view
// over the property section.
type Connack struct {
	SessionPresent bool
	ReasonCode     ReasonCode
	Properties     Properties

	frame *[]byte
}

// Type implements Packet.
func (*Connack) Type() PacketType { return CONNACK }

// Clone returns a deep copy of c that is safe to retain past the
// originating frame's lifetime. The clone is not pooled — let it be
// garbage-collected; do not call Release on it.
func (c *Connack) Clone() *Connack {
	var props Properties
	if raw := c.Properties.Raw(); len(raw) > 0 {
		props = PropertiesFromBytes(append([]byte(nil), raw...))
	}
	return &Connack{
		SessionPresent: c.SessionPresent,
		ReasonCode:     c.ReasonCode,
		Properties:     props,
	}
}

// Release returns the packet and its frame to their pools.
func (c *Connack) Release() {
	if c.frame != nil {
		releaseBuf(c.frame)
		c.frame = nil
	}
	*c = Connack{}
	connackPool.Put(c)
}

var connackPool = sync.Pool{New: func() any { return new(Connack) }}

// ConnackOpts is the input for WriteConnack.
type ConnackOpts struct {
	SessionPresent bool
	ReasonCode     ReasonCode

	SessionExpiryInterval           *uint32
	ReceiveMaximum                  *uint16
	MaximumQoS                      *byte
	RetainAvailable                 *byte
	MaximumPacketSize               *uint32
	AssignedClientIdentifier        string
	TopicAliasMaximum               uint16
	ReasonString                    string
	UserProperties                  []UserProperty
	WildcardSubscriptionAvailable   *byte
	SubscriptionIdentifierAvailable *byte
	SharedSubscriptionAvailable     *byte
	ServerKeepAlive                 *uint16
	ResponseInformation             string
	ServerReference                 string
	AuthenticationMethod            string
	AuthenticationData              []byte
}

// WriteConnack emits a CONNACK packet.
func WriteConnack(w io.Writer, opts ConnackOpts) (int64, error) {
	propsLen := connackPropsLen(&opts)
	bodyLen := 1 + 1 + VarintSize(uint32(propsLen)) + propsLen

	var fixedHdr [5]byte
	fixedHdr[0] = byte(CONNACK) << 4
	vbiN, err := EncodeVarint(fixedHdr[1:], uint32(bodyLen))
	if err != nil {
		return 0, err
	}

	bp := acquireBuf(bodyLen)
	defer releaseBuf(bp)
	buf := *bp

	if opts.SessionPresent {
		buf[0] = 0x01
	} else {
		buf[0] = 0x00
	}
	buf[1] = byte(opts.ReasonCode)

	off := writePropertiesPrefix(buf, 2, propsLen)
	off = writeProperty4(buf, off, PropSessionExpiryInterval, opts.SessionExpiryInterval)
	off = writeProperty2P(buf, off, PropReceiveMaximum, opts.ReceiveMaximum)
	off = writeProperty1(buf, off, PropMaximumQoS, opts.MaximumQoS)
	off = writeProperty1(buf, off, PropRetainAvailable, opts.RetainAvailable)
	off = writeProperty4(buf, off, PropMaximumPacketSize, opts.MaximumPacketSize)
	off = writePropertyString(buf, off, PropAssignedClientID, opts.AssignedClientIdentifier)
	off = writeProperty2(buf, off, PropTopicAliasMaximum, opts.TopicAliasMaximum)
	off = writePropertyString(buf, off, PropReasonString, opts.ReasonString)
	off = writePropertyUserProps(buf, off, opts.UserProperties)
	off = writeProperty1(buf, off, PropWildcardSubAvailable, opts.WildcardSubscriptionAvailable)
	off = writeProperty1(buf, off, PropSubscriptionIDAvailable, opts.SubscriptionIdentifierAvailable)
	off = writeProperty1(buf, off, PropSharedSubAvailable, opts.SharedSubscriptionAvailable)
	off = writeProperty2P(buf, off, PropServerKeepAlive, opts.ServerKeepAlive)
	off = writePropertyString(buf, off, PropResponseInformation, opts.ResponseInformation)
	off = writePropertyString(buf, off, PropServerReference, opts.ServerReference)
	off = writePropertyString(buf, off, PropAuthMethod, opts.AuthenticationMethod)
	_ = writePropertyBinary(buf, off, PropAuthData, opts.AuthenticationData)

	bufs := net.Buffers{fixedHdr[:1+vbiN], buf}
	return bufs.WriteTo(w)
}

// decodeConnack parses a CONNACK body. flags must be 0x00.
func decodeConnack(frame *[]byte, flags byte) (*Connack, error) {
	if flags != 0x00 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNACK reserved flags must be 0", ErrInvalidPacket)
	}

	buf := *frame
	if len(buf) < 2 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNACK truncated", ErrInvalidPacket)
	}

	ackFlags := buf[0]
	if ackFlags&0xFE != 0 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNACK reserved flag bits set", ErrInvalidPacket)
	}
	sessionPresent := ackFlags&0x01 != 0
	reason := ReasonCode(buf[1])
	buf = buf[2:]

	var props Properties
	if len(buf) > 0 {
		p, _, err := readProperties(buf)
		if err != nil {
			releaseBuf(frame)
			return nil, fmt.Errorf("%w: CONNACK properties: %w", ErrInvalidPacket, err)
		}
		props = p
	}

	c := connackPool.Get().(*Connack)
	c.SessionPresent = sessionPresent
	c.ReasonCode = reason
	c.Properties = props
	c.frame = frame
	return c, nil
}

func connackPropsLen(o *ConnackOpts) int {
	n := 0
	if o.SessionExpiryInterval != nil {
		n += 5
	}
	if o.ReceiveMaximum != nil {
		n += 3
	}
	if o.MaximumQoS != nil {
		n += 2
	}
	if o.RetainAvailable != nil {
		n += 2
	}
	if o.MaximumPacketSize != nil {
		n += 5
	}
	if o.AssignedClientIdentifier != "" {
		n += 1 + 2 + len(o.AssignedClientIdentifier)
	}
	if o.TopicAliasMaximum != 0 {
		n += 3
	}
	if o.ReasonString != "" {
		n += 1 + 2 + len(o.ReasonString)
	}
	if o.WildcardSubscriptionAvailable != nil {
		n += 2
	}
	if o.SubscriptionIdentifierAvailable != nil {
		n += 2
	}
	if o.SharedSubscriptionAvailable != nil {
		n += 2
	}
	if o.ServerKeepAlive != nil {
		n += 3
	}
	if o.ResponseInformation != "" {
		n += 1 + 2 + len(o.ResponseInformation)
	}
	if o.ServerReference != "" {
		n += 1 + 2 + len(o.ServerReference)
	}
	if o.AuthenticationMethod != "" {
		n += 1 + 2 + len(o.AuthenticationMethod)
	}
	if o.AuthenticationData != nil {
		n += 1 + 2 + len(o.AuthenticationData)
	}
	return n + userPropertiesLen(o.UserProperties)
}
