// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// ProtocolName is the canonical UTF-8 name carried in every CONNECT.
const ProtocolName = "MQTT"

// ProtocolVersion is the byte we send (and require) in CONNECT for v5.
const ProtocolVersion byte = 5

// Will is the decoded Will Message portion of a CONNECT packet
// (§3.1.3.2-§3.1.3.4). Topic, Payload, and Properties alias the frame.
type Will struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties Properties
}

// Connect is a decoded CONNECT packet (§3.1).
//
// Properties is a lazy view over the CONNECT properties section.
// Will is non-nil iff the Will Flag was set on the wire.
// Username / Password are empty / nil when their flags were not set.
type Connect struct {
	ProtocolName    string
	ProtocolVersion byte
	CleanStart      bool
	KeepAlive       uint16
	Properties      Properties

	ClientID string
	Username string
	Password []byte
	Will     *Will

	frame *[]byte
}

// Type implements Packet.
func (*Connect) Type() PacketType { return CONNECT }

// Release returns the packet, its will (if any), and frame to their pools.
func (c *Connect) Release() {
	if c.frame != nil {
		releaseBuf(c.frame)
		c.frame = nil
	}
	if c.Will != nil {
		*c.Will = Will{}
		willPool.Put(c.Will)
		c.Will = nil
	}
	*c = Connect{}
	connectPool.Put(c)
}

var (
	connectPool = sync.Pool{New: func() any { return new(Connect) }}
	willPool    = sync.Pool{New: func() any { return new(Will) }}
)

// WillOpts is the input for the Will portion of a ConnectOpts.
type WillOpts struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool

	WillDelayInterval      *uint32
	PayloadFormatIndicator *byte
	MessageExpiryInterval  *uint32
	ContentType            string
	ResponseTopic          string
	CorrelationData        []byte
	UserProperties         []UserProperty
}

// ConnectOpts is the input for WriteConnect.
type ConnectOpts struct {
	ClientID   string
	CleanStart bool
	KeepAlive  uint16

	Username string    // empty = no Username Flag
	Password []byte    // nil   = no Password Flag
	Will     *WillOpts // nil  = no Will Flag

	SessionExpiryInterval      *uint32
	ReceiveMaximum             *uint16
	MaximumPacketSize          *uint32
	TopicAliasMaximum          uint16
	RequestResponseInformation *byte
	RequestProblemInformation  *byte
	UserProperties             []UserProperty
	AuthenticationMethod       string
	AuthenticationData         []byte
}

// ErrInvalidProtocol is returned when a CONNECT carries a protocol name
// other than "MQTT" or a version other than 5.
var ErrInvalidProtocol = fmt.Errorf("mqttv5: invalid protocol name or version")

// WriteConnect emits a CONNECT packet.
func WriteConnect(w io.Writer, opts ConnectOpts) (int64, error) {
	// --- size up the connect-level property section ---
	propsLen := connectPropsLen(&opts)

	// --- size up the will-level property section (if any) ---
	willPropsLen := 0
	if opts.Will != nil {
		willPropsLen = willPropsLength(opts.Will)
	}

	// --- variable header: name + version + flags + keepalive + props ---
	varHdrLen := 2 + len(ProtocolName) + 1 + 1 + 2 +
		VarintSize(uint32(propsLen)) + propsLen

	// --- payload: ClientID, optional Will{Props,Topic,Payload}, optional Username, optional Password ---
	payloadLen := 2 + len(opts.ClientID)
	if opts.Will != nil {
		payloadLen += VarintSize(uint32(willPropsLen)) + willPropsLen
		payloadLen += 2 + len(opts.Will.Topic)
		payloadLen += 2 + len(opts.Will.Payload)
	}
	if opts.Username != "" {
		payloadLen += 2 + len(opts.Username)
	}
	if opts.Password != nil {
		payloadLen += 2 + len(opts.Password)
	}

	bodyLen := varHdrLen + payloadLen

	var fixedHdr [5]byte
	fixedHdr[0] = byte(CONNECT) << 4
	vbiN, err := EncodeVarint(fixedHdr[1:], uint32(bodyLen))
	if err != nil {
		return 0, err
	}

	bp := acquireBuf(bodyLen)
	defer releaseBuf(bp)
	buf := *bp

	// Protocol Name + Version
	off := writeUTF8String(buf, 0, ProtocolName)
	buf[off] = ProtocolVersion
	off++

	// Connect Flags
	buf[off] = encodeConnectFlags(&opts)
	off++

	// Keep Alive
	binary.BigEndian.PutUint16(buf[off:], opts.KeepAlive)
	off += 2

	// Properties
	off = writePropertiesPrefix(buf, off, propsLen)
	off = writeConnectProps(buf, off, &opts)

	// Payload: ClientID
	off = writeUTF8String(buf, off, opts.ClientID)

	// Payload: Will
	if opts.Will != nil {
		off = writePropertiesPrefix(buf, off, willPropsLen)
		off = writeWillProps(buf, off, opts.Will)
		off = writeUTF8String(buf, off, opts.Will.Topic)
		off = writeBinaryData(buf, off, opts.Will.Payload)
	}

	// Payload: Username
	if opts.Username != "" {
		off = writeUTF8String(buf, off, opts.Username)
	}

	// Payload: Password
	if opts.Password != nil {
		_ = writeBinaryData(buf, off, opts.Password)
	}

	bufs := net.Buffers{fixedHdr[:1+vbiN], buf}
	return bufs.WriteTo(w)
}

// decodeConnect parses a CONNECT body. flags must be 0x00.
func decodeConnect(frame *[]byte, flags byte) (*Connect, error) {
	if flags != 0x00 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNECT reserved flags must be 0", ErrInvalidPacket)
	}

	buf := *frame

	protocolName, n, err := readUTF8String(buf)
	if err != nil {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNECT protocol name: %w", ErrInvalidPacket, err)
	}
	buf = buf[n:]
	if protocolName != ProtocolName {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: %q", ErrInvalidProtocol, protocolName)
	}

	if len(buf) < 1 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNECT protocol version", ErrInvalidPacket)
	}
	version := buf[0]
	buf = buf[1:]
	if version != ProtocolVersion {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: version %d", ErrInvalidProtocol, version)
	}

	if len(buf) < 1 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNECT flags", ErrInvalidPacket)
	}
	connFlags := buf[0]
	buf = buf[1:]
	if connFlags&0x01 != 0 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNECT reserved flag bit set", ErrInvalidPacket)
	}

	if len(buf) < 2 {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNECT keepalive", ErrInvalidPacket)
	}
	keepAlive := binary.BigEndian.Uint16(buf)
	buf = buf[2:]

	props, n, err := readProperties(buf)
	if err != nil {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNECT properties: %w", ErrInvalidPacket, err)
	}
	buf = buf[n:]

	clientID, n, err := readUTF8String(buf)
	if err != nil {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNECT client id: %w", ErrInvalidPacket, err)
	}
	buf = buf[n:]

	c := connectPool.Get().(*Connect)
	c.ProtocolName = protocolName
	c.ProtocolVersion = version
	c.CleanStart = connFlags&0x02 != 0
	c.KeepAlive = keepAlive
	c.Properties = props
	c.ClientID = clientID

	willFlag := connFlags&0x04 != 0
	willQoS := (connFlags >> 3) & 0x03
	willRetain := connFlags&0x20 != 0
	if willFlag {
		willProps, m, err := readProperties(buf)
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("%w: CONNECT will properties: %w", ErrInvalidPacket, err)
		}
		buf = buf[m:]

		willTopic, m, err := readUTF8String(buf)
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("%w: CONNECT will topic: %w", ErrInvalidPacket, err)
		}
		buf = buf[m:]

		willPayload, m, err := readBinaryData(buf)
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("%w: CONNECT will payload: %w", ErrInvalidPacket, err)
		}
		buf = buf[m:]

		w := willPool.Get().(*Will)
		w.Topic = willTopic
		w.Payload = willPayload
		w.QoS = willQoS
		w.Retain = willRetain
		w.Properties = willProps
		c.Will = w
	} else if willQoS != 0 || willRetain {
		c.Release()
		return nil, fmt.Errorf("%w: CONNECT will qos/retain set without will flag", ErrInvalidPacket)
	}

	if connFlags&0x80 != 0 { // Username Flag
		username, m, err := readUTF8String(buf)
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("%w: CONNECT username: %w", ErrInvalidPacket, err)
		}
		buf = buf[m:]
		c.Username = username
	}

	if connFlags&0x40 != 0 { // Password Flag
		password, _, err := readBinaryData(buf)
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("%w: CONNECT password: %w", ErrInvalidPacket, err)
		}
		c.Password = password
	}

	c.frame = frame
	return c, nil
}

func encodeConnectFlags(o *ConnectOpts) byte {
	var f byte
	if o.CleanStart {
		f |= 0x02
	}
	if o.Will != nil {
		f |= 0x04
		f |= (o.Will.QoS & 0x03) << 3
		if o.Will.Retain {
			f |= 0x20
		}
	}
	if o.Password != nil {
		f |= 0x40
	}
	if o.Username != "" {
		f |= 0x80
	}
	return f
}

func connectPropsLen(o *ConnectOpts) int {
	n := 0
	if o.SessionExpiryInterval != nil {
		n += 5
	}
	if o.ReceiveMaximum != nil {
		n += 3
	}
	if o.MaximumPacketSize != nil {
		n += 5
	}
	if o.TopicAliasMaximum != 0 {
		n += 3
	}
	if o.RequestResponseInformation != nil {
		n += 2
	}
	if o.RequestProblemInformation != nil {
		n += 2
	}
	if o.AuthenticationMethod != "" {
		n += 1 + 2 + len(o.AuthenticationMethod)
	}
	if o.AuthenticationData != nil {
		n += 1 + 2 + len(o.AuthenticationData)
	}
	return n + userPropertiesLen(o.UserProperties)
}

func writeConnectProps(buf []byte, off int, o *ConnectOpts) int {
	off = writeProperty4(buf, off, PropSessionExpiryInterval, o.SessionExpiryInterval)
	off = writeProperty2P(buf, off, PropReceiveMaximum, o.ReceiveMaximum)
	off = writeProperty4(buf, off, PropMaximumPacketSize, o.MaximumPacketSize)
	off = writeProperty2(buf, off, PropTopicAliasMaximum, o.TopicAliasMaximum)
	off = writeProperty1(buf, off, PropRequestResponseInfo, o.RequestResponseInformation)
	off = writeProperty1(buf, off, PropRequestProblemInfo, o.RequestProblemInformation)
	off = writePropertyString(buf, off, PropAuthMethod, o.AuthenticationMethod)
	off = writePropertyBinary(buf, off, PropAuthData, o.AuthenticationData)
	return writePropertyUserProps(buf, off, o.UserProperties)
}

func willPropsLength(w *WillOpts) int {
	n := 0
	if w.WillDelayInterval != nil {
		n += 5
	}
	if w.PayloadFormatIndicator != nil {
		n += 2
	}
	if w.MessageExpiryInterval != nil {
		n += 5
	}
	if w.ContentType != "" {
		n += 1 + 2 + len(w.ContentType)
	}
	if w.ResponseTopic != "" {
		n += 1 + 2 + len(w.ResponseTopic)
	}
	if w.CorrelationData != nil {
		n += 1 + 2 + len(w.CorrelationData)
	}
	return n + userPropertiesLen(w.UserProperties)
}

func writeWillProps(buf []byte, off int, w *WillOpts) int {
	off = writeProperty4(buf, off, PropWillDelayInterval, w.WillDelayInterval)
	off = writeProperty1(buf, off, PropPayloadFormat, w.PayloadFormatIndicator)
	off = writeProperty4(buf, off, PropMessageExpiryInterval, w.MessageExpiryInterval)
	off = writePropertyString(buf, off, PropContentType, w.ContentType)
	off = writePropertyString(buf, off, PropResponseTopic, w.ResponseTopic)
	off = writePropertyBinary(buf, off, PropCorrelationData, w.CorrelationData)
	return writePropertyUserProps(buf, off, w.UserProperties)
}
