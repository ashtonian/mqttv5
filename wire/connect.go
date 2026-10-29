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
	if err := validateConnectOpts(&opts); err != nil {
		return 0, err
	}
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
	if err := validateConnackOpts(&opts); err != nil {
		return 0, err
	}
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

	// Unlike the acknowledgement packets, CONNACK always carries its
	// Property Length (§3.2.2.3.1).
	props, n, err := readProperties(buf)
	if err == nil && n != len(buf) {
		err = errTrailing
	}
	if err != nil {
		releaseBuf(frame)
		return nil, fmt.Errorf("%w: CONNACK properties: %w", ErrInvalidPacket, err)
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
	if err := validateAuthOpts(&opts); err != nil {
		return 0, err
	}
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
			p, n, err := readProperties(buf)
			if err == nil && n != len(buf) {
				err = errTrailing
			}
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
	if err := validateDisconnectOpts(&opts); err != nil {
		return 0, err
	}
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
		reason = ReasonNormalDisconnection
		props  Properties
	)
	if len(buf) > 0 {
		reason = ReasonCode(buf[0])
		buf = buf[1:]
		if len(buf) > 0 {
			p, n, err := readProperties(buf)
			if err == nil && n != len(buf) {
				err = errTrailing
			}
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

// Pingreq is a PINGREQ packet (MQTT v5 §3.12). Empty body.
type Pingreq struct{}

// Type implements Packet.
func (*Pingreq) Type() PacketType { return PINGREQ }

// Release is a no-op — Pingreq is a singleton with no pooled state.
func (*Pingreq) Release() {}

// Pingresp is a PINGRESP packet (MQTT v5 §3.13). Empty body.
type Pingresp struct{}

// Type implements Packet.
func (*Pingresp) Type() PacketType { return PINGRESP }

// Release is a no-op — Pingresp is a singleton with no pooled state.
func (*Pingresp) Release() {}

// Single shared instances. Decoder returns these unchanged; their zero
// state is correct.
var (
	pingreqInstance  = &Pingreq{}
	pingrespInstance = &Pingresp{}
)

// decodePingreq returns the shared PINGREQ singleton. The frame buffer
// is empty for PINGREQ (RemainingLength == 0) so we hand it straight
// back to the pool.
func decodePingreq(frame *[]byte, _ byte) (*Pingreq, error) {
	releaseBuf(frame)
	return pingreqInstance, nil
}

func decodePingresp(frame *[]byte, _ byte) (*Pingresp, error) {
	releaseBuf(frame)
	return pingrespInstance, nil
}

// WritePingreq emits a PINGREQ packet (2 bytes: type byte + 0 remaining
// length).
func WritePingreq(w io.Writer) (int64, error) {
	n, err := w.Write([]byte{byte(PINGREQ) << 4, 0x00})
	return int64(n), err
}

// WritePingresp emits a PINGRESP packet (2 bytes).
func WritePingresp(w io.Writer) (int64, error) {
	n, err := w.Write([]byte{byte(PINGRESP) << 4, 0x00})
	return int64(n), err
}
