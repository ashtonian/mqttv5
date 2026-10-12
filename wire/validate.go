// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
	"unsafe"
)

// errTrailing reports bytes after the last field of a packet that has
// no payload.
var errTrailing = errors.New("trailing bytes after properties")

var (
	// ErrInvalidTopic marks a topic name or filter that breaks §4.7.
	ErrInvalidTopic = errors.New("mqttv5: invalid topic")
	// ErrStringTooLong marks a UTF-8 string or binary field longer than
	// the 65,535 bytes its 2-byte length prefix can describe.
	ErrStringTooLong = errors.New("mqttv5: field longer than 65535 bytes")
)

// PacketError reports a packet that breaks MQTT v5. Reason is the code a
// receiver must close the connection with: 0x81 (Malformed Packet) for
// packets that cannot be parsed as the spec defines them, 0x82 (Protocol
// Error) for well-formed packets that break a protocol rule. errors.Is
// matches it against ErrInvalidPacket.
type PacketError struct {
	Packet PacketType
	Reason ReasonCode
	Detail string
}

func (e *PacketError) Error() string {
	kind := "malformed"
	if e.Reason == ReasonProtocolError {
		kind = "protocol error in"
	}
	return fmt.Sprintf("mqttv5: %s %s: %s", kind, e.Packet, e.Detail)
}

// Unwrap lets errors.Is(err, ErrInvalidPacket) match.
func (e *PacketError) Unwrap() error { return ErrInvalidPacket }

func malformed(t PacketType, format string, args ...any) *PacketError {
	return &PacketError{Packet: t, Reason: ReasonMalformedPacket, Detail: fmt.Sprintf(format, args...)}
}

func protocolError(t PacketType, format string, args ...any) *PacketError {
	return &PacketError{Packet: t, Reason: ReasonProtocolError, Detail: fmt.Sprintf(format, args...)}
}

// asPacketError turns a decode failure into a PacketError. I/O errors
// (EOF, timeouts, closed connections) are returned unchanged.
func asPacketError(t PacketType, err error) error {
	if err == nil {
		return nil
	}
	var pe *PacketError
	if errors.As(err, &pe) {
		return err
	}
	if errors.Is(err, ErrInvalidPacket) || errors.Is(err, ErrInvalidProperties) ||
		errors.Is(err, ErrTruncated) || errors.Is(err, ErrVarintMalformed) || errors.Is(err, ErrUnsupportedPacket) {
		return &PacketError{Packet: t, Reason: ReasonMalformedPacket, Detail: err.Error()}
	}
	return err
}

// fixedFlags are the required low nibbles of the fixed header (§2.1.3).
// PUBLISH carries DUP/QoS/RETAIN and is checked by its decoder.
var fixedFlags = [16]byte{PUBREL: 0x02, SUBSCRIBE: 0x02, UNSUBSCRIBE: 0x02}

// checkHeader validates the fixed header before the body is decoded.
// minimal reports whether the Remaining Length used the fewest bytes.
// tolerable marks the violations lenient decoding may let through:
// a non-minimal Remaining Length and reserved PINGRESP flags, both
// harmless and both seen from real brokers.
func checkHeader(t PacketType, flags byte, remaining uint32, minimal bool) (err *PacketError, tolerable bool) {
	if (t == PINGREQ || t == PINGRESP) && remaining != 0 {
		return malformed(t, "non-empty body (%d bytes)", remaining), false
	}
	if t != PUBLISH && t >= CONNECT && t <= AUTH && flags != fixedFlags[t] {
		return malformed(t, "reserved fixed-header flags %#x", flags), t == PINGRESP
	}
	if !minimal {
		return malformed(t, "Remaining Length not minimally encoded"), true
	}
	return nil, false
}

// validUTF8 reports whether s is a well-formed MQTT UTF-8 string
// (§1.5.4): valid UTF-8 without surrogates and without U+0000.
func validUTF8(s string) bool {
	return validUTF8Bytes(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// scanTopicName checks a topic name's encoding and looks for wildcards
// in one pass, with the same eight-byte screen as validUTF8Bytes.
func scanTopicName(s string) (valid, wildcard bool) {
	const ones, highs = 0x0101010101010101, 0x8080808080808080
	const plus, hash = 0x2B2B2B2B2B2B2B2B, 0x2323232323232323
	b := unsafe.Slice(unsafe.StringData(s), len(s))
	i := 0
	for ; i+8 <= len(b); i += 8 {
		x := binary.LittleEndian.Uint64(b[i:])
		p, h := x^plus, x^hash
		if ((x-ones)|x|(p-ones)|(h-ones))&highs != 0 {
			break
		}
	}
	for ; i < len(b); i++ {
		switch c := b[i]; {
		case c == '+' || c == '#':
			wildcard = true
		case c == 0:
			return false, wildcard
		case c >= utf8.RuneSelf:
			rest := b[i:]
			return utf8.Valid(rest) && bytes.IndexByte(rest, 0) < 0, wildcard || bytes.ContainsAny(rest, "+#")
		}
	}
	return true, wildcard
}

// validUTF8Bytes is validUTF8 over bytes. Fields are mostly short and
// ASCII, so it screens eight bytes at a time for a NUL or a non-ASCII
// byte and hands only the rest to utf8.Valid, rather than making two
// full passes.
func validUTF8Bytes(b []byte) bool {
	const ones, highs = 0x0101010101010101, 0x8080808080808080
	i := 0
	for ; i+8 <= len(b); i += 8 {
		// Flags any byte that is 0 or ≥ 0x80 (and, through borrows, a
		// few that are neither; those just take the slow path).
		if x := binary.LittleEndian.Uint64(b[i:]); ((x-ones)|x)&highs != 0 {
			break
		}
	}
	for ; i < len(b); i++ {
		c := b[i]
		if c == 0 {
			return false
		}
		if c >= utf8.RuneSelf {
			rest := b[i:]
			return utf8.Valid(rest) && bytes.IndexByte(rest, 0) < 0
		}
	}
	return true
}

// ValidTopicName checks a PUBLISH topic name (§4.7): a well-formed UTF-8
// string of at most 65,535 bytes without wildcard characters. An empty
// name is allowed only together with a Topic Alias, which the caller
// checks.
func ValidTopicName(name string) error {
	if len(name) > 0xffff {
		return fmt.Errorf("%w: topic name of %d bytes", ErrStringTooLong, len(name))
	}
	if !validUTF8(name) {
		return fmt.Errorf("%w: topic name is not well-formed UTF-8 or contains U+0000", ErrInvalidTopic)
	}
	if strings.ContainsAny(name, "+#") {
		return fmt.Errorf("%w: topic name %q contains a wildcard", ErrInvalidTopic, name)
	}
	return nil
}

// ValidTopicFilter checks a subscription topic filter (§4.7): non-empty
// well-formed UTF-8, '#' only as the whole last level, '+' only as a
// whole level, and for a shared subscription ($share/{ShareName}/{filter})
// a non-empty ShareName without '/', '+' or '#' and a valid filter.
func ValidTopicFilter(filter string) error {
	if filter == "" {
		return fmt.Errorf("%w: empty topic filter", ErrInvalidTopic)
	}
	if len(filter) > 0xffff {
		return fmt.Errorf("%w: topic filter of %d bytes", ErrStringTooLong, len(filter))
	}
	if !validUTF8(filter) {
		return fmt.Errorf("%w: topic filter is not well-formed UTF-8 or contains U+0000", ErrInvalidTopic)
	}
	if rest, ok := strings.CutPrefix(filter, "$share/"); ok {
		name, sub, found := strings.Cut(rest, "/")
		if !found || name == "" || strings.ContainsAny(name, "+#") {
			return fmt.Errorf("%w: shared subscription %q needs $share/{ShareName}/{filter}", ErrInvalidTopic, filter)
		}
		if sub == "" {
			return fmt.Errorf("%w: shared subscription %q has an empty filter", ErrInvalidTopic, filter)
		}
		filter = sub
	}
	levels := strings.Split(filter, "/")
	for i, l := range levels {
		switch {
		case l == "#" && i != len(levels)-1:
			return fmt.Errorf("%w: '#' must be the last level in %q", ErrInvalidTopic, filter)
		case l != "#" && strings.Contains(l, "#"):
			return fmt.Errorf("%w: '#' must occupy a whole level in %q", ErrInvalidTopic, filter)
		case l != "+" && strings.Contains(l, "+"):
			return fmt.Errorf("%w: '+' must occupy a whole level in %q", ErrInvalidTopic, filter)
		}
	}
	return nil
}

// IsSharedFilter reports whether filter names a shared subscription.
func IsSharedFilter(filter string) bool { return strings.HasPrefix(filter, "$share/") }

// Properties allowed on each packet (§2.2.2.2, Table 2-4), as bit masks
// over property identifiers.
func propMask(ids ...byte) uint64 {
	var m uint64
	for _, id := range ids {
		m |= 1 << id
	}
	return m
}

var (
	userProp     = propMask(PropUserProperty)
	reasonProps  = propMask(PropReasonString, PropUserProperty)
	publishProps = propMask(PropPayloadFormat, PropMessageExpiryInterval, PropContentType, PropResponseTopic,
		PropCorrelationData, PropSubscriptionIdentifier, PropTopicAlias, PropUserProperty)
	willProps = propMask(PropWillDelayInterval, PropPayloadFormat, PropMessageExpiryInterval, PropContentType,
		PropResponseTopic, PropCorrelationData, PropUserProperty)
	allowedProps = map[PacketType]uint64{
		CONNECT: propMask(PropSessionExpiryInterval, PropAuthMethod, PropAuthData, PropRequestProblemInfo,
			PropRequestResponseInfo, PropReceiveMaximum, PropTopicAliasMaximum, PropUserProperty, PropMaximumPacketSize),
		CONNACK: propMask(PropSessionExpiryInterval, PropAssignedClientID, PropServerKeepAlive, PropAuthMethod,
			PropAuthData, PropResponseInformation, PropServerReference, PropReasonString, PropReceiveMaximum,
			PropTopicAliasMaximum, PropMaximumQoS, PropRetainAvailable, PropUserProperty, PropMaximumPacketSize,
			PropWildcardSubAvailable, PropSubscriptionIDAvailable, PropSharedSubAvailable),
		PUBLISH:     publishProps,
		PUBACK:      reasonProps,
		PUBREC:      reasonProps,
		PUBREL:      reasonProps,
		PUBCOMP:     reasonProps,
		SUBSCRIBE:   propMask(PropSubscriptionIdentifier, PropUserProperty),
		SUBACK:      reasonProps,
		UNSUBSCRIBE: userProp,
		UNSUBACK:    reasonProps,
		DISCONNECT:  propMask(PropSessionExpiryInterval, PropServerReference, PropReasonString, PropUserProperty),
		AUTH:        propMask(PropAuthMethod, PropAuthData, PropReasonString, PropUserProperty),
	}
	booleanProps = propMask(PropPayloadFormat, PropRequestProblemInfo, PropRequestResponseInfo, PropMaximumQoS,
		PropRetainAvailable, PropWildcardSubAvailable, PropSubscriptionIDAvailable, PropSharedSubAvailable)
	stringProps = propMask(PropContentType, PropResponseTopic, PropAssignedClientID, PropAuthMethod,
		PropResponseInformation, PropServerReference, PropReasonString)
)

// validateProps checks a property section against the packet's
// allowlist (malformed), single occurrence and value ranges (protocol
// error), and string encoding (malformed). allowed is the mask; t names
// the packet in errors.
func validateProps(t PacketType, raw []byte, allowed uint64) *PacketError {
	var seen uint64
	for off := 0; off < len(raw); {
		id := raw[off]
		off++
		if id >= 64 || allowed&(1<<id) == 0 {
			return malformed(t, "property %#x not allowed", id)
		}
		size, err := propValueSize(id, raw[off:])
		if err != nil || off+size > len(raw) {
			return malformed(t, "property %#x truncated", id)
		}
		v := raw[off : off+size]
		off += size
		multi := id == PropUserProperty || (id == PropSubscriptionIdentifier && t == PUBLISH)
		if seen&(1<<id) != 0 && !multi {
			return protocolError(t, "property %#x appears more than once", id)
		}
		seen |= 1 << id
		switch {
		case booleanProps&(1<<id) != 0:
			if v[0] > 1 {
				return protocolError(t, "property %#x has value %d, want 0 or 1", id, v[0])
			}
		case stringProps&(1<<id) != 0:
			if !validUTF8Bytes(v[2:]) {
				return malformed(t, "property %#x is not well-formed UTF-8", id)
			}
			if id == PropResponseTopic && bytes.ContainsAny(v[2:], "+#") {
				return protocolError(t, "Response Topic %q contains a wildcard", v[2:])
			}
		case id == PropUserProperty:
			kn := int(v[0])<<8 | int(v[1])
			if !validUTF8Bytes(v[2:2+kn]) || !validUTF8Bytes(v[4+kn:]) {
				return malformed(t, "user property is not well-formed UTF-8")
			}
		case id == PropTopicAlias:
			if v[0] == 0 && v[1] == 0 {
				return &PacketError{Packet: t, Reason: ReasonTopicAliasInvalid, Detail: "Topic Alias 0"}
			}
		case id == PropReceiveMaximum:
			if v[0] == 0 && v[1] == 0 {
				return protocolError(t, "Receive Maximum is 0")
			}
		case id == PropMaximumPacketSize:
			if v[0]|v[1]|v[2]|v[3] == 0 {
				return protocolError(t, "Maximum Packet Size is 0")
			}
		case id == PropSubscriptionIdentifier:
			if n, _, err := DecodeVarint(v); err != nil || n == 0 {
				return protocolError(t, "Subscription Identifier is 0")
			}
		}
	}
	return nil
}

// Reason codes each packet may carry (§2.4 and the per-packet tables).
var reasonCodes = map[PacketType][]ReasonCode{
	CONNACK: {0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90, 0x95, 0x97, 0x99,
		0x9A, 0x9B, 0x9C, 0x9D, 0x9F},
	PUBACK:   {0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99},
	PUBREC:   {0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99},
	PUBREL:   {0x00, 0x92},
	PUBCOMP:  {0x00, 0x92},
	SUBACK:   {0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2},
	UNSUBACK: {0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91},
	DISCONNECT: {0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D, 0x8E, 0x8F, 0x90, 0x93, 0x94, 0x95, 0x96,
		0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2},
	AUTH: {0x00, 0x18, 0x19},
}

func checkReason(t PacketType, rc ReasonCode) *PacketError {
	for _, ok := range reasonCodes[t] {
		if rc == ok {
			return nil
		}
	}
	return malformed(t, "reason code %#x not defined for %s", byte(rc), t)
}

// validatePacket applies the semantic rules the per-type decoders do
// not.
func validatePacket(p Packet) *PacketError {
	t := p.Type()
	switch x := p.(type) {
	case *Publish:
		valid, wildcard := scanTopicName(x.Topic)
		if !valid {
			return malformed(t, "topic name is not well-formed UTF-8 or contains U+0000")
		}
		if wildcard {
			return protocolError(t, "topic name %q contains a wildcard", x.Topic)
		}
		if x.QoS > 0 && x.PacketID == 0 {
			return protocolError(t, "packet identifier 0")
		}
		if err := validateProps(t, x.Properties.Raw(), publishProps); err != nil {
			return err
		}
		if x.Topic == "" {
			if _, ok := x.Properties.Uint16(PropTopicAlias); !ok {
				return protocolError(t, "empty topic name without a Topic Alias")
			}
		}
	case *Connack:
		if err := checkReason(t, x.ReasonCode); err != nil {
			return err
		}
		return validateProps(t, x.Properties.Raw(), allowedProps[t])
	case *PubResp:
		if x.PacketID == 0 {
			return protocolError(t, "packet identifier 0")
		}
		if err := checkReason(t, x.ReasonCode); err != nil {
			return err
		}
		return validateProps(t, x.Properties.Raw(), allowedProps[t])
	case *Suback:
		if x.PacketID == 0 {
			return protocolError(t, "packet identifier 0")
		}
		for _, rc := range x.ReasonCodes {
			if err := checkReason(t, rc); err != nil {
				return err
			}
		}
		return validateProps(t, x.Properties.Raw(), allowedProps[t])
	case *Unsuback:
		if x.PacketID == 0 {
			return protocolError(t, "packet identifier 0")
		}
		for _, rc := range x.ReasonCodes {
			if err := checkReason(t, rc); err != nil {
				return err
			}
		}
		return validateProps(t, x.Properties.Raw(), allowedProps[t])
	case *Disconnect:
		if err := checkReason(t, x.ReasonCode); err != nil {
			return err
		}
		return validateProps(t, x.Properties.Raw(), allowedProps[t])
	case *Auth:
		if err := checkReason(t, x.ReasonCode); err != nil {
			return err
		}
		return validateProps(t, x.Properties.Raw(), allowedProps[t])
	case *Connect:
		if !validUTF8(x.ClientID) || !validUTF8(x.Username) {
			return malformed(t, "client identifier or user name is not well-formed UTF-8")
		}
		if err := validateProps(t, x.Properties.Raw(), allowedProps[t]); err != nil {
			return err
		}
		if x.Will != nil {
			if err := ValidTopicName(x.Will.Topic); err != nil || x.Will.Topic == "" {
				return protocolError(t, "invalid will topic %q", x.Will.Topic)
			}
			return validateProps(t, x.Will.Properties.Raw(), willProps)
		}
	case *Subscribe:
		if x.PacketID == 0 {
			return protocolError(t, "packet identifier 0")
		}
		for _, f := range x.Filters {
			if err := ValidTopicFilter(f.Topic); err != nil {
				return protocolError(t, "%v", err)
			}
			if f.NoLocal && IsSharedFilter(f.Topic) {
				return protocolError(t, "No Local on shared subscription %q", f.Topic)
			}
		}
		return validateProps(t, x.Properties.Raw(), allowedProps[t])
	case *Unsubscribe:
		if x.PacketID == 0 {
			return protocolError(t, "packet identifier 0")
		}
		for _, f := range x.Topics {
			if err := ValidTopicFilter(f); err != nil {
				return protocolError(t, "%v", err)
			}
		}
		return validateProps(t, x.Properties.Raw(), allowedProps[t])
	}
	return nil
}

// ErrInvalidField marks an option value MQTT v5 does not allow; the
// packet is not written.
var ErrInvalidField = errors.New("mqttv5: invalid field")

// The encoders check their options with these before writing anything,
// so an invalid packet is never sent and no length is ever truncated to
// fit its 2-byte prefix.
func checkString(field, s string) error {
	if len(s) > 0xffff {
		return fmt.Errorf("%w: %s is %d bytes", ErrStringTooLong, field, len(s))
	}
	if !validUTF8(s) {
		return fmt.Errorf("%w: %s is not well-formed UTF-8 or contains U+0000", ErrInvalidField, field)
	}
	return nil
}

func checkBinary(field string, b []byte) error {
	if len(b) > 0xffff {
		return fmt.Errorf("%w: %s is %d bytes", ErrStringTooLong, field, len(b))
	}
	return nil
}

func checkUserProps(props []UserProperty) error {
	for _, p := range props {
		if err := checkString("user property name", p.Key); err != nil {
			return err
		}
		if err := checkString("user property value", p.Value); err != nil {
			return err
		}
	}
	return nil
}

func checkBool(field string, b *byte) error {
	if b != nil && *b > 1 {
		return fmt.Errorf("%w: %s must be 0 or 1, got %d", ErrInvalidField, field, *b)
	}
	return nil
}

func checkReasonCode(t PacketType, rc ReasonCode) error {
	if perr := checkReason(t, rc); perr != nil {
		return fmt.Errorf("%w: reason code %#x is not defined for %s", ErrInvalidField, byte(rc), t)
	}
	return nil
}

func validatePublishOpts(o *PublishOpts) error {
	if o.QoS > 2 {
		return ErrInvalidQoS
	}
	if o.QoS > 0 && o.PacketID == 0 {
		return ErrPacketIDRequired
	}
	if o.Dup && o.QoS == 0 {
		return fmt.Errorf("%w: DUP set on a QoS 0 PUBLISH", ErrInvalidField)
	}
	if o.Topic == "" && o.TopicAlias == 0 {
		return fmt.Errorf("%w: empty topic name without a Topic Alias", ErrInvalidTopic)
	}
	if err := ValidTopicName(o.Topic); err != nil {
		return err
	}
	if o.ResponseTopic != "" {
		if err := ValidTopicName(o.ResponseTopic); err != nil {
			return fmt.Errorf("response topic: %w", err)
		}
	}
	if err := checkBool("Payload Format Indicator", o.PayloadFormatIndicator); err != nil {
		return err
	}
	if err := checkString("content type", o.ContentType); err != nil {
		return err
	}
	if err := checkBinary("correlation data", o.CorrelationData); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateWillOpts(w *WillOpts) error {
	if w.Topic == "" {
		return fmt.Errorf("%w: empty will topic", ErrInvalidTopic)
	}
	if err := ValidTopicName(w.Topic); err != nil {
		return fmt.Errorf("will topic: %w", err)
	}
	if w.QoS > 2 {
		return ErrInvalidQoS
	}
	if w.ResponseTopic != "" {
		if err := ValidTopicName(w.ResponseTopic); err != nil {
			return fmt.Errorf("will response topic: %w", err)
		}
	}
	if err := checkBool("will Payload Format Indicator", w.PayloadFormatIndicator); err != nil {
		return err
	}
	if err := checkString("will content type", w.ContentType); err != nil {
		return err
	}
	if err := checkBinary("will payload", w.Payload); err != nil {
		return err
	}
	if err := checkBinary("will correlation data", w.CorrelationData); err != nil {
		return err
	}
	return checkUserProps(w.UserProperties)
}

func validateConnectOpts(o *ConnectOpts) error {
	if err := checkString("client identifier", o.ClientID); err != nil {
		return err
	}
	if err := checkString("user name", o.Username); err != nil {
		return err
	}
	if err := checkBinary("password", o.Password); err != nil {
		return err
	}
	if o.Will != nil {
		if err := validateWillOpts(o.Will); err != nil {
			return err
		}
	}
	if o.ReceiveMaximum != nil && *o.ReceiveMaximum == 0 {
		return fmt.Errorf("%w: Receive Maximum 0", ErrInvalidField)
	}
	if o.MaximumPacketSize != nil && *o.MaximumPacketSize == 0 {
		return fmt.Errorf("%w: Maximum Packet Size 0", ErrInvalidField)
	}
	if err := checkBool("Request Response Information", o.RequestResponseInformation); err != nil {
		return err
	}
	if err := checkBool("Request Problem Information", o.RequestProblemInformation); err != nil {
		return err
	}
	if err := checkString("authentication method", o.AuthenticationMethod); err != nil {
		return err
	}
	if o.AuthenticationData != nil && o.AuthenticationMethod == "" {
		return fmt.Errorf("%w: authentication data without an authentication method", ErrInvalidField)
	}
	if err := checkBinary("authentication data", o.AuthenticationData); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateSubscribeOpts(o *SubscribeOpts) error {
	if o.PacketID == 0 {
		return ErrPacketIDRequired
	}
	if len(o.Filters) == 0 {
		return ErrEmptyFilterList
	}
	for _, f := range o.Filters {
		if err := ValidTopicFilter(f.Topic); err != nil {
			return err
		}
		if f.QoS > 2 {
			return ErrInvalidQoS
		}
		if f.RetainHandling > 2 {
			return fmt.Errorf("%w: Retain Handling %d", ErrInvalidField, f.RetainHandling)
		}
		if f.NoLocal && IsSharedFilter(f.Topic) {
			return fmt.Errorf("%w: No Local on shared subscription %q [MQTT-3.8.3-4]", ErrInvalidField, f.Topic)
		}
	}
	if id := o.SubscriptionIdentifier; id != nil && (*id == 0 || *id > MaxVarintValue) {
		return fmt.Errorf("%w: Subscription Identifier %d outside 1..%d", ErrInvalidField, *id, MaxVarintValue)
	}
	return checkUserProps(o.UserProperties)
}

func validateUnsubscribeOpts(o *UnsubscribeOpts) error {
	if o.PacketID == 0 {
		return ErrPacketIDRequired
	}
	if len(o.Topics) == 0 {
		return ErrEmptyFilterList
	}
	for _, f := range o.Topics {
		if err := ValidTopicFilter(f); err != nil {
			return err
		}
	}
	return checkUserProps(o.UserProperties)
}

func validatePubRespOpts(t PacketType, o *PubRespOpts) error {
	if o.PacketID == 0 {
		return ErrPacketIDRequired
	}
	if err := checkReasonCode(t, o.ReasonCode); err != nil {
		return err
	}
	if err := checkString("reason string", o.ReasonString); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateDisconnectOpts(o *DisconnectOpts) error {
	if err := checkReasonCode(DISCONNECT, o.ReasonCode); err != nil {
		return err
	}
	if err := checkString("reason string", o.ReasonString); err != nil {
		return err
	}
	if err := checkString("server reference", o.ServerReference); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateAuthOpts(o *AuthOpts) error {
	if err := checkReasonCode(AUTH, o.ReasonCode); err != nil {
		return err
	}
	if err := checkString("authentication method", o.AuthenticationMethod); err != nil {
		return err
	}
	if err := checkBinary("authentication data", o.AuthenticationData); err != nil {
		return err
	}
	if err := checkString("reason string", o.ReasonString); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateAckList(t PacketType, id uint16, codes []ReasonCode, reason string, props []UserProperty) error {
	if id == 0 {
		return ErrPacketIDRequired
	}
	if len(codes) == 0 {
		return fmt.Errorf("%w: %s needs at least one reason code", ErrInvalidField, t)
	}
	for _, rc := range codes {
		if err := checkReasonCode(t, rc); err != nil {
			return err
		}
	}
	if err := checkString("reason string", reason); err != nil {
		return err
	}
	return checkUserProps(props)
}

func validateConnackOpts(o *ConnackOpts) error {
	if err := checkReasonCode(CONNACK, o.ReasonCode); err != nil {
		return err
	}
	for _, f := range []struct {
		name string
		v    *byte
	}{{"Maximum QoS", o.MaximumQoS}, {"Retain Available", o.RetainAvailable},
		{"Wildcard Subscription Available", o.WildcardSubscriptionAvailable},
		{"Subscription Identifier Available", o.SubscriptionIdentifierAvailable},
		{"Shared Subscription Available", o.SharedSubscriptionAvailable}} {
		if err := checkBool(f.name, f.v); err != nil {
			return err
		}
	}
	if o.ReceiveMaximum != nil && *o.ReceiveMaximum == 0 {
		return fmt.Errorf("%w: Receive Maximum 0", ErrInvalidField)
	}
	if o.MaximumPacketSize != nil && *o.MaximumPacketSize == 0 {
		return fmt.Errorf("%w: Maximum Packet Size 0", ErrInvalidField)
	}
	for _, s := range []struct{ name, v string }{{"assigned client identifier", o.AssignedClientIdentifier},
		{"reason string", o.ReasonString}, {"response information", o.ResponseInformation},
		{"server reference", o.ServerReference}, {"authentication method", o.AuthenticationMethod}} {
		if err := checkString(s.name, s.v); err != nil {
			return err
		}
	}
	if err := checkBinary("authentication data", o.AuthenticationData); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}
