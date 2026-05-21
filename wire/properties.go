// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"errors"
	"iter"
)

// Property IDs as defined in MQTT v5 §2.2.2.2. Every PropID listed here
// is single-byte on the wire; the spec permits VBI but reserves nothing
// above 0x2A, so a fast byte path is correct for every conforming
// implementation.
const (
	PropPayloadFormat           byte = 0x01
	PropMessageExpiryInterval   byte = 0x02
	PropContentType             byte = 0x03
	PropResponseTopic           byte = 0x08
	PropCorrelationData         byte = 0x09
	PropSubscriptionIdentifier  byte = 0x0B
	PropSessionExpiryInterval   byte = 0x11
	PropAssignedClientID        byte = 0x12
	PropServerKeepAlive         byte = 0x13
	PropAuthMethod              byte = 0x15
	PropAuthData                byte = 0x16
	PropRequestProblemInfo      byte = 0x17
	PropWillDelayInterval       byte = 0x18
	PropRequestResponseInfo     byte = 0x19
	PropResponseInformation     byte = 0x1A
	PropServerReference         byte = 0x1C
	PropReasonString            byte = 0x1F
	PropReceiveMaximum          byte = 0x21
	PropTopicAliasMaximum       byte = 0x22
	PropTopicAlias              byte = 0x23
	PropMaximumQoS              byte = 0x24
	PropRetainAvailable         byte = 0x25
	PropUserProperty            byte = 0x26
	PropMaximumPacketSize       byte = 0x27
	PropWildcardSubAvailable    byte = 0x28
	PropSubscriptionIDAvailable byte = 0x29
	PropSharedSubAvailable      byte = 0x2A
)

// ErrInvalidProperties is returned when the property bytes are
// structurally malformed (truncated value, unknown property ID).
var ErrInvalidProperties = errors.New("mqttv5: malformed properties")

// Properties is a zero-copy lazy view over an MQTT property section.
// The underlying bytes belong to a frame buffer owned by the enclosing
// Packet; once that packet is Released, the Properties value is invalid.
//
// Lookups walk the raw bytes — O(N) in the number of properties. For
// packets where most callers only check one or two properties this beats
// pre-parsing into a map. For packets where every property is consumed
// (rare), an eager decode would win; switch strategies when a benchmark
// shows it.
type Properties struct {
	raw []byte
}

// PropertiesFromBytes wraps an already-parsed property byte slice. Used
// by tests and by per-packet decoders; production callers receive a
// Properties value attached to a decoded packet.
func PropertiesFromBytes(b []byte) Properties { return Properties{raw: b} }

// Raw returns the underlying property bytes (without the leading length
// VBI). Useful for re-encoding or for property types not yet exposed via
// dedicated accessors.
func (p Properties) Raw() []byte { return p.raw }

// Len reports the byte length of the property section.
func (p Properties) Len() int { return len(p.raw) }

// Byte returns the single-byte property value for id and whether it was
// present.
func (p Properties) Byte(id byte) (byte, bool) {
	v, ok := p.find(id)
	if !ok || len(v) < 1 {
		return 0, false
	}
	return v[0], true
}

// Uint16 returns the 2-byte big-endian property value for id and whether
// it was present.
func (p Properties) Uint16(id byte) (uint16, bool) {
	v, ok := p.find(id)
	if !ok || len(v) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(v), true
}

// Uint32 returns the 4-byte big-endian property value for id.
func (p Properties) Uint32(id byte) (uint32, bool) {
	v, ok := p.find(id)
	if !ok || len(v) < 4 {
		return 0, false
	}
	return binary.BigEndian.Uint32(v), true
}

// Varint returns the VBI-encoded property value for id (Subscription
// Identifier is the only such property in v5).
func (p Properties) Varint(id byte) (uint32, bool) {
	v, ok := p.find(id)
	if !ok {
		return 0, false
	}
	val, _, err := DecodeVarint(v)
	if err != nil {
		return 0, false
	}
	return val, true
}

// String returns the UTF-8 string property value for id.
//
// Zero-copy: the returned string shares memory with the underlying frame.
// Copy via strings.Clone if you need to retain it past packet Release.
func (p Properties) String(id byte) (string, bool) {
	v, ok := p.find(id)
	if !ok || len(v) < 2 {
		return "", false
	}
	n := int(binary.BigEndian.Uint16(v))
	if len(v) < 2+n {
		return "", false
	}
	return bytesToString(v[2 : 2+n]), true
}

// Binary returns the binary-data property value for id.
//
// Zero-copy: the returned slice aliases the frame.
func (p Properties) Binary(id byte) ([]byte, bool) {
	v, ok := p.find(id)
	if !ok || len(v) < 2 {
		return nil, false
	}
	n := int(binary.BigEndian.Uint16(v))
	if len(v) < 2+n {
		return nil, false
	}
	return v[2 : 2+n], true
}

// UserProperties yields all user-property key/value pairs in the order
// they appear on the wire. Zero allocations for the iteration itself —
// each yielded string aliases the frame.
func (p Properties) UserProperties() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		off := 0
		for off < len(p.raw) {
			id := p.raw[off]
			off++
			if id != PropUserProperty {
				skip, err := propValueSize(id, p.raw[off:])
				if err != nil {
					return
				}
				off += skip
				continue
			}
			// String pair: 2-byte length + key + 2-byte length + value.
			if off+2 > len(p.raw) {
				return
			}
			kn := int(binary.BigEndian.Uint16(p.raw[off:]))
			off += 2
			if off+kn > len(p.raw) {
				return
			}
			k := bytesToString(p.raw[off : off+kn])
			off += kn
			if off+2 > len(p.raw) {
				return
			}
			vn := int(binary.BigEndian.Uint16(p.raw[off:]))
			off += 2
			if off+vn > len(p.raw) {
				return
			}
			v := bytesToString(p.raw[off : off+vn])
			off += vn
			if !yield(k, v) {
				return
			}
		}
	}
}

// find locates the property with the given id and returns a slice
// positioned at the value (i.e., past the ID byte).
//
// For multi-occurrence properties (UserProperty, SubscriptionIdentifier),
// find returns the first occurrence. Callers that need all occurrences
// should walk via a higher-level iterator (see UserProperties).
func (p Properties) find(id byte) ([]byte, bool) {
	off := 0
	for off < len(p.raw) {
		curID := p.raw[off]
		off++
		if curID == id {
			return p.raw[off:], true
		}
		skip, err := propValueSize(curID, p.raw[off:])
		if err != nil {
			return nil, false
		}
		off += skip
	}
	return nil, false
}

// propValueSize reports the wire size of the value following property id
// at buf[0:]. It does NOT consume the property ID byte itself.
//
// Returns ErrInvalidProperties for unknown IDs or truncated values.
func propValueSize(id byte, buf []byte) (int, error) {
	switch id {
	case PropPayloadFormat,
		PropRequestProblemInfo,
		PropRequestResponseInfo,
		PropMaximumQoS,
		PropRetainAvailable,
		PropWildcardSubAvailable,
		PropSubscriptionIDAvailable,
		PropSharedSubAvailable:
		return 1, nil
	case PropServerKeepAlive,
		PropReceiveMaximum,
		PropTopicAliasMaximum,
		PropTopicAlias:
		return 2, nil
	case PropMessageExpiryInterval,
		PropSessionExpiryInterval,
		PropWillDelayInterval,
		PropMaximumPacketSize:
		return 4, nil
	case PropSubscriptionIdentifier:
		_, n, err := DecodeVarint(buf)
		if err != nil {
			return 0, err
		}
		return n, nil
	case PropContentType,
		PropResponseTopic,
		PropAssignedClientID,
		PropAuthMethod,
		PropResponseInformation,
		PropServerReference,
		PropReasonString,
		PropCorrelationData,
		PropAuthData:
		// UTF-8 string or binary data: identical wire format
		// (2-byte length prefix + N bytes).
		if len(buf) < 2 {
			return 0, ErrInvalidProperties
		}
		return 2 + int(binary.BigEndian.Uint16(buf)), nil
	case PropUserProperty:
		// String pair: 2 + key + 2 + value.
		if len(buf) < 2 {
			return 0, ErrInvalidProperties
		}
		kn := int(binary.BigEndian.Uint16(buf))
		if len(buf) < 2+kn+2 {
			return 0, ErrInvalidProperties
		}
		vn := int(binary.BigEndian.Uint16(buf[2+kn:]))
		return 2 + kn + 2 + vn, nil
	default:
		return 0, ErrInvalidProperties
	}
}
