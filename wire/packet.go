// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package wire implements the MQTT v5 wire-format codec. It is the
// foundation layer of the mqttv5 client and is the only package that
// touches packet bytes directly.
//
// Goals:
//
//   - Zero allocations on the receive path in steady state. Frames are
//     drawn from a pool; packet structs reference the frame buffer for
//     string/byte fields rather than copying.
//   - Lazy property decoding: property bytes are kept as a slice until a
//     consumer asks for a specific property by ID.
//   - Encode produces a `net.Buffers` (fixed header, variable header,
//     payload) so the writer can use vectored writes without an extra
//     copy.
//
// Lifetime contract:
//
//	A Packet returned from a Decoder borrows its underlying frame buffer.
//	The caller MUST call Packet.Release once they are done. After Release,
//	every []byte and string field is invalid — copy what you need to keep
//	(use strings.Clone, bytes.Clone, or assignment into a heap struct).
package wire

import "errors"

// PacketType is the MQTT control packet type byte (high nibble of byte 1).
type PacketType byte

// MQTT v5 control packet types (§2.1.2).
const (
	CONNECT     PacketType = 1
	CONNACK     PacketType = 2
	PUBLISH     PacketType = 3
	PUBACK      PacketType = 4
	PUBREC      PacketType = 5
	PUBREL      PacketType = 6
	PUBCOMP     PacketType = 7
	SUBSCRIBE   PacketType = 8
	SUBACK      PacketType = 9
	UNSUBSCRIBE PacketType = 10
	UNSUBACK    PacketType = 11
	PINGREQ     PacketType = 12
	PINGRESP    PacketType = 13
	DISCONNECT  PacketType = 14
	AUTH        PacketType = 15
)

// String returns the protocol name of the type.
func (t PacketType) String() string {
	switch t {
	case CONNECT:
		return "CONNECT"
	case CONNACK:
		return "CONNACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBACK:
		return "PUBACK"
	case PUBREC:
		return "PUBREC"
	case PUBREL:
		return "PUBREL"
	case PUBCOMP:
		return "PUBCOMP"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBACK:
		return "SUBACK"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case UNSUBACK:
		return "UNSUBACK"
	case PINGREQ:
		return "PINGREQ"
	case PINGRESP:
		return "PINGRESP"
	case DISCONNECT:
		return "DISCONNECT"
	case AUTH:
		return "AUTH"
	default:
		return "INVALID"
	}
}

// FixedHeader is the 2-5 byte MQTT fixed header. Type occupies the high
// nibble of byte 1; Flags the low nibble. RemainingLength is a VBI.
type FixedHeader struct {
	Type            PacketType
	Flags           byte
	RemainingLength uint32
}

// Packet is the polymorphic return value from Decoder.ReadPacket. Each
// concrete type (Publish, Connack, etc.) implements it.
//
// Release returns the packet and its frame buffer to their respective
// pools. Calling Release more than once is a programming error and may
// corrupt pooled memory.
type Packet interface {
	Type() PacketType
	Release()
}

// ErrInvalidPacket is returned for packets that violate MQTT structure
// (bad flags for the type, oversized properties section, etc.).
var ErrInvalidPacket = errors.New("mqttv5: invalid packet")
