// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"errors"
	"io"
)

// MaxVarintValue is the largest value an MQTT v5 Variable Byte Integer can
// represent: 268,435,455 (2^28 - 1). See MQTT 5 §1.5.5.
const MaxVarintValue uint32 = 268_435_455

const maxVarintBytes = 4

var (
	// ErrVarintTooLarge is returned when encoding a value above MaxVarintValue.
	ErrVarintTooLarge = errors.New("mqttv5: variable byte integer exceeds maximum (268435455)")

	// ErrVarintMalformed is returned when a varint encoding does not
	// terminate within 4 bytes, or the input buffer is exhausted before
	// the encoding completes.
	ErrVarintMalformed = errors.New("mqttv5: malformed variable byte integer")
)

// EncodeVarint writes the VBI encoding of v into buf and returns the byte
// count. buf must have capacity ≥ 4. Returns ErrVarintTooLarge if v
// exceeds MaxVarintValue.
//
// Zero allocations: caller supplies the buffer (typical pattern is a
// stack-allocated [4]byte).
func EncodeVarint(buf []byte, v uint32) (int, error) {
	if v > MaxVarintValue {
		return 0, ErrVarintTooLarge
	}
	i := 0
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v > 0 {
			b |= 0x80
		}
		buf[i] = b
		i++
		if v == 0 {
			return i, nil
		}
	}
}

// VarintSize reports the number of bytes EncodeVarint would write for v.
// Returns 0 if v exceeds MaxVarintValue (caller should treat as error).
func VarintSize(v uint32) int {
	switch {
	case v < 128:
		return 1
	case v < 16_384:
		return 2
	case v < 2_097_152:
		return 3
	case v <= MaxVarintValue:
		return 4
	default:
		return 0
	}
}

// DecodeVarint parses a VBI from the head of buf, returning the decoded
// value, byte count consumed, and any error. Used on already-buffered
// frames (properties, packet bodies). Zero allocations.
func DecodeVarint(buf []byte) (value uint32, n int, err error) {
	var multiplier uint32 = 1
	for n = 0; n < maxVarintBytes; n++ {
		if n >= len(buf) {
			return 0, 0, ErrVarintMalformed
		}
		b := buf[n]
		value += uint32(b&0x7F) * multiplier
		if b&0x80 == 0 {
			return value, n + 1, nil
		}
		multiplier *= 128
	}
	return 0, 0, ErrVarintMalformed
}

// ReadVarint reads a VBI byte-at-a-time from r. Used to parse the fixed
// header's Remaining Length before the body has been buffered. The
// returned byte count is what was consumed from r (≤ 4).
func ReadVarint(r io.ByteReader) (value uint32, n int, err error) {
	var multiplier uint32 = 1
	for n = 0; n < maxVarintBytes; n++ {
		b, rerr := r.ReadByte()
		if rerr != nil {
			return 0, n, rerr
		}
		value += uint32(b&0x7F) * multiplier
		if b&0x80 == 0 {
			return value, n + 1, nil
		}
		multiplier *= 128
	}
	return 0, n, ErrVarintMalformed
}
