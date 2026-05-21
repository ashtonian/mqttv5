// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"errors"
)

// ErrTruncated is returned when a packet body ends before a required
// field has been fully read. Caller context (which field) is wrapped via
// fmt.Errorf at the call site so we keep allocations down here.
var ErrTruncated = errors.New("mqttv5: truncated packet")

// readUTF8String reads a 2-byte big-endian length followed by len bytes
// and returns the result as a string aliasing the source buffer.
//
// Zero-copy: the returned string shares memory with buf. Same lifetime
// contract as packet field strings — copy out before Release.
func readUTF8String(buf []byte) (string, int, error) {
	if len(buf) < 2 {
		return "", 0, ErrTruncated
	}
	n := int(binary.BigEndian.Uint16(buf))
	if len(buf) < 2+n {
		return "", 0, ErrTruncated
	}
	return bytesToString(buf[2 : 2+n]), 2 + n, nil
}

// readBinaryData has the same wire format as readUTF8String but returns
// []byte. Zero-copy alias of the source buffer.
func readBinaryData(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, ErrTruncated
	}
	n := int(binary.BigEndian.Uint16(buf))
	if len(buf) < 2+n {
		return nil, 0, ErrTruncated
	}
	return buf[2 : 2+n], 2 + n, nil
}

// writeUTF8String emits a 2-byte length + s into buf at off, returning
// the new offset. Caller is responsible for ensuring buf has room
// (typically pre-sized by the packet's encode function).
func writeUTF8String(buf []byte, off int, s string) int {
	binary.BigEndian.PutUint16(buf[off:], uint16(len(s)))
	off += 2
	return off + copy(buf[off:], s)
}

// writeBinaryData is the same as writeUTF8String for []byte.
func writeBinaryData(buf []byte, off int, b []byte) int {
	binary.BigEndian.PutUint16(buf[off:], uint16(len(b)))
	off += 2
	return off + copy(buf[off:], b)
}

// readProperties reads a VBI length followed by N property bytes,
// returning a Properties view over the bytes. The view aliases buf.
func readProperties(buf []byte) (Properties, int, error) {
	n, vbiN, err := DecodeVarint(buf)
	if err != nil {
		return Properties{}, 0, err
	}
	if uint32(len(buf)-vbiN) < n {
		return Properties{}, 0, ErrTruncated
	}
	return PropertiesFromBytes(buf[vbiN : vbiN+int(n)]), vbiN + int(n), nil
}

// writePropertiesPrefix writes the VBI length prefix at off and returns
// the new offset. The caller follows up by writing the property bytes
// (totalling propsLen) starting at the returned offset.
func writePropertiesPrefix(buf []byte, off int, propsLen int) int {
	n, _ := EncodeVarint(buf[off:], uint32(propsLen))
	return off + n
}

// writeProperty1 emits a 1-byte property when v is non-nil.
func writeProperty1(buf []byte, off int, id byte, v *byte) int {
	if v == nil {
		return off
	}
	buf[off] = id
	buf[off+1] = *v
	return off + 2
}

// writeProperty2 emits a 2-byte property if v != 0. Use writeProperty2P
// for properties where 0 is a valid value distinct from "absent".
func writeProperty2(buf []byte, off int, id byte, v uint16) int {
	if v == 0 {
		return off
	}
	buf[off] = id
	binary.BigEndian.PutUint16(buf[off+1:], v)
	return off + 3
}

// writeProperty2P emits a 2-byte property when v is non-nil (0 is a
// valid distinct value).
func writeProperty2P(buf []byte, off int, id byte, v *uint16) int {
	if v == nil {
		return off
	}
	buf[off] = id
	binary.BigEndian.PutUint16(buf[off+1:], *v)
	return off + 3
}

// writeProperty4 emits a 4-byte property when v is non-nil.
func writeProperty4(buf []byte, off int, id byte, v *uint32) int {
	if v == nil {
		return off
	}
	buf[off] = id
	binary.BigEndian.PutUint32(buf[off+1:], *v)
	return off + 5
}

// writePropertyString emits a string property if v is non-empty. (Per
// MQTT, an empty string IS a valid property value; if you need to
// distinguish "empty" from "absent", use writePropertyStringP.)
func writePropertyString(buf []byte, off int, id byte, v string) int {
	if v == "" {
		return off
	}
	buf[off] = id
	off++
	return writeUTF8String(buf, off, v)
}

// writePropertyBinary emits a binary-data property if v is non-nil.
// A nil slice means "absent"; a non-nil zero-length slice is encoded.
func writePropertyBinary(buf []byte, off int, id byte, v []byte) int {
	if v == nil {
		return off
	}
	buf[off] = id
	off++
	return writeBinaryData(buf, off, v)
}

// writePropertyUserProps emits each user property in order.
func writePropertyUserProps(buf []byte, off int, props []UserProperty) int {
	for _, up := range props {
		buf[off] = PropUserProperty
		off++
		off = writeUTF8String(buf, off, up.Key)
		off = writeUTF8String(buf, off, up.Value)
	}
	return off
}

// userPropertiesLen returns the encoded byte size of the user properties
// list (one byte for the ID per entry + the two strings).
func userPropertiesLen(props []UserProperty) int {
	n := 0
	for _, up := range props {
		n += 1 + 2 + len(up.Key) + 2 + len(up.Value)
	}
	return n
}
