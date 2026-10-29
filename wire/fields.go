package wire

import (
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"unsafe"
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
// returned byte count is what was consumed from r (≤ 4). Like
// io.ReadFull, it returns io.EOF only if r ends before the first byte and
// io.ErrUnexpectedEOF if r ends inside the integer.
func ReadVarint(r io.ByteReader) (value uint32, n int, err error) {
	var multiplier uint32 = 1
	for n = 0; n < maxVarintBytes; n++ {
		b, rerr := r.ReadByte()
		if rerr != nil {
			if n > 0 && errors.Is(rerr, io.EOF) {
				rerr = io.ErrUnexpectedEOF
			}
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

// bytesToString returns a string that aliases the supplied byte slice.
// No allocation, no copy.
//
// CRITICAL CONTRACT: the byte slice must NOT be mutated for as long as
// the returned string is reachable, and the string must NOT outlive the
// underlying memory.
//
// In the wire package this is sound because:
//   - frame buffers from bufpool are only mutated by readers (the
//     decoder reads into them, then the body is parsed into immutable
//     []byte / string aliases until Release).
//   - on Release the frame returns to the pool; any caller holding a
//     reference past Release is violating the Packet lifetime contract,
//     and the docs say so.
//
// If a caller needs to retain a string past Release, they call
// strings.Clone (one alloc).
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

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

// SubscriptionIdentifiers yields every Subscription Identifier on a
// PUBLISH, one per subscription of the receiver that matched it
// (§3.3.2.3.8).
func (p Properties) SubscriptionIdentifiers() iter.Seq[uint32] {
	return func(yield func(uint32) bool) {
		for off := 0; off < len(p.raw); {
			id := p.raw[off]
			off++
			size, err := propValueSize(id, p.raw[off:])
			if err != nil || off+size > len(p.raw) {
				return
			}
			if id == PropSubscriptionIdentifier {
				if v, _, err := DecodeVarint(p.raw[off : off+size]); err == nil && !yield(v) {
					return
				}
			}
			off += size
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
