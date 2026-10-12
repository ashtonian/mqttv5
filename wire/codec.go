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
//   - PUBLISH encoders for each lifetime: AppendPublishHeader encodes
//     everything but the payload, so header and payload go out together
//     (net.Buffers, one writev) without copying the payload;
//     EncodePublish encodes the whole packet into a pooled buffer for a
//     write that happens later; MarshalPublish into an owned,
//     exact-size slice for a packet that is kept.
//
// Lifetime contract:
//
//	A Packet returned from a Decoder borrows its underlying frame buffer.
//	The caller MUST call Packet.Release once they are done. After Release,
//	every []byte and string field is invalid — copy what you need to keep
//	(use strings.Clone, bytes.Clone, or assignment into a heap struct).
package wire

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"
)

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

// DefaultReadBufferSize is the bufio.Reader window NewDecoder uses: how
// many bytes one read syscall can bring in. Packets larger than the
// window are read straight into their frame buffer.
const DefaultReadBufferSize = 4096

// largeBodyChunk is the unit in which bodies above it are read, so the
// memory a peer can make the decoder allocate is bounded by the bytes it
// actually sends rather than by the length it declares.
const largeBodyChunk = 64 << 10

// ErrUnsupportedPacket is returned by Decoder.ReadPacket when the fixed
// header carries a packet type outside the MQTT v5 set (i.e. a reserved
// or unknown value). All standard v5 packet types decode.
var ErrUnsupportedPacket = errors.New("mqttv5: unknown packet type")

// ErrPacketTooLarge is returned by Decoder.ReadPacket when a packet's
// declared size exceeds the limit set with SetMaxPacketSize. The body is
// not read, so the stream cannot be resumed.
var ErrPacketTooLarge = errors.New("mqttv5: packet exceeds the maximum packet size")

// Decoder reads MQTT v5 control packets from an underlying io.Reader.
//
// Each Decoder owns one bufio.Reader and one allocation footprint —
// per-packet allocations come only from the frame and packet pools.
// Decoders are not safe for concurrent ReadPacket calls; one Decoder per
// connection is the intended pattern.
type Decoder struct {
	br        *bufio.Reader
	maxPacket uint32 // 0: the protocol maximum
	lenient   func(*PacketError)
}

// NewDecoder wraps r in a Decoder with a DefaultReadBufferSize window.
func NewDecoder(r io.Reader) *Decoder {
	return NewDecoderSize(r, DefaultReadBufferSize)
}

// NewDecoderSize wraps r in a Decoder whose read window is size bytes.
// Larger windows mean fewer read syscalls for streams of small packets.
func NewDecoderSize(r io.Reader, size int) *Decoder {
	return &Decoder{br: bufio.NewReaderSize(r, size)}
}

// Ready reports whether a whole packet is buffered, so the next
// ReadPacket returns without reading from the connection.
func (d *Decoder) Ready() bool {
	n := d.br.Buffered()
	if n < 2 {
		return false
	}
	hdr, _ := d.br.Peek(min(n, 1+maxVarintBytes))
	size, k, err := DecodeVarint(hdr[1:])
	return err == nil && n >= 1+k+int(size)
}

// SetLenient makes ReadPacket accept the violations that are harmless
// in practice — a non-minimally encoded Remaining Length and reserved
// flag bits on PINGRESP — reporting each to report instead of failing.
// Every other violation still fails. Nil restores strict decoding.
func (d *Decoder) SetLenient(report func(*PacketError)) { d.lenient = report }

// SetMaxPacketSize makes ReadPacket reject packets whose total size —
// fixed header included, as MQTT's Maximum Packet Size counts it
// (§3.1.2.11.4) — exceeds n. Zero removes the limit.
func (d *Decoder) SetMaxPacketSize(n uint32) { d.maxPacket = n }

// Reset re-points the Decoder at a new reader, reusing the internal
// bufio buffer. Production callers use this on reconnect.
func (d *Decoder) Reset(r io.Reader) {
	d.br.Reset(r)
}

// ReadPacket reads one control packet from the underlying reader and
// checks it against MQTT v5: fixed-header flags, minimal Remaining
// Length, the properties each packet may carry and their values, UTF-8
// strings, topic names and filters, packet identifiers and reason codes.
// A violation is returned as a *PacketError whose Reason is the
// DISCONNECT code to close with; the stream cannot be resumed after it.
//
// The returned Packet borrows pooled memory; the caller MUST call
// Packet.Release once they are done reading its fields. Failing to do so
// leaks frame buffers; calling it twice corrupts the pool.
//
// Returns io.EOF when the underlying reader is exhausted between packets
// and io.ErrUnexpectedEOF if a packet is truncated mid-stream.
func (d *Decoder) ReadPacket() (Packet, error) {
	// Byte 1: high nibble = type, low nibble = flags.
	b0, err := d.br.ReadByte()
	if err != nil {
		return nil, err
	}
	pktType := PacketType(b0 >> 4)
	flags := b0 & 0x0F

	// Bytes 2-5: VBI Remaining Length.
	remaining, n, err := ReadVarint(d.br)
	if err != nil {
		return nil, asPacketError(pktType, fmt.Errorf("mqttv5: read remaining length: %w", err))
	}
	if total := 1 + uint64(n) + uint64(remaining); d.maxPacket > 0 && total > uint64(d.maxPacket) {
		return nil, fmt.Errorf("%w: %s of %d bytes, limit %d", ErrPacketTooLarge, pktType, total, d.maxPacket)
	}

	if perr, tolerable := checkHeader(pktType, flags, remaining, n == VarintSize(remaining)); perr != nil {
		if !tolerable || d.lenient == nil {
			return nil, perr
		}
		d.lenient(perr)
		if pktType == PINGRESP {
			flags = 0
		}
	}

	bp, err := d.readBody(int(remaining))
	if err != nil {
		return nil, err
	}
	p, err := decodeBody(bp, pktType, flags)
	if err != nil {
		return nil, asPacketError(pktType, err)
	}
	if perr := validatePacket(p); perr != nil {
		p.Release()
		return nil, perr
	}
	return p, nil
}

// decodeBody hands a body to its packet type's decoder, which takes over
// the frame.
func decodeBody(bp *[]byte, pktType PacketType, flags byte) (Packet, error) {
	switch pktType {
	case CONNECT:
		return decodeConnect(bp, flags)
	case CONNACK:
		return decodeConnack(bp, flags)
	case PUBLISH:
		return decodePublish(bp, flags)
	case PUBACK, PUBREC, PUBREL, PUBCOMP:
		return decodePubResp(bp, pktType, flags)
	case SUBSCRIBE:
		return decodeSubscribe(bp, flags)
	case SUBACK:
		return decodeSuback(bp, flags)
	case UNSUBSCRIBE:
		return decodeUnsubscribe(bp, flags)
	case UNSUBACK:
		return decodeUnsuback(bp, flags)
	case PINGREQ:
		return decodePingreq(bp, flags)
	case PINGRESP:
		return decodePingresp(bp, flags)
	case DISCONNECT:
		return decodeDisconnect(bp, flags)
	case AUTH:
		return decodeAuth(bp, flags)
	default:
		releaseBuf(bp)
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedPacket, pktType)
	}
}

// readBody reads a packet body of n bytes. Bodies up to largeBodyChunk
// go into a pooled frame. A larger body is read first into a pooled
// largeBodyChunk buffer and then, each time the buffer fills, into a new
// one of n bytes once n is at most eight times what has arrived, or else
// four times the size. A peer that declares a huge length and sends
// little makes the decoder hold at most eight times what it sent; a
// genuine large packet costs at most about 1.5 times its size in
// allocations.
func (d *Decoder) readBody(n int) (*[]byte, error) {
	if n <= largeBodyChunk {
		bp := acquireBuf(n)
		if _, err := io.ReadFull(d.br, *bp); err != nil {
			releaseBuf(bp)
			return nil, err
		}
		return bp, nil
	}
	first := acquireBuf(largeBodyChunk)
	defer func() {
		if first != nil {
			releaseBuf(first)
		}
	}()
	body := *first
	got := 0
	for {
		m, err := io.ReadFull(d.br, body[got:])
		got += m
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if got == n {
			return &body, nil
		}
		next := 4 * len(body)
		if n <= 8*got {
			next = n
		}
		grown := make([]byte, min(n, next))
		copy(grown, body[:got])
		body = grown
		if first != nil {
			releaseBuf(first)
			first = nil
		}
	}
}

// bufClasses are the capacities of pooled frame buffers. A request is
// served from the smallest class that fits, so a buffer never pins more
// than 4x the bytes it holds (plus the 256 B floor). Requests above the
// largest class are allocated exactly and left to the GC: pooling them
// would let one large packet pin its size for the life of the pool.
var bufClasses = [...]int{256, 1 << 10, 4 << 10, 16 << 10, 64 << 10}

// bufPools holds one pool per class. Pools store *[]byte to avoid the
// interface boxing allocation a slice value would incur.
var bufPools [len(bufClasses)]sync.Pool

func init() {
	for i, size := range bufClasses {
		bufPools[i].New = func() any {
			b := make([]byte, 0, size)
			return &b
		}
	}
}

// bufClass returns the index of the smallest class holding n bytes, or
// -1 when n is larger than every class.
func bufClass(n int) int {
	for i, size := range bufClasses {
		if n <= size {
			return i
		}
	}
	return -1
}

// acquireBuf returns a pointer to a slice of length n. The bytes may hold
// data from an earlier use; the caller overwrites them before reading.
func acquireBuf(n int) *[]byte {
	i := bufClass(n)
	if i < 0 {
		b := make([]byte, n)
		return &b
	}
	bp := bufPools[i].Get().(*[]byte)
	*bp = (*bp)[:n]
	return bp
}

// releaseBuf returns the buffer to its class pool. The caller MUST NOT use
// the slice after this returns — that includes any sub-slice it handed
// out to packet fields. Buffers whose capacity is not exactly a class
// size (oversized allocations) are dropped.
func releaseBuf(bp *[]byte) {
	if !pooled(*bp) {
		return
	}
	*bp = (*bp)[:0]
	bufPools[bufClass(cap(*bp))].Put(bp)
}

// pooled reports whether releaseBuf recycles b.
func pooled(b []byte) bool {
	i := bufClass(cap(b))
	return i >= 0 && bufClasses[i] == cap(b)
}

// ReasonCode is the single-byte reason code carried by CONNACK,
// PUBACK/REC/REL/COMP, SUBACK, UNSUBACK, DISCONNECT, and AUTH. The
// meaning of each value is context-dependent — see MQTT v5 §3.x.2 for
// which codes are valid in which packets.
type ReasonCode byte

// Success and granted-QoS codes (0x00 – 0x04).
const (
	ReasonSuccess                ReasonCode = 0x00 // CONNACK, PUBACK/REC/REL/COMP, SUBACK, UNSUBACK
	ReasonNormalDisconnection    ReasonCode = 0x00 // DISCONNECT
	ReasonGrantedQoS0            ReasonCode = 0x00 // SUBACK
	ReasonGrantedQoS1            ReasonCode = 0x01 // SUBACK
	ReasonGrantedQoS2            ReasonCode = 0x02 // SUBACK
	ReasonDisconnectWithWill     ReasonCode = 0x04 // DISCONNECT
	ReasonNoMatchingSubscribers  ReasonCode = 0x10 // PUBACK, PUBREC
	ReasonNoSubscriptionExisted  ReasonCode = 0x11 // UNSUBACK
	ReasonContinueAuthentication ReasonCode = 0x18 // AUTH
	ReasonReAuthenticate         ReasonCode = 0x19 // AUTH
)

// Error and refusal codes (0x80 – 0xA2).
const (
	ReasonUnspecifiedError                  ReasonCode = 0x80
	ReasonMalformedPacket                   ReasonCode = 0x81
	ReasonProtocolError                     ReasonCode = 0x82
	ReasonImplementationSpecificError       ReasonCode = 0x83
	ReasonUnsupportedProtocolVersion        ReasonCode = 0x84 // CONNACK
	ReasonClientIdentifierNotValid          ReasonCode = 0x85 // CONNACK
	ReasonBadUsernameOrPassword             ReasonCode = 0x86 // CONNACK
	ReasonNotAuthorized                     ReasonCode = 0x87
	ReasonServerUnavailable                 ReasonCode = 0x88 // CONNACK
	ReasonServerBusy                        ReasonCode = 0x89
	ReasonBanned                            ReasonCode = 0x8A // CONNACK
	ReasonServerShuttingDown                ReasonCode = 0x8B // DISCONNECT
	ReasonBadAuthenticationMethod           ReasonCode = 0x8C
	ReasonKeepAliveTimeout                  ReasonCode = 0x8D // DISCONNECT
	ReasonSessionTakenOver                  ReasonCode = 0x8E // DISCONNECT
	ReasonTopicFilterInvalid                ReasonCode = 0x8F
	ReasonTopicNameInvalid                  ReasonCode = 0x90
	ReasonPacketIdentifierInUse             ReasonCode = 0x91 // PUBACK, PUBREC, SUBACK, UNSUBACK
	ReasonPacketIdentifierNotFound          ReasonCode = 0x92 // PUBREL, PUBCOMP
	ReasonReceiveMaximumExceeded            ReasonCode = 0x93 // DISCONNECT
	ReasonTopicAliasInvalid                 ReasonCode = 0x94 // DISCONNECT
	ReasonPacketTooLarge                    ReasonCode = 0x95
	ReasonMessageRateTooHigh                ReasonCode = 0x96 // DISCONNECT
	ReasonQuotaExceeded                     ReasonCode = 0x97
	ReasonAdministrativeAction              ReasonCode = 0x98 // DISCONNECT
	ReasonPayloadFormatInvalid              ReasonCode = 0x99
	ReasonRetainNotSupported                ReasonCode = 0x9A
	ReasonQoSNotSupported                   ReasonCode = 0x9B
	ReasonUseAnotherServer                  ReasonCode = 0x9C
	ReasonServerMoved                       ReasonCode = 0x9D
	ReasonSharedSubscriptionsNotSupported   ReasonCode = 0x9E
	ReasonConnectionRateExceeded            ReasonCode = 0x9F
	ReasonMaximumConnectTime                ReasonCode = 0xA0 // DISCONNECT
	ReasonSubscriptionIDsNotSupported       ReasonCode = 0xA1 // SUBACK, DISCONNECT
	ReasonWildcardSubscriptionsNotSupported ReasonCode = 0xA2 // SUBACK, DISCONNECT
)

// IsError reports whether the reason code indicates an error (high bit set).
func (r ReasonCode) IsError() bool { return r >= 0x80 }
