// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

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
