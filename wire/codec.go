// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// readerBufSize is the bufio.Reader window. Sized to absorb typical small
// packets in one syscall without copying. Larger packets fall back to
// io.ReadFull into the frame buffer.
const readerBufSize = 4096

// ErrUnsupportedPacket is returned by Decoder.ReadPacket when the fixed
// header carries a packet type outside the MQTT v5 set (i.e. a reserved
// or unknown value). All standard v5 packet types decode.
var ErrUnsupportedPacket = errors.New("mqttv5: unknown packet type")

// Decoder reads MQTT v5 control packets from an underlying io.Reader.
//
// Each Decoder owns one bufio.Reader and one allocation footprint —
// per-packet allocations come only from the frame and packet pools.
// Decoders are not safe for concurrent ReadPacket calls; one Decoder per
// connection is the intended pattern.
type Decoder struct {
	br *bufio.Reader
}

// NewDecoder wraps r in a Decoder. The bufio.Reader is allocated once
// per Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{br: bufio.NewReaderSize(r, readerBufSize)}
}

// Reset re-points the Decoder at a new reader, reusing the internal
// bufio buffer. Production callers use this on reconnect.
func (d *Decoder) Reset(r io.Reader) {
	d.br.Reset(r)
}

// ReadPacket reads one control packet from the underlying reader.
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
	remaining, _, err := ReadVarint(d.br)
	if err != nil {
		return nil, fmt.Errorf("mqttv5: read remaining length: %w", err)
	}

	// Borrow a frame buffer and slurp the body.
	bp := acquireBuf(int(remaining))
	if _, err := io.ReadFull(d.br, *bp); err != nil {
		releaseBuf(bp)
		return nil, err
	}

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
