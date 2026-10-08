// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package inflight

import (
	"io"
	"net"

	"github.com/ashtonian/mqttv5/wire"
)

// maxPublishesPerCollect bounds one Collect so a single writev stays
// well under the platform iovec limit and a large backlog is written in
// slices rather than one unbounded batch.
const maxPublishesPerCollect = 64

// Frames is one batch of packets for the writer: acknowledgements and
// PUBRELs encoded into a small buffer, and stored PUBLISH packets
// referenced without copying. It is owned by one writer goroutine and
// reused across batches.
type Frames struct {
	small []byte    // acks and DUP-flagged PUBLISH first bytes
	segs  []segment // write order
	out   net.Buffers
	n     int // packets in the batch
}

// segment is either a range of small or a stored packet.
type segment struct {
	off, end int
	pkt      []byte
}

// Reset empties the batch, dropping references to stored packets.
func (f *Frames) Reset() {
	f.small = f.small[:0]
	clear(f.segs)
	f.segs = f.segs[:0]
	clear(f.out)
	f.out = f.out[:0]
	f.n = 0
}

// Len reports how many packets the batch holds.
func (f *Frames) Len() int { return f.n }

func (f *Frames) appendAck(t wire.PacketType, id uint16, rc wire.ReasonCode) {
	off := len(f.small)
	f.small = wire.AppendPubResp(f.small, t, id, rc)
	f.extendSmall(off)
	f.n++
}

// appendPublish adds a stored PUBLISH. A retransmission gets DUP=1 by
// writing a modified first byte from small followed by the rest of the
// stored packet, so the stored DUP=0 bytes are never mutated.
func (f *Frames) appendPublish(pkt []byte, dup bool) {
	if dup {
		off := len(f.small)
		f.small = append(f.small, pkt[0]|0x08)
		f.extendSmall(off)
		pkt = pkt[1:]
	}
	f.segs = append(f.segs, segment{pkt: pkt})
	f.n++
}

// extendSmall records small[off:] as the next segment, merging it into
// the previous segment when that one ends where this starts.
func (f *Frames) extendSmall(off int) {
	if last := len(f.segs) - 1; last >= 0 && f.segs[last].pkt == nil && f.segs[last].end == off {
		f.segs[last].end = len(f.small)
		return
	}
	f.segs = append(f.segs, segment{off: off, end: len(f.small)})
}

// WriteTo writes the batch with a single Write or writev.
func (f *Frames) WriteTo(w io.Writer) (int64, error) {
	if len(f.segs) == 1 {
		n, err := w.Write(f.bytes(f.segs[0]))
		return int64(n), err
	}
	bufs := f.Buffers()
	return bufs.WriteTo(w)
}

// Buffers returns the batch as one buffer per segment, for one writev.
// They belong to the batch and stay valid until Reset; writing them with
// net.Buffers.WriteTo consumes the returned slice, which then holds what
// is left unwritten.
func (f *Frames) Buffers() net.Buffers {
	f.out = f.out[:0]
	for _, s := range f.segs {
		f.out = append(f.out, f.bytes(s))
	}
	return f.out
}

func (f *Frames) bytes(s segment) []byte {
	if s.pkt != nil {
		return s.pkt
	}
	return f.small[s.off:s.end]
}
