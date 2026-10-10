// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"testing"
)

func TestDecoderMaxPacketSize(t *testing.T) {
	frame, err := MarshalPublish(PublishOpts{Topic: "t", Payload: make([]byte, 100), QoS: 1, PacketID: 1})
	if err != nil {
		t.Fatal(err)
	}
	size := uint32(len(frame))

	dec := NewDecoder(bytes.NewReader(frame))
	dec.SetMaxPacketSize(size)
	p, err := dec.ReadPacket()
	if err != nil {
		t.Fatalf("packet of exactly the limit: %v", err)
	}
	p.Release()

	dec = NewDecoder(bytes.NewReader(frame))
	dec.SetMaxPacketSize(size - 1)
	if _, err := dec.ReadPacket(); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("packet one byte over the limit: %v", err)
	}
}

// A peer that declares a huge Remaining Length but sends little must not
// make the decoder allocate the declared size: buffers grow fourfold as
// bytes arrive, so all of them together stay under 16/3 of what was sent.
func TestLyingRemainingLengthCostsWhatIsSent(t *testing.T) {
	const sent = 1 << 20
	var hdr [5]byte
	hdr[0] = byte(PUBLISH) << 4
	n, _ := EncodeVarint(hdr[1:], MaxVarintValue) // ~256 MiB declared
	stream := io.MultiReader(bytes.NewReader(hdr[:1+n]), bytes.NewReader(make([]byte, sent)))
	dec := NewDecoder(stream)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := dec.ReadPacket()
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated body: %v", err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 16*sent/3+largeBodyChunk {
		t.Fatalf("allocated %d bytes for %d received", alloc, sent)
	}
}

func TestLargePacketRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB
	frame, err := MarshalPublish(PublishOpts{Topic: "big", Payload: payload, QoS: 1, PacketID: 9})
	if err != nil {
		t.Fatal(err)
	}
	dec := NewDecoderSize(bytes.NewReader(frame), 512)
	p, err := dec.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()
	pub := p.(*Publish)
	if pub.Topic != "big" || !bytes.Equal(pub.Payload, payload) {
		t.Fatal("large PUBLISH did not round-trip")
	}
}

// Packets of every size must decode with windows smaller and larger than
// the packet.
func TestDecoderWindowSizes(t *testing.T) {
	var stream bytes.Buffer
	sizes := []int{0, 1, 100, 4000, 5000, 70000}
	for i, n := range sizes {
		b, err := MarshalPublish(PublishOpts{Topic: "w", Payload: bytes.Repeat([]byte{byte(i)}, n), QoS: 1, PacketID: uint16(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		stream.Write(b)
	}
	for _, window := range []int{16, 4096, 65536, 1 << 20} {
		dec := NewDecoderSize(bytes.NewReader(stream.Bytes()), window)
		for i, n := range sizes {
			p, err := dec.ReadPacket()
			if err != nil {
				t.Fatalf("window %d packet %d: %v", window, i, err)
			}
			pub := p.(*Publish)
			if len(pub.Payload) != n || pub.PacketID != uint16(i+1) {
				t.Fatalf("window %d packet %d: got %d bytes id %d", window, i, len(pub.Payload), pub.PacketID)
			}
			p.Release()
		}
	}
}

// Frames above the largest pool class are never recycled, so a decoded
// packet's views into them outlive Release.
func TestLargeFramesAreNotPooled(t *testing.T) {
	for _, tt := range []struct {
		size   int
		pooled bool
	}{{100, true}, {largeBodyChunk - 64, true}, {largeBodyChunk + 1, false}, {1 << 20, false}} {
		payload := bytes.Repeat([]byte{7}, tt.size)
		frame, err := MarshalPublish(PublishOpts{Topic: "t", Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		p, err := NewDecoder(bytes.NewReader(frame)).ReadPacket()
		if err != nil {
			t.Fatal(err)
		}
		pub := p.(*Publish)
		if pub.Pooled() != tt.pooled {
			t.Fatalf("%d-byte payload: Pooled() = %v, want %v", tt.size, pub.Pooled(), tt.pooled)
		}
		kept := pub.Payload
		p.Release()
		if !tt.pooled && !bytes.Equal(kept, payload) {
			t.Fatalf("%d-byte payload changed after Release", tt.size)
		}
	}
}

func TestMarshalPublishAllocatesOnce(t *testing.T) {
	opts := PublishOpts{Topic: "t", QoS: 1, PacketID: 1, Payload: make([]byte, 1<<20)}
	if n := testing.AllocsPerRun(10, func() {
		if _, err := MarshalPublish(opts); err != nil {
			t.Fatal(err)
		}
	}); n != 1 {
		t.Fatalf("MarshalPublish made %v allocations, want 1", n)
	}
}

// A large body is read with few, geometrically growing buffers: in all
// at most about 1.5 times the body, whatever its size, plus the pooled
// first stage when the pool is empty.
func TestLargeBodyAllocation(t *testing.T) {
	for _, size := range []int{largeBodyChunk + 1, 100 << 10, 512 << 10, 512<<10 + 1, 1 << 20, 4 << 20} {
		frame, err := MarshalPublish(PublishOpts{Topic: "t", Payload: make([]byte, size)})
		if err != nil {
			t.Fatal(err)
		}
		dec := NewDecoder(bytes.NewReader(append(append([]byte(nil), frame...), frame...)))
		warm, err := dec.ReadPacket() // fills the frame pool, as steady state has
		if err != nil {
			t.Fatal(err)
		}
		warm.Release()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		p, err := dec.ReadPacket()
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatal(err)
		}
		p.Release()
		if alloc := after.TotalAlloc - before.TotalAlloc; float64(alloc) > 1.55*float64(len(frame))+largeBodyChunk {
			t.Errorf("%d-byte frame: allocated %d bytes", len(frame), alloc)
		}
	}
}

// chunkReader returns one chunk per Read.
type chunkReader struct{ chunks [][]byte }

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[0])
	r.chunks[0] = r.chunks[0][n:]
	if len(r.chunks[0]) == 0 {
		r.chunks = r.chunks[1:]
	}
	return n, nil
}

// Ready reports a whole buffered packet, and nothing less: not an empty
// buffer, not part of a packet.
func TestDecoderReady(t *testing.T) {
	var frames [][]byte
	for i := range 3 {
		f, err := MarshalPublish(PublishOpts{Topic: "t", Payload: make([]byte, 200), QoS: 1, PacketID: uint16(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, f)
	}
	half := len(frames[2]) / 2
	first := append(append(append([]byte(nil), frames[0]...), frames[1]...), frames[2][:half]...)
	dec := NewDecoder(&chunkReader{chunks: [][]byte{first, frames[2][half:]}})
	for i, want := range []bool{false, true, false, false} {
		if got := dec.Ready(); got != want {
			t.Fatalf("before read %d: Ready = %v, want %v", i, got, want)
		}
		if i == 3 {
			break
		}
		p, err := dec.ReadPacket()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if id := p.(*Publish).PacketID; id != uint16(i+1) {
			t.Fatalf("read %d: packet %d", i, id)
		}
		p.Release()
	}
}
