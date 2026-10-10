// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package benchmarks

import (
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/eclipse/paho.golang/packets"

	"github.com/ashtonian/mqttv5/wire"
)

// The codec benchmarks compare mqttv5's wire package with
// eclipse/paho.golang's packets package on the same bytes, without a
// network. Names are key=value pairs for cmd/benchtab.

// codecCase is one PUBLISH shape: with no properties, or with a content
// type and five user properties as telemetry often carries.
type codecCase struct {
	props string
	frame func(payload []byte) []byte
}

var codecCases = []codecCase{
	{"none", func(p []byte) []byte { return EclipsePublishBytes(Topic, p) }},
	{"five", func(p []byte) []byte { return EclipsePublishBytesWithProps(Topic, p) }},
}

// BenchmarkDecodePublish decodes a PUBLISH from a stream, as a reader
// loop does: mqttv5 returns each packet to its pool, eclipse allocates a
// new one. mqttv5 decodes properties when they are read, eclipse while
// decoding; BenchmarkDecodePublishRead includes reading them.
func BenchmarkDecodePublish(b *testing.B) {
	runDecode(b, false)
}

// BenchmarkDecodePublishRead decodes a PUBLISH and reads its content
// type and every user property.
func BenchmarkDecodePublishRead(b *testing.B) {
	runDecode(b, true)
}

func runDecode(b *testing.B, read bool) {
	for _, c := range codecCases {
		for _, sz := range PayloadSizes {
			frame := c.frame(Payload(sz.Size))
			b.Run(fmt.Sprintf("lib=eclipse/props=%s/size=%s", c.props, sz.Name), func(b *testing.B) {
				r := &repeatReader{data: frame}
				b.SetBytes(int64(len(frame)))
				b.ReportAllocs()
				for range b.N {
					cp, err := packets.ReadPacket(r)
					if err != nil {
						b.Fatal(err)
					}
					pub := cp.Content.(*packets.Publish)
					if read {
						_ = pub.Properties.ContentType
						for range pub.Properties.User {
						}
					}
				}
			})
			b.Run(fmt.Sprintf("lib=mqttv5/props=%s/size=%s", c.props, sz.Name), func(b *testing.B) {
				d := wire.NewDecoder(&repeatReader{data: frame})
				if p, err := d.ReadPacket(); err != nil { // fill the frame pool
					b.Fatal(err)
				} else {
					p.Release()
				}
				b.SetBytes(int64(len(frame)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					p, err := d.ReadPacket()
					if err != nil {
						b.Fatal(err)
					}
					if read {
						pub := p.(*wire.Publish)
						_, _ = pub.Properties.String(wire.PropContentType)
						for range pub.Properties.UserProperties() {
						}
					}
					p.Release()
				}
			})
		}
	}
}

// BenchmarkEncodePublish encodes a QoS 1 PUBLISH and writes it to
// io.Discard. eclipse goes through ControlPacket.WriteTo, which writes
// its header and the payload as separate buffers. mqttv5 has three
// encoders, each measured here on the same packet; which one a publish
// uses depends on its path through the client:
//
//   - mqttv5-header: AppendPublishHeader, written with the caller's
//     payload as one net.Buffers, without copying it. A QoS 0 publish a
//     caller writes on its own goroutine (TCP, PublishWaitForFlush, an
//     idle connection).
//   - mqttv5-pooled: EncodePublish into a pooled buffer, the payload
//     copied. Any other QoS 0 publish, queued for the writer goroutine,
//     whose caller may reuse its payload before the write.
//   - mqttv5-owned: MarshalPublish into a buffer of its own, the payload
//     copied. Every QoS 1/2 publish: the session keeps the packet to
//     send again until the broker acknowledges it.
func BenchmarkEncodePublish(b *testing.B) {
	for _, props := range []string{"none", "five"} {
		for _, sz := range PayloadSizes {
			payload := Payload(sz.Size)
			b.Run(fmt.Sprintf("lib=eclipse/props=%s/size=%s", props, sz.Name), func(b *testing.B) {
				cp := packets.NewControlPacket(packets.PUBLISH)
				pub := cp.Content.(*packets.Publish)
				pub.QoS, pub.PacketID, pub.Topic, pub.Payload = 1, 1, Topic, payload
				if props == "five" {
					pub.Properties.ContentType = contentType
					for _, u := range userProperties {
						pub.Properties.User = append(pub.Properties.User, packets.User{Key: u.Key, Value: u.Value})
					}
				}
				b.ReportAllocs()
				var total int64
				for range b.N {
					n, err := cp.WriteTo(io.Discard)
					if err != nil {
						b.Fatal(err)
					}
					total += n
				}
				b.SetBytes(total / int64(b.N))
			})
			opts := wire.PublishOpts{Topic: Topic, Payload: payload, QoS: 1, PacketID: 1}
			if props == "five" {
				opts.ContentType = contentType
				opts.UserProperties = userProperties
			}
			b.Run(fmt.Sprintf("lib=mqttv5-header/props=%s/size=%s", props, sz.Name), func(b *testing.B) {
				// The header buffer and the buffer list are reused across
				// writes, as a connection reuses them.
				var hdr []byte
				vec := make(net.Buffers, 0, 2)
				b.ReportAllocs()
				var total int64
				for range b.N {
					var err error
					hdr, err = wire.AppendPublishHeader(hdr[:0], opts)
					if err != nil {
						b.Fatal(err)
					}
					full := append(vec[:0], hdr, opts.Payload)
					vec = full
					n, _ := vec.WriteTo(io.Discard)
					clear(full)
					vec = full[:0]
					total += n
				}
				b.SetBytes(total / int64(b.N))
			})
			b.Run(fmt.Sprintf("lib=mqttv5-pooled/props=%s/size=%s", props, sz.Name), func(b *testing.B) {
				b.ReportAllocs()
				var total int64
				for range b.N {
					bp, err := wire.EncodePublish(opts)
					if err != nil {
						b.Fatal(err)
					}
					n, _ := io.Discard.Write(*bp)
					wire.ReleaseBuf(bp)
					total += int64(n)
				}
				b.SetBytes(total / int64(b.N))
			})
			b.Run(fmt.Sprintf("lib=mqttv5-owned/props=%s/size=%s", props, sz.Name), func(b *testing.B) {
				b.ReportAllocs()
				var total int64
				for range b.N {
					pkt, err := wire.MarshalPublish(opts)
					if err != nil {
						b.Fatal(err)
					}
					n, _ := io.Discard.Write(pkt)
					total += int64(n)
				}
				b.SetBytes(total / int64(b.N))
			})
		}
	}
}

// repeatReader serves data over and over, so a decoder reads the same
// frame forever without the benchmark re-creating it.
type repeatReader struct {
	data []byte
	pos  int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) {
		if r.pos >= len(r.data) {
			r.pos = 0
		}
		c := copy(p[n:], r.data[r.pos:])
		n += c
		r.pos += c
	}
	return n, nil
}
