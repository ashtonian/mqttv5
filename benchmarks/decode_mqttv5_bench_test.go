package benchmarks

import (
	"io"
	"testing"

	"github.com/ashtonian/mqttv5/wire"
)

// repeatReader cycles infinitely over data so a benchmark can call Read
// without worrying about EOF. The bufio inside wire.Decoder sees the
// stream as continuous packets, mirroring what a real subscriber on a
// busy connection would see.
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

// BenchmarkDecode_mqttv5_PUBLISH measures the steady-state per-packet
// decode cost: one Decoder constructed once, reading a continuous stream
// of identical fixture packets. The Release call returns the packet +
// frame to their pools.
func BenchmarkDecode_mqttv5_PUBLISH(b *testing.B) {
	for _, sz := range PayloadSizes {
		fixture := EclipsePublishBytes(Topic, Payload(sz.Size))
		b.Run(sz.Name, func(b *testing.B) {
			d := wire.NewDecoder(&repeatReader{data: fixture})
			// Warm pool with one read so first iteration matches steady state.
			if pkt, err := d.ReadPacket(); err != nil {
				b.Fatal(err)
			} else {
				pkt.Release()
			}

			b.SetBytes(int64(len(fixture)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				pkt, err := d.ReadPacket()
				if err != nil {
					b.Fatal(err)
				}
				pkt.Release()
			}
		})
	}
}

// BenchmarkDecode_mqttv5_PUBLISH_WithProps measures decode + Release
// when the packet has 5 user properties + a content type but the
// handler doesn't read any of them. With lazy properties this should
// not pay for property decoding.
func BenchmarkDecode_mqttv5_PUBLISH_WithProps(b *testing.B) {
	for _, sz := range PayloadSizes {
		fixture := EclipsePublishBytesWithProps(Topic, Payload(sz.Size))
		b.Run(sz.Name, func(b *testing.B) {
			d := wire.NewDecoder(&repeatReader{data: fixture})
			if pkt, err := d.ReadPacket(); err != nil {
				b.Fatal(err)
			} else {
				pkt.Release()
			}

			b.SetBytes(int64(len(fixture)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				pkt, err := d.ReadPacket()
				if err != nil {
					b.Fatal(err)
				}
				pkt.Release()
			}
		})
	}
}

// BenchmarkDecode_mqttv5_PUBLISH_WithPropsRead is the apples-to-apples
// comparison with eclipse's eager decode: it touches every property
// after Read. This is the upper bound for a handler that actually uses
// properties.
func BenchmarkDecode_mqttv5_PUBLISH_WithPropsRead(b *testing.B) {
	for _, sz := range PayloadSizes {
		fixture := EclipsePublishBytesWithProps(Topic, Payload(sz.Size))
		b.Run(sz.Name, func(b *testing.B) {
			d := wire.NewDecoder(&repeatReader{data: fixture})
			if pkt, err := d.ReadPacket(); err != nil {
				b.Fatal(err)
			} else {
				pkt.Release()
			}

			b.SetBytes(int64(len(fixture)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				pkt, err := d.ReadPacket()
				if err != nil {
					b.Fatal(err)
				}
				pub := pkt.(*wire.Publish)
				_, _ = pub.Properties.String(wire.PropContentType)
				for range pub.Properties.UserProperties() {
				}
				pkt.Release()
			}
		})
	}
}
