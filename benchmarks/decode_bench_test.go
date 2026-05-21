package benchmarks

import (
	"testing"

	"github.com/eclipse/paho.golang/packets"
)

// BenchmarkDecode_Eclipse_PUBLISH measures the steady-state per-packet
// decode cost. Same repeatReader pattern as the mqttv5 variant so the
// readers are not the variable.
//
// Eclipse paho's packets.ReadPacket allocates per call (the packet
// struct, the *Properties, the bytes.Buffer for the body) so there is
// no decoder state to amortize.
func BenchmarkDecode_Eclipse_PUBLISH(b *testing.B) {
	for _, sz := range PayloadSizes {
		fixture := EclipsePublishBytes(Topic, Payload(sz.Size))
		b.Run(sz.Name, func(b *testing.B) {
			r := &repeatReader{data: fixture}
			b.SetBytes(int64(len(fixture)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				cp, err := packets.ReadPacket(r)
				if err != nil {
					b.Fatal(err)
				}
				_ = cp.Content.(*packets.Publish)
			}
		})
	}
}

// BenchmarkDecode_Eclipse_PUBLISH_WithProps adds five user properties +
// a content-type to exercise the properties decoder, which has its own
// allocation profile. Eclipse paho eagerly decodes properties — there is
// no "without read" variant because the work has already happened by
// the time ReadPacket returns.
func BenchmarkDecode_Eclipse_PUBLISH_WithProps(b *testing.B) {
	for _, sz := range PayloadSizes {
		fixture := EclipsePublishBytesWithProps(Topic, Payload(sz.Size))
		b.Run(sz.Name, func(b *testing.B) {
			r := &repeatReader{data: fixture}
			b.SetBytes(int64(len(fixture)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				cp, err := packets.ReadPacket(r)
				if err != nil {
					b.Fatal(err)
				}
				_ = cp.Content.(*packets.Publish)
			}
		})
	}
}
