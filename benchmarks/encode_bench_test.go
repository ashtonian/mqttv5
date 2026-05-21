package benchmarks

import (
	"io"
	"testing"

	"github.com/eclipse/paho.golang/packets"
)

// BenchmarkEncode_Eclipse_PUBLISH measures eclipse/paho.golang's full
// publish encode path: build *Publish -> WriteTo(io.Discard).
//
// We rebuild the *ControlPacket once per b.Run (not per iteration) so the
// allocation profile reflects steady-state encoding of an already-prepared
// packet, which is the realistic publisher hot path — a publisher
// typically holds long-lived packet structs and writes them repeatedly.
// The mqttv5 counterpart (PubOpts -> net.Buffers) lives in
// encode_mqttv5_bench_test.go.
func BenchmarkEncode_Eclipse_PUBLISH(b *testing.B) {
	for _, sz := range PayloadSizes {
		cp := packets.NewControlPacket(packets.PUBLISH)
		pub := cp.Content.(*packets.Publish)
		pub.QoS = 1
		pub.PacketID = 1
		pub.Topic = Topic
		pub.Payload = Payload(sz.Size)

		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
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
	}
}

// BenchmarkEncode_Eclipse_PUBLISH_WithProps adds the same five user
// properties used in the decode-with-props benchmark.
func BenchmarkEncode_Eclipse_PUBLISH_WithProps(b *testing.B) {
	for _, sz := range PayloadSizes {
		cp := packets.NewControlPacket(packets.PUBLISH)
		pub := cp.Content.(*packets.Publish)
		pub.QoS = 1
		pub.PacketID = 1
		pub.Topic = Topic
		pub.Payload = Payload(sz.Size)
		contentType := "application/json"
		pub.Properties.ContentType = contentType
		pub.Properties.User = []packets.User{
			{Key: "device_id", Value: "sensor-0001"},
			{Key: "site", Value: "us-west-2"},
			{Key: "schema", Value: "v3"},
			{Key: "tenant", Value: "acme"},
			{Key: "trace_id", Value: "01HXZ7QY9V5N3RW4G6FJ8KE2BD"},
		}

		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
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
	}
}
