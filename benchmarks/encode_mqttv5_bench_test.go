package benchmarks

import (
	"io"
	"testing"

	"github.com/ashtonian/mqttv5/wire"
)

// BenchmarkEncode_mqttv5_PUBLISH measures WritePublish into io.Discard.
//
// PublishOpts is constructed once outside the loop (same shape as
// eclipse's benchmark which reuses the ControlPacket struct), so the
// timed work is purely encode + Discard.Write.
func BenchmarkEncode_mqttv5_PUBLISH(b *testing.B) {
	for _, sz := range PayloadSizes {
		opts := wire.PublishOpts{
			Topic:    Topic,
			Payload:  Payload(sz.Size),
			QoS:      1,
			PacketID: 1,
		}
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var total int64
			for range b.N {
				n, err := wire.WritePublish(io.Discard, opts)
				if err != nil {
					b.Fatal(err)
				}
				total += n
			}
			b.SetBytes(total / int64(b.N))
		})
	}
}

// BenchmarkEncode_mqttv5_PUBLISH_WithProps measures encode with the same
// content-type + 5 user properties used in the eclipse comparison.
func BenchmarkEncode_mqttv5_PUBLISH_WithProps(b *testing.B) {
	for _, sz := range PayloadSizes {
		opts := wire.PublishOpts{
			Topic:       Topic,
			Payload:     Payload(sz.Size),
			QoS:         1,
			PacketID:    1,
			ContentType: "application/json",
			UserProperties: []wire.UserProperty{
				{Key: "device_id", Value: "sensor-0001"},
				{Key: "site", Value: "us-west-2"},
				{Key: "schema", Value: "v3"},
				{Key: "tenant", Value: "acme"},
				{Key: "trace_id", Value: "01HXZ7QY9V5N3RW4G6FJ8KE2BD"},
			},
		}
		b.Run(sz.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var total int64
			for range b.N {
				n, err := wire.WritePublish(io.Discard, opts)
				if err != nil {
					b.Fatal(err)
				}
				total += n
			}
			b.SetBytes(total / int64(b.N))
		})
	}
}
