// Package benchmarks holds head-to-head benchmarks comparing
// github.com/ashtonian/mqttv5 (the rewrite) against
// github.com/eclipse/paho.golang/packets (the baseline).
//
// Fixtures are pre-built wire-format byte slices for representative
// packet shapes. Generating them lives outside the benchmark loop so the
// timed code path matches what a real receiver does: read bytes, decode.
package benchmarks

import (
	"bytes"

	"github.com/eclipse/paho.golang/packets"
)

// PayloadSize is a parameterised payload size for size-sweep benchmarks.
type PayloadSize struct {
	Name string
	Size int
}

// PayloadSizes covers small (sensor-reading), medium (typical JSON), and
// large (binary blob) shapes.
var PayloadSizes = []PayloadSize{
	{"64B", 64},
	{"256B", 256},
	{"1KiB", 1024},
	{"16KiB", 16 * 1024},
}

// EclipsePublishBytes serialises a PUBLISH packet using eclipse/paho.golang
// and returns the resulting wire bytes. This is the byte sequence the
// decoder benchmarks feed back into eclipse paho's reader.
//
// QoS 1 because that's the most common production setting and exercises the
// PacketID path. The fixture has no user properties; a separate
// EclipsePublishBytesWithProps adds them.
func EclipsePublishBytes(topic string, payload []byte) []byte {
	cp := packets.NewControlPacket(packets.PUBLISH)
	pub := cp.Content.(*packets.Publish)
	pub.QoS = 1
	pub.PacketID = 1
	pub.Topic = topic
	pub.Payload = payload

	var buf bytes.Buffer
	if _, err := cp.WriteTo(&buf); err != nil {
		panic(err) // fixture build, not a hot path
	}
	return buf.Bytes()
}

// EclipsePublishBytesWithProps is the same as EclipsePublishBytes but adds
// five user properties — typical for telemetry that tags origin, device,
// schema version, etc.
func EclipsePublishBytesWithProps(topic string, payload []byte) []byte {
	cp := packets.NewControlPacket(packets.PUBLISH)
	pub := cp.Content.(*packets.Publish)
	pub.QoS = 1
	pub.PacketID = 1
	pub.Topic = topic
	pub.Payload = payload
	contentType := "application/json"
	pub.Properties.ContentType = contentType
	pub.Properties.User = []packets.User{
		{Key: "device_id", Value: "sensor-0001"},
		{Key: "site", Value: "us-west-2"},
		{Key: "schema", Value: "v3"},
		{Key: "tenant", Value: "acme"},
		{Key: "trace_id", Value: "01HXZ7QY9V5N3RW4G6FJ8KE2BD"},
	}

	var buf bytes.Buffer
	if _, err := cp.WriteTo(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// Payload returns a deterministic payload of the requested size, filled
// with a repeating pattern so compression-aware brokers don't skew
// benchmark behaviour.
func Payload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

// Topic is the canonical benchmark topic — depth 4, no wildcards.
const Topic = "bench/devices/sensor-0001/telemetry"
