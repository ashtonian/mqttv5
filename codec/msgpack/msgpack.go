// Package msgpack is a MessagePack Encoding for mqttv5's typed calls
// (Client.PublishValue, Client.SubscribeAs and their variants).
//
// External dependency: github.com/vmihailenco/msgpack/v5, whose API
// mirrors encoding/json. Pull in this module only when you need
// msgpack; the core mqttv5 module is stdlib-only by design. Encoding
// satisfies mqttv5.Encoding structurally, so this module does not
// import mqttv5.
//
// For other formats (Protobuf, FlatBuffers, Avro, custom binary)
// implement mqttv5.Encoding directly.
package msgpack

import "github.com/vmihailenco/msgpack/v5"

// MediaType is the MQTT 5 Content Type a publish encoded by Encoding
// declares: MessagePack's IANA registration.
const MediaType = "application/vnd.msgpack"

// Encoding encodes payloads as MessagePack. The zero value is ready to
// use and safe for concurrent use.
type Encoding struct{}

// ContentType returns [MediaType].
func (Encoding) ContentType() string { return MediaType }

// Marshal is msgpack.Marshal.
func (Encoding) Marshal(v any) ([]byte, error) { return msgpack.Marshal(v) }

// Unmarshal is msgpack.Unmarshal; the decoded value keeps nothing of
// data.
func (Encoding) Unmarshal(data []byte, v any) error { return msgpack.Unmarshal(data, v) }
