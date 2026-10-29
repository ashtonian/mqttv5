// Package json is a JSON Encoding for mqttv5's typed calls
// (Client.PublishValue, Client.SubscribeAs and their variants), built
// on the standard library's encoding/json. It sits in its own module so
// the core needs no codec; Encoding satisfies mqttv5.Encoding
// structurally, so this module does not import mqttv5.
package json

import "encoding/json"

// MediaType is the MQTT 5 Content Type a publish encoded by Encoding
// declares.
const MediaType = "application/json"

// Encoding encodes payloads as JSON with encoding/json. The zero value
// is ready to use and safe for concurrent use.
type Encoding struct{}

// ContentType returns [MediaType].
func (Encoding) ContentType() string { return MediaType }

// Marshal is json.Marshal.
func (Encoding) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal is json.Unmarshal; the decoded value keeps nothing of data.
func (Encoding) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
