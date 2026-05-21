// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package json is the JSON Codec[T] implementation. It sits in its
// own package so the core mqttv5 module can stay encoding/json-free
// for callers that want a different codec (protobuf, msgpack, etc.).
package json

import "encoding/json"

// Codec is a JSON encoder/decoder for T. The zero value is usable.
type Codec[T any] struct{}

// Encode marshals v.
func (Codec[T]) Encode(v T) ([]byte, error) { return json.Marshal(v) }

// Decode unmarshals b into a fresh T.
func (Codec[T]) Decode(b []byte) (T, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}
