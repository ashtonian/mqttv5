// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package msgpack is the MessagePack Codec[T] implementation.
//
// External dependency: github.com/vmihailenco/msgpack/v5 — well-known
// API mirroring encoding/json (Marshal/Unmarshal). Pull in this
// package only when you need msgpack; the core mqttv5 module is
// stdlib-only by design.
//
// For other encodings (Protobuf, FlatBuffers, Avro, custom binary)
// implement the mqttv5.Codec[T] interface directly — there is nothing
// in this package that the rest of mqttv5 depends on.
package msgpack

import "github.com/vmihailenco/msgpack/v5"

// Codec is a MessagePack encoder/decoder for T. The zero value is
// usable.
type Codec[T any] struct{}

// Encode marshals v to MessagePack.
func (Codec[T]) Encode(v T) ([]byte, error) { return msgpack.Marshal(v) }

// Decode unmarshals b into a fresh T.
func (Codec[T]) Decode(b []byte) (T, error) {
	var v T
	err := msgpack.Unmarshal(b, &v)
	return v, err
}
