// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package json

import (
	"reflect"
	"testing"
)

type sample struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestRoundTrip(t *testing.T) {
	codec := Codec[sample]{}
	want := sample{Name: "alice", Age: 42}
	b, err := codec.Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := codec.Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestDecodeInvalid(t *testing.T) {
	codec := Codec[sample]{}
	if _, err := codec.Decode([]byte("not-json")); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
