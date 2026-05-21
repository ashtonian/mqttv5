// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestVarintRoundTrip(t *testing.T) {
	// Sample boundary values plus a sweep across the four length bands.
	cases := []uint32{
		0, 1, 127, 128, 255, 16_383, 16_384, 65_535,
		2_097_151, 2_097_152, MaxVarintValue,
	}
	for _, v := range cases {
		t.Run(string(rune(v))+"-"+itoa(int(v)), func(t *testing.T) {
			var buf [4]byte
			n, err := EncodeVarint(buf[:], v)
			if err != nil {
				t.Fatalf("encode %d: %v", v, err)
			}
			if want := VarintSize(v); n != want {
				t.Fatalf("encode %d: wrote %d bytes, VarintSize said %d", v, n, want)
			}
			got, consumed, err := DecodeVarint(buf[:n])
			if err != nil {
				t.Fatalf("decode %d: %v", v, err)
			}
			if got != v {
				t.Fatalf("decode roundtrip: got %d, want %d", got, v)
			}
			if consumed != n {
				t.Fatalf("decode consumed %d, encode wrote %d", consumed, n)
			}
		})
	}
}

func TestEncodeVarintTooLarge(t *testing.T) {
	var buf [4]byte
	if _, err := EncodeVarint(buf[:], MaxVarintValue+1); !errors.Is(err, ErrVarintTooLarge) {
		t.Fatalf("expected ErrVarintTooLarge, got %v", err)
	}
}

func TestDecodeVarintMalformed(t *testing.T) {
	// A 4-byte sequence with continuation bits on every byte is malformed.
	bad := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	if _, _, err := DecodeVarint(bad); !errors.Is(err, ErrVarintMalformed) {
		t.Fatalf("expected ErrVarintMalformed, got %v", err)
	}
	// Truncated mid-encoding.
	if _, _, err := DecodeVarint([]byte{0x80}); !errors.Is(err, ErrVarintMalformed) {
		t.Fatalf("expected ErrVarintMalformed on truncated, got %v", err)
	}
}

func TestReadVarint(t *testing.T) {
	in := []byte{0xC0, 0x00} // encodes 64
	v, n, err := ReadVarint(bytes.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if v != 64 || n != 2 {
		t.Fatalf("got value=%d bytes=%d, want value=64 bytes=2", v, n)
	}
}

// itoa avoids strconv to keep test fixtures stdlib-tiny.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
