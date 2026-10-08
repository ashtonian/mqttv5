// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import "testing"

// The sink accepts each message once, intact, and nothing else.
func TestSinkChecksEveryDelivery(t *testing.T) {
	msg := func(seq uint64) []byte {
		p := Payload(64)
		stampSeq(p, seq)
		return p
	}
	for _, tc := range []struct {
		name    string
		deliver func(s *sink)
		ok      bool
	}{
		{"every message once", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)) }, true},
		{"a duplicate", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)); s.onMsg(msg(1)) }, false},
		{"one missing", func(s *sink) { s.onMsg(msg(0)) }, false},
		{"an interior byte changed", func(s *sink) {
			p := msg(1)
			p[32] ^= 0xff
			s.onMsg(msg(0))
			s.onMsg(p)
		}, false},
		{"two messages' halves", func(s *sink) {
			p := msg(1)
			copy(p[32:], msg(0)[32:])
			s.onMsg(msg(0))
			s.onMsg(p)
		}, false},
		{"a sequence number never sent", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)); s.onMsg(msg(7)) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSink(64, 2)
			tc.deliver(s)
			err := s.verify(func() int64 { return 0 })
			if (err == nil) != tc.ok {
				t.Fatalf("verify: %v, want ok %v", err, tc.ok)
			}
		})
	}
}
