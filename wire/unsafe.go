// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import "unsafe"

// bytesToString returns a string that aliases the supplied byte slice.
// No allocation, no copy.
//
// CRITICAL CONTRACT: the byte slice must NOT be mutated for as long as
// the returned string is reachable, and the string must NOT outlive the
// underlying memory.
//
// In the wire package this is sound because:
//   - frame buffers from bufpool are only mutated by readers (the
//     decoder reads into them, then the body is parsed into immutable
//     []byte / string aliases until Release).
//   - on Release the frame returns to the pool; any caller holding a
//     reference past Release is violating the Packet lifetime contract,
//     and the docs say so.
//
// If a caller needs to retain a string past Release, they call
// strings.Clone (one alloc).
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
