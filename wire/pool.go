// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import "sync"

// defaultBufferCap is the starting capacity of pooled []byte buffers.
// Sized to comfortably hold a typical small PUBLISH (1-2 KiB) without
// re-allocation. Larger packets grow on demand.
const defaultBufferCap = 4096

// bufPool stores reusable []byte buffers via *[]byte to avoid the
// interface boxing alloc that sync.Pool of a slice value would incur.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, defaultBufferCap)
		return &b
	},
}

// acquireBuf returns a pointer to a slice with length n. The underlying
// array may have been used previously; the n bytes will be overwritten
// by the caller before any read.
//
// If the pool's current buffer is smaller than n, a fresh slice is
// allocated and replaces the pool's storage on Release.
func acquireBuf(n int) *[]byte {
	bp := bufPool.Get().(*[]byte)
	if cap(*bp) < n {
		*bp = make([]byte, n)
	} else {
		*bp = (*bp)[:n]
	}
	return bp
}

// releaseBuf returns the buffer to the pool. The caller MUST NOT use
// the slice after this returns — that includes any sub-slice it handed
// out to packet fields.
func releaseBuf(bp *[]byte) {
	*bp = (*bp)[:0]
	bufPool.Put(bp)
}
