// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import "sync"

// bufClasses are the capacities of pooled frame buffers. A request is
// served from the smallest class that fits, so a buffer never pins more
// than 4x the bytes it holds (plus the 256 B floor). Requests above the
// largest class are allocated exactly and left to the GC: pooling them
// would let one large packet pin its size for the life of the pool.
var bufClasses = [...]int{256, 1 << 10, 4 << 10, 16 << 10, 64 << 10}

// bufPools holds one pool per class. Pools store *[]byte to avoid the
// interface boxing allocation a slice value would incur.
var bufPools [len(bufClasses)]sync.Pool

func init() {
	for i, size := range bufClasses {
		bufPools[i].New = func() any {
			b := make([]byte, 0, size)
			return &b
		}
	}
}

// bufClass returns the index of the smallest class holding n bytes, or
// -1 when n is larger than every class.
func bufClass(n int) int {
	for i, size := range bufClasses {
		if n <= size {
			return i
		}
	}
	return -1
}

// acquireBuf returns a pointer to a slice of length n. The bytes may hold
// data from an earlier use; the caller overwrites them before reading.
func acquireBuf(n int) *[]byte {
	i := bufClass(n)
	if i < 0 {
		b := make([]byte, n)
		return &b
	}
	bp := bufPools[i].Get().(*[]byte)
	*bp = (*bp)[:n]
	return bp
}

// releaseBuf returns the buffer to its class pool. The caller MUST NOT use
// the slice after this returns — that includes any sub-slice it handed
// out to packet fields. Buffers whose capacity is not exactly a class
// size (oversized allocations) are dropped.
func releaseBuf(bp *[]byte) {
	if !pooled(*bp) {
		return
	}
	*bp = (*bp)[:0]
	bufPools[bufClass(cap(*bp))].Put(bp)
}

// pooled reports whether releaseBuf recycles b.
func pooled(b []byte) bool {
	i := bufClass(cap(b))
	return i >= 0 && bufClasses[i] == cap(b)
}
