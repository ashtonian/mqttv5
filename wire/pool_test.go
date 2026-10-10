// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import "testing"

func TestAcquireBufUsesSmallestFittingClass(t *testing.T) {
	tests := []struct {
		n       int
		wantCap int
	}{
		{0, 256},
		{1, 256},
		{256, 256},
		{257, 1 << 10},
		{4 << 10, 4 << 10},
		{4<<10 + 1, 16 << 10},
		{64 << 10, 64 << 10},
		{64<<10 + 1, 64<<10 + 1},
		{1 << 20, 1 << 20},
	}
	for _, tt := range tests {
		bp := acquireBuf(tt.n)
		if len(*bp) != tt.n || cap(*bp) != tt.wantCap {
			t.Errorf("acquireBuf(%d): len %d cap %d, want len %d cap %d", tt.n, len(*bp), cap(*bp), tt.n, tt.wantCap)
		}
		releaseBuf(bp)
	}
}

func TestReleaseBufDropsUnclassedBuffers(t *testing.T) {
	for _, c := range []int{100, 300, 64<<10 + 1, 1 << 20} {
		b := make([]byte, 0, c)
		releaseBuf(&b)
		for i, size := range bufClasses {
			if size < c {
				continue
			}
			got := bufPools[i].Get().(*[]byte)
			if cap(*got) != size {
				t.Fatalf("class %d holds a buffer of cap %d after releasing cap %d", size, cap(*got), c)
			}
		}
	}
}

// A small packet read after a large one must not inherit the large
// buffer: that is how one burst used to pin its size per queued message.
func TestSmallAcquireAfterLargeReleaseStaysSmall(t *testing.T) {
	big := acquireBuf(60 << 10)
	releaseBuf(big)
	small := acquireBuf(64)
	defer releaseBuf(small)
	if cap(*small) != 256 {
		t.Fatalf("64 B acquire after 60 KiB release has cap %d, want 256", cap(*small))
	}
}
