// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package soak sizes the randomized tests. By default a test runs its
// own number of seeds starting at 0; the nightly soak workflow sets
// MQTTV5_SEEDS and MQTTV5_SEED_BASE to run many more, starting
// somewhere new each night. A failing test prints its seed, and running
// with MQTTV5_SEED_BASE set to it and MQTTV5_SEEDS=1 reproduces it.
package soak

import (
	"os"
	"strconv"
	"testing"
)

// Seeds returns the first seed and how many to run: n by default, short
// under -short, or what the environment asks for.
func Seeds(t testing.TB, n, short uint64) (base, count uint64) {
	t.Helper()
	count = n
	if testing.Short() {
		count = short
	}
	if v := os.Getenv("MQTTV5_SEEDS"); v != "" {
		c, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("MQTTV5_SEEDS=%q: %v", v, err)
		}
		count = c
	}
	if v := os.Getenv("MQTTV5_SEED_BASE"); v != "" {
		b, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("MQTTV5_SEED_BASE=%q: %v", v, err)
		}
		base = b
	}
	return base, count
}
