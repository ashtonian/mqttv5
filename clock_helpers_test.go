// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import "github.com/ashtonian/mqttv5/internal/clock"

// withClock injects a time source (normally a *clock.Fake) for tests.
func withClock(c clock.Clock) Option {
	return func(cfg *Config) error {
		cfg.clock = c
		return nil
	}
}
