// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"math/rand/v2"
	"time"
)

// ---------------- Authenticator (MQTT v5 enhanced authentication) ----------------

// Authenticator drives the client side of MQTT v5 enhanced
// authentication — the challenge-response handshake that runs during
// CONNECT (SCRAM, Kerberos, OAuth challenge, etc.).
//
// Method() supplies the AuthenticationMethod property; Begin() the
// initial AuthenticationData. For each AUTH packet from the broker
// before CONNACK, Continue(brokerData) returns the next response.
// done=true is informational — the broker decides when to send
// CONNACK. A non-nil error from Continue aborts the connection.
//
// Implementations need only be safe for concurrent use when shared
// across multiple Clients (e.g. with WithPublisherPool).
type Authenticator interface {
	Method() string
	Begin() (data []byte, err error)
	Continue(brokerData []byte) (response []byte, done bool, err error)
}

// WithAuthenticator enables MQTT v5 enhanced authentication. When
// combined with WithPublisherPool, the same Authenticator runs
// concurrently across the parent and every pool member; implementations
// used that way must be safe for concurrent use.
func WithAuthenticator(a Authenticator) Option {
	return func(c *Config) error {
		c.Authenticator = a
		return nil
	}
}

// ---------------- Reconnect backoff ----------------

// Backoff returns the delay before retry attempt N. The first retry
// after a failed connect is attempt 0 (the initial connect itself
// runs immediately, no backoff). Implementations must be safe for
// concurrent use.
type Backoff func(attempt int) time.Duration

// ConstantBackoff returns the same delay for every attempt.
// Predictable but vulnerable to thundering-herd reconnect storms;
// ExponentialBackoff is usually preferable.
func ConstantBackoff(d time.Duration) Backoff {
	return func(int) time.Duration { return d }
}

// ExponentialBackoff doubles min each attempt up to max with
// ±jitter added (range [computed-jitter, computed+jitter]).
func ExponentialBackoff(min, max, jitter time.Duration) Backoff {
	if min <= 0 {
		min = time.Second
	}
	if max < min {
		max = min
	}
	return func(attempt int) time.Duration {
		if attempt < 0 {
			attempt = 0
		}
		// Cap shift at 30 to keep the math sane on absurd attempt counts.
		shift := attempt
		if shift > 30 {
			shift = 30
		}
		d := min << shift
		if d > max || d < min { // overflow guard
			d = max
		}
		if jitter > 0 {
			// Symmetric jitter in the range [-jitter, +jitter].
			j := time.Duration(rand.Int64N(int64(jitter*2)+1)) - jitter
			d += j
		}
		if d < 0 {
			return 0
		}
		return d
	}
}
