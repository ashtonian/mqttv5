// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"math/rand/v2"
	"time"
)

// ---------------- Authenticator (MQTT v5 enhanced authentication) ----------------

// Authenticator drives the client side of MQTT v5 enhanced
// authentication — the challenge-response handshake that runs during
// CONNECT and during re-authentication (SCRAM, Kerberos, OAuth/JWT,
// etc.).
//
// Method() supplies the AuthenticationMethod property. Begin(ctx)
// resolves the initial AuthenticationData — for a bearer method this is
// where a fresh token is fetched, so ctx carries the deadline of the
// triggering operation (the connect timeout, or the ctx passed to
// Reauthenticate); honor it for any I/O. For each broker AUTH challenge
// — during CONNECT (before CONNACK) and during mid-session
// re-authentication (§4.12) — Continue(brokerData) returns the next
// response, which the client always sends as AUTH 0x18 Continue.
//
// done is informational only. The client never concludes the exchange:
// per §3.15.2.1 only the server sends AUTH 0x00 Success (or CONNACK for
// the initial handshake). Returning done=true does NOT make the client
// emit 0x00 — treat it as a hint that the Authenticator has no further
// data to send. A non-nil error from Continue aborts the connection.
//
// Implementations need only be safe for concurrent use when shared
// across multiple Clients (e.g. with WithPublisherPool).
type Authenticator interface {
	Method() string
	Begin(ctx context.Context) (data []byte, err error)
	Continue(brokerData []byte) (response []byte, done bool, err error)
}

// ServerFinalVerifier is an optional interface an Authenticator may also
// implement to verify the server's concluding AuthenticationData — the
// CONNACK on the initial handshake, or the AUTH 0x00 Success that ends a
// re-authentication (§4.12). For mechanisms with mutual authentication
// (e.g. SCRAM, where the server proves knowledge of the credential via a
// server signature) this closes the loop; a non-nil error aborts the
// connect, or fails Reauthenticate and tears the connection down.
//
// The serverData is whatever AuthenticationData the broker attached to the
// terminal packet (possibly empty). Authenticators whose mechanism has no
// server proof (e.g. a bearer token) simply do not implement this
// interface, and the terminal data is ignored. VerifyServerFinal is
// called on the same goroutine as the corresponding Continue.
type ServerFinalVerifier interface {
	VerifyServerFinal(serverData []byte) error
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
