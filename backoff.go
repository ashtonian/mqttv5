package mqttv5

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff is a retry delay policy: Delay returns how long to wait before
// retry attempt n, counting from 0. It must be safe for concurrent use.
// [ExponentialBackoff] and [ConstantBackoff] are the policies the client
// ships; [BackoffFunc] adapts any function.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// DefaultReconnectBackoff is the reconnect backoff when
// [WithReconnectBackoff] is not set: 1 s doubling to 30 s, each delay
// drawn from its upper half.
var DefaultReconnectBackoff = ExponentialBackoff{Initial: time.Second, Max: 30 * time.Second, Jitter: 0.5}

// ExponentialBackoff grows the delay from Initial by Multiplier per
// attempt, up to Max, and takes a random part of up to Jitter off each
// delay so that clients that failed together do not retry together.
type ExponentialBackoff struct {
	// Initial is the delay before attempt 0. Must be positive.
	Initial time.Duration
	// Max caps the delay. Must be at least Initial.
	Max time.Duration
	// Multiplier is the growth per attempt, at least 1. Zero means 2.
	Multiplier float64
	// Jitter is the fraction of each delay that is random, from 0 to 1:
	// a delay d becomes a uniform draw from [(1-Jitter)·d, d]. 0 retries
	// at exactly d; 0.5 keeps at least half of it; 1 ("full jitter")
	// spreads a fleet the widest. Jitter never lengthens a delay, so Max
	// stays a ceiling.
	Jitter float64
}

// Delay returns the delay before attempt n.
func (b ExponentialBackoff) Delay(attempt int) time.Duration {
	m := b.Multiplier
	if m == 0 {
		m = 2
	}
	d := b.Max
	// In float64 a large attempt count saturates to +Inf rather than
	// overflowing; anything at or past Max is Max.
	if grown := float64(b.Initial) * math.Pow(m, float64(max(attempt, 0))); grown < float64(b.Max) {
		d = time.Duration(grown)
	}
	if b.Jitter > 0 && d > 0 {
		spread := time.Duration(float64(d) * min(b.Jitter, 1))
		if spread <= 0 || spread > d {
			// float64 rounds a delay near MaxInt64 up past it.
			spread = d
		}
		d -= time.Duration(rand.Int64N(int64(spread)))
	}
	return d
}

func (b ExponentialBackoff) validate() error {
	switch {
	case b.Initial <= 0:
		return fmt.Errorf("mqttv5: ExponentialBackoff Initial %v must be positive", b.Initial)
	case b.Max < b.Initial:
		return fmt.Errorf("mqttv5: ExponentialBackoff Max %v is below Initial %v", b.Max, b.Initial)
	case b.Multiplier != 0 && !(b.Multiplier >= 1):
		return fmt.Errorf("mqttv5: ExponentialBackoff Multiplier %v must be at least 1", b.Multiplier)
	case !(b.Jitter >= 0 && b.Jitter <= 1):
		return fmt.Errorf("mqttv5: ExponentialBackoff Jitter %v must be within [0, 1]", b.Jitter)
	}
	return nil
}

// ConstantBackoff waits the same time before every attempt. Clients that
// fail together retry in lockstep with it: for reconnects prefer an
// [ExponentialBackoff] with Jitter.
type ConstantBackoff time.Duration

// Delay returns d whatever the attempt.
func (d ConstantBackoff) Delay(int) time.Duration { return time.Duration(d) }

func (d ConstantBackoff) validate() error {
	if d < 0 {
		return fmt.Errorf("mqttv5: negative ConstantBackoff %v", time.Duration(d))
	}
	return nil
}

// BackoffFunc adapts a function to [Backoff], for policies the client
// does not ship.
type BackoffFunc func(attempt int) time.Duration

// Delay returns f(attempt).
func (f BackoffFunc) Delay(attempt int) time.Duration { return f(attempt) }

func (f BackoffFunc) validate() error {
	if f == nil {
		return errors.New("mqttv5: nil BackoffFunc")
	}
	return nil
}

// validateBackoff rejects a policy that cannot produce delays. A policy
// of the caller's own type is the caller's to get right.
func validateBackoff(b Backoff) error {
	if b == nil {
		return errors.New("mqttv5: nil Backoff")
	}
	if v, ok := b.(interface{ validate() error }); ok {
		return v.validate()
	}
	return nil
}
