// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"math"
	"testing"
	"time"
)

func TestExponentialBackoffDelays(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		name string
		b    ExponentialBackoff
		want []time.Duration // by attempt, from 0
	}{
		{
			name: "doubles without a Multiplier",
			b:    ExponentialBackoff{Initial: time.Second, Max: 10 * time.Second},
			want: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second},
		},
		{
			name: "grows by Multiplier",
			b:    ExponentialBackoff{Initial: 100 * ms, Max: time.Second, Multiplier: 1.5},
			want: []time.Duration{100 * ms, 150 * ms, 225 * ms, 337500 * time.Microsecond, 506250 * time.Microsecond, 759375 * time.Microsecond, time.Second},
		},
		{
			name: "Multiplier 1 keeps Initial",
			b:    ExponentialBackoff{Initial: time.Second, Max: time.Minute, Multiplier: 1},
			want: []time.Duration{time.Second, time.Second, time.Second},
		},
		{
			name: "Max equal to Initial",
			b:    ExponentialBackoff{Initial: time.Second, Max: time.Second},
			want: []time.Duration{time.Second, time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for attempt, want := range tt.want {
				if got := tt.b.Delay(attempt); got != want {
					t.Errorf("Delay(%d) = %v, want %v", attempt, got, want)
				}
			}
		})
	}
}

// Attempt counts far past the point where the delay reaches Max, and
// negative ones, stay within [Initial, Max], with and without jitter.
func TestExponentialBackoffSaturates(t *testing.T) {
	const maxDur = time.Duration(math.MaxInt64)
	for _, jitter := range []float64{0, 1} {
		b := ExponentialBackoff{Initial: time.Nanosecond, Max: maxDur, Multiplier: 10, Jitter: jitter}
		for _, attempt := range []int{-5, 0, 1, 18, 19, 62, 1000, math.MaxInt} {
			got := b.Delay(attempt)
			if got < 0 || (jitter == 0 && got < time.Nanosecond) {
				t.Errorf("jitter %v: Delay(%d) = %v", jitter, attempt, got)
			}
		}
	}
	b := ExponentialBackoff{Initial: time.Nanosecond, Max: maxDur, Multiplier: 10}
	if got := b.Delay(-5); got != time.Nanosecond {
		t.Errorf("Delay(-5) = %v, want Initial", got)
	}
	if got := b.Delay(math.MaxInt); got != maxDur {
		t.Errorf("Delay(MaxInt) = %v, want Max", got)
	}
}

// Jitter takes up to its fraction off each delay, and never adds to it,
// so Max stays a ceiling.
func TestExponentialBackoffJitter(t *testing.T) {
	const maxDelay = 8 * time.Second
	for _, jitter := range []float64{0.2, 0.5, 1} {
		b := ExponentialBackoff{Initial: time.Second, Max: maxDelay, Jitter: jitter}
		for attempt := range 5 {
			ceiling := min(time.Second<<attempt, maxDelay)
			floor := time.Duration(float64(ceiling) * (1 - jitter))
			seen := make(map[time.Duration]bool)
			for range 200 {
				got := b.Delay(attempt)
				if got < floor || got > ceiling {
					t.Fatalf("jitter %v: Delay(%d) = %v, want within [%v, %v]", jitter, attempt, got, floor, ceiling)
				}
				seen[got] = true
			}
			if len(seen) < 2 {
				t.Fatalf("jitter %v: Delay(%d) returned one value 200 times", jitter, attempt)
			}
		}
	}
}

func TestBackoffValidated(t *testing.T) {
	const s = time.Second
	tests := []struct {
		name string
		b    Backoff
	}{
		{"nil", nil},
		{"nil BackoffFunc", BackoffFunc(nil)},
		{"negative ConstantBackoff", ConstantBackoff(-s)},
		{"no Initial", ExponentialBackoff{Max: s}},
		{"negative Initial", ExponentialBackoff{Initial: -s, Max: s}},
		{"Max below Initial", ExponentialBackoff{Initial: s, Max: s / 2}},
		{"Multiplier below 1", ExponentialBackoff{Initial: s, Max: 2 * s, Multiplier: 0.5}},
		{"NaN Multiplier", ExponentialBackoff{Initial: s, Max: 2 * s, Multiplier: math.NaN()}},
		{"negative Jitter", ExponentialBackoff{Initial: s, Max: 2 * s, Jitter: -0.1}},
		{"Jitter above 1", ExponentialBackoff{Initial: s, Max: 2 * s, Jitter: 1.5}},
		{"NaN Jitter", ExponentialBackoff{Initial: s, Max: 2 * s, Jitter: math.NaN()}},
		{"invalid behind a pointer", &ExponentialBackoff{Max: s}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(WithBroker("mqtt://127.0.0.1:1883"), WithReconnectBackoff(tt.b)); err == nil {
				t.Error("New accepted it as the reconnect backoff")
			}
			cli, err := New(WithBroker("mqtt://127.0.0.1:1883"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewQueuePublisher(cli, NewMemoryPublisherQueue(), WithQueueRetryBackoff(tt.b)); err == nil {
				t.Error("NewQueuePublisher accepted it as the retry backoff")
			}
		})
	}

	for _, b := range []Backoff{
		ConstantBackoff(0),
		ExponentialBackoff{Initial: s, Max: s, Multiplier: 1, Jitter: 1},
		&ExponentialBackoff{Initial: s, Max: 2 * s},
		BackoffFunc(func(int) time.Duration { return s }),
	} {
		if _, err := New(WithBroker("mqtt://127.0.0.1:1883"), WithReconnectBackoff(b)); err != nil {
			t.Errorf("New rejected %#v: %v", b, err)
		}
	}
}
