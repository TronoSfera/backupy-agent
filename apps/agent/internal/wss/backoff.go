package wss

import (
	"math/rand"
	"time"
)

// Backoff implements the exponential schedule specified in
// docs/03-agent-spec.md → "Reconnect: exponential backoff 1s → 2s → 4s …
// → 60s cap, jitter ±20%".
//
// The struct is safe for use from a single goroutine. The reconnect loop
// is single-threaded, so no mutex is needed.
type Backoff struct {
	Initial  time.Duration
	Max      time.Duration
	Factor   float64
	JitterPC float64 // 0.2 → ±20%

	current time.Duration
	rng     *rand.Rand
}

// NewBackoff returns a Backoff with spec defaults (1s → 60s, ±20%, ×2).
func NewBackoff() *Backoff {
	return &Backoff{
		Initial:  1 * time.Second,
		Max:      60 * time.Second,
		Factor:   2.0,
		JitterPC: 0.2,
		// Deterministic-ish but unique per agent: seed from the wall clock.
		rng: rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // not a security context
	}
}

// Next returns the next delay and advances the schedule.
func (b *Backoff) Next() time.Duration {
	if b.current == 0 {
		b.current = b.Initial
	} else {
		b.current = time.Duration(float64(b.current) * b.Factor)
		if b.current > b.Max {
			b.current = b.Max
		}
	}
	return b.withJitter(b.current)
}

// Reset rewinds the schedule. Call on a clean reconnect.
func (b *Backoff) Reset() {
	b.current = 0
}

func (b *Backoff) withJitter(d time.Duration) time.Duration {
	if b.JitterPC <= 0 {
		return d
	}
	// pick uniformly in [-jitter, +jitter]
	jitter := b.JitterPC * (b.rng.Float64()*2 - 1)
	return time.Duration(float64(d) * (1 + jitter))
}
