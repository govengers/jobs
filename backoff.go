package jobs

import (
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before the next attempt after attempt failed (1 for the first).
type Backoff func(attempt int) time.Duration

// The bounds of DefaultBackoff.
const (
	defaultBackoffBase    = time.Second
	defaultBackoffMaximum = time.Hour
)

// DefaultBackoff doubles from one second per attempt up to one hour, plus up to 20 % jitter.
func DefaultBackoff(attempt int) time.Duration {
	return ExponentialBackoff(defaultBackoffBase, defaultBackoffMaximum)(attempt)
}

// ExponentialBackoff returns a Backoff that starts at base, doubles per attempt up to maximum
// and adds up to 20 % jitter, so jobs that failed together do not retry together.
func ExponentialBackoff(base, maximum time.Duration) Backoff {
	return func(attempt int) time.Duration {
		delay := base
		for step := 1; step < attempt && delay < maximum; step++ {
			delay *= 2
		}
		delay = min(delay, maximum)
		return delay + rand.N(delay/5+1) //nolint:gosec // G404: jitter does not need a cryptographic RNG
	}
}
