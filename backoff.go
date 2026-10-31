package jobs

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before the next attempt after attempt failed (1 for the first).
// A negative delay runs the job again at once.
type Backoff func(attempt int) time.Duration

// The bounds of DefaultBackoff, and the smallest base ExponentialBackoff accepts.
const (
	defaultBackoffBase    = time.Second
	defaultBackoffMaximum = time.Hour
	minimumBackoffBase    = time.Millisecond
)

// DefaultBackoff doubles from one second per attempt up to one hour, plus up to 20 % jitter.
func DefaultBackoff(attempt int) time.Duration {
	return ExponentialBackoff(defaultBackoffBase, defaultBackoffMaximum)(attempt)
}

// ExponentialBackoff returns a Backoff that starts at base, doubles per attempt up to maximum
// and adds up to 20 % jitter, so jobs that failed together do not retry together. A base below
// one millisecond is raised to one millisecond and a maximum below base is raised to base, so
// every pair of durations gives a positive delay; the delay saturates instead of overflowing.
func ExponentialBackoff(base, maximum time.Duration) Backoff {
	base = max(base, minimumBackoffBase)
	maximum = max(maximum, base)
	return func(attempt int) time.Duration {
		delay := base
		for step := 1; step < attempt && delay < maximum; step++ {
			if delay > maximum/2 {
				delay = maximum
				break
			}
			delay *= 2
		}
		jitter := rand.N(delay/5 + 1) //nolint:gosec // G404: jitter does not need a cryptographic RNG
		if delay > math.MaxInt64-jitter {
			return math.MaxInt64
		}
		return delay + jitter
	}
}
