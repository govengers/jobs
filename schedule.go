package jobs

import "time"

// Schedule enqueues Job once per period across all replicas: the worker that claims the
// schedule's row enqueues the job and advances the row in one transaction. Periods missed
// while no worker ran are enqueued once, not as a burst.
type Schedule struct {
	Name string // unique per database; the key of the schedule's row
	// Next returns the start of the period after now. It must be after now and should not
	// depend on the replica, so every replica agrees on the periods.
	Next func(now time.Time) time.Time
	Job  *NewJob // enqueued at the start of every period
}

// Every returns a Next for periods of interval, aligned to multiples of interval since the
// zero time, so every replica computes the same boundaries.
func Every(interval time.Duration) func(now time.Time) time.Time {
	return func(now time.Time) time.Time {
		return now.Truncate(interval).Add(interval)
	}
}

// DailyAt returns a Next for once a day at hour:minute in location.
func DailyAt(hour, minute int, location *time.Location) func(now time.Time) time.Time {
	return func(now time.Time) time.Time {
		local := now.In(location)
		next := time.Date(local.Year(), local.Month(), local.Day(), hour, minute, 0, 0, location)
		if !next.After(local) {
			next = time.Date(local.Year(), local.Month(), local.Day()+1, hour, minute, 0, 0, location)
		}
		return next
	}
}
