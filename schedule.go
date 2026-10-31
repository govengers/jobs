package jobs

import "time"

// Schedule enqueues Job once per period across all replicas: the worker that claims the
// schedule's row enqueues the job and advances the row in one transaction. Periods missed
// while no worker ran are enqueued once, not as a burst. The job is enqueued on a context
// without the values of Run's context, so the propagators carry no identity or trace into it.
// NewWorker encodes Job's payload once; changing the payload value afterwards changes nothing.
type Schedule struct {
	Name string // unique per database; the key of the schedule's row
	// Next returns the start of the period after now. It must be after now and should not
	// depend on the replica, so every replica agrees on the periods. A Next that returns a time
	// not after now fails the schedule's transaction instead of enqueueing again and again.
	Next func(now time.Time) time.Time
	Job  *PreparedJob // enqueued at the start of every period
}

// daysToSearch bounds DailyAt's search; the next occurrence is at most two calendar days away.
const daysToSearch = 3

// Every returns a Next for periods of interval, aligned to multiples of interval since the
// zero time, so every replica computes the same boundaries.
func Every(interval time.Duration) func(now time.Time) time.Time {
	return func(now time.Time) time.Time {
		return now.Truncate(interval).Add(interval)
	}
}

// DailyAt returns a Next for once a day at hour:minute in location; a nil location is UTC.
// Across daylight saving changes it stays once per day: on a day where hour:minute does not
// exist (a gap) it runs at the end of the gap, on a day where it occurs twice it runs once. An
// hour outside 0..23 or a minute outside 0..59 gives a Next that NewWorker rejects.
func DailyAt(hour, minute int, location *time.Location) func(now time.Time) time.Time {
	if location == nil {
		location = time.UTC
	}
	validWallClock := hour >= 0 && hour < 24 && minute >= 0 && minute < 60
	return func(now time.Time) time.Time {
		if !validWallClock {
			return now // not after now: NewWorker and the store refuse it
		}
		local := now.In(location)
		for offset := range daysToSearch {
			// The calendar date moves on its own: adding a day to the instant would keep its
			// clock time, which a daylight saving change shifts onto the previous date.
			date := time.Date(local.Year(), local.Month(), local.Day()+offset, 0, 0, 0, 0, time.UTC)
			candidate := wallClock(date, hour, minute, location)
			if candidate.After(now) {
				return candidate
			}
		}
		return now // unreachable for a valid zone; refused like an invalid wall clock
	}
}

// wallClock is hour:minute on the calendar date of date (its year, month and day) in
// location. A time inside a daylight saving gap does not exist; time.Date then picks an
// instant just before or after the gap, and the first instant after the gap is the bound of
// that instant's zone period that lies closest to it.
func wallClock(date time.Time, hour, minute int, location *time.Location) time.Time {
	candidate := time.Date(date.Year(), date.Month(), date.Day(), hour, minute, 0, 0, location)
	if candidate.Hour() == hour && candidate.Minute() == minute {
		return candidate
	}
	start, end := candidate.ZoneBounds()
	if end.Sub(candidate) < candidate.Sub(start) {
		return end
	}
	return start
}
