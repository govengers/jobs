package jobs

import "time"

// Schedule enqueues Job once per period across all replicas: the worker that claims the
// schedule's row enqueues the job and advances the row in one transaction. Periods missed
// while no worker ran are enqueued once, not as a burst.
type Schedule struct {
	Name string // unique per database; the key of the schedule's row
	// Next returns the start of the period after now. It must be after now and should not
	// depend on the replica, so every replica agrees on the periods. A Next that returns a time
	// not after now fails the schedule's transaction instead of enqueueing again and again.
	Next func(now time.Time) time.Time
	Job  *NewJob // enqueued at the start of every period
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
// exist (a gap) it runs at the end of the gap, on a day where it occurs twice it runs once.
func DailyAt(hour, minute int, location *time.Location) func(now time.Time) time.Time {
	if location == nil {
		location = time.UTC
	}
	return func(now time.Time) time.Time {
		local := now.In(location)
		for day := range daysToSearch {
			candidate := wallClock(local.AddDate(0, 0, day), hour, minute)
			if candidate.After(now) {
				return candidate
			}
		}
		return now.Add(24 * time.Hour) // unreachable for a valid zone, keeps the contract "after now"
	}
}

// wallClock is hour:minute on the calendar day of day, in day's location. A time inside a
// daylight saving gap does not exist; time.Date then picks an instant just before or after the
// gap, and the first instant after the gap is the bound of that instant's zone period that
// lies closest to it.
func wallClock(day time.Time, hour, minute int) time.Time {
	candidate := time.Date(day.Year(), day.Month(), day.Day(), hour, minute, 0, 0, day.Location())
	if candidate.Hour() == hour && candidate.Minute() == minute {
		return candidate
	}
	start, end := candidate.ZoneBounds()
	if end.Sub(candidate) < candidate.Sub(start) {
		return end
	}
	return start
}
