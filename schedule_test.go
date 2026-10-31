package jobs

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func location(t *testing.T, name string) *time.Location {
	t.Helper()
	zone, err := time.LoadLocation(name)
	require.NoError(t, err)
	return zone
}

// Finding 7: across a daylight saving gap DailyAt returns a time after its input, the end of the
// gap, so the schedule is not due again on the next poll.
func TestDailyAtAcrossDaylightSavingChanges(t *testing.T) {
	havana, santiago, berlin, lordHowe := location(t, "America/Havana"), location(t, "America/Santiago"), location(t, "Europe/Berlin"), location(t, "Australia/Lord_Howe")
	tests := []struct {
		name         string
		hour, minute int
		zone         *time.Location
		now          time.Time
		want         time.Time
	}{
		{name: "Havana has no midnight on 08.03.2026", hour: 0, zone: havana, now: time.Date(2026, 3, 7, 23, 30, 0, 0, havana), want: time.Date(2026, 3, 8, 1, 0, 0, 0, havana)},
		{name: "Santiago has no midnight on 06.09.2026", hour: 0, zone: santiago, now: time.Date(2026, 9, 5, 23, 30, 0, 0, santiago), want: time.Date(2026, 9, 6, 1, 0, 0, 0, santiago)},
		{name: "Berlin spring gap", hour: 2, minute: 30, zone: berlin, now: time.Date(2026, 3, 29, 1, 0, 0, 0, berlin), want: time.Date(2026, 3, 29, 3, 0, 0, 0, berlin)},
		{name: "Lord Howe half-hour gap", hour: 2, minute: 15, zone: lordHowe, now: time.Date(2026, 10, 4, 1, 0, 0, 0, lordHowe), want: time.Date(2026, 10, 4, 2, 30, 0, 0, lordHowe)},
		{name: "Berlin autumn repeated hour", hour: 2, minute: 30, zone: berlin, now: time.Date(2026, 10, 25, 1, 0, 0, 0, berlin), want: time.Date(2026, 10, 25, 2, 30, 0, 0, berlin)},
		{name: "nil location is UTC", hour: 3, zone: nil, now: time.Date(2026, 10, 31, 4, 0, 0, 0, time.UTC), want: time.Date(2026, 11, 1, 3, 0, 0, 0, time.UTC)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next := DailyAt(test.hour, test.minute, test.zone)(test.now)

			require.True(t, next.After(test.now), "next %s is not after %s", next, test.now)
			require.True(t, test.want.Equal(next), "want %s, got %s", test.want, next)
		})
	}
}

// Round 2, finding 1: consecutive midnight runs around a midnight gap include the transition
// day (at the end of its gap); adding a day to the instant used to skip it.
func TestDailyAtMidnightAcrossAMidnightGap(t *testing.T) {
	havana, santiago := location(t, "America/Havana"), location(t, "America/Santiago")
	tests := []struct {
		name string
		zone *time.Location
		from time.Time
		want []time.Time
	}{
		{name: "Havana", zone: havana, from: time.Date(2026, 3, 5, 0, 0, 0, 0, havana), want: []time.Time{
			time.Date(2026, 3, 6, 0, 0, 0, 0, havana), time.Date(2026, 3, 7, 0, 0, 0, 0, havana),
			time.Date(2026, 3, 8, 1, 0, 0, 0, havana), time.Date(2026, 3, 9, 0, 0, 0, 0, havana),
			time.Date(2026, 3, 10, 0, 0, 0, 0, havana),
		}},
		{name: "Santiago", zone: santiago, from: time.Date(2026, 9, 4, 0, 0, 0, 0, santiago), want: []time.Time{
			time.Date(2026, 9, 5, 0, 0, 0, 0, santiago), time.Date(2026, 9, 6, 1, 0, 0, 0, santiago),
			time.Date(2026, 9, 7, 0, 0, 0, 0, santiago), time.Date(2026, 9, 8, 0, 0, 0, 0, santiago),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next := DailyAt(0, 0, test.zone)
			run := test.from
			for _, want := range test.want {
				run = next(run)
				require.True(t, want.Equal(run), "want %s, got %s", want, run)
			}
		})
	}
}

// Every calendar date of a year has exactly one run, in zones with gaps at midnight and in the
// night, with half-hour shifts and with repeated hours.
func TestDailyAtRunsOncePerDayThroughAYear(t *testing.T) {
	for _, zone := range []string{"UTC", "Europe/Berlin", "America/Havana", "America/Santiago", "Australia/Lord_Howe"} {
		for _, wallClock := range [][2]int{{0, 0}, {0, 30}, {2, 30}, {3, 0}, {23, 30}} {
			zoneLocation := location(t, zone)
			next := DailyAt(wallClock[0], wallClock[1], zoneLocation)
			first := time.Date(2026, 1, 1, 12, 0, 0, 0, zoneLocation)
			run := first
			days := map[string]int{}
			for range 366 {
				following := next(run)
				require.True(t, following.After(run), "%s %v: %s after %s", zone, wallClock, following, run)
				days[following.In(zoneLocation).Format(time.DateOnly)]++
				run = following
			}
			for date := first.AddDate(0, 0, 1); !date.After(run); date = date.AddDate(0, 0, 1) {
				day := date.Format(time.DateOnly)
				require.Equal(t, 1, days[day], "%s %v: %d runs on %s", zone, wallClock, days[day], day)
			}
		}
	}
}

// Round 2, finding 6: an impossible wall clock gives a Next that is never after now, which
// NewWorker rejects.
func TestDailyAtWithAnImpossibleWallClockIsNeverAfterNow(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	for _, wallClock := range [][2]int{{24, 0}, {-1, 0}, {3, 60}, {3, -5}} {
		require.False(t, DailyAt(wallClock[0], wallClock[1], time.UTC)(now).After(now), "%v", wallClock)
	}
}

// Finding 15: no overflow, no panic, whatever the bounds.
func TestExponentialBackoffBounds(t *testing.T) {
	tests := []struct {
		name          string
		base, maximum time.Duration
		attempt       int
		minimum       time.Duration
	}{
		{name: "no cap", base: time.Hour, maximum: math.MaxInt64, attempt: 100, minimum: time.Duration(math.MaxInt64) / 2},
		{name: "huge base", base: 1 << 62, maximum: math.MaxInt64, attempt: 2, minimum: 1 << 62},
		{name: "zero base", base: 0, maximum: time.Second, attempt: 1, minimum: time.Millisecond},
		{name: "negative base", base: -time.Second, maximum: time.Second, attempt: 3, minimum: time.Millisecond},
		{name: "maximum below base", base: time.Second, maximum: time.Millisecond, attempt: 5, minimum: time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var delay time.Duration
			require.NotPanics(t, func() { delay = ExponentialBackoff(test.base, test.maximum)(test.attempt) })
			require.GreaterOrEqual(t, delay, test.minimum)
		})
	}
}
