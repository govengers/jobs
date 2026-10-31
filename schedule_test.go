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

// Every day of a year has exactly one run, in zones with gaps at midnight and in the night,
// with half-hour shifts and with repeated hours.
func TestDailyAtRunsOncePerDayThroughAYear(t *testing.T) {
	for _, zone := range []string{"UTC", "Europe/Berlin", "America/Havana", "America/Santiago", "Australia/Lord_Howe"} {
		for _, wallClock := range [][2]int{{0, 0}, {0, 30}, {2, 30}, {3, 0}, {23, 30}} {
			zoneLocation := location(t, zone)
			next := DailyAt(wallClock[0], wallClock[1], zoneLocation)
			run := time.Date(2026, 1, 1, 12, 0, 0, 0, zoneLocation)
			days := map[string]bool{}
			for range 366 {
				following := next(run)
				require.True(t, following.After(run), "%s %v: %s after %s", zone, wallClock, following, run)
				require.Less(t, following.Sub(run), 49*time.Hour, "%s %v: a skipped day after %s", zone, wallClock, run)
				day := following.In(zoneLocation).Format(time.DateOnly)
				require.False(t, days[day], "%s %v: two runs on %s", zone, wallClock, day)
				days[day] = true
				run = following
			}
		}
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
