# Schedules

A schedule enqueues its job once per period across all replicas.

```go
nightlyReport := &jobs.Schedule{
    Name: "billing.nightly-report",                          // unique per database
    Next: jobs.DailyAt(3, 0, berlin),                       // or jobs.Every(15 * time.Minute)
    Job:  model.ReportKind.New(model.ReportPayload{}, nil), // enqueued at the start of every period
}
worker, err := jobs.NewWorker(&jobs.WorkerOptions{..., Schedules: []*jobs.Schedule{nightlyReport}})
```

## How it stays once per period

Every worker checks its schedules every `ScheduleInterval` (10 s). The schedule's row in `govengers_job_schedules` holds `next_run_at`. A worker locks the due row with `SKIP LOCKED`, enqueues the job and advances `next_run_at` in one transaction, so another worker either waits for that transaction or skips the row. No leader election, no cron library, no `time.Ticker` per replica (which would run the job once per replica).

- The first worker that sees a schedule registers it with `next_run_at = Next(now)`; the first run is at the next boundary, not at startup.
- `Next` must return a time after now and must give every replica the same boundaries: `jobs.Every` aligns to multiples of the interval, `jobs.DailyAt` to a wall-clock time in a location.
- Periods missed while no worker ran are enqueued once, not as a burst: the next period is computed from now.
- `DailyAt` stays once per day across daylight saving changes: a time inside a gap (02:30 on the spring day in Berlin, midnight in Havana) runs at the end of the gap, a repeated time on the autumn day runs once. A nil location is UTC.
- A `Next` that returns a time not after now fails the schedule's transaction (reported to the Observer as a failed `schedules` operation) instead of leaving the schedule due on every poll.
- Changing `Next` takes effect after the period already stored. Renaming a schedule starts a new row; the old row stays until you delete it.

The job a schedule enqueues is an ordinary job: retried, observed, idempotent like any other.
