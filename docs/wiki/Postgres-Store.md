# Postgres store

`postgresadapter.Store` implements `jobs.Store` on PostgreSQL 18 with pgx/v5. Another store can implement `jobs.Store` too; the interface may gain methods in minor versions, so an implementation outside this module should expect that.

```go
jobStore, err := postgresadapter.New(&postgresadapter.Options{
    Pool:        pool,                                       // its search_path reaches the tables
    Propagators: []jobs.Propagator{authadapter.Identity()},  // inject at enqueue
})
```

## Schema

`postgresadapter.MigrationV1` creates `govengers_jobs` and `govengers_job_schedules` with their indexes. Run it as one of your goose migrations: copy its SQL into `internal/database/migrations/00042_jobs.sql` under `-- +goose Up`, or execute the constant from a Go migration you register with goose's provider (no `init()`, DES-002). The tests of this module apply the same constant, so the copied SQL is the tested schema.

`postgresadapter.MigrationV2` adds the columns of [progress and results](Progress-and-Results.md). Run it as the next goose migration, then deploy the version of the module that needs it: it adds nullable columns with `NOT VALID` checks, so it rewrites nothing, and replicas of the previous version keep working on the new schema during the rollout. A livewire application adds `postgresadapter.LivewireTriggerSQL` after `MigrationV2` ([Progress-and-Results](Progress-and-Results.md#live-in-a-livewire-application)). Its trigger function has semicolons inside `$$ ... $$`, so a copy into a SQL migration file goes between `-- +goose StatementBegin` and `-- +goose StatementEnd` (goose splits at every semicolon otherwise); a Go migration executes the constant as it is.

The SQL sets `SET LOCAL` lock and statement timeouts and passes squawk. Later versions of the module add `MigrationV3` and so on; never edit an applied migration. The tables carry the `govengers_` prefix, so they do not collide with an application table named `jobs`.

| Column | Meaning |
|--------|---------|
| `status` | `pending`, `running`, `completed`, `dead`, `cancelled` |
| `run_at` | not claimed before; retries and snoozes move it |
| `attempt`, `maximum_attempts` | attempts started, the limit |
| `lease_token`, `lease_until`, `worker` | set while running; every write after the claim matches the token |
| `unique_key` | coalescing key while the job has not started |
| `headers` | values of the propagators |
| `last_error`, `error_class` | the latest failure and its class (`transient`, `permanent`, `lease_expired`) |
| `progress`, `progress_message` | the running or last attempt's progress report (0 to 1); a claim clears it |
| `result` | what the handler set with `jobs.SetResult`, kept when the job completed |
| `started_at`, `finished_at` | when the last attempt started; when the job ended |

## Admin operations

| Method | Does |
|--------|------|
| `Get(ctx, id)` | one job as `Record`, or `ErrNotFound`; `Record.View()` is its [view](Progress-and-Results.md#the-view) |
| `List(ctx, &ListFilter{Queue, Status, Before, Limit})` | newest first, keyset pages, at most 1000 |
| `Cancel(ctx, id)` | pending: cancelled at once; running: cancelled at its next heartbeat; `ErrFinished` otherwise |
| `Retry(ctx, id)` | a dead job back to pending, its attempts, last error, progress and any earlier cancellation request reset; `ErrNotDead` otherwise |
| `CancelOn(ctx, executor, id)`, `RetryOn(ctx, executor, id)` | the same on the caller's transaction (any `Executor`), so the change commits with the caller's |
| `ViewsOn(ctx, querier, ids)` | the views of the given jobs on the caller's transaction, for a list the caller pages itself |
| `QueueDepths(ctx)` | pending, scheduled, running and oldest pending age per queue, for metrics |

Build the admin view of your service on these, or declare `livewireadapter`'s live resource; the library ships no UI. Guard them with your authorization like any other mutation.

## Errors

Driver errors are wrapped as text (LIB-010); match only the sentinels: `ErrNotFound`, `ErrFinished`, `ErrNotDead`, `ErrSerializationFailure` and `jobs.ErrInvalidKind`; `jobs.ErrNoAttempt`, `jobs.ErrResultTooLarge` and `jobs.ErrResultNotStorable` come from `jobs.SetResult`.
