# Postgres store

`postgresadapter.Store` implements `jobs.Store` on PostgreSQL 18 with pgx/v5.

```go
jobStore, err := postgresadapter.NewStore(&postgresadapter.StoreOptions{
    Pool:        pool,                                       // its search_path reaches the tables
    Propagators: []jobs.Propagator{authadapter.Identity()},  // inject at enqueue
})
```

## Schema

`postgresadapter.MigrationV1` creates `govengers_jobs` and `govengers_job_schedules` with their indexes. Run it as one of your goose migrations: copy its SQL into `internal/database/migrations/00042_jobs.sql` under `-- +goose Up`, or execute the constant from a Go migration you register with goose's provider (no `init()`, DES-002). The tests of this module apply the same constant, so the copied SQL is the tested schema.

The SQL sets `SET LOCAL` lock and statement timeouts and passes squawk. Later versions of the module add `MigrationV2` and so on; never edit an applied migration. The tables carry the `govengers_` prefix, so they do not collide with an application table named `jobs`.

| Column | Meaning |
|--------|---------|
| `status` | `pending`, `running`, `completed`, `dead`, `cancelled` |
| `run_at` | not claimed before; retries and snoozes move it |
| `attempt`, `max_attempts` | attempts started, the limit |
| `lease_token`, `lease_until`, `worker` | set while running; every write after the claim matches the token |
| `unique_key` | coalescing key while the job has not started |
| `headers` | values of the propagators |
| `last_error` | the latest failure |

## Admin operations

| Method | Does |
|--------|------|
| `Get(ctx, id)` | one job as `Record`, or `ErrNotFound` |
| `List(ctx, &ListFilter{Queue, Status, Before, Limit})` | newest first, keyset pages, at most 1000 |
| `Cancel(ctx, id)` | pending: cancelled at once; running: cancelled at its next heartbeat; `ErrFinished` otherwise |
| `Retry(ctx, id)` | a dead job back to pending with its attempts reset; `ErrNotDead` otherwise |
| `QueueDepths(ctx)` | pending, scheduled, running and oldest pending age per queue, for metrics |

Build the admin view of your service on these; the library ships no UI. Guard them with your authorization like any other mutation.

## Errors

Driver errors are wrapped as text (LIB-010); match only the sentinels: `ErrNotFound`, `ErrFinished`, `ErrNotDead` and `jobs.ErrInvalidKind`.
