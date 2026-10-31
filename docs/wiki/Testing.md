# Testing

## Service tests

Build the worker with the production handlers on the test's database, call the code that enqueues, then drain:

```go
userService.SignUp(t.Context(), input)      // enqueues inside its transaction
attempts := jobstest.Drain(t, worker)        // runs every runnable job synchronously
require.Equal(t, 1, attempts)
require.Len(t, fakeMailer.Sent(), 1)
```

`Drain` calls `worker.RunDue`: it claims and runs batches until nothing runnable is left, with the real middleware and propagators, and fails the test on a store error. A retry waits for its backoff, so a failing job runs once per drain; give a kind `Backoff: func(int) time.Duration { return 0 }` in a test to run all its attempts. A failed finish write fails the test, and so does a job that runs 100 times in one drain (a handler that snoozes without a delay). Do not call it while `Run` runs.

The database is the test's own (TST-002, TST-013): an embedded PostgreSQL 18 per test binary and a fresh schema per test, migrated with your migrations including `postgresadapter.MigrationV1`. [examples/signup](../../examples/signup) shows the whole setup over HTTP.

Test handlers like controllers: assert the effect of the service call and the job's status (`jobStore.Get`), including the failure path (`StatusPending` with `Attempt` 1 after a transient error, `StatusDead` for a permanent one).

## What this repository tests

- Unit tests run against embedded PostgreSQL 18: concurrent claims never share a job, a stale finish is fenced, heartbeat loss and cancellation reach the handler, drain and release give the attempt back, schedules enqueue once per period, retention, admin operations.
- The end-to-end suite (`just test-e2e`, build tag `e2e`) starts two real worker processes on one database: a `kill -9` mid-job is recovered by the other process's rescuer, SIGTERM drains and releases, a schedule is enqueued once per period across both processes.
- `just test` reruns only changed packages from Go's test cache (TST-014); `just test-e2e` skips when the worker binary, the suite and their inputs equal a green run (TST-015). `just test-all` and `just test-e2e-all` never skip; CI runs them nightly.
