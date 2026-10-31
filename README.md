# jobs

Durable PostgreSQL work queue for govengers services. Module `github.com/govengers/jobs`, Go 1.26, PostgreSQL 18, Apache 2.0.

- **Enqueue inside your transaction.** The job commits or rolls back with the change that caused it: no lost mail after a commit, no mail for a rolled-back sign-up.
- **Many replicas, one table.** Workers claim with `FOR UPDATE SKIP LOCKED` under a lease, extend it with a heartbeat, and fence every write with the lease token, so a worker that lost its lease cannot overwrite a newer attempt.
- **Failures are handled.** Retries back off exponentially, `jobs.Permanent` ends a job at once, `jobs.Snooze` waits without spending an attempt, a crashed worker's jobs come back through the rescuer, SIGTERM drains and hands unfinished jobs to another replica.
- **Recurring jobs once per period,** across every replica, without a leader or a cron library.
- **Plugs into the govengers stack:** wide events (wide-event-logger), the acting identity (auth), Prometheus metrics, livewire's transaction.

Delivery is at least once: handlers must be idempotent.

```sh
go get github.com/govengers/jobs
```

## Usage

```go
// The contract, shared by the code that enqueues and the handler.
type RenderPayload struct {
    RenderID uuid.UUID `json:"renderId"`
}

var RenderKind = jobs.NewKind[RenderPayload]("generation.render", &jobs.KindOptions{Queue: "generation", MaximumAttempts: 5})

// main: one store, one worker per process.
jobStore, err := postgresadapter.New(&postgresadapter.Options{Pool: pool})
worker, err := jobs.NewWorker(&jobs.WorkerOptions{
    Store:  jobStore,
    Queues: map[string]jobs.QueueOptions{"generation": {Concurrency: 3}},
})
err = jobs.Handle(worker, RenderKind, renderJobController.Render)
group.Go(func() error { return worker.Run(ctx) }) // drains on cancel

// repository: in the transaction of the domain change.
_, err = jobStore.Enqueue(ctx, transaction, RenderKind.New(RenderPayload{RenderID: render.ID}, nil))

// job controller: translate the job, call the service (ARC-001).
func (c *RenderJobController) Render(ctx context.Context, job *jobs.Job[RenderPayload]) error {
    return c.renderService.Render(ctx, job.Payload.RenderID)
}
```

A handler returns:

| Return | Effect |
|--------|--------|
| `nil` | completed |
| `jobs.Permanent(err)` | dead at once; a retry cannot fix it |
| `jobs.Snooze(delay)` | pending again after `delay`, the attempt does not count |
| any other error, a panic | retried after the kind's backoff, dead after `MaximumAttempts` |

The tables come from `postgresadapter.MigrationV1`, which you run from one of your goose migrations. Nothing runs DDL at startup.

## Packages

| Import `github.com/govengers/jobs/...` | Holds | Depends on |
|---|---|---|
| (root) `jobs` | `Kind`, `Worker`, `Handle`, outcomes, `Schedule`, `Store`, `Observer`, `Propagator` | uuid, x/sync |
| `postgresadapter` | the `Store`: `Enqueue`, admin `Get`/`List`/`Cancel`/`Retry`, `QueueDepths`, `MigrationV1` | pgx/v5 |
| `wideeventadapter` | one wide event per attempt (LOG-001) | wide-event-logger |
| `authadapter` | carries the `auth.Identity` from the enqueuing request to the handler | govengers/auth |
| `prometheusadapter` | attempt and store metrics, scrape-time queue depth | client_golang |
| `jobstest` | `Drain`: run every due job synchronously in a test | |

## A complete service

[examples/signup](examples/signup) is a small, tested service: `POST /api/users` creates a user and enqueues the welcome mail in the same transaction, a job controller sends it through the service, and the tests drive it over HTTP and drain the worker on embedded-postgres.

## Guarantees and limits

- At least once. A handler may run again after a lost lease or a crash; make its effects idempotent (an idempotency key for external calls, check-then-act in a transaction for internal ones).
- No ordering across jobs. Workers on several replicas run jobs concurrently; carry a version or timestamp in what you write if order matters.
- Jobs stay inside one service and its database. Another service is reached through its API, called from a job with an idempotency key.
- The queue polls (default every second per queue). It is built for business work, not for sub-second dispatch or thousands of jobs per second.

## Documentation

- [docs/wiki](docs/wiki/Home.md): kinds and enqueue, the worker, schedules, the store, observability, testing, migration from a hand-rolled queue, gotchas.
- [docs/design.md](docs/design.md): the binding design with every decision (J1 onwards).
- [pkg.go.dev](https://pkg.go.dev/github.com/govengers/jobs): the API.

The repository follows [govengers/standards](https://github.com/govengers/standards), profile `library`. `just check` runs lint, unit tests, the end-to-end suite (two worker processes on one database) and govulncheck.

## License

Apache 2.0, see [LICENSE](LICENSE).
