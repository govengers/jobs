# jobs

Durable PostgreSQL work queue for govengers services. Jobs are enqueued inside the caller's transaction, claimed under a lease by workers on any replica, retried with backoff and fenced, so a worker that lost its lease cannot overwrite a newer attempt.

**v1 is in development.** The design is in [docs/design.md](docs/design.md).

```sh
go get github.com/govengers/jobs
```

## Usage

```go
// The contract, shared by the code that enqueues and the handler.
var RenderKind = jobs.NewKind[RenderPayload]("generation.render", &jobs.KindOptions{Queue: "generation", MaxAttempts: 5})

// main: the store enqueues and backs the worker.
jobStore, err := postgresadapter.NewStore(&postgresadapter.StoreOptions{Pool: pool})
worker, err := jobs.NewWorker(&jobs.WorkerOptions{
    Store:  jobStore,
    Queues: map[string]jobs.QueueOptions{"generation": {Concurrency: 3, Lease: 30 * time.Second}},
})
err = jobs.Handle(worker, RenderKind, renderJobController.Render)
group.Go(func() error { return worker.Run(ctx) }) // drains on cancel

// repository: the job commits or rolls back with the domain change.
_, err = jobStore.Enqueue(ctx, transaction, RenderKind.New(RenderPayload{RenderID: id}, nil))
```

A handler returns `nil` (completed), `jobs.Permanent(err)` (dead at once), `jobs.Snooze(delay)` (later, attempt not counted) or any other error (retry with backoff, dead after the last attempt). Handlers must be idempotent: delivery is at least once.

The tables come from `postgresadapter.MigrationV1`, run by the application's goose migrations; PostgreSQL 18.

## License

Apache 2.0, see [LICENSE](LICENSE).
