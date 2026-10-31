# govengers/jobs

Durable PostgreSQL work queue for govengers services. Module `github.com/govengers/jobs`, Go 1.26, PostgreSQL 18, Apache 2.0.

## Model

A **kind** (`jobs.Kind[P]`) is the typed contract of one kind of job: its name, its queue, its payload type, its attempts and backoff. The code that enqueues and the handler share it. A **job** is one row in `govengers_jobs`, enqueued by `postgresadapter.Store.Enqueue` on the caller's transaction. A **queue** is the unit of concurrency, lease and deployment: a worker consumes the queues it is configured with. A **worker** (`jobs.Worker`) claims runnable jobs of its queues under a lease, runs their handlers, writes the outcome and keeps the table healthy (rescuer, retention, schedules). Any number of workers, in one process or on many replicas, share one table.

Constructors are `New...(options *Options) (*X, error)` and validate at startup. The library never logs: the worker reports through an `Observer`, attempts are wrapped by `Middleware`, request values travel through `Propagator`s.

## Packages

| Import `github.com/govengers/jobs/...` | Use it for | Page |
|---|---|---|
| (root) `jobs` | kinds, the worker, handler outcomes, schedules | [Kinds-and-Enqueue](Kinds-and-Enqueue.md), [Worker](Worker.md), [Schedules](Schedules.md) |
| `postgresadapter` | the store: enqueue, admin operations, depth, `MigrationV1` | [Postgres-Store](Postgres-Store.md) |
| `wideeventadapter`, `prometheusadapter`, `authadapter` | wide events, metrics, the acting identity | [Observability](Observability.md) |
| `jobstest` | running jobs synchronously in tests | [Testing](Testing.md) |

## Which piece for which need

- Work that must survive a restart (mail, renders, syncs, webhooks out): a kind, enqueued in the transaction of the change. Tested: [examples/signup](../../examples/signup).
- Work that must run once per period (nightly report, cleanup): a `jobs.Schedule` on the worker. [Schedules](Schedules.md)
- Heavy jobs on their own pods: the same image, a second Deployment whose `JOB_QUEUES` names only the heavy queue. [Worker](Worker.md#split-deployments)
- A job that waits for something not ready yet: return `jobs.Snooze(delay)`.
- Telling another service: not a job. Call its API from a job with an idempotency key; events between services are `govengers/events` (designed in docs/design.md, built on this module).

## Wiring order in main

1. `pgxpool.New`; run the goose migrations, one of which executes `postgresadapter.MigrationV1`.
2. `postgresadapter.New` with the pool and the propagators (`authadapter.Identity()`).
3. Repositories get the store and enqueue on their transactions.
4. `jobs.NewWorker` with the store, the consumed queues, `wideeventadapter.NewMiddleware`, the same propagators and `prometheusadapter.New` as Observer.
5. `jobs.Handle` per kind, with the job controllers' methods.
6. `group.Go(recovering(func() error { return worker.Run(ctx) }))` next to the HTTP server (CON-001); cancel on SIGTERM, wait for the group.

## Pages

- [Kinds-and-Enqueue](Kinds-and-Enqueue.md): kinds, payloads, transactions, unique keys, run times.
- [Worker](Worker.md): queues, outcomes, leases, fencing, shutdown, rescuer, retention, split deployments.
- [Schedules](Schedules.md): recurring jobs.
- [Postgres-Store](Postgres-Store.md): schema, migration, admin operations.
- [Observability](Observability.md): wide events, metrics, identity.
- [Testing](Testing.md): `jobstest.Drain`, embedded Postgres, the end-to-end suite.
- [Migration](Migration.md): from a hand-rolled Postgres queue.
- [Gotchas](Gotchas.md): the rules that are easy to get wrong.

Reference: [README](../../README.md) (overview), [design](../design.md) (decisions), [pkg.go.dev](https://pkg.go.dev/github.com/govengers/jobs) (API).
