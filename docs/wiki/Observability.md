# Observability

The library never logs (LIB-003). Three adapters connect it to the govengers stack.

## Wide events

```go
wideEvents, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: emitter})
worker, err := jobs.NewWorker(&jobs.WorkerOptions{..., Middleware: []jobs.Middleware{wideEvents}})
```

Every attempt emits one event named `job.<queue>` (LOG-001) with `job.id`, `job.queue`, `job.kind`, `job.attempt`, `job.maximum_attempts`, the outcome and the error. The event is in the handler's context: services and repositories add to it with `wideevent.FromContext(ctx)` exactly as in a request. A snooze is a success with `job.snoozed`; a permanent failure sets `job.permanent`; a cancelled context records `job.cancel_cause`. A handler panic is recorded and returned as the attempt's error. Put the middleware first, so its event covers the other middleware.

## Metrics

```go
jobMetrics, err := prometheusadapter.New(&prometheusadapter.Options{
    Registerer:  registry,          // main's own registry (MET-002)
    Namespace:   "billing",         // the service name
    DepthSource: jobStore,          // optional: queue depth at scrape time
    Queues:      []string{"mail"},  // reported with zeros when empty
})
worker, err := jobs.NewWorker(&jobs.WorkerOptions{..., Observer: jobMetrics})
```

| Metric | Labels |
|--------|--------|
| `<ns>_jobs_attempts_total` | queue, kind, result (`completed`, `retried`, `snoozed`, `dead`, `cancelled`, `released`, `lease_lost`, `fenced`, `finish_failed`) |
| `<ns>_jobs_attempt_duration_seconds` | queue, kind |
| `<ns>_jobs_operations_total` | operation (`claim`, `extend_leases`, `finish`, `rescue`, `sweep`, `schedules`), outcome |
| `<ns>_jobs_operation_duration_seconds` | operation |
| `<ns>_jobs_pending`, `_scheduled`, `_running`, `_oldest_pending_age_seconds` | queue |
| `<ns>_jobs_depth_errors_total` | |

The result is what the store wrote, not what the handler asked for: a cancel request that arrived after the last heartbeat turns a retry into `cancelled`, a lease that expired first gives `fenced`, a failed write `finish_failed` (the job stays running until the rescuer returns it). `jobs.Outcome.Err` joins the handler's error with the write's error.

The depth gauges are read from the table at scrape time. They describe the shared table, not the scraped pod, so every replica reports the same value: **aggregate them with `max`, never `sum`**.

```promql
max by (queue) (billing_jobs_oldest_pending_age_seconds) > 300          # a queue is stuck
sum by (queue) (rate(billing_jobs_attempts_total{result="dead"}[15m])) > 0  # jobs are dying
```

A failed depth query skips the gauges and counts `depth_errors_total`; the rest of the scrape still works.

## Identity

`authadapter.Identity()` stores the JSON snapshot of `auth.IdentityFrom(ctx)` in the job's `auth.identity` header and restores it with `auth.WithIdentity` before the handler runs. A job a user started runs and audits as that user. The snapshot is taken at enqueue time: a role revoked afterwards still applies to the job, so a handler that must honor revocations checks the user again. Configure it on the store and on the worker.

## Your own Observer

`jobs.Observer` has two methods, `AttemptFinished(*jobs.Outcome)` and `OperationFinished(*jobs.Operation)`, called from several goroutines. Implement it to feed another metrics system, or wrap `prometheusadapter.Metrics` to add your own reactions.
