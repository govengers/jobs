# Worker

```go
worker, err := jobs.NewWorker(&jobs.WorkerOptions{
    Store:  jobStore,
    Queues: map[string]jobs.QueueOptions{
        "mail":       {Concurrency: 4},
        "generation": {Concurrency: 2, Lease: time.Minute},
    },
    Middleware:  []jobs.Middleware{wideEvents},          // first is outermost
    Propagators: []jobs.Propagator{authadapter.Identity()},
    Observer:    jobMetrics,                            // prometheusadapter.New
    Schedules:   []*jobs.Schedule{nightlyReport},
})
err = jobs.Handle(worker, model.WelcomeMailKind, mailJobController.SendWelcomeMail)
group.Go(recovering(func() error { return worker.Run(ctx) })) // CON-001
```

Register every handler before `Run`. A `Worker` runs once.

## Queue options

| Option | Default | Meaning |
|--------|---------|---------|
| `Concurrency` | 1 | jobs of this queue running at once in this process |
| `Lease` | 30 s | how long a claim holds a job without a heartbeat (minimum 30 ms) |
| `PollInterval` | 1 s | how long an idle queue waits before it looks again; a full batch polls again at once |

The worker claims only as many jobs as it has free slots, so no claimed job waits in memory while its lease runs out.

## Outcomes

| Handler returns | Job |
|-----------------|-----|
| `nil` | completed |
| `jobs.Permanent(err)` | dead at once |
| `jobs.Snooze(delay)` | pending after `delay`; the attempt does not count |
| other error, panic | pending after the kind's backoff; dead once the attempts are used up |

A handler is a job controller: it translates the job and calls a service method, the same method an HTTP controller calls (ARC-001, ARC-003). Map domain errors that no retry fixes (not found, invalid) to `jobs.Permanent` there; `jobs.Permanent(nil)` is nil, so a handler returning it completes.

A bug on the handler's side is a failed attempt, never an error of `Run`: a panicking handler, a panicking `Backoff` (the attempt is retried with `DefaultBackoff`) or a panicking Observer (its report is dropped). A panic of the worker's own goroutines or of the `Store` ends `Run` with the error.

## Leases, heartbeat and fencing

A claim sets a fresh lease token and `lease_until`. While the handler runs, the worker extends the leases of its running jobs every Lease/3, so a job may take ten seconds or ten minutes on the same queue. When the extension finds a job no longer leased, the handler's context is cancelled with cause `jobs.ErrLeaseLost` and its result is discarded. Every write after the claim matches the token, so a worker that lost its lease cannot overwrite a newer attempt. When the extensions fail (a database outage, a network partition) the worker cannot know whether another replica already rescued the job, so an attempt whose lease could not be confirmed for longer than the lease is cancelled with `jobs.ErrLeaseLost` as well. `context.Cause(ctx)` tells a handler why its context ended: `jobs.ErrLeaseLost`, `jobs.ErrCancelled`, or the end of the drain.

## Shutdown

When `Run`'s context is cancelled the worker stops claiming; no claim starts after the cancel. Running handlers keep a context of their own for up to `DrainTimeout` (default 20 s; it runs in parallel to the HTTP drain inside the pod's grace period), and the heartbeat keeps their leases. Then their contexts are cancelled with the drain's cause.

The job is released when its handler returns: an error after the cancel releases it (pending, runnable now, attempt given back), so another replica takes it at once; `nil` completes it; a `jobs.Snooze` takes precedence and returns it to pending after the snooze's delay. A handler that ignores its context keeps the job, and its lease, until it returns or the process is killed; then the rescuer returns the job once the lease expires. `Run` returns after the last handler returned. Pass `ctx` to every call a handler makes.

## Rescuer and retention

Every worker process returns jobs whose lease expired (a crash, an OOM kill, a lost node) to pending every `RescueInterval` (30 s), or to dead when their attempts are used up. The lost attempt counts, so a job that kills its worker every time ends dead. Every worker deletes completed and cancelled jobs after `RetainCompleted` (7 d) and dead jobs after `RetainDead` (30 d), checked every `SweepInterval` (1 h). The table's row locks keep workers from doing the same work twice; there is no leader.

## Unknown kinds

A claimed job without a handler fails with "no handler for kind": it is retried with the default backoff and dies at its last attempt. That covers a producer rolled out before its worker; a kind nobody handles any more ends dead, visible in the admin list.

## Split deployments

Heavy queues can run on their own pods without a second artifact (P10): one image, two Deployments, the configuration decides the consumed queues.

```go
// JOB_QUEUES=generation on the GPU Deployment, JOB_QUEUES=mail on the API Deployment
queues := map[string]jobs.QueueOptions{}
for _, name := range strings.Split(configuration.JobQueues, ",") {
    queues[name] = queueOptions[name]
}
```

All pods use the service's database. Handlers may be registered everywhere; a worker only claims the queues it consumes.

## Running jobs without Run

`worker.RunDue(ctx)` claims and runs every runnable job synchronously until none is left and returns the number of attempts. It is for tests ([Testing](Testing.md)) and one-off commands. It fails while `Run` runs, when a claim or a finish write fails, and when one job ran `jobs.MaximumRunsPerJob` (100) times in one call, which is what a handler that snoozes without a delay does.
