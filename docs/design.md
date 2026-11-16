# govengers/jobs and govengers/events: design

Status: agreed 31.10.2026 (open questions 1-6 accepted as proposed). Decisions are numbered (`J` jobs, `E` events); a later change gets a new number, never a rewrite.

`govengers/jobs` is the durable work queue of a govengers service: Postgres-backed, multi-replica, at-least-once, enqueued inside the caller's transaction. `govengers/events` adds publish/subscribe between services on top of it. Both are profile `library`, Apache 2.0, Go 1.26, Postgres 18 (`uuidv7()`); govengers/standards apply. They replace the hand-rolled queues in bathroom-planner, dots-ocr, reality-studio and the Hub, and the pattern currently described in JOB-001..006.

## 1. The model

**Jobs stay inside a service, events cross services, and an event arrives as a job.**

|  | Job | Event |
|--|-----|-------|
| Meaning | a command: "render this", "send that mail" | a fact: "order placed" |
| Producer knows the consumer | yes, the service owns the kind | no, subscribers come and go |
| Consumers | one handler per kind | one delivery per subscription |
| Scope | one service and its database, any of its processes | between services, each with its own database |
| Contract | `jobs.Kind[P]` inside the service | `events.Topic[P]` in the publisher's contract package |

Why a job is not "a kind of event": every job would pay for routing it never uses (subscription lookup, envelope, versioned topic), and command semantics (one owner, status per job, cancel, unique key) do not fit a fact with unknown subscribers. Why "an event triggers a job": once delivered, an event needs exactly what a job already has (lease, retry, backoff, dead letter, wide event, metrics). So events add routing and transport only, and both sides of the wire process through the job runtime.

```
events    topics, subscriptions, fan-out, outbound delivery, inbox, signing
  | uses
jobs      queue table, enqueue in the caller's transaction, lease, worker, retry, schedules
  |
Postgres
```

Where a consumer runs is wiring in `main`, not code:

| Producer and consumer | Mechanism | Hop |
|-----------------------|-----------|-----|
| same process | job, worker in the same binary | none |
| same service, separate Deployment (GPU pods, memory-heavy decode) | job; one image, the `JOB_QUEUES` setting decides which queues a Deployment consumes (P10: one artifact); all pods use the service's database | none |
| module of a modular monolith reacts to another module | event, local subscription: fan-out enqueues the subscriber's job in the publisher's transaction | none, exactly-once enqueue |
| different services, own databases | event, remote subscription: an outbound job in the publisher POSTs a signed CloudEvent to the subscriber's inbox, which enqueues the subscriber's job | one HTTP hop, at-least-once, deduplicated at the inbox |

Extracting a module into its own service turns its subscriptions from local to remote; the publisher's code does not change.

A command to another service is a call to that service's API from a job, with an idempotency key (ARC-005, RES-004). Jobs never cross a service boundary; there are no remote jobs.

Rejected: a shared "bus" database all services connect to (couples every service to one database and its credentials); NATS JetStream or another broker in v1 (new infrastructure, still needs an outbox for transactional publish; the outbound handler is the seam to add one later); a Kafka-style event log with consumer cursors on Postgres (sequence values commit out of order, so cursors skip in-flight rows); River (decided 31.10.2026: own library in govengers shape).

## 2. Decisions: jobs

| ID | Decision |
|----|----------|
| J1 | Module `github.com/govengers/jobs`. Postgres only. Tables `govengers_jobs` and `govengers_job_schedules` (prefix like livewire's `livewire_*`, no clash with an app's own `jobs` table). `postgresadapter` exports the DDL for the app's goose migrations; nothing runs DDL at startup (livewire D22). |
| J2 | Typed contract: `jobs.NewKind[P](name, &jobs.KindOptions{Queue, MaxAttempts, Backoff})`, declared once and used by producer and handler. Payload is JSON. Decoding is tolerant (unknown fields ignored), so a newer producer and an older worker coexist during a rollout; a payload that does not decode is dead at once with the decode error (JOB-006). Payloads carry references, not data, capped at 256 KiB by a `CHECK`. |
| J3 | Enqueue on the caller's transaction: `store.Enqueue(ctx, transaction, jobs...)` accepts a `pgx.Tx` or the pool. Repositories pass their own transaction or `livewire.TransactionFrom(ctx)` (livewire D31, D41). Returns the job IDs, which the app may store on its domain row to show progress. Per job: `RunAt`, `UniqueKey` (at most one pending job per queue and key; a second enqueue returns the pending job's ID instead of inserting), headers from the propagators (J15). |
| J4 | A queue is the unit of concurrency, lease and deployment. The worker takes per-queue `Concurrency`, `Lease` and `PollInterval` and only the queues it consumes. It claims at most its free slots, so no claimed job waits in memory while its lease runs out. |
| J5 | Claim: one statement, a CTE with `FOR UPDATE SKIP LOCKED` feeding an `UPDATE`, that sets `running`, `attempt + 1`, a fresh `lease_token` and `lease_until = now() + lease`. All times are database time, never the worker's clock. |
| J6 | Fencing: every write after the claim (heartbeat, complete, retry, dead, snooze, release, cancel) matches `id` and `lease_token`. Zero rows means the lease was lost: the result is discarded and reported to the Observer. The token, not the attempt counter, fences, so a release can give the attempt back without ever reusing a fence. |
| J7 | Heartbeat: every lease/3 the worker extends the leases of its running jobs, one statement per queue. A job missing from the result lost its lease; its handler context is cancelled with cause `jobs.ErrLeaseLost`. The lease only has to outlive a missed heartbeat, not the job's p99, so a job may run 10 s or 10 min on the same queue. |
| J8 | Handler outcomes: `nil` completes. `jobs.Permanent(err)` is dead at once. `jobs.Snooze(delay)` goes back to pending at now + delay without counting the attempt (a precondition not met yet). Any other error, and a recovered panic, retries at now + backoff(attempt) (exponential with jitter and a cap, per kind) and is dead once `attempt = max_attempts`. Unclassified errors retry by default: handlers are idempotent (JOB-003) and attempts are capped, while opt-in retry silently kills a job on every transient error nobody classified (a reset connection). |
| J9 | Rescuer: every process running a worker periodically moves running jobs with an expired lease back to pending, or to dead at `max_attempts`, in bounded `SKIP LOCKED` batches; no leader. The lost attempt counts, so a payload that OOM-kills the pod ends dead instead of looping forever. |
| J10 | Shutdown: when the context is cancelled the worker stops claiming. Running handlers run on a context detached from it (`context.WithoutCancel`) until `DrainTimeout` (default 20 s; it runs in parallel to LAY-003's HTTP drain inside the pod's grace period). Then their contexts are cancelled and every job still held is released (pending, runnable now, attempt given back), so another replica takes it immediately. `Run` returns after every handler returned. |
| J11 | Schedules: `jobs.Schedule{Name, Next, Job}` with `Next func(time.Time) time.Time` and helpers `jobs.Every(interval)` and `jobs.DailyAt(hour, minute, location)`. A process claims a due row of `govengers_job_schedules` with `SKIP LOCKED`, enqueues the job and advances `next_run_at` in one transaction, so exactly one replica enqueues per period. Periods missed during downtime run once, not as a burst. No leader election, no cron dependency. This replaces "time.Ticker in every replica" (CON-006) for recurring work. |
| J12 | Cancel: a pending job is cancelled at once; a running job gets `cancel_requested`, which the next heartbeat reads and turns into a cancelled handler context with cause `jobs.ErrCancelled`. |
| J13 | Retention: every worker process deletes completed and cancelled jobs after `RetainCompleted` (default 7 d) and dead jobs after `RetainDead` (default 30 d), in bounded batches. |
| J14 | Admin operations on the store: `Get`, `List` (bounded, by queue and status), `Cancel`, `Retry` (dead to pending, attempts reset). The app builds its admin view on them; the library ships no UI. |
| J15 | Context propagation: `jobs.Propagator` with `Inject(ctx, headers)` and `Extract(ctx, headers) context.Context`. Headers are stored with the job at enqueue and restored before the handler runs. `authadapter` carries the `auth.Identity` as its JSON snapshot at enqueue time, so a job started by a user audits as that user; an OpenTelemetry propagator carries `traceparent` (OBS-001) through `propagation.MapCarrier`. |
| J16 | No logging (LIB-003). Middleware `func(next jobs.Handler) jobs.Handler` wraps every attempt. `wideeventadapter.Middleware` begins one wide event per attempt (`job.<queue>` with id, kind, attempt, outcome) and puts it into the context (LOG-001). An `Observer` reports claims, outcomes, lost leases and rescues; `prometheusadapter` turns them into metrics on the caller's `Registerer` (LIB-011) and adds a scrape-time collector for pending count, running count and oldest pending age per queue (MET-006). The depth gauges describe the shared table, so every pod reports the same value: dashboards aggregate them with `max`, not `sum`. |
| J17 | A job of a kind the consuming worker has no handler for (a producer rolled out before its worker) retries with backoff and is dead at `max_attempts`. |
| J18 | `jobstest.Drain(ctx, worker)` runs every due job synchronously until the consumed queues are empty, for service tests and e2e on embedded-postgres (TST, DAT-015). |
| J19 | (refines J3, WP2) `UniqueKey` coalesces only jobs that have not started: claiming a job clears its key, so a retried, snoozed or released job never collides with a newer pending job of the same key, and an enqueue during a run starts another run. A cancelled pending job releases its key too. |
| J20 | (WP1) `NewKind` does not validate, so a kind can initialize a package-level variable; `Handle` and `Enqueue` return `jobs.ErrInvalidKind` for an empty name, an empty queue or a negative `MaxAttempts`. |
| J21 | (WP2) Propagators are configured twice from the same slice in `main`: `postgresadapter.StoreOptions.Propagators` inject at enqueue, `jobs.WorkerOptions.Propagators` extract before the handler. |
| J22 | (WP2) `MigrationV1` is a Go constant that starts with `SET LOCAL lock_timeout` and `statement_timeout` and passes squawk 2.66 for Postgres 18; the attempt counters stay `integer` with line-level `squawk-ignore prefer-bigint-over-int`. The admin types (`Record`, `ListFilter`) and their errors (`ErrNotFound`, `ErrFinished`, `ErrNotDead`) live in `postgresadapter`, not in the core. |
| J23 | (WP3) Store operations of the worker (claim, heartbeat, finish, rescue, sweep, schedules) run on a context detached from shutdown with a 10 s timeout, so a cancel during a claim never strands claimed rows. A failed finish write is reported to the Observer and the job stays running until the rescuer returns it. A handler that returns nil after its cancellation was requested completes. The minimum lease is 30 ms. |
| J24 | (WP3) Results reported to the Observer add `fenced` (the store refused the finish because the lease had expired before the heartbeat noticed) to J8's outcomes and `lease_lost`; result and operation names are snake_case so they work as metric labels. |
| J25 | (WP4, implements J18) `jobstest.Drain(tb, worker)` calls the new `Worker.RunDue(ctx)`: it claims a batch per queue and runs it synchronously, without heartbeats, until nothing runnable is left, and returns the number of attempts. It needs the worker's handlers, so it lives on the worker; it may be called repeatedly but not while `Run` runs. A retry waiting for its backoff is not runnable. |
| J26 | (WP4) `jobs.IsPermanent(err)` and `jobs.SnoozeDelay(err)` are exported for middleware, so the wide event records a snooze as a success and marks permanent failures. |
| J27 | (WP4) Depth: `jobs.QueueDepth` (pending = runnable now, scheduled = pending with a future run time, running, oldest pending age) is read by `postgresadapter.Store.QueueDepths` through the `prometheusadapter.DepthSource` interface. A failed read skips the gauges and counts `<namespace>_jobs_depth_errors_total` instead of failing the scrape; `Options.Queues` are reported with zeros so their series never disappear. Attempt durations are labeled by queue and kind only; the result is a label of the counter. |
| J28 | (WP4) `wideeventadapter.NewMiddleware(&Options{Emitter})` mirrors wide-event-logger's ginadapter. It recovers a handler panic into the attempt's error (with stack), records the cancel cause, and treats a snooze as success. |
| J29 | (WP4) `authadapter.Identity()` stores the full `auth.Identity` JSON snapshot in header `auth.identity`; revocations after enqueue do not reach the job. An undecodable header restores no identity. |
| J30 | (WP5) The end-to-end suite runs the binary of `test/e2e/worker` (build tag `e2e`, built with `-race` by `just e2e-build`, path in `E2E_WORKER`) twice on one embedded PostgreSQL; the worker reads its lease, drain timeout and schedule period from `E2E_*` variables and records every run in a table of the test. The skip key of TST-015 covers the worker binary, the compiled suite and `test/e2e`. |
| J31 | (WP5) `examples/signup` keeps three simplifications, each named in its code: `net/http` instead of Gin (no Gin in the module's dependencies), inline SQL instead of sqlc in the repository, and a self-applied schema instead of goose. Everything else follows the service rules: options constructors, controller -> service -> repository / client, the kind in `model`, enqueue in the repository's transaction, a job controller that maps not-found to `jobs.Permanent`. |
| J32 | (WP5) Splitting queues across Deployments (`JOB_QUEUES`) is a wiring pattern in `main`, documented in the wiki, not a library option: `WorkerOptions.Queues` already is the switch. |
| J33 | (fix round, review findings 12 and 13) Names are spelled out before v1 (NAM-001): `KindOptions.MaximumAttempts`, `DefaultMaximumAttempts`, `NewJob.MaximumAttempts()`, the `MaximumAttempts` fields of `Claim`, `Job` and `Record`, the column `maximum_attempts` and the wide event key `job.maximum_attempts` replace the `MaxAttempts` / `max_attempts` of J2, J8, J9, J17 and J20. `postgresadapter.New(&postgresadapter.Options{...})` replaces `NewStore` / `StoreOptions` (J21), matching DES-001's one-component naming and `prometheusadapter.New`. |
| J34 | (fix round, findings 10, 11, 24, 25) `WorkerOptions.Schedules` is a `[]Schedule` value slice the worker copies (a `NewJob` is immutable, so sharing its pointer is safe), and `Store.EnqueueDueSchedules` takes `[]Schedule`. `postgresadapter.Executor` is only `QueryRow`, the one method `Enqueue` calls. `NewJob.EncodePayload` validates the job's own fields, so a zero `NewJob` is `ErrInvalidKind` too. `NewWorker` and `postgresadapter.New` reject nil middleware and propagators. |
| J35 | (fix round, finding 1) A handler-side bug is a failed attempt, never an error of `Run`: `Permanent(nil)` is nil; a panic of the kind's `Backoff` or of the handler error's `Error` method makes the attempt a failure retried with `DefaultBackoff`; a negative backoff runs now; every Observer call is guarded, and a report whose call panics is dropped. A panic of the worker's own goroutines or of the `Store` still ends `Run` with the error (J23). |
| J36 | (fix round, finding 3) `Store.Finish` returns the status the job has after the write (empty when the lease was gone) and the write error. The worker reports that stored outcome: `cancelled` when a cancel request turned a pending result into cancelled, `fenced` when nothing was written, the new `finish_failed` when the write failed, with `Outcome.Err` the handler error joined with the write error. `RunDue` returns a failed write as its error, so `jobstest.Drain` fails the test (finding 14). |
| J37 | (fix round, findings 4, 6) The worker tracks running attempts by their full lease (job ID and token): a heartbeat result or the cleanup of an older attempt never touches a newer attempt of the same job; `HeldLease` carries the `Lease`. The heartbeat self-fences: an attempt whose lease was not confirmed for longer than the lease (failed or hanging extensions; each extension is bounded by the heartbeat interval) is cancelled with `ErrLeaseLost`, because another replica may already run it. This refines J7. |
| J38 | (fix round, findings 5, 16, 21, 31, 32) A queue runner's drain and heartbeat stop are deferred, so a panic while polling ends `Run` instead of hanging it. After the context is cancelled no claim starts, even when the timer or a freed slot is ready at the same time (J10). `RunDue` stops with an error once one job ran `MaximumRunsPerJob` (100) times in one call. Panics are formatted by one internal helper (`internal/recovery`) shared with `wideeventadapter`; queue runners receive their dependencies instead of the `*Worker`. |
| J39 | (fix round, findings 7, 15, 22; refines J11) `DailyAt` stays once per day across daylight saving changes: a wall-clock time inside a gap runs at the end of the gap (the bound of the zone period `time.Date` picked that lies closest to it), a repeated time runs once, and the result is always after its input; a nil location is UTC. The store refuses a `Next` that is not after now, failing the schedule's transaction instead of storing a schedule that stays due. `ExponentialBackoff` raises a base below 1 ms to 1 ms and a maximum below the base to the base, and saturates instead of overflowing. |
| J40 | (fix round, findings 2, 19, 20) `Retry` starts a dead job over: attempts, last error and an earlier cancellation request are reset. `Enqueue` checks every kind and payload before its first insert; a database error after some inserts returns the IDs inserted so far with the error (kept on a pool or connection, rolled back on a transaction); a transaction is the way to all or nothing. A `UniqueKey` needs READ COMMITTED; under REPEATABLE READ or SERIALIZABLE a concurrent enqueue of the same key fails with the new sentinel `postgresadapter.ErrSerializationFailure`, which the caller retries with the transaction. |
| J41 | (fix round, findings 17, 18, 23, 25, 26; refines J15, J27, J29) `Propagator.Inject` returns an error for a value it cannot carry, and `Enqueue` then fails before inserting anything, so a user-started job never silently loses its actor; a context without the value is no error. `Extract` must not let a value of the worker's own context through where the headers carry none: `authadapter` shadows the identity with nil for a missing or undecodable header. `prometheusadapter.New` registers all collectors or none and rejects a negative `DepthTimeout`. |
| J42 | (fix round, findings 9, 28, 30, 31, 34; refines J30) The e2e harness stops every worker the test did not kill with SIGTERM and fails the test on an exit status other than 0, a race report or a panic in the worker's output; a harness test proves it notices each. Suite and worker share their kinds through `test/e2e/internal/contract`. The schedule test counts enqueues per `Every` period. Release tags run CI with `full`. `.govengers.yml` declares the DES-003 exception for the test harness's one PostgreSQL per test binary, and the harness stops the instances of test binaries that died before stopping them. |
| J43 | (fix round, findings 33, 35, 36, 37; refines J10, J15, J16) The wide-event middleware is `wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter})` (J28), not the `Middleware(emitter)` of J16 and the old section 6. J15's OpenTelemetry propagation is the application's: the module ships no OTel dependency, the wiki shows the propagator in a few lines with `propagation.TraceContext` and `propagation.MapCarrier`. J10's release happens when the handler returns after the drain's cancel: an error releases, nil completes, a `Snooze` wins and delays; a handler that ignores its context keeps the job until it returns or the process dies, then the rescuer takes over. Examples start `Run` inside `recovering` (CON-001). |
| J44 | (fix round, findings 38, 39, 40; refines J31) `examples/signup` names its scope in its package comment (job integration, not a service template) and all three simplifications, `net/http` instead of Gin included. It wires `prometheusadapter` as the worker's Observer and serves `/metrics` on an internal listener, and its repository wraps every error with the operation and the user. |
| J45 | (review round 2, findings 1, 5, 6, 7, 8, 9, 15, 18; refines J11, J34, J39) `DailyAt` advances the calendar date on its own (year, month, day + n) before it resolves the wall clock, so a midnight schedule in a zone that switches at midnight runs on the transition day at the end of the gap; an hour outside 0..23 or a minute outside 0..59 gives a `Next` that `NewWorker` rejects. Each schedule has its own transaction per check, so one refused `Next` or failed enqueue does not roll back the others; the failures come back joined, each naming its schedule. `Next` is compared with now at microsecond precision. Scheduled jobs are enqueued on a context without the values of `Run`'s context, so propagators inject nothing from it. `NewWorker` encodes a schedule's payload once (a `NewJob` it keeps is now really immutable). |
| J46 | (review round 2, findings 2, 3, 4, 10, 11, 17, 19; refines J4, J7, J37, J38) The lease durations are `QueueOptions.LeaseDuration`, `ClaimRequest.LeaseDuration` and `DefaultLeaseDuration` (NAM-009: `Lease` is the claim, a job ID and a token). The worker is no longer "running" once `Run` returned: `Run`, `RunDue` and `Handle` work again. `RunDue` claims no batch on a cancelled context. J37's grace period, the heartbeat interval of a third of the lease, the extension's bound by that interval and the forgetting of finished attempts are each pinned by a test. |
| J47 | (review round 2, findings 12, 13, 14, 16, 20-23; refines J28, J43) `jobs.OnResult(ctx, report)` lets middleware learn an attempt's stored result and the finish write's error; the worker calls the reports after the finish, panics dropped. `wideeventadapter` emits the attempt's event then, with `job.result` and `job.finish_error`, and an outcome that was not stored (fenced, finish_failed, lease_lost) is a failure; outside a worker it emits on return. The OpenTelemetry propagator of J43 is a compiled, tested example (`example_trace_test.go`) that shadows the worker's span before extracting; `go.opentelemetry.io/otel` is a test-only dependency, the library imports none. The depth collector clones what a `DepthSource` returns before adding declared queues. The test harness signals an orphan's postmaster only when the process still runs from that directory. The example owns its worker (cancel and wait before the pool closes) and names its mail client `mailer.New(&mailer.Options{...})`. |
| J48 | (review round 3, findings 1, 2, 5, 7, 8; refines J11, J45) The store checks one schedule per call: `Store.EnqueueDueSchedule(ctx, schedule) (bool, error)` replaces `EnqueueDueSchedules`, and every error names the schedule. The worker gives each schedule its own operation timeout, and the schedule's transaction sets a 1 s `lock_timeout`, so a schedule that waits on another transaction fails fast and is tried again at the next check. Schedules run in a goroutine of their own, so they never delay rescue and sweep. A `Next` that panics fails its schedule's check (and `NewWorker` with an error), not `Run`. `NewWorker` encodes a scheduled payload exactly once. The skip of a schedule row another worker holds is pinned by a test that fails without `SKIP LOCKED` and with a plain `FOR UPDATE`. |
| J49 | (review round 3, findings 3, 4, 6, 9, 10; refines J47) `jobs.OnResult` stays (the wide event needs the stored result, and an application's own tracing middleware does too) with a complete lifetime: register while the handler or the middleware runs; every report is called exactly once, in order, on the attempt's goroutine after the finish was written, before the slot is freed and before the Observer; after the delivery `OnResult` returns false. The OpenTelemetry propagator moved into the module `examples/tracing` (own `go.mod`, `replace` to the parent, run by `just lint`, `just test` and `just vulncheck`), so `github.com/govengers/jobs` requires no OpenTelemetry: a consumer's `go mod why -m go.opentelemetry.io/otel` reports that it does not need it. `postgresadapter.Store.List(ctx, nil)` is the zero filter; `Handle` with a nil worker or kind returns an error. |
| J50 | (review round 4, findings 1, 2, 4; refines J48, J49) The store, not the worker, turns a panic of a schedule's `Next` into that schedule's error (`nextRun` recovers it); the worker no longer recovers on the schedule path, so any other panic of the store ends `Run` again, as J35 says. A check calls `Next` once and uses the period for both the registration and the advance. `OnResult`'s order is documented as it is: the reports run after the Observer's `OperationFinished` for the finish write and before its `AttemptFinished` (J49 said "before the Observer"). |
| J51 | (MaPa, 31.10.2026: the two owner decisions of the v1 review) Extension policy: `jobs.Store` and `jobs.Observer` may gain methods in minor versions, and an implementation outside this module should expect that; their doc comments and the wiki pages that invite own implementations (Observability, Postgres-Store) say so. No embeddable no-op base type. Naming: the exported type `NewJob` is renamed `PreparedJob` (a `New...` identifier reads as a constructor everywhere else in the API); `Kind.New` keeps its name as the constructor that returns a `*PreparedJob`. Earlier decisions that say `NewJob` (J33, J34, J45) keep their wording and mean `PreparedJob`. |
| J52 | (04.11.2026, supersedes the `examples/tracing` part of J49) The OpenTelemetry propagator and an attempt-span middleware ship as `oteladapter`, a subpackage of this module like `prometheusadapter`: it imports only the OpenTelemetry API (`go.opentelemetry.io/otel`, `otel/trace`), and module graph pruning keeps it out of a consumer that does not import the package, so J49's property holds (`go mod why -m go.opentelemetry.io/otel` reports it is not needed). The span is named `job.<queue>` like the wide event, kind consumer, ends once the outcome is stored (`jobs.OnResult`), and writes `trace.id`/`trace.span_id` onto the attempt's wide event; the key strings are the standards' TRC contract, duplicated instead of importing govengers/telemetry. `examples/tracing` is removed. |
| J53 | (15.11.2026, progress WP; schema) `postgresadapter.MigrationV2` adds `progress` (double precision, 0 to 1), `progress_message` (at most 200 characters), `result` (json, at most `jobs.MaximumResultBytes` of text, J60), `error_class` and `started_at` to `govengers_jobs`: nullable columns without defaults and `NOT VALID` checks, so the migration rewrites and scans nothing and holds its `ACCESS EXCLUSIVE` lock briefly; it passes squawk 2.66 for Postgres 18. Rollout: run the migration, then deploy. A replica of the previous version works on the new schema (it names its columns and never touches the new ones); during the rollout its claims do not clear the progress of the attempt before and its finish writes keep the stored error class. Rows of V1 keep working (pinned by a test that migrates a V1 table with pending, running and dead jobs). `Retry` clears progress, error class and start with the attempts. |
| J54 | (progress) `jobs.ReportProgress(ctx, jobs.Progress{Fraction, Message})` from a handler. Never blocking: the attempt keeps the latest report and a timer writes it in the background, at most once per `QueueOptions.ProgressInterval` (default 1 s), trailing edge, so the last report of an interval always arrives; the first report goes out at once. The write is `Store.WriteProgress(ctx, lease, progress)`, fenced by the lease token like every write after the claim (a new `jobs.Store` method under J51). When the handler returns, the timer is stopped, a write in flight is waited for (no progress write lands after the finish), and the report no write took travels in `Finish.Progress`, so the finish stores it in the same statement. A failed or panicking progress write is an `Operation` `progress` with the error; the attempt goes on. A claim clears the progress: it belongs to one attempt, and a retry that waits shows how far the failed attempt got. Outside an attempt ReportProgress does nothing (shared service code may report); a fraction outside 0..1 or NaN is an error; a longer message is cut. The throttle is tested on `testing/synctest`'s clock, without real time. Rejected: writing progress with the heartbeat (lease/3 is 10 s by default and couples two concerns), synchronous writes in the handler (database latency inside the handler loop). |
| J55 | (results, error class, view) `jobs.SetResult(ctx, value)` attaches JSON (at most `MaximumResultBytes`, 64 KiB encoded; the column keeps that text as it is and its check has the same bound, J60) that the completed finish stores in `Finish.Result`; the last call wins, a failed attempt drops it, JSON null removes it; outside an attempt it is `ErrNoAttempt`. `jobs.ErrorClass` classes the last error: `transient` (an unclassified error or a panic), `permanent` (`Permanent`, an undecodable payload), `lease_expired` (the rescuer); the status says what became of the job, so a dead job with a transient error used up its attempts. `postgresadapter.View` is the stable view for user interfaces and API clients: id, queue, kind, `status` as `State` (`queued`, `running`, `succeeded`, `failed`, `cancelled`, mapped by `StateOf`), attempt and maximum, progress in percent (nil while none, 100 once succeeded), message, result, error and class, cancel request, created, run, started, finished and updated times. It leaves out payload and headers: headers carry the enqueuer's identity snapshot (J29). Fields are only added. `Record` carries the raw new columns and `Record.View()`. |
| J56 | (live) `postgresadapter.LivewireTriggerSQL` makes `govengers_jobs` notify a govengers/livewire engine (through its own trigger function since J61): insert, delete and TRUNCATE always, an update only when a column of `View` changed. It replaces livewire's `TriggerSQL("govengers_jobs")`, which would notify every heartbeat: one update per running job every lease/3, each re-running every subscription of the table (pinned by a test that fails with livewire's trigger). The SQL is exported by jobs, not livewire, because the column list is the jobs schema's. |
| J57 | (live) `livewireadapter.Declarations(&Options{Store, Pool, Path, Read, Write, Scope})` declares the read-only resource `jobs` of `View` (list and `jobs.item`, filter and sort by id, queue, kind, status, attempt, error class and times; search by kind) and, only with a `Write` check, the mutations `cancelJob` and `retryJob` (`POST /jobs/{id}/cancel`, `/retry`), which answer the view and upsert it into the live lists (livewire's `Upserts`). The application supplies the authorization: `Read` and `Write` are static checks, `Scope` a predicate over `govengers_jobs` that narrows the reads and the jobs the mutations reach (a job outside it is 404). The mutations run on the engine's transaction (`Store.CancelOn`, `RetryOn` on any `Executor`) and the hydration reads on the list's snapshot (`Store.ViewsOn` on any `Querier`), so the audit row and the change commit together and a unit of work never takes a second connection. A finished job's cancel and a retry of a job that is not dead are 409. Module graph: livewire does not import jobs, so `livewireadapter` is a subpackage like `authadapter`; module graph pruning keeps gin and the rest of livewire's dependencies out of a consumer that does not import it. |
| J58 | (scope of the progress WP) Not built: a distinct read on the jobs resource (the status filter is an enum; add it when a facet filter needs it), a configurable resource name (one jobs resource per registry), progress in the wide event and the span, a Postgres store partitioned by queue for the trigger. Cross-repo: govengers/ui's `JobLike` status lacks `cancelled` and its progress is a number, where `View.progress` is null while a running job reported nothing. |
| J59 | (16.11.2026, review round 1 of the progress WP) What a handler hands over must never cost the outcome, because the last progress report and the result travel in the finish write: a value PostgreSQL refuses there fails the finish, and the job is rescued and run again until dead. So: `ReportProgress` stores its message, and `decide` the handler's error text, as valid UTF-8 without NUL (NUL dropped, invalid bytes become U+FFFD) before the cut; `SetResult` refuses invalid UTF-8 in a `json.RawMessage` with `ErrResultNotStorable` (the result's column type is J60). `SetResult`'s errors are deterministic: returned from the handler they fail the attempt and the retries repeat its work, so the docs show dropping or `Permanent`. `View.errorClass` is `null` without an error, the value the nullable filter matches with `isNull`. The list items of `livewireadapter` carry no `result` (`Store.ListViewsOn`), the item read does: every notification re-runs every subscription of the table, and a page of 100 results of 64 KiB each would be read and sent on every re-run. `updatedAt` is not filterable or sortable: the heartbeat moves it without a notification. A mutation answers the changed job's view without the `Read` check: write implies reading the job written. Not built: a second finish attempt without the optional data after a data error (J60 removes the result's cause; the texts are made storable). |
| J60 | (16.11.2026, review round 2 of the progress WP; refines J53, J55, J59) `result` is `json`, not `jsonb`. jsonb refuses JSON that `encoding/json` accepts and passes through a `json.RawMessage` unchanged: a lone surrogate escape (`\ud83c`, what JavaScript's `JSON.stringify` writes for a string cut inside an emoji), `\u0000`, a number beyond numeric's range; such a result failed the finish write of every attempt until the rescuer killed the job, and `SetResult` cannot list jsonb's input rules. `json` checks syntax only and keeps the text `json.Marshal` encoded as it is (compacted, with `<`, `>`, `&`, U+2028 and U+2029 escaped, so a relayed body keeps its value, not its bytes), so every result `SetResult` accepts is stored (with no trigger converting the row to jsonb, J61), the check is exact (`octet_length(result::text) <= 65536`, the cap), and a read returns at most what was accepted (jsonb printed `1e131071` as 131072 digits, so 901 bytes accepted read back as 13 MB). `SetResult` keeps refusing invalid UTF-8, which every text type refuses. The live trigger compares `result::text`, json has no equality. Cost: SQL that wants jsonb operators on a result casts it, and the cast refuses the values above. Pinned by a worker test that stores each of them and the cap as encoded. |
| J61 | (16.11.2026, review round 3 of the progress WP; refines J56, J60) `LivewireTriggerSQL` brings its own trigger function, `govengers_jobs_livewire_notify()`, instead of livewire's `livewire_notify()`. livewire's function starts with `to_jsonb(OLD)` / `to_jsonb(NEW)`, which parses the `json` result into jsonb inside the finish statement: with the live trigger installed, every result J60 made storable failed the finish write again (lone surrogate 22P02, `\u0000` 22P05, `1e131072` 22003), and a row holding one, stored before the trigger, failed every `Sweep` batch it was in. The jobs function reads only `id` and sends livewire's documented notification (`{"table", "op", "id", "partition": ""}` on the channel `livewire`, TRUNCATE as a `delete` without id), so it needs no livewire SQL and converts no payload or result on a claim, progress write or finish. Rejected: a second finish without the result after a data error (it would drop a result the handler set, and the known cause is closed at the source). Any other trigger an application puts on `govengers_jobs` must not convert the row to jsonb either. Cross-repo: livewire's `livewire_notify()` could read only the id and partition columns. Pinned by the result test with the trigger installed, including a sweep, and the trigger test, which runs without livewire's function. |
| J62 | (16.11.2026; refines J42) The tests take PostgreSQL from `github.com/govengers/postgrestest` (pinned to the pseudo-version of its untagged `main`) instead of a copied harness. `internal/postgrestest` stays as a wrapper of about 50 lines so no test file changes: `New` is the library's `Pool` on a template with MigrationV1 and V2, `NewMigrated` a second template on the same server per set of migrations (`WithTemplate`), `Main` its `Main` under goleak. Each test gets its own database copied from the template instead of a schema with the migrations applied per test; the tables are in `public`, so `TestTheStoreWorksInTheSchemaOfTheSearchPath` keeps the coverage of a store whose tables only its pool's `search_path` reaches. The orphan sweep, the binaries cache (`~/.embedded-postgres-go`, which CI restores) and their tests are the library's. `just test` runs through the library's launcher, one PostgreSQL for every test binary of the run, at the version `go.mod` requires (`go run` without `@version`: the requirement exists already, so TCH-002's concern, tool requirements in a library's `go.mod`, does not apply); `go test ./pkg` and the e2e suite (one test binary) start their own. The launcher's URL depends only on the checkout's path, so the test cache keeps hitting; a plain `go test` after `just test` reruns each package once. TST-013 asks for a server the test started itself; the launcher's server is started by the same run for it alone, new per run, with a database per test from a template of this module's own migrations, so both reasons of the rule hold (`.govengers.yml` exception). |

## 3. Decisions: events

E1-E11 were agreed here; the events library now exists and its [docs/design.md of github.com/govengers/events](https://github.com/govengers/events/blob/main/docs/design.md) is their source of truth, with E12 onwards decided while building it. A refinement there wins over the text below.

| ID | Decision |
|----|----------|
| E1 | Module `github.com/govengers/events`, depends on `govengers/jobs`. A separate module, so the younger events API can change without forcing a jobs major (LIB-001). Built after jobs v1. |
| E2 | Typed contract `events.NewTopic[P]("orders.order-placed.v1")` in a contract package of the publisher that depends on nothing beyond the standard library and uuid; subscribers import it. The major version is part of the name; within a version only additive changes; a breaking change publishes v1 and v2 side by side until every subscriber moved. |
| E3 | Envelope: CloudEvents 1.0, structured JSON mode: `id` (uuidv7), `source` (publisher service), `type` (topic), `time`, `data`, extensions `traceparent` and `actor`. A known standard instead of an own format: readable by any tool, ready for a broker later. |
| E4 | Publish on the caller's transaction: `publisher.Publish(ctx, transaction, OrderPlacedTopic.New(payload))` reads the topic's subscriptions and enqueues one job per subscription in that transaction. Local subscription: a job on queue `events.<subscription>`. Remote subscription: a `govengers.events.deliver` job on queue `govengers.events.outbound`. A topic without subscriptions stores nothing. No event log table in v1. A publisher runs a worker that consumes `govengers.events.outbound`. Refined in govengers/events: queues by E13, the delivery ledger by E15 (its `docs/design.md`). |
| E5 | Subscriptions live in the publisher's database: `govengers_event_subscriptions(subscriber, name, topic, delivery, endpoint, secret)`. A subscriber declares its complete set per publisher and the publisher replaces that subscriber's rows in one transaction, so a removed handler stops receiving and nothing goes stale. Local: the subscriber writes its set at startup (same database). Remote: the subscriber sends it with `PUT {publisher}/events/subscribers/{subscriber}` from a job in its own queue, retried until the publisher answers, so the subscriber starts even while the publisher is down. Events published before a subscription exists are not delivered. |
| E6 | Outbound delivery (publisher's worker): loads the subscription at send time (deleted: complete and drop) and POSTs `application/cloudevents+json`, signed per Standard Webhooks (`webhook-id`, `webhook-timestamp`, `webhook-signature`, HMAC-SHA256). 2xx completes. 404 (subscriber mid-rollout), 408, 429 (honoring `Retry-After`), 5xx and transport errors retry with backoff; other 4xx are dead. Defaults: 30 attempts, backoff capped at 1 h, about one day. |
| E7 | Inbox (subscriber): `POST /events/{subscription}` verifies the signature and that the event type is the subscription's topic, then in one transaction inserts `(subscription, event id)` into `govengers_event_inbox` and, only when that row is new, enqueues the handler's job. Answers 202 for new and duplicate deliveries alike. Inbox rows expire after 7 d, longer than E6's delivery window. The handler is a jobs handler: retries, wide events and metrics come from jobs. |
| E8 | Secrets: a subscription's signing secret is HMAC(subscriber's `EVENTS_INBOX_KEY`, subscription name). The subscriber sends it at registration and stores nothing; the publisher stores it. Rotating the key re-registers. The registration request authenticates with the subscriber's API key at the publisher (govengers/auth `apikey`); an authorization check in the publisher's options decides which topics a subscriber may read. |
| E9 | Identity across services: the `actor` extension (`provider:subject`, user ID) is audit information only. A subscriber's job runs as the subscriber's system identity; an identity issued by another service is never treated as authenticated. Inside one service (jobs, local subscriptions) J15 carries the real identity. |
| E10 | Delivery is at-least-once and unordered. Payloads carry the aggregate's version or update time, so a subscriber can drop a stale update (Hub PR #322: `SKIP LOCKED` never orders). |
| E11 | Packages: `events` (topics, envelope, signing, publisher and subscriber, outbound, inbox and registration handlers on `net/http`), `events/postgresadapter` (Publish, subscription and inbox stores, DDL), `events/ginadapter` (routes). As built, see govengers/events `docs/design.md`: `ginadapter` dropped by E21. |

## 4. Packages (jobs)

| Package | Holds | Depends on |
|---------|-------|-----------|
| `jobs` | `Kind`, `PreparedJob`, `Job[P]`, `Worker`, `Handler`, `Middleware`, outcomes, backoff, `Schedule`, `Propagator`, `Observer`, the `Store` interface | standard library, uuid, golang.org/x/sync |
| `jobs/postgresadapter` | `Store`: enqueue, claim, heartbeat, finish, rescue, sweep, schedules, admin; DDL | pgx/v5 |
| `jobs/wideeventadapter` | per-attempt wide event middleware | wide-event-logger |
| `jobs/authadapter` | identity propagator | govengers/auth |
| `jobs/prometheusadapter` | Observer metrics, depth collector | client_golang |
| `jobs/jobstest` | `Drain` | |
| `jobs/livewireadapter` | the live jobs resource, cancel and retry mutations (J57) | govengers/livewire |

## 5. Schema (jobs)

```sql
CREATE TABLE govengers_jobs (
    id               uuid PRIMARY KEY DEFAULT uuidv7(),
    queue            text NOT NULL CHECK (queue <> ''),
    kind             text NOT NULL CHECK (kind <> ''),
    payload          jsonb NOT NULL CHECK (pg_column_size(payload) <= 262144),
    headers          jsonb NOT NULL DEFAULT '{}',
    status           text NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'running', 'completed', 'dead', 'cancelled')),
    run_at           timestamptz NOT NULL DEFAULT now(),
    attempt          integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    maximum_attempts integer NOT NULL CHECK (maximum_attempts > 0),
    unique_key       text,
    lease_token      uuid,
    lease_until      timestamptz,
    worker           text,
    cancel_requested boolean NOT NULL DEFAULT false,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    CHECK ((status = 'running') = (lease_token IS NOT NULL))
);

CREATE INDEX govengers_jobs_claim ON govengers_jobs (queue, run_at) WHERE status = 'pending';
CREATE INDEX govengers_jobs_lease ON govengers_jobs (lease_until) WHERE status = 'running';
CREATE INDEX govengers_jobs_finished ON govengers_jobs (finished_at) WHERE status IN ('completed', 'dead', 'cancelled');
CREATE UNIQUE INDEX govengers_jobs_unique ON govengers_jobs (queue, unique_key)
    WHERE unique_key IS NOT NULL AND status = 'pending';

CREATE TABLE govengers_job_schedules (
    name        text PRIMARY KEY CHECK (name <> ''),
    next_run_at timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
```

`MigrationV2` (J53) adds `progress double precision`, `progress_message text`, `result json`, `error_class text` and `started_at timestamptz`, each nullable, with `NOT VALID` checks (progress 0 to 1, message at most 200 characters, result at most 64 KiB of JSON text, the three error classes).

Status machine:

```
pending --claim--> running --nil--------------------> completed
   ^                 |  --Permanent / attempts used--> dead
   |                 |  --cancel requested-----------> cancelled
   +---retry, snooze, release, rescue (lease expired)-+
pending --cancel--> cancelled
```

## 6. Usage sketch

```go
// internal/generation/kinds.go: the contract, used by the producer and the handler
type RenderPayload struct {
    RenderID uuid.UUID `json:"renderId"`
}

var RenderKind = jobs.NewKind[RenderPayload]("generation.render", &jobs.KindOptions{
    Queue:           "generation",
    MaximumAttempts: 5,
})
```

```go
// repository: the job commits or rolls back with the render
func (r *RenderRepository) Create(ctx context.Context, input *model.RenderInput) (*model.Render, error) {
    transaction, err := r.options.Pool.Begin(ctx)
    if err != nil {
        return nil, fmt.Errorf("begin create render: %w", err)
    }
    defer transaction.Rollback(ctx)

    row, err := r.queries.WithTx(transaction).CreateRender(ctx, toCreateRenderParams(input))
    if err != nil {
        return nil, mapError(err)
    }
    payload := generation.RenderPayload{RenderID: row.ID}
    _, err = r.options.JobStore.Enqueue(ctx, transaction, generation.RenderKind.New(payload, nil))
    if err != nil {
        return nil, fmt.Errorf("enqueue render %s: %w", row.ID, err)
    }
    if err := transaction.Commit(ctx); err != nil {
        return nil, fmt.Errorf("commit create render %s: %w", row.ID, err)
    }
    return toRender(row), nil
}
```

```go
// main: the job controller is the transport layer, the service holds the logic (ARC-001)
worker, err := jobs.NewWorker(&jobs.WorkerOptions{
    Store:       jobStore,
    Queues:      jobQueues, // from JOB_QUEUES, e.g. {"generation": {Concurrency: 3, LeaseDuration: 30 * time.Second}}
    Middleware:  []jobs.Middleware{wideEvents}, // wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: emitter})
    Propagators: []jobs.Propagator{authadapter.Identity()},
    Observer:    jobMetrics,
})
if err != nil {
    return fmt.Errorf("create job worker: %w", err)
}
if err := jobs.Handle(worker, generation.RenderKind, renderJobController.Render); err != nil {
    return fmt.Errorf("register render handler: %w", err)
}
group.Go(recovering(func() error { return worker.Run(ctx) }))
```

```go
func (c *RenderJobController) Render(ctx context.Context, job *jobs.Job[generation.RenderPayload]) error {
    return c.options.RenderService.Render(ctx, job.Payload.RenderID)
}
```

Events, publisher and subscriber:

```go
// orders contract package
var OrderPlacedTopic = events.NewTopic[OrderPlaced]("orders.order-placed.v1")

// orders repository, in the transaction that inserted the order
err = r.options.Publisher.Publish(ctx, transaction, contract.OrderPlacedTopic.New(contract.OrderPlaced{OrderID: order.ID, Version: order.Version}))

// billing main: a remote subscription; the handler is a jobs handler
err = events.Subscribe(subscriber, ordersPublisher, ordercontract.OrderPlacedTopic, "billing.create-invoice", invoiceEventController.CreateForOrder)
```

## 7. Not in v1, and when to add it

| Feature | Add when |
|---------|----------|
| `LISTEN/NOTIFY` wake-up (livewire's postgresadapter already has a listener) | a queue needs pickup well under the poll interval |
| concurrency limits across replicas, per queue or per tenant | a rate-limited provider is shared by all replicas (bathroom-planner's 20 global / 10 per tenant) |
| serialization key (at most one running job per key) | per-entity ordering matters |
| `CompleteInTransaction` (exactly-once internal effects) | an internal effect cannot be made idempotent cheaply |
| job dependencies, workflows | a pipeline needs more than the last-one-out fan-in pattern |
| priorities | one queue mixes urgent and bulk work and a second queue is not enough |
| event log table, replay to a new subscription | a subscriber needs history it was not subscribed for |
| batched deliveries, NATS outbound adapter | event volume makes one POST per event per subscription too expensive |

## 8. Work packages

| WP | Delivers | Gate |
|----|----------|------|
| WP0 | jobs repo skeleton: `.govengers.yml` (library), `.golangci.yml`, justfile, CI, LICENSE, docs stubs | `just check` green |
| WP1 | core: kinds, jobs, outcomes, backoff, middleware, propagators, observer, `Store` interface | unit tests |
| WP2 | `postgresadapter`: DDL, enqueue, claim, heartbeat, fenced finish, rescue, sweep, schedules, admin | embedded-postgres tests: two workers on one database, lease loss, fencing, crash recovery |
| WP3 | worker: queues, slots, heartbeat, drain and release, schedules, rescuer, retention | race tests, goleak |
| WP4 | `wideeventadapter`, `authadapter`, `prometheusadapter`, `jobstest` | tests |
| WP5 | example service, e2e (two processes, `kill -9` mid-job), README, `llms.txt`, v1.0.0 | dual review |
| WP6 | standards: JOB rules rewritten around govengers/jobs, LOG-001 example, CON-006 note on recurring work, livewire D41 pointer | govengers-review on the standards diff |
| WP7 | events: core, `postgresadapter`, local subscriptions | embedded-postgres tests |
| WP8 | events: outbound, inbox, registration, signing, `ginadapter` | e2e: two services, two databases, subscriber down and back |
| WP9 | events v1.0.0, standards rules for events | dual review |

WP7-WP9 are built in the govengers/events repo: its [docs/design.md](https://github.com/govengers/events/blob/main/docs/design.md) holds what was delivered (v0, E12 onwards; `ginadapter` dropped by E21).

## 9. Open questions

1. Two modules (`jobs`, `events`) or one module with an `events` package. Proposal: two (E1).
2. Cross-service transport in v1: signed HTTP push with inbox deduplication (E6, E7), or a shared bus database, or NATS. Proposal: HTTP.
3. Subscriptions: subscriber registers its set at the publisher (E5), or the publisher lists its subscribers in its own configuration. Proposal: registration; the subscriber's code stays the single source of truth.
4. Table names with `govengers_` prefix in the app's schema, or an own Postgres schema. Proposal: prefix, like livewire.
5. Unclassified handler errors retry (J8), or retry only when wrapped as retryable (bathroom-planner's choice). Proposal: retry.
6. Cross-service identity is audit-only (E9). Proposal: yes.
