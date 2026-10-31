# Migration from a hand-rolled Postgres queue

Most services that predate this module have their own queue: a jobs table, a `FOR UPDATE SKIP LOCKED` claim, a poll loop, a reaper or a startup sweep. This page maps that shape onto the module. It is about the change, not about one project.

## What maps to what

| Hand-rolled | govengers/jobs |
|-------------|----------------|
| own `jobs` / `work_queue_jobs` table | `govengers_jobs` from `postgresadapter.MigrationV1` |
| `kind` string plus `json.Unmarshal` in a switch | `jobs.NewKind[P]` and `jobs.Handle` per kind |
| `Enqueue(ctx, payload)` on the pool after the domain write | `jobStore.Enqueue(ctx, transaction, kind.New(payload, nil))` in the repository's transaction |
| `status`, `attempts`, `locked_until`, `locked_by` | `status`, `attempt`, `lease_until` plus `lease_token`, `worker` |
| complete/fail `WHERE id = $1` | fenced by the lease token; a lost lease writes nothing |
| retry at the next poll | `run_at = now() + backoff`; `jobs.Permanent`, `jobs.Snooze` |
| "retryable" wrapper around transient errors | the default: any error retries; mark the hopeless ones `jobs.Permanent` |
| startup sweep "running jobs back to pending" (one replica only) | the rescuer on every worker, lease based, safe with many replicas |
| `time.Ticker` per replica for recurring work | `jobs.Schedule`, once per period across replicas |
| handler on the service (`service.HandleJob`) | a job controller that calls the service method |
| logs per attempt | `wideeventadapter` (one wide event per attempt), `prometheusadapter` |
| `DependsOn` between jobs | not in v1: the parent enqueues the child in its own transaction, or a fan-in counter enqueues the last step |
| per-tenant concurrency caps | not in v1: one queue per class of work, `Concurrency` per process |

## Steps

1. Add a goose migration that runs `postgresadapter.MigrationV1`. Keep the old table for now.
2. Declare one kind per old job type, with the payload struct the old handler decoded. Keep the old names as kind names if dashboards or admin views show them.
3. Move each handler into a job controller method that calls the service; return `jobs.Permanent` where the old code marked a job failed without retry.
4. Change the producers: enqueue in the repository's transaction instead of after it.
5. Deploy with both workers running: the new worker on the new table, the old worker draining the old table. Stop enqueueing into the old table first, then wait until it has no pending or running rows.
6. Remove the old worker, the old reaper or sweep and the old table in a later migration.

## Behavior that changes

- **Unclassified errors retry.** Code that relied on "only wrapped errors retry" must now mark permanent failures. Handlers must be idempotent anyway; the module delivers at least once.
- **A crashed attempt counts.** A job that kills its worker every time ends dead after its attempts instead of being reset forever.
- **Unique keys coalesce only jobs that have not started.** An enqueue while the job runs starts another run.
- **Payload decoding is tolerant.** Unknown fields no longer fail a job; a payload that does not decode at all is dead at once.
- **Status per job lives in `govengers_jobs`.** A UI that showed the old row's status reads `jobStore.Get(ctx, id)` with the ID `Enqueue` returned, or keeps its own status on the domain row.
