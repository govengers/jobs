# Gotchas

- **Enqueue on the transaction, not on the pool.** `jobStore.Enqueue(ctx, pool, ...)` inside a transaction commits the job even when the transaction rolls back. Pass the transaction.
- **Handlers run at least once.** A lost lease, a crash after the effect, a released job: the same job can run again. Use an idempotency key for external calls (RES-004) and check-then-act in a transaction for internal effects.
- **No order.** Jobs on several replicas run concurrently, retries overtake newer jobs. Carry a version or a timestamp and drop stale updates.
- **Payloads are references.** A payload that carries the data runs on stale data after a retry and blows the 256 KiB cap.
- **A context the handler ignores blocks shutdown.** After the drain timeout the worker cancels the handler's context and waits for it to return. Pass `ctx` to every call.
- **`time.Sleep` in a handler holds a slot.** Return `jobs.Snooze(delay)` instead.
- **Retries need a backoff that fits.** The default doubles from 1 s and dies at the tenth attempt, about 9 minutes after the first failure. A provider that is down for an hour needs more attempts or a longer cap per kind.
- **Depth gauges are per table.** Every replica reports the same `_pending`; `sum` multiplies it by the replica count. Use `max`.
- **Propagators on both sides.** A propagator only on the store writes headers nobody reads; only on the worker it restores nothing.
- **Identity snapshots are old.** A job runs with the identity of the moment it was enqueued. Check revocable permissions again in the handler.
- **`RunDue` is not a worker.** It runs without heartbeats; a test handler that blocks longer than the lease loses its job to the rescuer of another worker sharing the database.
- **One worker per process and queue set.** Two workers in one process with the same queues work, but double the polling for nothing.
