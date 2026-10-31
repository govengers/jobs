# Gotchas

- **Enqueue on the transaction, not on the pool.** `jobStore.Enqueue(ctx, pool, ...)` inside a transaction commits the job even when the transaction rolls back. Pass the transaction.
- **Handlers run at least once.** A lost lease, a crash after the effect, a released job: the same job can run again. Use an idempotency key for external calls (RES-004) and check-then-act in a transaction for internal effects.
- **No order.** Jobs on several replicas run concurrently, retries overtake newer jobs. Carry a version or a timestamp and drop stale updates.
- **Payloads are references.** A payload that carries the data runs on stale data after a retry and blows the 256 KiB cap.
- **A context the handler ignores blocks shutdown.** After the drain timeout the worker cancels the handler's context and waits for it to return. Pass `ctx` to every call.
- **`time.Sleep` in a handler holds a slot.** Return `jobs.Snooze(delay)` instead.
- **Retries need a backoff that fits.** The default doubles from 1 s and dies at the tenth attempt, about 9 minutes after the first failure. A provider that is down for an hour needs more attempts or a longer cap per kind.
- **Depth gauges are per table.** Every replica reports the same `_pending`; `sum` multiplies it by the replica count. Use `max`.
- **Propagators on both sides.** A propagator only on the store writes headers nobody reads; only on the worker it restores nothing. An `Inject` error fails the enqueue.
- **`UniqueKey` needs READ COMMITTED.** A repository transaction at REPEATABLE READ gets `ErrSerializationFailure` for a concurrent enqueue of the same key; retry the transaction.
- **Identity snapshots are old.** A job runs with the identity of the moment it was enqueued. Check revocable permissions again in the handler.
- **`RunDue` is not a worker.** It runs without heartbeats; a test handler that blocks longer than the lease loses its job to the rescuer of another worker sharing the database.
- **Size the pool for the worker too.** The heartbeat, the finish writes, claims, rescue, sweep and every schedule check use the store's pool, the same pool the handlers use. A pool whose `MaxConns` (pgxpool's default is `max(4, NumCPU)`) only covers the handlers' summed `Concurrency` starves the heartbeat: after one lease without a confirmed extension the self-fence cancels the running handlers with `ErrLeaseLost`. Give `MaxConns` room for the handlers, the HTTP side and a few worker connections, or give the store a pool of its own.
- **One worker per process and queue set.** Two workers in one process with the same queues work, but double the polling for nothing.
