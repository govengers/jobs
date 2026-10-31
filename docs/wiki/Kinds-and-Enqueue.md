# Kinds and enqueue

## Declare a kind

```go
type WelcomeMailPayload struct {
    UserID uuid.UUID `json:"userId"`
}

var WelcomeMailKind = jobs.NewKind[WelcomeMailPayload]("signup.welcome_mail", &jobs.KindOptions{
    Queue:       "mail", // required
    MaxAttempts: 8,      // zero means 10
    Backoff:     nil,    // nil means 1 s doubling to 1 h, plus up to 20 % jitter
})
```

`NewKind` does not validate, so it can initialize a package-level variable; `Handle` and `Enqueue` return `jobs.ErrInvalidKind` for an empty name or queue. Put the kind in `internal/model` (or wherever both the repository and the job controller can import it).

## Payloads

- Carry references, not data: the user ID, not the user. The handler loads the current state; a payload is capped at 256 KiB.
- JSON. Decoding ignores unknown fields, so a producer that added a field and an older worker run side by side during a rollout.
- A payload that does not decode is dead at once with the decode error; retrying cannot fix it.
- A breaking change gets a new kind name (`signup.welcome_mail.v2`); keep the old handler until the old jobs are gone.

## Enqueue in the transaction

```go
transaction, err := r.pool.Begin(ctx)
...
user, err := insertUser(ctx, transaction, input)
...
_, err = r.jobStore.Enqueue(ctx, transaction, model.WelcomeMailKind.New(model.WelcomeMailPayload{UserID: user.ID}, nil))
...
err = transaction.Commit(ctx)
```

`Enqueue` takes any `postgresadapter.Executor`: a `pgx.Tx`, the pool, a `*pgx.Conn`. Enqueue on the transaction of the domain change, in the repository (DAT-001). With livewire, that is `livewire.TransactionFrom(ctx)`. Enqueueing after the commit loses the job when the process dies in between; enqueueing on the pool inside a transaction enqueues a job for a change that may roll back.

`Enqueue` takes several jobs and returns their IDs in order. Store an ID on the domain row when the UI shows the job's progress.

## Options per job

```go
WelcomeMailKind.New(payload, &jobs.EnqueueOptions{
    RunAt:     time.Now().Add(time.Hour), // not claimed before; zero means now (database time)
    UniqueKey: "sync-tenant:" + tenantID.String(),
})
```

`UniqueKey` coalesces enqueues while a job with the same queue and key has not started: the second enqueue returns the waiting job's ID. Claiming a job releases its key (J19), so an enqueue while it runs starts another run, which then sees the newer state. It is debouncing, not exactly-once: two runs for one key can still overlap, so the handler stays idempotent.

## Propagated values

`postgresadapter.StoreOptions.Propagators` write values of the enqueuing context into the job's headers; the worker's `Propagators` restore them before the handler runs. Configure the same propagators on both sides. `authadapter.Identity()` carries the acting user ([Observability](Observability.md#identity)); an OpenTelemetry propagator carries the trace through `propagation.MapCarrier`.
