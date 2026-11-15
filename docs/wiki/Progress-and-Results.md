# Progress and results

A handler can tell how far it got and leave a small result; the store keeps both with the job, and a user interface reads them as a `postgresadapter.View`, live through govengers/livewire if the service uses it. Design: J53 to J58 in [docs/design.md](../design.md).

## Reporting progress

```go
func (c *ImportJobController) Import(ctx context.Context, job *jobs.Job[ImportPayload]) error {
    rows, err := c.importService.Rows(ctx, job.Payload.FileID)
    if err != nil {
        return err
    }
    for index, row := range rows {
        if err := c.importService.ImportRow(ctx, row); err != nil {
            return err
        }
        _ = jobs.ReportProgress(ctx, jobs.Progress{ // only a fraction outside 0..1 is an error
            Fraction: float64(index+1) / float64(len(rows)),
            Message:  fmt.Sprintf("row %d of %d", index+1, len(rows)),
        })
    }
    return nil
}
```

- `ReportProgress` never waits for the database. The worker writes the latest report in the background, at most once per `QueueOptions.ProgressInterval` (default 1 s); reports in between replace each other, and the last one is written when the interval ends. Report as often as the loop runs.
- The write is fenced by the attempt's lease: a worker that lost the job cannot overwrite the progress of the attempt that took it over.
- When the handler returns, the report no background write took is stored with the outcome, in the same statement. A failed attempt keeps its last progress, so a retry that waits shows how far it got ("row 214 of 500"); the next claim clears it.
- Outside an attempt (the same service method called from an HTTP request) `ReportProgress` does nothing. The message is stored as valid UTF-8 without NUL characters (NUL dropped, invalid bytes such as Latin-1 data become U+FFFD), then cut to 200 characters.
- A failed progress write does not fail the attempt. The Observer gets it as operation `progress` with the error, so `<ns>_jobs_operations_total{operation="progress",outcome="failure"}` counts it.

## Leaving a result

```go
if err := jobs.SetResult(ctx, ExportResult{FileID: file.ID, Rows: count}); err != nil {
    return jobs.Permanent(err) // deterministic: a retry would repeat the export and fail again
}
return nil
```

The result is JSON, at most 64 KiB encoded (`jobs.MaximumResultBytes`), and stored only when the job completes; a failed attempt drops it, the last call wins, a value that encodes as `null` removes it. Outside an attempt `SetResult` returns `jobs.ErrNoAttempt`, above the cap `jobs.ErrResultTooLarge`, for invalid UTF-8 in a `json.RawMessage` `jobs.ErrResultNotStorable`. The store keeps the encoded JSON as it is (a `json` column), so a relayed body is read back byte for byte; SQL that wants jsonb operators on it casts `result::jsonb`. These errors are deterministic: returned as they are, the attempt fails and every retry repeats the handler's work. Wrap them with `jobs.Permanent`, or drop the error when the result is optional. Keep it a reference plus a summary, like payloads: a file ID and a row count, not the file. Results are deleted with their job by the retention (7 days for completed jobs by default).

## Error classes

`Record.ErrorClass` and `View.ErrorClass` say what kind of error the last failed attempt had; the status says what became of the job:

| Class | Cause | A dead job with it |
|-------|-------|--------------------|
| `transient` | an unclassified error, a panic | used up its attempts; a `Retry` may succeed |
| `permanent` | `jobs.Permanent(err)`, a payload that did not decode | needs a code or data fix before a `Retry` |
| `lease_expired` | the worker stopped without finishing (crash, out-of-memory kill, lost connection) | died with its worker every time |

## The view

`postgresadapter.View` is the stable shape for user interfaces and API clients; `Record.View()` and `Store.ViewsOn` return it, `Store.ListViewsOn` without the result. Its JSON:

| Field | Meaning |
|-------|---------|
| `id`, `queue`, `kind` | the job |
| `status` | `queued` (pending, retries included), `running`, `succeeded`, `failed` (dead), `cancelled`; `postgresadapter.StateOf` maps a status |
| `attempt`, `maximumAttempts` | attempts started, the limit |
| `progress`, `progressMessage` | percent, 0 to 100; `null` while the running or last attempt reported none, 100 once succeeded |
| `result` | what the handler set with `SetResult`; `null` otherwise, and `null` in the list items of `livewireadapter` (read the item) |
| `error`, `errorClass` | the last failed attempt's error (`""` without one) and its class (`null` without one); a job that succeeded after a retry keeps them |
| `cancelRequested` | a cancel reached a running job; its next heartbeat stops it |
| `createdAt`, `runAt`, `startedAt`, `finishedAt`, `updatedAt` | when it was enqueued, may run next, last started, ended, last written (the heartbeat's lease extensions included; not live, see below) |

It leaves out the payload and the headers: the headers carry the identity snapshot of whoever enqueued the job. Fields are only ever added.

## Live in a livewire application

`livewireadapter` declares the jobs for a livewire registry:

```go
jobDeclarations, err := livewireadapter.Declarations(&livewireadapter.Options{
    Store: jobStore,
    Pool:  pool,
    Read:  authorization.IsUser(),
    Write: authorization.HasRole("operator"), // nil: read only, no mutations
    Scope: ownJobs,                           // optional: which jobs an identity sees and may change
})
if err != nil {
    return fmt.Errorf("declare the jobs: %w", err)
}
if err := registry.Add(jobDeclarations...); err != nil {
    return fmt.Errorf("register the API: %w", err)
}
```

| Declaration | Route | Does |
|-------------|-------|------|
| `jobs` | `GET /jobs`, `GET /jobs/{id}` (`jobs.item`) | filterable list (without `result`) and item of `View`; filter and sort by `id`, `queue`, `kind`, `status`, `attempt`, `errorClass` (filter only) and the times but `updatedAt`, search by kind, newest first |
| `cancelJob` | `POST /jobs/{id}/cancel` | `Store.Cancel` on the engine's transaction; answers the view (writing implies reading the job, `Read` is not checked); 409 for a finished job |
| `retryJob` | `POST /jobs/{id}/retry` | `Store.Retry`; answers the view; 409 for a job that is not dead |

`Read` and `Write` are livewire's static checks. Data-dependent rules go into `Scope`, a predicate over `govengers_jobs`; it narrows the reads, and a mutation on a job outside it is 404, as if the job did not exist. A scope by the enqueuing user reads the identity snapshot of `authadapter`:

```go
func ownJobs(ctx context.Context) (query.Predicate, error) {
    identity := auth.IdentityFrom(ctx)
    if identity == nil {
        return nil, errors.New("jobs read without an identity")
    }
    if slices.Contains(identity.Roles, "operator") {
        return nil, nil // every job: the scope narrows the mutations too
    }
    return func(builder *query.Builder) {
        builder.WriteString("(govengers_jobs.headers->>'auth.identity')::jsonb->>'provider' = ").Argument(identity.Provider)
        builder.WriteString(" AND (govengers_jobs.headers->>'auth.identity')::jsonb->>'subject' = ").Argument(identity.Subject)
    }, nil
}
```

The scope is compiled and tested in [livewireadapter/example_test.go](../../livewireadapter/example_test.go). A job without an identity (a schedule's) matches no user's scope. Since the scope narrows the mutations as well, an operator reaches every job only because the scope gives it every job.

For the subscriptions to go live, the migrations run livewire's `TriggerFunctionSQL` and then `postgresadapter.LivewireTriggerSQL`, not livewire's `TriggerSQL("govengers_jobs")`: the jobs trigger skips the heartbeat's lease extensions, which would otherwise re-run every subscription of the table every lease/3 per running job. Claims, finishes and progress writes still notify; `updatedAt`, which the heartbeat moves, is therefore not live and not a filter or sort field; on a busy queue the jobs subscriptions re-run often (livewire coalesces per subscription). The Stale gate of `livewiretest` names the table as `livewireadapter.Table`.

A govengers/ui `JobProgress` follows one job with `live['jobs.item']` and the id that the call which started the job answered (the repository returns the ID `Enqueue` gave it).
