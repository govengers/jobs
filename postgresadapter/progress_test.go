package postgresadapter_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/internal/postgrestest"
	"github.com/govengers/jobs/postgresadapter"
)

// J54: a progress write is fenced by the lease token like every write after the claim.
func TestWriteProgressIsFencedByTheLease(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)

	written, err := store.WriteProgress(ctx, claim.Lease, jobs.Progress{Fraction: 0.3, Message: "page 3"})
	require.NoError(t, err)
	require.True(t, written)
	require.Equal(t, &jobs.Progress{Fraction: 0.3, Message: "page 3"}, get(t, store, id).Progress)

	stale := jobs.Lease{JobID: id, Token: uuid.New()}
	written, err = store.WriteProgress(ctx, stale, jobs.Progress{Fraction: 0.9})
	require.NoError(t, err)
	require.False(t, written, "another token holds the job")

	_, err = store.Finish(ctx, &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusCompleted})
	require.NoError(t, err)
	written, err = store.WriteProgress(ctx, claim.Lease, jobs.Progress{Fraction: 1})
	require.NoError(t, err)
	require.False(t, written, "a finished job takes no progress")
	require.InDelta(t, 0.3, get(t, store, id).Progress.Fraction, 1e-9)
}

// J54: a claim starts the attempt's progress over and records when it started.
func TestAClaimClearsTheProgressOfTheAttemptBefore(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)
	_, err := store.Finish(ctx, &jobs.Finish{
		Lease: claim.Lease, Status: jobs.StatusPending, Error: "timeout", ErrorClass: jobs.ErrorClassTransient,
		Progress: &jobs.Progress{Fraction: 0.8, Message: "almost"},
	})
	require.NoError(t, err)
	retried := get(t, store, id)
	require.Equal(t, &jobs.Progress{Fraction: 0.8, Message: "almost"}, retried.Progress, "a waiting retry shows how far the attempt got")
	require.Equal(t, postgresadapter.StateQueued, retried.View().Status)

	claimOne(t, store, lease)

	record := get(t, store, id)
	require.Nil(t, record.Progress)
	require.NotNil(t, record.StartedAt)
	require.Equal(t, "timeout", record.LastError, "the error of the attempt before stays until the next one fails or succeeds")
	require.Equal(t, jobs.ErrorClassTransient, record.ErrorClass)
}

// J53, J55: the finish write stores the result of a completed job and the class of an error;
// an empty class keeps the stored one.
func TestFinishStoresTheResultAndTheErrorClass(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)
	_, err := store.Finish(ctx, &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusPending, Error: "down", ErrorClass: jobs.ErrorClassTransient, Result: json.RawMessage(`"ignored"`)})
	require.NoError(t, err)
	require.Nil(t, get(t, store, id).Result, "only a completed job keeps a result")

	claim = claimOne(t, store, lease)
	_, err = store.Finish(ctx, &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusCompleted, Result: json.RawMessage(`{"pages": 12}`)})
	require.NoError(t, err)

	record := get(t, store, id)
	require.JSONEq(t, `{"pages": 12}`, string(record.Result))
	require.Equal(t, jobs.ErrorClassTransient, record.ErrorClass)
	view := record.View()
	require.Equal(t, postgresadapter.StateSucceeded, view.Status)
	require.InDelta(t, 100, *view.Progress, 1e-9)
	encoded, err := json.Marshal(view)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"result":{"pages":12}`)
}

// J55: a rescued job's error is classed as an expired lease.
func TestARescueClassesTheErrorAsAnExpiredLease(t *testing.T) {
	store, pool := newStore(t)
	id := enqueue(t, store, pool, nil)
	claimOne(t, store, time.Microsecond)
	require.Eventually(t, func() bool {
		rescued, err := store.Rescue(context.Background(), 10)
		require.NoError(t, err)
		return rescued == 1
	}, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, jobs.ErrorClassLeaseExpired, get(t, store, id).ErrorClass)
}

// J53: Retry starts the job over without the traces of its last run.
func TestRetryClearsProgressErrorClassAndStart(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)
	_, err := store.Finish(ctx, &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusDead, Error: "bad", ErrorClass: jobs.ErrorClassPermanent, Progress: &jobs.Progress{Fraction: 0.5}})
	require.NoError(t, err)

	require.NoError(t, store.Retry(ctx, id))

	record := get(t, store, id)
	require.Nil(t, record.Progress)
	require.Empty(t, record.ErrorClass)
	require.Nil(t, record.StartedAt)
}

// J57: CancelOn and RetryOn run on the caller's executor and roll back with it.
func TestCancelOnRollsBackWithTheTransaction(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	transaction, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = transaction.Rollback(ctx) }()

	require.NoError(t, store.CancelOn(ctx, transaction, id))
	require.ErrorIs(t, store.RetryOn(ctx, transaction, id), postgresadapter.ErrNotDead)
	require.ErrorIs(t, store.CancelOn(ctx, queryRowOnly{pool: pool}, uuid.New()), postgresadapter.ErrNotFound)
	require.NoError(t, transaction.Rollback(ctx))

	require.Equal(t, jobs.StatusPending, get(t, store, id).Status)
}

// J57: ViewsOn reads the views of the given jobs on the caller's querier.
func TestViewsOnReadsTheGivenJobs(t *testing.T) {
	store, pool := newStore(t)
	first := enqueue(t, store, pool, nil)
	second := enqueue(t, store, pool, nil)

	views, err := store.ViewsOn(context.Background(), pool, []uuid.UUID{first, second, uuid.New()})

	require.NoError(t, err)
	require.Len(t, views, 2)
	require.Equal(t, postgresadapter.StateQueued, views[first].Status)
	require.Nil(t, views[first].Progress)
	require.Equal(t, 3, views[second].MaximumAttempts)
}

// J57: ListViewsOn is ViewsOn without the result.
func TestListViewsOnLeavesTheResultOut(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)
	_, err := store.Finish(ctx, &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusCompleted, Result: json.RawMessage(`{"pages": 12}`)})
	require.NoError(t, err)

	full, err := store.ViewsOn(ctx, pool, []uuid.UUID{id})
	require.NoError(t, err)
	listed, err := store.ListViewsOn(ctx, pool, []uuid.UUID{id})
	require.NoError(t, err)

	require.JSONEq(t, `{"pages": 12}`, string(full[id].Result))
	expected := full[id]
	expected.Result = nil
	require.Equal(t, expected, listed[id])
}

func TestStateOfMapsEveryStatus(t *testing.T) {
	for status, state := range map[jobs.Status]postgresadapter.State{
		jobs.StatusPending: postgresadapter.StateQueued, jobs.StatusRunning: postgresadapter.StateRunning,
		jobs.StatusCompleted: postgresadapter.StateSucceeded, jobs.StatusDead: postgresadapter.StateFailed,
		jobs.StatusCancelled: postgresadapter.StateCancelled,
	} {
		require.Equal(t, state, postgresadapter.StateOf(status))
	}
}

// J53: MigrationV2 upgrades a table with jobs in every state; the jobs keep working.
func TestMigrationV2KeepsTheJobsOfV1Working(t *testing.T) {
	pool := postgrestest.NewMigrated(t, postgresadapter.MigrationV1)
	ctx := context.Background()
	var pending, running uuid.UUID
	token := uuid.New()
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO govengers_jobs (queue, kind, payload, maximum_attempts)
		VALUES ('mail', 'mail.send', '{}', 3) RETURNING id`).Scan(&pending))
	require.NoError(t, pool.QueryRow(ctx, `INSERT INTO govengers_jobs (queue, kind, payload, maximum_attempts, status, attempt, lease_token, lease_until)
		VALUES ('mail', 'mail.send', '{}', 3, 'running', 1, $1, now() + interval '1 hour') RETURNING id`, token).Scan(&running))
	_, err := pool.Exec(ctx, `INSERT INTO govengers_jobs (queue, kind, payload, maximum_attempts, status, finished_at, last_error)
		VALUES ('mail', 'mail.send', '{}', 3, 'dead', now(), 'v1 error')`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, postgresadapter.MigrationV2)
	require.NoError(t, err)

	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool})
	require.NoError(t, err)
	everything, err := store.List(ctx, nil)
	require.NoError(t, err)
	require.Len(t, everything, 3)
	for _, record := range everything {
		require.Nil(t, record.Progress)
		require.Empty(t, record.ErrorClass)
	}
	written, err := store.WriteProgress(ctx, jobs.Lease{JobID: running, Token: token}, jobs.Progress{Fraction: 0.5})
	require.NoError(t, err)
	require.True(t, written, "an attempt claimed before the upgrade reports progress")
	stored, err := store.Finish(ctx, &jobs.Finish{Lease: jobs.Lease{JobID: running, Token: token}, Status: jobs.StatusCompleted})
	require.NoError(t, err)
	require.Equal(t, jobs.StatusCompleted, stored)
	claim := claimOne(t, store, lease)
	require.Equal(t, pending, claim.Lease.JobID)
}

// J53: the checks of MigrationV2 refuse what the library never writes.
func TestMigrationV2ChecksTheNewColumns(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	for _, statement := range []string{
		"UPDATE govengers_jobs SET progress = 1.5 WHERE id = $1",
		"UPDATE govengers_jobs SET progress = 'NaN' WHERE id = $1",
		"UPDATE govengers_jobs SET error_class = 'unknown' WHERE id = $1",
		"UPDATE govengers_jobs SET progress_message = repeat('x', 201) WHERE id = $1",
		"UPDATE govengers_jobs SET result = to_json(repeat('x', 65535)) WHERE id = $1", // 65537 bytes of JSON
	} {
		_, err := pool.Exec(ctx, statement, id)
		require.Error(t, err, statement)
	}
}
