package postgresadapter_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

func TestGetNotFound(t *testing.T) {
	store, _ := newStore(t)

	_, err := store.Get(context.Background(), uuid.New())

	require.ErrorIs(t, err, postgresadapter.ErrNotFound)
}

func TestListFiltersAndPages(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	oldest := enqueue(t, store, pool, nil)
	middle := enqueue(t, store, pool, nil)
	newest := enqueue(t, store, pool, nil)
	_, err := store.Enqueue(ctx, pool, jobs.NewKind[mailPayload]("report.build", &jobs.KindOptions{Queue: "reports"}).New(mailPayload{}, nil))
	require.NoError(t, err)

	page, err := store.List(ctx, &postgresadapter.ListFilter{Queue: "mail", Status: jobs.StatusPending, Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{newest, middle}, recordIDs(page))

	page, err = store.List(ctx, &postgresadapter.ListFilter{Queue: "mail", Before: middle})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{oldest}, recordIDs(page))

	everything, err := store.List(ctx, &postgresadapter.ListFilter{})
	require.NoError(t, err)
	require.Len(t, everything, 4)

	running, err := store.List(ctx, &postgresadapter.ListFilter{Status: jobs.StatusRunning})
	require.NoError(t, err)
	require.Empty(t, running)
}

func recordIDs(records []*postgresadapter.Record) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

func TestCancelPending(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, &jobs.EnqueueOptions{UniqueKey: "ada"})

	require.NoError(t, store.Cancel(ctx, id))

	record := get(t, store, id)
	require.Equal(t, jobs.StatusCancelled, record.Status)
	require.NotNil(t, record.FinishedAt)
	require.ErrorIs(t, store.Cancel(ctx, id), postgresadapter.ErrFinished)
	require.ErrorIs(t, store.Cancel(ctx, uuid.New()), postgresadapter.ErrNotFound)
	require.NotEqual(t, id, enqueue(t, store, pool, &jobs.EnqueueOptions{UniqueKey: "ada"}), "a cancelled job releases its unique key")
}

func TestRetryDead(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	require.ErrorIs(t, store.Retry(ctx, id), postgresadapter.ErrNotDead)
	finish(t, store, jobs.StatusDead)

	require.NoError(t, store.Retry(ctx, id))

	record := get(t, store, id)
	require.Equal(t, jobs.StatusPending, record.Status)
	require.Equal(t, 0, record.Attempt)
	require.Nil(t, record.FinishedAt)
	require.ErrorIs(t, store.Retry(ctx, uuid.New()), postgresadapter.ErrNotFound)
}
