package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/govengers/jobs"
)

// Errors of the admin operations.
var (
	ErrNotFound = errors.New("postgresadapter: job not found")
	// ErrFinished is returned by Cancel for a job that already completed, died or was cancelled.
	ErrFinished = errors.New("postgresadapter: job already finished")
	// ErrNotDead is returned by Retry for a job that is not dead.
	ErrNotDead = errors.New("postgresadapter: job is not dead")
)

// The bounds of List.
const (
	defaultListLimit = 100
	maximumListLimit = 1000
)

// Record is a stored job as the admin operations return it.
type Record struct {
	ID              uuid.UUID
	Queue           string
	Kind            string
	Payload         json.RawMessage
	Headers         map[string]string
	Status          jobs.Status
	RunAt           time.Time
	Attempt         int
	MaximumAttempts int
	UniqueKey       string // empty once the job started (J19)
	Worker          string // the worker holding the lease while running
	CancelRequested bool
	LastError       string          // the error of the last failed attempt; a later success keeps it
	ErrorClass      jobs.ErrorClass // the class of LastError; empty without one (J55)
	// Progress is the last progress report of the running attempt, or of the last attempt once
	// it ended; nil while none was reported. A claim clears it (J54).
	Progress   *jobs.Progress
	Result     json.RawMessage // what the handler set with jobs.SetResult; nil unless completed with a result
	CreatedAt  time.Time
	UpdatedAt  time.Time
	StartedAt  *time.Time // when the last attempt was claimed; nil before the first claim
	FinishedAt *time.Time // nil until completed, dead or cancelled
}

// ListFilter selects the jobs of List, newest first.
type ListFilter struct {
	Queue  string      // empty means every queue
	Status jobs.Status // empty means every status
	Before uuid.UUID   // keyset page: only jobs with a smaller (older) ID; uuid.Nil starts at the newest
	Limit  int         // zero means 100, at most 1000
}

const recordColumns = `id, queue, kind, payload, headers, status, run_at, attempt, maximum_attempts,
    COALESCE(unique_key, ''), COALESCE(worker, ''), cancel_requested, COALESCE(last_error, ''),
    COALESCE(error_class, ''), progress, COALESCE(progress_message, ''), result,
    created_at, updated_at, started_at, finished_at`

// Get returns the job with id, or ErrNotFound.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (*Record, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+recordColumns+" FROM govengers_jobs WHERE id = $1", id)
	if err != nil {
		return nil, wrapDriverError(err, "get job %s", id)
	}
	record, err := pgx.CollectExactlyOneRow(rows, scanRecord)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("get job %s: %w", id, ErrNotFound)
	}
	return record, wrapDriverError(err, "get job %s", id)
}

const listJobs = `SELECT ` + recordColumns + ` FROM govengers_jobs
WHERE ($1 = '' OR queue = $1) AND ($2 = '' OR status = $2) AND ($3::uuid IS NULL OR id < $3)
ORDER BY id DESC
LIMIT $4`

// List returns the jobs that match filter, newest first; a nil filter is the zero filter (every
// queue and status, the newest 100).
func (s *Store) List(ctx context.Context, filter *ListFilter) ([]*Record, error) {
	if filter == nil {
		filter = &ListFilter{}
	}
	limit := defaultListLimit
	if filter.Limit > 0 {
		limit = min(filter.Limit, maximumListLimit)
	}
	var before *uuid.UUID
	if filter.Before != uuid.Nil {
		before = new(filter.Before)
	}
	rows, err := s.pool.Query(ctx, listJobs, filter.Queue, string(filter.Status), before, limit)
	if err != nil {
		return nil, wrapDriverError(err, "list jobs")
	}
	records, err := pgx.CollectRows(rows, scanRecord)
	return records, wrapDriverError(err, "list jobs")
}

func scanRecord(row pgx.CollectableRow) (*Record, error) {
	record := &Record{}
	var fraction *float64
	var message string
	err := row.Scan(&record.ID, &record.Queue, &record.Kind, &record.Payload, &record.Headers, &record.Status,
		&record.RunAt, &record.Attempt, &record.MaximumAttempts, &record.UniqueKey, &record.Worker, &record.CancelRequested,
		&record.LastError, &record.ErrorClass, &fraction, &message, &record.Result,
		&record.CreatedAt, &record.UpdatedAt, &record.StartedAt, &record.FinishedAt)
	if fraction != nil {
		record.Progress = &jobs.Progress{Fraction: *fraction, Message: message}
	}
	return record, err
}

// Querier runs the read of ViewsOn: a pgx.Tx, a *pgxpool.Pool, a *pgx.Conn, or the handle of a
// govengers/livewire unit of work (its postgresadapter.DBTX).
type Querier interface {
	Query(ctx context.Context, statement string, arguments ...any) (pgx.Rows, error)
}

// ViewsOn reads the views of the jobs with ids on querier, usually the caller's transaction,
// so the read sees the caller's snapshot; ids without a job are absent from the map. It is the
// hydration of a list the caller pages itself (livewireadapter does).
func (s *Store) ViewsOn(ctx context.Context, querier Querier, ids []uuid.UUID) (map[uuid.UUID]View, error) {
	return viewsOn(ctx, querier, recordColumns, ids)
}

// ListViewsOn is ViewsOn without View.Result, for the items of a list: a page of jobs does not
// read and send a result of up to jobs.MaximumResultBytes per job each time it re-runs.
func (s *Store) ListViewsOn(ctx context.Context, querier Querier, ids []uuid.UUID) (map[uuid.UUID]View, error) {
	return viewsOn(ctx, querier, listColumns, ids)
}

// listColumns are recordColumns without the result.
var listColumns = strings.Replace(recordColumns, " result,", " NULL::jsonb,", 1)

func viewsOn(ctx context.Context, querier Querier, columns string, ids []uuid.UUID) (map[uuid.UUID]View, error) {
	rows, err := querier.Query(ctx, "SELECT "+columns+" FROM govengers_jobs WHERE id = ANY($1)", ids)
	if err != nil {
		return nil, wrapDriverError(err, "read %d job views", len(ids))
	}
	records, err := pgx.CollectRows(rows, scanRecord)
	if err != nil {
		return nil, wrapDriverError(err, "read %d job views", len(ids))
	}
	views := make(map[uuid.UUID]View, len(records))
	for _, record := range records {
		views[record.ID] = record.View()
	}
	return views, nil
}

// cancelJob cancels a pending job at once and asks the worker of a running job to cancel it.
const cancelJob = `
UPDATE govengers_jobs SET
    status = CASE WHEN status = 'pending' THEN 'cancelled' ELSE status END,
    finished_at = CASE WHEN status = 'pending' THEN now() ELSE finished_at END,
    cancel_requested = true,
    updated_at = now()
WHERE id = $1 AND status IN ('pending', 'running')`

// Cancel cancels the job with id: a pending job at once, a running job when its worker's next
// heartbeat reads the request, which cancels the handler's context with cause jobs.ErrCancelled.
// It returns ErrNotFound or ErrFinished.
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) error {
	return s.CancelOn(ctx, s.pool, id)
}

// CancelOn is Cancel on executor, usually the caller's transaction, so the cancellation commits
// or rolls back with the caller's change.
func (s *Store) CancelOn(ctx context.Context, executor Executor, id uuid.UUID) error {
	return updateOne(ctx, executor, cancelUpdate, id)
}

// retryJob starts a dead job over: its attempts, its last error with its class, its progress,
// its start and a cancellation requested before it died are reset, so the first heartbeat of
// the new run does not cancel it.
const retryJob = `
UPDATE govengers_jobs SET
    status = 'pending',
    attempt = 0,
    run_at = now(),
    started_at = NULL,
    finished_at = NULL,
    cancel_requested = false,
    last_error = NULL,
    error_class = NULL,
    progress = NULL,
    progress_message = NULL,
    updated_at = now()
WHERE id = $1 AND status = 'dead'`

// Retry returns the dead job with id to pending with its attempts, its last error, its
// progress and any earlier cancellation request reset. It returns ErrNotFound or ErrNotDead.
func (s *Store) Retry(ctx context.Context, id uuid.UUID) error {
	return s.RetryOn(ctx, s.pool, id)
}

// RetryOn is Retry on executor, usually the caller's transaction.
func (s *Store) RetryOn(ctx context.Context, executor Executor, id uuid.UUID) error {
	return updateOne(ctx, executor, retryUpdate, id)
}

// adminUpdate is one admin operation that updates a job: its statement, the error of a job in
// a status the statement does not change, and its name for the errors.
type adminUpdate struct {
	statement   string
	wrongStatus error
	name        string
}

var (
	cancelUpdate = adminUpdate{statement: cancelJob, wrongStatus: ErrFinished, name: "cancel"}
	retryUpdate  = adminUpdate{statement: retryJob, wrongStatus: ErrNotDead, name: "retry"}
)

// updateOne runs update on the job with id and tells a missing job (ErrNotFound) from one in
// the wrong status when it matched no row.
func updateOne(ctx context.Context, executor Executor, update adminUpdate, id uuid.UUID) error {
	var updated uuid.UUID
	err := executor.QueryRow(ctx, update.statement+" RETURNING id", id).Scan(&updated)
	if !errors.Is(err, pgx.ErrNoRows) {
		return wrapDriverError(err, "%s job %s", update.name, id)
	}
	var exists bool
	if err := executor.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM govengers_jobs WHERE id = $1)", id).Scan(&exists); err != nil {
		return wrapDriverError(err, "%s job %s", update.name, id)
	}
	if !exists {
		return fmt.Errorf("%s job %s: %w", update.name, id, ErrNotFound)
	}
	return fmt.Errorf("%s job %s: %w", update.name, id, update.wrongStatus)
}

const queueDepths = `
SELECT queue,
    count(*) FILTER (WHERE status = 'pending' AND run_at <= now()),
    count(*) FILTER (WHERE status = 'pending' AND run_at > now()),
    count(*) FILTER (WHERE status = 'running'),
    COALESCE(EXTRACT(EPOCH FROM now() - min(run_at) FILTER (WHERE status = 'pending' AND run_at <= now())), 0)::float8
FROM govengers_jobs
WHERE status IN ('pending', 'running')
GROUP BY queue
ORDER BY queue`

// QueueDepths returns the backlog of every queue with pending or running jobs, for the depth
// gauges of prometheusadapter. Queues without such jobs are absent.
func (s *Store) QueueDepths(ctx context.Context) ([]jobs.QueueDepth, error) {
	rows, err := s.pool.Query(ctx, queueDepths)
	if err != nil {
		return nil, wrapDriverError(err, "read queue depths")
	}
	depths, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.QueueDepth, error) {
		var depth jobs.QueueDepth
		var oldestSeconds float64
		err := row.Scan(&depth.Queue, &depth.Pending, &depth.Scheduled, &depth.Running, &oldestSeconds)
		depth.OldestPendingAge = time.Duration(oldestSeconds * float64(time.Second))
		return depth, err
	})
	return depths, wrapDriverError(err, "read queue depths")
}
