package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	LastError       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	FinishedAt      *time.Time // nil until completed, dead or cancelled
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
    created_at, updated_at, finished_at`

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

// List returns the jobs that match filter, newest first.
func (s *Store) List(ctx context.Context, filter *ListFilter) ([]*Record, error) {
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
	err := row.Scan(&record.ID, &record.Queue, &record.Kind, &record.Payload, &record.Headers, &record.Status,
		&record.RunAt, &record.Attempt, &record.MaximumAttempts, &record.UniqueKey, &record.Worker, &record.CancelRequested,
		&record.LastError, &record.CreatedAt, &record.UpdatedAt, &record.FinishedAt)
	return record, err
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
	tag, err := s.pool.Exec(ctx, cancelJob, id)
	if err != nil {
		return wrapDriverError(err, "cancel job %s", id)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return s.explainMiss(ctx, id, ErrFinished, "cancel")
}

const retryJob = `
UPDATE govengers_jobs SET status = 'pending', attempt = 0, run_at = now(), finished_at = NULL, updated_at = now()
WHERE id = $1 AND status = 'dead'`

// Retry returns the dead job with id to pending with its attempts reset. It returns
// ErrNotFound or ErrNotDead.
func (s *Store) Retry(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, retryJob, id)
	if err != nil {
		return wrapDriverError(err, "retry job %s", id)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return s.explainMiss(ctx, id, ErrNotDead, "retry")
}

// explainMiss tells a missing job from one in the wrong status after an update matched no row.
func (s *Store) explainMiss(ctx context.Context, id uuid.UUID, wrongStatus error, operation string) error {
	if _, err := s.Get(ctx, id); err != nil {
		return fmt.Errorf("%s job: %w", operation, err)
	}
	return fmt.Errorf("%s job %s: %w", operation, id, wrongStatus)
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
