package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/govengers/jobs"
)

// Durations travel as microseconds: "$n::bigint * interval '1 microsecond'".

const claimJobs = `
WITH next AS (
    SELECT id FROM govengers_jobs
    WHERE queue = $1 AND status = 'pending' AND run_at <= now()
    ORDER BY run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE govengers_jobs AS job SET
    status = 'running',
    attempt = job.attempt + 1,
    lease_token = gen_random_uuid(),
    lease_until = now() + $3::bigint * interval '1 microsecond',
    worker = $4,
    unique_key = NULL,
    updated_at = now()
FROM next
WHERE job.id = next.id
RETURNING job.id, job.lease_token, job.queue, job.kind, job.payload, job.headers, job.attempt, job.maximum_attempts`

// Claim implements jobs.Store.
func (s *Store) Claim(ctx context.Context, request *jobs.ClaimRequest) ([]*jobs.Claim, error) {
	rows, err := s.pool.Query(ctx, claimJobs, request.Queue, request.Limit, request.LeaseDuration.Microseconds(), request.Worker)
	if err != nil {
		return nil, wrapDriverError(err, "claim jobs of queue %s", request.Queue)
	}
	claims, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*jobs.Claim, error) {
		claim := &jobs.Claim{}
		err := row.Scan(&claim.Lease.JobID, &claim.Lease.Token, &claim.Queue, &claim.Kind, &claim.Payload, &claim.Headers, &claim.Attempt, &claim.MaximumAttempts)
		return claim, err
	})
	return claims, wrapDriverError(err, "claim jobs of queue %s", request.Queue)
}

const extendLeases = `
UPDATE govengers_jobs AS job SET
    lease_until = now() + $3::bigint * interval '1 microsecond',
    updated_at = now()
FROM unnest($1::uuid[], $2::uuid[]) AS lease(job_id, token)
WHERE job.id = lease.job_id AND job.lease_token = lease.token AND job.status = 'running'
RETURNING job.id, job.lease_token, job.cancel_requested`

// ExtendLeases implements jobs.Store.
func (s *Store) ExtendLeases(ctx context.Context, leases []jobs.Lease, duration time.Duration) ([]jobs.HeldLease, error) {
	jobIDs := make([]uuid.UUID, len(leases))
	tokens := make([]uuid.UUID, len(leases))
	for index, lease := range leases {
		jobIDs[index], tokens[index] = lease.JobID, lease.Token
	}
	rows, err := s.pool.Query(ctx, extendLeases, jobIDs, tokens, duration.Microseconds())
	if err != nil {
		return nil, wrapDriverError(err, "extend %d leases", len(leases))
	}
	held, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobs.HeldLease, error) {
		var lease jobs.HeldLease
		err := row.Scan(&lease.Lease.JobID, &lease.Lease.Token, &lease.CancelRequested)
		return lease, err
	})
	return held, wrapDriverError(err, "extend %d leases", len(leases))
}

// finishJob ends an attempt. A pending result of a job whose cancellation was requested
// becomes cancelled; the CHECK on lease_token holds because every result leaves running.
const finishJob = `
UPDATE govengers_jobs SET
    status = CASE WHEN $3::text = 'pending' AND cancel_requested THEN 'cancelled' ELSE $3::text END,
    run_at = CASE WHEN $3::text = 'pending' THEN now() + $4::bigint * interval '1 microsecond' ELSE run_at END,
    attempt = CASE WHEN $5::boolean THEN attempt - 1 ELSE attempt END,
    last_error = COALESCE(NULLIF($6::text, ''), last_error),
    finished_at = CASE WHEN $3::text = 'pending' AND NOT cancel_requested THEN NULL ELSE now() END,
    lease_token = NULL,
    lease_until = NULL,
    worker = NULL,
    updated_at = now()
WHERE id = $1 AND lease_token = $2 AND status = 'running'
RETURNING status`

// Finish implements jobs.Store.
func (s *Store) Finish(ctx context.Context, finish *jobs.Finish) (jobs.Status, error) {
	var stored jobs.Status
	err := s.pool.QueryRow(ctx, finishJob, finish.Lease.JobID, finish.Lease.Token, string(finish.Status), finish.Delay.Microseconds(), finish.ReturnAttempt, finish.Error).Scan(&stored)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", nil // the lease is no longer held
	case err != nil:
		return "", wrapDriverError(err, "finish job %s as %s", finish.Lease.JobID, finish.Status)
	default:
		return stored, nil
	}
}

// rescueJobs returns jobs whose lease expired. The attempt the lost lease belonged to counts,
// so a job that kills its worker every time ends dead instead of looping.
const rescueJobs = `
WITH expired AS (
    SELECT id FROM govengers_jobs
    WHERE status = 'running' AND lease_until < now()
    ORDER BY lease_until
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE govengers_jobs AS job SET
    status = CASE
        WHEN job.cancel_requested THEN 'cancelled'
        WHEN job.attempt >= job.maximum_attempts THEN 'dead'
        ELSE 'pending'
    END,
    run_at = now(),
    last_error = 'lease expired: the worker stopped without finishing the job',
    finished_at = CASE WHEN job.cancel_requested OR job.attempt >= job.maximum_attempts THEN now() END,
    lease_token = NULL,
    lease_until = NULL,
    worker = NULL,
    updated_at = now()
FROM expired
WHERE job.id = expired.id`

// Rescue implements jobs.Store.
func (s *Store) Rescue(ctx context.Context, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, rescueJobs, limit)
	if err != nil {
		return 0, wrapDriverError(err, "rescue jobs with an expired lease")
	}
	return int(tag.RowsAffected()), nil
}

const sweepJobs = `
DELETE FROM govengers_jobs WHERE id IN (
    SELECT id FROM govengers_jobs
    WHERE (status IN ('completed', 'cancelled') AND finished_at < now() - $1::bigint * interval '1 microsecond')
       OR (status = 'dead' AND finished_at < now() - $2::bigint * interval '1 microsecond')
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)`

// Sweep implements jobs.Store.
func (s *Store) Sweep(ctx context.Context, request *jobs.SweepRequest) (int, error) {
	tag, err := s.pool.Exec(ctx, sweepJobs, request.RetainCompleted.Microseconds(), request.RetainDead.Microseconds(), request.Limit)
	if err != nil {
		return 0, wrapDriverError(err, "sweep finished jobs")
	}
	return int(tag.RowsAffected()), nil
}

const (
	registerSchedule = `
INSERT INTO govengers_job_schedules (name, next_run_at) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`
	claimDueSchedule = `
SELECT EXISTS (
    SELECT 1 FROM govengers_job_schedules
    WHERE name = $1 AND next_run_at <= now()
    FOR UPDATE SKIP LOCKED
)`
	advanceSchedule = `
UPDATE govengers_job_schedules SET next_run_at = $2, updated_at = now() WHERE name = $1`
)

// EnqueueDueSchedules implements jobs.Store. Every schedule has a transaction of its own that
// registers, claims, enqueues and advances it, so its job is enqueued once per period however
// many workers call this, and a schedule that fails (a refused Next, a failed enqueue) does not
// hold back the others. The failures come back joined, each naming its schedule.
func (s *Store) EnqueueDueSchedules(ctx context.Context, schedules []jobs.Schedule) (int, error) {
	enqueued := 0
	var failures []error
	for _, schedule := range schedules {
		wasDue, err := s.enqueueIfDue(ctx, schedule)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if wasDue {
			enqueued++
		}
	}
	return enqueued, errors.Join(failures...)
}

// enqueueIfDue runs one schedule's transaction and reports whether it enqueued the job.
func (s *Store) enqueueIfDue(ctx context.Context, schedule jobs.Schedule) (bool, error) {
	transaction, err := s.pool.Begin(ctx)
	if err != nil {
		return false, wrapDriverError(err, "begin schedule %s", schedule.Name)
	}
	defer func() { _ = transaction.Rollback(ctx) }() // a no-op after Commit
	now, err := databaseNow(ctx, transaction)
	if err != nil {
		return false, err
	}
	wasDue, err := registerAndClaim(ctx, transaction, schedule, now)
	if err != nil {
		return false, err
	}
	if wasDue {
		if err := s.enqueueAndAdvance(ctx, transaction, schedule, now); err != nil {
			return false, err
		}
	}
	return wasDue, wrapDriverError(transaction.Commit(ctx), "commit schedule %s", schedule.Name)
}

// databaseNow is the transaction's now(): schedules compute their periods in database time.
func databaseNow(ctx context.Context, transaction pgx.Tx) (time.Time, error) {
	var now time.Time
	err := transaction.QueryRow(ctx, "SELECT now()").Scan(&now)
	return now, wrapDriverError(err, "read the database time")
}

// registerAndClaim registers the schedule unless it has a row and locks the row when it is
// due and no other worker holds it.
func registerAndClaim(ctx context.Context, transaction pgx.Tx, schedule jobs.Schedule, now time.Time) (bool, error) {
	next, err := nextRun(schedule, now)
	if err != nil {
		return false, err
	}
	if _, err := transaction.Exec(ctx, registerSchedule, schedule.Name, next); err != nil {
		return false, wrapDriverError(err, "register schedule %s", schedule.Name)
	}
	var due bool
	err = transaction.QueryRow(ctx, claimDueSchedule, schedule.Name).Scan(&due)
	return due, wrapDriverError(err, "claim schedule %s", schedule.Name)
}

func (s *Store) enqueueAndAdvance(ctx context.Context, transaction pgx.Tx, schedule jobs.Schedule, now time.Time) error {
	if _, err := s.Enqueue(ctx, transaction, schedule.Job); err != nil {
		return fmt.Errorf("enqueue schedule %s: %w", schedule.Name, err)
	}
	next, err := nextRun(schedule, now)
	if err != nil {
		return err
	}
	if _, err := transaction.Exec(ctx, advanceSchedule, schedule.Name, next); err != nil {
		return wrapDriverError(err, "advance schedule %s", schedule.Name)
	}
	return nil
}

// nextRun asks the schedule for its next period and refuses one that is not after now in the
// database's microsecond precision: stored, it would leave the schedule due and enqueue on
// every poll (a Next one nanosecond after now is stored as now).
func nextRun(schedule jobs.Schedule, now time.Time) (time.Time, error) {
	next := schedule.Next(now)
	if !next.Truncate(time.Microsecond).After(now.Truncate(time.Microsecond)) {
		return time.Time{}, fmt.Errorf("schedule %s: Next(%s) returned %s, which is not after it", schedule.Name, now.Format(time.RFC3339Nano), next.Format(time.RFC3339Nano))
	}
	return next, nil
}
