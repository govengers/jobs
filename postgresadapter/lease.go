package postgresadapter

import (
	"context"
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
	rows, err := s.pool.Query(ctx, claimJobs, request.Queue, request.Limit, request.Lease.Microseconds(), request.Worker)
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
RETURNING job.id, job.cancel_requested`

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
		err := row.Scan(&lease.JobID, &lease.CancelRequested)
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
WHERE id = $1 AND lease_token = $2 AND status = 'running'`

// Finish implements jobs.Store.
func (s *Store) Finish(ctx context.Context, finish *jobs.Finish) (bool, error) {
	tag, err := s.pool.Exec(ctx, finishJob, finish.Lease.JobID, finish.Lease.Token, string(finish.Status), finish.Delay.Microseconds(), finish.ReturnAttempt, finish.Error)
	if err != nil {
		return false, wrapDriverError(err, "finish job %s as %s", finish.Lease.JobID, finish.Status)
	}
	return tag.RowsAffected() == 1, nil
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
	claimDueSchedules = `
SELECT name FROM govengers_job_schedules
WHERE name = ANY($1) AND next_run_at <= now()
FOR UPDATE SKIP LOCKED`
	advanceSchedule = `
UPDATE govengers_job_schedules SET next_run_at = $2, updated_at = now() WHERE name = $1`
)

// EnqueueDueSchedules implements jobs.Store. One transaction registers, claims, enqueues and
// advances, so a schedule's job is enqueued once per period however many workers call it.
func (s *Store) EnqueueDueSchedules(ctx context.Context, schedules []jobs.Schedule) (int, error) {
	transaction, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, wrapDriverError(err, "begin schedules")
	}
	defer func() { _ = transaction.Rollback(ctx) }() // a no-op after Commit
	var now time.Time
	if err := transaction.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return 0, wrapDriverError(err, "read the database time")
	}
	byName, err := registerSchedules(ctx, transaction, schedules, now)
	if err != nil {
		return 0, err
	}
	due, err := claimDue(ctx, transaction, byName)
	if err != nil {
		return 0, err
	}
	for _, schedule := range due {
		if err := s.enqueueAndAdvance(ctx, transaction, schedule, now); err != nil {
			return 0, err
		}
	}
	return len(due), wrapDriverError(transaction.Commit(ctx), "commit schedules")
}

func registerSchedules(ctx context.Context, transaction pgx.Tx, schedules []jobs.Schedule, now time.Time) (map[string]jobs.Schedule, error) {
	byName := make(map[string]jobs.Schedule, len(schedules))
	for _, schedule := range schedules {
		byName[schedule.Name] = schedule
		if _, err := transaction.Exec(ctx, registerSchedule, schedule.Name, schedule.Next(now)); err != nil {
			return nil, wrapDriverError(err, "register schedule %s", schedule.Name)
		}
	}
	return byName, nil
}

func claimDue(ctx context.Context, transaction pgx.Tx, byName map[string]jobs.Schedule) ([]jobs.Schedule, error) {
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	rows, err := transaction.Query(ctx, claimDueSchedules, names)
	if err != nil {
		return nil, wrapDriverError(err, "claim due schedules")
	}
	dueNames, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, wrapDriverError(err, "claim due schedules")
	}
	due := make([]jobs.Schedule, 0, len(dueNames))
	for _, name := range dueNames {
		due = append(due, byName[name])
	}
	return due, nil
}

func (s *Store) enqueueAndAdvance(ctx context.Context, transaction pgx.Tx, schedule jobs.Schedule, now time.Time) error {
	if _, err := s.Enqueue(ctx, transaction, schedule.Job); err != nil {
		return fmt.Errorf("enqueue schedule %s: %w", schedule.Name, err)
	}
	if _, err := transaction.Exec(ctx, advanceSchedule, schedule.Name, schedule.Next(now)); err != nil {
		return wrapDriverError(err, "advance schedule %s", schedule.Name)
	}
	return nil
}
