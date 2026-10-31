package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/jobs"
)

// uniqueKeyRetries bounds Enqueue's retries when the pending job holding a unique key is
// claimed between the insert and the lookup of its ID.
const uniqueKeyRetries = 3

// Store is the jobs.Store on PostgreSQL. It is safe for concurrent use.
type Store struct {
	pool        *pgxpool.Pool
	propagators []jobs.Propagator
}

// StoreOptions configures a Store.
type StoreOptions struct {
	Pool *pgxpool.Pool // required; its search_path must reach the tables of MigrationV1
	// Propagators write values of the enqueuing context into every job's headers; the worker's
	// Propagators restore them.
	Propagators []jobs.Propagator
}

// NewStore returns a Store on options.Pool.
func NewStore(options *StoreOptions) (*Store, error) {
	if options == nil || options.Pool == nil {
		return nil, errors.New("postgresadapter: StoreOptions.Pool is required")
	}
	return &Store{pool: options.Pool, propagators: append([]jobs.Propagator(nil), options.Propagators...)}, nil
}

// Executor runs the store's statements: a pgx.Tx, a *pgxpool.Pool or a *pgx.Conn.
type Executor interface {
	Exec(ctx context.Context, statement string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, statement string, arguments ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, statement string, arguments ...any) pgx.Row
}

const insertJob = `
INSERT INTO govengers_jobs (queue, kind, payload, headers, run_at, max_attempts, unique_key)
VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6, NULLIF($7, ''))
ON CONFLICT (queue, unique_key) WHERE unique_key IS NOT NULL AND status = 'pending' DO NOTHING
RETURNING id`

const selectPendingUniqueJob = `
SELECT id FROM govengers_jobs WHERE queue = $1 AND unique_key = $2 AND status = 'pending'`

// Enqueue inserts the jobs with executor, usually the caller's transaction, so they commit or
// roll back with the caller's change. It returns their IDs in order; a job whose unique key is
// held by a pending job gets that job's ID instead of a new row. It fails with
// jobs.ErrInvalidKind for a job of an invalid kind.
func (s *Store) Enqueue(ctx context.Context, executor Executor, newJobs ...*jobs.NewJob) ([]uuid.UUID, error) {
	headers := map[string]string{}
	for _, propagator := range s.propagators {
		propagator.Inject(ctx, headers)
	}
	ids := make([]uuid.UUID, 0, len(newJobs))
	for _, job := range newJobs {
		id, err := insert(ctx, executor, job, headers)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func insert(ctx context.Context, executor Executor, job *jobs.NewJob, headers map[string]string) (uuid.UUID, error) {
	payload, err := job.EncodePayload()
	if err != nil {
		return uuid.Nil, err
	}
	var runAt *time.Time
	if !job.RunAt().IsZero() {
		runAt = new(job.RunAt())
	}
	for range uniqueKeyRetries {
		var id uuid.UUID
		err := executor.QueryRow(ctx, insertJob, job.Queue(), job.Kind(), payload, headers, runAt, job.MaxAttempts(), job.UniqueKey()).Scan(&id)
		if !errors.Is(err, pgx.ErrNoRows) {
			return id, wrapDriverError(err, "enqueue %s", job.Kind())
		}
		err = executor.QueryRow(ctx, selectPendingUniqueJob, job.Queue(), job.UniqueKey()).Scan(&id)
		if !errors.Is(err, pgx.ErrNoRows) {
			return id, wrapDriverError(err, "find pending %s with unique key %q", job.Kind(), job.UniqueKey())
		}
	}
	return uuid.Nil, fmt.Errorf("enqueue %s: unique key %q kept changing hands", job.Kind(), job.UniqueKey())
}

// wrapDriverError adds context to a driver error without making the driver's error types part
// of the API (LIB-010); nil stays nil.
func wrapDriverError(err error, format string, arguments ...any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %v", fmt.Sprintf(format, arguments...), err) //nolint:errorlint // LIB-010: pgx errors are not part of the API
}
