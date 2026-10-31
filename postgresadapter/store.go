package postgresadapter

import (
	"context"
	"encoding/json"
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

// Options configures a Store.
type Options struct {
	Pool *pgxpool.Pool // required; its search_path must reach the tables of MigrationV1
	// Propagators write values of the enqueuing context into every job's headers; the worker's
	// Propagators restore them.
	Propagators []jobs.Propagator
}

// New returns a Store on options.Pool.
func New(options *Options) (*Store, error) {
	if options == nil || options.Pool == nil {
		return nil, errors.New("postgresadapter: Options.Pool is required")
	}
	for index, propagator := range options.Propagators {
		if propagator == nil {
			return nil, fmt.Errorf("postgresadapter: Options.Propagators[%d] is nil", index)
		}
	}
	return &Store{pool: options.Pool, propagators: append([]jobs.Propagator(nil), options.Propagators...)}, nil
}

// Executor runs Enqueue's statements: a pgx.Tx, a *pgxpool.Pool or a *pgx.Conn. It is the one
// method Enqueue needs, so any wrapper of those types satisfies it.
type Executor interface {
	QueryRow(ctx context.Context, statement string, arguments ...any) pgx.Row
}

const insertJob = `
INSERT INTO govengers_jobs (queue, kind, payload, headers, run_at, maximum_attempts, unique_key)
VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6, NULLIF($7, ''))
ON CONFLICT (queue, unique_key) WHERE unique_key IS NOT NULL AND status = 'pending' DO NOTHING
RETURNING id`

const selectPendingUniqueJob = `
SELECT id FROM govengers_jobs WHERE queue = $1 AND unique_key = $2 AND status = 'pending'`

// Enqueue inserts the jobs with executor, usually the caller's transaction, so they commit or
// roll back with the caller's change. It returns their IDs in order; a job whose unique key is
// held by a pending job gets that job's ID instead of a new row.
//
// Propagators, kinds and payloads are checked before the first insert, so a value a propagator
// cannot carry, jobs.ErrInvalidKind or a payload that does not encode inserts nothing. A database error after some inserts returns the IDs
// inserted so far with the error: on a transaction they roll back with it, on the pool or a
// connection they stay. Pass a transaction for all or nothing.
//
// A UniqueKey needs READ COMMITTED, PostgreSQL's default. Under REPEATABLE READ or SERIALIZABLE
// a pending job with the same key that committed after the transaction's snapshot makes the
// insert fail with ErrSerializationFailure; retry the whole transaction then.
func (s *Store) Enqueue(ctx context.Context, executor Executor, preparedJobs ...*jobs.PreparedJob) ([]uuid.UUID, error) {
	headers := map[string]string{}
	for _, propagator := range s.propagators {
		if err := propagator.Inject(ctx, headers); err != nil {
			return nil, fmt.Errorf("enqueue: carry the context into the job headers: %w", err)
		}
	}
	payloads, err := encodePayloads(preparedJobs)
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(preparedJobs))
	for index, job := range preparedJobs {
		id, err := insert(ctx, executor, job, payloads[index], headers)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func encodePayloads(preparedJobs []*jobs.PreparedJob) ([]json.RawMessage, error) {
	payloads := make([]json.RawMessage, len(preparedJobs))
	for index, job := range preparedJobs {
		payload, err := job.EncodePayload()
		if err != nil {
			return nil, err
		}
		payloads[index] = payload
	}
	return payloads, nil
}

func insert(ctx context.Context, executor Executor, job *jobs.PreparedJob, payload json.RawMessage, headers map[string]string) (uuid.UUID, error) {
	var runAt *time.Time
	if !job.RunAt().IsZero() {
		runAt = new(job.RunAt())
	}
	for range uniqueKeyRetries {
		var id uuid.UUID
		err := executor.QueryRow(ctx, insertJob, job.Queue(), job.Kind(), payload, headers, runAt, job.MaximumAttempts(), job.UniqueKey()).Scan(&id)
		if isSerializationFailure(err) {
			return uuid.Nil, fmt.Errorf("enqueue %s with unique key %q: %w", job.Kind(), job.UniqueKey(), ErrSerializationFailure)
		}
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

// serializationFailure is SQLSTATE 40001.
const serializationFailure = "40001"

// ErrSerializationFailure is returned by Enqueue when PostgreSQL could not serialize the insert
// with a concurrent one (a UniqueKey under REPEATABLE READ or SERIALIZABLE); retry the
// transaction.
var ErrSerializationFailure = errors.New("postgresadapter: could not serialize the enqueue with a concurrent one")

func isSerializationFailure(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == serializationFailure
}

// wrapDriverError adds context to a driver error without making the driver's error types part
// of the API (LIB-010); nil stays nil.
func wrapDriverError(err error, format string, arguments ...any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %v", fmt.Sprintf(format, arguments...), err) //nolint:errorlint // LIB-010: pgx errors are not part of the API
}
