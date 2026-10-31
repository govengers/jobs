package postgresadapter

// MigrationV1 creates the jobs tables and indexes. Run it from one of the application's goose
// migrations, in the schema the store's pool uses:
//
//	-- +goose Up
//	-- the content of postgresadapter.MigrationV1
//
// or from a goose Go migration with tx.ExecContext(ctx, postgresadapter.MigrationV1). Later
// versions of the module add MigrationV2 and so on; never edit an applied migration.
// It runs inside the migration's transaction (SET LOCAL) and passes squawk. PostgreSQL 18 is
// required for uuidv7().
const MigrationV1 = `
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE govengers_jobs (
    id               uuid PRIMARY KEY DEFAULT uuidv7(),
    queue            text NOT NULL CHECK (queue <> ''),
    kind             text NOT NULL CHECK (kind <> ''),
    payload          jsonb NOT NULL CHECK (pg_column_size(payload) <= 262144),
    headers          jsonb NOT NULL DEFAULT '{}',
    status           text NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'running', 'completed', 'dead', 'cancelled')),
    run_at           timestamptz NOT NULL DEFAULT now(),
    -- attempt counters stay far below the int range
    -- squawk-ignore prefer-bigint-over-int
    attempt          integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    -- squawk-ignore prefer-bigint-over-int
    max_attempts     integer NOT NULL CHECK (max_attempts > 0),
    unique_key       text,
    lease_token      uuid,
    lease_until      timestamptz,
    worker           text,
    cancel_requested boolean NOT NULL DEFAULT false,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    CHECK ((status = 'running') = (lease_token IS NOT NULL))
);

CREATE INDEX govengers_jobs_claim ON govengers_jobs (queue, run_at) WHERE status = 'pending';
CREATE INDEX govengers_jobs_lease ON govengers_jobs (lease_until) WHERE status = 'running';
CREATE INDEX govengers_jobs_finished ON govengers_jobs (finished_at) WHERE status IN ('completed', 'dead', 'cancelled');
CREATE UNIQUE INDEX govengers_jobs_unique ON govengers_jobs (queue, unique_key)
    WHERE unique_key IS NOT NULL AND status = 'pending';

CREATE TABLE govengers_job_schedules (
    name        text PRIMARY KEY CHECK (name <> ''),
    next_run_at timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
`
