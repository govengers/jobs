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
    maximum_attempts integer NOT NULL CHECK (maximum_attempts > 0),
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

// MigrationV2 adds what an attempt reports besides its outcome (J53): the progress of the
// running attempt, the result of a completed job, the class of the last error and when the
// last attempt started. Run it in a goose migration after MigrationV1, before the replicas run
// this version of the module: it only adds nullable columns and NOT VALID checks, so it takes
// a short ACCESS EXCLUSIVE lock and rewrites nothing, and the replicas of the previous version
// keep working on the new schema (they never read or write the new columns).
const MigrationV2 = `
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE govengers_jobs
    ADD COLUMN progress double precision,
    ADD COLUMN progress_message text,
    ADD COLUMN result jsonb,
    ADD COLUMN error_class text,
    ADD COLUMN started_at timestamptz;

-- every row has NULL in the new columns, so NOT VALID skips nothing; new writes are checked
ALTER TABLE govengers_jobs ADD CONSTRAINT govengers_jobs_progress
    CHECK (progress >= 0 AND progress <= 1) NOT VALID;
ALTER TABLE govengers_jobs ADD CONSTRAINT govengers_jobs_progress_message
    CHECK (char_length(progress_message) <= 200) NOT VALID;
ALTER TABLE govengers_jobs ADD CONSTRAINT govengers_jobs_result
    CHECK (pg_column_size(result) <= 262144) NOT VALID;
ALTER TABLE govengers_jobs ADD CONSTRAINT govengers_jobs_error_class
    CHECK (error_class IN ('transient', 'permanent', 'lease_expired')) NOT VALID;
`

// LivewireTriggerSQL makes govengers_jobs notify a govengers/livewire engine of the changes a
// user interface shows, so a live subscription of a resource that lists the table re-runs
// (J56). Use it instead of livewire's TriggerSQL("govengers_jobs"), after livewire's
// TriggerFunctionSQL, in a goose migration after MigrationV2. An insert, a delete and a
// TRUNCATE always notify; an update notifies only when a column of View changed, so a lease
// extension of the heartbeat, which runs every lease/3 for every running job, notifies nothing.
// Every claim, finish and progress write still notifies: on a busy queue the subscriptions of
// the table re-run often (livewire coalesces them per subscription).
const LivewireTriggerSQL = `
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE TRIGGER livewire_notify AFTER INSERT OR DELETE ON govengers_jobs
    FOR EACH ROW EXECUTE FUNCTION livewire_notify();
CREATE OR REPLACE TRIGGER livewire_notify_update AFTER UPDATE ON govengers_jobs
    FOR EACH ROW WHEN (
        OLD.status IS DISTINCT FROM NEW.status
        OR OLD.attempt IS DISTINCT FROM NEW.attempt
        OR OLD.run_at IS DISTINCT FROM NEW.run_at
        OR OLD.cancel_requested IS DISTINCT FROM NEW.cancel_requested
        OR OLD.last_error IS DISTINCT FROM NEW.last_error
        OR OLD.error_class IS DISTINCT FROM NEW.error_class
        OR OLD.progress IS DISTINCT FROM NEW.progress
        OR OLD.progress_message IS DISTINCT FROM NEW.progress_message
        OR OLD.result IS DISTINCT FROM NEW.result
        OR OLD.started_at IS DISTINCT FROM NEW.started_at
        OR OLD.finished_at IS DISTINCT FROM NEW.finished_at
    )
    EXECUTE FUNCTION livewire_notify();
CREATE OR REPLACE TRIGGER livewire_notify_truncate AFTER TRUNCATE ON govengers_jobs
    FOR EACH STATEMENT EXECUTE FUNCTION livewire_notify();
`
