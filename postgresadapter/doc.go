// Package postgresadapter implements the jobs.Store on PostgreSQL 18 with pgx/v5: it enqueues
// jobs on the caller's transaction, claims them with FOR UPDATE SKIP LOCKED, fences every
// write after the claim with the lease token, and offers the admin operations Get, List,
// Cancel and Retry.
//
// Nothing runs DDL at startup. MigrationV1 and MigrationV2 hold the tables and indexes for
// the application's own goose migrations, LivewireTriggerSQL the trigger of a livewire
// application. View is the stable view of a job for user interfaces.
package postgresadapter
