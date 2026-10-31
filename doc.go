// Package jobs is the durable work queue of a govengers service: jobs are enqueued inside the
// caller's PostgreSQL transaction, claimed by workers on any replica under a lease, retried
// with backoff and fenced so that a worker that lost its lease cannot overwrite a newer
// attempt. Delivery is at least once; handlers must be idempotent.
//
// The package declares the model (Kind, Job, Claim, Schedule), the handler outcomes
// (Permanent, Snooze) and the Worker that runs handlers. It does not talk to a database
// itself: the Store interface is implemented by the postgresadapter package, which also
// enqueues jobs and exports the DDL. The design is in docs/design.md.
//
// The package never logs and never starts goroutines outside Worker.Run.
package jobs
