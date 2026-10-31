package jobs

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Store persists jobs for the Worker; postgresadapter.Store implements it. Every write after
// a claim is fenced by the claim's Lease. All times are the database's.
type Store interface {
	// Claim leases up to request.Limit runnable jobs of request.Queue, oldest run time first.
	// Claiming releases a job's unique key.
	Claim(ctx context.Context, request *ClaimRequest) ([]*Claim, error)
	// ExtendLeases extends the given leases by duration and returns those still held.
	ExtendLeases(ctx context.Context, leases []Lease, duration time.Duration) ([]HeldLease, error)
	// Finish applies finish if its lease is still held and reports whether it did. A job
	// whose cancellation was requested goes to cancelled instead of back to pending.
	Finish(ctx context.Context, finish *Finish) (bool, error)
	// Rescue returns up to limit running jobs whose lease expired to pending, or to dead when
	// their attempts are used up; the lost attempt counts. It returns how many it moved.
	Rescue(ctx context.Context, limit int) (int, error)
	// Sweep deletes up to request.Limit finished jobs older than their retention and returns
	// how many it deleted.
	Sweep(ctx context.Context, request *SweepRequest) (int, error)
	// EnqueueDueSchedules registers the schedules that have no row yet, enqueues the job of
	// every due schedule no other worker is enqueuing, advances it and returns how many jobs
	// it enqueued.
	EnqueueDueSchedules(ctx context.Context, schedules []*Schedule) (int, error)
}

// ClaimRequest asks the store for runnable jobs of one queue.
type ClaimRequest struct {
	Queue  string
	Limit  int
	Lease  time.Duration
	Worker string // recorded with the job for diagnostics
}

// HeldLease is a lease ExtendLeases extended.
type HeldLease struct {
	JobID uuid.UUID
	// CancelRequested means the job was cancelled while it runs; the worker cancels the handler.
	CancelRequested bool
}

// SweepRequest asks the store to delete finished jobs.
type SweepRequest struct {
	RetainCompleted time.Duration // completed and cancelled jobs are kept this long after they finished
	RetainDead      time.Duration // dead jobs are kept this long after they died
	Limit           int
}

// QueueDepth is the backlog of one queue at one moment, read at scrape time for metrics
// (postgresadapter.Store.QueueDepths). It describes the shared table, not one worker.
type QueueDepth struct {
	Queue            string
	Pending          int           // runnable now
	Scheduled        int           // pending with a run time in the future: retries, snoozes, RunAt
	Running          int           // leased by any worker
	OldestPendingAge time.Duration // how long the oldest runnable job has waited; zero without one
}
