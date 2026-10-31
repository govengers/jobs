package jobs

import (
	"context"
	"time"
)

// Store persists jobs for the Worker; postgresadapter.Store implements it. Every write after
// a claim is fenced by the claim's Lease. All times are the database's.
type Store interface {
	// Claim leases up to request.Limit runnable jobs of request.Queue, oldest run time first.
	// Claiming releases a job's unique key.
	Claim(ctx context.Context, request *ClaimRequest) ([]*Claim, error)
	// ExtendLeases extends the given leases by duration and returns those still held.
	ExtendLeases(ctx context.Context, leases []Lease, duration time.Duration) ([]HeldLease, error)
	// Finish applies finish if its lease is still held and returns the status the job now has:
	// a pending result of a job whose cancellation was requested is stored as cancelled. An
	// empty status means the lease was no longer held and nothing was written.
	Finish(ctx context.Context, finish *Finish) (Status, error)
	// Rescue returns up to limit running jobs whose lease expired to pending, or to dead when
	// their attempts are used up; the lost attempt counts. It returns how many it moved.
	Rescue(ctx context.Context, limit int) (int, error)
	// Sweep deletes up to request.Limit finished jobs older than their retention and returns
	// how many it deleted.
	Sweep(ctx context.Context, request *SweepRequest) (int, error)
	// EnqueueDueSchedule registers the schedule unless it has a row and, when it is due and no
	// other worker holds its row, enqueues its job and advances it, in one transaction. It
	// reports whether it enqueued. A row another worker holds is skipped, never waited for;
	// errors name the schedule.
	EnqueueDueSchedule(ctx context.Context, schedule Schedule) (bool, error)
}

// ClaimRequest asks the store for runnable jobs of one queue.
type ClaimRequest struct {
	Queue         string
	Limit         int
	LeaseDuration time.Duration
	Worker        string // recorded with the job for diagnostics
}

// HeldLease is a lease ExtendLeases extended.
type HeldLease struct {
	Lease Lease
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
