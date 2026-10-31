package jobs

import (
	"context"
	"time"
)

// Observer receives what the worker would otherwise have to log (LIB-003). The worker calls
// it from several goroutines; implementations must be safe for concurrent use and fast.
type Observer interface {
	// AttemptFinished reports how one attempt ended.
	AttemptFinished(outcome *Outcome)
	// OperationFinished reports one background store operation.
	OperationFinished(operation *Operation)
}

// Outcome is one finished attempt.
type Outcome struct {
	Claim    *Claim
	Result   Result
	Duration time.Duration // the handler's run time
	Err      error         // the handler's error, or the finish write's error
}

// OperationName names a background store operation of the worker.
type OperationName string

// The operations an Observer receives.
const (
	OperationClaim        OperationName = "claim"
	OperationExtendLeases OperationName = "extend_leases"
	OperationFinish       OperationName = "finish"
	OperationRescue       OperationName = "rescue"
	OperationSweep        OperationName = "sweep"
	OperationSchedules    OperationName = "schedules"
)

// Operation is one background store operation.
type Operation struct {
	Name     OperationName
	Queue    string // empty for rescue, sweep and schedules, which cover every queue
	Count    int    // jobs claimed, leases held, jobs rescued, deleted or enqueued
	Duration time.Duration
	Err      error
}

// Propagator carries request-scoped values from the context that enqueues a job to the
// context its handler runs in: an identity, a trace. Inject writes them into the job's
// headers at enqueue; Extract restores them before the handler runs.
type Propagator interface {
	Inject(ctx context.Context, headers map[string]string)
	Extract(ctx context.Context, headers map[string]string) context.Context
}
