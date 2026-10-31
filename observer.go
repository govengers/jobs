package jobs

import (
	"context"
	"time"
)

// Observer receives what the worker would otherwise have to log (LIB-003). The worker calls
// it from several goroutines; implementations must be safe for concurrent use and fast. The
// interface may gain methods in minor versions, so an implementation outside this module should
// expect that.
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
	Err      error         // the handler's error joined with the finish write's error; nil when both are nil
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
// headers at enqueue and returns an error for a value it cannot carry, which makes the
// enqueue fail instead of losing the value; a context without the value is no error. Extract
// restores them before the handler runs, and must leave no value of the worker's own context
// in place where the headers carry none. A panic of Extract is a failed attempt; a panic of
// Inject is a panic of the enqueue that called it, which on the schedule path ends Run.
type Propagator interface {
	Inject(ctx context.Context, headers map[string]string) error
	Extract(ctx context.Context, headers map[string]string) context.Context
}
