package jobs

import (
	"errors"
	"fmt"
	"time"
)

// Context causes of a handler's cancelled context; read them with context.Cause.
var (
	// ErrLeaseLost means another worker may run the job now: the heartbeat could not extend
	// the lease. Whatever the handler returns is discarded.
	ErrLeaseLost = errors.New("jobs: lease lost")
	// ErrCancelled means the job was cancelled while it ran.
	ErrCancelled = errors.New("jobs: job cancelled")
)

// errShutdown is the cause of a handler context cancelled after the drain timeout.
var errShutdown = errors.New("jobs: worker shut down")

// Permanent marks a handler error that no retry can fix: the job is dead at once.
func Permanent(err error) error {
	return &permanentError{err: err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Snooze returns the job to pending for delay without counting the attempt, for a
// precondition that is not met yet. A handler returns it instead of an error.
func Snooze(delay time.Duration) error {
	return &snoozeError{delay: delay}
}

type snoozeError struct{ delay time.Duration }

func (e *snoozeError) Error() string { return fmt.Sprintf("snoozed for %s", e.delay) }

// Result is how an attempt ended.
type Result string

// The results an Observer receives.
const (
	ResultCompleted Result = "completed"  // the handler returned nil
	ResultRetried   Result = "retried"    // the handler failed; the job runs again after its backoff
	ResultSnoozed   Result = "snoozed"    // the handler snoozed; the attempt did not count
	ResultDead      Result = "dead"       // a permanent failure, or the last attempt failed
	ResultCancelled Result = "cancelled"  // the job was cancelled while it ran
	ResultReleased  Result = "released"   // the worker shut down; the job runs again now, the attempt did not count
	ResultLeaseLost Result = "lease_lost" // the heartbeat lost the lease; the result was discarded
	ResultFenced    Result = "fenced"     // the result was not written: the lease had expired before the finish
)

// Finish is the write that ends an attempt. The store applies it only while the lease is
// still held.
type Finish struct {
	Lease  Lease
	Status Status        // pending, completed, dead or cancelled
	Delay  time.Duration // pending: the job is claimable again after Delay
	// ReturnAttempt gives the attempt back (snooze, release): it does not count towards MaxAttempts.
	ReturnAttempt bool
	Error         string // recorded as the job's last error; empty keeps the previous one
}

// decide maps how a handler ended to the finish write and the result to report. cause is the
// handler context's cause, nil while it was not cancelled. A nil Finish means nothing may be
// written: the lease is lost.
func decide(claim *Claim, handlerError, cause error, backoff Backoff) (*Finish, Result) {
	if errors.Is(cause, ErrLeaseLost) {
		return nil, ResultLeaseLost
	}
	finish := &Finish{Lease: claim.Lease}
	if handlerError == nil {
		finish.Status = StatusCompleted
		return finish, ResultCompleted
	}
	finish.Error = handlerError.Error()
	var snooze *snoozeError
	var permanent *permanentError
	switch {
	case errors.As(handlerError, &snooze):
		finish.Status, finish.Delay, finish.ReturnAttempt, finish.Error = StatusPending, snooze.delay, true, ""
		return finish, ResultSnoozed
	case errors.Is(cause, ErrCancelled):
		finish.Status = StatusCancelled
		return finish, ResultCancelled
	case errors.Is(cause, errShutdown):
		finish.Status, finish.ReturnAttempt = StatusPending, true
		return finish, ResultReleased
	case errors.As(handlerError, &permanent), claim.Attempt >= claim.MaxAttempts:
		finish.Status = StatusDead
		return finish, ResultDead
	default:
		finish.Status, finish.Delay = StatusPending, backoff(claim.Attempt)
		return finish, ResultRetried
	}
}
