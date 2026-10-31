package jobs

import (
	"errors"
	"fmt"
	"time"

	"github.com/govengers/jobs/internal/recovery"
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

// Permanent marks a handler error that no retry can fix: the job is dead at once. Permanent(nil)
// is nil, so a handler that returns Permanent(err) on a branch where err is nil completes.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or an error it wraps, was marked with Permanent.
func IsPermanent(err error) bool {
	var permanent *permanentError
	return errors.As(err, &permanent)
}

type permanentError struct{ err error }

func (e *permanentError) Error() string {
	if e.err == nil {
		return "permanent"
	}
	return "permanent: " + e.err.Error()
}
func (e *permanentError) Unwrap() error { return e.err }

// Snooze returns the job to pending for delay without counting the attempt, for a
// precondition that is not met yet. A handler returns it instead of an error.
func Snooze(delay time.Duration) error {
	return &snoozeError{delay: delay}
}

// SnoozeDelay reports whether err, or an error it wraps, is a Snooze, and its delay. Middleware
// uses it to tell a snooze from a failure.
func SnoozeDelay(err error) (time.Duration, bool) {
	var snooze *snoozeError
	if errors.As(err, &snooze) {
		return snooze.delay, true
	}
	return 0, false
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
	// ResultFinishFailed means the finish write failed: nothing was stored, the job stays running
	// until its lease expires and the rescuer returns it. Outcome.Err carries the write's error.
	ResultFinishFailed Result = "finish_failed"
)

// Finish is the write that ends an attempt. The store applies it only while the lease is
// still held.
type Finish struct {
	Lease  Lease
	Status Status        // pending, completed, dead or cancelled
	Delay  time.Duration // pending: the job is claimable again after Delay
	// ReturnAttempt gives the attempt back (snooze, release): it does not count towards MaximumAttempts.
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
	snoozeDelay, snoozed := SnoozeDelay(handlerError)
	switch {
	case snoozed:
		finish.Status, finish.Delay, finish.ReturnAttempt, finish.Error = StatusPending, snoozeDelay, true, ""
		return finish, ResultSnoozed
	case errors.Is(cause, ErrCancelled):
		finish.Status = StatusCancelled
		return finish, ResultCancelled
	case errors.Is(cause, errShutdown):
		finish.Status, finish.ReturnAttempt = StatusPending, true
		return finish, ResultReleased
	case IsPermanent(handlerError), claim.Attempt >= claim.MaximumAttempts:
		finish.Status = StatusDead
		return finish, ResultDead
	default:
		finish.Status, finish.Delay = StatusPending, max(0, backoff(claim.Attempt))
		return finish, ResultRetried
	}
}

// decideRecovering is decide that survives a panic of the kind's Backoff or of the handler
// error's Error method: the attempt is then a failure retried with DefaultBackoff, so a bug in
// a kind never stops the worker.
func decideRecovering(claim *Claim, handlerError, cause error, backoff Backoff) (finish *Finish, result Result) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicError := recovery.Error("deciding the outcome of job "+claim.Lease.JobID.String(), recovered)
			finish, result = decide(claim, panicError, cause, DefaultBackoff)
		}
	}()
	return decide(claim, handlerError, cause, backoff)
}

// storedResult maps the decided result to what the store wrote: nothing when the write failed,
// nothing when the lease was gone, cancelled when a cancellation request turned a pending
// result into cancelled.
func storedResult(decided Result, stored Status, writeError error) Result {
	switch {
	case writeError != nil:
		return ResultFinishFailed
	case stored == "":
		return ResultFenced
	case stored == StatusCancelled:
		return ResultCancelled
	default:
		return decided
	}
}
