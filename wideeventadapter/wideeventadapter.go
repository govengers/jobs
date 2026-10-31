// Package wideeventadapter records one wide event per job attempt (LOG-001) with
// github.com/govengers/wide-event-logger: the middleware begins the event, puts it into the
// handler's context so every layer below can add to it with wideevent.FromContext, and emits it
// when the attempt ends.
package wideeventadapter

import (
	"context"
	"errors"
	"fmt"

	wideevent "github.com/govengers/wide-event-logger"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/internal/recovery"
)

// The fields the middleware records, besides the event name job.<queue>, outcome and error.
const (
	KeyJobID              = "job.id"
	KeyJobQueue           = "job.queue"
	KeyJobKind            = "job.kind"
	KeyJobAttempt         = "job.attempt"
	KeyJobMaximumAttempts = "job.maximum_attempts"
	KeyJobPermanent       = "job.permanent"    // true when the handler marked its failure with jobs.Permanent
	KeyJobSnoozed         = "job.snoozed"      // the snooze delay when the handler returned jobs.Snooze
	KeyJobCancelCause     = "job.cancel_cause" // why the attempt's context was cancelled: lease lost, cancelled, shutdown
	KeyJobResult          = "job.result"       // what the store kept: the jobs.Result the Observer receives
	KeyJobFinishError     = "job.finish_error" // why the outcome could not be written
)

// Options configures the middleware.
type Options struct {
	Emitter wideevent.Emitter // required; the service's emitter, the same the HTTP middleware uses
}

// NewMiddleware returns the jobs.Middleware that wraps every attempt in a wide event named
// job.<queue>. Put it first in jobs.WorkerOptions.Middleware, so the event covers the other
// middleware. A panic of the handler is recorded as a failure and returned as the attempt's
// error, so the event is emitted and the worker retries the job like any failed attempt. A
// snooze is a success with job.snoozed set. In a worker the event is emitted once the outcome
// is stored, with job.result: an attempt whose outcome was not stored (fenced, finish_failed,
// lease_lost) is a failure even when its handler succeeded. Called outside a worker, the event
// is emitted when the handler returns.
func NewMiddleware(options *Options) (jobs.Middleware, error) {
	if options == nil || options.Emitter == nil {
		return nil, errors.New("wideeventadapter: Options.Emitter is required")
	}
	emitter := options.Emitter
	return func(next jobs.Handler) jobs.Handler {
		return func(ctx context.Context, claim *jobs.Claim) (err error) {
			we := begin(emitter, claim)
			defer func() {
				if recovered := recover(); recovered != nil {
					err = recovery.Error("job "+claim.Lease.JobID.String(), recovered)
				}
				recordHandler(ctx, we, err)
				stored := jobs.OnResult(ctx, func(result jobs.Result, writeError error) {
					recordStored(we, claim, result, writeError)
					we.Emit()
				})
				if !stored {
					we.Emit()
				}
			}()
			return next(wideevent.NewContext(ctx, we), claim)
		}
	}, nil
}

func begin(emitter wideevent.Emitter, claim *jobs.Claim) *wideevent.Event {
	we := wideevent.Begin(emitter, "job."+claim.Queue)
	we.UUID(KeyJobID, claim.Lease.JobID).
		String(KeyJobQueue, claim.Queue).
		String(KeyJobKind, claim.Kind).
		Int(KeyJobAttempt, claim.Attempt).
		Int(KeyJobMaximumAttempts, claim.MaximumAttempts)
	return we
}

// recordHandler records how the handler ended.
func recordHandler(ctx context.Context, we *wideevent.Event, err error) {
	if cause := context.Cause(ctx); cause != nil {
		we.Err(KeyJobCancelCause, cause)
	}
	delay, snoozed := jobs.SnoozeDelay(err)
	switch {
	case err == nil:
		we.Success()
	case snoozed:
		we.Duration(KeyJobSnoozed, delay).Success()
	default:
		we.Bool(KeyJobPermanent, jobs.IsPermanent(err)).Failure(err)
	}
}

// recordStored records what the store kept; an outcome that was not stored makes the attempt a
// failure, whatever the handler returned.
func recordStored(we *wideevent.Event, claim *jobs.Claim, result jobs.Result, writeError error) {
	we.String(KeyJobResult, string(result))
	if writeError != nil {
		we.Err(KeyJobFinishError, writeError)
	}
	notStored := result == jobs.ResultFenced || result == jobs.ResultFinishFailed || result == jobs.ResultLeaseLost
	if notStored && !we.HasError() {
		we.Failure(fmt.Errorf("job %s: the outcome was not stored (%s)", claim.Lease.JobID, result))
	}
}
