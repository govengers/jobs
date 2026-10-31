package jobs

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// queueRunner consumes one queue: it claims at most its free slots, runs every claimed job's
// handler in its own goroutine, extends the leases of the running jobs and drains on shutdown.
type queueRunner struct {
	worker   *Worker
	queue    string
	options  QueueOptions
	handlers map[string]registeredHandler
	group    *errgroup.Group

	slots    chan struct{} // one element per running job; capacity is the concurrency
	released chan struct{} // signals a freed slot to the poll loop
	attempts sync.WaitGroup

	mutex   sync.Mutex
	running map[uuid.UUID]*runningJob
}

type runningJob struct {
	claim  *Claim
	cancel context.CancelCauseFunc
}

func newQueueRunner(worker *Worker, queue string, options QueueOptions, handlers map[string]registeredHandler, group *errgroup.Group) *queueRunner {
	return &queueRunner{
		worker:   worker,
		queue:    queue,
		options:  options,
		handlers: handlers,
		group:    group,
		slots:    make(chan struct{}, options.Concurrency),
		released: make(chan struct{}, 1),
		running:  map[uuid.UUID]*runningJob{},
	}
}

// run polls until ctx is cancelled, then drains; the heartbeat runs until the last handler
// returned, so draining jobs keep their leases.
func (r *queueRunner) run(ctx context.Context) error {
	heartbeatContext, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	heartbeatStopped := make(chan error, 1)
	r.group.Go(func() error {
		err := recovering(func() error { return r.heartbeat(heartbeatContext) })()
		heartbeatStopped <- err
		return err
	})
	r.poll(ctx)
	r.drain()
	stopHeartbeat()
	return <-heartbeatStopped
}

func (r *queueRunner) poll(ctx context.Context) {
	wait := time.NewTimer(0)
	defer wait.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wait.C:
		case <-r.released:
		}
		if r.claimAndStart(ctx) {
			wait.Reset(0) // a full batch: more jobs may be waiting
		} else {
			wait.Reset(r.options.PollInterval)
		}
	}
}

// claimAndStart claims up to the free slots and starts the claimed jobs. It reports whether it
// filled every free slot.
func (r *queueRunner) claimAndStart(ctx context.Context) bool {
	free := cap(r.slots) - len(r.slots)
	if free == 0 {
		return false
	}
	claims, err := r.claim(ctx, free)
	if err != nil {
		return false
	}
	for _, claim := range claims {
		r.slots <- struct{}{}
		r.start(ctx, claim)
	}
	return len(claims) == free
}

func (r *queueRunner) claim(ctx context.Context, limit int) ([]*Claim, error) {
	operationContext, cancel := detached(ctx)
	defer cancel()
	started := time.Now()
	claims, err := r.worker.store.Claim(operationContext, &ClaimRequest{
		Queue: r.queue, Limit: limit, Lease: r.options.Lease, Worker: r.worker.name,
	})
	r.worker.observer.OperationFinished(&Operation{
		Name: OperationClaim, Queue: r.queue, Count: len(claims), Duration: time.Since(started), Err: err,
	})
	return claims, err
}

// start runs the claim's attempt on a context of its own: shutdown does not cancel it, only a
// lost lease, a cancellation or the end of the drain does.
func (r *queueRunner) start(ctx context.Context, claim *Claim) {
	attemptContext, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	r.mutex.Lock()
	r.running[claim.Lease.JobID] = &runningJob{claim: claim, cancel: cancel}
	r.mutex.Unlock()
	r.attempts.Add(1)
	r.group.Go(recovering(func() error {
		defer r.attempts.Done()
		defer r.freeSlot()
		defer cancel(nil)
		r.attempt(attemptContext, claim)
		return nil
	}))
}

func (r *queueRunner) attempt(ctx context.Context, claim *Claim) {
	registered := r.worker.handlerFor(r.handlers, claim.Kind)
	started := time.Now()
	handlerError := r.invoke(ctx, registered.handler, claim)
	duration := time.Since(started)
	finish, result := decide(claim, handlerError, context.Cause(ctx), registered.backoff)
	if finish != nil && r.writeFinish(ctx, finish) {
		result = ResultFenced
	}
	r.mutex.Lock()
	delete(r.running, claim.Lease.JobID)
	r.mutex.Unlock()
	r.worker.observer.AttemptFinished(&Outcome{Claim: claim, Result: result, Duration: duration, Err: handlerError})
}

// invoke runs the handler with the propagated values restored; a panic is a failed attempt.
func (r *queueRunner) invoke(ctx context.Context, handler Handler, claim *Claim) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("job %s panicked: %v\n%s", claim.Lease.JobID, recovered, debug.Stack())
		}
	}()
	for _, propagator := range r.worker.propagators {
		ctx = propagator.Extract(ctx, claim.Headers) //nolint:fatcontext // one layer per propagator, bounded by the options
	}
	return handler(ctx, claim)
}

// writeFinish applies the finish and reports whether the store fenced it out because the lease
// was no longer held. A failed write is reported to the Observer and leaves the job running until its
// lease expires and the rescuer returns it.
func (r *queueRunner) writeFinish(ctx context.Context, finish *Finish) (fenced bool) {
	operationContext, cancel := detached(ctx)
	defer cancel()
	started := time.Now()
	written, err := r.worker.store.Finish(operationContext, finish)
	r.worker.observer.OperationFinished(&Operation{
		Name: OperationFinish, Queue: r.queue, Count: boolToCount(written), Duration: time.Since(started), Err: err,
	})
	return err == nil && !written
}

func (r *queueRunner) freeSlot() {
	<-r.slots
	select {
	case r.released <- struct{}{}:
	default: // the poll loop is already signalled
	}
}

// heartbeat extends the leases of the running jobs every Lease/3 and cancels the handlers of
// jobs whose lease is lost or whose cancellation was requested.
func (r *queueRunner) heartbeat(ctx context.Context) error {
	ticker := time.NewTicker(r.options.Lease / heartbeatsPerLease)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.extendLeases(ctx)
		}
	}
}

func (r *queueRunner) extendLeases(ctx context.Context) {
	r.mutex.Lock()
	leases := make([]Lease, 0, len(r.running))
	for _, job := range r.running {
		leases = append(leases, job.claim.Lease)
	}
	r.mutex.Unlock()
	if len(leases) == 0 {
		return
	}
	operationContext, cancel := detached(ctx)
	defer cancel()
	started := time.Now()
	held, err := r.worker.store.ExtendLeases(operationContext, leases, r.options.Lease)
	r.worker.observer.OperationFinished(&Operation{
		Name: OperationExtendLeases, Queue: r.queue, Count: len(held), Duration: time.Since(started), Err: err,
	})
	if err != nil {
		return // unknown: keep the handlers running; a lease that expires meanwhile is fenced
	}
	r.cancelUnheld(leases, held)
}

func (r *queueRunner) cancelUnheld(leases []Lease, held []HeldLease) {
	heldByJob := make(map[uuid.UUID]HeldLease, len(held))
	for _, lease := range held {
		heldByJob[lease.JobID] = lease
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, lease := range leases {
		job, stillRunning := r.running[lease.JobID]
		if !stillRunning {
			continue // finished while the heartbeat ran
		}
		status, isHeld := heldByJob[lease.JobID]
		switch {
		case !isHeld:
			job.cancel(ErrLeaseLost)
		case status.CancelRequested:
			job.cancel(ErrCancelled)
		}
	}
}

// drain waits for the running handlers up to the drain timeout, then cancels them so their
// jobs are released, and waits until they returned.
func (r *queueRunner) drain() {
	finished := make(chan struct{})
	r.group.Go(recovering(func() error {
		r.attempts.Wait()
		close(finished)
		return nil
	}))
	timeout := time.NewTimer(r.worker.timing.drainTimeout)
	defer timeout.Stop()
	select {
	case <-finished:
		return
	case <-timeout.C:
	}
	r.mutex.Lock()
	for _, job := range r.running {
		job.cancel(errShutdown)
	}
	r.mutex.Unlock()
	<-finished
}

func boolToCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
