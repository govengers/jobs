package jobs

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/govengers/jobs/internal/recovery"
)

// runnerDependencies is what a queue runner takes from its worker.
type runnerDependencies struct {
	store        Store
	observer     guardedObserver
	propagators  []Propagator
	workerName   string
	drainTimeout time.Duration
	handlerFor   func(kind string) registeredHandler
}

// queueRunner consumes one queue: it claims at most its free slots, runs every claimed job's
// handler in its own goroutine, extends the leases of the running jobs and drains on shutdown.
type queueRunner struct {
	runnerDependencies
	queue   string
	options QueueOptions
	group   *errgroup.Group // nil for RunDue, which runs attempts synchronously

	slots    chan struct{} // one element per running job; capacity is the concurrency
	released chan struct{} // signals a freed slot to the poll loop
	attempts sync.WaitGroup

	mutex sync.Mutex
	// running holds the attempts by their lease, so a stale heartbeat result or the cleanup of
	// an older attempt of the same job never touches a newer one.
	running map[Lease]*runningJob
}

type runningJob struct {
	cancel context.CancelCauseFunc
	// confirmedAt is when the lease was last known to be held: the start of the claim, then the
	// start of every extension that returned it.
	confirmedAt time.Time
}

func newQueueRunner(dependencies runnerDependencies, queue string, options QueueOptions, group *errgroup.Group) *queueRunner {
	return &queueRunner{
		runnerDependencies: dependencies,
		queue:              queue,
		options:            options,
		group:              group,
		slots:              make(chan struct{}, options.Concurrency),
		released:           make(chan struct{}, 1),
		running:            map[Lease]*runningJob{},
	}
}

// run polls until ctx is cancelled, then drains; the heartbeat runs until the last handler
// returned, so draining jobs keep their leases. The drain and the heartbeat's stop are
// deferred, so a panic while polling still ends them instead of leaving Run waiting.
func (r *queueRunner) run(ctx context.Context) (err error) {
	heartbeatContext, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	heartbeatStopped := make(chan error, 1)
	r.group.Go(func() error {
		heartbeatError := recovering(func() error { return r.heartbeat(heartbeatContext) })()
		heartbeatStopped <- heartbeatError
		return heartbeatError
	})
	defer func() {
		r.drain()
		stopHeartbeat()
		err = errors.Join(err, <-heartbeatStopped)
	}()
	r.poll(ctx)
	return nil
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
		if ctx.Err() != nil {
			return // select picks any ready case: a ready timer must not start a claim after the cancel (J10)
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
	claimStarted := time.Now()
	claims, err := r.claim(ctx, free)
	if err != nil {
		return false
	}
	for _, claim := range claims {
		r.slots <- struct{}{}
		r.start(ctx, claim, claimStarted)
	}
	return len(claims) == free
}

func (r *queueRunner) claim(ctx context.Context, limit int) ([]*Claim, error) {
	operationContext, cancel := detached(ctx)
	defer cancel()
	started := time.Now()
	claims, err := r.store.Claim(operationContext, &ClaimRequest{
		Queue: r.queue, Limit: limit, Lease: r.options.Lease, Worker: r.workerName,
	})
	r.observer.operationFinished(&Operation{
		Name: OperationClaim, Queue: r.queue, Count: len(claims), Duration: time.Since(started), Err: err,
	})
	return claims, err
}

// start runs the claim's attempt on a context of its own: shutdown does not cancel it, only a
// lost lease, a cancellation or the end of the drain does.
func (r *queueRunner) start(ctx context.Context, claim *Claim, claimStarted time.Time) {
	attemptContext, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	r.mutex.Lock()
	r.running[claim.Lease] = &runningJob{cancel: cancel, confirmedAt: claimStarted}
	r.mutex.Unlock()
	r.attempts.Add(1)
	r.group.Go(recovering(func() error {
		defer r.attempts.Done()
		defer r.freeSlot()
		defer cancel(nil)
		// A failed write is in the Outcome and the Operation the observer receives; the
		// rescuer returns the job once its lease expires.
		_ = r.attempt(attemptContext, claim)
		return nil
	}))
}

// attempt runs one claimed job to its stored outcome and returns the finish write's error.
func (r *queueRunner) attempt(ctx context.Context, claim *Claim) error {
	registered := r.handlerFor(claim.Kind)
	started := time.Now()
	handlerError := r.invoke(ctx, registered.handler, claim)
	duration := time.Since(started)
	finish, decided := decideRecovering(claim, handlerError, context.Cause(ctx), registered.backoff)
	result, writeError := r.writeFinish(ctx, finish, decided)
	r.forget(claim)
	r.observer.attemptFinished(&Outcome{Claim: claim, Result: result, Duration: duration, Err: errors.Join(handlerError, writeError)})
	return writeError
}

// invoke runs the handler with the propagated values restored; a panic is a failed attempt.
func (r *queueRunner) invoke(ctx context.Context, handler Handler, claim *Claim) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = recovery.Error("job "+claim.Lease.JobID.String(), recovered)
		}
	}()
	for _, propagator := range r.propagators {
		ctx = propagator.Extract(ctx, claim.Headers) //nolint:fatcontext // one layer per propagator, bounded by the options
	}
	return handler(ctx, claim)
}

// writeFinish applies the finish and returns the result as stored, with the write's error. A
// nil finish (a lost lease) writes nothing.
func (r *queueRunner) writeFinish(ctx context.Context, finish *Finish, decided Result) (Result, error) {
	if finish == nil {
		return decided, nil
	}
	operationContext, cancel := detached(ctx)
	defer cancel()
	started := time.Now()
	stored, err := r.store.Finish(operationContext, finish)
	written := 0
	if stored != "" {
		written = 1
	}
	r.observer.operationFinished(&Operation{
		Name: OperationFinish, Queue: r.queue, Count: written, Duration: time.Since(started), Err: err,
	})
	return storedResult(decided, stored, err), err
}

// forget ends the heartbeat of this attempt, and only of this attempt.
func (r *queueRunner) forget(claim *Claim) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.running, claim.Lease)
}

func (r *queueRunner) freeSlot() {
	<-r.slots
	select {
	case r.released <- struct{}{}:
	default: // the poll loop is already signalled
	}
}

// heartbeat extends the leases of the running jobs every Lease/3 and cancels the handlers of
// jobs whose lease is lost, whose cancellation was requested or whose lease could not be
// confirmed for longer than the lease.
func (r *queueRunner) heartbeat(ctx context.Context) error {
	ticker := time.NewTicker(r.heartbeatInterval())
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

func (r *queueRunner) heartbeatInterval() time.Duration {
	return r.options.Lease / heartbeatsPerLease
}

func (r *queueRunner) extendLeases(ctx context.Context) {
	leases := r.heldLeases()
	if len(leases) == 0 {
		return
	}
	// One interval at most: a hanging database must not delay the self-fence below.
	operationContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.heartbeatInterval())
	defer cancel()
	started := time.Now()
	held, err := r.store.ExtendLeases(operationContext, leases, r.options.Lease)
	r.observer.operationFinished(&Operation{
		Name: OperationExtendLeases, Queue: r.queue, Count: len(held), Duration: time.Since(started), Err: err,
	})
	if err != nil {
		r.cancelUnconfirmed(time.Now())
		return
	}
	r.reconcile(leases, held, started)
}

// heldLeases is the snapshot of the running attempts' leases.
func (r *queueRunner) heldLeases() []Lease {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	leases := make([]Lease, 0, len(r.running))
	for lease := range r.running {
		leases = append(leases, lease)
	}
	return leases
}

// reconcile applies an extension's result: a lease it did not return is lost, a returned one is
// confirmed as of confirmedAt and may carry a cancellation request.
func (r *queueRunner) reconcile(leases []Lease, held []HeldLease, confirmedAt time.Time) {
	heldByLease := make(map[Lease]HeldLease, len(held))
	for _, lease := range held {
		heldByLease[lease.Lease] = lease
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, lease := range leases {
		job, stillRunning := r.running[lease]
		if !stillRunning {
			continue // finished while the heartbeat ran
		}
		status, isHeld := heldByLease[lease]
		switch {
		case !isHeld:
			job.cancel(ErrLeaseLost)
		case status.CancelRequested:
			job.confirmedAt = confirmedAt
			job.cancel(ErrCancelled)
		default:
			job.confirmedAt = confirmedAt
		}
	}
}

// cancelUnconfirmed self-fences after failed extensions: an attempt whose lease was not
// confirmed for longer than the lease may already run on another replica, so its handler is
// cancelled with ErrLeaseLost and its result discarded.
func (r *queueRunner) cancelUnconfirmed(now time.Time) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, job := range r.running {
		if now.Sub(job.confirmedAt) > r.options.Lease {
			job.cancel(ErrLeaseLost)
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
	timeout := time.NewTimer(r.drainTimeout)
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
