package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/govengers/jobs/internal/recovery"
)

// Defaults of WorkerOptions and QueueOptions.
const (
	DefaultConcurrency      = 1
	DefaultLeaseDuration    = 30 * time.Second
	DefaultPollInterval     = time.Second
	DefaultDrainTimeout     = 20 * time.Second
	DefaultRescueInterval   = 30 * time.Second
	DefaultSweepInterval    = time.Hour
	DefaultScheduleInterval = 10 * time.Second
	DefaultRetainCompleted  = 7 * 24 * time.Hour
	DefaultRetainDead       = 30 * 24 * time.Hour
)

// Bounds of the background store operations.
const (
	operationTimeout   = 10 * time.Second
	rescueBatch        = 100
	sweepBatch         = 1000
	heartbeatsPerLease = 3
)

// MaximumRunsPerJob bounds how often RunDue runs one job before it reports a job that never
// finishes; it is far above the attempts a kind with a zero backoff spends.
const MaximumRunsPerJob = 100

var errWorkerRunning = errors.New("jobs: the worker is already running")

// Handler runs one attempt of a claimed job. Handle registers typed handlers; middleware wraps
// this untyped form.
type Handler func(ctx context.Context, claim *Claim) error

// Middleware wraps every attempt's handler; the first middleware of WorkerOptions.Middleware
// is the outermost.
type Middleware func(next Handler) Handler

// WorkerOptions configures a Worker. Zero durations take the defaults above.
type WorkerOptions struct {
	Store Store  // required
	Name  string // recorded with every claimed job; empty means the host name plus a random suffix
	// Queues are the queues this worker consumes, by name. A service that splits its workers
	// across Deployments sets them from configuration (JOB_QUEUES).
	Queues      map[string]QueueOptions
	Middleware  []Middleware
	Propagators []Propagator // restore the enqueuing context's values before the handler runs (J15)
	Observer    Observer     // nil reports nothing
	Schedules   []Schedule   // recurring jobs this worker enqueues once per period with every other worker
	// DrainTimeout is how long Run lets running handlers finish after its context is cancelled;
	// then their contexts are cancelled and their jobs released to another worker.
	DrainTimeout     time.Duration
	RescueInterval   time.Duration // how often expired leases of any worker are rescued
	SweepInterval    time.Duration // how often finished jobs past their retention are deleted
	ScheduleInterval time.Duration // how often due schedules are checked
	RetainCompleted  time.Duration // how long completed and cancelled jobs are kept
	RetainDead       time.Duration // how long dead jobs are kept
}

// QueueOptions configures one consumed queue.
type QueueOptions struct {
	Concurrency int // jobs of this queue running at once in this worker
	// LeaseDuration is how long a claim holds a job without a heartbeat; the worker extends the
	// lease every LeaseDuration/3 while the handler runs, so it does not depend on how long a job
	// takes. (A Lease is the claim itself: a job ID and a token.)
	LeaseDuration time.Duration
	PollInterval  time.Duration // how long an idle queue waits before it looks for jobs again
}

// Worker claims jobs of its queues, runs their handlers and writes the outcome, and keeps the
// shared table healthy: it rescues expired leases, deletes old jobs and enqueues schedules.
// Several workers, in one process or on many replicas, may share a database.
type Worker struct {
	store       Store
	name        string
	queues      map[string]QueueOptions
	middleware  []Middleware
	propagators []Propagator
	observer    guardedObserver
	schedules   []Schedule
	timing      timing

	mutex    sync.Mutex
	handlers map[string]registeredHandler // by kind
	running  bool
}

type registeredHandler struct {
	handler Handler
	backoff Backoff
}

// NewWorker validates options and returns a Worker. Register handlers with Handle, then call Run.
func NewWorker(options *WorkerOptions) (*Worker, error) {
	if options == nil || options.Store == nil {
		return nil, errors.New("jobs: WorkerOptions.Store is required")
	}
	queues, err := queuesWithDefaults(options.Queues)
	if err != nil {
		return nil, err
	}
	durations, err := timingWithDefaults(options)
	if err != nil {
		return nil, err
	}
	schedules, err := snapshotSchedules(options.Schedules)
	if err != nil {
		return nil, err
	}
	if err := validateCollaborators(options); err != nil {
		return nil, err
	}
	name, err := workerName(options.Name)
	if err != nil {
		return nil, err
	}
	observer := options.Observer
	if observer == nil {
		observer = silentObserver{}
	}
	return &Worker{
		store:       options.Store,
		name:        name,
		queues:      queues,
		middleware:  append([]Middleware(nil), options.Middleware...),
		propagators: append([]Propagator(nil), options.Propagators...),
		observer:    guardedObserver{observer: observer},
		schedules:   schedules,
		timing:      durations,
		handlers:    map[string]registeredHandler{},
	}, nil
}

// Handle registers handler for the jobs of kind. Register every handler before Run. A payload
// that does not decode into P is dead at once; unknown JSON fields are ignored, so a newer
// producer and an older worker can run side by side. It returns ErrInvalidKind for an invalid
// kind and an error for a second handler of the same kind.
func Handle[P any](worker *Worker, kind *Kind[P], handler func(ctx context.Context, job *Job[P]) error) error {
	if err := kind.validate(); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("jobs: nil handler for %s", kind.name)
	}
	decoding := func(ctx context.Context, claim *Claim) error {
		var payload P
		if err := json.Unmarshal(claim.Payload, &payload); err != nil {
			return Permanent(fmt.Errorf("decode payload of %s: %w", claim.Kind, err))
		}
		return handler(ctx, &Job[P]{
			ID: claim.Lease.JobID, Queue: claim.Queue, Kind: claim.Kind,
			Attempt: claim.Attempt, MaximumAttempts: claim.MaximumAttempts, Payload: payload,
		})
	}
	return worker.register(kind.name, registeredHandler{handler: decoding, backoff: kind.backoff})
}

func (w *Worker) register(kind string, handler registeredHandler) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.running {
		return errWorkerRunning
	}
	if _, exists := w.handlers[kind]; exists {
		return fmt.Errorf("jobs: a handler for %s is already registered", kind)
	}
	w.handlers[kind] = handler
	return nil
}

// Run consumes the worker's queues and runs the maintenance until ctx is cancelled, then
// drains (J10) and returns nil. It returns an error when a goroutine of the worker panicked;
// panics of handlers are failed attempts, not errors of Run. Run, RunDue and Handle fail while
// Run runs; after Run returned the worker may run again.
func (w *Worker) Run(ctx context.Context) error {
	handlers, err := w.start()
	if err != nil {
		return err
	}
	defer w.stop()
	group, groupContext := errgroup.WithContext(ctx)
	dependencies := w.runnerDependencies(handlers)
	for queue, options := range w.queues {
		runner := newQueueRunner(dependencies, queue, options, group)
		group.Go(recovering(func() error { return runner.run(groupContext) }))
	}
	group.Go(recovering(func() error { return w.maintain(groupContext) }))
	if len(w.schedules) > 0 {
		group.Go(recovering(func() error { return w.enqueueSchedules(groupContext) }))
	}
	return group.Wait()
}

// RunDue claims and runs every runnable job of the consumed queues, a batch of Concurrency
// jobs per queue at a time, one job after the other and without heartbeats, until none is
// left, and returns how many attempts it ran. A retry that waits for its backoff is not
// runnable. It is meant for tests (jobstest.Drain) and one-off commands. It returns an error
// while Run runs, when a claim or a finish write fails, and when one job runs
// MaximumRunsPerJob times (a handler that snoozes without a delay would never let it end).
func (w *Worker) RunDue(ctx context.Context) (int, error) {
	w.mutex.Lock()
	if w.running {
		w.mutex.Unlock()
		return 0, errWorkerRunning
	}
	handlers := w.wrappedHandlers()
	w.mutex.Unlock()
	dependencies := w.runnerDependencies(handlers)
	runsByJob := map[uuid.UUID]int{}
	attempts := 0
	for {
		ran, err := w.runDueBatches(ctx, dependencies, runsByJob)
		attempts += ran
		if err != nil || ran == 0 {
			return attempts, err
		}
	}
}

// runDueBatches runs one claimed batch of every queue synchronously.
func (w *Worker) runDueBatches(ctx context.Context, dependencies runnerDependencies, runsByJob map[uuid.UUID]int) (int, error) {
	ran := 0
	for queue, options := range w.queues {
		if err := ctx.Err(); err != nil {
			return ran, err // J38: no claim after the cancel
		}
		runner := newQueueRunner(dependencies, queue, options, nil)
		claims, err := runner.claim(ctx, options.Concurrency)
		if err != nil {
			return ran, fmt.Errorf("jobs: claim jobs of queue %s: %w", queue, err)
		}
		for _, claim := range claims {
			ran++
			if err := runner.attempt(ctx, claim); err != nil {
				return ran, fmt.Errorf("jobs: write the outcome of job %s: %w", claim.Lease.JobID, err)
			}
			runsByJob[claim.Lease.JobID]++
			if runsByJob[claim.Lease.JobID] >= MaximumRunsPerJob {
				return ran, fmt.Errorf("jobs: job %s of kind %s ran %d times in one RunDue; does its handler snooze without a delay?", claim.Lease.JobID, claim.Kind, MaximumRunsPerJob)
			}
		}
	}
	return ran, ctx.Err()
}

// runnerDependencies hands a queue runner what it needs from the worker.
func (w *Worker) runnerDependencies(handlers map[string]registeredHandler) runnerDependencies {
	return runnerDependencies{
		store:        w.store,
		observer:     w.observer,
		propagators:  w.propagators,
		workerName:   w.name,
		drainTimeout: w.timing.drainTimeout,
		handlerFor:   func(kind string) registeredHandler { return w.handlerFor(handlers, kind) },
	}
}

// start freezes the handlers and wraps them in the middleware.
func (w *Worker) start() (map[string]registeredHandler, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.running {
		return nil, errWorkerRunning
	}
	w.running = true
	return w.wrappedHandlers(), nil
}

// stop marks the worker as no longer running, once Run returns.
func (w *Worker) stop() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.running = false
}

// wrappedHandlers wraps every registered handler in the middleware; the caller holds the mutex.
func (w *Worker) wrappedHandlers() map[string]registeredHandler {
	wrapped := make(map[string]registeredHandler, len(w.handlers))
	for kind, registered := range w.handlers {
		wrapped[kind] = registeredHandler{handler: w.wrap(registered.handler), backoff: registered.backoff}
	}
	return wrapped
}

func (w *Worker) wrap(handler Handler) Handler {
	for index := len(w.middleware) - 1; index >= 0; index-- {
		handler = w.middleware[index](handler)
	}
	return handler
}

// handlerFor returns the kind's handler; a kind without one fails every attempt (J17), through
// the middleware like any other attempt.
func (w *Worker) handlerFor(handlers map[string]registeredHandler, kind string) registeredHandler {
	if registered, ok := handlers[kind]; ok {
		return registered
	}
	missing := func(context.Context, *Claim) error {
		return fmt.Errorf("no handler for kind %s in worker %s", kind, w.name)
	}
	return registeredHandler{handler: w.wrap(missing), backoff: DefaultBackoff}
}

// recovering turns a panic in work into an error, so a crashed goroutine stops Run instead of
// the process (CON-001).
func recovering(work func() error) func() error {
	return func() (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = recovery.Error("jobs: a worker goroutine", recovered)
			}
		}()
		return work()
	}
}

// detached returns a context for a store write that must finish even while ctx is cancelled.
func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), operationTimeout)
}

func workerName(name string) (string, error) {
	if name != "" {
		return name, nil
	}
	host, err := os.Hostname()
	if err != nil {
		host = "worker"
	}
	suffix := make([]byte, 3)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("jobs: random worker name suffix: %w", err)
	}
	return host + "-" + hex.EncodeToString(suffix), nil
}

type silentObserver struct{}

func (silentObserver) AttemptFinished(*Outcome)     {}
func (silentObserver) OperationFinished(*Operation) {}

// guardedObserver calls the Observer and drops a report whose call panicked: telemetry must
// not stop the worker or keep an attempt from being written. The observer that panicked
// cannot be told, so the report is lost.
type guardedObserver struct{ observer Observer }

func (g guardedObserver) attemptFinished(outcome *Outcome) {
	defer func() { _ = recover() }() // see the type's comment
	g.observer.AttemptFinished(outcome)
}

func (g guardedObserver) operationFinished(operation *Operation) {
	defer func() { _ = recover() }() // see the type's comment
	g.observer.OperationFinished(operation)
}
