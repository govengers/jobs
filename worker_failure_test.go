package jobs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

// faultyStore is the Postgres store with single operations replaced by a test.
type faultyStore struct {
	*postgresadapter.Store
	claim        func(ctx context.Context, request *jobs.ClaimRequest) ([]*jobs.Claim, error)
	extendLeases func(ctx context.Context, leases []jobs.Lease, duration time.Duration) ([]jobs.HeldLease, error)
	finish       func(ctx context.Context, finish *jobs.Finish) (jobs.Status, error)
}

func (s *faultyStore) Claim(ctx context.Context, request *jobs.ClaimRequest) ([]*jobs.Claim, error) {
	if s.claim != nil {
		return s.claim(ctx, request)
	}
	return s.Store.Claim(ctx, request)
}

func (s *faultyStore) ExtendLeases(ctx context.Context, leases []jobs.Lease, duration time.Duration) ([]jobs.HeldLease, error) {
	if s.extendLeases != nil {
		return s.extendLeases(ctx, leases, duration)
	}
	return s.Store.ExtendLeases(ctx, leases, duration)
}

func (s *faultyStore) Finish(ctx context.Context, finish *jobs.Finish) (jobs.Status, error) {
	if s.finish != nil {
		return s.finish(ctx, finish)
	}
	return s.Store.Finish(ctx, finish)
}

// outcomes returns the observer's outcomes of one job.
func (o *recordingObserver) outcomesOf(id any) []jobs.Outcome {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	var outcomes []jobs.Outcome
	for _, outcome := range o.outcomes {
		if outcome.Claim.Lease.JobID == id {
			outcomes = append(outcomes, outcome)
		}
	}
	return outcomes
}

func requireNoLostAttempt(t *testing.T, results []jobs.Result) {
	t.Helper()
	require.NotContains(t, results, jobs.ResultLeaseLost)
	require.NotContains(t, results, jobs.ResultFenced)
}

// Finding 1: a handler-side bug is a failed attempt, never an error of Run.
func TestHandlerSideBugsDoNotStopTheWorker(t *testing.T) {
	harness := newHarness(t)
	worker := harness.worker(nil)
	permanentOfNil := jobs.NewKind[mailPayload]("mail.permanent_nil", &jobs.KindOptions{Queue: "mail"})
	panickingBackoff := jobs.NewKind[mailPayload]("mail.panicking_backoff", &jobs.KindOptions{Queue: "mail", Backoff: func(int) time.Duration { panic("bad backoff") }})
	plain := mailKind(nil)
	require.NoError(t, jobs.Handle(worker, permanentOfNil, func(context.Context, *jobs.Job[mailPayload]) error {
		var err error
		return jobs.Permanent(err)
	}))
	require.NoError(t, jobs.Handle(worker, panickingBackoff, func(context.Context, *jobs.Job[mailPayload]) error {
		return errors.New("smtp down")
	}))
	require.NoError(t, jobs.Handle(worker, plain, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	completed := harness.enqueue(context.Background(), permanentOfNil.New(mailPayload{}, nil))
	retried := harness.enqueue(context.Background(), panickingBackoff.New(mailPayload{}, nil))

	harness.run(worker)

	harness.eventuallyStatus(completed, jobs.StatusCompleted)
	require.Eventually(t, func() bool { return harness.record(retried).Attempt == 1 }, waitFor, pollEvery)
	record := harness.record(retried)
	require.Equal(t, jobs.StatusPending, record.Status)
	require.Contains(t, record.LastError, "panicked: bad backoff")
	later := harness.enqueue(context.Background(), plain.New(mailPayload{}, nil))
	harness.eventuallyStatus(later, jobs.StatusCompleted) // the worker still runs; the cleanup asserts Run returns nil
}

type panickingObserver struct{}

func (panickingObserver) AttemptFinished(*jobs.Outcome)     { panic("observer bug") }
func (panickingObserver) OperationFinished(*jobs.Operation) { panic("observer bug") }

func TestAPanickingObserverDoesNotStopTheWorker(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(&jobs.WorkerOptions{Observer: panickingObserver{}})
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	first := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	harness.eventuallyStatus(first, jobs.StatusCompleted)
	second := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	harness.eventuallyStatus(second, jobs.StatusCompleted)
}

// Finding 3: the outcome reports what was stored.
func TestAFailedFinishIsReportedAsFinishFailed(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	writeError := errors.New("connection reset by peer")
	var failed atomic.Bool
	store := &faultyStore{Store: harness.store, finish: func(context.Context, *jobs.Finish) (jobs.Status, error) {
		failed.Store(true)
		return "", writeError
	}}
	worker := harness.worker(&jobs.WorkerOptions{Store: store, Queues: map[string]jobs.QueueOptions{"mail": {Lease: time.Minute, PollInterval: pollEvery}}})
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	require.Eventually(t, func() bool { return len(harness.observer.outcomesOf(id)) == 1 }, waitFor, pollEvery)
	outcome := harness.observer.outcomesOf(id)[0]
	require.Equal(t, jobs.ResultFinishFailed, outcome.Result)
	require.ErrorIs(t, outcome.Err, writeError)
	require.Equal(t, jobs.StatusRunning, harness.record(id).Status, "nothing was written")
}

func TestACancelBetweenHandlerAndFinishIsReportedAsCancelled(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(&jobs.KindOptions{Backoff: noBackoff})
	worker := harness.worker(nil)
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, job *jobs.Job[mailPayload]) error {
		if err := harness.store.Cancel(context.WithoutCancel(ctx), job.ID); err != nil {
			return err
		}
		return errors.New("failed after the cancel request")
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	harness.eventuallyStatus(id, jobs.StatusCancelled)
	require.Eventually(t, func() bool { return len(harness.observer.outcomesOf(id)) == 1 }, waitFor, pollEvery)
	require.Equal(t, jobs.ResultCancelled, harness.observer.outcomesOf(id)[0].Result)
}

// Finding 4, cancel path: a heartbeat result about an older attempt must not cancel a newer
// attempt of the same job.
func TestAStaleHeartbeatResultDoesNotCancelANewerAttempt(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	snapshotTaken, releaseHeartbeat, secondStarted, laterHeartbeat := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var extensions atomic.Int32
	var laterOnce sync.Once
	store := &faultyStore{Store: harness.store, extendLeases: func(ctx context.Context, leases []jobs.Lease, duration time.Duration) ([]jobs.HeldLease, error) {
		if extensions.Add(1) == 1 {
			close(snapshotTaken)
			<-releaseHeartbeat // the first attempt finishes and the job is claimed again meanwhile
		} else {
			laterOnce.Do(func() { close(laterHeartbeat) })
		}
		return harness.store.ExtendLeases(ctx, leases, duration)
	}}
	worker := harness.worker(&jobs.WorkerOptions{Store: store})
	var runs atomic.Int32
	causes := make(chan error, 1)
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		if runs.Add(1) == 1 {
			<-snapshotTaken
			return jobs.Snooze(0)
		}
		close(secondStarted)
		select {
		case <-laterHeartbeat:
		case <-ctx.Done():
		}
		causes <- context.Cause(ctx)
		return nil
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)
	<-secondStarted
	close(releaseHeartbeat)

	require.NoError(t, <-causes, "the newer attempt keeps running")
	harness.eventuallyStatus(id, jobs.StatusCompleted)
	requireNoLostAttempt(t, harness.observer.results())
}

// Finding 4, cleanup path: the cleanup of an older attempt must not stop the heartbeat of a
// newer attempt of the same job.
func TestTheCleanupOfAnOlderAttemptKeepsTheNewerOneAlive(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	secondStarted := make(chan struct{})
	extended := make(chan struct{}, 100)
	var finishes atomic.Int32
	store := &faultyStore{
		Store: harness.store,
		finish: func(ctx context.Context, finish *jobs.Finish) (jobs.Status, error) {
			stored, err := harness.store.Finish(ctx, finish)
			if finishes.Add(1) == 1 {
				<-secondStarted // the job is claimed again before the first attempt cleans up
			}
			return stored, err
		},
		extendLeases: func(ctx context.Context, leases []jobs.Lease, duration time.Duration) ([]jobs.HeldLease, error) {
			held, err := harness.store.ExtendLeases(ctx, leases, duration)
			extended <- struct{}{}
			return held, err
		},
	}
	worker := harness.worker(&jobs.WorkerOptions{Store: store})
	var runs atomic.Int32
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		if runs.Add(1) == 1 {
			return jobs.Snooze(0)
		}
		close(secondStarted)
		for range 2 * 3 { // two lease lengths of heartbeats, each Lease/3
			select {
			case <-extended:
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitFor):
				return errors.New("the newer attempt is no longer extended")
			}
		}
		return nil
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	record := harness.eventuallyStatus(id, jobs.StatusCompleted)
	require.Equal(t, 1, record.Attempt)
	requireNoLostAttempt(t, harness.observer.results())
}

// Finding 5: a panic while polling ends Run with the error instead of hanging it.
func TestAPanickingClaimEndsRunWithAnError(t *testing.T) {
	harness := newHarness(t)
	store := &faultyStore{Store: harness.store, claim: func(context.Context, *jobs.ClaimRequest) ([]*jobs.Claim, error) {
		panic("claim bug")
	}}
	worker := harness.worker(&jobs.WorkerOptions{Store: store, DrainTimeout: 50 * time.Millisecond})
	returned := make(chan error, 1)

	go func() { returned <- worker.Run(context.Background()) }()

	select {
	case err := <-returned:
		require.ErrorContains(t, err, "panicked: claim bug")
	case <-time.After(waitFor):
		t.Fatal("Run hangs after a panic while polling")
	}
}

// Finding 6: a worker that cannot confirm a lease for longer than the lease cancels the handler,
// because another replica may already run the job.
func TestAWorkerThatCannotConfirmItsLeaseCancelsItsHandler(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	partitioned := harness.worker(&jobs.WorkerOptions{Store: &faultyStore{Store: harness.store, extendLeases: func(context.Context, []jobs.Lease, time.Duration) ([]jobs.HeldLease, error) {
		return nil, errors.New("network partition")
	}}})
	started, causes := make(chan struct{}, 1), make(chan error, 1)
	require.NoError(t, jobs.Handle(partitioned, kind, blockingHandler(started, causes)))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	harness.run(partitioned)
	<-started

	healthy := harness.worker(nil)
	require.NoError(t, jobs.Handle(healthy, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	harness.run(healthy)

	select {
	case cause := <-causes:
		require.ErrorIs(t, cause, jobs.ErrLeaseLost)
	case <-time.After(waitFor):
		t.Fatal("the partitioned worker keeps running a job it can no longer confirm")
	}
	record := harness.eventuallyStatus(id, jobs.StatusCompleted)
	require.GreaterOrEqual(t, record.Attempt, 2, "the job ran again after the rescue")
}

// Finding 14: RunDue, and so jobstest.Drain, fails when an outcome cannot be written.
func TestRunDueReturnsAFailedFinish(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(&jobs.WorkerOptions{Store: &faultyStore{Store: harness.store, finish: func(context.Context, *jobs.Finish) (jobs.Status, error) {
		return "", errors.New("connection reset by peer")
	}}})
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	_, err := worker.RunDue(context.Background())

	require.ErrorContains(t, err, "connection reset by peer")
}

// Finding 21: RunDue stops a job that never finishes instead of looping until the test times out.
func TestRunDueStopsAJobThatSnoozesWithoutADelay(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(nil)
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return jobs.Snooze(0) }))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	attempts, err := worker.RunDue(context.Background())

	require.ErrorContains(t, err, id.String())
	require.Equal(t, jobs.MaximumRunsPerJob, attempts)
}
