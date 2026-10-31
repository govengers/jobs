package jobs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
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
	// the claim already sets attempt 1; wait for the retry the worker wrote
	require.Eventually(t, func() bool {
		record := harness.record(retried)
		return record.Status == jobs.StatusPending && record.Attempt == 1
	}, waitFor, pollEvery)
	require.Contains(t, harness.record(retried).LastError, "panicked: bad backoff")
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
	worker := harness.worker(&jobs.WorkerOptions{Store: store, Queues: map[string]jobs.QueueOptions{"mail": {LeaseDuration: time.Minute, PollInterval: pollEvery}}})
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

// Finding 8: the heartbeat keeps a job that runs for several lease lengths, with a rescuer
// polling every 20 ms; a heartbeat slower than Lease/3 loses it.
func TestAJobLongerThanItsLeaseKeepsItsLease(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(nil)
	release := make(chan struct{})
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)
	require.Eventually(t, func() bool { return harness.observer.count(jobs.OperationExtendLeases) >= 4*3 }, waitFor, pollEvery, "four lease lengths of heartbeats")
	close(release)

	record := harness.eventuallyStatus(id, jobs.StatusCompleted)
	require.Equal(t, 1, record.Attempt)
	requireNoLostAttempt(t, harness.observer.results())
}

// Finding 8: a finish the store fences out is reported as fenced.
func TestAFencedFinishIsReportedAsFenced(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(&jobs.WorkerOptions{Store: &faultyStore{Store: harness.store, finish: func(context.Context, *jobs.Finish) (jobs.Status, error) {
		return "", nil // the lease was gone
	}}})
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	require.Eventually(t, func() bool { return len(harness.observer.outcomesOf(id)) >= 1 }, waitFor, pollEvery)
	require.Equal(t, jobs.ResultFenced, harness.observer.outcomesOf(id)[0].Result)
}

// Finding 8 (J4): a worker claims at most its free slots, so it never holds leases of jobs it
// cannot run yet.
func TestTheWorkerClaimsOnlyItsFreeSlots(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(&jobs.WorkerOptions{Name: "two-slots", Queues: map[string]jobs.QueueOptions{"mail": {Concurrency: 2, LeaseDuration: testLease, PollInterval: pollEvery}}})
	var running atomic.Int32
	release := make(chan struct{})
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		running.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}))
	for range 4 {
		harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	}

	harness.run(worker)
	require.Eventually(t, func() bool { return running.Load() == 2 }, waitFor, pollEvery)
	rescues := harness.observer.count(jobs.OperationRescue)
	require.Eventually(t, func() bool { return harness.observer.count(jobs.OperationRescue) >= rescues+10 }, waitFor, pollEvery, "many poll intervals pass while both slots are busy")

	var leased int
	require.NoError(t, harness.pool.QueryRow(context.Background(), "SELECT count(*) FROM govengers_jobs WHERE status = 'running' AND worker = 'two-slots'").Scan(&leased))
	require.Equal(t, 2, leased)
	close(release)
}

// Round 2, finding 2: a single failed extension cancels nothing while the lease was confirmed
// less than a lease ago; the grace period is measured from the last confirmation.
func TestASingleFailedExtensionCancelsNothing(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	var extensions atomic.Int32
	failed := make(chan struct{})
	store := &faultyStore{Store: harness.store, extendLeases: func(ctx context.Context, leases []jobs.Lease, duration time.Duration) ([]jobs.HeldLease, error) {
		if extensions.Add(1) == 4 { // one lease length of confirmed extensions at Lease/3, then one failure
			close(failed)
			return nil, errors.New("connection reset by peer")
		}
		return harness.store.ExtendLeases(ctx, leases, duration)
	}}
	worker := harness.worker(&jobs.WorkerOptions{Store: store})
	release := make(chan struct{})
	causes := make(chan error, 1)
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		causes <- context.Cause(ctx)
		return nil
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)
	<-failed
	require.Eventually(t, func() bool { return extensions.Load() >= 7 }, waitFor, pollEvery, "more than a lease length after the failure")
	close(release)

	require.NoError(t, <-causes, "the transient failure must not cancel the handler")
	record := harness.eventuallyStatus(id, jobs.StatusCompleted)
	require.Equal(t, 1, record.Attempt)
	requireNoLostAttempt(t, harness.observer.results())
}

// Round 2, finding 3: an extension that hangs is bounded by the heartbeat interval, so the
// self-fence still cancels within about a lease, not after the 10 s operation timeout.
func TestAHangingExtensionStillSelfFences(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(&jobs.WorkerOptions{Store: &faultyStore{Store: harness.store, extendLeases: func(ctx context.Context, _ []jobs.Lease, _ time.Duration) ([]jobs.HeldLease, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}})
	started, causes := make(chan struct{}, 1), make(chan error, 1)
	require.NoError(t, jobs.Handle(worker, kind, blockingHandler(started, causes)))
	harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	harness.run(worker)
	<-started

	select {
	case cause := <-causes:
		require.ErrorIs(t, cause, jobs.ErrLeaseLost)
	case <-time.After(2 * time.Second):
		t.Fatal("a hanging extension delayed the self-fence")
	}
}

// Round 2, finding 4: finished attempts are forgotten, so the heartbeat stops once no job runs.
func TestFinishedAttemptsAreNoLongerExtended(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	var nonEmptyExtensions atomic.Int32
	worker := harness.worker(&jobs.WorkerOptions{Store: &faultyStore{Store: harness.store, extendLeases: func(ctx context.Context, leases []jobs.Lease, duration time.Duration) ([]jobs.HeldLease, error) {
		if len(leases) > 0 {
			nonEmptyExtensions.Add(1)
		}
		return harness.store.ExtendLeases(ctx, leases, duration)
	}}})
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	ids := make([]uuid.UUID, 0, 5)
	for range 5 {
		ids = append(ids, harness.enqueue(context.Background(), kind.New(mailPayload{}, nil)))
	}

	harness.run(worker)
	for _, id := range ids {
		harness.eventuallyStatus(id, jobs.StatusCompleted)
	}
	afterCompletion := nonEmptyExtensions.Load()
	rescues := harness.observer.count(jobs.OperationRescue)
	require.Eventually(t, func() bool { return harness.observer.count(jobs.OperationRescue) >= rescues+15 }, waitFor, pollEvery, "several heartbeat intervals pass")

	require.Equal(t, afterCompletion, nonEmptyExtensions.Load(), "the heartbeat still extends finished attempts")
}

// Round 2, finding 10: RunDue on a cancelled context claims nothing, so no attempt is spent.
func TestRunDueOnACancelledContextClaimsNothing(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(nil)
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	attempts, err := worker.RunDue(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, attempts)
	record := harness.record(id)
	require.Equal(t, jobs.StatusPending, record.Status)
	require.Zero(t, record.Attempt)
}

// Round 2, finding 11: after Run returned, the worker is not "running" any more.
func TestAWorkerCanBeUsedAgainAfterRunReturned(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(nil)
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	stop := harness.run(worker)
	stop()

	require.NoError(t, jobs.Handle(worker, jobs.NewKind[mailPayload]("mail.later", &jobs.KindOptions{Queue: "mail"}), func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	attempts, err := worker.RunDue(context.Background())

	require.NoError(t, err)
	require.Equal(t, 1, attempts)
	require.Equal(t, jobs.StatusCompleted, harness.record(id).Status)
}
