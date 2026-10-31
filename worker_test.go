package jobs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/internal/postgrestest"
	"github.com/govengers/jobs/postgresadapter"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

type mailPayload struct {
	Recipient string `json:"recipient"`
}

const (
	testLease = 300 * time.Millisecond
	waitFor   = 10 * time.Second
	pollEvery = 10 * time.Millisecond
)

var noBackoff = func(int) time.Duration { return 0 }

func mailKind(options *jobs.KindOptions) *jobs.Kind[mailPayload] {
	if options == nil {
		options = &jobs.KindOptions{}
	}
	options.Queue = "mail"
	return jobs.NewKind[mailPayload]("mail.send", options)
}

type testHarness struct {
	t        *testing.T
	pool     *pgxpool.Pool
	store    *postgresadapter.Store
	observer *recordingObserver
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	pool := postgrestest.New(t)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, Propagators: []jobs.Propagator{tenantPropagator{}}})
	require.NoError(t, err)
	return &testHarness{t: t, pool: pool, store: store, observer: &recordingObserver{}}
}

// worker returns a worker on the harness's store with short intervals; options may override them.
func (harness *testHarness) worker(options *jobs.WorkerOptions) *jobs.Worker {
	harness.t.Helper()
	if options == nil {
		options = &jobs.WorkerOptions{}
	}
	if options.Store == nil {
		options.Store = harness.store
	}
	if options.Observer == nil {
		options.Observer = harness.observer
	}
	if options.Queues == nil {
		options.Queues = map[string]jobs.QueueOptions{"mail": {Concurrency: 2, LeaseDuration: testLease, PollInterval: pollEvery}}
	}
	options.Propagators = []jobs.Propagator{tenantPropagator{}}
	options.RescueInterval = orDefault(options.RescueInterval, 20*time.Millisecond)
	// A test's cleanup waits waitFor for Run; a handler that re-runs a rescued job must not
	// hold the drain longer than that.
	options.DrainTimeout = orDefault(options.DrainTimeout, 500*time.Millisecond)
	options.ScheduleInterval = orDefault(options.ScheduleInterval, 20*time.Millisecond)
	worker, err := jobs.NewWorker(options)
	require.NoError(harness.t, err)
	return worker
}

func orDefault(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return value
}

// run starts the worker and returns a stop function that cancels it and waits for Run, which
// the test cleanup also does.
func (harness *testHarness) run(worker *jobs.Worker) func() {
	harness.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() { returned <- worker.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-returned:
				require.NoError(harness.t, err)
			case <-time.After(waitFor):
				harness.t.Error("the worker did not stop")
			}
		})
	}
	harness.t.Cleanup(stop)
	return stop
}

func (harness *testHarness) enqueue(ctx context.Context, job *jobs.NewJob) uuid.UUID {
	harness.t.Helper()
	ids, err := harness.store.Enqueue(ctx, harness.pool, job)
	require.NoError(harness.t, err)
	return ids[0]
}

func (harness *testHarness) record(id uuid.UUID) *postgresadapter.Record {
	harness.t.Helper()
	record, err := harness.store.Get(context.Background(), id)
	require.NoError(harness.t, err)
	return record
}

func (harness *testHarness) eventuallyStatus(id uuid.UUID, status jobs.Status) *postgresadapter.Record {
	harness.t.Helper()
	require.Eventually(harness.t, func() bool { return harness.record(id).Status == status }, waitFor, pollEvery, "job %s never became %s", id, status)
	return harness.record(id)
}

type recordingObserver struct {
	mutex      sync.Mutex
	outcomes   []jobs.Outcome
	operations map[jobs.OperationName]int
}

func (o *recordingObserver) AttemptFinished(outcome *jobs.Outcome) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.outcomes = append(o.outcomes, *outcome)
}

func (o *recordingObserver) OperationFinished(operation *jobs.Operation) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.operations == nil {
		o.operations = map[jobs.OperationName]int{}
	}
	o.operations[operation.Name]++
}

func (o *recordingObserver) results() []jobs.Result {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	results := make([]jobs.Result, 0, len(o.outcomes))
	for _, outcome := range o.outcomes {
		results = append(results, outcome.Result)
	}
	return results
}

func (o *recordingObserver) count(name jobs.OperationName) int {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.operations[name]
}

type tenantKey struct{}

type tenantPropagator struct{}

func (tenantPropagator) Inject(ctx context.Context, headers map[string]string) error {
	if tenant, ok := ctx.Value(tenantKey{}).(string); ok {
		headers["tenant"] = tenant
	}
	return nil
}

func (tenantPropagator) Extract(ctx context.Context, headers map[string]string) context.Context {
	return context.WithValue(ctx, tenantKey{}, headers["tenant"])
}

func TestWorkerRunsTheHandlerWithPayloadContextAndMiddleware(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	var middlewareSaw atomic.Value
	worker := harness.worker(&jobs.WorkerOptions{Middleware: []jobs.Middleware{func(next jobs.Handler) jobs.Handler {
		return func(ctx context.Context, claim *jobs.Claim) error {
			middlewareSaw.Store(claim.Kind)
			return next(ctx, claim)
		}
	}}})
	received := make(chan *jobs.Job[mailPayload], 1)
	tenants := make(chan string, 1)
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, job *jobs.Job[mailPayload]) error {
		tenants <- ctx.Value(tenantKey{}).(string)
		received <- job
		return nil
	}))
	ctx := context.WithValue(context.Background(), tenantKey{}, "tenant-7")
	id := harness.enqueue(ctx, kind.New(mailPayload{Recipient: "ada@example.com"}, nil))

	harness.run(worker)

	harness.eventuallyStatus(id, jobs.StatusCompleted)
	job := <-received
	require.Equal(t, id, job.ID)
	require.Equal(t, "ada@example.com", job.Payload.Recipient)
	require.Equal(t, 1, job.Attempt)
	require.Equal(t, jobs.DefaultMaximumAttempts, job.MaximumAttempts)
	require.Equal(t, "mail", job.Queue)
	require.Equal(t, "tenant-7", <-tenants)
	require.Equal(t, "mail.send", middlewareSaw.Load())
	require.Eventually(t, func() bool { return len(harness.observer.results()) == 1 }, waitFor, pollEvery)
	require.Equal(t, []jobs.Result{jobs.ResultCompleted}, harness.observer.results())
}

func TestTwoWorkersNeverRunTheSameJobAtOnce(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	const jobCount = 40
	var mutex sync.Mutex
	runs := map[uuid.UUID]int{}
	concurrent := map[uuid.UUID]int{}
	handler := func(_ context.Context, job *jobs.Job[mailPayload]) error {
		mutex.Lock()
		runs[job.ID]++
		concurrent[job.ID]++
		overlap := concurrent[job.ID] > 1
		mutex.Unlock()
		defer func() {
			mutex.Lock()
			concurrent[job.ID]--
			mutex.Unlock()
		}()
		if overlap {
			return errors.New("ran twice at once")
		}
		return nil
	}
	ids := make([]uuid.UUID, 0, jobCount)
	for range jobCount {
		ids = append(ids, harness.enqueue(context.Background(), kind.New(mailPayload{}, nil)))
	}
	for range 2 {
		worker := harness.worker(&jobs.WorkerOptions{Queues: map[string]jobs.QueueOptions{"mail": {Concurrency: 4, LeaseDuration: testLease, PollInterval: pollEvery}}})
		require.NoError(t, jobs.Handle(worker, kind, handler))
		harness.run(worker)
	}

	for _, id := range ids {
		harness.eventuallyStatus(id, jobs.StatusCompleted)
	}
	mutex.Lock()
	defer mutex.Unlock()
	for id, count := range runs {
		require.Equal(t, 1, count, "job %s ran %d times", id, count)
	}
}

func TestPermanentIsDeadAtOnce(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(&jobs.KindOptions{MaximumAttempts: 5})
	worker := harness.worker(nil)
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error {
		return jobs.Permanent(errors.New("mailbox does not exist"))
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	record := harness.eventuallyStatus(id, jobs.StatusDead)
	require.Equal(t, 1, record.Attempt)
	require.Equal(t, "permanent: mailbox does not exist", record.LastError)
}

func TestSnoozeDoesNotCountTheAttempt(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(&jobs.KindOptions{MaximumAttempts: 1})
	worker := harness.worker(nil)
	var calls, lastAttempt atomic.Int32
	require.NoError(t, jobs.Handle(worker, kind, func(_ context.Context, job *jobs.Job[mailPayload]) error {
		lastAttempt.Store(int32(job.Attempt)) //nolint:gosec // G115: attempts stay tiny in this test
		if calls.Add(1) < 3 {
			return jobs.Snooze(0)
		}
		return nil
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	record := harness.eventuallyStatus(id, jobs.StatusCompleted)
	require.Equal(t, 1, record.Attempt, "two snoozes and one run on a kind with a single attempt")
	require.Equal(t, int32(3), calls.Load())
	require.Equal(t, int32(1), lastAttempt.Load(), "every run is the first attempt")
}

func TestRetryWaitsForTheBackoff(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(&jobs.KindOptions{Backoff: func(int) time.Duration { return time.Hour }})
	worker := harness.worker(nil)
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error {
		return errors.New("smtp unavailable")
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	require.Eventually(t, func() bool {
		record := harness.record(id)
		return record.Status == jobs.StatusPending && record.Attempt == 1
	}, waitFor, pollEvery)
	require.Equal(t, "smtp unavailable", harness.record(id).LastError)
	var secondsUntilRun float64
	require.NoError(t, harness.pool.QueryRow(context.Background(), "SELECT EXTRACT(EPOCH FROM run_at - now())::float8 FROM govengers_jobs WHERE id = $1", id).Scan(&secondsUntilRun))
	require.InDelta(t, time.Hour.Seconds(), secondsUntilRun, 60, "run_at is an hour after the database's now() (TST-005)")
}

func TestDeadAfterTheLastAttempt(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(&jobs.KindOptions{MaximumAttempts: 3, Backoff: noBackoff})
	worker := harness.worker(nil)
	var calls atomic.Int32
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error {
		calls.Add(1)
		panic("template missing")
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	record := harness.eventuallyStatus(id, jobs.StatusDead)
	require.Equal(t, 3, record.Attempt)
	require.Equal(t, int32(3), calls.Load())
	require.Contains(t, record.LastError, "panicked: template missing")
	require.Eventually(t, func() bool { return len(harness.observer.results()) == 3 }, waitFor, pollEvery)
	require.Equal(t, []jobs.Result{jobs.ResultRetried, jobs.ResultRetried, jobs.ResultDead}, harness.observer.results())
}

// blockingHandler blocks until its context is done and reports the context's cause. It reports
// only the first run: a rescued job that runs again must not block on a full channel, it would
// ignore its context and hold the drain.
func blockingHandler(started chan<- struct{}, causes chan<- error) func(context.Context, *jobs.Job[mailPayload]) error {
	return func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		select {
		case causes <- context.Cause(ctx):
		default:
		}
		return ctx.Err()
	}
}

func TestLostLeaseCancelsTheHandler(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(nil)
	started, causes := make(chan struct{}, 1), make(chan error, 1)
	require.NoError(t, jobs.Handle(worker, kind, blockingHandler(started, causes)))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	harness.run(worker)
	<-started

	_, err := harness.pool.Exec(context.Background(), "UPDATE govengers_jobs SET lease_token = gen_random_uuid() WHERE id = $1", id)
	require.NoError(t, err)

	require.ErrorIs(t, <-causes, jobs.ErrLeaseLost)
	require.Eventually(t, func() bool { return len(harness.observer.results()) == 1 }, waitFor, pollEvery)
	require.Equal(t, []jobs.Result{jobs.ResultLeaseLost}, harness.observer.results())
	require.Equal(t, jobs.StatusRunning, harness.record(id).Status, "the worker writes nothing for a lost lease")
}

func TestCancelARunningJob(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(nil)
	started, causes := make(chan struct{}, 1), make(chan error, 1)
	require.NoError(t, jobs.Handle(worker, kind, blockingHandler(started, causes)))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	harness.run(worker)
	<-started

	require.NoError(t, harness.store.Cancel(context.Background(), id))

	require.ErrorIs(t, <-causes, jobs.ErrCancelled)
	harness.eventuallyStatus(id, jobs.StatusCancelled)
}

func TestShutdownDrainsThenReleasesTheJob(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(&jobs.WorkerOptions{DrainTimeout: 50 * time.Millisecond})
	started, causes := make(chan struct{}, 1), make(chan error, 1)
	require.NoError(t, jobs.Handle(worker, kind, blockingHandler(started, causes)))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	stop := harness.run(worker)
	<-started

	stop()

	require.Error(t, <-causes, "the drain timeout cancels the handler")
	record := harness.record(id)
	require.Equal(t, jobs.StatusPending, record.Status)
	require.Equal(t, 0, record.Attempt, "a released attempt does not count")
	require.Equal(t, []jobs.Result{jobs.ResultReleased}, harness.observer.results())
}

// Finding 27: the cancel of Run's context provably happens while the handler blocks, and the
// handler's context stays live through the drain.
func TestShutdownLetsARunningHandlerFinish(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(&jobs.WorkerOptions{DrainTimeout: waitFor})
	started, proceed := make(chan struct{}), make(chan struct{})
	handlerContextLive := make(chan bool, 1)
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		close(started)
		<-proceed
		handlerContextLive <- ctx.Err() == nil
		return nil
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	runContext, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() { returned <- worker.Run(runContext) }()
	<-started

	cancel() // returns after runContext is cancelled, while the handler still blocks
	close(proceed)

	require.True(t, <-handlerContextLive, "shutdown must not cancel a handler within the drain timeout")
	require.NoError(t, <-returned)
	require.Equal(t, jobs.StatusCompleted, harness.record(id).Status)
}

func TestRescuerReturnsAJobOfADeadWorker(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))
	_, err := harness.store.Claim(context.Background(), &jobs.ClaimRequest{Queue: "mail", Limit: 1, LeaseDuration: time.Microsecond, Worker: "crashed"})
	require.NoError(t, err)
	worker := harness.worker(nil)
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))

	harness.run(worker)

	record := harness.eventuallyStatus(id, jobs.StatusCompleted)
	require.Equal(t, 2, record.Attempt, "the crashed attempt counts")
}

func TestSchedulesEnqueueOncePerPeriodAcrossTwoWorkers(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	_, err := harness.pool.Exec(context.Background(), "INSERT INTO govengers_job_schedules (name, next_run_at) VALUES ('daily-digest', now() - interval '1 minute')")
	require.NoError(t, err)
	schedule := jobs.Schedule{Name: "daily-digest", Next: jobs.Every(24 * time.Hour), Job: kind.New(mailPayload{Recipient: "team@example.com"}, nil)}
	var runs atomic.Int32
	for range 2 {
		worker := harness.worker(&jobs.WorkerOptions{Schedules: []jobs.Schedule{schedule}})
		require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error {
			runs.Add(1)
			return nil
		}))
		harness.run(worker)
	}

	require.Eventually(t, func() bool { return harness.observer.count(jobs.OperationSchedules) >= 10 }, waitFor, pollEvery)
	require.Eventually(t, func() bool { return runs.Load() == 1 }, waitFor, pollEvery)
	records, err := harness.store.List(context.Background(), &postgresadapter.ListFilter{Queue: "mail"})
	require.NoError(t, err)
	require.Len(t, records, 1)
}

func TestUnknownKindRetries(t *testing.T) {
	harness := newHarness(t)
	worker := harness.worker(nil)
	id := harness.enqueue(context.Background(), mailKind(nil).New(mailPayload{}, nil))

	harness.run(worker)

	require.Eventually(t, func() bool {
		record := harness.record(id)
		return record.Status == jobs.StatusPending && record.Attempt == 1
	}, waitFor, pollEvery)
	require.Contains(t, harness.record(id).LastError, "no handler for kind mail.send")
}

func TestPayloadDecoding(t *testing.T) {
	type mailWithAttachments struct {
		Recipient   string   `json:"recipient"`
		Attachments []string `json:"attachments"`
	}
	type mailWithNumber struct {
		Recipient int `json:"recipient"`
	}
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(nil)
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	newerProducer := jobs.NewKind[mailWithAttachments]("mail.send", &jobs.KindOptions{Queue: "mail"})
	brokenProducer := jobs.NewKind[mailWithNumber]("mail.send", &jobs.KindOptions{Queue: "mail"})
	tolerated := harness.enqueue(context.Background(), newerProducer.New(mailWithAttachments{Recipient: "ada@example.com", Attachments: []string{"a.pdf"}}, nil))
	undecodable := harness.enqueue(context.Background(), brokenProducer.New(mailWithNumber{Recipient: 7}, nil))

	harness.run(worker)

	harness.eventuallyStatus(tolerated, jobs.StatusCompleted)
	record := harness.eventuallyStatus(undecodable, jobs.StatusDead)
	require.Equal(t, 1, record.Attempt)
	require.Contains(t, record.LastError, "decode payload of mail.send")
}

func TestRetentionSweepsCompletedJobs(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(nil)
	worker := harness.worker(&jobs.WorkerOptions{RetainCompleted: time.Nanosecond, SweepInterval: 20 * time.Millisecond})
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	harness.run(worker)

	require.Eventually(t, func() bool {
		_, err := harness.store.Get(context.Background(), id)
		return errors.Is(err, postgresadapter.ErrNotFound)
	}, waitFor, pollEvery)
}

func TestRunDueRunsRunnableJobsSynchronously(t *testing.T) {
	harness := newHarness(t)
	kind := mailKind(&jobs.KindOptions{Backoff: noBackoff, MaximumAttempts: 2})
	worker := harness.worker(nil)
	var calls atomic.Int32
	require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error {
		if calls.Add(1) == 1 {
			return errors.New("first attempt fails")
		}
		return nil
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	attempts, err := worker.RunDue(context.Background())

	require.NoError(t, err)
	require.Equal(t, 2, attempts)
	require.Equal(t, jobs.StatusCompleted, harness.record(id).Status)
}

func TestNewWorkerOwnsItsSchedules(t *testing.T) {
	harness := newHarness(t)
	options := &jobs.WorkerOptions{Schedules: []jobs.Schedule{{Name: "original", Next: jobs.Every(time.Hour), Job: mailKind(nil).New(mailPayload{}, nil)}}}
	worker := harness.worker(options)

	options.Schedules[0].Name = "changed after NewWorker"
	harness.run(worker)

	require.Eventually(t, func() bool { return harness.observer.count(jobs.OperationSchedules) >= 2 }, waitFor, pollEvery)
	rows, err := harness.pool.Query(context.Background(), "SELECT name FROM govengers_job_schedules")
	require.NoError(t, err)
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.Equal(t, []string{"original"}, names)
}

type digestPayload struct {
	Recipients map[string]string `json:"recipients"`
}

// Round 2, findings 8 and 9: a scheduled job carries no value of Run's context, and its payload
// is the one the schedule had at NewWorker.
func TestAScheduledJobIsFreeOfRunsContextAndKeepsItsPayload(t *testing.T) {
	harness := newHarness(t)
	_, err := harness.pool.Exec(context.Background(), "INSERT INTO govengers_job_schedules (name, next_run_at) VALUES ('digest', now() - interval '1 minute')")
	require.NoError(t, err)
	recipients := map[string]string{"team": "team@example.com"}
	digestKind := jobs.NewKind[digestPayload]("mail.digest", &jobs.KindOptions{Queue: "mail"})
	worker := harness.worker(&jobs.WorkerOptions{Schedules: []jobs.Schedule{{Name: "digest", Next: jobs.Every(time.Hour), Job: digestKind.New(digestPayload{Recipients: recipients}, nil)}}})
	recipients["intruder"] = "changed after NewWorker"
	require.NoError(t, jobs.Handle(worker, digestKind, func(context.Context, *jobs.Job[digestPayload]) error { return nil }))
	runContext, cancel := context.WithCancel(context.WithValue(context.Background(), tenantKey{}, "tenant-of-main"))
	returned := make(chan error, 1)
	go func() { returned <- worker.Run(runContext) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-returned)
	})

	var records []*postgresadapter.Record
	require.Eventually(t, func() bool {
		records, err = harness.store.List(context.Background(), &postgresadapter.ListFilter{Queue: "mail"})
		return err == nil && len(records) == 1
	}, waitFor, pollEvery)

	require.Empty(t, records[0].Headers, "no tenant from Run's context")
	require.JSONEq(t, `{"recipients":{"team":"team@example.com"}}`, string(records[0].Payload))
}
