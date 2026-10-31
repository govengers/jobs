package jobs_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/wideeventadapter"
)

type eventRecorder struct {
	mutex  sync.Mutex
	events []map[string]any
}

func (r *eventRecorder) Emit(we *wideevent.Event) {
	fields := map[string]any{}
	for _, field := range we.Fields() {
		fields[field.Key] = field.Value
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.events = append(r.events, fields)
}

func (r *eventRecorder) emitted() []map[string]any {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return append([]map[string]any(nil), r.events...)
}

// Round 2, finding 13 and round 3, finding 9: the wide event of an attempt says what the store
// kept, so a handler that succeeded but whose outcome was not stored (a failed write, a fenced
// finish, a lost lease) is logged as a failure.
func TestTheWideEventRecordsTheStoredResult(t *testing.T) {
	tests := map[string]struct {
		finish      func(context.Context, *jobs.Finish) (jobs.Status, error)
		loseLease   bool
		wantResult  jobs.Result
		wantOutcome string
	}{
		"stored": {wantResult: jobs.ResultCompleted, wantOutcome: wideevent.OutcomeSuccess},
		"write failed": {finish: func(context.Context, *jobs.Finish) (jobs.Status, error) {
			return "", errors.New("connection reset by peer")
		}, wantResult: jobs.ResultFinishFailed, wantOutcome: wideevent.OutcomeFailure},
		"fenced":     {finish: func(context.Context, *jobs.Finish) (jobs.Status, error) { return "", nil }, wantResult: jobs.ResultFenced, wantOutcome: wideevent.OutcomeFailure},
		"lease lost": {loseLease: true, wantResult: jobs.ResultLeaseLost, wantOutcome: wideevent.OutcomeFailure},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			harness := newHarness(t)
			kind := mailKind(nil)
			events := &eventRecorder{}
			middleware, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: events})
			require.NoError(t, err)
			store := &faultyStore{Store: harness.store, finish: test.finish}
			worker := harness.worker(&jobs.WorkerOptions{Store: store, Middleware: []jobs.Middleware{middleware}})
			require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, job *jobs.Job[mailPayload]) error {
				if test.loseLease {
					_, err := harness.pool.Exec(context.WithoutCancel(ctx), "UPDATE govengers_jobs SET lease_token = gen_random_uuid() WHERE id = $1", job.ID)
					if err != nil {
						return err
					}
					<-ctx.Done() // the next heartbeat finds the lease gone
				}
				return nil // the handler itself succeeded
			}))
			harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

			harness.run(worker)

			require.Eventually(t, func() bool { return len(events.emitted()) == 1 }, waitFor, pollEvery)
			event := events.emitted()[0]
			require.Equal(t, string(test.wantResult), event[wideeventadapter.KeyJobResult])
			require.Equal(t, test.wantOutcome, event[wideevent.KeyOutcome])
			if name == "write failed" {
				require.Equal(t, "connection reset by peer", event[wideeventadapter.KeyJobFinishError])
			}
		})
	}
}
