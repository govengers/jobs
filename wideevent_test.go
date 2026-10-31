package jobs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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

// Round 2, finding 13: the wide event of an attempt says what the store kept, so a handler that
// succeeded but whose outcome was not written is logged as a failure.
func TestTheWideEventRecordsTheStoredResult(t *testing.T) {
	for name, finishFails := range map[string]bool{"stored": false, "write failed": true} {
		t.Run(name, func(t *testing.T) {
			harness := newHarness(t)
			kind := mailKind(nil)
			events := &eventRecorder{}
			middleware, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: events})
			require.NoError(t, err)
			store := &faultyStore{Store: harness.store}
			if finishFails {
				store.finish = func(context.Context, *jobs.Finish) (jobs.Status, error) {
					return "", errors.New("connection reset by peer")
				}
			}
			worker := harness.worker(&jobs.WorkerOptions{Store: store, Middleware: []jobs.Middleware{middleware}, Queues: map[string]jobs.QueueOptions{"mail": {LeaseDuration: time.Minute, PollInterval: pollEvery}}})
			require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
			harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

			harness.run(worker)

			require.Eventually(t, func() bool { return len(events.emitted()) == 1 }, waitFor, pollEvery)
			event := events.emitted()[0]
			if finishFails {
				require.Equal(t, string(jobs.ResultFinishFailed), event[wideeventadapter.KeyJobResult])
				require.Equal(t, wideevent.OutcomeFailure, event[wideevent.KeyOutcome])
				require.Equal(t, "connection reset by peer", event[wideeventadapter.KeyJobFinishError])
				return
			}
			require.Equal(t, string(jobs.ResultCompleted), event[wideeventadapter.KeyJobResult])
			require.Equal(t, wideevent.OutcomeSuccess, event[wideevent.KeyOutcome])
		})
	}
}
