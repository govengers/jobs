package jobs_test

import (
	"context"
	"testing"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/internal/postgrestest"
	"github.com/govengers/jobs/oteladapter"
	"github.com/govengers/jobs/postgresadapter"
	"github.com/govengers/jobs/wideeventadapter"
)

// An enqueue inside a span yields an attempt span in the same trace, a child of the enqueuing
// span, that ends with the stored result; the attempt's wide event carries its IDs.
func TestTheAttemptSpanContinuesTheEnqueuingTrace(t *testing.T) {
	tests := map[string]struct {
		finish     func(context.Context, *jobs.Finish) (jobs.Status, error)
		wantResult jobs.Result
		wantStatus codes.Code
	}{
		"stored": {wantResult: jobs.ResultCompleted, wantStatus: codes.Unset},
		"fenced": {finish: func(context.Context, *jobs.Finish) (jobs.Status, error) { return "", nil }, wantResult: jobs.ResultFenced, wantStatus: codes.Error},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			pool := postgrestest.New(t)
			propagators := []jobs.Propagator{oteladapter.NewPropagator()}
			store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, Propagators: propagators})
			require.NoError(t, err)
			events := &eventRecorder{}
			jobEvents, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: events})
			require.NoError(t, err)
			jobSpans, err := oteladapter.NewMiddleware(&oteladapter.Options{TracerProvider: provider})
			require.NoError(t, err)
			worker, err := jobs.NewWorker(&jobs.WorkerOptions{
				Store:       &faultyStore{Store: store, finish: test.finish},
				Queues:      map[string]jobs.QueueOptions{"mail": {}},
				Middleware:  []jobs.Middleware{jobEvents, jobSpans},
				Propagators: propagators,
			})
			require.NoError(t, err)
			kind := mailKind(nil)
			require.NoError(t, jobs.Handle(worker, kind, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
			requestContext, request := provider.Tracer("test").Start(t.Context(), "POST /api/v1/signups")
			_, err = store.Enqueue(requestContext, pool, kind.New(mailPayload{Recipient: "alice@example.com"}, nil))
			require.NoError(t, err)
			request.End()

			_, err = worker.RunDue(t.Context())
			require.NoError(t, err)

			spans := recorder.Ended()
			require.Len(t, spans, 2)
			attempt := spans[1]
			require.Equal(t, "job.mail", attempt.Name())
			require.Equal(t, request.SpanContext().TraceID(), attempt.SpanContext().TraceID())
			require.Equal(t, request.SpanContext().SpanID(), attempt.Parent().SpanID())
			require.Equal(t, test.wantStatus, attempt.Status().Code)
			var result string
			for _, recorded := range attempt.Attributes() {
				if recorded.Key == wideeventadapter.KeyJobResult {
					result = recorded.Value.AsString()
				}
			}
			require.Equal(t, string(test.wantResult), result)
			emitted := events.emitted()
			require.Len(t, emitted, 1)
			require.Equal(t, attempt.SpanContext().TraceID().String(), emitted[0][oteladapter.KeyTraceID])
			require.Equal(t, attempt.SpanContext().SpanID().String(), emitted[0][oteladapter.KeySpanID])
			require.NotEqual(t, wideevent.OutcomeSuccess == emitted[0][wideevent.KeyOutcome], test.wantStatus == codes.Error)
		})
	}
}
