package oteladapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	wideevent "github.com/govengers/wide-event-logger"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/oteladapter"
	"github.com/govengers/jobs/wideeventadapter"
)

func spanContext(t *testing.T, traceID, spanID string) trace.SpanContext {
	t.Helper()
	parsedTrace, err := trace.TraceIDFromHex(traceID)
	require.NoError(t, err)
	parsedSpan, err := trace.SpanIDFromHex(spanID)
	require.NoError(t, err)
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: parsedTrace, SpanID: parsedSpan, TraceFlags: trace.FlagsSampled, Remote: true})
}

// The propagator carries the enqueuer's trace and never lets the worker's own span through.
func TestPropagator(t *testing.T) {
	enqueuer := spanContext(t, "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")
	worker := spanContext(t, "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
	workerContext := trace.ContextWithSpanContext(context.Background(), worker)
	headers := map[string]string{}
	require.NoError(t, oteladapter.NewPropagator().Inject(trace.ContextWithSpanContext(context.Background(), enqueuer), headers))

	tests := map[string]struct {
		headers map[string]string
		want    trace.SpanContext
	}{
		"valid header carries the enqueuer's trace": {headers: headers, want: enqueuer},
		"no header":        {headers: map[string]string{}, want: trace.SpanContext{}},
		"malformed header": {headers: map[string]string{"traceparent": "not-a-trace"}, want: trace.SpanContext{}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			restored := trace.SpanContextFromContext(oteladapter.NewPropagator().Extract(workerContext, test.headers))

			require.Equal(t, test.want.TraceID(), restored.TraceID())
			require.Equal(t, test.want.SpanID(), restored.SpanID())
		})
	}
}

func TestInjectWithoutASpanWritesNothing(t *testing.T) {
	headers := map[string]string{}

	require.NoError(t, oteladapter.NewPropagator().Inject(context.Background(), headers))

	require.Empty(t, headers)
}

func TestNewMiddlewareRequiresATracerProvider(t *testing.T) {
	_, err := oteladapter.NewMiddleware(nil)
	require.Error(t, err)
	_, err = oteladapter.NewMiddleware(&oteladapter.Options{})
	require.Error(t, err)
}

func newClaim() *jobs.Claim {
	return &jobs.Claim{
		Lease: jobs.Lease{JobID: uuid.MustParse("019a3b5c-0000-7000-8000-000000000001"), Token: uuid.New()},
		Queue: "mail", Kind: "mail.send", Attempt: 2, MaximumAttempts: 5,
	}
}

// run calls handler through the wide-event and the tracing middleware, outside a worker, so the
// span ends when the handler returns.
func run(ctx context.Context, t *testing.T, handler jobs.Handler) (sdktrace.ReadOnlySpan, *wideevent.Event, error) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tracing, err := oteladapter.NewMiddleware(&oteladapter.Options{TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))})
	require.NoError(t, err)
	var event *wideevent.Event
	wideEvents, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: wideevent.EmitterFunc(func(we *wideevent.Event) { event = we })})
	require.NoError(t, err)

	handlerError := wideEvents(tracing(handler))(ctx, newClaim())

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	return spans[0], event, handlerError
}

func attributes(span sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	values := map[attribute.Key]attribute.Value{}
	for _, recorded := range span.Attributes() {
		values[recorded.Key] = recorded.Value
	}
	return values
}

func field(we *wideevent.Event, key string) any {
	for _, recorded := range we.Fields() {
		if recorded.Key == key {
			return recorded.Value
		}
	}
	return nil
}

func TestAttemptSpanContinuesTheRestoredTrace(t *testing.T) {
	enqueuer := spanContext(t, "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")

	span, event, err := run(trace.ContextWithSpanContext(context.Background(), enqueuer), t, func(context.Context, *jobs.Claim) error { return nil })

	require.NoError(t, err)
	require.Equal(t, "job.mail", span.Name())
	require.Equal(t, trace.SpanKindConsumer, span.SpanKind())
	require.Equal(t, enqueuer.TraceID(), span.SpanContext().TraceID())
	require.Equal(t, enqueuer.SpanID(), span.Parent().SpanID())
	require.Equal(t, codes.Unset, span.Status().Code)
	values := attributes(span)
	require.Equal(t, oteladapter.MessagingSystem, values[oteladapter.AttributeMessagingSystem].AsString())
	require.Equal(t, oteladapter.MessagingOperationProcess, values[oteladapter.AttributeMessagingOperationType].AsString())
	require.Equal(t, "mail", values[oteladapter.AttributeMessagingDestination].AsString())
	require.Equal(t, "019a3b5c-0000-7000-8000-000000000001", values[oteladapter.AttributeMessagingMessageID].AsString())
	require.Equal(t, "mail.send", values[wideeventadapter.KeyJobKind].AsString())
	require.Equal(t, int64(2), values[wideeventadapter.KeyJobAttempt].AsInt64())
	require.Equal(t, int64(5), values[wideeventadapter.KeyJobMaximumAttempts].AsInt64())
	require.Equal(t, span.SpanContext().TraceID().String(), field(event, oteladapter.KeyTraceID))
	require.Equal(t, span.SpanContext().SpanID().String(), field(event, oteladapter.KeySpanID))
}

func TestTheHandlerSeesTheAttemptSpan(t *testing.T) {
	var seen trace.SpanContext

	span, _, err := run(context.Background(), t, func(ctx context.Context, _ *jobs.Claim) error {
		seen = trace.SpanContextFromContext(ctx)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, span.SpanContext(), seen)
	require.False(t, span.Parent().IsValid(), "a job without a restored trace starts its own")
}

func TestFailuresAreRecorded(t *testing.T) {
	tests := map[string]struct {
		handlerError  error
		wantStatus    codes.Code
		wantPermanent any
		wantSnoozed   string
	}{
		"failure":   {handlerError: errors.New("smtp: 451"), wantStatus: codes.Error, wantPermanent: false},
		"permanent": {handlerError: jobs.Permanent(errors.New("no such mailbox")), wantStatus: codes.Error, wantPermanent: true},
		"snooze":    {handlerError: jobs.Snooze(time.Minute), wantStatus: codes.Unset, wantSnoozed: "1m0s"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			span, _, _ := run(context.Background(), t, func(context.Context, *jobs.Claim) error { return test.handlerError })

			require.Equal(t, test.wantStatus, span.Status().Code)
			values := attributes(span)
			if test.wantPermanent != nil {
				require.Equal(t, test.wantPermanent, values[wideeventadapter.KeyJobPermanent].AsBool())
				require.Len(t, span.Events(), 1, "the error is recorded")
			}
			require.Equal(t, test.wantSnoozed, values[wideeventadapter.KeyJobSnoozed].AsString())
		})
	}
}

func TestAPanicIsRecordedAndGoesOn(t *testing.T) {
	span, event, err := run(context.Background(), t, func(context.Context, *jobs.Claim) error { panic("boom") })

	require.ErrorContains(t, err, "boom", "the wideeventadapter middleware turned it into the attempt's error")
	require.Equal(t, codes.Error, span.Status().Code)
	require.Equal(t, "panic: boom", span.Status().Description)
	require.Equal(t, span.SpanContext().TraceID().String(), field(event, oteladapter.KeyTraceID))
}

func TestANoOpProviderWritesNoTraceID(t *testing.T) {
	tracing, err := oteladapter.NewMiddleware(&oteladapter.Options{TracerProvider: noop.NewTracerProvider()})
	require.NoError(t, err)
	var event *wideevent.Event
	wideEvents, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: wideevent.EmitterFunc(func(we *wideevent.Event) { event = we })})
	require.NoError(t, err)

	require.NoError(t, wideEvents(tracing(func(context.Context, *jobs.Claim) error { return nil }))(context.Background(), newClaim()))

	require.Nil(t, field(event, oteladapter.KeyTraceID))
}
