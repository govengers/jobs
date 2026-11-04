// Package oteladapter traces jobs with OpenTelemetry: NewPropagator carries the W3C trace
// context from the request that enqueues a job into the job, NewMiddleware opens one span per
// attempt in that trace and puts its IDs on the attempt's wide event. The package depends on the
// OpenTelemetry API only; the service builds the SDK provider (github.com/govengers/telemetry/tracing).
package oteladapter

import (
	"context"
	"errors"
	"fmt"

	wideevent "github.com/govengers/wide-event-logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/wideeventadapter"
)

// ScopeName is the instrumentation scope of the attempt spans.
const ScopeName = "github.com/govengers/jobs/oteladapter"

// The wide-event fields the middleware writes, the same telemetry/tracing.RecordTraceID writes
// on a request's event (standards TRC).
const (
	KeyTraceID = "trace.id"
	KeySpanID  = "trace.span_id"
)

// The span attributes besides the job's own (job.kind, job.attempt, job.result, ...), after the
// OpenTelemetry messaging conventions.
const (
	AttributeMessagingSystem        = "messaging.system"
	AttributeMessagingOperationType = "messaging.operation.type"
	AttributeMessagingDestination   = "messaging.destination.name"
	AttributeMessagingMessageID     = "messaging.message.id"
	MessagingSystem                 = "govengers_jobs"
	MessagingOperationProcess       = "process"
)

// NewPropagator returns the jobs.Propagator for the W3C trace context (traceparent,
// tracestate). Configure it on both sides, in postgresadapter.Options.Propagators and
// jobs.WorkerOptions.Propagators. Inject writes the enqueuing context's trace context into the
// job's headers, nothing without one. Extract restores it and leaves no span of the worker's own
// context in place: a job enqueued without a trace starts a trace of its own.
func NewPropagator() jobs.Propagator {
	return tracePropagator{}
}

type tracePropagator struct{}

func (tracePropagator) Inject(ctx context.Context, headers map[string]string) error {
	propagation.TraceContext{}.Inject(ctx, propagation.MapCarrier(headers))
	return nil
}

func (tracePropagator) Extract(ctx context.Context, headers map[string]string) context.Context {
	// TraceContext.Extract returns its input unchanged when the headers carry no valid trace.
	ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(headers))
}

// Options configures the middleware.
type Options struct {
	// TracerProvider starts the spans; required. Pass the provider main builds, or
	// otel.GetTracerProvider() when main installed it globally.
	TracerProvider trace.TracerProvider
}

// NewMiddleware returns the jobs.Middleware that wraps every attempt in a span named
// job.<queue> of kind consumer, a child of the trace NewPropagator restored. Put it right after
// the wideeventadapter middleware in jobs.WorkerOptions.Middleware: it writes trace.id and
// trace.span_id onto the attempt's wide event, which must exist by then.
//
// A failed attempt records the error and sets the span's status to error; a snooze is no error;
// a permanent failure sets job.permanent. In a worker the span ends once the outcome is stored,
// with job.result: an attempt whose outcome was not stored (fenced, finish_failed, lease_lost)
// is an error even when its handler succeeded. A panic of the handler is recorded and goes on.
func NewMiddleware(options *Options) (jobs.Middleware, error) {
	if options == nil || options.TracerProvider == nil {
		return nil, errors.New("oteladapter: Options.TracerProvider is required")
	}
	tracer := options.TracerProvider.Tracer(ScopeName)
	return func(next jobs.Handler) jobs.Handler {
		return func(ctx context.Context, claim *jobs.Claim) (err error) {
			ctx, span := tracer.Start(ctx, "job."+claim.Queue, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(
				attribute.String(AttributeMessagingSystem, MessagingSystem),
				attribute.String(AttributeMessagingOperationType, MessagingOperationProcess),
				attribute.String(AttributeMessagingDestination, claim.Queue),
				attribute.String(AttributeMessagingMessageID, claim.Lease.JobID.String()),
				attribute.String(wideeventadapter.KeyJobKind, claim.Kind),
				attribute.Int(wideeventadapter.KeyJobAttempt, claim.Attempt),
				attribute.Int(wideeventadapter.KeyJobMaximumAttempts, claim.MaximumAttempts),
			))
			recordTraceID(ctx, span.SpanContext())
			defer func() {
				recovered := recover()
				handlerError := err
				if recovered != nil {
					handlerError = fmt.Errorf("panic: %v", recovered)
				}
				recordHandler(span, handlerError)
				stored := jobs.OnResult(ctx, func(result jobs.Result, writeError error) {
					recordStored(span, result, writeError)
					span.End()
				})
				if !stored {
					span.End()
				}
				if recovered != nil {
					panic(recovered) // the wideeventadapter middleware outside turns it into the attempt's error
				}
			}()
			return next(ctx, claim)
		}
	}, nil
}

// recordTraceID writes the attempt span's IDs onto the attempt's wide event; a no-op provider
// without a restored trace has no valid span context and writes nothing.
func recordTraceID(ctx context.Context, spanContext trace.SpanContext) {
	if !spanContext.IsValid() {
		return
	}
	wideevent.FromContext(ctx).String(KeyTraceID, spanContext.TraceID().String()).String(KeySpanID, spanContext.SpanID().String())
}

// recordHandler records how the handler ended.
func recordHandler(span trace.Span, err error) {
	if err == nil {
		return
	}
	if delay, snoozed := jobs.SnoozeDelay(err); snoozed {
		span.SetAttributes(attribute.String(wideeventadapter.KeyJobSnoozed, delay.String()))
		return
	}
	span.SetAttributes(attribute.Bool(wideeventadapter.KeyJobPermanent, jobs.IsPermanent(err)))
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// recordStored records what the store kept; an outcome that was not stored makes the attempt an
// error, whatever the handler returned.
func recordStored(span trace.Span, result jobs.Result, writeError error) {
	span.SetAttributes(attribute.String(wideeventadapter.KeyJobResult, string(result)))
	if writeError != nil {
		span.RecordError(writeError)
	}
	switch result {
	case jobs.ResultFenced, jobs.ResultFinishFailed, jobs.ResultLeaseLost:
		span.SetStatus(codes.Error, "the outcome was not stored ("+string(result)+")")
	default:
	}
}
