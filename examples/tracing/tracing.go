// Package tracing is a tested example, not a dependency: a jobs.Propagator that carries the
// W3C trace context (traceparent, tracestate) from the request that enqueues a job into the
// job's handler (OBS-001). It is a module of its own so that github.com/govengers/jobs does not
// require OpenTelemetry; a service copies these lines.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/govengers/jobs"
)

// Propagator carries the trace context. Configure it on both sides, in
// postgresadapter.Options.Propagators and jobs.WorkerOptions.Propagators.
type Propagator struct{}

var _ jobs.Propagator = Propagator{}

// Inject writes the enqueuing context's trace context into the job's headers; a context
// without one writes nothing.
func (Propagator) Inject(ctx context.Context, headers map[string]string) error {
	propagation.TraceContext{}.Inject(ctx, propagation.MapCarrier(headers))
	return nil
}

// Extract restores the trace context of the headers. It first removes the span of the worker's
// own context: TraceContext.Extract returns its input unchanged when the headers carry no valid
// trace, and a job enqueued without one must not join the worker's trace (the jobs.Propagator
// contract).
func (Propagator) Extract(ctx context.Context, headers map[string]string) context.Context {
	ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(headers))
}
