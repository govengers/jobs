package tracing_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/govengers/jobs/examples/tracing"
)

func spanContext(t *testing.T, traceID, spanID string) trace.SpanContext {
	t.Helper()
	parsedTrace, err := trace.TraceIDFromHex(traceID)
	require.NoError(t, err)
	parsedSpan, err := trace.SpanIDFromHex(spanID)
	require.NoError(t, err)
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: parsedTrace, SpanID: parsedSpan, TraceFlags: trace.FlagsSampled, Remote: true})
}

// Round 2, finding 14: the documented propagator carries the enqueuer's trace and never lets the
// worker's own span through.
func TestTracePropagator(t *testing.T) {
	enqueuer := spanContext(t, "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")
	worker := spanContext(t, "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
	workerContext := trace.ContextWithSpanContext(context.Background(), worker)
	headers := map[string]string{}
	require.NoError(t, tracing.Propagator{}.Inject(trace.ContextWithSpanContext(context.Background(), enqueuer), headers))

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
			restored := trace.SpanContextFromContext(tracing.Propagator{}.Extract(workerContext, test.headers))

			require.Equal(t, test.want.TraceID(), restored.TraceID())
			require.Equal(t, test.want.SpanID(), restored.SpanID())
		})
	}
}

func Example() {
	// main configures it on both sides, like authadapter.Identity():
	//   postgresadapter.Options{Propagators: []jobs.Propagator{tracing.Propagator{}}}
	//   jobs.WorkerOptions{Propagators: []jobs.Propagator{tracing.Propagator{}}}
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736") // a valid constant
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")                   // a valid constant
	request := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled}))
	headers := map[string]string{} // stored with the job
	if err := (tracing.Propagator{}).Inject(request, headers); err != nil {
		fmt.Println(err)
		return
	}

	handlerContext := tracing.Propagator{}.Extract(context.Background(), headers)
	fmt.Println(trace.SpanContextFromContext(handlerContext).TraceID())
	// Output: 4bf92f3577b34da6a3ce929d0e0e4736
}
