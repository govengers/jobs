package oteladapter_test

import (
	"fmt"

	wideevent "github.com/govengers/wide-event-logger"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/oteladapter"
	"github.com/govengers/jobs/wideeventadapter"
)

func Example() {
	tracerProvider := noop.NewTracerProvider() // in a service: provider.TracerProvider() of telemetry/tracing
	jobEvents, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: wideevent.NewJSONEmitter(nil)})
	if err != nil {
		fmt.Println(err)
		return
	}
	jobSpans, err := oteladapter.NewMiddleware(&oteladapter.Options{TracerProvider: tracerProvider})
	if err != nil {
		fmt.Println(err)
		return
	}
	options := jobs.WorkerOptions{
		Middleware:  []jobs.Middleware{jobEvents, jobSpans}, // the wide event first, the span inside it
		Propagators: []jobs.Propagator{oteladapter.NewPropagator()},
	}
	// and the same propagator in postgresadapter.Options.Propagators, where jobs are enqueued
	fmt.Println(len(options.Middleware), len(options.Propagators))
	// Output: 2 1
}
