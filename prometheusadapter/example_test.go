package prometheusadapter_test

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/prometheusadapter"
)

func ExampleNew() {
	registry := prometheus.NewRegistry() // main's own registry, served on the internal /metrics port (MET-001, MET-002)
	metrics, err := prometheusadapter.New(&prometheusadapter.Options{
		Registerer: registry,
		Namespace:  "billing",
		// DepthSource: jobStore, // the postgresadapter.Store adds the depth gauges
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	// jobs.WorkerOptions{Observer: metrics} makes the worker report every attempt.
	metrics.AttemptFinished(&jobs.Outcome{
		Claim:    &jobs.Claim{Queue: "invoices", Kind: "invoice.render"},
		Result:   jobs.ResultCompleted,
		Duration: 800 * time.Millisecond,
	})

	fmt.Println(testutil.CollectAndCount(registry, "billing_jobs_attempts_total"))
	// Output: 1
}
