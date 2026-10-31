// Package prometheusadapter exports the worker's metrics on a Prometheus registry the caller
// passes in (LIB-011, MET-006). Metrics implements jobs.Observer; give it to
// jobs.WorkerOptions.Observer. With a DepthSource it also reads every queue's backlog at scrape
// time.
//
// The depth gauges (<namespace>_jobs_pending, _scheduled, _running, _oldest_pending_age_seconds)
// describe the shared table, not the scraped process: every replica reports the same values.
// Aggregate them with max, never sum: max by (queue) (billing_jobs_pending).
package prometheusadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/govengers/jobs"
)

// DefaultDepthTimeout bounds the scrape-time depth query when Options.DepthTimeout is zero.
const DefaultDepthTimeout = 5 * time.Second

const subsystem = "jobs"

// The values of the outcome label of the operation metrics.
const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"
)

// attemptBuckets reach from 50 ms to about 16 minutes: jobs range from a mail to an AI render.
var attemptBuckets = prometheus.ExponentialBuckets(0.05, 3, 10)

// DepthSource reads the backlog of the queues; postgresadapter.Store implements it.
type DepthSource interface {
	QueueDepths(ctx context.Context) ([]jobs.QueueDepth, error)
}

// Options configures Metrics.
type Options struct {
	Registerer prometheus.Registerer // required; the caller's registry, never the default one
	Namespace  string                // required: the service name, e.g. "billing" for billing_jobs_attempts_total
	// DepthSource enables the depth gauges, read once per scrape; nil exports none. Run it on
	// every replica that serves /metrics, or on one, either way aggregate with max.
	DepthSource DepthSource
	// Queues are reported with zero depth when they have no pending or running job, so their
	// series do not disappear from dashboards and alerts.
	Queues       []string
	DepthTimeout time.Duration // bounds the depth query of a scrape; zero means DefaultDepthTimeout
}

// Metrics records attempts and store operations; it implements jobs.Observer and is safe for
// concurrent use. Every label is a closed set: queues and kinds are declared in code, results
// and operations are the jobs constants.
//
//	<namespace>_jobs_attempts_total{queue,kind,result}
//	<namespace>_jobs_attempt_duration_seconds{queue,kind}
//	<namespace>_jobs_operations_total{operation,outcome}
//	<namespace>_jobs_operation_duration_seconds{operation}
//	<namespace>_jobs_depth_errors_total
//	<namespace>_jobs_pending{queue}, _scheduled{queue}, _running{queue}, _oldest_pending_age_seconds{queue}
type Metrics struct {
	attempts           *prometheus.CounterVec
	attemptDurations   *prometheus.HistogramVec
	operations         *prometheus.CounterVec
	operationDurations *prometheus.HistogramVec
}

// New registers the metrics on options.Registerer. A failed registration, a duplicate for
// example, is returned as an error.
func New(options *Options) (*Metrics, error) {
	if options == nil || options.Registerer == nil {
		return nil, errors.New("prometheusadapter: Registerer is required")
	}
	if options.Namespace == "" {
		return nil, errors.New("prometheusadapter: Namespace is required")
	}
	if options.DepthTimeout < 0 {
		return nil, errors.New("prometheusadapter: DepthTimeout must not be negative")
	}
	metrics := newMetrics(options.Namespace)
	collectors := []prometheus.Collector{metrics.attempts, metrics.attemptDurations, metrics.operations, metrics.operationDurations}
	if options.DepthSource != nil {
		collectors = append(collectors, newDepthCollector(options))
	}
	if err := registerAll(options.Registerer, collectors); err != nil {
		return nil, fmt.Errorf("prometheusadapter: register the %s_%s metrics: %v", options.Namespace, subsystem, err) //nolint:errorlint // LIB-010: client_golang errors are not part of the API
	}
	return metrics, nil
}

// registerAll registers every collector or none: after a failure it unregisters the ones it
// registered, so the caller can retry on the same registry.
func registerAll(registerer prometheus.Registerer, collectors []prometheus.Collector) error {
	for index, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			for _, registered := range collectors[:index] {
				registerer.Unregister(registered)
			}
			return err
		}
	}
	return nil
}

func newMetrics(namespace string) *Metrics {
	return &Metrics{
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "attempts_total",
			Help: "Finished job attempts by queue, kind and result.",
		}, []string{"queue", "kind", "result"}),
		attemptDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "attempt_duration_seconds",
			Help: "Run time of job handlers by queue and kind.", Buckets: attemptBuckets,
		}, []string{"queue", "kind"}),
		operations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "operations_total",
			Help: "Store operations of the job worker by operation and outcome.",
		}, []string{"operation", "outcome"}),
		operationDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "operation_duration_seconds",
			Help: "Duration of the job worker's store operations.", Buckets: prometheus.DefBuckets,
		}, []string{"operation"}),
	}
}

// AttemptFinished implements jobs.Observer. An outcome with a label value Prometheus refuses
// (invalid UTF-8 in a kind) is not counted: an observer must not panic.
func (m *Metrics) AttemptFinished(outcome *jobs.Outcome) {
	claim := outcome.Claim
	if counter, err := m.attempts.GetMetricWithLabelValues(claim.Queue, claim.Kind, string(outcome.Result)); err == nil {
		counter.Inc()
	}
	if histogram, err := m.attemptDurations.GetMetricWithLabelValues(claim.Queue, claim.Kind); err == nil {
		histogram.Observe(outcome.Duration.Seconds())
	}
}

// OperationFinished implements jobs.Observer.
func (m *Metrics) OperationFinished(operation *jobs.Operation) {
	outcome := outcomeSuccess
	if operation.Err != nil {
		outcome = outcomeFailure
	}
	m.operations.WithLabelValues(string(operation.Name), outcome).Inc()
	m.operationDurations.WithLabelValues(string(operation.Name)).Observe(operation.Duration.Seconds())
}
