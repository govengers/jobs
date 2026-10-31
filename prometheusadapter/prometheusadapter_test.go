package prometheusadapter_test

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/prometheusadapter"
)

type depthStub struct {
	depths []jobs.QueueDepth
	err    error
}

func (s *depthStub) QueueDepths(context.Context) ([]jobs.QueueDepth, error) { return s.depths, s.err }

func newMetrics(t *testing.T, options *prometheusadapter.Options) (*prometheusadapter.Metrics, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	options.Registerer = registry
	options.Namespace = "billing"
	metrics, err := prometheusadapter.New(options)
	require.NoError(t, err)
	return metrics, registry
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	for name, options := range map[string]*prometheusadapter.Options{
		"nil options":            nil,
		"no registerer":          {Namespace: "billing"},
		"no namespace":           {Registerer: prometheus.NewRegistry()},
		"negative depth timeout": {Registerer: prometheus.NewRegistry(), Namespace: "billing", DepthTimeout: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			metrics, err := prometheusadapter.New(options)

			require.Error(t, err)
			require.Nil(t, metrics)
		})
	}
}

func TestNewReturnsADuplicateRegistrationAsError(t *testing.T) {
	registry := prometheus.NewRegistry()
	_, err := prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "billing"})
	require.NoError(t, err)

	_, err = prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "billing"})

	require.ErrorContains(t, err, "billing_jobs")
	var alreadyRegistered prometheus.AlreadyRegisteredError
	require.False(t, errors.As(err, &alreadyRegistered), "the client_golang error is not part of the API")
}

func TestAttemptsAndOperations(t *testing.T) {
	metrics, registry := newMetrics(t, &prometheusadapter.Options{})
	claim := &jobs.Claim{Queue: "mail", Kind: "mail.send"}

	metrics.AttemptFinished(&jobs.Outcome{Claim: claim, Result: jobs.ResultCompleted, Duration: 120 * time.Millisecond})
	metrics.AttemptFinished(&jobs.Outcome{Claim: claim, Result: jobs.ResultRetried, Duration: 2 * time.Second, Err: errors.New("smtp down")})
	metrics.AttemptFinished(&jobs.Outcome{Claim: claim, Result: jobs.ResultCompleted})
	metrics.OperationFinished(&jobs.Operation{Name: jobs.OperationClaim, Queue: "mail", Count: 3, Duration: time.Millisecond})
	metrics.OperationFinished(&jobs.Operation{Name: jobs.OperationRescue, Err: errors.New("connection refused")})

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
# HELP billing_jobs_attempts_total Finished job attempts by queue, kind and result.
# TYPE billing_jobs_attempts_total counter
billing_jobs_attempts_total{kind="mail.send",queue="mail",result="completed"} 2
billing_jobs_attempts_total{kind="mail.send",queue="mail",result="retried"} 1
# HELP billing_jobs_operations_total Store operations of the job worker by operation and outcome.
# TYPE billing_jobs_operations_total counter
billing_jobs_operations_total{operation="claim",outcome="success"} 1
billing_jobs_operations_total{operation="rescue",outcome="failure"} 1
`), "billing_jobs_attempts_total", "billing_jobs_operations_total"))
	attemptDurations := histogram(t, registry, "billing_jobs_attempt_duration_seconds", map[string]string{"queue": "mail", "kind": "mail.send"})
	require.Equal(t, uint64(3), attemptDurations.GetSampleCount(), "every attempt is observed")
	require.InDelta(t, 2.12, attemptDurations.GetSampleSum(), 0.001)
	claimDurations := histogram(t, registry, "billing_jobs_operation_duration_seconds", map[string]string{"operation": "claim"})
	require.Equal(t, uint64(1), claimDurations.GetSampleCount())
	require.InDelta(t, 0.001, claimDurations.GetSampleSum(), 0.0001)
	require.Equal(t, uint64(1), histogram(t, registry, "billing_jobs_operation_duration_seconds", map[string]string{"operation": "rescue"}).GetSampleCount())
}

// histogram returns the histogram of the family with exactly these labels.
func histogram(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) *dto.Histogram {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			found := map[string]string{}
			for _, pair := range metric.GetLabel() {
				found[pair.GetName()] = pair.GetValue()
			}
			if maps.Equal(found, labels) {
				return metric.GetHistogram()
			}
		}
	}
	t.Fatalf("no %s%v", name, labels)
	return nil
}

func TestAttemptWithAnInvalidLabelIsDroppedInsteadOfPanicking(t *testing.T) {
	metrics, registry := newMetrics(t, &prometheusadapter.Options{})

	require.NotPanics(t, func() {
		metrics.AttemptFinished(&jobs.Outcome{Claim: &jobs.Claim{Queue: "mail", Kind: "\xff"}, Result: jobs.ResultCompleted})
	})
	require.Equal(t, 0, testutil.CollectAndCount(registry, "billing_jobs_attempts_total"))
}

func TestDepthGaugesReadTheStoreAtScrapeTime(t *testing.T) {
	source := &depthStub{depths: []jobs.QueueDepth{{Queue: "mail", Pending: 4, Scheduled: 2, Running: 3, OldestPendingAge: 90 * time.Second}}}
	_, registry := newMetrics(t, &prometheusadapter.Options{DepthSource: source, Queues: []string{"mail", "reports"}})

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
# HELP billing_jobs_pending Runnable jobs waiting for a worker, by queue; the same on every replica, aggregate with max.
# TYPE billing_jobs_pending gauge
billing_jobs_pending{queue="mail"} 4
billing_jobs_pending{queue="reports"} 0
# HELP billing_jobs_scheduled Pending jobs with a future run time (retries, snoozes, RunAt), by queue; aggregate with max.
# TYPE billing_jobs_scheduled gauge
billing_jobs_scheduled{queue="mail"} 2
billing_jobs_scheduled{queue="reports"} 0
# HELP billing_jobs_running Jobs leased by any worker, by queue; aggregate with max.
# TYPE billing_jobs_running gauge
billing_jobs_running{queue="mail"} 3
billing_jobs_running{queue="reports"} 0
# HELP billing_jobs_oldest_pending_age_seconds How long the oldest runnable job has waited, by queue; aggregate with max.
# TYPE billing_jobs_oldest_pending_age_seconds gauge
billing_jobs_oldest_pending_age_seconds{queue="mail"} 90
billing_jobs_oldest_pending_age_seconds{queue="reports"} 0
`), "billing_jobs_pending", "billing_jobs_scheduled", "billing_jobs_running", "billing_jobs_oldest_pending_age_seconds"))

	source.depths[0].Pending = 1
	require.Equal(t, 1.0, gaugeValue(t, registry, "billing_jobs_pending", "mail"), "every scrape reads again")
}

func TestFailedDepthReadSkipsTheGaugesAndCountsTheFailure(t *testing.T) {
	_, registry := newMetrics(t, &prometheusadapter.Options{DepthSource: &depthStub{err: errors.New("database down")}, Queues: []string{"mail"}})

	families, err := registry.Gather()

	require.NoError(t, err, "a failed depth read must not fail the scrape")
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	require.Equal(t, []string{"billing_jobs_depth_errors_total"}, names)
	require.Equal(t, 1.0, families[0].GetMetric()[0].GetCounter().GetValue(), "the failed read is counted")
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Equal(t, 2.0, families[0].GetMetric()[0].GetCounter().GetValue(), "every failed scrape counts")
}

// Finding 23: a failed New leaves nothing registered, so a retry on the same registry works.
func TestFailedNewLeavesTheRegistryUnchanged(t *testing.T) {
	registry := prometheus.NewRegistry()
	// the same descriptor as the adapter's third collector, so its registration is the one that fails
	squatter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "billing", Subsystem: "jobs", Name: "operations_total",
		Help: "Store operations of the job worker by operation and outcome.",
	}, []string{"operation", "outcome"})
	require.NoError(t, registry.Register(squatter))

	_, err := prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "billing"})
	require.Error(t, err)

	require.True(t, registry.Unregister(squatter))
	metrics, err := prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "billing"})
	require.NoError(t, err, "the collectors of the failed attempt were unregistered")
	require.NotNil(t, metrics)
}

// TestMetricsPassTheLinter is MET-007: names, units and help of every collector pass promlint
// (GatherAndLint is CollectAndLint over the whole registry).
func TestMetricsPassTheLinter(t *testing.T) {
	metrics, registry := newMetrics(t, &prometheusadapter.Options{DepthSource: &depthStub{depths: []jobs.QueueDepth{{Queue: "mail"}}}})
	metrics.AttemptFinished(&jobs.Outcome{Claim: &jobs.Claim{Queue: "mail", Kind: "mail.send"}, Result: jobs.ResultDead})
	metrics.OperationFinished(&jobs.Operation{Name: jobs.OperationSweep})

	problems, err := testutil.GatherAndLint(registry)

	require.NoError(t, err)
	require.Empty(t, problems)
}

func gaugeValue(t *testing.T, registry *prometheus.Registry, name, queue string) float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metric.GetLabel()[0].GetValue() == queue {
				return metric.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("no %s{queue=%q}", name, queue)
	return 0
}
