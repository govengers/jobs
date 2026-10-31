package prometheusadapter

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/govengers/jobs"
)

// depthCollector reads the queues' backlog once per scrape instead of updating gauges on every
// enqueue and claim (MET-006): the table is the truth, and every replica's enqueues are in it.
type depthCollector struct {
	source  DepthSource
	queues  []string
	timeout time.Duration

	pending    *prometheus.Desc
	scheduled  *prometheus.Desc
	running    *prometheus.Desc
	oldestAge  *prometheus.Desc
	failedRead prometheus.Counter
}

func newDepthCollector(options *Options) *depthCollector {
	describe := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(options.Namespace, subsystem, name), help, []string{"queue"}, nil)
	}
	timeout := options.DepthTimeout
	if timeout <= 0 {
		timeout = DefaultDepthTimeout
	}
	return &depthCollector{
		source:    options.DepthSource,
		queues:    append([]string(nil), options.Queues...),
		timeout:   timeout,
		pending:   describe("pending", "Runnable jobs waiting for a worker, by queue; the same on every replica, aggregate with max."),
		scheduled: describe("scheduled", "Pending jobs with a future run time (retries, snoozes, RunAt), by queue; aggregate with max."),
		running:   describe("running", "Jobs leased by any worker, by queue; aggregate with max."),
		oldestAge: describe("oldest_pending_age_seconds", "How long the oldest runnable job has waited, by queue; aggregate with max."),
		failedRead: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: options.Namespace, Subsystem: subsystem, Name: "depth_errors_total",
			Help: "Scrapes whose queue depth query failed; their depth gauges are missing.",
		}),
	}
}

func (c *depthCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.pending
	descriptions <- c.scheduled
	descriptions <- c.running
	descriptions <- c.oldestAge
	c.failedRead.Describe(descriptions)
}

// Collect queries the depths. A failed query skips the gauges and counts the failure instead
// of failing the whole scrape: a slow database must not take the service's other metrics down.
func (c *depthCollector) Collect(metrics chan<- prometheus.Metric) {
	// A scrape has no context of its own; the timeout bounds the query.
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	depths, err := c.source.QueueDepths(ctx)
	if err != nil {
		c.failedRead.Inc()
	} else {
		for _, depth := range c.withDeclaredQueues(depths) {
			send(metrics, c.pending, float64(depth.Pending), depth.Queue)
			send(metrics, c.scheduled, float64(depth.Scheduled), depth.Queue)
			send(metrics, c.running, float64(depth.Running), depth.Queue)
			send(metrics, c.oldestAge, depth.OldestPendingAge.Seconds(), depth.Queue)
		}
	}
	c.failedRead.Collect(metrics)
}

// withDeclaredQueues adds a zero depth for every declared queue the store did not report.
func (c *depthCollector) withDeclaredQueues(depths []jobs.QueueDepth) []jobs.QueueDepth {
	reported := make(map[string]bool, len(depths))
	for _, depth := range depths {
		reported[depth.Queue] = true
	}
	for _, queue := range c.queues {
		if !reported[queue] {
			depths = append(depths, jobs.QueueDepth{Queue: queue})
			reported[queue] = true
		}
	}
	return depths
}

// send emits one gauge; a queue name Prometheus refuses (invalid UTF-8) is skipped instead of
// panicking inside the scrape.
func send(metrics chan<- prometheus.Metric, description *prometheus.Desc, value float64, queue string) {
	metric, err := prometheus.NewConstMetric(description, prometheus.GaugeValue, value, queue)
	if err == nil {
		metrics <- metric
	}
}
