// Package jobstest helps service tests run their jobs synchronously (J18).
package jobstest

import (
	"testing"

	"github.com/govengers/jobs"
)

// Drain runs every runnable job of the worker's queues until none is left (Worker.RunDue) and
// returns how many attempts ran. A store error fails the test. Handlers and middleware run as in
// production; a failed job is retried only once its backoff passed, so a kind with a zero backoff
// runs until it completes or is dead. The worker must not be running.
func Drain(tb testing.TB, worker *jobs.Worker) int {
	tb.Helper()
	attempts, err := worker.RunDue(tb.Context())
	if err != nil {
		tb.Fatalf("jobstest: drain: %v", err)
	}
	return attempts
}
