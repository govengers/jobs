package jobstest_test

import (
	"fmt"
	"testing"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/jobstest"
)

// In a service test: build the worker with the production handlers on the test's database,
// call the service that enqueues inside its transaction, then drain and assert on the effects.
// The example only compiles: it needs the test's t and database.
func ExampleDrain() {
	var t *testing.T        // the test's t
	var worker *jobs.Worker // jobs.NewWorker on the test's store, handlers registered with jobs.Handle

	// userService.SignUp(t.Context(), input) enqueued the welcome mail; run it now:
	attempts := jobstest.Drain(t, worker)
	fmt.Println(attempts)
}
