//go:build e2e

// Package e2e_test runs two worker processes, the binary of ./worker built by `just e2e-build`
// and passed in E2E_WORKER, on one PostgreSQL and checks what only real processes can show: a
// job of a killed process is recovered by the other, SIGTERM drains and releases, and a
// schedule is enqueued once per period across both.
package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/internal/postgrestest"
	"github.com/govengers/jobs/postgresadapter"
	"github.com/govengers/jobs/test/e2e/internal/contract"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

const (
	waitFor   = 30 * time.Second
	pollEvery = 50 * time.Millisecond
)

type database struct {
	pool  *pgxpool.Pool
	store *postgresadapter.Store
}

func newDatabase(t *testing.T) *database {
	t.Helper()
	pool := postgrestest.New(t)
	_, err := pool.Exec(t.Context(), `CREATE TABLE e2e_runs (
        job_id uuid NOT NULL, worker text NOT NULL, attempt integer NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp())`)
	require.NoError(t, err)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool})
	require.NoError(t, err)
	return &database{pool: pool, store: store}
}

func (d *database) enqueue(t *testing.T, payload contract.TaskPayload) uuid.UUID {
	t.Helper()
	ids, err := d.store.Enqueue(t.Context(), d.pool, contract.TaskKind.New(payload, nil))
	require.NoError(t, err)
	return ids[0]
}

func (d *database) record(t *testing.T, id uuid.UUID) *postgresadapter.Record {
	t.Helper()
	record, err := d.store.Get(t.Context(), id)
	require.NoError(t, err)
	return record
}

type run struct {
	worker  string
	attempt int
}

// runs lists the runs of the job in order.
func (d *database) runs(t *testing.T, id uuid.UUID) []run {
	t.Helper()
	rows, err := d.pool.Query(t.Context(), "SELECT worker, attempt FROM e2e_runs WHERE job_id = $1 ORDER BY created_at", id)
	require.NoError(t, err)
	runs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (run, error) {
		var result run
		err := row.Scan(&result.worker, &result.attempt)
		return result, err
	})
	require.NoError(t, err)
	return runs
}

func (d *database) awaitRuns(t *testing.T, id uuid.UUID, count int) []run {
	t.Helper()
	require.Eventually(t, func() bool { return len(d.runs(t, id)) >= count }, waitFor, pollEvery, "job %s never ran %d times", id, count)
	return d.runs(t, id)
}

func (d *database) awaitStatus(t *testing.T, id uuid.UUID, status jobs.Status) *postgresadapter.Record {
	t.Helper()
	require.Eventually(t, func() bool { return d.record(t, id).Status == status }, waitFor, pollEvery, "job %s never became %s", id, status)
	return d.record(t, id)
}

// process is one worker process.
type process struct {
	name    string
	command *exec.Cmd
	output  *lockedBuffer // stdout after the ready line, and stderr
	exited  chan struct{} // closed once Wait returned and exitError is set
	// exitError is Wait's result, written before exited is closed.
	exitError error
	killed    bool // the test killed it on purpose: its exit status is no finding
	checked   bool // a harness test inspected its problems itself
}

type lockedBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

// settings are the worker's E2E_* variables.
type settings struct {
	lease, drainTimeout, scheduleEvery string
	extra                              []string // more NAME=value pairs, for the harness tests
}

var defaultSettings = settings{lease: "1s", drainTimeout: "20s", scheduleEvery: "1h"}

// startWorker starts the worker binary and waits until it is ready. The test's cleanup stops
// every worker the test did not kill with SIGTERM and fails the test when one did not exit
// cleanly or reported a race or a panic (TST-004, TST-011).
func startWorker(t *testing.T, database *database, name string, worker settings) *process {
	t.Helper()
	binary := os.Getenv("E2E_WORKER")
	if binary == "" {
		t.Fatal("E2E_WORKER is not set; run the suite with just test-e2e")
	}
	// Never cancelled: the cleanup stops the process itself, gracefully.
	command := exec.CommandContext(context.WithoutCancel(t.Context()), binary) //nolint:gosec // G702: the suite runs the worker binary just e2e-build built
	command.Env = append(append(os.Environ(),
		"DATABASE_URL="+database.pool.Config().ConnString(),
		"E2E_WORKER_NAME="+name,
		"E2E_LEASE="+worker.lease,
		"E2E_DRAIN_TIMEOUT="+worker.drainTimeout,
		"E2E_SCHEDULE_EVERY="+worker.scheduleEvery,
	), worker.extra...)
	output := &lockedBuffer{}
	command.Stderr = output
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	started := &process{name: name, command: command, output: output, exited: make(chan struct{})}
	ready := make(chan struct{})
	go started.forwardOutput(stdout, ready)
	t.Cleanup(func() { started.finish(t) })
	select {
	case <-ready:
	case <-time.After(waitFor):
		t.Fatalf("worker %s did not start:\n%s", name, output)
	}
	return started
}

// forwardOutput signals the ready line, copies the rest of stdout and reports the exit.
func (p *process) forwardOutput(stdout io.Reader, ready chan<- struct{}) {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if scanner.Text() == "ready" {
			close(ready)
			continue
		}
		_, _ = p.output.Write(append(scanner.Bytes(), '\n')) // a lockedBuffer write never fails
	}
	p.exitError = p.command.Wait()
	close(p.exited)
}

// kill ends the process with SIGKILL, on purpose.
func (p *process) kill(t *testing.T) {
	t.Helper()
	p.killed = true
	require.NoError(t, p.command.Process.Signal(syscall.SIGKILL))
	require.Error(t, p.awaitExit(t), "killed, no drain, no release")
}

func (p *process) awaitExit(t *testing.T) error {
	t.Helper()
	select {
	case <-p.exited:
		return p.exitError
	case <-time.After(waitFor):
		t.Fatalf("worker %s did not exit:\n%s", p.name, p.output)
		return nil
	}
}

// problems stops a running process with SIGTERM and returns what is wrong with it: an exit
// status other than 0 (unless the test killed it), a race report or a panic in its output.
func (p *process) problems(t *testing.T) []string {
	t.Helper()
	select {
	case <-p.exited:
	default:
		if !p.killed {
			_ = p.command.Process.Signal(syscall.SIGTERM) // fails only when it exited meanwhile
		}
	}
	var found []string
	if err := p.awaitExit(t); err != nil && !p.killed {
		found = append(found, fmt.Sprintf("worker %s exited with %v", p.name, err))
	}
	output := p.output.String()
	for _, marker := range []string{"WARNING: DATA RACE", "panic:", "fatal error:"} {
		if strings.Contains(output, marker) {
			found = append(found, fmt.Sprintf("worker %s reported %q", p.name, marker))
		}
	}
	return found
}

func (p *process) finish(t *testing.T) {
	t.Helper()
	if p.checked {
		return
	}
	problems := p.problems(t)
	for _, problem := range problems {
		t.Error(problem)
	}
	if len(problems) > 0 || t.Failed() {
		t.Logf("worker %s output:\n%s", p.name, p.output)
	}
}

// The harness must notice a worker that crashes or reports a race, otherwise a broken worker
// leaves the suite green.
func TestTheHarnessReportsABrokenWorker(t *testing.T) {
	database := newDatabase(t)
	for name, extra := range map[string][]string{
		"non-zero exit": {"E2E_EXIT_AFTER_READY=1"},
		"race report":   {"E2E_REPORT_ON_STDERR=WARNING: DATA RACE"},
		"panic":         {"E2E_REPORT_ON_STDERR=panic: something broke"},
	} {
		t.Run(name, func(t *testing.T) {
			broken := startWorker(t, database, "broken", settings{lease: "1s", drainTimeout: "1s", scheduleEvery: "1h", extra: extra})

			problems := broken.problems(t)
			broken.checked = true

			require.NotEmpty(t, problems)
		})
	}
}

func TestAJobOfAKilledProcessIsRecoveredByTheOther(t *testing.T) {
	database := newDatabase(t)
	workers := map[string]*process{
		"first":  startWorker(t, database, "first", defaultSettings),
		"second": startWorker(t, database, "second", defaultSettings),
	}
	id := database.enqueue(t, contract.TaskPayload{Block: true})
	victim := workers[database.awaitRuns(t, id, 1)[0].worker]

	victim.kill(t)

	record := database.awaitStatus(t, id, jobs.StatusCompleted)
	runs := database.runs(t, id)
	require.Len(t, runs, 2)
	require.NotEqual(t, victim.name, runs[1].worker, "the surviving process ran it")
	require.Equal(t, 2, record.Attempt, "the killed attempt counts")
}

func TestSigtermDrainsThenReleasesTheJob(t *testing.T) {
	database := newDatabase(t)
	draining := startWorker(t, database, "draining", settings{lease: "1s", drainTimeout: "300ms", scheduleEvery: "1h"})
	id := database.enqueue(t, contract.TaskPayload{Block: true})
	database.awaitRuns(t, id, 1)

	require.NoError(t, draining.command.Process.Signal(syscall.SIGTERM))

	require.NoError(t, draining.awaitExit(t), "a clean shutdown exits 0")
	released := database.record(t, id)
	require.Equal(t, jobs.StatusPending, released.Status)
	require.Equal(t, 0, released.Attempt, "a released attempt does not count")
	require.Empty(t, released.Worker)

	startWorker(t, database, "next", defaultSettings)
	record := database.awaitStatus(t, id, jobs.StatusCompleted)
	require.Equal(t, 1, record.Attempt)
	require.Equal(t, []run{{worker: "draining", attempt: 1}, {worker: "next", attempt: 1}}, database.runs(t, id))
}

func TestScheduleIsEnqueuedOncePerPeriodAcrossProcesses(t *testing.T) {
	database := newDatabase(t)
	scheduled := settings{lease: "1s", drainTimeout: "1s", scheduleEvery: "1s"}
	startWorker(t, database, "first", scheduled)
	startWorker(t, database, "second", scheduled)
	const periods = 4

	enqueuedAt := func() []time.Time {
		rows, err := database.pool.Query(context.Background(), "SELECT created_at FROM govengers_jobs WHERE kind = $1 AND status = 'completed' ORDER BY created_at", contract.TickKind.Name())
		require.NoError(t, err)
		created, err := pgx.CollectRows(rows, pgx.RowTo[time.Time])
		require.NoError(t, err)
		return created
	}
	require.Eventually(t, func() bool { return len(enqueuedAt()) >= periods }, waitFor, pollEvery)

	// created_at is the now() of the transaction that enqueued and advanced the schedule, so two
	// enqueues of one period fall into the same Every(1s) bucket.
	perPeriod := map[time.Time]int{}
	for _, created := range enqueuedAt() {
		perPeriod[created.Truncate(time.Second)]++
	}
	for period, count := range perPeriod {
		require.Equal(t, 1, count, "%d jobs in the period starting %s", count, period)
	}
}
