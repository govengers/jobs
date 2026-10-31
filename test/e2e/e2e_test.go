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
	"io"
	"os"
	"os/exec"
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
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

const (
	waitFor   = 30 * time.Second
	pollEvery = 50 * time.Millisecond
)

type taskPayload struct {
	Block bool `json:"block"`
}

// taskKind matches the worker binary's e2e.task.
var taskKind = jobs.NewKind[taskPayload]("e2e.task", &jobs.KindOptions{Queue: "e2e", MaximumAttempts: 5})

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

func (d *database) enqueue(t *testing.T, payload taskPayload) uuid.UUID {
	t.Helper()
	ids, err := d.store.Enqueue(t.Context(), d.pool, taskKind.New(payload, nil))
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
	output  *lockedBuffer
	exited  chan error
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

// timing is the worker's E2E_* durations.
type timing struct {
	lease, drainTimeout, scheduleEvery string
}

var defaultTiming = timing{lease: "1s", drainTimeout: "20s", scheduleEvery: "1h"}

// startWorker starts the worker binary and waits until it is ready. The test's cleanup kills it
// if it still runs and logs its output when the test failed.
func startWorker(t *testing.T, database *database, name string, durations timing) *process {
	t.Helper()
	binary := os.Getenv("E2E_WORKER")
	if binary == "" {
		t.Fatal("E2E_WORKER is not set; run the suite with just test-e2e")
	}
	command := exec.CommandContext(t.Context(), binary) //nolint:gosec // G702: the suite runs the worker binary just e2e-build built
	command.Env = append(os.Environ(),
		"DATABASE_URL="+database.pool.Config().ConnString(),
		"E2E_WORKER_NAME="+name,
		"E2E_LEASE="+durations.lease,
		"E2E_DRAIN_TIMEOUT="+durations.drainTimeout,
		"E2E_SCHEDULE_EVERY="+durations.scheduleEvery,
	)
	output := &lockedBuffer{}
	command.Stderr = output
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	worker := &process{name: name, command: command, output: output, exited: make(chan error, 1)}
	ready := make(chan struct{})
	go worker.forwardOutput(stdout, ready)
	t.Cleanup(func() { worker.stop(t) })
	select {
	case <-ready:
	case <-time.After(waitFor):
		t.Fatalf("worker %s did not start:\n%s", name, output)
	}
	return worker
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
	p.exited <- p.command.Wait()
}

func (p *process) awaitExit(t *testing.T) error {
	t.Helper()
	select {
	case err := <-p.exited:
		p.exited <- err // keep it for stop
		return err
	case <-time.After(waitFor):
		t.Fatalf("worker %s did not exit:\n%s", p.name, p.output)
		return nil
	}
}

func (p *process) stop(t *testing.T) {
	t.Helper()
	var exitError error
	select {
	case exitError = <-p.exited:
	default:
		_ = p.command.Process.Kill() // fails only when the process exited meanwhile
		exitError = <-p.exited
	}
	if t.Failed() {
		t.Logf("worker %s exited with %v, output:\n%s", p.name, exitError, p.output)
	}
}

func TestAJobOfAKilledProcessIsRecoveredByTheOther(t *testing.T) {
	database := newDatabase(t)
	workers := map[string]*process{
		"first":  startWorker(t, database, "first", defaultTiming),
		"second": startWorker(t, database, "second", defaultTiming),
	}
	id := database.enqueue(t, taskPayload{Block: true})
	victim := workers[database.awaitRuns(t, id, 1)[0].worker]

	require.NoError(t, victim.command.Process.Signal(syscall.SIGKILL))
	require.Error(t, victim.awaitExit(t), "killed, no drain, no release")

	record := database.awaitStatus(t, id, jobs.StatusCompleted)
	runs := database.runs(t, id)
	require.Len(t, runs, 2)
	require.NotEqual(t, victim.name, runs[1].worker, "the surviving process ran it")
	require.Equal(t, 2, record.Attempt, "the killed attempt counts")
}

func TestSigtermDrainsThenReleasesTheJob(t *testing.T) {
	database := newDatabase(t)
	draining := startWorker(t, database, "draining", timing{lease: "1s", drainTimeout: "300ms", scheduleEvery: "1h"})
	id := database.enqueue(t, taskPayload{Block: true})
	database.awaitRuns(t, id, 1)

	require.NoError(t, draining.command.Process.Signal(syscall.SIGTERM))

	require.NoError(t, draining.awaitExit(t), "a clean shutdown exits 0")
	released := database.record(t, id)
	require.Equal(t, jobs.StatusPending, released.Status)
	require.Equal(t, 0, released.Attempt, "a released attempt does not count")
	require.Empty(t, released.Worker)

	startWorker(t, database, "next", defaultTiming)
	record := database.awaitStatus(t, id, jobs.StatusCompleted)
	require.Equal(t, 1, record.Attempt)
	require.Equal(t, []run{{worker: "draining", attempt: 1}, {worker: "next", attempt: 1}}, database.runs(t, id))
}

func TestScheduleIsEnqueuedOncePerPeriodAcrossProcesses(t *testing.T) {
	database := newDatabase(t)
	scheduled := timing{lease: "1s", drainTimeout: "1s", scheduleEvery: "1s"}
	startWorker(t, database, "first", scheduled)
	startWorker(t, database, "second", scheduled)
	const periods = 4

	ticks := func() []time.Time {
		rows, err := database.pool.Query(context.Background(), "SELECT created_at FROM govengers_jobs WHERE kind = 'e2e.tick' AND status = 'completed' ORDER BY created_at")
		require.NoError(t, err)
		created, err := pgx.CollectRows(rows, pgx.RowTo[time.Time])
		require.NoError(t, err)
		return created
	}
	require.Eventually(t, func() bool { return len(ticks()) >= periods }, waitFor, pollEvery)

	created := ticks()
	for index := 1; index < len(created); index++ {
		gap := created[index].Sub(created[index-1])
		require.Greater(t, gap, 500*time.Millisecond, "two jobs in one period: tick %d came %s after the previous", index, gap)
	}
}
