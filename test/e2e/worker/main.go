//go:build e2e

// Command worker is the process the end-to-end suite starts twice on one database: a
// jobs.Worker with the suite's handlers, configured through the environment, stopped by
// SIGTERM or killed.
//
// It handles kind e2e.task, which records every run in e2e_runs and, with block set, blocks its
// first run until its context ends, and enqueues e2e.tick once per E2E_SCHEDULE_EVERY.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

// TaskPayload is the payload of e2e.task.
type TaskPayload struct {
	Block bool `json:"block"` // block the first run until the attempt's context ends
}

var (
	taskKind = jobs.NewKind[TaskPayload]("e2e.task", &jobs.KindOptions{Queue: "e2e", MaxAttempts: 5})
	tickKind = jobs.NewKind[struct{}]("e2e.tick", &jobs.KindOptions{Queue: "e2e"})
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	durations, err := readTiming()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("open the database: %w", err)
	}
	defer pool.Close()
	worker, err := newWorker(pool, os.Getenv("E2E_WORKER_NAME"), durations)
	if err != nil {
		return err
	}
	fmt.Println("ready") // the suite waits for this line
	return worker.Run(ctx)
}

// timing holds the durations the suite sets per test.
type timing struct {
	lease         time.Duration
	drainTimeout  time.Duration
	scheduleEvery time.Duration
}

func readTiming() (timing, error) {
	var values [3]time.Duration
	for index, variable := range []string{"E2E_LEASE", "E2E_DRAIN_TIMEOUT", "E2E_SCHEDULE_EVERY"} {
		value, err := time.ParseDuration(os.Getenv(variable))
		if err != nil {
			return timing{}, fmt.Errorf("%s: %w", variable, err)
		}
		values[index] = value
	}
	return timing{lease: values[0], drainTimeout: values[1], scheduleEvery: values[2]}, nil
}

func newWorker(pool *pgxpool.Pool, name string, durations timing) (*jobs.Worker, error) {
	store, err := postgresadapter.NewStore(&postgresadapter.StoreOptions{Pool: pool})
	if err != nil {
		return nil, err
	}
	worker, err := jobs.NewWorker(&jobs.WorkerOptions{
		Store:            store,
		Name:             name,
		Queues:           map[string]jobs.QueueOptions{"e2e": {Concurrency: 4, Lease: durations.lease, PollInterval: 50 * time.Millisecond}},
		DrainTimeout:     durations.drainTimeout,
		RescueInterval:   200 * time.Millisecond,
		ScheduleInterval: 100 * time.Millisecond,
		Schedules: []*jobs.Schedule{{
			Name: "e2e.tick",
			Next: jobs.Every(durations.scheduleEvery),
			Job:  tickKind.New(struct{}{}, nil),
		}},
	})
	if err != nil {
		return nil, err
	}
	record := func(ctx context.Context, id fmt.Stringer, attempt int) (int, error) {
		var runs int
		err := pool.QueryRow(ctx, `WITH run AS (INSERT INTO e2e_runs (job_id, worker, attempt) VALUES ($1, $2, $3))
            SELECT count(*) + 1 FROM e2e_runs WHERE job_id = $1`, id.String(), name, attempt).Scan(&runs)
		return runs, err
	}
	err = jobs.Handle(worker, taskKind, func(ctx context.Context, job *jobs.Job[TaskPayload]) error {
		runs, err := record(context.WithoutCancel(ctx), job.ID, job.Attempt)
		if err != nil || !job.Payload.Block || runs > 1 {
			return err
		}
		<-ctx.Done()
		return context.Cause(ctx)
	})
	if err != nil {
		return nil, err
	}
	return worker, jobs.Handle(worker, tickKind, func(ctx context.Context, job *jobs.Job[struct{}]) error {
		_, err := record(ctx, job.ID, job.Attempt)
		return err
	})
}
