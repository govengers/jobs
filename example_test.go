package jobs_test

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

type renderPayload struct {
	RenderID uuid.UUID `json:"renderId"`
}

// renderKind is the contract shared by the code that enqueues renders and the worker.
var renderKind = jobs.NewKind[renderPayload]("generation.render", &jobs.KindOptions{Queue: "generation", MaximumAttempts: 5})

// This example wires a worker in main and enqueues a job inside a transaction, the way a
// repository does. It needs a database with postgresadapter.MigrationV1 applied.
func ExampleNewWorker() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://localhost/app")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pool.Close()
	jobStore, err := postgresadapter.New(&postgresadapter.Options{Pool: pool})
	if err != nil {
		fmt.Println(err)
		return
	}
	worker, err := jobs.NewWorker(&jobs.WorkerOptions{
		Store:  jobStore,
		Queues: map[string]jobs.QueueOptions{"generation": {Concurrency: 3, LeaseDuration: 30 * time.Second}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = jobs.Handle(worker, renderKind, func(ctx context.Context, job *jobs.Job[renderPayload]) error {
		return render(ctx, job.Payload.RenderID) // a job controller calls the service
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	group, groupContext := errgroup.WithContext(ctx)
	group.Go(recovering(func() error { return worker.Run(groupContext) })) // CON-001

	transaction, err := pool.Begin(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = transaction.Rollback(ctx) }() // a no-op after Commit
	// ... insert the render row on transaction, then enqueue its job on the same transaction:
	if _, err := jobStore.Enqueue(ctx, transaction, renderKind.New(renderPayload{RenderID: uuid.New()}, nil)); err != nil {
		fmt.Println(err)
		return
	}
	if err := transaction.Commit(ctx); err != nil {
		fmt.Println(err)
		return
	}
	if err := group.Wait(); err != nil {
		fmt.Println(err)
	}
}

func render(context.Context, uuid.UUID) error { return nil }

// recovering turns a panic in work into an error for its errgroup (CON-001).
func recovering(work func() error) func() error {
	return func() (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panic: %v", recovered)
			}
		}()
		return work()
	}
}
