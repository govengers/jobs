// Command signup is the govengers/jobs example service. POST /api/users signs a user up and
// enqueues the welcome mail in the same transaction; the worker sends it. Run it with
// DATABASE_URL pointing at an empty PostgreSQL 18 database:
//
//	DATABASE_URL=postgres://localhost/signup go run ./examples/signup/cmd/signup
//	curl -X POST localhost:8080/api/users -d '{"email":"ada@example.com","name":"Ada"}'
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/examples/signup/internal/client/mailer"
	"github.com/govengers/jobs/examples/signup/internal/controller"
	"github.com/govengers/jobs/examples/signup/internal/database"
	"github.com/govengers/jobs/examples/signup/internal/repository"
	"github.com/govengers/jobs/examples/signup/internal/service"
	"github.com/govengers/jobs/postgresadapter"
	"github.com/govengers/jobs/wideeventadapter"
)

const (
	shutdownTimeout = 20 * time.Second
	defaultAddress  = ":8080"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run bootstraps every layer in order (LAY-002) and serves until SIGTERM.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	address := os.Getenv("LISTEN_ADDRESS")
	if address == "" {
		address = defaultAddress
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open the database: %w", err)
	}
	defer pool.Close()
	if err := database.Migrate(ctx, pool); err != nil {
		return err
	}
	jobStore, err := postgresadapter.NewStore(&postgresadapter.StoreOptions{Pool: pool})
	if err != nil {
		return err
	}
	worker, router, err := wire(pool, jobStore)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: address, Handler: router, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}

	group, groupContext := errgroup.WithContext(ctx)
	group.Go(recovering(func() error { return worker.Run(groupContext) }))
	group.Go(recovering(func() error { return serve(groupContext, server) }))
	return group.Wait()
}

// wire builds the repository, the mail client, the service, the controllers and the worker.
func wire(pool *pgxpool.Pool, jobStore *postgresadapter.Store) (*jobs.Worker, *http.ServeMux, error) {
	userRepository, err := repository.NewUserRepository(&repository.UserRepositoryOptions{Pool: pool, JobStore: jobStore})
	if err != nil {
		return nil, nil, err
	}
	mailClient, err := mailer.NewClient(&mailer.ClientOptions{Writer: os.Stdout})
	if err != nil {
		return nil, nil, err
	}
	userService, err := service.NewUserService(&service.UserServiceOptions{UserRepository: userRepository, MailSender: mailClient})
	if err != nil {
		return nil, nil, err
	}
	wideEvents, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: wideevent.NewJSONEmitter(&wideevent.JSONEmitterOptions{Writer: os.Stderr})})
	if err != nil {
		return nil, nil, err
	}
	worker, err := jobs.NewWorker(&jobs.WorkerOptions{
		Store:      jobStore,
		Queues:     map[string]jobs.QueueOptions{"mail": {Concurrency: 4}},
		Middleware: []jobs.Middleware{wideEvents},
	})
	if err != nil {
		return nil, nil, err
	}
	mailJobController, err := controller.NewMailJobController(&controller.MailJobControllerOptions{UserService: userService})
	if err != nil {
		return nil, nil, err
	}
	if err := mailJobController.Register(worker); err != nil {
		return nil, nil, err
	}
	userController, err := controller.NewUserController(&controller.UserControllerOptions{UserService: userService})
	if err != nil {
		return nil, nil, err
	}
	router := http.NewServeMux()
	userController.Register(router)
	return worker, router, nil
}

// serve runs the server until ctx is cancelled, then waits for running requests (LAY-003).
func serve(ctx context.Context, server *http.Server) error {
	failed := make(chan error, 1)
	go func() { failed <- recovering(server.ListenAndServe)() }()
	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}
	shutdownContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	return server.Shutdown(shutdownContext)
}

// recovering turns a panic in work into an error for its errgroup (CON-001).
func recovering(work func() error) func() error {
	return func() (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panic: %v\n%s", recovered, debug.Stack())
			}
		}()
		return work()
	}
}
