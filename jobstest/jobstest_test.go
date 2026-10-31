package jobstest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/internal/postgrestest"
	"github.com/govengers/jobs/jobstest"
	"github.com/govengers/jobs/postgresadapter"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

type mailPayload struct {
	Recipient string `json:"recipient"`
}

func TestDrainRunsEveryRunnableJob(t *testing.T) {
	pool := postgrestest.New(t)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool})
	require.NoError(t, err)
	mail := jobs.NewKind[mailPayload]("mail.send", &jobs.KindOptions{Queue: "mail", MaximumAttempts: 3, Backoff: func(int) time.Duration { return 0 }})
	waiting := jobs.NewKind[mailPayload]("mail.later", &jobs.KindOptions{Queue: "mail"})
	worker, err := jobs.NewWorker(&jobs.WorkerOptions{Store: store, Queues: map[string]jobs.QueueOptions{"mail": {Concurrency: 2}}})
	require.NoError(t, err)
	var sent []string
	require.NoError(t, jobs.Handle(worker, mail, func(_ context.Context, job *jobs.Job[mailPayload]) error {
		if job.Payload.Recipient == "flaky@example.com" && job.Attempt == 1 {
			return errors.New("smtp hiccup")
		}
		sent = append(sent, job.Payload.Recipient)
		return nil
	}))
	require.NoError(t, jobs.Handle(worker, waiting, func(context.Context, *jobs.Job[mailPayload]) error { return nil }))
	ids, err := store.Enqueue(t.Context(), pool,
		mail.New(mailPayload{Recipient: "ada@example.com"}, nil),
		mail.New(mailPayload{Recipient: "grace@example.com"}, nil),
		mail.New(mailPayload{Recipient: "flaky@example.com"}, nil),
		waiting.New(mailPayload{}, &jobs.EnqueueOptions{RunAt: time.Now().Add(time.Hour)}),
	)
	require.NoError(t, err)

	attempts := jobstest.Drain(t, worker)

	require.Equal(t, 4, attempts, "three jobs, one of them retried at once")
	require.ElementsMatch(t, []string{"ada@example.com", "grace@example.com", "flaky@example.com"}, sent)
	later, err := store.Get(t.Context(), ids[3])
	require.NoError(t, err)
	require.Equal(t, jobs.StatusPending, later.Status, "a job scheduled for later is not runnable")
	require.Equal(t, 0, jobstest.Drain(t, worker), "nothing left")
}
