package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

// J54: a running attempt's progress is in the store while the handler still runs.
func TestProgressIsVisibleWhileTheJobRuns(t *testing.T) {
	harness := newHarness(t)
	worker := harness.worker(nil)
	release := make(chan struct{})
	require.NoError(t, jobs.Handle(worker, mailKind(nil), func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		if err := jobs.ReportProgress(ctx, jobs.Progress{Fraction: 0.4, Message: "rendering"}); err != nil {
			return err
		}
		<-release
		return nil
	}))
	id := harness.enqueue(context.Background(), mailKind(nil).New(mailPayload{}, nil))
	harness.run(worker)

	require.Eventually(t, func() bool {
		progress := harness.record(id).Progress
		return progress != nil && *progress == jobs.Progress{Fraction: 0.4, Message: "rendering"}
	}, waitFor, pollEvery)
	running := harness.record(id)
	require.Equal(t, jobs.StatusRunning, running.Status)
	require.NotNil(t, running.StartedAt)
	view := running.View()
	require.Equal(t, postgresadapter.StateRunning, view.Status)
	require.InDelta(t, 40, *view.Progress, 1e-9)
	require.Equal(t, "rendering", view.ProgressMessage)

	close(release)
	completed := harness.eventuallyStatus(id, jobs.StatusCompleted)
	require.InDelta(t, 100, *completed.View().Progress, 1e-9, "a succeeded job shows 100 %")
	require.GreaterOrEqual(t, harness.observer.count(jobs.OperationProgress), 1)
}

// J53: the result of a completed attempt is kept, the result of a failed one is dropped.
func TestAResultIsKeptOnlyByACompletedJob(t *testing.T) {
	harness := newHarness(t)
	worker := harness.worker(nil)
	succeeding := jobs.NewKind[mailPayload]("mail.result", &jobs.KindOptions{Queue: "mail"})
	failing := jobs.NewKind[mailPayload]("mail.result_then_fail", &jobs.KindOptions{Queue: "mail"})
	require.NoError(t, jobs.Handle(worker, succeeding, func(ctx context.Context, job *jobs.Job[mailPayload]) error {
		return jobs.SetResult(ctx, map[string]string{"sentTo": job.Payload.Recipient})
	}))
	require.NoError(t, jobs.Handle(worker, failing, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		require.NoError(t, jobs.SetResult(ctx, "never stored"))
		return jobs.Permanent(errors.New("mailbox does not exist"))
	}))
	completed := harness.enqueue(context.Background(), succeeding.New(mailPayload{Recipient: "ada@example.com"}, nil))
	dead := harness.enqueue(context.Background(), failing.New(mailPayload{}, nil))

	_, err := worker.RunDue(context.Background())
	require.NoError(t, err)

	require.JSONEq(t, `{"sentTo": "ada@example.com"}`, string(harness.record(completed).Result))
	record := harness.record(dead)
	require.Equal(t, jobs.StatusDead, record.Status)
	require.Nil(t, record.Result)
	require.Equal(t, jobs.ErrorClassPermanent, record.ErrorClass)
}

// J54, J55: the last report of a failed attempt is stored with its outcome, even when no
// background write took it, and a transient error that used up the attempts says so.
func TestAFailedAttemptKeepsItsLastProgressAndErrorClass(t *testing.T) {
	harness := newHarness(t)
	worker := harness.worker(&jobs.WorkerOptions{Queues: map[string]jobs.QueueOptions{"mail": {ProgressInterval: 24 * time.Hour}}})
	kind := mailKind(&jobs.KindOptions{MaximumAttempts: 1})
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		for _, message := range []string{"row 1", "row 214"} {
			if err := jobs.ReportProgress(ctx, jobs.Progress{Fraction: 0.6, Message: message}); err != nil {
				return err
			}
		}
		return errors.New("row 214: unknown customer")
	}))
	id := harness.enqueue(context.Background(), kind.New(mailPayload{}, nil))

	_, err := worker.RunDue(context.Background())
	require.NoError(t, err)

	record := harness.record(id)
	require.Equal(t, jobs.StatusDead, record.Status)
	require.Equal(t, &jobs.Progress{Fraction: 0.6, Message: "row 214"}, record.Progress)
	require.Equal(t, jobs.ErrorClassTransient, record.ErrorClass)
	view := record.View()
	require.Equal(t, postgresadapter.StateFailed, view.Status)
	require.Equal(t, "row 214: unknown customer", view.Error)
}

// J54: a progress write that fails or panics reaches the Observer; the attempt goes on.
func TestAFailedProgressWriteDoesNotFailTheAttempt(t *testing.T) {
	harness := newHarness(t)
	var writes atomic.Int32
	store := &faultyStore{Store: harness.store, writeProgress: func(context.Context, jobs.Lease, jobs.Progress) (bool, error) {
		if writes.Add(1) == 1 {
			return false, errors.New("database gone")
		}
		panic("broken store")
	}}
	worker := harness.worker(&jobs.WorkerOptions{Store: store, Queues: map[string]jobs.QueueOptions{"mail": {ProgressInterval: 1}}})
	require.NoError(t, jobs.Handle(worker, mailKind(nil), func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		for written := range 2 {
			if err := jobs.ReportProgress(ctx, jobs.Progress{Fraction: 0.1}); err != nil {
				return err
			}
			require.Eventually(t, func() bool { return harness.observer.count(jobs.OperationProgress) == written+1 }, waitFor, pollEvery)
		}
		return nil
	}))
	id := harness.enqueue(context.Background(), mailKind(nil).New(mailPayload{}, nil))

	_, err := worker.RunDue(context.Background())

	require.NoError(t, err)
	require.Equal(t, jobs.StatusCompleted, harness.record(id).Status)
	require.Equal(t, 2, harness.observer.failed(jobs.OperationProgress))
}

// J54: the last report of an attempt is written with its outcome, so a message PostgreSQL
// would refuse (a NUL, a Latin-1 byte from foreign data) must not fail the finish write; the
// same holds for an error that quotes such bytes.
func TestTextPostgreSQLRefusesDoesNotFailTheFinishWrite(t *testing.T) {
	harness := newHarness(t)
	worker := harness.worker(&jobs.WorkerOptions{Queues: map[string]jobs.QueueOptions{"mail": {ProgressInterval: 24 * time.Hour}}})
	succeeding := jobs.NewKind[mailPayload]("mail.import", &jobs.KindOptions{Queue: "mail"})
	failing := jobs.NewKind[mailPayload]("mail.import_fails", &jobs.KindOptions{Queue: "mail", MaximumAttempts: 1})
	report := func(ctx context.Context) {
		require.NoError(t, jobs.ReportProgress(ctx, jobs.Progress{Fraction: 0.1, Message: "start"}))
		require.NoError(t, jobs.ReportProgress(ctx, jobs.Progress{Fraction: 1, Message: "imported a\x00b.csv, M\xfcller"}))
	}
	require.NoError(t, jobs.Handle(worker, succeeding, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		report(ctx)
		return nil
	}))
	require.NoError(t, jobs.Handle(worker, failing, func(ctx context.Context, _ *jobs.Job[mailPayload]) error {
		report(ctx)
		return errors.New("bad name \xff\x00")
	}))
	completed := harness.enqueue(context.Background(), succeeding.New(mailPayload{}, nil))
	dead := harness.enqueue(context.Background(), failing.New(mailPayload{}, nil))

	_, err := worker.RunDue(context.Background())

	require.NoError(t, err)
	record := harness.record(completed)
	require.Equal(t, jobs.StatusCompleted, record.Status)
	require.Equal(t, "imported ab.csv, M�ller", record.Progress.Message)
	record = harness.record(dead)
	require.Equal(t, jobs.StatusDead, record.Status)
	require.Equal(t, "bad name �", record.LastError)
}

// J60, J61: every result SetResult accepts, the finish write stores as encoded, also what jsonb
// refuses: a relayed body with a lone surrogate escape (JavaScript cutting a string inside an
// emoji), the escape \u0000, a number beyond numeric, and a result exactly at the cap. The
// live trigger is installed, so its notification must not convert the row to jsonb, neither on
// the finish nor on the sweep that deletes the jobs.
func TestEveryResultSetResultAcceptsIsStored(t *testing.T) {
	type relayed struct {
		Status int             `json:"status"`
		Body   json.RawMessage `json:"body"`
	}
	harness := newHarness(t)
	_, err := harness.pool.Exec(context.Background(), postgresadapter.LivewireTriggerSQL)
	require.NoError(t, err)
	worker := harness.worker(nil)
	results := map[string]any{
		"surrogate": relayed{Status: 200, Body: json.RawMessage(`{"title":"Launch party \ud83c"}`)},
		"nul":       map[string]string{"name": "a\x00b"},
		"number":    json.Number("1e131072"),
		"cap":       strings.Repeat("x", jobs.MaximumResultBytes-2), // with its quotes exactly the cap
	}
	kind := jobs.NewKind[mailPayload]("mail.result_shape", &jobs.KindOptions{Queue: "mail", MaximumAttempts: 1})
	require.NoError(t, jobs.Handle(worker, kind, func(ctx context.Context, job *jobs.Job[mailPayload]) error {
		return jobs.SetResult(ctx, results[job.Payload.Recipient])
	}))
	ids := map[string]uuid.UUID{}
	for name := range results {
		ids[name] = harness.enqueue(context.Background(), kind.New(mailPayload{Recipient: name}, nil))
	}

	_, err = worker.RunDue(context.Background())

	require.NoError(t, err)
	for name, id := range ids {
		record := harness.record(id)
		require.Equal(t, jobs.StatusCompleted, record.Status, name)
		encoded, err := json.Marshal(results[name])
		require.NoError(t, err)
		require.Equal(t, string(encoded), string(record.Result), name)
	}
	deleted, err := harness.store.Sweep(context.Background(), &jobs.SweepRequest{Limit: len(ids)})
	require.NoError(t, err)
	require.Equal(t, len(ids), deleted)
}
