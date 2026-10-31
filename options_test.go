package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
)

// storeStub satisfies jobs.Store for option tests that never run the worker.
type storeStub struct{ jobs.Store }

func TestNewWorkerValidatesOptions(t *testing.T) {
	kind := mailKind(nil)
	validSchedule := func() *jobs.Schedule {
		return &jobs.Schedule{Name: "digest", Next: jobs.Every(time.Hour), Job: kind.New(mailPayload{}, nil)}
	}
	tests := map[string]*jobs.WorkerOptions{
		"nil options":         nil,
		"no store":            {},
		"unnamed queue":       {Store: storeStub{}, Queues: map[string]jobs.QueueOptions{"": {}}},
		"negative queue":      {Store: storeStub{}, Queues: map[string]jobs.QueueOptions{"mail": {Concurrency: -1}}},
		"lease too short":     {Store: storeStub{}, Queues: map[string]jobs.QueueOptions{"mail": {Lease: time.Millisecond}}},
		"negative duration":   {Store: storeStub{}, DrainTimeout: -time.Second},
		"schedule without fn": {Store: storeStub{}, Schedules: []*jobs.Schedule{{Name: "digest", Job: kind.New(mailPayload{}, nil)}}},
		"schedule twice":      {Store: storeStub{}, Schedules: []*jobs.Schedule{validSchedule(), validSchedule()}},
		"schedule in the past": {Store: storeStub{}, Schedules: []*jobs.Schedule{{
			Name: "digest", Next: func(now time.Time) time.Time { return now }, Job: kind.New(mailPayload{}, nil),
		}}},
		"schedule of invalid kind": {Store: storeStub{}, Schedules: []*jobs.Schedule{{
			Name: "digest", Next: jobs.Every(time.Hour), Job: jobs.NewKind[mailPayload]("", nil).New(mailPayload{}, nil),
		}}},
	}
	for name, options := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := jobs.NewWorker(options)
			require.Error(t, err)
		})
	}

	worker, err := jobs.NewWorker(&jobs.WorkerOptions{Store: storeStub{}, Schedules: []*jobs.Schedule{validSchedule()}, Queues: map[string]jobs.QueueOptions{"mail": {}}})
	require.NoError(t, err)
	require.NotNil(t, worker)
}

func TestHandleValidates(t *testing.T) {
	worker, err := jobs.NewWorker(&jobs.WorkerOptions{Store: storeStub{}, Name: "test"})
	require.NoError(t, err)
	handler := func(context.Context, *jobs.Job[mailPayload]) error { return nil }

	require.ErrorIs(t, jobs.Handle(worker, jobs.NewKind[mailPayload]("mail.send", nil), handler), jobs.ErrInvalidKind)
	require.Error(t, jobs.Handle(worker, mailKind(nil), nil))
	require.NoError(t, jobs.Handle(worker, mailKind(nil), handler))
	require.Error(t, jobs.Handle(worker, mailKind(nil), handler), "a second handler for the same kind")
}

func TestRunOnlyOnce(t *testing.T) {
	harness := newHarness(t)
	worker := harness.worker(nil)
	harness.run(worker)

	require.Eventually(t, func() bool {
		return jobs.Handle(worker, jobs.NewKind[mailPayload]("late", &jobs.KindOptions{Queue: "mail"}), func(context.Context, *jobs.Job[mailPayload]) error { return nil }) != nil
	}, waitFor, pollEvery, "Handle after Run must fail")
	require.Error(t, worker.Run(context.Background()))
}
