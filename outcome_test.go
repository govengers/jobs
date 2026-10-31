package jobs

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDecide(t *testing.T) {
	failure := errors.New("upstream down")
	fixedBackoff := func(attempt int) time.Duration { return time.Duration(attempt) * time.Minute }
	tests := []struct {
		name          string
		attempt       int
		handlerError  error
		cause         error
		wantFinish    bool
		wantStatus    Status
		wantDelay     time.Duration
		wantReturned  bool
		wantErrorText string
		wantResult    Result
	}{
		{name: "success", attempt: 1, wantFinish: true, wantStatus: StatusCompleted, wantResult: ResultCompleted},
		{name: "success after cancel request still completes", attempt: 1, cause: ErrCancelled, wantFinish: true, wantStatus: StatusCompleted, wantResult: ResultCompleted},
		{name: "failure retries with backoff", attempt: 2, handlerError: failure, wantFinish: true, wantStatus: StatusPending, wantDelay: 2 * time.Minute, wantErrorText: "upstream down", wantResult: ResultRetried},
		{name: "failure on the last attempt is dead", attempt: 3, handlerError: failure, wantFinish: true, wantStatus: StatusDead, wantErrorText: "upstream down", wantResult: ResultDead},
		{name: "permanent is dead at once", attempt: 1, handlerError: Permanent(failure), wantFinish: true, wantStatus: StatusDead, wantErrorText: "permanent: upstream down", wantResult: ResultDead},
		{name: "snooze returns the attempt", attempt: 1, handlerError: Snooze(time.Hour), wantFinish: true, wantStatus: StatusPending, wantDelay: time.Hour, wantReturned: true, wantResult: ResultSnoozed},
		{name: "cancelled", attempt: 1, handlerError: failure, cause: ErrCancelled, wantFinish: true, wantStatus: StatusCancelled, wantErrorText: "upstream down", wantResult: ResultCancelled},
		{name: "shutdown releases", attempt: 3, handlerError: failure, cause: errShutdown, wantFinish: true, wantStatus: StatusPending, wantReturned: true, wantErrorText: "upstream down", wantResult: ResultReleased},
		{name: "lease lost writes nothing", attempt: 1, handlerError: failure, cause: ErrLeaseLost, wantResult: ResultLeaseLost},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claim := &Claim{Lease: Lease{JobID: uuid.New(), Token: uuid.New()}, Attempt: test.attempt, MaximumAttempts: 3}

			finish, result := decide(claim, test.handlerError, test.cause, fixedBackoff)

			require.Equal(t, test.wantResult, result)
			if !test.wantFinish {
				require.Nil(t, finish)
				return
			}
			require.Equal(t, claim.Lease, finish.Lease)
			require.Equal(t, test.wantStatus, finish.Status)
			require.Equal(t, test.wantDelay, finish.Delay)
			require.Equal(t, test.wantReturned, finish.ReturnAttempt)
			require.Equal(t, test.wantErrorText, finish.Error)
		})
	}
}

func TestPermanentUnwraps(t *testing.T) {
	failure := errors.New("bad input")

	require.ErrorIs(t, Permanent(failure), failure)
	require.Contains(t, Snooze(time.Second).Error(), "1s")
}

func TestExponentialBackoff(t *testing.T) {
	backoff := ExponentialBackoff(time.Second, 10*time.Second)
	tests := []struct {
		attempt int
		minimum time.Duration
	}{
		{attempt: 1, minimum: time.Second},
		{attempt: 2, minimum: 2 * time.Second},
		{attempt: 4, minimum: 8 * time.Second},
		{attempt: 5, minimum: 10 * time.Second},
		{attempt: 1000, minimum: 10 * time.Second},
	}
	for _, test := range tests {
		delay := backoff(test.attempt)

		require.GreaterOrEqual(t, delay, test.minimum, "attempt %d", test.attempt)
		require.LessOrEqual(t, delay, test.minimum+test.minimum/5, "attempt %d", test.attempt)
	}
	require.GreaterOrEqual(t, DefaultBackoff(1), time.Second)
}

func TestEvery(t *testing.T) {
	next := Every(10 * time.Minute)
	now := time.Date(2026, 10, 31, 12, 3, 0, 0, time.UTC)

	require.Equal(t, time.Date(2026, 10, 31, 12, 10, 0, 0, time.UTC), next(now))
	require.Equal(t, time.Date(2026, 10, 31, 12, 20, 0, 0, time.UTC), next(next(now)))
}

func TestDailyAt(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	next := DailyAt(3, 0, berlin)

	before := time.Date(2026, 10, 31, 1, 0, 0, 0, berlin)
	require.Equal(t, time.Date(2026, 10, 31, 3, 0, 0, 0, berlin), next(before))
	atBoundary := time.Date(2026, 10, 31, 3, 0, 0, 0, berlin)
	require.Equal(t, time.Date(2026, 11, 1, 3, 0, 0, 0, berlin), next(atBoundary))
}

func TestKindNew(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	runAt := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	kind := NewKind[payload]("mail.send", &KindOptions{Queue: "mail", MaximumAttempts: 3})

	job := kind.New(payload{Name: "ada"}, &EnqueueOptions{RunAt: runAt, UniqueKey: "ada"})

	encoded, err := job.EncodePayload()
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"ada"}`, string(encoded))
	require.Equal(t, "mail.send", job.Kind())
	require.Equal(t, "mail", job.Queue())
	require.Equal(t, 3, job.MaximumAttempts())
	require.Equal(t, runAt, job.RunAt())
	require.Equal(t, "ada", job.UniqueKey())
	require.Equal(t, "mail.send", kind.Name())
	require.Equal(t, "mail", kind.Queue())
}

func TestKindDefaultsAndValidation(t *testing.T) {
	defaulted := NewKind[int]("count", &KindOptions{Queue: "counts"}).New(1, nil)
	require.Equal(t, DefaultMaximumAttempts, defaulted.MaximumAttempts())

	for _, kind := range []*Kind[int]{
		NewKind[int]("", &KindOptions{Queue: "counts"}),
		NewKind[int]("count", nil),
		NewKind[int]("count", &KindOptions{Queue: "counts", MaximumAttempts: -1}),
	} {
		_, err := kind.New(1, nil).EncodePayload()
		require.ErrorIs(t, err, ErrInvalidKind)
	}

	_, err := NewKind[func()]("function", &KindOptions{Queue: "q"}).New(func() {}, nil).EncodePayload()
	require.Error(t, err)

	_, err = (&NewJob{}).EncodePayload()
	require.ErrorIs(t, err, ErrInvalidKind, "a zero NewJob is an invalid kind, not a database error")
}
