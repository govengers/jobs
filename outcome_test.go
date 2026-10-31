package jobs

import (
	"context"
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
		{name: "Permanent(nil) completes", attempt: 1, handlerError: Permanent(nil), wantFinish: true, wantStatus: StatusCompleted, wantResult: ResultCompleted},
		{name: "a negative backoff runs now", attempt: 1, handlerError: failure, wantFinish: true, wantStatus: StatusPending, wantErrorText: "upstream down", wantResult: ResultRetried},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claim := &Claim{Lease: Lease{JobID: uuid.New(), Token: uuid.New()}, Attempt: test.attempt, MaximumAttempts: 3}
			backoff := fixedBackoff
			if test.name == "a negative backoff runs now" {
				backoff = func(int) time.Duration { return -time.Hour }
			}

			finish, result := decide(claim, test.handlerError, test.cause, backoff)

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

	_, err = (&PreparedJob{}).EncodePayload()
	require.ErrorIs(t, err, ErrInvalidKind, "a zero PreparedJob is an invalid kind, not a database error")
}

func TestPermanentErrorWithoutCauseFormats(t *testing.T) {
	require.Equal(t, "permanent", (&permanentError{}).Error())
}

func TestDecideRecoveringSurvivesAPanickingBackoff(t *testing.T) {
	claim := &Claim{Lease: Lease{JobID: uuid.New(), Token: uuid.New()}, Attempt: 1, MaximumAttempts: 3}

	finish, result := decideRecovering(claim, errors.New("upstream down"), nil, func(int) time.Duration { panic("bad backoff") })

	require.Equal(t, ResultRetried, result)
	require.Equal(t, StatusPending, finish.Status)
	require.Contains(t, finish.Error, "panicked: bad backoff")
	require.GreaterOrEqual(t, finish.Delay, time.Second, "the default backoff replaces the broken one")
}

func TestStoredResult(t *testing.T) {
	writeError := errors.New("connection reset")
	tests := []struct {
		name       string
		decided    Result
		stored     Status
		writeError error
		want       Result
	}{
		{name: "written as decided", decided: ResultRetried, stored: StatusPending, want: ResultRetried},
		{name: "write failed", decided: ResultCompleted, writeError: writeError, want: ResultFinishFailed},
		{name: "lease gone", decided: ResultDead, want: ResultFenced},
		{name: "a cancel request turned the retry into cancelled", decided: ResultRetried, stored: StatusCancelled, want: ResultCancelled},
		{name: "a cancel request turned the release into cancelled", decided: ResultReleased, stored: StatusCancelled, want: ResultCancelled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, storedResult(test.decided, test.stored, test.writeError))
		})
	}
}

func TestOnResultOutsideAnAttemptRegistersNothing(t *testing.T) {
	require.False(t, OnResult(context.Background(), func(Result, error) {}))

	ctx, reports := withResultReports(context.Background())
	require.False(t, OnResult(ctx, nil))
	var got []Result
	require.True(t, OnResult(ctx, func(result Result, _ error) { got = append(got, result) }))
	require.True(t, OnResult(ctx, func(Result, error) { panic("a broken callback") }))
	require.NotPanics(t, func() { reports.report(ResultFenced, nil) })
	require.Equal(t, []Result{ResultFenced}, got)

	// Round 3, finding 3: a registration after the delivery is refused, never silently dropped.
	require.False(t, OnResult(ctx, func(Result, error) { t.Error("a late report must not be registered") }))
}
