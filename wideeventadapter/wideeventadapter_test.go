package wideeventadapter_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	wideevent "github.com/govengers/wide-event-logger"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/wideeventadapter"
)

type recordingEmitter struct {
	mutex  sync.Mutex
	events []*wideevent.Event
}

func (e *recordingEmitter) Emit(we *wideevent.Event) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.events = append(e.events, we)
}

func (e *recordingEmitter) only(t *testing.T) map[string]any {
	t.Helper()
	e.mutex.Lock()
	defer e.mutex.Unlock()
	require.Len(t, e.events, 1)
	fields := map[string]any{}
	for _, field := range e.events[0].Fields() {
		fields[field.Key] = field.Value
	}
	return fields
}

func newClaim() *jobs.Claim {
	return &jobs.Claim{
		Lease: jobs.Lease{JobID: uuid.MustParse("019a3b5c-0000-7000-8000-000000000001"), Token: uuid.New()},
		Queue: "mail", Kind: "mail.send", Attempt: 2, MaximumAttempts: 5,
	}
}

func run(ctx context.Context, t *testing.T, handler jobs.Handler) (map[string]any, error) {
	t.Helper()
	emitter := &recordingEmitter{}
	middleware, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: emitter})
	require.NoError(t, err)

	handlerError := middleware(handler)(ctx, newClaim())

	return emitter.only(t), handlerError
}

func TestNewMiddlewareRequiresAnEmitter(t *testing.T) {
	_, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{})
	require.Error(t, err)
	_, err = wideeventadapter.NewMiddleware(nil)
	require.Error(t, err)
}

func TestSuccessRecordsTheJobAndLetsLayersAddFields(t *testing.T) {
	fields, err := run(context.Background(), t, func(ctx context.Context, _ *jobs.Claim) error {
		wideevent.FromContext(ctx).String("mail.recipient_domain", "example.com")
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, "job.mail", fields[wideevent.KeyName])
	require.Equal(t, wideevent.OutcomeSuccess, fields[wideevent.KeyOutcome])
	require.Equal(t, "019a3b5c-0000-7000-8000-000000000001", fields[wideeventadapter.KeyJobID])
	require.Equal(t, "mail", fields[wideeventadapter.KeyJobQueue])
	require.Equal(t, "mail.send", fields[wideeventadapter.KeyJobKind])
	require.Equal(t, 2, fields[wideeventadapter.KeyJobAttempt])
	require.Equal(t, 5, fields[wideeventadapter.KeyJobMaximumAttempts])
	require.Equal(t, "example.com", fields["mail.recipient_domain"])
	require.Contains(t, fields, wideevent.KeyDuration)
}

func TestFailureRecordsTheErrorAndWhetherItIsPermanent(t *testing.T) {
	failure := errors.New("mailbox does not exist")

	fields, err := run(context.Background(), t, func(context.Context, *jobs.Claim) error { return jobs.Permanent(failure) })

	require.ErrorIs(t, err, failure)
	require.Equal(t, wideevent.OutcomeFailure, fields[wideevent.KeyOutcome])
	require.Equal(t, "permanent: mailbox does not exist", fields[wideevent.KeyError])
	require.Equal(t, true, fields[wideeventadapter.KeyJobPermanent])
}

func TestSnoozeIsASuccess(t *testing.T) {
	fields, err := run(context.Background(), t, func(context.Context, *jobs.Claim) error { return jobs.Snooze(time.Minute) })

	delay, snoozed := jobs.SnoozeDelay(err)
	require.True(t, snoozed, "the snooze reaches the worker unchanged")
	require.Equal(t, time.Minute, delay)
	require.Equal(t, wideevent.OutcomeSuccess, fields[wideevent.KeyOutcome])
	require.Equal(t, time.Minute, fields[wideeventadapter.KeyJobSnoozed])
}

func TestPanicIsRecordedAndReturnedAsTheAttemptsError(t *testing.T) {
	fields, err := run(context.Background(), t, func(context.Context, *jobs.Claim) error { panic("template missing") })

	require.ErrorContains(t, err, "panicked: template missing")
	require.Equal(t, wideevent.OutcomeFailure, fields[wideevent.KeyOutcome])
}

func TestCancelCauseIsRecorded(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(jobs.ErrLeaseLost)

	fields, err := run(ctx, t, func(ctx context.Context, _ *jobs.Claim) error { return ctx.Err() })

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, jobs.ErrLeaseLost.Error(), fields[wideeventadapter.KeyJobCancelCause])
}
