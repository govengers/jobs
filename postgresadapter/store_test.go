package postgresadapter_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/internal/postgrestest"
	"github.com/govengers/jobs/postgresadapter"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

type mailPayload struct {
	Recipient string `json:"recipient"`
}

var mailKind = jobs.NewKind[mailPayload]("mail.send", &jobs.KindOptions{Queue: "mail", MaximumAttempts: 3})

const lease = time.Minute

func newStore(t *testing.T) (*postgresadapter.Store, *pgxpool.Pool) {
	t.Helper()
	pool := postgrestest.New(t)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool})
	require.NoError(t, err)
	return store, pool
}

func enqueue(t *testing.T, store *postgresadapter.Store, pool *pgxpool.Pool, options *jobs.EnqueueOptions) uuid.UUID {
	t.Helper()
	ids, err := store.Enqueue(context.Background(), pool, mailKind.New(mailPayload{Recipient: "ada@example.com"}, options))
	require.NoError(t, err)
	require.Len(t, ids, 1)
	return ids[0]
}

func claimOne(t *testing.T, store *postgresadapter.Store, duration time.Duration) *jobs.Claim {
	t.Helper()
	claims, err := store.Claim(context.Background(), &jobs.ClaimRequest{Queue: "mail", Limit: 1, Lease: duration, Worker: "test"})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	return claims[0]
}

func get(t *testing.T, store *postgresadapter.Store, id uuid.UUID) *postgresadapter.Record {
	t.Helper()
	record, err := store.Get(context.Background(), id)
	require.NoError(t, err)
	return record
}

func TestNewValidatesOptions(t *testing.T) {
	_, err := postgresadapter.New(&postgresadapter.Options{})
	require.Error(t, err)
	_, err = postgresadapter.New(nil)
	require.Error(t, err)
	_, err = postgresadapter.New(&postgresadapter.Options{Pool: &pgxpool.Pool{}, Propagators: []jobs.Propagator{nil}})
	require.ErrorContains(t, err, "Propagators[0] is nil")
}

// queryRowOnly is the smallest Executor: Enqueue needs nothing but QueryRow (DES-004).
type queryRowOnly struct{ pool *pgxpool.Pool }

func (e queryRowOnly) QueryRow(ctx context.Context, statement string, arguments ...any) pgx.Row {
	return e.pool.QueryRow(ctx, statement, arguments...)
}

func TestEnqueueNeedsOnlyQueryRow(t *testing.T) {
	store, pool := newStore(t)
	var executor postgresadapter.Executor = queryRowOnly{pool: pool}

	ids, err := store.Enqueue(context.Background(), executor, mailKind.New(mailPayload{}, &jobs.EnqueueOptions{UniqueKey: "once"}), mailKind.New(mailPayload{}, &jobs.EnqueueOptions{UniqueKey: "once"}))

	require.NoError(t, err)
	require.Equal(t, ids[0], ids[1])
}

func TestEnqueueAndClaim(t *testing.T) {
	store, pool := newStore(t)
	id := enqueue(t, store, pool, &jobs.EnqueueOptions{UniqueKey: "ada"})

	claim := claimOne(t, store, lease)

	require.Equal(t, id, claim.Lease.JobID)
	require.NotEqual(t, uuid.Nil, claim.Lease.Token)
	require.Equal(t, "mail.send", claim.Kind)
	require.Equal(t, 1, claim.Attempt)
	require.Equal(t, 3, claim.MaximumAttempts)
	require.JSONEq(t, `{"recipient":"ada@example.com"}`, string(claim.Payload))
	record := get(t, store, id)
	require.Equal(t, jobs.StatusRunning, record.Status)
	require.Equal(t, "test", record.Worker)
	require.Empty(t, record.UniqueKey, "claiming releases the unique key (J19)")
}

func TestEnqueueRollsBackWithTheTransaction(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	transaction, err := pool.Begin(ctx)
	require.NoError(t, err)
	ids, err := store.Enqueue(ctx, transaction, mailKind.New(mailPayload{}, nil))
	require.NoError(t, err)

	require.NoError(t, transaction.Rollback(ctx))

	_, err = store.Get(ctx, ids[0])
	require.ErrorIs(t, err, postgresadapter.ErrNotFound)
}

func TestEnqueueRejectsAnInvalidKind(t *testing.T) {
	store, pool := newStore(t)
	invalid := jobs.NewKind[mailPayload]("mail.send", nil)

	_, err := store.Enqueue(context.Background(), pool, invalid.New(mailPayload{}, nil))

	require.ErrorIs(t, err, jobs.ErrInvalidKind)
}

func TestEnqueueRunAtDelaysTheClaim(t *testing.T) {
	store, pool := newStore(t)
	enqueue(t, store, pool, &jobs.EnqueueOptions{RunAt: time.Now().Add(time.Hour)})

	claims, err := store.Claim(context.Background(), &jobs.ClaimRequest{Queue: "mail", Limit: 10, Lease: lease, Worker: "test"})

	require.NoError(t, err)
	require.Empty(t, claims)
}

func TestUniqueKeyCoalescesJobsThatHaveNotStarted(t *testing.T) {
	store, pool := newStore(t)
	first := enqueue(t, store, pool, &jobs.EnqueueOptions{UniqueKey: "sync-tenant-1"})

	second := enqueue(t, store, pool, &jobs.EnqueueOptions{UniqueKey: "sync-tenant-1"})
	require.Equal(t, first, second)

	claimOne(t, store, lease)
	third := enqueue(t, store, pool, &jobs.EnqueueOptions{UniqueKey: "sync-tenant-1"})
	require.NotEqual(t, first, third, "an enqueue during a run starts another run")
}

type tenantPropagator struct{}

type tenantKey struct{}

func (tenantPropagator) Inject(ctx context.Context, headers map[string]string) error {
	if tenant, ok := ctx.Value(tenantKey{}).(string); ok {
		headers["tenant"] = tenant
	}
	return nil
}

func (tenantPropagator) Extract(ctx context.Context, headers map[string]string) context.Context {
	return context.WithValue(ctx, tenantKey{}, headers["tenant"])
}

func TestEnqueueStoresPropagatedHeaders(t *testing.T) {
	pool := postgrestest.New(t)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, Propagators: []jobs.Propagator{tenantPropagator{}}})
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), tenantKey{}, "tenant-7")

	_, err = store.Enqueue(ctx, pool, mailKind.New(mailPayload{}, nil))
	require.NoError(t, err)

	require.Equal(t, map[string]string{"tenant": "tenant-7"}, claimOne(t, store, lease).Headers)
}

func TestConcurrentClaimsNeverShareAJob(t *testing.T) {
	store, pool := newStore(t)
	const jobCount = 60
	for range jobCount {
		enqueue(t, store, pool, nil)
	}
	var mutex sync.Mutex
	claimed := map[uuid.UUID]int{}
	group, ctx := errgroup.WithContext(context.Background())
	for worker := range 4 {
		group.Go(func() error {
			for {
				claims, err := store.Claim(ctx, &jobs.ClaimRequest{Queue: "mail", Limit: 3, Lease: lease, Worker: fmt.Sprintf("worker-%d", worker)})
				if err != nil || len(claims) == 0 {
					return err
				}
				mutex.Lock()
				for _, claim := range claims {
					claimed[claim.Lease.JobID]++
				}
				mutex.Unlock()
			}
		})
	}

	require.NoError(t, group.Wait())
	require.Len(t, claimed, jobCount)
	for id, count := range claimed {
		require.Equal(t, 1, count, "job %s claimed twice", id)
	}
}

func TestStaleFinishIsFencedAfterTheLeaseWasRescued(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	stale := claimOne(t, store, time.Microsecond)
	rescued, err := store.Rescue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, rescued)
	current := claimOne(t, store, lease)

	stored, err := store.Finish(ctx, &jobs.Finish{Lease: stale.Lease, Status: jobs.StatusCompleted})
	require.NoError(t, err)
	require.Empty(t, stored, "the stale worker must not overwrite the newer attempt")
	require.Equal(t, jobs.StatusRunning, get(t, store, id).Status)

	stored, err = store.Finish(ctx, &jobs.Finish{Lease: current.Lease, Status: jobs.StatusCompleted})
	require.NoError(t, err)
	require.Equal(t, jobs.StatusCompleted, stored)
	record := get(t, store, id)
	require.Equal(t, jobs.StatusCompleted, record.Status)
	require.Equal(t, 2, record.Attempt, "the rescued attempt counts")
	require.NotNil(t, record.FinishedAt)
}

func TestFinishRetryDelaysAndRecordsTheError(t *testing.T) {
	store, pool := newStore(t)
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)

	stored, err := store.Finish(context.Background(), &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusPending, Delay: time.Hour, Error: "upstream down"})

	require.NoError(t, err)
	require.Equal(t, jobs.StatusPending, stored)
	record := get(t, store, id)
	require.Equal(t, jobs.StatusPending, record.Status)
	require.Equal(t, 1, record.Attempt)
	require.Equal(t, "upstream down", record.LastError)
	require.InDelta(t, time.Hour.Seconds(), secondsUntil(t, pool, id, "run_at"), 60, "run_at is an hour after the database's now()")
	require.Nil(t, record.FinishedAt)
	require.Empty(t, record.Worker)
}

func TestFinishReturnAttemptDoesNotCount(t *testing.T) {
	store, pool := newStore(t)
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)

	_, err := store.Finish(context.Background(), &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusPending, ReturnAttempt: true})

	require.NoError(t, err)
	require.Equal(t, 0, get(t, store, id).Attempt)
	require.Equal(t, 1, claimOne(t, store, lease).Attempt)
}

func TestFinishDead(t *testing.T) {
	store, pool := newStore(t)
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)

	_, err := store.Finish(context.Background(), &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusDead, Error: "bad recipient"})

	require.NoError(t, err)
	record := get(t, store, id)
	require.Equal(t, jobs.StatusDead, record.Status)
	require.Equal(t, "bad recipient", record.LastError)
	require.NotNil(t, record.FinishedAt)
}

func TestExtendLeases(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	enqueue(t, store, pool, nil)
	held := claimOne(t, store, lease)
	foreign := jobs.Lease{JobID: held.Lease.JobID, Token: uuid.New()}

	require.InDelta(t, lease.Seconds(), secondsUntil(t, pool, held.Lease.JobID, "lease_until"), 30)

	leases, err := store.ExtendLeases(ctx, []jobs.Lease{held.Lease, foreign}, time.Hour)
	require.NoError(t, err)
	require.Equal(t, []jobs.HeldLease{{Lease: held.Lease}}, leases)
	require.InDelta(t, time.Hour.Seconds(), secondsUntil(t, pool, held.Lease.JobID, "lease_until"), 30, "the extension moved lease_until")

	require.NoError(t, store.Cancel(ctx, held.Lease.JobID))
	leases, err = store.ExtendLeases(ctx, []jobs.Lease{held.Lease}, lease)
	require.NoError(t, err)
	require.Equal(t, []jobs.HeldLease{{Lease: held.Lease, CancelRequested: true}}, leases)

	leases, err = store.ExtendLeases(ctx, nil, lease)
	require.NoError(t, err)
	require.Empty(t, leases)
}

func TestFinishPendingOfACancelledJobCancelsIt(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	id := enqueue(t, store, pool, nil)
	claim := claimOne(t, store, lease)
	require.NoError(t, store.Cancel(ctx, id))

	stored, err := store.Finish(ctx, &jobs.Finish{Lease: claim.Lease, Status: jobs.StatusPending, Delay: time.Minute, ReturnAttempt: true})

	require.NoError(t, err)
	require.Equal(t, jobs.StatusCancelled, stored, "Finish returns the status it stored, not the one it was asked for")
	record := get(t, store, id)
	require.Equal(t, jobs.StatusCancelled, record.Status)
	require.NotNil(t, record.FinishedAt)
}

func TestRescue(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	retried := enqueue(t, store, pool, nil)
	claimOne(t, store, time.Microsecond)
	exhausted, err := store.Enqueue(ctx, pool, jobs.NewKind[mailPayload]("mail.once", &jobs.KindOptions{Queue: "mail", MaximumAttempts: 1}).New(mailPayload{}, nil))
	require.NoError(t, err)
	claimOne(t, store, time.Microsecond)
	cancelled := enqueue(t, store, pool, nil)
	claimOne(t, store, time.Microsecond)
	require.NoError(t, store.Cancel(ctx, cancelled))
	healthy := enqueue(t, store, pool, nil)
	claimOne(t, store, lease)

	rescued, err := store.Rescue(ctx, 10)

	require.NoError(t, err)
	require.Equal(t, 3, rescued)
	require.Equal(t, jobs.StatusPending, get(t, store, retried).Status)
	require.Equal(t, 1, get(t, store, retried).Attempt)
	require.Contains(t, get(t, store, retried).LastError, "lease expired")
	require.Equal(t, jobs.StatusDead, get(t, store, exhausted[0]).Status)
	require.Equal(t, jobs.StatusCancelled, get(t, store, cancelled).Status)
	require.Equal(t, jobs.StatusRunning, get(t, store, healthy).Status)
}

func TestSweep(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	completed := enqueue(t, store, pool, nil)
	finish(t, store, jobs.StatusCompleted)
	dead := enqueue(t, store, pool, nil)
	finish(t, store, jobs.StatusDead)
	pending := enqueue(t, store, pool, nil)
	cancelled := enqueue(t, store, pool, &jobs.EnqueueOptions{RunAt: time.Now().Add(time.Hour)})
	require.NoError(t, store.Cancel(ctx, cancelled))

	deleted, err := store.Sweep(ctx, &jobs.SweepRequest{RetainCompleted: 0, RetainDead: time.Hour, Limit: 100})

	require.NoError(t, err)
	require.Equal(t, 2, deleted, "the completed and the cancelled job")
	_, err = store.Get(ctx, cancelled)
	require.ErrorIs(t, err, postgresadapter.ErrNotFound)
	_, err = store.Get(ctx, completed)
	require.ErrorIs(t, err, postgresadapter.ErrNotFound)
	require.Equal(t, jobs.StatusDead, get(t, store, dead).Status)
	require.Equal(t, jobs.StatusPending, get(t, store, pending).Status)
}

func finish(t *testing.T, store *postgresadapter.Store, status jobs.Status) {
	t.Helper()
	claim := claimOne(t, store, lease)
	stored, err := store.Finish(context.Background(), &jobs.Finish{Lease: claim.Lease, Status: status})
	require.NoError(t, err)
	require.Equal(t, status, stored)
}

func TestSchedulesEnqueueOncePerPeriodAcrossWorkers(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, "INSERT INTO govengers_job_schedules (name, next_run_at) VALUES ('nightly-report', now() - interval '1 minute')")
	require.NoError(t, err)
	schedule := jobs.Schedule{
		Name: "nightly-report",
		Next: func(now time.Time) time.Time { return now.Add(time.Hour) },
		Job:  mailKind.New(mailPayload{Recipient: "reports@example.com"}, nil),
	}
	registered := jobs.Schedule{Name: "registered-later", Next: schedule.Next, Job: schedule.Job}
	var total sync.Map
	group, groupContext := errgroup.WithContext(ctx)
	for worker := range 8 {
		group.Go(func() error {
			count, err := store.EnqueueDueSchedules(groupContext, []jobs.Schedule{schedule, registered})
			total.Store(worker, count)
			return err
		})
	}
	require.NoError(t, group.Wait())

	enqueued := 0
	total.Range(func(_, count any) bool {
		enqueued += count.(int)
		return true
	})
	require.Equal(t, 1, enqueued)
	records, err := store.List(ctx, &postgresadapter.ListFilter{Queue: "mail"})
	require.NoError(t, err)
	require.Len(t, records, 1)
	var dueAgain bool
	require.NoError(t, pool.QueryRow(ctx, "SELECT bool_or(next_run_at <= now()) FROM govengers_job_schedules").Scan(&dueAgain))
	require.False(t, dueAgain, "both schedules wait for their next period")
}

// Finding 7: a Next that does not move forward fails the transaction instead of storing a
// schedule that stays due.
func TestScheduleWithANonFutureNextIsRefused(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, "INSERT INTO govengers_job_schedules (name, next_run_at) VALUES ('stuck', now() - interval '1 minute')")
	require.NoError(t, err)
	stuck := jobs.Schedule{Name: "stuck", Next: func(now time.Time) time.Time { return now.Add(-time.Hour) }, Job: mailKind.New(mailPayload{}, nil)}

	enqueued, err := store.EnqueueDueSchedules(ctx, []jobs.Schedule{stuck})

	require.ErrorContains(t, err, "schedule stuck")
	require.Zero(t, enqueued)
	records, err := store.List(ctx, &postgresadapter.ListFilter{})
	require.NoError(t, err)
	require.Empty(t, records, "the transaction rolled back")
}

// secondsUntil reads how far a timestamp column of the job lies after the database's now(), so
// tests compare database times with database time (TST-005).
func secondsUntil(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, column string) float64 {
	t.Helper()
	var seconds float64
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT EXTRACT(EPOCH FROM "+column+" - now())::float8 FROM govengers_jobs WHERE id = $1", id).Scan(&seconds))
	return seconds
}

type largePayload struct {
	Data string `json:"data"`
}

// Finding 19: kinds and payloads are checked before the first insert; a database error later
// returns the IDs inserted so far.
func TestEnqueueChecksEveryPayloadBeforeTheFirstInsert(t *testing.T) {
	store, pool := newStore(t)
	unencodable := jobs.NewKind[func()]("broken", &jobs.KindOptions{Queue: "mail"}).New(func() {}, nil)

	ids, err := store.Enqueue(context.Background(), pool, mailKind.New(mailPayload{}, nil), unencodable)

	require.Error(t, err)
	require.Empty(t, ids)
	records, err := store.List(context.Background(), &postgresadapter.ListFilter{})
	require.NoError(t, err)
	require.Empty(t, records, "nothing was inserted")
}

func TestEnqueueReturnsTheIDsInsertedBeforeADatabaseError(t *testing.T) {
	store, pool := newStore(t)
	random := make([]byte, 600_000)
	_, err := rand.Read(random)
	require.NoError(t, err)
	tooLarge := jobs.NewKind[largePayload]("large", &jobs.KindOptions{Queue: "mail"}).New(largePayload{Data: base64.StdEncoding.EncodeToString(random)}, nil)

	ids, err := store.Enqueue(context.Background(), pool, mailKind.New(mailPayload{}, nil), tooLarge)

	require.Error(t, err, "the payload breaks the 256 KiB CHECK")
	require.Len(t, ids, 1, "on the pool the first job stays inserted")
	get(t, store, ids[0])
}

// Finding 20: under REPEATABLE READ a unique key that a concurrent enqueue took is a
// serialization failure the caller can match and retry.
func TestUniqueKeyUnderRepeatableReadIsASerializationFailure(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	transaction, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	require.NoError(t, err)
	defer func() { _ = transaction.Rollback(ctx) }() // the failed transaction is aborted anyway
	var snapshot time.Time
	require.NoError(t, transaction.QueryRow(ctx, "SELECT now()").Scan(&snapshot))
	enqueue(t, store, pool, &jobs.EnqueueOptions{UniqueKey: "sync-tenant-1"})

	_, err = store.Enqueue(ctx, transaction, mailKind.New(mailPayload{}, &jobs.EnqueueOptions{UniqueKey: "sync-tenant-1"}))

	require.ErrorIs(t, err, postgresadapter.ErrSerializationFailure)
}

type failingPropagator struct{}

func (failingPropagator) Inject(context.Context, map[string]string) error {
	return errors.New("cannot carry the actor")
}

func (failingPropagator) Extract(ctx context.Context, _ map[string]string) context.Context {
	return ctx
}

// Finding 18: a value a propagator cannot carry fails the enqueue; nothing is inserted.
func TestEnqueueFailsWhenAPropagatorCannotCarryItsValue(t *testing.T) {
	pool := postgrestest.New(t)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, Propagators: []jobs.Propagator{failingPropagator{}}})
	require.NoError(t, err)

	ids, err := store.Enqueue(context.Background(), pool, mailKind.New(mailPayload{}, nil))

	require.ErrorContains(t, err, "cannot carry the actor")
	require.Empty(t, ids)
	records, err := store.List(context.Background(), &postgresadapter.ListFilter{})
	require.NoError(t, err)
	require.Empty(t, records)
}

// Round 2, finding 5: a Next one nanosecond after now is now at the database's microsecond
// precision, so it is refused like any Next that does not move forward.
func TestScheduleWithANanosecondNextIsRefused(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, "INSERT INTO govengers_job_schedules (name, next_run_at) VALUES ('tick', now() - interval '1 minute')")
	require.NoError(t, err)
	tick := jobs.Schedule{Name: "tick", Next: func(now time.Time) time.Time { return now.Add(time.Nanosecond) }, Job: mailKind.New(mailPayload{}, nil)}

	enqueued, err := store.EnqueueDueSchedules(ctx, []jobs.Schedule{tick})

	require.ErrorContains(t, err, "schedule tick")
	require.Zero(t, enqueued)
}

// Round 2, finding 7: every schedule has its own transaction; a refused one does not roll back
// the others.
func TestAFailingScheduleDoesNotHoldBackTheOthers(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, "INSERT INTO govengers_job_schedules (name, next_run_at) VALUES ('broken', now() - interval '1 minute'), ('healthy', now() - interval '1 minute')")
	require.NoError(t, err)
	broken := jobs.Schedule{Name: "broken", Next: func(now time.Time) time.Time { return now }, Job: mailKind.New(mailPayload{}, nil)}
	healthy := jobs.Schedule{Name: "healthy", Next: func(now time.Time) time.Time { return now.Add(time.Hour) }, Job: mailKind.New(mailPayload{Recipient: "healthy"}, nil)}

	enqueued, err := store.EnqueueDueSchedules(ctx, []jobs.Schedule{broken, healthy})

	require.ErrorContains(t, err, "schedule broken")
	require.NotContains(t, err.Error(), "schedule healthy")
	require.Equal(t, 1, enqueued)
	records, err := store.List(ctx, &postgresadapter.ListFilter{})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.JSONEq(t, `{"recipient":"healthy"}`, string(records[0].Payload))
}
