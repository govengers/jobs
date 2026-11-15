package livewireadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govengers/auth"
	"github.com/govengers/auth/authorization"
	"github.com/govengers/livewire"
	livewirepostgres "github.com/govengers/livewire/postgresadapter"
	"github.com/govengers/livewire/query"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/authadapter"
	"github.com/govengers/jobs/internal/postgrestest"
	"github.com/govengers/jobs/livewireadapter"
	"github.com/govengers/jobs/postgresadapter"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

type mailPayload struct {
	Recipient string `json:"recipient"`
}

var (
	mailKind   = jobs.NewKind[mailPayload]("mail.send", &jobs.KindOptions{Queue: "mail", MaximumAttempts: 1})
	reportKind = jobs.NewKind[mailPayload]("report.build", &jobs.KindOptions{Queue: "reports"})
	operator   = &auth.Identity{Provider: "password", Subject: "ada", Name: "Ada", Active: true, Roles: []string{"operator"}}
	viewer     = &auth.Identity{Provider: "password", Subject: "bob", Name: "Bob", Active: true}
)

// identityOfContext is the engine's IdentitySource; the tests call the engine in process.
type identityOfContext struct{}

func (identityOfContext) Identify(ctx context.Context, _ *http.Request) (*auth.Identity, error) {
	return auth.IdentityFrom(ctx), nil
}

type fixture struct {
	t      *testing.T
	pool   *pgxpool.Pool
	store  *postgresadapter.Store
	engine *livewire.Engine
}

func newFixture(t *testing.T, options *livewireadapter.Options) *fixture {
	t.Helper()
	pool := postgrestest.New(t)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, Propagators: []jobs.Propagator{authadapter.Identity()}})
	require.NoError(t, err)
	options.Store, options.Pool = store, pool
	declarations, err := livewireadapter.Declarations(options)
	require.NoError(t, err)
	registry, err := livewire.NewRegistry(&livewire.RegistryOptions{})
	require.NoError(t, err)
	require.NoError(t, registry.Add(declarations...))
	transactor, err := livewirepostgres.NewTransactor(&livewirepostgres.TransactorOptions{Pool: pool})
	require.NoError(t, err)
	engine, err := livewire.NewEngine(&livewire.EngineOptions{
		Registry: registry, IdentitySource: identityOfContext{}, Transactor: transactor, SameHostOnly: true,
	})
	require.NoError(t, err)
	return &fixture{t: t, pool: pool, store: store, engine: engine}
}

func newOperatorFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixture(t, &livewireadapter.Options{Read: authorization.IsUser(), Write: authorization.HasRole("operator")})
}

func as(identity *auth.Identity) context.Context {
	return auth.WithIdentity(context.Background(), identity.Clone())
}

func (f *fixture) enqueue(job *jobs.PreparedJob) uuid.UUID {
	f.t.Helper()
	return f.enqueueAs(context.Background(), job)
}

func (f *fixture) enqueueAs(ctx context.Context, job *jobs.PreparedJob) uuid.UUID {
	f.t.Helper()
	ids, err := f.store.Enqueue(ctx, f.pool, job)
	require.NoError(f.t, err)
	return ids[0]
}

// claim claims the next job of queue and finishes it as status.
func (f *fixture) finishNext(queue string, status jobs.Status) {
	f.t.Helper()
	claims, err := f.store.Claim(context.Background(), &jobs.ClaimRequest{Queue: queue, Limit: 1, LeaseDuration: time.Minute, Worker: "test"})
	require.NoError(f.t, err)
	require.Len(f.t, claims, 1)
	_, err = f.store.Finish(context.Background(), &jobs.Finish{Lease: claims[0].Lease, Status: status, Error: "boom", ErrorClass: jobs.ErrorClassPermanent})
	require.NoError(f.t, err)
}

func (f *fixture) list(ctx context.Context, parameters livewire.MapSource) (livewire.ListResponse[postgresadapter.View], error) {
	var response livewire.ListResponse[postgresadapter.View]
	payload, err := f.engine.Read(ctx, livewire.ReadRequest{Target: livewireadapter.ResourceName, Source: parameters})
	if err != nil {
		return response, err
	}
	return response, json.Unmarshal(payload, &response)
}

func (f *fixture) item(ctx context.Context, id uuid.UUID) (postgresadapter.View, error) {
	var view postgresadapter.View
	payload, err := f.engine.Read(ctx, livewire.ReadRequest{Target: livewireadapter.ResourceName, Kind: livewire.ReadItem, Source: livewire.MapSource{"id": id.String()}})
	if err != nil {
		return view, err
	}
	return view, json.Unmarshal(payload, &view)
}

func (f *fixture) call(ctx context.Context, target string, id uuid.UUID) (postgresadapter.View, error) {
	var view postgresadapter.View
	_, payload, err := f.engine.Call(ctx, livewire.CallRequest{Target: target, Source: livewire.MapSource{"id": id.String()}})
	if err != nil {
		return view, err
	}
	return view, json.Unmarshal(payload, &view)
}

func requireStatus(t *testing.T, err error, status int) {
	t.Helper()
	require.Error(t, err)
	got, _ := livewire.ResponseFor(err)
	require.Equal(t, status, got, "%v", err)
}

func TestDeclarationsNeedStorePoolAndRead(t *testing.T) {
	_, err := livewireadapter.Declarations(nil)
	require.Error(t, err)
	_, err = livewireadapter.Declarations(&livewireadapter.Options{Read: authorization.IsUser()})
	require.Error(t, err)
}

func TestTheResourceListsAndReadsJobViews(t *testing.T) {
	fixture := newOperatorFixture(t)
	mail := fixture.enqueue(mailKind.New(mailPayload{Recipient: "ada@example.com"}, nil))
	report := fixture.enqueue(reportKind.New(mailPayload{}, nil))
	fixture.finishNext("reports", jobs.StatusCompleted)

	page, err := fixture.list(as(viewer), livewire.MapSource{})
	require.NoError(t, err)
	require.Equal(t, 2, page.Total)
	require.Equal(t, []uuid.UUID{report, mail}, []uuid.UUID{page.Items[0].ID, page.Items[1].ID}, "newest first")
	require.Equal(t, postgresadapter.StateSucceeded, page.Items[0].Status)
	require.Equal(t, postgresadapter.StateQueued, page.Items[1].Status)

	succeeded, err := fixture.list(as(viewer), livewire.MapSource{"filter": `{"where": {"field": "status", "op": "eq", "value": "succeeded"}}`})
	require.NoError(t, err)
	require.Len(t, succeeded.Items, 1)
	require.Equal(t, report, succeeded.Items[0].ID)

	view, err := fixture.item(as(viewer), mail)
	require.NoError(t, err)
	require.Equal(t, "mail.send", view.Kind)
	require.Equal(t, "mail", view.Queue)

	_, err = fixture.item(as(viewer), uuid.New())
	requireStatus(t, err, http.StatusNotFound)
}

// A list item leaves the result out, the item read carries it; a job without an error has a
// null error class, the value the errorClass filter matches with isNull.
func TestListItemsLeaveTheResultToTheItemRead(t *testing.T) {
	fixture := newOperatorFixture(t)
	id := fixture.enqueue(reportKind.New(mailPayload{}, nil))
	claims, err := fixture.store.Claim(context.Background(), &jobs.ClaimRequest{Queue: "reports", Limit: 1, LeaseDuration: time.Minute, Worker: "test"})
	require.NoError(t, err)
	_, err = fixture.store.Finish(context.Background(), &jobs.Finish{Lease: claims[0].Lease, Status: jobs.StatusCompleted, Result: json.RawMessage(`{"pages": 12}`)})
	require.NoError(t, err)

	payload, err := fixture.engine.Read(as(viewer), livewire.ReadRequest{Target: livewireadapter.ResourceName, Source: livewire.MapSource{}})
	require.NoError(t, err)
	require.Contains(t, string(payload), `"result":null`)
	require.Contains(t, string(payload), `"errorClass":null`)
	withoutError, err := fixture.list(as(viewer), livewire.MapSource{"filter": `{"where": {"field": "errorClass", "op": "isNull", "value": true}}`})
	require.NoError(t, err)
	require.Len(t, withoutError.Items, 1)

	view, err := fixture.item(as(viewer), id)
	require.NoError(t, err)
	require.JSONEq(t, `{"pages": 12}`, string(view.Result))
}

func TestTheReadCheckIsTheApplications(t *testing.T) {
	fixture := newFixture(t, &livewireadapter.Options{Read: authorization.HasRole("operator")})

	_, err := fixture.list(as(viewer), livewire.MapSource{})

	requireStatus(t, err, http.StatusForbidden)
	_, err = fixture.list(as(operator), livewire.MapSource{})
	require.NoError(t, err)
}

func TestWithoutAWriteCheckTheJobsAreReadOnly(t *testing.T) {
	fixture := newFixture(t, &livewireadapter.Options{Read: authorization.IsUser()})
	id := fixture.enqueue(mailKind.New(mailPayload{}, nil))

	_, err := fixture.call(as(operator), livewireadapter.CancelName, id)

	require.Error(t, err)
	require.Equal(t, jobs.StatusPending, fixture.record(id).Status)
}

func TestCancelAndRetryAnswerTheView(t *testing.T) {
	fixture := newOperatorFixture(t)
	pending := fixture.enqueue(mailKind.New(mailPayload{}, nil))

	_, err := fixture.call(as(viewer), livewireadapter.CancelName, pending)
	requireStatus(t, err, http.StatusForbidden)

	view, err := fixture.call(as(operator), livewireadapter.CancelName, pending)
	require.NoError(t, err)
	require.Equal(t, postgresadapter.StateCancelled, view.Status)

	_, err = fixture.call(as(operator), livewireadapter.CancelName, pending)
	requireStatus(t, err, http.StatusConflict)
	_, err = fixture.call(as(operator), livewireadapter.RetryName, pending)
	requireStatus(t, err, http.StatusConflict)
	_, err = fixture.call(as(operator), livewireadapter.RetryName, uuid.New())
	requireStatus(t, err, http.StatusNotFound)

	dead := fixture.enqueue(mailKind.New(mailPayload{}, nil))
	fixture.finishNext("mail", jobs.StatusDead)
	view, err = fixture.call(as(operator), livewireadapter.RetryName, dead)
	require.NoError(t, err)
	require.Equal(t, postgresadapter.StateQueued, view.Status)
	require.Empty(t, view.ErrorClass)
}

func TestTheScopeNarrowsReadsAndMutations(t *testing.T) {
	onlyMail := func(context.Context) (query.Predicate, error) {
		return func(builder *query.Builder) { builder.WriteString("govengers_jobs.queue = ").Argument("mail") }, nil
	}
	fixture := newFixture(t, &livewireadapter.Options{Read: authorization.IsUser(), Write: authorization.IsUser(), Scope: onlyMail})
	mail := fixture.enqueue(mailKind.New(mailPayload{}, nil))
	report := fixture.enqueue(reportKind.New(mailPayload{}, nil))

	page, err := fixture.list(as(viewer), livewire.MapSource{})
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, mail, page.Items[0].ID)
	_, err = fixture.item(as(viewer), report)
	requireStatus(t, err, http.StatusNotFound)

	_, err = fixture.call(as(viewer), livewireadapter.CancelName, report)
	requireStatus(t, err, http.StatusNotFound)
	require.Equal(t, jobs.StatusPending, fixture.record(report).Status, "a job outside the scope is untouched")

	_, err = fixture.call(as(viewer), livewireadapter.CancelName, mail)
	require.NoError(t, err)
}

func TestAFailingScopeFailsTheMutation(t *testing.T) {
	broken := errors.New("no tenant")
	fixture := newFixture(t, &livewireadapter.Options{
		Read: authorization.IsUser(), Write: authorization.IsUser(),
		Scope: func(context.Context) (query.Predicate, error) { return nil, broken },
	})
	id := fixture.enqueue(mailKind.New(mailPayload{}, nil))

	_, err := fixture.call(as(viewer), livewireadapter.CancelName, id)

	requireStatus(t, err, http.StatusInternalServerError)
	require.Equal(t, jobs.StatusPending, fixture.record(id).Status)
}

func (f *fixture) record(id uuid.UUID) *postgresadapter.Record {
	f.t.Helper()
	record, err := f.store.Get(context.Background(), id)
	require.NoError(f.t, err)
	return record
}

// The scope of the example reaches the jobs an identity enqueued, through authadapter's
// identity snapshot, and every job for an operator.
func TestTheExampleScopeShowsTheJobsAnIdentityStarted(t *testing.T) {
	fixture := newFixture(t, &livewireadapter.Options{Read: authorization.IsUser(), Write: authorization.IsUser(), Scope: ownJobs})
	own := fixture.enqueueAs(as(viewer), mailKind.New(mailPayload{}, nil))
	other := fixture.enqueueAs(as(operator), mailKind.New(mailPayload{}, nil))
	fixture.enqueue(mailKind.New(mailPayload{}, nil)) // a job without an identity, such as a schedule's

	page, err := fixture.list(as(viewer), livewire.MapSource{})
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, own, page.Items[0].ID)
	_, err = fixture.call(as(viewer), livewireadapter.CancelName, other)
	requireStatus(t, err, http.StatusNotFound)

	page, err = fixture.list(as(operator), livewire.MapSource{})
	require.NoError(t, err)
	require.Len(t, page.Items, 3, "an operator sees every job")
	_, err = fixture.call(as(operator), livewireadapter.CancelName, own)
	require.NoError(t, err, "and cancels the jobs of others")
}
