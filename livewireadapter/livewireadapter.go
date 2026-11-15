// Package livewireadapter declares the jobs of a postgresadapter.Store for a govengers/livewire
// registry (J57): the read-only resource "jobs" of postgresadapter.View (list items without
// the result, the item read with it), live once the
// migrations ran postgresadapter.LivewireTriggerSQL, and the mutations "cancelJob" and
// "retryJob". The application decides who may read and who may write; the declarations bring
// no authorization of their own.
package livewireadapter

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/govengers/auth/authorization"
	"github.com/govengers/livewire"
	livewirepostgres "github.com/govengers/livewire/postgresadapter"
	"github.com/govengers/livewire/query"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

// The names of the declarations: the socket targets and the identifiers of the generated
// clients ("jobs.item" is the item read).
const (
	ResourceName = "jobs"
	CancelName   = "cancelJob"
	RetryName    = "retryJob"
)

// DefaultPath is the path of the resource when Options.Path is empty; the mutations are
// POST Path/{id}/cancel and POST Path/{id}/retry.
const DefaultPath = "/jobs"

// Table is the table the resource reads; postgresadapter.LivewireTriggerSQL makes it notify.
const Table = "govengers_jobs"

// Options configures the declarations.
type Options struct {
	Store *postgresadapter.Store // required
	Pool  *pgxpool.Pool          // required: the store's pool, for reads outside an engine unit of work
	Path  string                 // the resource's path; empty means DefaultPath
	// Read decides who may read jobs; required. It is static (roles, permissions), like every
	// livewire Check; narrow the rows with Scope.
	Read authorization.Check
	// Write decides who may cancel and retry jobs. Nil declares no mutation: the jobs are read
	// only. Write implies reading the job changed: a mutation answers the job's View, whether
	// or not Read passes; Scope narrows the mutations like the reads.
	Write authorization.Check
	// Scope narrows the jobs an identity reads and may cancel or retry, as a predicate over
	// govengers_jobs (a queue, a kind, a header); a job outside it is not found. Nil, or a nil
	// predicate, is every job.
	Scope func(ctx context.Context) (query.Predicate, error)
}

// Declarations returns the resource and, with a Write check, the two mutations, for
// Registry.Add. It fails without a store, a pool or a Read check.
func Declarations(options *Options) ([]livewire.Declaration, error) {
	if options == nil || options.Store == nil || options.Pool == nil || options.Read == nil {
		return nil, errors.New("livewireadapter: Options.Store, Pool and Read are required")
	}
	adapter := &adapter{options: *options}
	if adapter.options.Path == "" {
		adapter.options.Path = DefaultPath
	}
	declarations := []livewire.Declaration{adapter.resource()}
	if options.Write != nil {
		declarations = append(declarations,
			adapter.mutation(CancelName, "/cancel", adapter.options.Store.CancelOn),
			adapter.mutation(RetryName, "/retry", adapter.options.Store.RetryOn))
	}
	return declarations, nil
}

type adapter struct {
	options Options
}

// jobsSQL is what the reads of the resource read from.
var jobsSQL = &query.ResourceSQL{From: Table, IDColumn: Table + ".id"}

// stateSQL maps the stored status to postgresadapter.StateOf in SQL, so a filter by status
// uses the names the items carry.
const stateSQL = `CASE govengers_jobs.status WHEN 'pending' THEN 'queued' WHEN 'running' THEN 'running'
    WHEN 'completed' THEN 'succeeded' WHEN 'dead' THEN 'failed' ELSE 'cancelled' END`

// fields are the filterable and sortable fields of the resource, by the JSON names of View.
// updatedAt is not one: the heartbeat moves it without a notification, so a live list sorted
// or filtered by it would not follow.
func fields() query.Fields {
	return query.Fields{
		{Name: "id", Type: query.TypeUUID, SQL: Table + ".id", Filter: true, Sort: true},
		{Name: "queue", Type: query.TypeString, SQL: Table + ".queue", Filter: true, Sort: true},
		{Name: "kind", Type: query.TypeString, SQL: Table + ".kind", Filter: true, Sort: true, Search: true},
		{Name: "status", Type: query.TypeEnum, SQL: stateSQL, Filter: true, Sort: true, Enum: []string{
			string(postgresadapter.StateQueued), string(postgresadapter.StateRunning), string(postgresadapter.StateSucceeded),
			string(postgresadapter.StateFailed), string(postgresadapter.StateCancelled),
		}},
		{Name: "attempt", Type: query.TypeInteger, SQL: Table + ".attempt", Filter: true, Sort: true},
		{Name: "errorClass", Type: query.TypeEnum, SQL: Table + ".error_class", Nullable: true, Filter: true, Enum: []string{
			string(jobs.ErrorClassTransient), string(jobs.ErrorClassPermanent), string(jobs.ErrorClassLeaseExpired),
		}},
		{Name: "createdAt", Type: query.TypeTime, SQL: Table + ".created_at", Filter: true, Sort: true},
		{Name: "runAt", Type: query.TypeTime, SQL: Table + ".run_at", Filter: true, Sort: true},
		{Name: "startedAt", Type: query.TypeTime, SQL: Table + ".started_at", Nullable: true, Filter: true, Sort: true},
		{Name: "finishedAt", Type: query.TypeTime, SQL: Table + ".finished_at", Nullable: true, Filter: true, Sort: true},
	}
}

func (a *adapter) resource() *livewire.Resource {
	return livewire.NewResource(ResourceName, a.options.Path, &livewire.ResourceOptions[postgresadapter.View]{
		Tables:      []string{Table},
		Read:        a.options.Read,
		Fields:      fields(),
		DefaultSort: []query.SortTerm{{Field: "createdAt", Descending: true}},
		Scope:       a.options.Scope,
		Item:        &livewire.ItemKey{Parameter: "id", Field: "id"},
		List:        a.list,
		Get:         a.get,
	})
}

// list answers the views without their results; the item read carries the result.
func (a *adapter) list(ctx context.Context, listQuery *query.ListQuery) (*livewire.Page[postgresadapter.View], error) {
	return livewirepostgres.ListPage(ctx, a.options.Pool, jobsSQL, listQuery,
		func(ctx context.Context, database livewirepostgres.DBTX, rows []livewirepostgres.PageRow) (map[uuid.UUID]postgresadapter.View, error) {
			return a.options.Store.ListViewsOn(ctx, database, livewirepostgres.PageIDs(rows))
		})
}

func (a *adapter) get(ctx context.Context, listQuery *query.ListQuery) (*postgresadapter.View, error) {
	page, err := livewirepostgres.ListPage(ctx, a.options.Pool, jobsSQL, listQuery,
		func(ctx context.Context, database livewirepostgres.DBTX, rows []livewirepostgres.PageRow) (map[uuid.UUID]postgresadapter.View, error) {
			return a.options.Store.ViewsOn(ctx, database, livewirepostgres.PageIDs(rows))
		})
	if err != nil {
		return nil, err
	}
	if len(page.Items) == 0 {
		return nil, livewire.NotFound()
	}
	return &page.Items[0], nil
}

// jobParameters names the job of a mutation.
type jobParameters struct {
	ID uuid.UUID `path:"id"`
}

// change is CancelOn or RetryOn of the store.
type change func(ctx context.Context, executor postgresadapter.Executor, id uuid.UUID) error

// mutation declares POST Path/{id}<suffix>: change on the engine's transaction, answered
// with the job's view, which the live lists of the resource upsert at once.
func (a *adapter) mutation(name, suffix string, apply change) *livewire.Mutation {
	return livewire.NewMutation(name, http.MethodPost, a.options.Path+"/{id}"+suffix, &livewire.MutationOptions[jobParameters, livewire.NoBody, postgresadapter.View]{
		Check:  a.options.Write,
		Effect: livewire.Upserts(ResourceName),
		Handle: func(ctx context.Context, parameters *jobParameters, _ *livewire.NoBody) (*postgresadapter.View, error) {
			database := a.database(ctx)
			if err := a.requireInScope(ctx, database, parameters.ID); err != nil {
				return nil, err
			}
			if err := apply(ctx, database, parameters.ID); err != nil {
				return nil, responseError(err)
			}
			views, err := a.options.Store.ViewsOn(ctx, database, []uuid.UUID{parameters.ID})
			if err != nil {
				return nil, err
			}
			view, ok := views[parameters.ID]
			if !ok {
				return nil, livewire.NotFound()
			}
			return &view, nil
		},
	})
}

// database is the engine's transaction, or the pool outside an engine unit of work.
func (a *adapter) database(ctx context.Context) livewirepostgres.DBTX {
	if transaction := livewirepostgres.TransactionFrom(ctx); transaction != nil {
		return transaction
	}
	return a.options.Pool
}

// requireInScope answers NotFound for a job the identity may not read, so a mutation reaches
// exactly the jobs the resource shows it.
func (a *adapter) requireInScope(ctx context.Context, database livewirepostgres.DBTX, id uuid.UUID) error {
	if a.options.Scope == nil {
		return nil
	}
	predicate, err := a.options.Scope(ctx)
	if err != nil {
		return err
	}
	if predicate == nil {
		return nil
	}
	var builder query.Builder
	builder.WriteString("SELECT EXISTS (SELECT 1 FROM " + Table + " WHERE " + Table + ".id = ").Argument(id).WriteString(" AND (")
	predicate(&builder)
	builder.WriteString("))")
	statement, arguments := builder.Query()
	var visible bool
	if err := database.QueryRow(ctx, statement, arguments...).Scan(&visible); err != nil {
		return err
	}
	if !visible {
		return livewire.NotFound()
	}
	return nil
}

// responseError maps the store's refusals to livewire's: a missing job is 404, a job in the
// wrong status (cancelling a finished job, retrying one that is not dead) is 409.
func responseError(err error) error {
	switch {
	case errors.Is(err, postgresadapter.ErrNotFound):
		return livewire.NotFound()
	case errors.Is(err, postgresadapter.ErrFinished), errors.Is(err, postgresadapter.ErrNotDead):
		return livewire.Conflict()
	default:
		return err
	}
}
