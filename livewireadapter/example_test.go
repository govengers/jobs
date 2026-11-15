package livewireadapter_test

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/govengers/auth"
	"github.com/govengers/auth/authorization"
	"github.com/govengers/livewire"
	"github.com/govengers/livewire/query"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/jobs/livewireadapter"
	"github.com/govengers/jobs/postgresadapter"
)

// ownJobs is a Scope: an operator sees every job, any other identity the jobs it enqueued, by
// the identity snapshot authadapter.Identity() writes into the job's headers. The scope
// narrows the mutations too, so an operator reaches every job only because it sees every job.
func ownJobs(ctx context.Context) (query.Predicate, error) {
	identity := auth.IdentityFrom(ctx)
	if identity == nil {
		return nil, errors.New("jobs read without an identity")
	}
	if slices.Contains(identity.Roles, "operator") {
		return nil, nil //nolint:nilnil // a nil predicate is every job (Options.Scope)
	}
	return func(builder *query.Builder) {
		builder.WriteString("(govengers_jobs.headers->>'auth.identity')::jsonb->>'provider' = ").Argument(identity.Provider)
		builder.WriteString(" AND (govengers_jobs.headers->>'auth.identity')::jsonb->>'subject' = ").Argument(identity.Subject)
	}, nil
}

// The jobs of a livewire application: every user reads the jobs it started, operators read,
// cancel and retry every job. The migrations run livewire's TriggerFunctionSQL, then
// postgresadapter.LivewireTriggerSQL, so the subscriptions go live.
func ExampleDeclarations() {
	var (
		pool     *pgxpool.Pool          // the application's pool
		jobStore *postgresadapter.Store // postgresadapter.New on it, with authadapter.Identity() as propagator
		registry *livewire.Registry     // the application's registry
	)
	declarations, err := livewireadapter.Declarations(&livewireadapter.Options{
		Store: jobStore,
		Pool:  pool,
		Read:  authorization.IsUser(),
		Write: authorization.HasRole("operator"),
		Scope: ownJobs,
	})
	if err != nil {
		fmt.Println("declare the jobs:", err)
		return
	}
	if err := registry.Add(declarations...); err != nil {
		fmt.Println("register the jobs:", err)
	}
}
