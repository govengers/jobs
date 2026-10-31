// Package authadapter carries the identity of github.com/govengers/auth from the request that
// enqueues a job to the job's handler (J15): a job a user started runs, and audits, as that
// user.
package authadapter

import (
	"context"
	"encoding/json"

	"github.com/govengers/auth"

	"github.com/govengers/jobs"
)

// HeaderIdentity is the job header that holds the identity as JSON.
const HeaderIdentity = "auth.identity"

// Identity returns the jobs.Propagator for the identity. Configure it on both sides, in
// postgresadapter.Options.Propagators and jobs.WorkerOptions.Propagators.
//
// Inject stores the JSON snapshot of auth.IdentityFrom(ctx); without an identity it stores
// nothing. Extract restores it with auth.WithIdentity. The snapshot is the identity at enqueue
// time: roles and permissions revoked later still apply to the job, so a handler that must
// honor a revocation checks the user again. A header that does not decode leaves the context
// without an identity, the same as a job enqueued without one.
func Identity() jobs.Propagator {
	return identityPropagator{}
}

type identityPropagator struct{}

func (identityPropagator) Inject(ctx context.Context, headers map[string]string) {
	identity := auth.IdentityFrom(ctx)
	if identity == nil {
		return
	}
	snapshot, err := json.Marshal(identity)
	if err != nil {
		return // an Identity is plain data; Data values that do not encode leave the job without one
	}
	headers[HeaderIdentity] = string(snapshot)
}

func (identityPropagator) Extract(ctx context.Context, headers map[string]string) context.Context {
	snapshot, ok := headers[HeaderIdentity]
	if !ok {
		return ctx
	}
	identity := &auth.Identity{}
	if err := json.Unmarshal([]byte(snapshot), identity); err != nil {
		return ctx
	}
	return auth.WithIdentity(ctx, identity)
}
