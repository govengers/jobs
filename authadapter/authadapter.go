// Package authadapter carries the identity of github.com/govengers/auth from the request that
// enqueues a job to the job's handler (J15): a job a user started runs, and audits, as that
// user.
package authadapter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/govengers/auth"

	"github.com/govengers/jobs"
)

// HeaderIdentity is the job header that holds the identity as JSON.
const HeaderIdentity = "auth.identity"

// Identity returns the jobs.Propagator for the identity. Configure it on both sides, in
// postgresadapter.Options.Propagators and jobs.WorkerOptions.Propagators.
//
// Inject stores the JSON snapshot of auth.IdentityFrom(ctx); without an identity it stores
// nothing, and an identity that does not encode (a Data value JSON cannot hold) is an error, so
// the enqueue fails instead of running the job without its actor. Extract restores the
// snapshot with auth.WithIdentity. Without a header, or with one that does not decode, the
// handler runs without an identity: Extract shadows any identity the worker's own context
// carries. The snapshot is the identity at enqueue time: roles and permissions revoked later
// still apply to the job, so a handler that must honor a revocation checks the user again.
func Identity() jobs.Propagator {
	return identityPropagator{}
}

type identityPropagator struct{}

func (identityPropagator) Inject(ctx context.Context, headers map[string]string) error {
	identity := auth.IdentityFrom(ctx)
	if identity == nil {
		return nil
	}
	snapshot, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("authadapter: encode the identity of %s:%s: %w", identity.Provider, identity.Subject, err)
	}
	headers[HeaderIdentity] = string(snapshot)
	return nil
}

func (identityPropagator) Extract(ctx context.Context, headers map[string]string) context.Context {
	snapshot, ok := headers[HeaderIdentity]
	if !ok {
		return auth.WithIdentity(ctx, nil)
	}
	identity := &auth.Identity{}
	if err := json.Unmarshal([]byte(snapshot), identity); err != nil {
		return auth.WithIdentity(ctx, nil)
	}
	return auth.WithIdentity(ctx, identity)
}
