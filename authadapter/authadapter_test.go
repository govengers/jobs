package authadapter_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/govengers/auth"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs/authadapter"
)

func TestIdentityTravelsFromEnqueueToHandler(t *testing.T) {
	propagator := authadapter.Identity()
	userID := uuid.New()
	identity := &auth.Identity{
		Provider: "oidc", Subject: "ada", UserID: userID, Username: "ada", Active: true,
		Roles: []string{auth.RoleAdmin}, Permissions: []auth.Permission{{Resource: "renders", Access: auth.AccessWrite}},
		Impersonator: &auth.Identity{Provider: "password", Subject: "support", Active: true},
	}
	headers := map[string]string{}

	require.NoError(t, propagator.Inject(auth.WithIdentity(context.Background(), identity), headers))
	restored := auth.IdentityFrom(propagator.Extract(context.Background(), headers))

	require.Contains(t, headers, authadapter.HeaderIdentity)
	require.Equal(t, identity, restored)
	require.Equal(t, "support", restored.Real().Subject, "an impersonation keeps its real identity")
}

func TestNoIdentityStoresNothing(t *testing.T) {
	propagator := authadapter.Identity()
	headers := map[string]string{}

	require.NoError(t, propagator.Inject(context.Background(), headers))

	require.Empty(t, headers)
	require.Nil(t, auth.IdentityFrom(propagator.Extract(context.Background(), headers)))
}

// Finding 17: a job enqueued without an identity runs without one, even when the worker's own
// context carries an identity (Run's or RunDue's root context).
func TestExtractShadowsTheWorkersIdentity(t *testing.T) {
	administrator := &auth.Identity{Provider: "password", Subject: "root", Roles: []string{auth.RoleAdmin}, Active: true}
	workerContext := auth.WithIdentity(context.Background(), administrator)
	for name, headers := range map[string]map[string]string{
		"no header":          {},
		"undecodable header": {authadapter.HeaderIdentity: "{not json"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := authadapter.Identity().Extract(workerContext, headers)

			require.Nil(t, auth.IdentityFrom(ctx))
		})
	}
}

// Finding 18: an identity that cannot be carried fails the enqueue instead of vanishing.
func TestUnencodableIdentityIsAnError(t *testing.T) {
	identity := &auth.Identity{Provider: "oidc", Subject: "ada", Data: map[string]any{"callback": func() {}}}
	headers := map[string]string{}

	err := authadapter.Identity().Inject(auth.WithIdentity(context.Background(), identity), headers)

	require.ErrorContains(t, err, "oidc:ada")
	require.Empty(t, headers)
}
