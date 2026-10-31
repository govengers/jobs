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

	propagator.Inject(auth.WithIdentity(context.Background(), identity), headers)
	restored := auth.IdentityFrom(propagator.Extract(context.Background(), headers))

	require.Contains(t, headers, authadapter.HeaderIdentity)
	require.Equal(t, identity, restored)
	require.Equal(t, "support", restored.Real().Subject, "an impersonation keeps its real identity")
}

func TestNoIdentityStoresNothing(t *testing.T) {
	propagator := authadapter.Identity()
	headers := map[string]string{}

	propagator.Inject(context.Background(), headers)

	require.Empty(t, headers)
	require.Nil(t, auth.IdentityFrom(propagator.Extract(context.Background(), headers)))
}

func TestUndecodableHeaderLeavesTheContextWithoutIdentity(t *testing.T) {
	ctx := authadapter.Identity().Extract(context.Background(), map[string]string{authadapter.HeaderIdentity: "{not json"})

	require.Nil(t, auth.IdentityFrom(ctx))
}

func TestUnencodableIdentityStoresNothing(t *testing.T) {
	identity := &auth.Identity{Provider: "oidc", Subject: "ada", Data: map[string]any{"callback": func() {}}}
	headers := map[string]string{}

	authadapter.Identity().Inject(auth.WithIdentity(context.Background(), identity), headers)

	require.Empty(t, headers)
}
