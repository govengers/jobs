package authadapter_test

import (
	"context"
	"fmt"

	"github.com/govengers/auth"

	"github.com/govengers/jobs/authadapter"
)

func ExampleIdentity() {
	// main configures the same propagator on both sides:
	//   postgresadapter.Options{Propagators: []jobs.Propagator{authadapter.Identity()}}
	//   jobs.WorkerOptions{Propagators: []jobs.Propagator{authadapter.Identity()}}
	propagator := authadapter.Identity()

	// The request that enqueues the job carries the user's identity.
	request := auth.WithIdentity(context.Background(), &auth.Identity{Provider: "oidc", Subject: "ada", Username: "ada", Active: true})
	headers := map[string]string{} // stored with the job
	propagator.Inject(request, headers)

	// The worker restores it before the handler runs.
	handlerContext := propagator.Extract(context.Background(), headers)
	fmt.Println(auth.IdentityFrom(handlerContext).Username)
	// Output: ada
}
