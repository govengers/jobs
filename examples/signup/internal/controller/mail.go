package controller

import (
	"context"
	"errors"

	"github.com/govengers/jobs"

	"github.com/govengers/jobs/examples/signup/internal/model"
	"github.com/govengers/jobs/examples/signup/internal/service"
)

// MailJobController handles the mail jobs: it translates a job into a service call, as the
// HTTP controller translates a request.
type MailJobController struct {
	service *service.UserService
}

// MailJobControllerOptions configures a MailJobController.
type MailJobControllerOptions struct {
	UserService *service.UserService // required
}

// NewMailJobController returns a MailJobController.
func NewMailJobController(options *MailJobControllerOptions) (*MailJobController, error) {
	if options == nil || options.UserService == nil {
		return nil, errors.New("controller: MailJobControllerOptions.UserService is required")
	}
	return &MailJobController{service: options.UserService}, nil
}

// Register registers the handlers on the worker.
func (c *MailJobController) Register(worker *jobs.Worker) error {
	return jobs.Handle(worker, model.WelcomeMailKind, c.sendWelcomeMail)
}

// sendWelcomeMail sends the mail; a user that no longer exists will not come back, so its job
// is dead at once instead of retried.
func (c *MailJobController) sendWelcomeMail(ctx context.Context, job *jobs.Job[model.WelcomeMailPayload]) error {
	err := c.service.SendWelcomeMail(ctx, job.Payload.UserID)
	if errors.Is(err, model.ErrNotFound) {
		return jobs.Permanent(err)
	}
	return err
}
