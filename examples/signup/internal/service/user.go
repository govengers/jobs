// Package service holds the signup example's business logic. It is transport-agnostic: the HTTP
// controller and the job controller call the same methods (ARC-003).
package service

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"

	"github.com/google/uuid"

	"github.com/govengers/jobs/examples/signup/internal/model"
	"github.com/govengers/jobs/examples/signup/internal/repository"
)

// MailSender sends mail; the service declares what it needs from the mail client (ARC-005).
type MailSender interface {
	Send(ctx context.Context, mail *model.Mail) error
}

// UserService signs users up and sends their welcome mail.
type UserService struct {
	repository *repository.UserRepository
	mailSender MailSender
}

// UserServiceOptions configures a UserService.
type UserServiceOptions struct {
	UserRepository *repository.UserRepository // required
	MailSender     MailSender                 // required
}

// NewUserService returns a UserService.
func NewUserService(options *UserServiceOptions) (*UserService, error) {
	if options == nil || options.UserRepository == nil || options.MailSender == nil {
		return nil, errors.New("service: UserServiceOptions.UserRepository and MailSender are required")
	}
	return &UserService{repository: options.UserRepository, mailSender: options.MailSender}, nil
}

// SignUp validates the input and creates the user; the welcome mail is enqueued with it. It
// returns model.ErrInvalidInput or model.ErrEmailTaken.
func (s *UserService) SignUp(ctx context.Context, input *model.SignUpInput) (*model.User, error) {
	normalized, err := validateSignUp(input)
	if err != nil {
		return nil, err
	}
	return s.repository.Create(ctx, normalized)
}

// SendWelcomeMail sends the user's welcome mail unless it was sent already. Running it twice
// sends one mail: a finished send is recorded, and a send whose record was lost carries the
// same idempotency key, so the provider drops the duplicate (JOB-003).
func (s *UserService) SendWelcomeMail(ctx context.Context, userID uuid.UUID) error {
	user, err := s.repository.Get(ctx, userID)
	if err != nil {
		return err
	}
	if user.WelcomeMailSentAt != nil {
		return nil
	}
	if err := s.mailSender.Send(ctx, welcomeMail(user)); err != nil {
		return fmt.Errorf("send the welcome mail of user %s: %w", user.ID, err)
	}
	_, err = s.repository.MarkWelcomeMailSent(ctx, user.ID)
	return err
}

// validateSignUp is the sign-up rule as a pure function (FUN-005).
func validateSignUp(input *model.SignUpInput) (*model.SignUpInput, error) {
	email := strings.ToLower(strings.TrimSpace(input.Email))
	name := strings.TrimSpace(input.Name)
	address, err := mail.ParseAddress(email)
	if err != nil || address.Address != email || name == "" {
		return nil, fmt.Errorf("%w: a valid email and a name are required", model.ErrInvalidInput)
	}
	return &model.SignUpInput{Email: email, Name: name}, nil
}

func welcomeMail(user *model.User) *model.Mail {
	return &model.Mail{
		IdempotencyKey: "welcome-mail:" + user.ID.String(),
		To:             user.Email,
		Subject:        "Welcome, " + user.Name,
		Body:           "Your account is ready.",
	}
}
