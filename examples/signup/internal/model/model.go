// Package model holds the signup example's domain types, errors and job kinds; every layer
// imports it, it imports no other package of the example (ARC-002).
package model

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/govengers/jobs"
)

// Domain errors; the controllers map them to responses (ERR-005).
var (
	ErrInvalidInput = errors.New("invalid input")
	ErrEmailTaken   = errors.New("email already registered")
	ErrNotFound     = errors.New("user not found")
)

// User is a signed-up user.
type User struct {
	ID                uuid.UUID  `json:"id"`
	Email             string     `json:"email"`
	Name              string     `json:"name"`
	CreatedAt         time.Time  `json:"createdAt"`
	WelcomeMailSentAt *time.Time `json:"welcomeMailSentAt,omitempty"`
}

// SignUpInput is what a sign-up provides.
type SignUpInput struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// Mail is one outgoing mail. IdempotencyKey lets the provider drop a resend of the same mail
// after a retry (RES-004, JOB-003).
type Mail struct {
	IdempotencyKey string
	To             string
	Subject        string
	Body           string
}

// WelcomeMailPayload references the user; the handler loads the current data (payloads carry
// references, not data).
type WelcomeMailPayload struct {
	UserID uuid.UUID `json:"userId"`
}

// WelcomeMailKind is the contract between the repository that enqueues the welcome mail and
// the job controller that sends it.
var WelcomeMailKind = jobs.NewKind[WelcomeMailPayload]("signup.welcome_mail", &jobs.KindOptions{Queue: "mail", MaxAttempts: 8})
