// Package repository persists users (DAT-001). The queries are inline SQL to keep the example
// small; a service generates them with sqlc (DAT-002).
package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/jobs/examples/signup/internal/model"
	"github.com/govengers/jobs/postgresadapter"
)

const uniqueViolation = "23505"

// UserRepository stores users and enqueues the jobs that belong to their changes.
type UserRepository struct {
	pool     *pgxpool.Pool
	jobStore *postgresadapter.Store
}

// UserRepositoryOptions configures a UserRepository.
type UserRepositoryOptions struct {
	Pool     *pgxpool.Pool          // required
	JobStore *postgresadapter.Store // required; enqueues on the repository's transactions
}

// NewUserRepository returns a UserRepository.
func NewUserRepository(options *UserRepositoryOptions) (*UserRepository, error) {
	if options == nil || options.Pool == nil || options.JobStore == nil {
		return nil, errors.New("repository: UserRepositoryOptions.Pool and JobStore are required")
	}
	return &UserRepository{pool: options.Pool, jobStore: options.JobStore}, nil
}

const userColumns = "id, email, name, created_at, welcome_mail_sent_at"

// Create inserts the user and enqueues the welcome mail in one transaction: both commit or
// neither, so no user misses the mail and no mail goes to a user that was rolled back. A taken
// email is model.ErrEmailTaken.
func (r *UserRepository) Create(ctx context.Context, input *model.SignUpInput) (*model.User, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin create user: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }() // a no-op after Commit
	user, err := scanUser(transaction.QueryRow(ctx,
		"INSERT INTO users (email, name) VALUES ($1, $2) RETURNING "+userColumns, input.Email, input.Name))
	if err != nil {
		return nil, fmt.Errorf("create user: %w", mapError(err))
	}
	welcomeMail := model.WelcomeMailKind.New(model.WelcomeMailPayload{UserID: user.ID}, nil)
	if _, err := r.jobStore.Enqueue(ctx, transaction, welcomeMail); err != nil {
		return nil, fmt.Errorf("enqueue the welcome mail of user %s: %w", user.ID, err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit user %s: %w", user.ID, err)
	}
	return user, nil
}

// Get returns the user with id, or model.ErrNotFound.
func (r *UserRepository) Get(ctx context.Context, id uuid.UUID) (*model.User, error) {
	user, err := scanUser(r.pool.QueryRow(ctx, "SELECT "+userColumns+" FROM users WHERE id = $1", id))
	if err != nil {
		return nil, fmt.Errorf("get user %s: %w", id, mapError(err))
	}
	return user, nil
}

// MarkWelcomeMailSent records the welcome mail and reports whether this call recorded it.
func (r *UserRepository) MarkWelcomeMailSent(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := r.pool.Exec(ctx, "UPDATE users SET welcome_mail_sent_at = now() WHERE id = $1 AND welcome_mail_sent_at IS NULL", id)
	if err != nil {
		return false, fmt.Errorf("mark the welcome mail of user %s: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

func scanUser(row pgx.Row) (*model.User, error) {
	user := &model.User{}
	err := row.Scan(&user.ID, &user.Email, &user.Name, &user.CreatedAt, &user.WelcomeMailSentAt)
	return user, err
}

// mapError maps driver errors to domain errors once (DAT-004).
func mapError(err error) error {
	var postgresError *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return model.ErrNotFound
	case errors.As(err, &postgresError) && postgresError.Code == uniqueViolation:
		return model.ErrEmailTaken
	default:
		return err
	}
}
