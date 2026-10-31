// Package database holds the example's schema. A service runs these statements as goose
// migrations (DAT-005); the example applies them itself to stay small.
package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/jobs/postgresadapter"
)

// MigrationUsers creates the users table; it runs after postgresadapter.MigrationV1.
const MigrationUsers = `
CREATE TABLE users (
    id                   uuid PRIMARY KEY DEFAULT uuidv4(),
    email                text NOT NULL UNIQUE CHECK (email <> ''),
    name                 text NOT NULL CHECK (name <> ''),
    created_at           timestamptz NOT NULL DEFAULT now(),
    welcome_mail_sent_at timestamptz
);`

// Migrate creates the jobs and users tables unless they exist.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('users') IS NOT NULL").Scan(&exists); err != nil {
		return fmt.Errorf("check the schema: %w", err)
	}
	if exists {
		return nil
	}
	transaction, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }() // a no-op after Commit
	for _, migration := range []string{postgresadapter.MigrationV1, MigrationUsers} {
		if _, err := transaction.Exec(ctx, migration); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit the migration: %w", err)
	}
	return nil
}
