// Package postgrestest gives the tests of this module a real PostgreSQL 18 through
// github.com/govengers/postgrestest: one server per test binary (or the one POSTGRESTEST_URL
// names, which `just test` starts for the whole run), and a fresh database per test, copied from
// a template with the jobs tables, dropped when the test ends.
//
// Every package that uses New runs its tests through Main:
//
//	func TestMain(m *testing.M) { postgrestest.Main(m) }
package postgrestest

import (
	"strings"
	"sync"
	"testing"

	"github.com/govengers/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"

	"github.com/govengers/jobs/postgresadapter"
)

var (
	// server holds the PostgreSQL of this test binary and the template of every migration.
	server = postgrestest.New(&postgrestest.Options{
		Migrate:     postgrestest.Statements(postgresadapter.MigrationV1, postgresadapter.MigrationV2),
		Fingerprint: postgresadapter.MigrationV1 + postgresadapter.MigrationV2,
	})
	// others maps the joined migrations of NewMigrated to the *postgrestest.Server of their template.
	others sync.Map
)

// Main runs the package's tests, closes the server (which stops the PostgreSQL this binary started,
// not the one of POSTGRESTEST_URL) and fails on leaked goroutines (TST-009).
func Main(m *testing.M) { goleak.VerifyTestMain(server.Main(m)) }

// New returns a pool on a fresh database of the test PostgreSQL with postgresadapter.MigrationV1
// and MigrationV2 applied; the pool is closed and the database dropped when the test ends. The
// pool's connection string, pool.Config().ConnString(), reaches the same database, for processes
// the test starts.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return server.Pool(t)
}

// NewMigrated is New with the given migrations instead of every migration of the module: the
// tests of an upgrade, an example with its own tables, or none for an empty database. Each set of migrations gets its own template on the same PostgreSQL.
func NewMigrated(t *testing.T, migrations ...string) *pgxpool.Pool {
	t.Helper()
	key := strings.Join(migrations, "\x00")
	other, _ := others.LoadOrStore(key, server.WithTemplate(&postgrestest.Options{
		Migrate:     postgrestest.Statements(migrations...),
		Fingerprint: key,
	}))
	return other.(*postgrestest.Server).Pool(t)
}
