// Package postgrestest gives tests a real PostgreSQL 18: one embedded-postgres instance per
// test binary, and a fresh schema with the jobs tables per test, dropped when the test ends.
//
// Every package that uses New runs its tests through Main:
//
//	func TestMain(m *testing.M) { postgrestest.Main(m) }
package postgrestest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"

	"github.com/govengers/jobs/postgresadapter"
)

const version = embeddedpostgres.V18

// server is the instance of this test binary, started by the first New.
type server struct {
	once      sync.Once
	postgres  *embeddedpostgres.EmbeddedPostgres
	directory string
	url       string
	err       error
}

// instance is the one PostgreSQL of this test binary, started by the first New and stopped by Main.
var instance server

// Main runs the package's tests, stops the PostgreSQL and fails on leaked goroutines (TST-009).
func Main(m *testing.M) {
	code := m.Run()
	if instance.postgres != nil {
		if err := instance.postgres.Stop(); err != nil {
			fmt.Fprintln(os.Stderr, "stop the test PostgreSQL:", err)
			code = 1
		}
		_ = os.RemoveAll(instance.directory) // a leftover temp dir is harmless
	}
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintln(os.Stderr, "leaked goroutines:", err)
			code = 1
		}
	}
	os.Exit(code)
}

// New returns a pool on a fresh schema of the test PostgreSQL with postgresadapter.MigrationV1
// applied; the pool is closed and the schema dropped when the test ends. The pool's connection
// string, pool.Config().ConnString(), reaches the same schema, for processes the test starts.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	instance.once.Do(instance.start)
	if instance.err != nil {
		t.Fatalf("start the test PostgreSQL: %v", instance.err)
	}
	ctx := context.Background()
	schema := randomSchema(t)
	createSchema(t, schema)

	poolConfiguration, err := pgxpool.ParseConfig(instance.url + "&search_path=" + schema)
	if err != nil {
		t.Fatalf("parse the test database URL: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfiguration)
	if err != nil {
		t.Fatalf("open the test pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropSchema(t, schema)
	})
	if _, err := pool.Exec(ctx, postgresadapter.MigrationV1); err != nil {
		t.Fatalf("apply MigrationV1: %v", err)
	}
	return pool
}

func (s *server) start() {
	stopOrphans()
	binaries, err := unpackedBinaries()
	if err != nil {
		s.err = err
		return
	}
	port, err := freePort()
	if err != nil {
		s.err = err
		return
	}
	if s.directory, s.err = os.MkdirTemp("", instancePrefix); s.err != nil {
		return
	}
	if s.err = os.WriteFile(filepath.Join(s.directory, ownerFile), []byte(strconv.Itoa(os.Getpid())), 0o600); s.err != nil {
		return
	}
	postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(version).
		Port(port).
		BinariesPath(binaries).
		RuntimePath(filepath.Join(s.directory, "runtime")).
		Logger(io.Discard))
	if s.err = postgres.Start(); s.err != nil {
		return
	}
	s.postgres = postgres
	s.url = fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
}

// instancePrefix names the directories of the test binaries' PostgreSQL instances; ownerFile
// in each holds the process ID of the test binary that started it.
const (
	instancePrefix = "jobs-postgrestest-"
	ownerFile      = "owner"
)

// stopOrphans stops the instances of test binaries that died before Main could stop them (a
// killed binary, a test timeout) and removes their directories, so they do not pile up.
func stopOrphans() {
	directories, _ := filepath.Glob(filepath.Join(os.TempDir(), instancePrefix+"*")) // a bad pattern is impossible here
	for _, directory := range directories {
		owner, err := os.ReadFile(filepath.Join(directory, ownerFile)) //nolint:gosec // G304: a directory of this harness, found by its own prefix
		if err != nil || alive(string(owner)) {
			continue // not ours, or its test binary still runs
		}
		if postmaster, err := os.ReadFile(filepath.Join(directory, "runtime", "data", "postmaster.pid")); err == nil { //nolint:gosec // G304: same directory
			_ = signal(strings.TrimSpace(strings.SplitN(string(postmaster), "\n", 2)[0]), os.Interrupt) // fast shutdown; gone already is fine
		}
		_ = os.RemoveAll(directory) // a leftover directory is harmless
	}
}

func alive(processID string) bool { return signal(processID, syscall.Signal(0)) == nil }

func signal(processID string, sent os.Signal) error {
	number, err := strconv.Atoi(strings.TrimSpace(processID))
	if err != nil {
		return err
	}
	process, err := os.FindProcess(number)
	if err != nil {
		return err
	}
	return process.Signal(sent)
}

// unpackedBinaries unpacks the PostgreSQL binaries once per machine: unpacking inside a -race
// test binary takes about 40 s, starting from unpacked binaries about 3 s. Test binaries of
// several packages run in parallel, so each unpacks into its own directory and renames it into
// place; the loser drops its copy.
func unpackedBinaries() (string, error) {
	final := filepath.Join(os.TempDir(), "govengers-jobs-postgres-"+string(version))
	if _, err := os.Stat(filepath.Join(final, "bin", "pg_ctl")); err == nil {
		return final, nil
	}
	defer closeDownloadConnections()
	scratch, err := os.MkdirTemp("", "govengers-jobs-postgres-unpack-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(scratch) }() // only the unpacked copy matters
	port, err := freePort()
	if err != nil {
		return "", err
	}
	unpack := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(version).
		Port(port).
		BinariesPath(filepath.Join(scratch, "binaries")).
		RuntimePath(filepath.Join(scratch, "runtime")).
		Logger(io.Discard))
	if err := unpack.Start(); err != nil { // the library unpacks only as part of a start
		return "", err
	}
	if err := unpack.Stop(); err != nil {
		return "", err
	}
	if err := os.Rename(filepath.Join(scratch, "binaries"), final); err != nil {
		if _, statError := os.Stat(filepath.Join(final, "bin", "pg_ctl")); statError != nil {
			return "", err
		}
	}
	return final, nil
}

// closeDownloadConnections closes the keep-alive connection embedded-postgres leaves open after
// it downloaded the PostgreSQL archive with http.Get (the default transport): its read loop
// would outlive the tests and fail Main's goroutine-leak check on a cold machine.
func closeDownloadConnections() {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

func freePort() (uint32, error) {
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = listener.Close() }()                 // only the port number was needed
	return uint32(listener.Addr().(*net.TCPAddr).Port), nil //nolint:gosec // G115: a TCP port fits in uint32
}

func randomSchema(t *testing.T) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("random schema name: %v", err)
	}
	return "test_" + hex.EncodeToString(suffix)
}

func createSchema(t *testing.T, schema string) {
	t.Helper()
	executeAdmin(t, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
}

func dropSchema(t *testing.T, schema string) {
	t.Helper()
	executeAdmin(t, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
}

func executeAdmin(t *testing.T, statement string) {
	t.Helper()
	ctx := context.Background()
	connection, err := pgx.Connect(ctx, instance.url)
	if err != nil {
		t.Fatalf("connect to the test PostgreSQL: %v", err)
	}
	defer func() { _ = connection.Close(ctx) }() // a failed close of a test connection changes nothing
	if _, err := connection.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}
