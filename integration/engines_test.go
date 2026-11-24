// Package integration runs sqlgen's runtime and generated code against real database engines.
//
// SQLGEN_ENGINES=postgres15,mysql84 restricts a run to the named engines (CI runs one per job).
// Engines are configured by DSN environment variables (see engines below). Unset engines are skipped
// so `go test ./...` stays hermetic, unless SQLGEN_REQUIRE_ENGINES=1, in which case a missing DSN is
// a failure (CI sets this so the suite can never pass by silently skipping everything).
//
//	make engines-up && make test-engines     # from the repository root
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/CAFxX/sqlgen/runtime"
)

type family int

const (
	famPostgres family = iota
	famCockroach
	famMySQL
	famMariaDB
	famSQLite // only used by the fuzz tests (in-process, no DSN); not part of engines
)

type engine struct {
	name    string
	env     string // DSN environment variable: SQLGEN_<NAME>_DSN (see dsnEnv)
	driver  string
	family  family
	dialect runtime.Dialect
}

var engines = []engine{
	newEngine("postgres15", "pgx", famPostgres, runtime.DialectDollar),
	newEngine("postgres18", "pgx", famPostgres, runtime.DialectDollar),
	newEngine("cockroach", "pgx", famCockroach, runtime.DialectDollar),
	newEngine("mysql84", "mysql", famMySQL, runtime.DialectQuestion),
	newEngine("mysql97", "mysql", famMySQL, runtime.DialectQuestion),
	newEngine("mariadb1011", "mysql", famMariaDB, runtime.DialectQuestion),
	newEngine("mariadb123", "mysql", famMariaDB, runtime.DialectQuestion),
}

// dsnEnv is the DSN environment variable of an engine id: every engine is spelled the same way in the
// compose file, SQLGEN_ENGINES, the CI matrix and here.
func dsnEnv(name string) string { return "SQLGEN_" + strings.ToUpper(name) + "_DSN" }

func newEngine(name, driver string, f family, d runtime.Dialect) engine {
	return engine{name: name, env: dsnEnv(name), driver: driver, family: f, dialect: d}
}

func (e engine) isPG() bool    { return e.family == famPostgres || e.family == famCockroach }
func (e engine) isMySQL() bool { return e.family == famMySQL || e.family == famMariaDB }

// forEachEngine runs fn as a subtest against every configured engine.
func forEachEngine(t *testing.T, fn func(t *testing.T, e engine, db *sql.DB)) {
	t.Helper()
	selected := selectedEngines()
	for _, e := range engines {
		if selected != nil && !selected[e.name] {
			continue
		}
		t.Run(e.name, func(t *testing.T) {
			dsn := os.Getenv(e.env)
			if dsn == "" {
				if os.Getenv("SQLGEN_REQUIRE_ENGINES") == "1" {
					t.Fatalf("%s is not set but SQLGEN_REQUIRE_ENGINES=1", e.env)
				}
				t.Skipf("%s not set", e.env)
			}
			db, err := sql.Open(e.driver, dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := db.PingContext(ctx); err != nil {
				t.Fatalf("ping %s: %v", e.name, err)
			}
			fn(t, e, db)
		})
	}
}

// selectedEngines parses SQLGEN_ENGINES (comma-separated engine names); nil means all engines.
func selectedEngines() map[string]bool {
	v := strings.TrimSpace(os.Getenv("SQLGEN_ENGINES"))
	if v == "" {
		return nil
	}
	m := map[string]bool{}
	for _, n := range strings.Split(v, ",") {
		m[strings.TrimSpace(n)] = true
	}
	return m
}

var tableSeq atomic.Int64

// uniqueName returns a table name unique to this test process run, so reruns and parallel packages
// never collide and tests need no global cleanup.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), tableSeq.Add(1))
}

// mustExec runs DDL/DML and fails the test on error.
func mustExec(t testing.TB, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", firstLine(q), err)
	}
}

// makeTable creates a table (dropped on cleanup). ddl uses %s for the table name.
func makeTable(t testing.TB, db *sql.DB, prefix, ddl string) string {
	t.Helper()
	name := uniqueName(prefix)
	mustExec(t, db, fmt.Sprintf(ddl, name))
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + name) })
	return name
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
