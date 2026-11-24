package sqlgen_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// A collection that fits one statement under the wire limit is ALWAYS sent as exactly one statement of exactly N
// rows: one round trip, statement-level atomicity, no implicit transaction, identical on the first call with a
// size and on every later call. Only a collection above the limit is chunked (full-limit chunks, the remainder
// last) inside an implicit transaction.

// traceDriver records every statement, prepare and transaction event that reaches the underlying driver.
type traceDriver struct {
	base driver.Driver
	dsn  string
	tr   *trace
}

type trace struct {
	mu sync.Mutex
	ev map[string]int
}

func (t *trace) add(k string) { t.mu.Lock(); t.ev[k]++; t.mu.Unlock() }
func (t *trace) reset()       { t.mu.Lock(); t.ev = map[string]int{}; t.mu.Unlock() }
func (t *trace) get(k string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ev[k]
}

// stmts is the number of statements executed (directly or through a prepared statement).
func (t *trace) stmts() int { return t.get("exec") + t.get("query") }

func (d traceDriver) Open(string) (driver.Conn, error) {
	c, err := d.base.Open(d.dsn)
	if err != nil {
		return nil, err
	}
	return &traceConn{Conn: c, tr: d.tr}, nil
}
func (d traceDriver) Connect(context.Context) (driver.Conn, error) { return d.Open("") }
func (d traceDriver) Driver() driver.Driver                        { return d }

type traceConn struct {
	driver.Conn
	tr *trace
}

func (c *traceConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	c.tr.add("prepare")
	s, err := c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, q)
	if err != nil {
		return nil, err
	}
	return &traceStmt{Stmt: s, tr: c.tr}, nil
}

func (c *traceConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	c.tr.add("begin")
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	return &traceTx{Tx: tx, tr: c.tr}, nil
}

func (c *traceConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	c.tr.add("exec")
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, a)
}

func (c *traceConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	c.tr.add("query")
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, a)
}

type traceStmt struct {
	driver.Stmt
	tr *trace
}

func (s *traceStmt) ExecContext(ctx context.Context, a []driver.NamedValue) (driver.Result, error) {
	s.tr.add("exec")
	return s.Stmt.(driver.StmtExecContext).ExecContext(ctx, a)
}

func (s *traceStmt) QueryContext(ctx context.Context, a []driver.NamedValue) (driver.Rows, error) {
	s.tr.add("query")
	return s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, a)
}

type traceTx struct {
	driver.Tx
	tr *trace
}

func (x *traceTx) Commit() error   { x.tr.add("commit"); return x.Tx.Commit() }
func (x *traceTx) Rollback() error { x.tr.add("rollback"); return x.Tx.Rollback() }

func setupTraceDB(t *testing.T, opts ...runtime.CacheOption) (*Queries, *trace) {
	t.Helper()
	plain, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	tr := &trace{ev: map[string]int{}}
	db := sql.OpenDB(traceDriver{base: plain.Driver(), dsn: ":memory:", tr: tr})
	_ = plain.Close()
	db.SetMaxOpenConns(1) // one connection keeps the in-memory schema
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE batch_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, points INTEGER NOT NULL);
		CREATE TABLE quad_items (id INTEGER PRIMARY KEY, group_key TEXT NOT NULL, priority INTEGER NOT NULL, created_ts INTEGER NOT NULL);
		CREATE TABLE users (id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL, email TEXT NOT NULL, role TEXT NOT NULL, score INTEGER NOT NULL, bio TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	q := New(db, opts...)
	t.Cleanup(func() { _ = q.Close() })
	tr.reset()
	return q, tr
}

func TestBatchUnderTheLimitIsOneStatementWithoutTransaction(t *testing.T) {
	ctx := context.Background()
	q, tr := setupTraceDB(t) // default promote threshold: sizes get promoted from the second call on

	next := int64(0)
	for _, n := range []int{1, 3, 5, 7, 100} {
		// Three calls per size: the first sees a cold size, the later ones a recurring (promoted) one.
		for call := 0; call < 3; call++ {
			quads := make([]QuadInput, n)
			users := make([]BatchUserInput, n)
			ids := make([]int64, n)
			for i := range quads {
				next++
				quads[i] = QuadInput{ID: next, GroupKey: "g", Priority: 1, CreatedTs: 2}
				users[i] = BatchUserInput{ID: next + 1_000_000, Name: "n", Points: 3}
				ids[i] = users[i].ID
			}
			what := fmt.Sprintf("n=%d call=%d", n, call)
			check := func(op string) {
				t.Helper()
				if tr.stmts() != 1 || tr.get("begin") != 0 || tr.get("commit") != 0 || tr.get("rollback") != 0 {
					t.Fatalf("%s %s: statements=%d begin=%d commit=%d rollback=%d, want exactly 1 statement and no transaction",
						what, op, tr.stmts(), tr.get("begin"), tr.get("commit"), tr.get("rollback"))
				}
				tr.reset()
			}

			res, err := q.InsertQuadBatch(ctx, quads) // exec
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			if got, _ := res.RowsAffected(); got != int64(n) {
				t.Fatalf("%s: exec affected %d rows", what, got)
			}
			check("exec")

			ret, err := q.InsertUsersBatchReturning(ctx, users) // many, RETURNING
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			if len(ret) != n {
				t.Fatalf("%s: RETURNING gave %d rows", what, len(ret))
			}
			for i, r := range ret {
				if r.ID != users[i].ID {
					t.Fatalf("%s: RETURNING row %d has id %d, want %d (input order)", what, i, r.ID, users[i].ID)
				}
			}
			check("many")

			got, err := q.ListUsersByIDs(ctx, ids) // many, IN list (none exist in users: an empty result is fine)
			if err != nil || len(got) != 0 {
				t.Fatalf("%s: ListUsersByIDs = %v, %v", what, got, err)
			}
			check("in-list")
		}
	}
}

func TestBatchOverTheLimitChunksInOneTransaction(t *testing.T) {
	ctx := context.Background()
	// 40 parameters / 4 columns = 10 rows per statement for InsertQuadBatch.
	q, tr := setupTraceDB(t, runtime.WithMaxBatchParams(40))
	maxRows := q.Cache().MaxRowsPerBatch(runtime.DialectQuestion, 0, 4)
	if maxRows != 10 {
		t.Fatalf("test setup: max rows per statement = %d, want 10", maxRows)
	}
	next := int64(0)
	for _, tc := range []struct{ n, stmts int }{{10, 1}, {11, 2}, {20, 2}, {25, 3}, {31, 4}} {
		for call := 0; call < 3; call++ {
			quads := make([]QuadInput, tc.n)
			for i := range quads {
				next++
				quads[i] = QuadInput{ID: next, GroupKey: "g", Priority: 1, CreatedTs: 2}
			}
			res, err := q.InsertQuadBatch(ctx, quads)
			if err != nil {
				t.Fatalf("n=%d call=%d: %v", tc.n, call, err)
			}
			if got, _ := res.RowsAffected(); got != int64(tc.n) {
				t.Fatalf("n=%d call=%d: affected %d rows", tc.n, call, got)
			}
			wantTx := 0
			if tc.stmts > 1 {
				wantTx = 1
			}
			if tr.stmts() != tc.stmts || tr.get("begin") != wantTx || tr.get("commit") != wantTx {
				t.Fatalf("n=%d call=%d: statements=%d begin=%d commit=%d, want %d statements and %d transaction(s)",
					tc.n, call, tr.stmts(), tr.get("begin"), tr.get("commit"), tc.stmts, wantTx)
			}
			tr.reset()
		}
	}
	var count int
	if err := q.db.QueryRow(`SELECT COUNT(*) FROM quad_items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if want := 3 * (10 + 11 + 20 + 25 + 31); count != want {
		t.Fatalf("rows stored = %d, want %d", count, want)
	}
}
