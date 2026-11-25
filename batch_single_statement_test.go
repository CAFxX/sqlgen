package sqlgen_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
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
	maxRows := q.cache.MaxRowsPerBatch(runtime.DialectQuestion, 0, 4)
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

// A size is counted once per call: the first call with a size runs it as a plain (unprepared) statement and only a
// size that recurs is promoted and prepared. The slice form of a many statement used to plan its first chunk twice
// (once to decide on the implicit transaction, once in the iterator it collects), so a one-off size was promoted
// and prepared by its first and only call.
func TestBatchOneOffSizeIsNotPrepared(t *testing.T) {
	ctx := context.Background()
	q, tr := setupTraceDB(t)
	const n = 37
	quads := make([]QuadInput, n)
	users := make([]BatchUserInput, n)
	ids := make([]int64, n)
	for i := range quads {
		quads[i] = QuadInput{ID: int64(i + 1), GroupKey: "g", Priority: 1, CreatedTs: 2}
		users[i] = BatchUserInput{ID: int64(i + 1_000_001), Name: "n", Points: 3}
		ids[i] = users[i].ID
	}
	for _, c := range []struct {
		name string
		run  func() error
	}{
		{"exec", func() error { _, err := q.InsertQuadBatch(ctx, quads); return err }},
		{"many RETURNING", func() error { _, err := q.InsertUsersBatchReturning(ctx, users); return err }},
		{"many IN list", func() error { _, err := q.ListUsersByIDs(ctx, ids); return err }},
		{"many IN list again", func() error { _, err := q.ListUsersByIDs(ctx, ids); return err }},
	} {
		tr.reset()
		if err := c.run(); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		want := 0
		if c.name == "many IN list again" {
			want = 1 // the size recurred: promoted, prepared on this miss
		}
		if got := tr.get("prepare"); got != want {
			t.Errorf("%s: %d prepares, want %d", c.name, got, want)
		}
	}
}

// A statement whose result is not the concatenation of per-chunk results (GROUP BY, an aggregate, ORDER BY,
// LIMIT, ...) is never chunked: over the single-statement limit it fails with ErrCollectionTooLarge before
// sending anything; under it, it runs as one statement with the exact result. Chunking such a statement used
// to return duplicate partial groups and per-chunk LIMITs.
func TestNonDecomposableStatementIsNeverChunked(t *testing.T) {
	ctx := context.Background()
	small, trs := setupTraceDB(t, runtime.WithMaxBatchParams(10)) // 10 single-column items per statement
	ids := make([]int64, 30)
	for i := range ids {
		ids[i] = int64(i + 1)
		role := "r" + fmt.Sprint(i%2)
		if _, err := small.db.Exec(`INSERT INTO users (id, org_id, email, role, score, bio) VALUES (?, 1, 'e', ?, 0, '')`, ids[i], role); err != nil {
			t.Fatal(err)
		}
	}
	tooLarge := func(name string, err error) {
		t.Helper()
		var e *runtime.CollectionTooLargeError
		if !errors.As(err, &e) || !errors.Is(err, runtime.ErrCollectionTooLarge) || e.Statement != name || e.Items != 30 || e.Limit != 10 {
			t.Fatalf("%s: err = %v, want a CollectionTooLargeError{%s, 30, 10}", name, err, name)
		}
		if n := trs.stmts() + trs.get("begin") + trs.get("prepare"); n != 0 {
			t.Fatalf("%s: %d statements/transactions/prepares sent, want none", name, n)
		}
		trs.reset()
	}
	trs.reset()
	_, err := small.UserRoleCountsIn(ctx, ids)
	tooLarge("UserRoleCountsIn", err)
	_, err = small.TopUsersIn(ctx, ids)
	tooLarge("TopUsersIn", err)
	_, err = small.ListUsersByIDs(ctx, ids)
	tooLarge("ListUsersByIDs", err)
	yields := 0
	for _, err := range small.TopUsersInSeq(ctx, ids) {
		yields++
		tooLarge("TopUsersIn", err)
	}
	if yields != 1 {
		t.Fatalf("TopUsersInSeq yielded %d times, want the error once", yields)
	}

	// Under the limit: one statement, the exact result.
	big, tr := setupTraceDB(t)
	for _, id := range ids {
		if _, err := big.db.Exec(`INSERT INTO users (id, org_id, email, role, score, bio) VALUES (?, 1, 'e', ?, 0, '')`, id, "r"+fmt.Sprint(id%2)); err != nil {
			t.Fatal(err)
		}
	}
	tr.reset()
	counts, err := big.UserRoleCountsIn(ctx, ids)
	if err != nil || len(counts) != 2 || counts[0].N+counts[1].N != 30 || counts[0].N != 15 {
		t.Fatalf("UserRoleCountsIn = %+v, %v; want two groups of 15", counts, err)
	}
	top, err := big.TopUsersIn(ctx, ids)
	if err != nil || !slices.Equal(top, []int64{30, 29, 28}) {
		t.Fatalf("TopUsersIn = %v, %v; want [30 29 28]", top, err)
	}
	if tr.stmts() != 2 || tr.get("begin") != 0 {
		t.Fatalf("statements=%d begin=%d, want 2 single statements", tr.stmts(), tr.get("begin"))
	}

	// A decomposable statement is still chunked under the small limit.
	got, err := small.UserIDsNearScore(ctx, 0, ids)
	if err != nil || len(got) != 30 || trs.stmts() != 5 {
		t.Fatalf("UserIDsNearScore = %d rows in %d statements, %v; want 30 rows in 5 chunks of 7", len(got), trs.stmts(), err)
	}
}

// A pipeline operation is never chunked (it is one statement of the batch): a collection over the
// single-statement limit fails the pipeline with a CollectionTooLargeError naming the statement before
// anything is sent, instead of reaching the database and failing there ("too many SQL variables"). Every
// future of the pipeline reports the error.
func TestPipelineOperationOverTheLimitFailsBeforeSending(t *testing.T) {
	ctx := context.Background()
	// The limit is the wire's (32766 placeholders on SQLite), not the WithMaxBatchParams chunk size.
	small, trs := setupTraceDB(t, runtime.WithMaxBatchParams(10))
	ids := make([]int64, 32767)
	rows := make([]BatchUserInput, 10923) // 3 fields: 10922 rows per statement
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	for i := range rows {
		rows[i] = BatchUserInput{ID: int64(i + 1), Name: "n", Points: 1}
	}
	check := func(name string, items, limit int, err error) {
		t.Helper()
		var e *runtime.CollectionTooLargeError
		if !errors.As(err, &e) || !errors.Is(err, runtime.ErrCollectionTooLarge) || e.Statement != name || e.Items != items || e.Limit != limit || !e.Pipeline {
			t.Fatalf("%s: err = %v, want a pipeline CollectionTooLargeError{%s, %d, %d}", name, err, name, items, limit)
		}
		if !strings.Contains(err.Error(), "pipeline operation") {
			t.Fatalf("%s: message %q does not say a pipeline operation is never chunked", name, err)
		}
		if n := trs.stmts() + trs.get("begin") + trs.get("prepare"); n != 0 {
			t.Fatalf("%s: %d statements/transactions/prepares sent, want none", name, n)
		}
		trs.reset()
	}
	trs.reset()
	var before *runtime.RowFuture[User]
	err := small.PipelineTx(ctx, nil, func(p *Pipeline) error {
		before = p.GetUserByID(1)
		p.ListUsersByIDs(ids)
		return nil
	})
	check("ListUsersByIDs", 32767, 32766, err)
	if _, ferr := before.Get(); ferr != err {
		t.Fatalf("future of the failed pipeline: %v, want %v", ferr, err)
	}
	p := small.Pipeline(ctx)
	p.InsertUsersBatchReturning(rows)
	check("InsertUsersBatchReturning", 10923, 10922, p.Flush())
	// Over the chunk size but within the wire limit: one statement of the batch, not chunked.
	p = small.Pipeline(ctx)
	p.InsertUsersBatchReturning(rows[:30])
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
}

// A decomposable statement over a 1D slice streams like a tuple-slice one: <Name>Stream takes the values
// from an iterator in pooled chunks, each run through the chunked method, so an arbitrarily long id source
// needs O(1) memory. Empty and nil sources fail like an empty IN list.
func TestOneDimensionalSliceStream(t *testing.T) {
	ctx := context.Background()
	q, trs := setupTraceDB(t, runtime.WithMaxBatchParams(4)) // 1 scalar + 3 ids per statement
	ids := make([]int64, 10)
	for i := range ids {
		ids[i] = int64(10 - i) // unsorted: each chunk is presorted (ASC) by the chunked method
		if _, err := q.db.Exec(`INSERT INTO batch_users (id, name, points) VALUES (?, 'n', 0)`, ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	trs.reset()
	res, err := q.BumpUsersByIDsStream(ctx, 5, runtime.FromSlice(ids))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 10 {
		t.Fatalf("rows affected = %d, %v; want 10", n, err)
	}
	if n := trs.stmts(); n != 4 { // 3 + 3 + 3 + 1
		t.Fatalf("%d statements, want 4 chunks", n)
	}
	var total int64
	if err := q.db.QueryRow(`SELECT SUM(points) FROM batch_users`).Scan(&total); err != nil || total != 50 {
		t.Fatalf("total points = %d, %v; want 50", total, err)
	}
	if _, err := q.BumpUsersByIDsStream(ctx, 5, runtime.FromSlice([]int64(nil))); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("empty source: %v", err)
	}
	if _, err := q.BumpUsersByIDsStream(ctx, 5, nil); !errors.Is(err, runtime.ErrNilArgument) {
		t.Fatalf("nil source: %v", err)
	}

	// A many statement: the rows of every chunk, in chunk order.
	for i, id := range ids {
		if _, err := q.db.Exec(`INSERT INTO users (id, org_id, email, role, score, bio) VALUES (?, 1, 'e', 'r', ?, '')`, id, int64(i%2)*7); err != nil {
			t.Fatal(err)
		}
	}
	trs.reset()
	got, err := runtime.Collect(q.UserIDsNearScoreStreamSeq(ctx, 7, runtime.FromSlice(ids)))
	if err != nil {
		t.Fatal(err)
	}
	// ids 10..1 with scores 0, 7, 0, 7, ...: score 7 matches 9, 7, 5, 3, 1 (the row order of one statement
	// is unspecified: compare sorted)
	slices.Sort(got)
	if !slices.Equal(got, []int64{1, 3, 5, 7, 9}) {
		t.Fatalf("UserIDsNearScoreStreamSeq = %v", got)
	}
	if n := trs.stmts(); n < 2 {
		t.Fatalf("%d statements, want the source split into chunks", n)
	}
}
