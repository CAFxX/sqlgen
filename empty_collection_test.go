package sqlgen_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// An empty collection is a successful no-op where emptiness means "nothing to do" (the multi-row VALUES list
// of an INSERT) and runtime.ErrEmptySliceParam where it would change the meaning of the SQL (IN lists, a VALUES
// list that feeds something other than an INSERT). Either way nothing is sent: these tests count every
// statement, prepare and transaction that reaches the driver.

// countingDriver wraps a database/sql driver and counts what reaches it.
type countingDriver struct {
	base driver.Driver
	dsn  string
	ops  *atomic.Int64 // statements, prepares and BeginTx
}

func (d countingDriver) Open(string) (driver.Conn, error) {
	c, err := d.base.Open(d.dsn)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: c, ops: d.ops}, nil
}

func (d countingDriver) Connect(context.Context) (driver.Conn, error) { return d.Open("") }
func (d countingDriver) Driver() driver.Driver                        { return d }

type countingConn struct {
	driver.Conn
	ops *atomic.Int64
}

func (c *countingConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	c.ops.Add(1)
	if p, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, q)
	}
	return c.Conn.Prepare(q)
}

func (c *countingConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	c.ops.Add(1)
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, o)
	}
	return c.Conn.Begin()
}

func (c *countingConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	c.ops.Add(1)
	if e, ok := c.Conn.(driver.ExecerContext); ok {
		return e.ExecContext(ctx, q, a)
	}
	return nil, driver.ErrSkip
}

func (c *countingConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	c.ops.Add(1)
	if e, ok := c.Conn.(driver.QueryerContext); ok {
		return e.QueryContext(ctx, q, a)
	}
	return nil, driver.ErrSkip
}

func setupCountingDB(t *testing.T) (*Queries, *sql.DB, *atomic.Int64) {
	t.Helper()
	plain, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	var ops atomic.Int64
	db := sql.OpenDB(countingDriver{base: plain.Driver(), dsn: ":memory:", ops: &ops})
	_ = plain.Close()
	db.SetMaxOpenConns(1) // one connection keeps the in-memory schema
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE batch_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, points INTEGER NOT NULL);
		CREATE TABLE quad_items (id INTEGER PRIMARY KEY, group_key TEXT NOT NULL, priority INTEGER NOT NULL, created_ts INTEGER NOT NULL);
		CREATE TABLE users (id INTEGER PRIMARY KEY, org_id INTEGER NOT NULL, email TEXT NOT NULL, role TEXT NOT NULL, score INTEGER NOT NULL, bio TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	q := New(db)
	t.Cleanup(func() { _ = q.Close() })
	ops.Store(0)
	return q, db, &ops
}

func emptyUsers() iter2[BatchUserInput] { return func(func(BatchUserInput, error) bool) {} }
func emptyQuads() iter2[QuadInput]      { return func(func(QuadInput, error) bool) {} }

type iter2[T any] = func(yield func(T, error) bool)

func TestEmptyValuesCollectionIsANoOp(t *testing.T) {
	ctx := context.Background()
	q, _, ops := setupCountingDB(t)
	sent := func(what string) {
		t.Helper()
		if n := ops.Load(); n != 0 {
			t.Fatalf("%s: %d statements/prepares/transactions reached the driver, want 0", what, n)
		}
	}

	for _, rows := range [][]QuadInput{nil, {}} {
		res, err := q.InsertQuadBatch(ctx, rows)
		if err != nil {
			t.Fatalf("exec with %#v: %v", rows, err)
		}
		if n, err := res.RowsAffected(); err != nil || n != 0 {
			t.Fatalf("exec RowsAffected = %d, %v; want 0, nil", n, err)
		}
		sent("exec")
	}
	if res, err := q.InsertQuadBatchStream(ctx, emptyQuads()); err != nil {
		t.Fatalf("exec Stream of nothing: %v", err)
	} else if n, _ := res.RowsAffected(); n != 0 {
		t.Fatalf("exec Stream of nothing affected %d rows", n)
	}
	sent("exec Stream")

	// many with RETURNING: an empty result, never an error, nil slice.
	if rows, err := q.InsertUsersBatchReturning(ctx, nil); err != nil || len(rows) != 0 || rows != nil {
		t.Fatalf("many = %#v, %v; want a nil slice and no error", rows, err)
	}
	for _, err := range q.InsertUsersBatchReturningSeq(ctx, []BatchUserInput{}) {
		t.Fatalf("many Seq yielded (err = %v) for an empty collection", err)
	}
	if rows, err := q.InsertUsersBatchReturningStream(ctx, emptyUsers()); err != nil || len(rows) != 0 {
		t.Fatalf("many Stream of nothing = %v, %v", rows, err)
	}
	for _, err := range q.InsertUsersBatchReturningStreamSeq(ctx, emptyUsers()) {
		t.Fatalf("many StreamSeq yielded (err = %v) for an empty collection", err)
	}
	sent("many")

	// A nil iterator is still an error, empty or not.
	if _, err := q.InsertQuadBatchStream(ctx, nil); !errors.Is(err, runtime.ErrNilArgument) {
		t.Fatalf("Stream(nil iterator) err = %v, want ErrNilArgument", err)
	}

	// Inside a caller's transaction the no-op does not touch the transaction either.
	if err := q.InTx(ctx, nil, func(txq *Queries) error {
		before := ops.Load()
		if res, err := txq.InsertQuadBatch(ctx, nil); err != nil {
			return err
		} else if n, _ := res.RowsAffected(); n != 0 {
			t.Errorf("tx no-op affected %d rows", n)
		}
		if rows, err := txq.InsertUsersBatchReturning(ctx, nil); err != nil || len(rows) != 0 {
			t.Errorf("tx no-op many = %v, %v", rows, err)
		}
		if ops.Load() != before {
			t.Errorf("a no-op inside a transaction reached the driver")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyInListsStayErrors(t *testing.T) {
	ctx := context.Background()
	q, _, ops := setupCountingDB(t)
	check := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Fatalf("%s: err = %v, want ErrEmptySliceParam", what, err)
		}
	}
	// 1D slice in IN (...), many.
	_, err := q.ListUsersByIDs(ctx, nil)
	check("many IN", err)
	for _, err := range q.ListUsersByIDsSeq(ctx, []int64{}) {
		check("many IN Seq", err)
	}
	_, err = q.UserIDsNearScore(ctx, 1, nil)
	check("scalar many IN", err)
	// A VALUES list that feeds a CTE rather than an INSERT: what an empty list means depends on how the
	// rows are used, so it is an error, not a no-op.
	_, err = q.UpdateUsersBatchFromValues(ctx, nil)
	check("VALUES in a CTE", err)
	// ... in a pipeline the error surfaces at flush, nothing is sent.
	pl := q.Pipeline(ctx)
	_ = pl.UpdateUsersBatchFromValues(nil)
	check("pipeline VALUES in a CTE", pl.Flush())
	pl = q.Pipeline(ctx)
	_ = pl.ListUsersByIDs(nil)
	check("pipeline IN", pl.Flush())
	if n := ops.Load(); n != 0 {
		t.Fatalf("%d statements reached the driver for invalid empty collections", n)
	}
}

func TestEmptyValuesCollectionInPipelines(t *testing.T) {
	ctx := context.Background()
	q, db, ops := setupCountingDB(t)

	// Only no-ops: nothing is sent and no transaction is opened, and the futures resolve to empty/zero.
	pl := q.Pipeline(ctx)
	ex := pl.InsertQuadBatch(nil)
	many := pl.InsertUsersBatchReturning([]BatchUserInput{})
	if err := pl.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n, err := ex.RowsAffected(); err != nil || n != 0 {
		t.Fatalf("exec future: %d, %v", n, err)
	}
	if rows, err := many.Get(); err != nil || len(rows) != 0 {
		t.Fatalf("many future: %v, %v", rows, err)
	}
	if n := ops.Load(); n != 0 {
		t.Fatalf("a pipeline of no-ops sent %d statements/transactions", n)
	}

	// Mixed: the real operations run, in order, in one transaction; the no-ops add no statement.
	pl = q.Pipeline(ctx)
	e1 := pl.InsertQuadBatch([]QuadInput{{ID: 1, GroupKey: "a", Priority: 1, CreatedTs: 1}})
	n1 := pl.InsertQuadBatch(nil)
	m1 := pl.InsertUsersBatchReturning([]BatchUserInput{{ID: 7, Name: "x", Points: 2}})
	m0 := pl.InsertUsersBatchReturning(nil)
	if err := pl.Flush(); err != nil {
		t.Fatalf("mixed Flush: %v", err)
	}
	if n, err := e1.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("real exec: %d, %v", n, err)
	}
	if n, err := n1.RowsAffected(); err != nil || n != 0 {
		t.Fatalf("no-op exec: %d, %v", n, err)
	}
	if rows, err := m1.Get(); err != nil || !slices.Equal(rows, []InsertUsersBatchReturningRow{{ID: 7, Name: "x", Points: 2}}) {
		t.Fatalf("real many: %v, %v", rows, err)
	}
	if rows, err := m0.Get(); err != nil || len(rows) != 0 {
		t.Fatalf("no-op many: %v, %v", rows, err)
	}
	var cnt int
	if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM quad_items) + (SELECT COUNT(*) FROM batch_users)`).Scan(&cnt); err != nil || cnt != 2 {
		t.Fatalf("rows = %d, %v; want 2", cnt, err)
	}

	// PipelineTx with only no-ops: no transaction.
	ops.Store(0)
	var f *runtime.ExecFuture
	if err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
		f = p.InsertQuadBatch(nil)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := f.RowsAffected(); err != nil || n != 0 {
		t.Fatalf("PipelineTx no-op: %d, %v", n, err)
	}
	if n := ops.Load(); n != 0 {
		t.Fatalf("PipelineTx of no-ops sent %d statements/transactions", n)
	}

	// A no-op in an untaken branch is skipped, like any operation.
	pl = q.Pipeline(ctx)
	probe := pl.CreateUser(1, 1, "a@x", "m", 1, "b")
	var skipped *runtime.ExecFuture
	res := pl.If(probe.RowsEq(5), func(p *Pipeline) { skipped = p.InsertQuadBatch(nil) })
	if err := pl.Flush(); err != nil {
		t.Fatal(err)
	}
	if res.Taken(0) {
		t.Fatal("branch must not be taken")
	}
	if _, err := skipped.RowsAffected(); !errors.Is(err, runtime.ErrBranchNotTaken) {
		t.Fatalf("no-op in an untaken branch: err = %v", err)
	}
}

// A many statement that matches nothing returns a nil slice and no error from every entry point (document:
// test len(rows) == 0, not rows == nil).
func TestNoRowsIsANilSliceEverywhere(t *testing.T) {
	ctx := context.Background()
	_, q := setupE2EDB(t)
	if rows, err := q.ListUsersByIDs(ctx, []int64{404}); err != nil || rows != nil {
		t.Fatalf("chunked many with no match = %#v, %v; want nil, nil", rows, err)
	}
	if rows, err := q.SelectUserProfileSubset(ctx, SelectUserProfileSubsetParams{OrgID: 404}); err != nil || rows != nil {
		t.Fatalf("plain many with no match = %#v, %v; want nil, nil", rows, err)
	}
	if rows, err := runtime.Collect(q.ListUsersByIDsSeq(ctx, []int64{404})); err != nil || rows != nil {
		t.Fatalf("Collect with no match = %#v, %v; want nil, nil", rows, err)
	}
	keep := []User{{ID: 1}}
	if out, err := runtime.AppendTo(keep, q.ListUsersByIDsSeq(ctx, []int64{404})); err != nil || len(out) != 1 || out[0].ID != 1 {
		t.Fatalf("AppendTo with no match = %v, %v; want the caller's rows untouched", out, err)
	}
	// ... and never drops the caller's rows on a failure.
	out, err := runtime.AppendTo(keep, q.ListUsersByIDsSeq(ctx, nil))
	if !errors.Is(err, runtime.ErrEmptySliceParam) || len(out) != 1 || out[0].ID != 1 {
		t.Fatalf("AppendTo on error = %v, %v; want dst unchanged and the error", out, err)
	}
	pl := q.Pipeline(ctx)
	f := pl.ListUsersByIDs([]int64{404})
	if err := pl.Flush(); err != nil {
		t.Fatal(err)
	}
	if rows, err := f.Get(); err != nil || rows != nil {
		t.Fatalf("pipeline many with no match = %#v, %v; want nil, nil", rows, err)
	}
}

// An empty IN list is ErrEmptySliceParam on every entry point, the Stream forms included (an empty INSERT
// VALUES list stays a no-op); nothing is sent.
func TestEmptyInListStreamIsAnError(t *testing.T) {
	q, _, ops := setupCountingDB(t)
	ctx := context.Background()
	if _, err := q.DeleteQuadsByKeys(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("DeleteQuadsByKeys(nil): err = %v", err)
	}
	if _, err := q.DeleteQuadsByKeysStream(ctx, func(func(QuadDelKey, error) bool) {}); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("DeleteQuadsByKeysStream(empty): err = %v, want ErrEmptySliceParam", err)
	}
	if rows, err := q.QuadIDsByKeysStream(ctx, QuadIDsByKeysParams{}, func(func(QuadKey, error) bool) {}); rows != nil || !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("QuadIDsByKeysStream(empty) = %v, %v; want ErrEmptySliceParam", rows, err)
	}
	if n := ops.Load(); n != 0 {
		t.Fatalf("%d driver operations, want none", n)
	}
	// A non-empty stream works.
	if rows, err := q.QuadIDsByKeysStream(ctx, QuadIDsByKeysParams{}, runtime.FromSlice([]QuadKey{{GroupKey: "g", ID: 1}})); err != nil || len(rows) != 0 {
		t.Fatalf("QuadIDsByKeysStream = %v, %v", rows, err)
	}
}

// A struct-style Stream takes its rows from seq: a non-empty collection field in the params struct is an
// error naming the field instead of being silently ignored.
func TestStructStreamRejectsCollectionField(t *testing.T) {
	q, _, ops := setupCountingDB(t)
	ctx := context.Background()
	p := QuadIDsByKeysParams{Keys: []QuadKey{{GroupKey: "g", ID: 1}}}
	_, err := q.QuadIDsByKeysStream(ctx, p, runtime.FromSlice([]QuadKey{{GroupKey: "g", ID: 2}}))
	if !errors.Is(err, runtime.ErrInvalidArgument) || !strings.Contains(err.Error(), "p.Keys must be empty") {
		t.Fatalf("err = %v, want ErrInvalidArgument naming p.Keys", err)
	}
	if n := ops.Load(); n != 0 {
		t.Fatalf("%d driver operations, want none", n)
	}
}
