package pgxwire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/CAFxX/sqlgen/runtime"
)

type foreignConn struct{ driver.Conn }

func (foreignConn) Close() error { return nil }

type foreignConnector struct{}

func (foreignConnector) Connect(context.Context) (driver.Conn, error) { return foreignConn{}, nil }
func (foreignConnector) Driver() driver.Driver                        { return nil }

func TestConnectorRejectsForeignConnections(t *testing.T) {
	if _, err := (&connector{base: foreignConnector{}}).Connect(context.Background()); err == nil || !strings.Contains(err.Error(), "unexpected connection type") {
		t.Fatalf("err = %v", err)
	}
}

func TestBeginSQL(t *testing.T) {
	cases := []struct {
		o    *sql.TxOptions
		want string
	}{
		{nil, "BEGIN"},
		{&sql.TxOptions{}, "BEGIN"},
		{&sql.TxOptions{ReadOnly: true}, "BEGIN READ ONLY"},
		{&sql.TxOptions{Isolation: sql.LevelReadUncommitted}, "BEGIN ISOLATION LEVEL READ UNCOMMITTED"},
		{&sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: true}, "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY"},
		{&sql.TxOptions{Isolation: sql.LevelRepeatableRead}, "BEGIN ISOLATION LEVEL REPEATABLE READ"},
		{&sql.TxOptions{Isolation: sql.LevelSnapshot}, "BEGIN ISOLATION LEVEL REPEATABLE READ"},
		{&sql.TxOptions{Isolation: sql.LevelSerializable}, "BEGIN ISOLATION LEVEL SERIALIZABLE"},
	}
	for _, tc := range cases {
		if got, err := beginSQL(tc.o); err != nil || got != tc.want {
			t.Errorf("beginSQL(%+v) = %q, %v; want %q", tc.o, got, err, tc.want)
		}
	}
	if _, err := beginSQL(&sql.TxOptions{Isolation: sql.LevelLinearizable}); err == nil {
		t.Error("LevelLinearizable must be rejected")
	}
}

func TestCondValueAndOpName(t *testing.T) {
	res := func(rows ...[]byte) *pgconn.Result {
		r := &pgconn.Result{}
		for _, v := range rows {
			r.Rows = append(r.Rows, [][]byte{v})
		}
		return r
	}
	for _, tc := range []struct {
		res      *pgconn.Result
		val, set bool
	}{
		{res([]byte("true")), true, true},
		{res([]byte("false")), false, true},
		{res(nil), false, false},                            // NULL: the producer was gated off
		{res([]byte("")), false, false},                     // the placeholder GUC outlives the transaction as ''
		{res(), false, false},                               // no row
		{res([]byte("true"), []byte("true")), false, false}, // not a single answer
	} {
		if v, s := condValue(tc.res); v != tc.val || s != tc.set {
			t.Errorf("condValue(%v) = %v, %v", tc.res.Rows, v, s)
		}
	}
	if opName(&runtime.PipelineOp{}) != "pipeline statement" || opName(&runtime.PipelineOp{Desc: &runtime.QueryDescriptor{Name: "Upd"}}) != "Upd" {
		t.Error("opName")
	}
}

// ---- tests that need a server (any of the configured engines) ----

func rawConn(t *testing.T) (*conn, func()) {
	t.Helper()
	dsn := firstDSN(t)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	dc, err := (&connector{base: stdlib.GetConnector(*cfg)}).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return dc.(*conn), func() { _ = dc.Close() }
}

func TestRowsConversionErrors(t *testing.T) {
	c, done := rawConn(t)
	defer done()
	fds := []pgconn.FieldDescription{
		{Name: "n", DataTypeOID: pgtype.Int8OID},
		{Name: "d", DataTypeOID: pgtype.DateOID},
		{Name: "t", DataTypeOID: pgtype.TextOID},
	}
	r := c.driverRows(&pgconn.Result{FieldDescriptions: fds, Rows: [][][]byte{
		{[]byte("7"), []byte("2024-02-29"), nil},
		{[]byte("8"), []byte("not a date"), []byte("x")},
	}})
	if cols := r.Columns(); len(cols) != 3 || cols[1] != "d" {
		t.Fatalf("Columns = %v", cols)
	}
	dest := make([]driver.Value, 3)
	if err := r.Next(dest); err != nil || dest[0] != int64(7) || dest[2] != nil {
		t.Fatalf("first row: %v %v", dest, err)
	}
	if tm, ok := dest[1].(time.Time); !ok || tm.Day() != 29 {
		t.Fatalf("date = %v", dest[1])
	}
	if err := r.Next(dest); err == nil || !strings.Contains(err.Error(), "convert field 1 (d)") {
		t.Fatalf("second row err = %v", err)
	}
	if err := r.Next(dest); err == nil || err.Error() != "EOF" {
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = r.Close()
	// converters are cached per type
	if c.converterFor(pgtype.Int8OID) == nil || len(c.conv) != 3 {
		t.Fatalf("converter cache = %d entries", len(c.conv))
	}
}

func TestDeliverOpErrors(t *testing.T) {
	c, done := rawConn(t)
	defer done()
	ctx := context.Background()
	guarded := &runtime.PipelineOp{Decorators: runtime.OpDecorators{AssertRows: 1}}
	if err := c.deliverOp(ctx, guarded, &pgconn.Result{Rows: [][][]byte{{[]byte("not a number")}}}); err == nil || !strings.Contains(err.Error(), "affected-row count") {
		t.Fatalf("err = %v", err)
	}
	if err := c.deliverOp(ctx, guarded, &pgconn.Result{}); err != nil || guarded.RowCount != 0 {
		t.Fatalf("no row: %v, RowCount=%d", err, guarded.RowCount)
	}
	boom := errors.New("scan failed")
	rowsOp := &runtime.PipelineOp{HasResultCols: true, Decorators: runtime.OpDecorators{AssertRows: -1},
		ScanWireRows: func(*sql.Rows) (int64, error) { return 0, boom }}
	if err := c.deliverOp(ctx, rowsOp, &pgconn.Result{FieldDescriptions: []pgconn.FieldDescription{{Name: "x", DataTypeOID: pgtype.Int8OID}}}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestFailTranslatesByStatementRole(t *testing.T) {
	c, done := rawConn(t)
	defer done()
	pg := c.Conn.Conn().PgConn()
	ctx := context.Background()
	op := &runtime.PipelineOp{Decorators: runtime.OpDecorators{AssertRows: 1}}
	refs := []stmtRef{{role: roleBegin}, {role: roleOp, op: op}, {role: roleCond, op: op}, {role: roleCommit}}
	ok := &pgconn.Result{}
	// the guard's own error: invalid_text_representation carrying the assertion marker
	serverErr := &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type integer: "sqlgen:assert:0"`}

	// the failed statement is the first one without a clean result
	if err := c.fail(ctx, pg, false, refs, nil, serverErr); err == nil || errors.Is(err, runtime.ErrAssertionFailed) {
		t.Fatalf("BEGIN failing must pass the server error through, got %v", err)
	}
	if err := c.fail(ctx, pg, false, refs, []*pgconn.Result{ok}, serverErr); !errors.Is(err, runtime.ErrAssertionFailed) {
		t.Fatalf("the guard's error on an asserting op is an assertion failure, got %v", err)
	}
	failedRes := &pgconn.Result{Err: serverErr}
	if err := c.fail(ctx, pg, false, refs, []*pgconn.Result{ok, failedRes}, serverErr); !errors.Is(err, runtime.ErrAssertionFailed) {
		t.Fatalf("a failed result is the statement that failed, got %v", err)
	}
	// a genuine overflow of the user's own statement is NOT the guard: it passes through unchanged
	overflow := &pgconn.PgError{Code: "22003", Message: "bigint out of range"}
	if err := c.fail(ctx, pg, false, refs, []*pgconn.Result{ok}, overflow); err != overflow {
		t.Fatalf("a genuine overflow must surface as the database error, got %v", err)
	}
	if err := c.fail(ctx, pg, false, refs, []*pgconn.Result{ok, ok, ok}, serverErr); err == nil {
		t.Fatal("expected the commit error")
	} else if _, isCommit := err.(*runtime.CommitError); !isCommit {
		t.Fatalf("a failing COMMIT must be a *CommitError, got %T", err)
	}
	if err := c.fail(ctx, pg, false, refs, []*pgconn.Result{ok, ok}, serverErr); err != serverErr {
		t.Fatalf("a failing condition read-back is passed through, got %v", err)
	}
	if err := c.fail(ctx, pg, false, refs[:1], []*pgconn.Result{ok}, serverErr); err != serverErr {
		t.Fatalf("an error past the last statement is passed through, got %v", err)
	}
	// transport errors: a commit-time failure inside a transaction, plain otherwise
	transport := errors.New("connection reset")
	if err := c.fail(ctx, pg, false, refs, nil, transport); err != transport {
		t.Fatalf("got %v", err)
	}
	if err := c.fail(ctx, pg, true, refs, nil, transport); !errors.Is(err, transport) {
		t.Fatalf("got %v", err)
	} else if _, isCommit := err.(*runtime.CommitError); !isCommit {
		t.Fatalf("a transport error inside a transaction has an unknown outcome and must not be retried: %T", err)
	}
}

// If even the ROLLBACK cannot be delivered the connection is closed; database/sql then discards it at the
// next checkout because ResetSession reports ErrBadConn.
func TestFailClosesAConnectionThatCannotBeRolledBack(t *testing.T) {
	c, done := rawConn(t)
	defer done()
	pg := c.Conn.Conn().PgConn()
	ctx := context.Background()
	if v, err := pg.Exec(ctx, "SELECT version()").ReadAll(); err != nil || strings.Contains(string(v[0].Rows[0][0]), "CockroachDB") {
		t.Skip("needs pg_terminate_backend (PostgreSQL)")
	}
	if _, err := pg.Exec(ctx, "BEGIN").ReadAll(); err != nil || pg.TxStatus() != 'T' {
		t.Fatalf("BEGIN: %v, status %c", err, pg.TxStatus())
	}
	var pid int
	res, err := pg.Exec(ctx, "SELECT pg_backend_pid()").ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	pid = int(parseInt(t, res[0].Rows[0][0]))

	killer, err := pgx.Connect(ctx, firstDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer killer.Close(ctx)
	if _, err := killer.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	got := c.fail(ctx, pg, true, nil, nil, &pgconn.PgError{Code: "40001"})
	if got == nil || !pg.IsClosed() || !errors.Is(c.ResetSession(ctx), driver.ErrBadConn) {
		t.Fatalf("err = %v, closed = %v", got, pg.IsClosed())
	}
	if err := c.ExecPipeline(ctx, nil); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("a closed connection must report ErrBadConn, got %v", err)
	}
}

// firstDSN returns the DSN of the first selected engine (SQLGEN_ENGINES, as in the integration module).
func firstDSN(t *testing.T) string {
	t.Helper()
	sel := strings.Split(strings.TrimSpace(os.Getenv("SQLGEN_ENGINES")), ",")
	for _, e := range [][2]string{{"postgres15", "SQLGEN_POSTGRES15_DSN"}, {"postgres18", "SQLGEN_POSTGRES18_DSN"}, {"cockroach", "SQLGEN_COCKROACH_DSN"}, {"yugabyte", "SQLGEN_YUGABYTE_DSN"}} {
		if sel[0] != "" && !slices.Contains(sel, e[0]) {
			continue
		}
		if dsn := os.Getenv(e[1]); dsn != "" {
			return dsn
		}
	}
	t.Skip("no PostgreSQL-protocol DSN configured")
	return ""
}

func parseInt(t *testing.T, b []byte) int64 {
	t.Helper()
	var n int64
	for _, c := range b {
		n = n*10 + int64(c-'0')
	}
	return n
}
