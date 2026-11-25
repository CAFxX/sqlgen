package mysqlwire

// Hermetic tests: a scripted driver.Conn stands in for the server, to reach the error paths a real server
// cannot be made to produce on demand (a transport error at the COMMIT, a recovery turn that fails, a
// summary row that is missing). No database is needed.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/runtime"
)

// ---- scripted rows ----

type fset struct {
	cols    []string
	rows    [][]driver.Value
	nextErr error // returned by NextResultSet when moving to this set
	rowErr  error // returned by Next instead of io.EOF once the rows are exhausted
}

type frows struct {
	sets     []fset
	cur, pos int
	closeErr error
	closed   bool
}

func (r *frows) Columns() []string { return r.sets[r.cur].cols }
func (r *frows) Close() error      { r.closed = true; return r.closeErr }
func (r *frows) Next(dest []driver.Value) error {
	s := &r.sets[r.cur]
	if r.pos >= len(s.rows) {
		if s.rowErr != nil {
			return s.rowErr
		}
		return io.EOF
	}
	copy(dest, s.rows[r.pos])
	r.pos++
	return nil
}
func (r *frows) HasNextResultSet() bool { return r.cur+1 < len(r.sets) }
func (r *frows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.cur++
	r.pos = 0
	return r.sets[r.cur].nextErr
}

// onlyRows hides the multi-result-set capability of frows.
type onlyRows struct{ r *frows }

func (o onlyRows) Columns() []string              { return o.r.Columns() }
func (o onlyRows) Close() error                   { return o.r.Close() }
func (o onlyRows) Next(dest []driver.Value) error { return o.r.Next(dest) }

func one(vals ...driver.Value) fset {
	return fset{cols: make([]string, len(vals)), rows: [][]driver.Value{vals}}
}

func rowsOf(sets ...fset) *frows { return &frows{sets: sets} }

// ---- scripted connection ----

type resp struct {
	rows driver.Rows
	err  error
}

type minConn struct {
	mu      sync.Mutex
	queries []string
	script  []resp                                    // consumed in order
	route   func(q string) (driver.Rows, error, bool) // consulted first
	closed  bool
	beginE  error
	fwd     func() (driver.Rows, error) // answers the forwarding probe (default: a capable server)
}

func (c *minConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("fake: prepare") }
func (c *minConn) Close() error                        { c.closed = true; return nil }
func (c *minConn) Begin() (driver.Tx, error)           { return &fakeTx{}, c.beginE }

func (c *minConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, q)
	if c.route != nil {
		if r, err, ok := c.route(q); ok {
			return r, err
		}
	}
	if strings.HasPrefix(q, "DO LAST_INSERT_ID(0);SELECT (@sqlgen_probe_v") {
		if c.fwd != nil {
			return c.fwd()
		}
		return rowsOf(one(int64(2), int64(0))), nil
	}
	if len(c.script) == 0 {
		return nil, fmt.Errorf("fake: unscripted query %.60q", q)
	}
	r := c.script[0]
	c.script = c.script[1:]
	return r.rows, r.err
}

func (c *minConn) log() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.queries...)
}

type fakeTx struct{ committed, rolled bool }

func (t *fakeTx) Commit() error   { t.committed = true; return nil }
func (t *fakeTx) Rollback() error { t.rolled = true; return nil }

// fullConn adds every optional driver interface conn forwards to.
type fullConn struct {
	*minConn
	valid     bool
	beginOpts driver.TxOptions
	calls     []string
}

func (c *fullConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.calls = append(c.calls, "exec:"+q)
	return driver.RowsAffected(7), nil
}
func (c *fullConn) BeginTx(_ context.Context, o driver.TxOptions) (driver.Tx, error) {
	c.beginOpts = o
	c.calls = append(c.calls, "begintx")
	return &fakeTx{}, c.beginE
}
func (c *fullConn) PrepareContext(_ context.Context, q string) (driver.Stmt, error) {
	c.calls = append(c.calls, "prepare:"+q)
	return nil, nil
}
func (c *fullConn) Ping(context.Context) error { c.calls = append(c.calls, "ping"); return nil }
func (c *fullConn) ResetSession(context.Context) error {
	c.calls = append(c.calls, "reset")
	return nil
}
func (c *fullConn) IsValid() bool { return c.valid }
func (c *fullConn) CheckNamedValue(*driver.NamedValue) error {
	c.calls = append(c.calls, "check")
	return nil
}

type fakeConnector struct {
	conn driver.Conn
	err  error
}

func (f fakeConnector) Connect(context.Context) (driver.Conn, error) { return f.conn, f.err }
func (f fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("fake") }

// connNoQuery is a driver.Conn without QueryerContext.
type connNoQuery struct{ closed *bool }

func (c connNoQuery) Prepare(string) (driver.Stmt, error) { return nil, nil }
func (c connNoQuery) Close() error                        { *c.closed = true; return nil }
func (c connNoQuery) Begin() (driver.Tx, error)           { return nil, nil }

func myErr(n uint16) error {
	return &mysql.MySQLError{Number: n, Message: fmt.Sprintf("fake error %d", n)}
}

// ---- helpers ----

func newConn(mc *minConn) *conn {
	return &conn{Conn: mc, qc: mc, c: &connector{state: capYes, loc: time.UTC}, pipelining: true, maxPacket: 1 << 20}
}

var errTransport = errors.New("fake: connection reset by peer")

func isMySQL(err error, n uint16) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == n
}

func isCommitError(err error) bool {
	var ce *runtime.CommitError
	return errors.As(err, &ce)
}

// A batch of ops [select, update with an assertion]: its result sets are the select's rows and the summary
// row (@sqlgen_f, @sqlgen_n1).
func selUpdOps(scanErr error, scanned *int64) []runtime.PipelineOp {
	sel := &runtime.QueryDescriptor{Name: "Sel", StmtVerb: runtime.VerbSelect, Segments: []string{"SELECT id, v FROM t WHERE id = ", ""}, Slots: []runtime.Slot{{Kind: runtime.SlotScalar}}}
	upd := &runtime.QueryDescriptor{Name: "Upd", StmtVerb: runtime.VerbUpdate, Segments: []string{"UPDATE t SET v = ", " WHERE id = ", ""}, Slots: []runtime.Slot{{Kind: runtime.SlotScalar}, {Kind: runtime.SlotScalar}}}
	return []runtime.PipelineOp{
		{Desc: sel, Args: []any{int64(1)}, HasResultCols: true, Decorators: runtime.OpDecorators{AssertRows: -1}, ScanWireRows: scanFn(scanErr, scanned)},
		{Desc: upd, Args: []any{int64(5), int64(1)}, Decorators: runtime.OpDecorators{AssertRows: 1}},
	}
}

func scanFn(scanErr error, scanned *int64) func(*sql.Rows) (int64, error) {
	return func(rows *sql.Rows) (int64, error) {
		var n int64
		for rows.Next() {
			var id, v int64
			if err := rows.Scan(&id, &v); err != nil {
				return n, err
			}
			n++
		}
		if scanned != nil {
			*scanned = n
		}
		if scanErr != nil {
			return n, scanErr
		}
		return n, rows.Err()
	}
}

func selSet(rows ...[]driver.Value) fset { return fset{cols: []string{"id", "v"}, rows: rows} }
func sumSet(vals ...driver.Value) fset   { return one(vals...) }

// ---- tests ----

func TestHasReturning(t *testing.T) {
	for v, want := range map[string]bool{
		"10.11.19-MariaDB":           true,
		"10.11.19-MariaDB-ubu2204":   true,
		"5.5.5-10.6.12-MariaDB":      true,
		"5.5.5-10.4.0-MariaDB":       false,
		"10.4.0-MariaDB":             false,
		"10.5.0-MariaDB":             true,
		"11.0.1-MariaDB":             true,
		"12.3.1-MariaDB-ubu2404":     true,
		"9.0.0-MARIADB":              false,
		"8.4.0":                      false,
		"9.7.0":                      false,
		"10.11.2-log":                false,
		"MariaDB":                    false,
		"x-MariaDB":                  false,
		"":                           false,
		"5.5.5-MariaDB":              false,
		"10-MariaDB":                 false,
		"5.5.5-11.2.2-MariaDB-debug": true,
	} {
		if got := hasReturning(v); got != want {
			t.Errorf("hasReturning(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestIsTiDB(t *testing.T) {
	for v, want := range map[string]bool{
		"8.0.11-TiDB-v8.5.8":            true,
		"5.7.25-TiDB-v6.5.0-serverless": true,
		"8.4.0":                         false,
		"10.11.2-MariaDB":               false,
		"tidb":                          false,
		"":                              false,
	} {
		if got := isTiDB(v); got != want {
			t.Errorf("isTiDB(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestSummaryIntAndAsString(t *testing.T) {
	for _, tc := range []struct {
		in   driver.Value
		want int64
		ok   bool
	}{
		{int64(-3), -3, true}, {uint64(7), 7, true}, {float64(2.9), 2, true},
		{[]byte("42"), 42, true}, {"-1", -1, true},
		{[]byte("x"), 0, false}, {"1.5", 0, false}, {nil, 0, false}, {true, 0, false}, {time.Time{}, 0, false},
	} {
		if got, ok := summaryInt(tc.in); got != tc.want || ok != tc.ok {
			t.Errorf("summaryInt(%#v) = %d, %v; want %d, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
	if asString([]byte("a")) != "a" || asString("b") != "b" || asString(int64(1)) != "" || asString(nil) != "" {
		t.Error("asString")
	}
}

func TestWireFlavorAndErrorTypes(t *testing.T) {
	if (&conn{}).WireFlavor() != runtime.WireFlavorMySQL {
		t.Error("WireFlavor")
	}
	se := &scanError{err: errTransport}
	if se.Error() != errTransport.Error() || !errors.Is(se, errTransport) {
		t.Error("scanError")
	}
	if (&connector{base: fakeConnector{}}).Driver() == nil {
		t.Error("connector.Driver forwards to the base connector")
	}
}

func probeRows(sets ...fset) *frows { return rowsOf(sets...) }

func goodProbe() *frows { return probeRows(one(int64(1)), one(int64(0), int64(0), []byte("1"))) }

func probeConn(rows func() (driver.Rows, error)) (*connector, *conn, *minConn) {
	mc := &minConn{route: func(q string) (driver.Rows, error, bool) {
		if strings.HasPrefix(q, "SET @sqlgen_probe") {
			r, err := rows()
			return r, err, true
		}
		return nil, nil, false
	}}
	c := &connector{}
	return c, &conn{Conn: mc, qc: mc, c: c}, mc
}

func TestProbeVerdicts(t *testing.T) {
	ctx := context.Background()
	closeFail := func(err error) func() (driver.Rows, error) {
		return func() (driver.Rows, error) { r := goodProbe(); r.closeErr = err; return r, nil }
	}
	for _, tc := range []struct {
		name      string
		rows      func() (driver.Rows, error)
		version   string
		wantErr   bool
		wantState capState
		returning bool
	}{
		{"success", func() (driver.Rows, error) { return goodProbe(), nil }, "8.4.0", false, capYes, false},
		{"success on mariadb with RETURNING", func() (driver.Rows, error) { return goodProbe(), nil }, "10.11.19-MariaDB", false, capYes, true},
		{"success on an old mariadb", func() (driver.Rows, error) { return goodProbe(), nil }, "10.4.0-MariaDB", false, capYes, false},
		{"server error is a verdict", func() (driver.Rows, error) { return nil, myErr(1064) }, "", false, capNo, false},
		{"transport error is not", func() (driver.Rows, error) { return nil, errTransport }, "", true, capUnknown, false},
		{"first row fails", func() (driver.Rows, error) {
			return probeRows(fset{cols: []string{"1"}, rowErr: errTransport}, one(int64(0), int64(0), int64(1))), nil
		}, "", false, capNo, false},
		{"no second result set", func() (driver.Rows, error) { return probeRows(one(int64(1))), nil }, "", false, capNo, false},
		{"second result set fails to open", func() (driver.Rows, error) {
			s := one(int64(0), int64(0), int64(1))
			s.nextErr = errTransport
			return probeRows(one(int64(1)), s), nil
		}, "", false, capNo, false},
		{"rows without multi result sets", func() (driver.Rows, error) { return onlyRows{goodProbe()}, nil }, "", false, capNo, false},
		{"values row fails", func() (driver.Rows, error) {
			return probeRows(one(int64(1)), fset{cols: []string{"a", "b", "c"}, rowErr: errTransport}), nil
		}, "", false, capNo, false},
		{"user variable lost", func() (driver.Rows, error) {
			return probeRows(one(int64(1)), one(int64(0), int64(0), int64(2))), nil
		}, "", false, capNo, false},
		{"user variable not a number", func() (driver.Rows, error) {
			return probeRows(one(int64(1)), one(int64(0), int64(0), nil)), nil
		}, "", false, capNo, false},
		{"close fails with a server error", closeFail(myErr(1105)), "", false, capNo, false},
		{"close fails with a transport error", closeFail(errTransport), "", true, capUnknown, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, cn, mc := probeConn(tc.rows)
			err := c.probe(ctx, cn, tc.version)
			if (err != nil) != tc.wantErr {
				t.Fatalf("probe err = %v, want error: %v", err, tc.wantErr)
			}
			ok, ret, _, _ := c.capable()
			if c.state != tc.wantState || ok != (tc.wantState == capYes) || ret != tc.returning {
				t.Fatalf("state = %v capable = %v returning = %v", c.state, ok, ret)
			}
			// a verdict is cached; a transport error is not
			before := len(mc.log())
			_ = c.probe(ctx, cn, tc.version)
			after := len(mc.log())
			if (tc.wantState == capUnknown) != (after > before) {
				t.Fatalf("second probe: %d -> %d queries", before, after)
			}
		})
	}
}

func TestProbeIsRunOncePerConnectorUnderConcurrency(t *testing.T) {
	var n atomic.Int64
	c := &connector{}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		mc := &minConn{route: func(q string) (driver.Rows, error, bool) {
			if strings.HasPrefix(q, "SET @sqlgen_probe") {
				n.Add(1)
				time.Sleep(time.Millisecond)
				return goodProbe(), nil, true
			}
			return nil, nil, false
		}}
		cn := &conn{Conn: mc, qc: mc, c: c}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.probe(context.Background(), cn, "10.6.1-MariaDB"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n.Load() != 1 {
		t.Fatalf("the probe ran %d times", n.Load())
	}
	if ok, ret, _, _ := c.capable(); !ok || !ret {
		t.Fatalf("capable = %v, %v", ok, ret)
	}
}

func TestInit(t *testing.T) {
	ctx := context.Background()
	facts := func(max driver.Value) *frows {
		return rowsOf(one(max, []byte("utf8mb4"), []byte("10.11.4-MariaDB")))
	}
	for _, tc := range []struct {
		name       string
		facts      func() (driver.Rows, error)
		probe      func() (driver.Rows, error)
		drvLimit   int
		wantErr    bool
		pipelining bool
		maxPacket  int
	}{
		{"success", func() (driver.Rows, error) { return facts(int64(4096)), nil }, func() (driver.Rows, error) { return goodProbe(), nil }, 0, false, true, 4096},
		{"the driver's limit is smaller", func() (driver.Rows, error) { return facts(int64(4096)), nil }, func() (driver.Rows, error) { return goodProbe(), nil }, 1000, false, true, 1000},
		{"the driver's limit is larger", func() (driver.Rows, error) { return facts([]byte("4096")), nil }, func() (driver.Rows, error) { return goodProbe(), nil }, 1 << 20, false, true, 4096},
		{"query refused by the server", func() (driver.Rows, error) { return nil, myErr(1193) }, nil, 0, false, false, 0},
		{"transport error", func() (driver.Rows, error) { return nil, errTransport }, nil, 0, true, false, 0},
		{"row refused by the server", func() (driver.Rows, error) {
			return rowsOf(fset{cols: []string{"a", "b", "c"}, rowErr: myErr(1105)}), nil
		}, nil, 0, false, false, 0},
		{"row read fails", func() (driver.Rows, error) {
			return rowsOf(fset{cols: []string{"a", "b", "c"}, rowErr: errTransport}), nil
		}, nil, 0, true, false, 0},
		{"close fails", func() (driver.Rows, error) { r := facts(int64(1)); r.closeErr = errTransport; return r, nil }, nil, 0, true, false, 0},
		{"unknown max_allowed_packet", func() (driver.Rows, error) { return facts(nil), nil }, nil, 0, false, false, 0},
		{"zero max_allowed_packet", func() (driver.Rows, error) { return facts(int64(0)), nil }, nil, 0, false, false, 0},
		{"probe transport error", func() (driver.Rows, error) { return facts(int64(4096)), nil }, func() (driver.Rows, error) { return nil, errTransport }, 0, true, false, 4096},
		{"probe says no", func() (driver.Rows, error) { return facts(int64(4096)), nil }, func() (driver.Rows, error) { return nil, myErr(1064) }, 0, false, true, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := &minConn{route: func(q string) (driver.Rows, error, bool) {
				switch {
				case strings.HasPrefix(q, "SELECT @@max_allowed_packet"):
					r, err := tc.facts()
					return r, err, true
				case strings.HasPrefix(q, "SET @sqlgen_probe"):
					r, err := tc.probe()
					return r, err, true
				}
				return nil, nil, false
			}}
			c := &connector{maxPacket: tc.drvLimit}
			cn := &conn{Conn: mc, qc: mc, c: c}
			err := cn.init(ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("init err = %v", err)
			}
			if cn.pipelining != tc.pipelining || cn.maxPacket != tc.maxPacket {
				t.Fatalf("pipelining = %v maxPacket = %d", cn.pipelining, cn.maxPacket)
			}
			if tc.pipelining && (cn.charset != "utf8mb4" || cn.version != "10.11.4-MariaDB") {
				t.Fatalf("charset = %q version = %q", cn.charset, cn.version)
			}
		})
	}
}

func TestConnectorConnect(t *testing.T) {
	ctx := context.Background()
	if _, err := (&connector{base: fakeConnector{err: errTransport}}).Connect(ctx); !errors.Is(err, errTransport) {
		t.Fatalf("err = %v", err)
	}
	closed := false
	if _, err := (&connector{base: fakeConnector{conn: connNoQuery{&closed}}}).Connect(ctx); err == nil || !strings.Contains(err.Error(), "unexpected connection type") || !closed {
		t.Fatalf("err = %v, closed = %v", err, closed)
	}
	// an init failure closes the connection
	mc := &minConn{script: []resp{{err: errTransport}}}
	if _, err := (&connector{base: fakeConnector{conn: mc}}).Connect(ctx); !errors.Is(err, errTransport) || !mc.closed {
		t.Fatalf("err = %v, closed = %v", err, mc.closed)
	}
	// success
	mc = &minConn{script: []resp{
		{rows: rowsOf(one(int64(1<<20), []byte("utf8mb4"), []byte("8.4.0")))},
		{rows: goodProbe()},
	}}
	dc, err := (&connector{base: fakeConnector{conn: mc}}).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cn := dc.(*conn)
	if !cn.pipelining || cn.version != "8.4.0" || cn.charset != "utf8mb4" {
		t.Fatalf("%+v", cn)
	}
}

func TestForwardingToABasicDriverConn(t *testing.T) {
	ctx := context.Background()
	mc := &minConn{}
	cn := newConn(mc)
	if _, err := cn.ExecContext(ctx, "x", nil); !errors.Is(err, driver.ErrSkip) {
		t.Fatalf("ExecContext = %v", err)
	}
	if _, err := cn.PrepareContext(ctx, "x"); err == nil || err.Error() != "fake: prepare" {
		t.Fatalf("PrepareContext = %v", err)
	}
	if cn.Ping(ctx) != nil || cn.ResetSession(ctx) != nil || !cn.IsValid() {
		t.Fatal("Ping/ResetSession/IsValid must be no-ops without driver support")
	}
	if err := cn.CheckNamedValue(&driver.NamedValue{}); !errors.Is(err, driver.ErrSkip) {
		t.Fatalf("CheckNamedValue = %v", err)
	}
	// Begin falls back to the driver's own Begin and tracks the transaction
	tx, err := cn.Begin()
	if err != nil || !cn.inTx {
		t.Fatalf("Begin = %v, inTx = %v", err, cn.inTx)
	}
	if err := tx.Commit(); err != nil || cn.inTx {
		t.Fatalf("Commit = %v, inTx = %v", err, cn.inTx)
	}
	tx, _ = cn.BeginTx(ctx, driver.TxOptions{})
	if err := tx.Rollback(); err != nil || cn.inTx {
		t.Fatalf("Rollback = %v, inTx = %v", err, cn.inTx)
	}
	mc.beginE = errTransport
	if _, err := cn.BeginTx(ctx, driver.TxOptions{}); !errors.Is(err, errTransport) || cn.inTx {
		t.Fatalf("a failed BeginTx must not mark the connection: %v, inTx = %v", err, cn.inTx)
	}
	// QueryContext goes straight through
	mc.script = []resp{{rows: rowsOf(one(int64(1)))}}
	if r, err := cn.QueryContext(ctx, "SELECT 1", nil); err != nil || r == nil {
		t.Fatalf("QueryContext = %v, %v", r, err)
	}
}

func TestForwardingToADriverConnWithEveryInterface(t *testing.T) {
	ctx := context.Background()
	fc := &fullConn{minConn: &minConn{}, valid: true}
	cn := &conn{Conn: fc, qc: fc.minConn, c: &connector{}}
	if _, err := cn.ExecContext(ctx, "x", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := cn.PrepareContext(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	if cn.Ping(ctx) != nil || cn.ResetSession(ctx) != nil || cn.CheckNamedValue(&driver.NamedValue{}) != nil || !cn.IsValid() {
		t.Fatal("forwarding")
	}
	fc.valid = false
	if cn.IsValid() {
		t.Fatal("IsValid must reflect the driver connection")
	}
	if _, err := cn.BeginTx(ctx, driver.TxOptions{ReadOnly: true}); err != nil || !fc.beginOpts.ReadOnly || !cn.inTx {
		t.Fatalf("BeginTx: %v %+v %v", err, fc.beginOpts, cn.inTx)
	}
	if got := strings.Join(fc.calls, ","); got != "exec:x,prepare:p,ping,reset,check,begintx" {
		t.Fatalf("calls = %s", got)
	}
}

func TestExecGuards(t *testing.T) {
	ctx := context.Background()
	ops := func() []runtime.PipelineOp { return selUpdOps(nil, nil) }

	// a connection that cannot pipeline
	cn := newConn(&minConn{})
	cn.pipelining = false
	if err := cn.ExecPipeline(ctx, ops()); !errors.Is(err, runtime.ErrWireNotApplicable) {
		t.Fatalf("not pipelining: %v", err)
	}
	// a server that failed the probe
	cn = newConn(&minConn{})
	cn.c.state = capNo
	if err := cn.ExecTxPipeline(ctx, nil, ops()); !errors.Is(err, runtime.ErrWireNotApplicable) {
		t.Fatalf("probe says no: %v", err)
	}
	// inside a user transaction ExecTxPipeline refuses before looking at anything
	cn = newConn(&minConn{})
	cn.inTx = true
	if err := cn.ExecTxPipeline(ctx, nil, ops()); !errors.Is(err, runtime.ErrWireNotApplicable) {
		t.Fatalf("in a tx: %v", err)
	}
	// a connection the driver has marked invalid
	fc := &fullConn{minConn: &minConn{}, valid: false}
	cn = &conn{Conn: fc, qc: fc.minConn, c: &connector{state: capYes}, pipelining: true, maxPacket: 1 << 20}
	if err := cn.ExecPipeline(ctx, ops()); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("invalid connection: %v", err)
	}
	// render errors pass through, nothing is sent
	mc := &minConn{}
	cn = newConn(mc)
	bad := ops()
	bad[1].Args = []any{make(chan int), int64(1)}
	if err := cn.ExecPipeline(ctx, bad); !errors.Is(err, runtime.ErrWireNotApplicable) {
		t.Fatalf("render error: %v", err)
	}
	if err := cn.ExecTxPipeline(ctx, &sql.TxOptions{Isolation: sql.LevelLinearizable}, ops()); !errors.Is(err, runtime.ErrWireNotApplicable) {
		t.Fatalf("isolation: %v", err)
	}
	// oversized batch
	cn.maxPacket = 64
	if err := cn.ExecPipeline(ctx, ops()); !errors.Is(err, runtime.ErrWireNotApplicable) || !strings.Contains(err.Error(), "max_allowed_packet") {
		t.Fatalf("oversized: %v", err)
	}
	if len(mc.log()) != 0 {
		t.Fatalf("a refused batch sent %v", mc.log())
	}
}

func okSets() *frows {
	return rowsOf(selSet([]driver.Value{int64(1), int64(10)}, []driver.Value{int64(2), int64(20)}), sumSet(int64(-1), int64(1)))
}

func TestSendDeliversRowsAndCounts(t *testing.T) {
	ctx := context.Background()
	var scanned int64
	mc := &minConn{script: []resp{{rows: okSets()}}}
	cn := newConn(mc)
	ops := selUpdOps(nil, &scanned)
	if err := cn.ExecTxPipeline(ctx, nil, ops); err != nil {
		t.Fatal(err)
	}
	if scanned != 2 || ops[0].RowCount != 2 || ops[1].RowCount != 1 {
		t.Fatalf("scanned %d, counts %d / %d", scanned, ops[0].RowCount, ops[1].RowCount)
	}
	if q := mc.log(); len(q) != 1 || !strings.HasPrefix(q[0], "SET @sqlgen_p=-1;") || !strings.Contains(q[0], "START TRANSACTION") {
		t.Fatalf("queries = %q", q)
	}
}

func TestSendAssertionFailure(t *testing.T) {
	ctx := context.Background()
	failing := func() *frows {
		return rowsOf(selSet([]driver.Value{int64(1), int64(10)}), sumSet(int64(1), int64(0)))
	}
	// own transaction: the batch rolled itself back; the error carries the counts
	mc := &minConn{script: []resp{{rows: failing()}}}
	ops := selUpdOps(nil, nil)
	err := newConn(mc).ExecTxPipeline(ctx, nil, ops)
	var ae *runtime.AssertionError
	if !errors.As(err, &ae) || ae.Statement != "Upd" || ae.Want != 1 || ae.Got != 0 || !ae.GotKnown || ops[1].Err != err {
		t.Fatalf("err = %v (%+v)", err, ae)
	}
	if isCommitError(err) {
		t.Fatal("an assertion failure is not a commit error")
	}
	if len(mc.log()) != 1 {
		t.Fatalf("queries = %q", mc.log())
	}

	// user transaction: the failed operation is undone with ROLLBACK TO SAVEPOINT
	mc = &minConn{script: []resp{{rows: failing()}, {rows: rowsOf(one())}}}
	cn := newConn(mc)
	cn.inTx = true
	err = cn.ExecPipeline(ctx, selUpdOps(nil, nil))
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v", err)
	}
	if q := mc.log(); len(q) != 2 || q[1] != "ROLLBACK TO SAVEPOINT sqlgen_sp1" {
		t.Fatalf("queries = %q", q)
	}
	// the savepoint rollback failing changes nothing for the caller
	mc = &minConn{script: []resp{{rows: failing()}, {err: errTransport}}}
	cn = newConn(mc)
	cn.inTx = true
	if err := cn.ExecPipeline(ctx, selUpdOps(nil, nil)); !errors.As(err, &ae) {
		t.Fatalf("err = %v", err)
	}
}

func TestSendAbort(t *testing.T) {
	ctx := context.Background()
	stop := errors.New("stop")
	abortOp := runtime.PipelineOp{Desc: &runtime.QueryDescriptor{Name: "Abort"}, Decorators: runtime.OpDecorators{AssertRows: -1, IsAbort: true}, AbortErr: stop}
	mc := &minConn{script: []resp{{rows: rowsOf(sumSet(int64(0)))}}}
	ops := []runtime.PipelineOp{abortOp}
	err := newConn(mc).ExecTxPipeline(ctx, nil, ops)
	if err != stop || ops[0].Err != stop {
		t.Fatalf("err = %v", err)
	}
}

func TestSendProtocolAnomalies(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		rows        func() driver.Rows
		wantMessage string
	}{
		{"fewer result sets than expected", func() driver.Rows { return rowsOf(selSet([]driver.Value{int64(1), int64(10)})) }, "fewer result sets"},
		{"the summary has no row", func() driver.Rows { return rowsOf(selSet(), fset{cols: []string{"f", "n"}}) }, "no summary row"},
		{"the summary row fails to read", func() driver.Rows { return rowsOf(selSet(), fset{cols: []string{"f", "n"}, rowErr: errTransport}) }, errTransport.Error()},
		{"the summary result set fails to open", func() driver.Rows {
			return rowsOf(selSet(), fset{cols: []string{"f", "n"}, nextErr: errTransport})
		}, errTransport.Error()},
		{"close fails after the summary", func() driver.Rows {
			r := rowsOf(selSet(), sumSet(int64(-1), int64(1)))
			r.closeErr = errTransport
			return r
		}, errTransport.Error()},
		{"the summary is not made of integers", func() driver.Rows { return rowsOf(selSet(), sumSet(int64(-1), []byte("many"))) }, "want an integer"},
		{"the summary names an operation that does not exist", func() driver.Rows { return rowsOf(selSet(), sumSet(int64(9), int64(1))) }, "names operation 9"},
		{"rows that cannot move to the next result set", func() driver.Rows { return onlyRows{rowsOf(selSet(), sumSet(int64(-1), int64(1)))} }, "fewer result sets"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// own transaction: the outcome of the COMMIT is unknown
			mc := &minConn{script: []resp{{rows: tc.rows()}}}
			err := newConn(mc).ExecTxPipeline(ctx, nil, selUpdOps(nil, nil))
			if err == nil || !strings.Contains(err.Error(), tc.wantMessage) || !isCommitError(err) {
				t.Fatalf("own tx: err = %v (commit error: %v), want %q", err, isCommitError(err), tc.wantMessage)
			}
			// user transaction: a plain error
			mc = &minConn{script: []resp{{rows: tc.rows()}}}
			cn := newConn(mc)
			cn.inTx = true
			err = cn.ExecPipeline(ctx, selUpdOps(nil, nil))
			if err == nil || !strings.Contains(err.Error(), tc.wantMessage) || isCommitError(err) {
				t.Fatalf("user tx: err = %v (commit error: %v), want %q", err, isCommitError(err), tc.wantMessage)
			}
		})
	}
}

func TestSendScanErrorIsNotACommitError(t *testing.T) {
	ctx := context.Background()
	scanErr := errors.New("fake: scan")
	var scanned int64
	mc := &minConn{script: []resp{{rows: okSets()}}}
	err := newConn(mc).ExecTxPipeline(ctx, nil, selUpdOps(scanErr, &scanned))
	if err != scanErr || isCommitError(err) {
		t.Fatalf("err = %v", err)
	}
	if len(mc.log()) != 1 {
		t.Fatalf("a scan error must not trigger a recovery turn: %q", mc.log())
	}
}

// Two selects: the scan of the first fails, the rest of the result sets is still drained and the summary
// is not applied.
func TestScanErrorDrainsTheRemainingResultSets(t *testing.T) {
	ctx := context.Background()
	scanErr := errors.New("fake: scan")
	var second int64
	sel := &runtime.QueryDescriptor{Name: "Sel", StmtVerb: runtime.VerbSelect, Segments: []string{"SELECT id, v FROM t WHERE id = ", ""}, Slots: []runtime.Slot{{Kind: runtime.SlotScalar}}}
	mk := func() []runtime.PipelineOp {
		return []runtime.PipelineOp{
			{Desc: sel, Args: []any{int64(1)}, HasResultCols: true, Decorators: runtime.OpDecorators{AssertRows: -1}, ScanWireRows: scanFn(scanErr, nil)},
			{Desc: sel, Args: []any{int64(2)}, HasResultCols: true, Decorators: runtime.OpDecorators{AssertRows: -1}, ScanWireRows: scanFn(nil, &second)},
		}
	}
	// without assertions there is no summary at all
	r := rowsOf(selSet([]driver.Value{int64(1), int64(1)}), selSet([]driver.Value{int64(2), int64(2)}))
	mc := &minConn{script: []resp{{rows: r}}}
	cn := newConn(mc)
	cn.inTx = true
	if err := cn.ExecPipeline(ctx, mk()); err != scanErr {
		t.Fatalf("err = %v", err)
	}
	if r.cur != 1 || second != 0 || !r.closed {
		t.Fatalf("the second result set was not drained: cur = %d, scanned = %d, closed = %v", r.cur, second, r.closed)
	}
	// without summary: success and a failing Close
	r = rowsOf(selSet([]driver.Value{int64(1), int64(1)}), selSet([]driver.Value{int64(2), int64(2)}))
	ops := mk()
	ops[0].ScanWireRows = scanFn(nil, nil)
	mc = &minConn{script: []resp{{rows: r}}}
	cn = newConn(mc)
	cn.inTx = true
	if err := cn.ExecPipeline(ctx, ops); err != nil || second != 1 || ops[1].RowCount != 1 {
		t.Fatalf("err = %v, scanned = %d", err, second)
	}
	r = rowsOf(selSet(), selSet())
	r.closeErr = errTransport
	mc = &minConn{script: []resp{{rows: r}}}
	cn = newConn(mc)
	cn.inTx = true
	if err := cn.ExecPipeline(ctx, mk()); !errors.Is(err, errTransport) {
		t.Fatalf("err = %v", err)
	}
	// without summary: the next result set is missing
	mc = &minConn{script: []resp{{rows: rowsOf(selSet())}}}
	cn = newConn(mc)
	cn.inTx = true
	if err := cn.ExecPipeline(ctx, mk()); err == nil || !strings.Contains(err.Error(), "fewer result sets") {
		t.Fatalf("err = %v", err)
	}
	// with a summary the scan error of the first set wins over everything after it
	withSummary := selUpdOps(scanErr, nil)
	mc = &minConn{script: []resp{{rows: okSets()}}}
	if err := newConn(mc).ExecTxPipeline(ctx, nil, withSummary); err != scanErr {
		t.Fatalf("err = %v", err)
	}
}

func TestFailRecoversAndAttributes(t *testing.T) {
	ctx := context.Background()
	pRow := func(p driver.Value) resp { return resp{rows: rowsOf(one(p))} }
	for _, tc := range []struct {
		name       string
		recovery   []resp
		wantCommit bool
		wantErrOp  int // -1: none
		queries    int
	}{
		{"error of the first operation", []resp{pRow(int64(0))}, false, 0, 2},
		{"error of the second operation", []resp{pRow(int64(1))}, false, 1, 2},
		{"error of the COMMIT", []resp{pRow(int64(2))}, true, -1, 2},
		{"error of START TRANSACTION", []resp{pRow(int64(-1))}, false, -1, 2},
		{"position as bytes", []resp{pRow([]byte("1"))}, false, 1, 2},
		{"unreadable position", []resp{pRow("x")}, false, -1, 2},
		{"recovery turn refused", []resp{{err: myErr(1105)}}, false, -1, 2},
		{"recovery turn on a dead connection", []resp{{err: errTransport}}, false, -1, 2},
		{"recovery row missing", []resp{{rows: rowsOf(fset{cols: []string{"p"}})}}, false, -1, 2},
		{"recovery close fails", []resp{{rows: &frows{sets: []fset{one(int64(1))}, closeErr: errTransport}}}, false, -1, 2},
		{"DEALLOCATE of an unknown statement is ignored", []resp{{rows: &frows{sets: []fset{one(int64(1))}, closeErr: myErr(1243)}}}, false, 1, 2},
		{"other server error of the recovery", []resp{{rows: &frows{sets: []fset{one(int64(1))}, closeErr: myErr(1105)}}}, false, -1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbErr := myErr(1062)
			mc := &minConn{script: append([]resp{{err: dbErr}}, tc.recovery...)}
			ops := selUpdOps(nil, nil)
			err := newConn(mc).ExecTxPipeline(ctx, nil, ops)
			if !isMySQL(err, 1062) {
				t.Fatalf("err = %v", err)
			}
			if isCommitError(err) != tc.wantCommit {
				t.Fatalf("commit error = %v, want %v", isCommitError(err), tc.wantCommit)
			}
			for i := range ops {
				if (ops[i].Err != nil) != (i == tc.wantErrOp) {
					t.Fatalf("ops[%d].Err = %v, want attribution to op %d", i, ops[i].Err, tc.wantErrOp)
				}
			}
			q := mc.log()
			if len(q) != tc.queries || q[1] != "ROLLBACK;SELECT @sqlgen_p;DEALLOCATE PREPARE sqlgen_s;" {
				t.Fatalf("queries = %q", q[1:])
			}
		})
	}

	// a batch without a guard has no prepared statement to deallocate
	noGuard := func() []runtime.PipelineOp {
		ops := selUpdOps(nil, nil)
		ops[1].Decorators.AssertRows = -1
		return ops
	}
	mc := &minConn{script: []resp{{err: myErr(1062)}, pRow(int64(1))}}
	if err := newConn(mc).ExecTxPipeline(ctx, nil, noGuard()); !isMySQL(err, 1062) {
		t.Fatal(err)
	}
	if q := mc.log(); q[1] != "ROLLBACK;SELECT @sqlgen_p;" {
		t.Fatalf("queries = %q", q)
	}
	// in a transaction of the caller nothing is rolled back; only the position is read
	mc = &minConn{script: []resp{{err: myErr(1062)}, pRow(int64(1))}}
	cn := newConn(mc)
	cn.inTx = true
	ops := noGuard()
	if err := cn.ExecPipeline(ctx, ops); !isMySQL(err, 1062) || isCommitError(err) || ops[1].Err == nil {
		t.Fatalf("err = %v", err)
	}
	if q := mc.log(); q[1] != "SELECT @sqlgen_p" {
		t.Fatalf("queries = %q", q)
	}
	// even a failing COMMIT position is no commit error there (there is no COMMIT)
	mc = &minConn{script: []resp{{err: myErr(1062)}, pRow(int64(2))}}
	cn = newConn(mc)
	cn.inTx = true
	if err := cn.ExecPipeline(ctx, noGuard()); !isMySQL(err, 1062) || isCommitError(err) {
		t.Fatalf("err = %v", err)
	}
}

func TestTransportErrorMarksOnlyOwnTransactionsAsCommitErrors(t *testing.T) {
	ctx := context.Background()
	mc := &minConn{script: []resp{{err: errTransport}}}
	err := newConn(mc).ExecTxPipeline(ctx, nil, selUpdOps(nil, nil))
	if !isCommitError(err) || !errors.Is(err, errTransport) {
		t.Fatalf("own tx: %v", err)
	}
	if len(mc.log()) != 1 {
		t.Fatal("no recovery turn is attempted on a dead connection")
	}
	mc = &minConn{script: []resp{{err: errTransport}}}
	cn := newConn(mc)
	cn.inTx = true
	err = cn.ExecPipeline(ctx, selUpdOps(nil, nil))
	if isCommitError(err) || err != errTransport {
		t.Fatalf("user tx: %v", err)
	}
	// a canceled context is a non-server error as well
	mc = &minConn{script: []resp{{err: context.Canceled}}}
	if err := newConn(mc).ExecPipeline(ctx, selUpdOps(nil, nil)); !isCommitError(err) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

// The counter column that tells how many rows a RETURNING statement produced is hidden from the scan.
func TestSetRowsHidesTheRowCounter(t *testing.T) {
	inner := rowsOf(fset{cols: []string{"id", "v", "k"}, rows: [][]driver.Value{{int64(1), int64(2), int64(1)}, {int64(3), int64(4), int64(2)}}})
	tr := &setRows{inner: inner, trim: 1}
	if got := tr.Columns(); len(got) != 2 {
		t.Fatalf("Columns = %v", got)
	}
	dest := make([]driver.Value, 2)
	for _, want := range [][2]int64{{1, 2}, {3, 4}} {
		if err := tr.Next(dest); err != nil || dest[0] != want[0] || dest[1] != want[1] {
			t.Fatalf("Next = %v, %v", dest, err)
		}
	}
	if err := tr.Next(dest); err != io.EOF {
		t.Fatalf("Next at the end = %v", err)
	}
	if tr.Close() != nil || inner.closed {
		t.Fatal("setRows.Close must not close the underlying rows")
	}
	// no trim: straight through; an empty column list is tolerated with trim
	plain := &setRows{inner: rowsOf(fset{cols: []string{"a"}, rows: [][]driver.Value{{int64(9)}}})}
	d1 := make([]driver.Value, 1)
	if got := plain.Columns(); len(got) != 1 || plain.Next(d1) != nil || d1[0] != int64(9) {
		t.Fatalf("plain: %v %v", plain.Columns(), d1)
	}
	if got := (&setRows{inner: rowsOf(fset{}), trim: 1}).Columns(); len(got) != 0 {
		t.Fatalf("Columns = %v", got)
	}
}

// RETURNING (MariaDB): a guarded INSERT ... RETURNING has its counter column trimmed on the way in.
func TestReturningStatementTrimsTheCounter(t *testing.T) {
	ctx := context.Background()
	ret := &runtime.QueryDescriptor{
		Name: "InsRet", StmtVerb: runtime.VerbInsert,
		Segments: []string{"INSERT INTO t (id, v) ", "", ", ", "", " RETURNING ", ""},
		Slots:    []runtime.Slot{{Kind: runtime.SlotInsertValuesOpen}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotInsertValuesClose}, {Kind: runtime.SlotSelectCols}},

		SelectColExprs: []string{"id", "v"},
	}
	ops := []runtime.PipelineOp{{Desc: ret, Args: []any{int64(7), int64(70)}, HasResultCols: true, IsSingleRow: true,
		Decorators:   runtime.OpDecorators{AssertRows: 1},
		ScanWireRows: scanFn(nil, nil)}}
	mc := &minConn{script: []resp{{rows: rowsOf(
		fset{cols: []string{"id", "v", "k"}, rows: [][]driver.Value{{int64(7), int64(70), int64(1)}}},
		sumSet(int64(-1), int64(1)),
	)}}}
	cn := newConn(mc)
	cn.c.returning = true
	if err := cn.ExecTxPipeline(ctx, nil, ops); err != nil {
		t.Fatal(err)
	}
	if ops[0].RowCount != 1 {
		t.Fatalf("RowCount = %d", ops[0].RowCount)
	}
	// without RETURNING support the batch cannot be expressed
	cn.c.returning = false
	if err := cn.ExecTxPipeline(ctx, nil, ops); !errors.Is(err, runtime.ErrWireNotApplicable) {
		t.Fatalf("err = %v", err)
	}
}

// The forwarding probe (assignment inside an expression, DO LAST_INSERT_ID) is a separate verdict: a
// server that refuses it keeps the base capability; a transport error fails the probe and is not cached.
func TestProbeForwardingVerdicts(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name             string
		fwd              func() (driver.Rows, error)
		wantErr, wantFwd bool
		wantState        capState
	}{
		{"capable", nil, false, true, capYes},
		{"assignment refused", func() (driver.Rows, error) { return nil, myErr(1064) }, false, false, capYes},
		{"wrong value", func() (driver.Rows, error) { return rowsOf(one(int64(1), int64(0))), nil }, false, false, capYes},
		{"stale id", func() (driver.Rows, error) { return rowsOf(one(int64(2), int64(7))), nil }, false, false, capYes},
		{"no row", func() (driver.Rows, error) { return rowsOf(fset{cols: []string{"a", "b"}}), nil }, false, false, capYes},
		{"close fails", func() (driver.Rows, error) {
			r := rowsOf(one(int64(2), int64(0)))
			r.closeErr = myErr(1064)
			return r, nil
		}, false, false, capYes},
		{"transport error", func() (driver.Rows, error) { return nil, errTransport }, true, false, capUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, cn, mc := probeConn(func() (driver.Rows, error) { return goodProbe(), nil })
			mc.fwd = tc.fwd
			err := c.probe(ctx, cn, "8.4.0")
			if (err != nil) != tc.wantErr {
				t.Fatalf("probe err = %v, want error: %v", err, tc.wantErr)
			}
			ok, _, _, fwd := c.capable()
			if c.state != tc.wantState || ok != (tc.wantState == capYes) || fwd != tc.wantFwd {
				t.Fatalf("state = %v capable = %v forward = %v", c.state, ok, fwd)
			}
		})
	}
}
