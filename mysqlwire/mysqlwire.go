// Package mysqlwire gives sqlgen's pipelines real single-round-trip execution on MySQL-protocol servers
// (MySQL, MariaDB, ...) through the go-sql-driver/mysql driver.
//
// It returns an ordinary *sql.DB. Everything that is not a sqlgen pipeline behaves exactly as with
// go-sql-driver/mysql; a sqlgen Pipeline, PipelineTx or Pipeline.Flush additionally sends all of its
// statements, and for PipelineTx and standalone flushes also START TRANSACTION and COMMIT, to the server in
// ONE network round-trip (one multi-statement text). See the sqlgen DOCS.md, section 3.4, for when that
// path is taken, how to verify it and how it differs from the PostgreSQL one.
//
//	db, err := mysqlwire.OpenDB("user:pass@tcp(host)/db")
//	q := mydb.New(db)
//	err = q.PipelineTx(ctx, nil, func(p *mydb.Pipeline) error { ...; return nil })
//
// # What the connector changes
//
//   - It forces multiStatements=true: that is how several statements travel in one packet. Every
//     statement you run on this pool can then contain several statements, which widens the damage of a
//     SQL injection in a hand-written query; use a plain go-sql-driver pool for such code.
//   - The arguments of a pipeline are sent INLINED as literals (the protocol has no way to bind
//     parameters of a multi-statement text), rendered by the sqlgen runtime according to rules that were
//     verified against the prepared-statement path; see runtime.MySQLFlavor.WriteLiteral. An argument that
//     cannot be inlined (an unsupported Go type, NaN) makes the pipeline run on the sequential path.
//   - Per connection it reads max_allowed_packet and the character set once, and per connector it probes
//     once that the server runs the statement sequences the pipelines are made of (a proxy in front of
//     the server may not). A batch larger than max_allowed_packet, or a failed probe, makes the pipeline
//     run on the sequential path (CacheStats.SequentialPipelines counts it); nothing is lost but the
//     round-trip saving.
//
// Connections must be in autocommit mode; transactions are tracked through database/sql's BeginTx,
// Commit and Rollback (a hand-written START TRANSACTION through Exec is not seen).
package mysqlwire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/runtime"
)

// OpenDB parses dsn (as accepted by go-sql-driver/mysql) and returns a *sql.DB whose connections run
// sqlgen pipelines in a single round-trip.
func OpenDB(dsn string) (*sql.DB, error) {
	c, err := NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(c), nil
}

// NewConnector is Connector for a DSN.
func NewConnector(dsn string) (driver.Connector, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return Connector(cfg)
}

// Connector returns a driver.Connector for sql.OpenDB. cfg is copied, with MultiStatements forced on;
// everything else (Loc, Collation, ClientFoundRows, ...) is kept. The connections implement
// runtime.WirePipeliner and runtime.WireTxPipeliner and are already wrapped with runtime.WrapConnector
// so that user-managed transactions (q.WithTx(tx).Pipeline(ctx)) also flush in one round-trip.
func Connector(cfg *mysql.Config) (driver.Connector, error) {
	c := cfg.Clone()
	c.MultiStatements = true
	base, err := mysql.NewConnector(c)
	if err != nil {
		return nil, err
	}
	loc := c.Loc
	if loc == nil {
		loc = time.UTC
	}
	return runtime.WrapConnector(&connector{base: base, loc: loc, maxPacket: c.MaxAllowedPacket}), nil
}

type capState uint8

const (
	capUnknown capState = iota
	capYes
	capNo
)

type connector struct {
	base      driver.Connector
	loc       *time.Location
	maxPacket int // the driver's own limit (maxAllowedPacket); <= 0: none

	mu        sync.Mutex
	state     capState
	returning bool // the server has INSERT/DELETE ... RETURNING (MariaDB 10.5+)
}

func (c *connector) Driver() driver.Driver { return c.base.Driver() }

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	dc, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	qc, ok := dc.(driver.QueryerContext)
	if !ok {
		_ = dc.Close()
		return nil, fmt.Errorf("mysqlwire: unexpected connection type %T", dc)
	}
	cn := &conn{Conn: dc, qc: qc, c: c}
	if err := cn.init(ctx); err != nil {
		_ = dc.Close()
		return nil, err
	}
	return cn, nil
}

// probe checks, once per connector, that the server runs the statement sequences pipelines consist of
// (user variables, ROW_COUNT, FOUND_ROWS, a server-side prepared statement, several statements in one
// text) and learns whether it has RETURNING. A server that rejects it (a proxy in front of MySQL, an old
// version) is remembered as not capable; a transport error is not remembered and fails the Connect.
func (c *connector) probe(ctx context.Context, cn *conn, version string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != capUnknown {
		return nil
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	const q = "SET @sqlgen_probe=1;SET @sqlgen_probe_q='DO 0';PREPARE sqlgen_s FROM @sqlgen_probe_q;EXECUTE sqlgen_s;DEALLOCATE PREPARE sqlgen_s;" +
		"SELECT 1;SELECT ROW_COUNT(),FOUND_ROWS(),@sqlgen_probe;"
	rows, err := cn.qc.QueryContext(pctx, q, nil)
	ok := err == nil
	if ok {
		// first result set: SELECT 1; second: the three values.
		dest := make([]driver.Value, 1)
		ok = rows.Next(dest) == nil
		if rs, is := rows.(driver.RowsNextResultSet); ok && is && rs.HasNextResultSet() && rs.NextResultSet() == nil {
			dest3 := make([]driver.Value, 3)
			ok = rows.Next(dest3) == nil
			if v, vok := summaryInt(dest3[2]); !vok || v != 1 {
				ok = false
			}
		} else {
			ok = false
		}
		if cerr := rows.Close(); cerr != nil {
			ok, err = false, cerr
		}
	}
	if !ok {
		var me *mysql.MySQLError
		if err != nil && !errors.As(err, &me) {
			return err // transport problem: not a verdict about the server
		}
		c.state = capNo
		return nil
	}
	c.state = capYes
	c.returning = hasReturning(version)
	return nil
}

func (c *connector) capable() (ok, returning bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == capYes, c.returning
}

// hasReturning reports whether the server version string names a MariaDB that has INSERT/DELETE ...
// RETURNING (10.5 and later). MySQL has none.
func hasReturning(version string) bool {
	if !strings.Contains(strings.ToLower(version), "mariadb") {
		return false
	}
	major, minor := 0, 0
	rest := version
	// MariaDB reports e.g. "10.11.19-MariaDB-ubu2204" or, behind old replication proxies, "5.5.5-10.11.19-MariaDB".
	if strings.HasPrefix(rest, "5.5.5-") {
		rest = rest[len("5.5.5-"):]
	}
	parts := strings.SplitN(rest, ".", 3)
	if len(parts) >= 2 {
		major, _ = strconv.Atoi(parts[0])
		minor, _ = strconv.Atoi(parts[1])
	}
	return major > 10 || (major == 10 && minor >= 5)
}

// conn is a go-sql-driver connection that can also run pipelines. Prepare, query, exec, ping, reset and
// validity are forwarded to the driver connection; BeginTx is wrapped to track the open transaction.
type conn struct {
	driver.Conn
	qc driver.QueryerContext
	c  *connector

	inTx       bool // a transaction began through BeginTx is open (a conn is used by one goroutine at a time)
	maxPacket  int  // statement size limit: the smaller of the server's and the driver's
	charset    string
	version    string
	pipelining bool // this connection can run pipelines (see init)
}

var (
	_ runtime.WirePipeliner   = (*conn)(nil)
	_ runtime.WireTxPipeliner = (*conn)(nil)
	_ runtime.WireFlavored    = (*conn)(nil)
)

// WireFlavor tells the runtime that this connection renders its own statement sequences.
func (c *conn) WireFlavor() runtime.WireFlavor { return runtime.WireFlavorMySQL }

// init reads the session facts the renderer and the size check need, in one round-trip when the
// connection is made, so that the first pipeline on a connection already costs one round-trip only.
func (c *conn) init(ctx context.Context) error {
	ictx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	rows, err := c.qc.QueryContext(ictx, "SELECT @@max_allowed_packet,@@character_set_client,VERSION()", nil)
	if err != nil {
		return c.initError(err)
	}
	dest := make([]driver.Value, 3)
	nerr := rows.Next(dest)
	cerr := rows.Close()
	if nerr != nil || cerr != nil {
		return c.initError(errors.Join(nerr, cerr))
	}
	max, ok := summaryInt(dest[0])
	if !ok || max <= 0 {
		return nil // unknown: this connection stays on the sequential path
	}
	c.maxPacket = int(max)
	if lim := c.c.maxPacket; lim > 0 && lim < c.maxPacket {
		c.maxPacket = lim
	}
	c.charset = asString(dest[1])
	c.version = asString(dest[2])
	if err := c.c.probe(ctx, c, c.version); err != nil {
		return err
	}
	c.pipelining = true
	return nil
}

// initError decides whether a failed init makes the connection unusable (a transport error) or just
// keeps it on the sequential path (the server refused the query).
func (c *conn) initError(err error) error {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return nil
	}
	return err
}

func asString(v driver.Value) string {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case string:
		return x
	}
	return ""
}

func summaryInt(v driver.Value) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case uint64:
		return int64(x), true
	case float64:
		return int64(x), true
	case []byte:
		n, err := strconv.ParseInt(string(x), 10, 64)
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	}
	return 0, false
}

// --- plain driver.Conn behaviour, forwarded -------------------------------------------------------

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var tx driver.Tx
	var err error
	if bt, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = bt.BeginTx(ctx, opts)
	} else {
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}
	c.inTx = true
	return &trackedTx{Tx: tx, c: c}, nil
}

func (c *conn) Begin() (driver.Tx, error) { return c.BeginTx(context.Background(), driver.TxOptions{}) }

type trackedTx struct {
	driver.Tx
	c *conn
}

func (t *trackedTx) Commit() error   { t.c.inTx = false; return t.Tx.Commit() }
func (t *trackedTx) Rollback() error { t.c.inTx = false; return t.Tx.Rollback() }

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ex, ok := c.Conn.(driver.ExecerContext); ok {
		return ex.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.qc.QueryContext(ctx, query, args)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if p, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, query)
	}
	return c.Conn.Prepare(query)
}

func (c *conn) Ping(ctx context.Context) error {
	if p, ok := c.Conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (c *conn) ResetSession(ctx context.Context) error {
	if r, ok := c.Conn.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}

func (c *conn) IsValid() bool {
	if v, ok := c.Conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if v, ok := c.Conn.(driver.NamedValueChecker); ok {
		return v.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

// --- pipelines ------------------------------------------------------------------------------------

// ExecPipeline sends ops in one round-trip. Inside a transaction the caller began (a user-managed
// *sql.Tx) the ops simply run there and the caller owns commit and rollback; a failed assertion or Abort
// leaves that transaction usable (each operation with an assertion runs after a SAVEPOINT, and the
// failure rolls back to it, one extra round-trip on that error path only), a database error of one of the
// operations leaves it open exactly like the sequential path. On an idle connection the batch is wrapped
// in START TRANSACTION ... COMMIT, like ExecTxPipeline with default options.
func (c *conn) ExecPipeline(ctx context.Context, ops []runtime.PipelineOp) error {
	return c.exec(ctx, nil, ops)
}

// ExecTxPipeline sends START TRANSACTION (with the isolation level and read-only mode of txOpts), ops and
// COMMIT in one round-trip.
func (c *conn) ExecTxPipeline(ctx context.Context, txOpts *sql.TxOptions, ops []runtime.PipelineOp) error {
	if c.inTx {
		return fmt.Errorf("mysqlwire: connection already in a transaction: %w", runtime.ErrWireNotApplicable)
	}
	if txOpts == nil {
		txOpts = &sql.TxOptions{}
	}
	return c.exec(ctx, txOpts, ops)
}

func (c *conn) exec(ctx context.Context, txOpts *sql.TxOptions, ops []runtime.PipelineOp) error {
	ok, returning := c.c.capable()
	if !c.pipelining || !ok {
		return fmt.Errorf("mysqlwire: the server does not run the pipeline statement sequences: %w", runtime.ErrWireNotApplicable)
	}
	if mc, isMy := c.Conn.(interface{ IsValid() bool }); isMy && !mc.IsValid() {
		return driver.ErrBadConn
	}
	flavor := runtime.MySQLFlavor{Loc: c.c.loc, Charset: c.charset, Returning: returning}
	batch, err := runtime.RenderMySQLBatch(ops, runtime.MySQLRenderOptions{Flavor: &flavor, InTx: c.inTx, TxOpts: txOpts})
	if err != nil {
		return err
	}
	if 1+len(batch.SQL) > c.maxPacket {
		return fmt.Errorf("mysqlwire: batch of %d bytes exceeds max_allowed_packet (%d): %w", len(batch.SQL), c.maxPacket, runtime.ErrWireNotApplicable)
	}
	return c.send(ctx, batch, ops)
}

// send runs the batch and delivers the answers.
func (c *conn) send(ctx context.Context, b *runtime.MySQLBatch, ops []runtime.PipelineOp) error {
	rows, err := c.qc.QueryContext(ctx, b.SQL, nil)
	if err != nil {
		return c.fail(ctx, b, ops, err)
	}
	failed, err := c.deliver(ctx, rows, b, ops)
	if err != nil {
		var se *scanError
		if errors.As(err, &se) {
			// The batch itself succeeded (the transaction is over); the rows did not scan.
			return se.err
		}
		return c.fail(ctx, b, ops, err)
	}
	if failed >= 0 {
		// A guard failed: the batch rolled itself back (own transaction) or gated the rest off (user
		// transaction, where the failed operation is undone here).
		gerr := ops[failed].Err
		if sp, ok := b.Savepoint(failed); ok {
			c.rollbackToSavepoint(ctx, sp)
		}
		return gerr
	}
	return nil
}

// scanError marks an error of the scan of an operation's rows, as opposed to one of the batch.
type scanError struct{ err error }

func (e *scanError) Error() string { return e.err.Error() }
func (e *scanError) Unwrap() error { return e.err }

// deliver reads every result set of the batch (the rows of the operations that return rows, then the
// summary row) and closes rows. A returned error is a driver error (the batch itself failed, or the
// connection did); scan errors of an operation are returned too, after the rest has been drained.
func (c *conn) deliver(ctx context.Context, rows driver.Rows, b *runtime.MySQLBatch, ops []runtime.PipelineOp) (failed int, err error) {
	failed = -1
	next := func() error {
		rs, ok := rows.(driver.RowsNextResultSet)
		if !ok || !rs.HasNextResultSet() {
			return fmt.Errorf("mysqlwire: the server answered fewer result sets than the batch has")
		}
		return rs.NextResultSet()
	}
	sets := b.ResultSetOps()
	var firstErr error
	for k, i := range sets {
		if k > 0 {
			if err := next(); err != nil {
				_ = rows.Close()
				return -1, err
			}
		}
		if firstErr != nil {
			continue // keep draining
		}
		op := &ops[i]
		view := &setRows{inner: rows, trim: b.TrimsLastColumn(i)}
		sqlRows, err := runtime.RowsFromDriverRows(ctx, view)
		if err != nil {
			firstErr = &scanError{err}
			continue
		}
		n, serr := op.ScanWireRows(sqlRows)
		_ = sqlRows.Close()
		var me *mysql.MySQLError
		if errors.As(serr, &me) {
			// The statement failed while its rows were streamed (MariaDB sends the column definitions of an
			// INSERT ... SELECT ... RETURNING before it inserts, so a duplicate key arrives as a row-read
			// error): that is a database error that ended the batch, not a scan error. Nothing follows it.
			_ = rows.Close()
			return -1, serr
		}
		if serr != nil {
			firstErr = &scanError{serr}
			continue
		}
		op.RowCount = n
	}
	if b.HasSummary() {
		if len(sets) > 0 {
			if err := next(); err != nil {
				_ = rows.Close()
				return -1, err
			}
		}
		dest := make([]driver.Value, b.SummaryLen())
		if err := rows.Next(dest); err != nil {
			_ = rows.Close()
			if err == io.EOF {
				err = fmt.Errorf("mysqlwire: the batch produced no summary row")
			}
			return -1, err
		}
		if cerr := rows.Close(); cerr != nil {
			return -1, cerr
		}
		if firstErr != nil {
			return -1, firstErr
		}
		row := make([]any, len(dest))
		for i, v := range dest {
			row[i] = v
		}
		f, aerr := b.Apply(ops, row)
		if aerr != nil && f < 0 {
			return -1, aerr
		}
		return f, nil
	}
	if cerr := rows.Close(); cerr != nil {
		return -1, cerr
	}
	return -1, firstErr
}

// fail leaves the connection clean after a failed batch and translates err for the runtime.
func (c *conn) fail(ctx context.Context, b *runtime.MySQLBatch, ops []runtime.PipelineOp, err error) error {
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		// Transport, protocol, scan or context error. In a transaction of our own the COMMIT may already
		// have been processed, so the outcome is unknown: report it as a commit-time failure (never
		// retried). Scan errors are not database errors, but the batch is complete by then.
		if b.OwnsTx() {
			return &runtime.CommitError{Err: err}
		}
		return err
	}
	// A statement failed (the server stopped the batch there). Roll back a transaction of our own, and
	// ask which operation it was: the only extra round-trip, and only on this error path.
	p, known := c.recover(ctx, b)
	switch {
	case !known:
		return err
	case b.OwnsTx() && p == b.CommitPos():
		return &runtime.CommitError{Err: err}
	case p >= 0 && p < len(ops):
		ops[p].Err = err
	}
	return err
}

// recover rolls back the transaction of an own-transaction batch and reads which operation was running
// (@sqlgen_p). known is false when that could not be determined (the connection is in trouble: it will
// be discarded by the pool).
func (c *conn) recover(ctx context.Context, b *runtime.MySQLBatch) (p int, known bool) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	q := "SELECT @sqlgen_p"
	if b.OwnsTx() {
		q = "ROLLBACK;SELECT @sqlgen_p;"
		if b.UsesPrepare() {
			// A failed COMMIT leaves the prepared statement behind; any other failure does not have one,
			// and the DEALLOCATE then fails with error 1243, which is ignored below.
			q += "DEALLOCATE PREPARE sqlgen_s;"
		}
	}
	rows, err := c.qc.QueryContext(rctx, q, nil)
	if err != nil {
		return 0, false
	}
	dest := make([]driver.Value, 1)
	nerr := rows.Next(dest)
	cerr := rows.Close()
	var me *mysql.MySQLError
	if cerr != nil && !(errors.As(cerr, &me) && me.Number == 1243) {
		return 0, false
	}
	if nerr != nil {
		return 0, false
	}
	n, ok := summaryInt(dest[0])
	return int(n), ok
}

// rollbackToSavepoint undoes the operation that failed its assertion in a user-managed transaction
// (the work of the operations before it stays). When this fails the transaction stays as it is and the
// caller's own rollback handles it; the guard's error is returned either way.
func (c *conn) rollbackToSavepoint(ctx context.Context, savepoint string) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	rows, err := c.qc.QueryContext(rctx, "ROLLBACK TO SAVEPOINT "+savepoint, nil)
	if err == nil {
		_ = rows.Close()
	}
}

// setRows exposes the current result set of a multi-result driver.Rows as a driver.Rows of its own, so
// that it can be scanned like the result of a single query. Close does not close the underlying rows.
// With trim, the last column (the row counter appended to a RETURNING statement) is hidden.
type setRows struct {
	inner driver.Rows
	trim  bool
	buf   []driver.Value
}

func (r *setRows) Columns() []string {
	cols := r.inner.Columns()
	if r.trim && len(cols) > 0 {
		return cols[:len(cols)-1]
	}
	return cols
}

func (r *setRows) Close() error { return nil }

func (r *setRows) Next(dest []driver.Value) error {
	if !r.trim {
		return r.inner.Next(dest)
	}
	if cap(r.buf) < len(dest)+1 {
		r.buf = make([]driver.Value, len(dest)+1)
	}
	buf := r.buf[:len(dest)+1]
	err := r.inner.Next(buf)
	copy(dest, buf)
	return err
}
