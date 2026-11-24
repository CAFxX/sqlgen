// Package wire is the contract a database driver implements to run a sqlgen pipeline in a single
// network round trip. Applications never import it: generated code and the user-facing API live in
// package runtime; this package is for the authors of wire drivers (pgxwire, mysqlwire and your own).
//
// A driver connection (the driver.Conn behind a driver.Connector wrapped with runtime.WrapConnector)
// implements Pipeliner, and optionally TxPipeliner and Flavored. The runtime discovers them by
// structural type assertion, so the interfaces below are the documentation of the contract rather than
// a dependency of the runtime: a connection that has the methods is used, whichever package declared
// the interface. The data a driver receives and fills in (runtime.PipelineOp, runtime.ProducedCond,
// runtime.WireResultKind, runtime.WireFlavor, runtime.CommitError, runtime.ErrWireNotApplicable,
// runtime.TranslateWireError, runtime.RenderMySQLBatch, ...) is owned by the pipeline core and stays in
// package runtime, because the core mutates it and cannot import this package.
package wire

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/CAFxX/sqlgen/runtime"
)

// Pipeliner is implemented by driver connections capable of executing a batch of
// runtime.PipelineOp statements in a single network round-trip.
//
// ExecPipeline runs the operations in the transaction the connection is in (a user-managed
// *sql.Tx; the caller commits or rolls back) or, on an idle connection, atomically in a transaction of
// its own. In a user-managed transaction a failed assertion or Abort (the errors runtime.TranslateWireError
// maps) must leave the transaction usable, with the work of the operations before the failing one
// kept, as the sequential path does; a database error of the caller's own statement need not. The
// conditions recorded by set_config(..., true) need an explicit transaction block on some engines
// (CockroachDB), so an implementation should open one itself rather than rely on the implicit
// transaction of a Sync. It returns runtime.ErrWireNotApplicable, before sending anything, when it
// cannot run this batch: the runtime then falls back to the sequential path. Connections that can also
// fold BEGIN and COMMIT into the same round-trip should implement TxPipeliner too.
type Pipeliner interface {
	ExecPipeline(ctx context.Context, ops []runtime.PipelineOp) error
}

// TxPipeliner is implemented by driver connections that can run a complete transaction -- BEGIN
// (with the isolation level and read-only mode of txOpts), every queued operation, COMMIT -- in a single
// network round-trip. The runtime prefers it over Pipeliner whenever it owns the transaction
// (PipelineTx, and a standalone Pipeline.Flush), because with a Pipeliner alone BEGIN and COMMIT are
// two more round-trips.
//
// ExecTxPipeline must deliver op results exactly as Pipeliner.ExecPipeline does. When it returns an
// error the transaction must no longer be open on the connection (the implementation rolls it back, which
// may cost one extra round-trip on this error path only) and the connection must be reusable or report
// itself invalid so the pool discards it. An error raised by the COMMIT itself (for example a
// serialization failure) must be wrapped in a *runtime.CommitError so that retry classification can tell
// that the failure happened while committing.
type TxPipeliner interface {
	ExecTxPipeline(ctx context.Context, txOpts *sql.TxOptions, ops []runtime.PipelineOp) error
}

// Flavored is optionally implemented by a Pipeliner / TxPipeliner to tell the runtime which rendering
// it needs. A connection without it is runtime.WireFlavorPostgres.
type Flavored interface {
	WireFlavor() runtime.WireFlavor
}

// RowsFromDriverRows wraps a low-level driver.Rows in a standard *sql.Rows, so a Pipeliner can hand a
// result set to a future's scanner. The *sql.Rows must be closed by the caller.
func RowsFromDriverRows(ctx context.Context, dRows driver.Rows) (*sql.Rows, error) {
	return rowsDB.QueryContext(context.WithValue(ctx, rowsCtxKey{}, dRows), "wire")
}

type rowsCtxKey struct{}

type rowsConnector struct{}

func (rowsConnector) Connect(context.Context) (driver.Conn, error) { return rowsConn{}, nil }
func (rowsConnector) Driver() driver.Driver                        { return nil }

type rowsConn struct{}

func (rowsConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (rowsConn) Close() error                        { return nil }
func (rowsConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (rowsConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	r, ok := ctx.Value(rowsCtxKey{}).(driver.Rows)
	if !ok || r == nil {
		return nil, driver.ErrSkip
	}
	return r, nil
}

var rowsDB = sql.OpenDB(rowsConnector{})
