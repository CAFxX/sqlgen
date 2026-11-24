package runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// rowsFromDriverRows is a test-only twin of wire.RowsFromDriverRows (this package cannot import runtime/wire).
func rowsFromDriverRows(ctx context.Context, dRows driver.Rows) (*sql.Rows, error) {
	return rowsHelperDB.QueryContext(context.WithValue(ctx, rowsHelperKey{}, dRows), "wire")
}

type rowsHelperKey struct{}

type rowsHelperConnector struct{}

func (rowsHelperConnector) Connect(context.Context) (driver.Conn, error) {
	return rowsHelperConn{}, nil
}
func (rowsHelperConnector) Driver() driver.Driver { return nil }

type rowsHelperConn struct{}

func (rowsHelperConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (rowsHelperConn) Close() error                        { return nil }
func (rowsHelperConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (rowsHelperConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	r, _ := ctx.Value(rowsHelperKey{}).(driver.Rows)
	return r, nil
}

var rowsHelperDB = sql.OpenDB(rowsHelperConnector{})
