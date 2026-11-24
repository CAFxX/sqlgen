package wire_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
	"github.com/CAFxX/sqlgen/runtime/wire"
)

type oneRow struct{ done bool }

func (r *oneRow) Columns() []string { return []string{"id"} }
func (r *oneRow) Close() error      { return nil }
func (r *oneRow) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(42)
	return nil
}

func TestRowsFromDriverRows(t *testing.T) {
	rows, err := wire.RowsFromDriverRows(context.Background(), &oneRow{})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var id int64
	if !rows.Next() {
		t.Fatalf("no row: %v", rows.Err())
	}
	if err := rows.Scan(&id); err != nil || id != 42 {
		t.Fatalf("Scan = %d, %v", id, err)
	}
	if rows.Next() {
		t.Fatal("more than one row")
	}
}

// The driver-side interfaces are structural: a connection implementing them is what runtime.WrapConnector
// and the pipeline core look for.
type fakeConn struct{}

func (fakeConn) ExecPipeline(context.Context, []runtime.PipelineOp) error { return nil }
func (fakeConn) ExecTxPipeline(context.Context, *sql.TxOptions, []runtime.PipelineOp) error {
	return nil
}
func (fakeConn) WireFlavor() runtime.WireFlavor { return runtime.WireFlavorMySQL }

var (
	_ wire.Pipeliner   = fakeConn{}
	_ wire.TxPipeliner = fakeConn{}
	_ wire.Flavored    = fakeConn{}
)
