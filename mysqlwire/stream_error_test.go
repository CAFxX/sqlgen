package mysqlwire

import (
	"context"
	"strings"
	"testing"
)

// MariaDB sends the column definitions of an INSERT ... SELECT ... RETURNING before it inserts, so a
// duplicate key arrives while the rows of the result set are read. That is a database error that ended
// the batch (it must be recovered like any statement error: rollback, which operation), not a scan
// error followed by a hunt for result sets that never come.
func TestServerErrorWhileStreamingRowsIsABatchError(t *testing.T) {
	mc := &minConn{script: []resp{
		{rows: rowsOf(fset{cols: []string{"id", "v"}, rowErr: myErr(1062)})},
		{rows: rowsOf(one(int64(0)))}, // the recovery turn: @sqlgen_p
	}}
	ops := selUpdOps(nil, nil)
	err := newConn(mc).ExecTxPipeline(context.Background(), nil, ops)
	if !isMySQL(err, 1062) || strings.Contains(err.Error(), "result sets") {
		t.Fatalf("err = %v", err)
	}
	if q := mc.log(); len(q) != 2 || !strings.HasPrefix(q[1], "ROLLBACK;SELECT @sqlgen_p") {
		t.Fatalf("queries = %q", q)
	}
	if !isMySQL(ops[0].Err, 1062) {
		t.Fatalf("the failed operation's error = %v", ops[0].Err)
	}
}
