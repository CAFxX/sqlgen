package fintech_ledger

import (
	"context"
	"database/sql"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// Database failures are reported by every call shape of a collection statement: a closed database fails
// before the statement (or the implicit transaction of a multi-chunk call) starts; a database without the
// schema fails the statement after that transaction began, which is rolled back.
func TestDatabaseErrorsAreReportedByEveryCallShape(t *testing.T) {
	ctx := context.Background()
	settlements := []AccountSettlement{{OrgID: 2, AccountID: 1}, {OrgID: 1, AccountID: 2}, {OrgID: 1, AccountID: 1}}
	for _, closed := range []bool{true, false} {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		if closed {
			_ = db.Close()
		} else {
			t.Cleanup(func() { _ = db.Close() })
		}
		q := New(db, runtime.WithMaxBatchParams(1)) // one row per statement: 3 rows are three chunks
		t.Cleanup(func() { _ = q.Close() })
		for _, n := range []int{1, 3} {
			if _, err := q.UpsertAccountBalances(ctx, settlements[:n]); err == nil {
				t.Errorf("closed=%v: UpsertAccountBalances(%d rows) succeeded, want an error", closed, n)
			}
		}
	}
}
