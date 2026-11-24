package clearing_exchange

import (
	"context"
	"database/sql"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// Every way a generated call can fail on the database side reports the error and leaves nothing behind:
// a closed database fails before a statement (or the implicit transaction of a multi-chunk call) starts, a
// database without the schema fails the statement after the implicit transaction began, and that
// transaction is rolled back.
func TestDatabaseErrorsAreReportedByEveryCallShape(t *testing.T) {
	ctx := context.Background()
	open := func(closed bool) *Queries {
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
		return q
	}
	settlements := []MarginSettlement{{FirmID: 2, AccountID: 2}, {FirmID: 1, AccountID: 1}, {FirmID: 1, AccountID: 2}}
	orders := []OrderSubmission{{FirmID: 2, AccountID: 1, ClientOrderID: "c"}, {FirmID: 1, AccountID: 1, ClientOrderID: "a"}, {FirmID: 1, AccountID: 2, ClientOrderID: "b"}}

	for _, tc := range []struct {
		name string
		q    *Queries
	}{{"closed database", open(true)}, {"no schema", open(false)}} {
		q := tc.q
		check := func(what string, err error) {
			t.Helper()
			if err == nil {
				t.Errorf("%s: %s succeeded, want an error", tc.name, what)
			}
		}
		// 1 row: a single statement; 3 rows: more than one chunk, so an implicit transaction.
		for _, n := range []int{1, 3} {
			_, err := q.ApplyMarginSettlements(ctx, settlements[:n])
			check("ApplyMarginSettlements", err)
			_, err = q.IngestOrders(ctx, orders[:n], 1)
			check("IngestOrders", err)
			_, err = q.CancelOrdersByIDs(ctx, 1, 1, []int64{3, 1, 2}[:n])
			check("CancelOrdersByIDs", err)
		}
		for _, err := range q.IngestOrdersSeq(ctx, orders, 1) {
			check("IngestOrdersSeq", err)
		}
		for _, err := range q.CancelOrdersByIDsSeq(ctx, 1, 1, []int64{3, 1, 2}) {
			check("CancelOrdersByIDsSeq", err)
		}
		_, err := q.ListOpenOrdersForSymbol(ctx, "X", "buy", 5)
		check("ListOpenOrdersForSymbol", err)
		for _, err := range q.ListOpenOrdersForSymbolSeq(ctx, "X", "buy", 5) {
			check("ListOpenOrdersForSymbolSeq", err)
		}
		_, err = q.QueryRiskPositionsPage(ctx, QueryRiskPositionsPageParams{})
		check("QueryRiskPositionsPage", err)
		for _, err := range q.QueryRiskPositionsPageSeq(ctx, QueryRiskPositionsPageParams{}) {
			check("QueryRiskPositionsPageSeq", err)
		}
		_, err = q.GetMarginAccount(ctx, 1, 1)
		check("GetMarginAccount", err)
		_, err = q.GetAccountCollateralCents(ctx, 1, 1)
		check("GetAccountCollateralCents", err)
		_, err = q.TotalExchangeEquityCents(ctx)
		check("TotalExchangeEquityCents", err)
		_, err = q.TotalNetPositionBySymbol(ctx, "X")
		check("TotalNetPositionBySymbol", err)
		_, err = q.CountClearingFills(ctx)
		check("CountClearingFills", err)
		_, err = q.CountComplianceNotices(ctx, "n")
		check("CountComplianceNotices", err)
	}
}
