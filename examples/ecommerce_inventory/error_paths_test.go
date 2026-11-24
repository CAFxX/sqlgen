package ecommerce_inventory

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
	keys := []SKUKey{{WarehouseID: 2, SKU: "b"}, {WarehouseID: 1, SKU: "z"}, {WarehouseID: 1, SKU: "a"}}
	items := []NewOrderItem{{OrderID: 2, SKU: "b"}, {OrderID: 1, SKU: "z"}, {OrderID: 1, SKU: "a"}}
	archived := []ArchivedOrderRow{{ID: 2}, {ID: 1}, {ID: 3}}
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
		for _, n := range []int{1, 3} { // 1 row: one statement; 3 rows: several chunks, so an implicit transaction
			_, err := q.GetInventoryBySKUs(ctx, keys[:n])
			check("GetInventoryBySKUs", err)
			_, err = q.InsertOrderItems(ctx, items[:n])
			check("InsertOrderItems", err)
			_, err = q.InsertArchivedOrders(ctx, archived[:n])
			check("InsertArchivedOrders", err)
			_, err = q.DeleteFulfilledOrdersByIDs(ctx, []int64{3, 1, 2}[:n])
			check("DeleteFulfilledOrdersByIDs", err)
		}
		for _, err := range q.GetInventoryBySKUsSeq(ctx, keys) {
			check("GetInventoryBySKUsSeq", err)
		}
		_, err := q.StreamFulfilledOrders(ctx, 1)
		check("StreamFulfilledOrders", err)
		for _, err := range q.StreamFulfilledOrdersSeq(ctx, 1) {
			check("StreamFulfilledOrdersSeq", err)
		}
	}
}
