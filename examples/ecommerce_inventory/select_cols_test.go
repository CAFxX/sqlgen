package ecommerce_inventory

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

// Statements are positional here (no flag blocks), and still project columns: the column constants are the
// trailing variadic argument, on the plain, the Seq and the tuple-slice forms.
func TestPositionalStatementsProjectColumns(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, SchemaDDL); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{
		`INSERT INTO inventory VALUES (1, 'A', 5, 100), (1, 'B', 7, 200)`,
		`INSERT INTO orders VALUES (1, 10, 999, 'fulfilled', 50), (2, 11, 888, 'fulfilled', 60)`,
	} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	q := New(db)

	keys := []SKUKey{{WarehouseID: 1, SKU: "A"}, {WarehouseID: 1, SKU: "B"}}
	rows, err := q.GetInventoryBySKUs(ctx, keys, GetInventoryBySKUsColSKU, GetInventoryBySKUsColQuantity)
	if err != nil || len(rows) != 2 || rows[0] != (InventoryItem{SKU: "A", Quantity: 5}) || rows[1] != (InventoryItem{SKU: "B", Quantity: 7}) {
		t.Fatalf("projected tuple-slice rows = %+v, %v", rows, err)
	}
	full, err := q.GetInventoryBySKUs(ctx, keys, GetInventoryBySKUsColsAll)
	if err != nil || len(full) != 2 || full[1] != (InventoryItem{WarehouseID: 1, SKU: "B", Quantity: 7, UpdatedAt: 200}) {
		t.Fatalf("all-columns rows = %+v, %v", full, err)
	}
	n := 0
	for it, err := range q.GetInventoryBySKUsSeq(ctx, keys, GetInventoryBySKUsColUpdatedAt) {
		if err != nil || it.UpdatedAt == 0 || it.SKU != "" {
			t.Fatalf("projected Seq item = %+v, %v", it, err)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("projected Seq yielded %d items", n)
	}

	orders, err := q.StreamFulfilledOrders(ctx, 55, StreamFulfilledOrdersColID, StreamFulfilledOrdersColTotalCents)
	if err != nil || len(orders) != 1 || orders[0] != (FulfilledOrder{ID: 1, TotalCents: 999}) {
		t.Fatalf("projected orders = %+v, %v", orders, err)
	}
	if orders, err := q.StreamFulfilledOrders(ctx, 55, StreamFulfilledOrdersColsAll); err != nil || len(orders) != 1 || orders[0].FulfilledAt != 50 {
		t.Fatalf("all-columns orders = %+v, %v", orders, err)
	}

	// The Pipeline forms take the same trailing argument; a transactional pipeline too.
	pl := q.Pipeline(ctx)
	fInv := pl.GetInventoryBySKUs(keys, GetInventoryBySKUsColSKU)
	fOrd := pl.StreamFulfilledOrders(55, StreamFulfilledOrdersColID)
	if err := pl.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, err := fInv.Get(); err != nil || len(got) != 2 || got[0] != (InventoryItem{SKU: "A"}) {
		t.Fatalf("pipeline projected inventory = %+v, %v", got, err)
	}
	if got, err := fOrd.Get(); err != nil || len(got) != 1 || got[0] != (FulfilledOrder{ID: 1}) {
		t.Fatalf("pipeline projected orders = %+v, %v", got, err)
	}
	var fTx *runtime.SliceFuture[FulfilledOrder]
	if err := q.PipelineTx(ctx, nil, func(pl *Pipeline) error {
		fTx = pl.StreamFulfilledOrders(100, StreamFulfilledOrdersColCustomerID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := fTx.Get(); err != nil || len(got) != 2 || got[1] != (FulfilledOrder{CustomerID: 11}) {
		t.Fatalf("PipelineTx projected orders = %+v, %v", got, err)
	}
	if err := q.PipelineTx(ctx, nil, nil); err == nil {
		t.Fatal("PipelineTx(nil fn) = nil error")
	}
}

// An empty VALUES list is a no-op in every call shape (no SQL, nothing queued that touches the database); an
// empty IN list and a nil iterator are errors.
func TestEmptyAndNilCollections(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, SchemaDDL); err != nil {
		t.Fatal(err)
	}
	q := New(db)

	pl := q.Pipeline(ctx)
	fItems := pl.InsertOrderItems(nil)
	fArchived := pl.InsertArchivedOrders(nil)
	if err := pl.Flush(); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]*runtime.ExecFuture{"InsertOrderItems": fItems, "InsertArchivedOrders": fArchived} {
		res, err := f.Result()
		if err != nil {
			t.Fatalf("%s(nil) = %v", name, err)
		}
		if n, _ := res.RowsAffected(); n != 0 {
			t.Fatalf("%s(nil) affected %d rows", name, n)
		}
	}

	if _, err := q.DeleteFulfilledOrdersByIDs(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("DeleteFulfilledOrdersByIDs(nil) = %v", err)
	}
	if _, err := q.DeleteFulfilledOrdersByIDsStream(ctx, runtime.FromSlice([]int64(nil))); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("DeleteFulfilledOrdersByIDsStream(empty) = %v", err)
	}
	if _, err := q.DeleteFulfilledOrdersByIDsStream(ctx, nil); !errors.Is(err, runtime.ErrNilArgument) {
		t.Fatalf("DeleteFulfilledOrdersByIDsStream(nil) = %v", err)
	}
	if res, err := q.DeleteFulfilledOrdersByIDsStream(ctx, runtime.FromSlice([]int64{998, 999})); err != nil {
		t.Fatalf("DeleteFulfilledOrdersByIDsStream = %v", err)
	} else if n, _ := res.RowsAffected(); n != 0 {
		t.Fatalf("DeleteFulfilledOrdersByIDsStream deleted %d unknown orders", n)
	}
	if _, err := q.GetInventoryBySKUs(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("GetInventoryBySKUs(nil) = %v", err)
	}
	if _, err := q.InsertArchivedOrdersStream(ctx, nil); !errors.Is(err, runtime.ErrNilArgument) {
		t.Fatalf("InsertArchivedOrdersStream(nil) = %v", err)
	}

	// Unsorted tuple and plain slices are sorted on expansion (the composable query, the queued exec); an
	// empty one fails the expansion.
	unsorted := []SKUKey{{WarehouseID: 2, SKU: "Z"}, {WarehouseID: 1, SKU: "B"}, {WarehouseID: 1, SKU: "A"}}
	qq := GetInventoryBySKUsQuery(unsorted)
	var args []any
	if err := qq.Expand(&args, runtime.DialectQuestion, &qq); err != nil {
		t.Fatal(err)
	}
	if want := []any{int64(1), "A", int64(1), "B", int64(2), "Z"}; !slices.Equal(args, want) {
		t.Fatalf("expanded args = %v, want %v", args, want)
	}
	empty := GetInventoryBySKUsQuery(nil)
	if err := empty.Expand(&args, runtime.DialectQuestion, &empty); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expanding an empty tuple slice = %v", err)
	}
	pl = q.Pipeline(ctx)
	pl.DeleteFulfilledOrdersByIDs([]int64{9, 3, 5})
	pl.DeleteFulfilledOrdersByIDs(nil)
	if err := pl.Flush(); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("Flush with an empty IN list = %v", err)
	}
}
