package ecommerce_inventory

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

// A nil iterator or callback is a caller error reported as runtime.ErrNilArgument, never a nil-pointer panic.
func TestNilIteratorAndCallbackArgumentsAreErrors(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(SchemaDDL); err != nil {
		t.Fatal(err)
	}
	q := New(db)
	defer func() { _ = q.Close() }()
	ctx := context.Background()
	keys := []SKUKey{{WarehouseID: 1, SKU: "a"}}
	if _, err := db.Exec(`INSERT INTO inventory (warehouse_id, sku, quantity, updated_at) VALUES (1, 'a', 1, 1)`); err != nil {
		t.Fatal(err)
	}

	_, execStream := q.InsertOrderItemsStream(ctx, nil)
	_, manyStream := q.GetInventoryBySKUsStream(ctx, nil)
	var manyStreamSeq error
	for _, err := range q.GetInventoryBySKUsStreamSeq(ctx, nil) {
		manyStreamSeq = err
	}
	for name, err := range map[string]error{
		"exec Stream":     execStream,
		"many Stream":     manyStream,
		"many StreamSeq":  manyStreamSeq,
		"Each":            runtime.Each(q.GetInventoryBySKUsSeq(ctx, keys), nil),
		"single-row Each": runtime.Each(q.StreamFulfilledOrdersSeq(ctx, 1), nil),
		"InTx":            q.InTx(ctx, nil, nil),
		"PipelineTx":      q.PipelineTx(ctx, nil, nil),
	} {
		if !errors.Is(err, runtime.ErrNilArgument) {
			t.Errorf("%s: err = %v, want runtime.ErrNilArgument", name, err)
		}
	}
}
