package ecommerce_inventory

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/CAFxX/sqlgen"
	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

func TestEcommerceInventoryWorkerEndToEnd(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, SchemaDDL); err != nil {
		t.Fatalf("schema DDL: %v", err)
	}

	worker := NewInventoryWorker(db, runtime.WithPromoteThreshold(2))
	defer func() { _ = worker.Close() }()
	q := worker.Queries()

	// 1. Seed 300 inventory items across 3 warehouses and test composite tuple IN lookup
	for wh := int64(1); wh <= 3; wh++ {
		for s := 1; s <= 100; s++ {
			sku := fmt.Sprintf("SKU-%04d", s)
			if _, err := db.ExecContext(ctx,
				`INSERT INTO inventory (warehouse_id, sku, quantity, updated_at) VALUES (?, ?, ?, ?)`,
				wh, sku, wh*100+int64(s), 1700000000+int64(s),
			); err != nil {
				t.Fatalf("insert inventory (%d, %s): %v", wh, sku, err)
			}
		}
	}

	// Lookup in reverse order to verify non-mutating presort of (WarehouseID ASC, SKU ASC)
	lookupKeys := []SKUKey{
		{WarehouseID: 3, SKU: "SKU-0050"},
		{WarehouseID: 1, SKU: "SKU-0020"},
		{WarehouseID: 2, SKU: "SKU-0010"},
		{WarehouseID: 1, SKU: "SKU-0005"},
	}
	invItems, err := worker.LookupInventoryBatch(ctx, lookupKeys)
	if err != nil {
		t.Fatalf("LookupInventoryBatch: %v", err)
	}
	if len(invItems) != 4 {
		t.Fatalf("expected 4 inventory items, got %d", len(invItems))
	}
	if lookupKeys[0].WarehouseID != 3 || lookupKeys[0].SKU != "SKU-0050" {
		t.Fatalf("caller lookupKeys slice was mutated: %+v", lookupKeys)
	}
	// Promote 2-key batch shape then run a 5-key batch (sorted and unsorted) to exercise multi-chunk BatchInto/BatchSeq
	for range 2 {
		if _, err := worker.LookupInventoryBatch(ctx, lookupKeys[:2]); err != nil {
			t.Fatalf("2-key LookupInventoryBatch: %v", err)
		}
	}
	sortedKeys := []SKUKey{
		{WarehouseID: 1, SKU: "SKU-0001"},
		{WarehouseID: 1, SKU: "SKU-0002"},
		{WarehouseID: 2, SKU: "SKU-0001"},
		{WarehouseID: 2, SKU: "SKU-0002"},
		{WarehouseID: 3, SKU: "SKU-0001"},
	}
	if got, err := worker.LookupInventoryBatch(ctx, sortedKeys); err != nil || len(got) != 5 {
		t.Fatalf("5-key sorted LookupInventoryBatch: len=%d, err=%v", len(got), err)
	}
	unsortedKeys := []SKUKey{
		{WarehouseID: 3, SKU: "SKU-0001"},
		{WarehouseID: 2, SKU: "SKU-0002"},
		{WarehouseID: 2, SKU: "SKU-0001"},
		{WarehouseID: 1, SKU: "SKU-0002"},
		{WarehouseID: 1, SKU: "SKU-0001"},
	}
	if got, err := worker.LookupInventoryBatch(ctx, unsortedKeys); err != nil || len(got) != 5 {
		t.Fatalf("5-key unsorted LookupInventoryBatch: len=%d, err=%v", len(got), err)
	}

	// 2. Ingest 3,000 order items across recurring 500-row and 400-row batches, then verify
	// heavy-hitter promotion and 1,400 = 500 + 500 + 400 chunk decomposition.
	makeOrderItems := func(startID, count int) []NewOrderItem {
		out := make([]NewOrderItem, count)
		for i := range count {
			id := int64(startID + i)
			out[i] = NewOrderItem{
				OrderID:        (id % 500) + 1,
				SKU:            fmt.Sprintf("SKU-%04d", (id%100)+1),
				Quantity:       (id % 5) + 1,
				UnitPriceCents: 1999,
			}
		}
		return out
	}

	// 3 batches of 500 (1,500) + 3 batches of 400 (1,200) + 1 batch of 300 = 3,000 items
	offset := 1
	for range 3 {
		n, err := worker.IngestOrderItemsBatch(ctx, makeOrderItems(offset, 500))
		if err != nil || n != 500 {
			t.Fatalf("IngestOrderItemsBatch(500): n=%d, err=%v", n, err)
		}
		offset += 500
	}
	for range 3 {
		n, err := worker.IngestOrderItemsBatch(ctx, makeOrderItems(offset, 400))
		if err != nil || n != 400 {
			t.Fatalf("IngestOrderItemsBatch(400): n=%d, err=%v", n, err)
		}
		offset += 400
	}
	if n, err := worker.IngestOrderItemsBatch(ctx, makeOrderItems(offset, 300)); err != nil || n != 300 {
		t.Fatalf("IngestOrderItemsBatch(300): n=%d, err=%v", n, err)
	}
	offset += 300

	// Verify 500-row and 400-row variants are promoted to StatePrepared in the cache
	vk500, _, _, _ := computeInsertOrderItemsKey(paramsInsertOrderItems{Items: make([]NewOrderItem, 500)})
	vk400, _, _, _ := computeInsertOrderItemsKey(paramsInsertOrderItems{Items: make([]NewOrderItem, 400)})
	if st := q.Cache().LookupState(vk500, ""); st != runtime.StatePrepared {
		t.Fatalf("expected 500-row batch shape to be StatePrepared, got %v", st)
	}
	if st := q.Cache().LookupState(vk400, ""); st != runtime.StatePrepared {
		t.Fatalf("expected 400-row batch shape to be StatePrepared, got %v", st)
	}

	// Now ingest a 1,400-row batch: PlanBatchChunk must decompose 1400 = 500 + 500 + 400
	// without creating a 1,400-row cache entry!
	n1400, err := worker.IngestOrderItemsBatch(ctx, makeOrderItems(offset, 1400))
	if err != nil || n1400 != 1400 {
		t.Fatalf("IngestOrderItemsBatch(1400): n=%d, err=%v", n1400, err)
	}
	vk1400, _, _, _ := computeInsertOrderItemsKey(paramsInsertOrderItems{Items: make([]NewOrderItem, 1400)})
	if st := q.Cache().LookupState(vk1400, ""); st != runtime.StateAbsent {
		t.Fatalf("expected 1400-row shape to be decomposed into 500+500+400 (StateAbsent), got %v", st)
	}

	// 3. Seed 2,000 fulfilled orders (<= cutoff) and 500 unfulfilled/future orders, then run ArchiveFulfilledOrders
	const cutoffTS int64 = 1700005000
	const archivedAt int64 = 1700009999
	for i := 1; i <= 2500; i++ {
		status := "fulfilled"
		ts := cutoffTS - int64(2500-i)
		if i > 2000 {
			if i%2 == 0 {
				status = "pending"
			} else {
				ts = cutoffTS + int64(i)
			}
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO orders (id, customer_id, total_cents, status, fulfilled_at) VALUES (?, ?, ?, ?, ?)`,
			i, (i%200)+1, (i%50+1)*1000, status, ts,
		); err != nil {
			t.Fatalf("insert order %d: %v", i, err)
		}
	}

	archivedCount, err := worker.ArchiveFulfilledOrders(ctx, cutoffTS, archivedAt)
	if err != nil {
		t.Fatalf("ArchiveFulfilledOrders: %v", err)
	}
	if archivedCount != 2000 {
		t.Fatalf("expected 2000 archived orders, got %d", archivedCount)
	}
	// Second run should find 0 fulfilled orders <= cutoffTS
	if archivedAgain, err := worker.ArchiveFulfilledOrders(ctx, cutoffTS, archivedAt); err != nil || archivedAgain != 0 {
		t.Fatalf("second ArchiveFulfilledOrders: count=%d, err=%v", archivedAgain, err)
	}

	var remainingOrders, totalArchivedRows int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM orders`).Scan(&remainingOrders); err != nil || remainingOrders != 500 {
		t.Fatalf("remaining orders = %d (want 500), err=%v", remainingOrders, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM archived_orders`).Scan(&totalArchivedRows); err != nil || totalArchivedRows != 2000 {
		t.Fatalf("total archived_orders = %d (want 2000), err=%v", totalArchivedRows, err)
	}

	// 4. Exercise remaining generated methods, Stream/StreamSeq, BatchEach, Pipeline, and error paths
	if _, err := q.GetInventoryBySKUs(ctx, sortedKeys[:2]); err != nil {
		t.Fatalf("GetInventoryBySKUs(sorted): %v", err)
	}
	if _, err := q.GetInventoryBySKUs(ctx, unsortedKeys[:2]); err != nil {
		t.Fatalf("GetInventoryBySKUs(unsorted): %v", err)
	}
	var invEachCount int
	if err := q.GetInventoryBySKUsEach(ctx, sortedKeys[:2], func(item InventoryItem) error {
		invEachCount++
		return nil
	}); err != nil || invEachCount != 2 {
		t.Fatalf("GetInventoryBySKUsEach: count=%d, err=%v", invEachCount, err)
	}
	for range q.GetInventoryBySKUsSeq(ctx, sortedKeys[:2]) {
		break
	}
	errStop := errors.New("stop")
	_ = q.GetInventoryBySKUsEach(ctx, sortedKeys[:2], func(item InventoryItem) error { return errStop })
	_ = q.GetInventoryBySKUsEach(ctx, nil, func(item InventoryItem) error { return nil })

	var batchEachCount int
	if err := q.GetInventoryBySKUsBatchEach(ctx, sortedKeys, func(item InventoryItem) error {
		batchEachCount++
		return nil
	}); err != nil || batchEachCount != 5 {
		t.Fatalf("GetInventoryBySKUsBatchEach: count=%d, err=%v", batchEachCount, err)
	}
	for range q.GetInventoryBySKUsBatchSeq(ctx, sortedKeys) {
		break
	}
	_ = q.GetInventoryBySKUsBatchEach(ctx, sortedKeys, func(item InventoryItem) error { return errStop })
	_ = q.GetInventoryBySKUsBatchEach(ctx, nil, func(item InventoryItem) error { return nil })
	_, _ = q.GetInventoryBySKUsBatch(ctx, nil)

	// GetInventoryBySKUsStream and GetInventoryBySKUsStreamSeq (including >256 items, empty stream, early break, error stream)
	bigKeySlice := make([]SKUKey, 260)
	for i := range bigKeySlice {
		bigKeySlice[i] = SKUKey{WarehouseID: int64((i % 3) + 1), SKU: fmt.Sprintf("SKU-%04d", (i%100)+1)}
	}
	if _, err := q.GetInventoryBySKUsStream(ctx, runtime.FromSlice(bigKeySlice)); err != nil {
		t.Fatalf("GetInventoryBySKUsStream(260): %v", err)
	}
	if _, err := q.GetInventoryBySKUsStream(ctx, runtime.FromSlice([]SKUKey(nil))); err != nil {
		t.Fatalf("GetInventoryBySKUsStream(empty): %v", err)
	}
	for range q.GetInventoryBySKUsStreamSeq(ctx, runtime.FromSlice(bigKeySlice)) {
		break
	}
	errSeqSKU := func(yield func(SKUKey, error) bool) {
		yield(SKUKey{}, errStop)
	}
	_, _ = q.GetInventoryBySKUsStream(ctx, errSeqSKU)

	// InsertOrderItems direct, Stream, StreamSeq, empty/error paths
	if _, err := q.InsertOrderItems(ctx, makeOrderItems(9000, 2)); err != nil {
		t.Fatalf("InsertOrderItems: %v", err)
	}
	if _, err := q.InsertOrderItems(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam, got %v", err)
	}
	if _, err := q.InsertOrderItemsBatch(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam, got %v", err)
	}
	if _, err := q.InsertOrderItemsStream(ctx, runtime.FromSlice(makeOrderItems(9100, 260))); err != nil {
		t.Fatalf("InsertOrderItemsStream(260): %v", err)
	}
	if _, err := q.InsertOrderItemsStreamSeq(ctx, runtime.FromSlice([]NewOrderItem(nil))); err != nil {
		t.Fatalf("InsertOrderItemsStreamSeq(empty): %v", err)
	}
	_, _ = q.InsertOrderItemsStream(ctx, func(yield func(NewOrderItem, error) bool) {
		yield(NewOrderItem{}, errStop)
	})

	// StreamFulfilledOrdersEach, Seq, Query
	_ = q.StreamFulfilledOrdersEach(ctx, cutoffTS+100000, func(o FulfilledOrder) error {
		return nil
	})
	_ = q.StreamFulfilledOrdersEach(ctx, cutoffTS+100000, func(o FulfilledOrder) error {
		return errStop
	})
	for range q.StreamFulfilledOrdersSeq(ctx, cutoffTS+100000) {
		break
	}

	// InsertArchivedOrders direct, Batch, StreamSeq, empty/error paths
	if _, err := q.InsertArchivedOrders(ctx, []ArchivedOrderRow{
		{ID: 99001, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
	}); err != nil {
		t.Fatalf("InsertArchivedOrders: %v", err)
	}
	_, _ = q.InsertArchivedOrders(ctx, nil)
	_, _ = q.InsertArchivedOrdersBatch(ctx, nil)
	_, _ = q.InsertArchivedOrdersStreamSeq(ctx, runtime.FromSlice([]ArchivedOrderRow(nil)))
	_, _ = q.InsertArchivedOrdersStream(ctx, func(yield func(ArchivedOrderRow, error) bool) {
		yield(ArchivedOrderRow{}, errStop)
	})
	// Promote 2-row InsertArchivedOrdersBatch then run 3-row batch for multi-chunk autoTx
	for i := range 2 {
		baseID := int64(99100 + i*10)
		if _, err := q.InsertArchivedOrdersBatch(ctx, []ArchivedOrderRow{
			{ID: baseID + 1, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
			{ID: baseID + 2, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
		}); err != nil {
			t.Fatalf("InsertArchivedOrdersBatch(2): %v", err)
		}
	}
	if _, err := q.InsertArchivedOrdersBatch(ctx, []ArchivedOrderRow{
		{ID: 99201, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
		{ID: 99202, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
		{ID: 99203, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
	}); err != nil {
		t.Fatalf("InsertArchivedOrdersBatch(3): %v", err)
	}

	// DeleteFulfilledOrdersByIDs direct & Batch (sorted and unsorted)
	if _, err := q.DeleteFulfilledOrdersByIDs(ctx, []int64{2003, 2001}); err != nil {
		t.Fatalf("DeleteFulfilledOrdersByIDs(unsorted): %v", err)
	}
	if _, err := q.DeleteFulfilledOrdersByIDs(ctx, []int64{2005, 2007}); err != nil {
		t.Fatalf("DeleteFulfilledOrdersByIDs(sorted): %v", err)
	}
	_, _ = q.DeleteFulfilledOrdersByIDs(ctx, nil)
	_, _ = q.DeleteFulfilledOrdersByIDsBatch(ctx, nil)
	for range 2 {
		if _, err := q.DeleteFulfilledOrdersByIDsBatch(ctx, []int64{999991, 999992}); err != nil {
			t.Fatalf("DeleteFulfilledOrdersByIDsBatch(2): %v", err)
		}
	}
	if _, err := q.DeleteFulfilledOrdersByIDsBatch(ctx, []int64{999995, 999994, 999993}); err != nil {
		t.Fatalf("DeleteFulfilledOrdersByIDsBatch(3 unsorted): %v", err)
	}
	if _, err := q.DeleteFulfilledOrdersByIDsBatch(ctx, []int64{999996, 999997, 999998}); err != nil {
		t.Fatalf("DeleteFulfilledOrdersByIDsBatch(3 sorted): %v", err)
	}

	// Curried Query constructors & Pipeline methods
	var dummyArgs []any
	invQ := q.GetInventoryBySKUsQuery(sortedKeys[:2])
	_ = invQ.Expand(&dummyArgs, runtime.DialectQuestion, &invQ)
	sfoQ := q.StreamFulfilledOrdersQuery(cutoffTS)
	_ = sfoQ.Expand(&dummyArgs, runtime.DialectQuestion, &sfoQ)
	_ = WhereCol(&runtime.RowFuture[int]{}, "1=1", nil)
	_ = NewWithCache(db, nil)
	_ = q.CacheStats()
	_ = q.WithPrepared(false)

	if err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
		f1 := p.GetInventoryBySKUs(sortedKeys[:2])
		_ = p.InsertOrderItems(makeOrderItems(98000, 1))
		_ = p.StreamFulfilledOrders(cutoffTS)
		_ = p.InsertArchivedOrders([]ArchivedOrderRow{
			{ID: 99301, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
		})
		_ = p.DeleteFulfilledOrdersByIDs([]int64{999999})
		p.If(f1.Found(), func(thenP *Pipeline) {
			_ = thenP.DeleteFulfilledOrdersByIDs([]int64{999998})
		}, func(elseP *Pipeline) {
			_ = elseP.DeleteFulfilledOrdersByIDs([]int64{999997})
		})
		return nil
	}); err != nil {
		t.Fatalf("PipelineTx: %v", err)
	}
	_ = q.InTxOpts(ctx, nil, func(txQ *Queries) error { return nil })
	_ = q.PipelineTx(ctx, nil, func(p *Pipeline) error { return errStop })
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	txView := q.WithTx(tx)
	_ = txView.Close()
	_ = tx.Rollback()

	// Exercise duplicate keys in unsorted slices & DB error paths on a closed connection
	_, _ = q.GetInventoryBySKUs(ctx, []SKUKey{
		{WarehouseID: 2, SKU: "SKU-0002"},
		{WarehouseID: 1, SKU: "SKU-0002"},
		{WarehouseID: 1, SKU: "SKU-0001"},
		{WarehouseID: 1, SKU: "SKU-0002"},
	})
	_, _ = q.DeleteFulfilledOrdersByIDs(ctx, []int64{2009, 2008, 2009})

	// Duplicate PK error path inside ArchiveFulfilledOrders
	_, _ = db.ExecContext(ctx, `INSERT INTO orders (id, customer_id, total_cents, status, fulfilled_at) VALUES (99001, 1, 100, 'fulfilled', ?)`, cutoffTS)
	if _, err := worker.ArchiveFulfilledOrders(ctx, cutoffTS, archivedAt); err == nil {
		t.Fatalf("expected duplicate PK error from ArchiveFulfilledOrders")
	}

	closedDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_ = closedDB.Close()
	closedW := NewInventoryWorker(closedDB)
	_, _ = closedW.LookupInventoryBatch(ctx, sortedKeys[:2])
	_, _ = closedW.IngestOrderItemsBatch(ctx, makeOrderItems(1, 2))
	_, _ = closedW.ArchiveFulfilledOrders(ctx, cutoffTS, archivedAt)
	_, _ = closedW.Queries().GetInventoryBySKUs(ctx, sortedKeys[:2])
	_, _ = closedW.Queries().GetInventoryBySKUsStream(ctx, runtime.FromSlice(bigKeySlice))
	_, _ = closedW.Queries().InsertOrderItems(ctx, makeOrderItems(1, 2))
	_, _ = closedW.Queries().InsertOrderItemsStream(ctx, runtime.FromSlice(makeOrderItems(1, 260)))
	_, _ = closedW.Queries().InsertOrderItemsStream(ctx, runtime.FromSlice(makeOrderItems(1, 2)))
	_, _ = closedW.Queries().InsertArchivedOrders(ctx, []ArchivedOrderRow{{ID: 1}})
	_, _ = closedW.Queries().InsertArchivedOrdersBatch(ctx, []ArchivedOrderRow{{ID: 1}})
	_, _ = closedW.Queries().InsertArchivedOrdersStream(ctx, runtime.FromSlice(make([]ArchivedOrderRow, 260)))
	_, _ = closedW.Queries().InsertArchivedOrdersStream(ctx, runtime.FromSlice([]ArchivedOrderRow{{ID: 1}}))
	_, _ = closedW.Queries().DeleteFulfilledOrdersByIDs(ctx, []int64{1})
	_, _ = closedW.Queries().DeleteFulfilledOrdersByIDsBatch(ctx, []int64{1})

	// Exercise multi-chunk autoTx rollback when chunk 2 fails on duplicate PK (99201 already exists)
	if _, err := q.InsertArchivedOrdersBatch(ctx, []ArchivedOrderRow{
		{ID: 99401, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
		{ID: 99402, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
		{ID: 99201, CustomerID: 1, TotalCents: 100, FulfilledAt: cutoffTS, ArchivedAt: archivedAt},
	}); err == nil {
		t.Fatalf("expected duplicate PK error on chunk 2 of InsertArchivedOrdersBatch")
	}

	// Exercise multi-chunk autoTx BeginTx error paths using a canceled context while heavy chunks are promoted
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, _ = q.GetInventoryBySKUsBatch(canceledCtx, sortedKeys)
	_, _ = q.InsertOrderItemsBatch(canceledCtx, makeOrderItems(1, 1400))
	_, _ = q.InsertArchivedOrdersBatch(canceledCtx, make([]ArchivedOrderRow, 3))
	_, _ = q.DeleteFulfilledOrdersByIDsBatch(canceledCtx, []int64{1, 2, 3})
	_, _ = q.StreamFulfilledOrders(canceledCtx, cutoffTS)
	_ = worker.Close()
}

func TestDogfoodingCheckIdempotency(t *testing.T) {
	configs := []struct {
		cfgPath string
		genPath string
	}{
		{cfgPath: "../fintech_ledger/sqlgen.yaml", genPath: "../fintech_ledger/ledger_gen.go"},
		{cfgPath: "../saas_multitenant/sqlgen.yaml", genPath: "../saas_multitenant/portal_gen.go"},
		{cfgPath: "sqlgen.yaml", genPath: "inventory_gen.go"},
	}
	for _, tc := range configs {
		yamlData, err := os.ReadFile(filepath.Clean(tc.cfgPath))
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", tc.cfgPath, err)
		}
		genBytes, err := sqlgen.GenerateFromYAML(yamlData)
		if err != nil {
			t.Fatalf("GenerateFromYAML(%s): %v", tc.cfgPath, err)
		}
		diskBytes, err := os.ReadFile(filepath.Clean(tc.genPath))
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", tc.genPath, err)
		}
		if !bytes.Equal(genBytes, diskBytes) {
			t.Fatalf("generated code drift detected for %s", tc.cfgPath)
		}
	}
}
