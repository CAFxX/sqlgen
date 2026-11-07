//go:generate go run ../../cmd/sqlgen -config sqlgen.yaml

package ecommerce_inventory

import (
	"context"
	"database/sql"

	"github.com/CAFxX/sqlgen/runtime"
)

const SchemaDDL = `
CREATE TABLE IF NOT EXISTS inventory (
	warehouse_id INTEGER NOT NULL,
	sku          TEXT    NOT NULL,
	quantity     INTEGER NOT NULL,
	updated_at   INTEGER NOT NULL,
	PRIMARY KEY (warehouse_id, sku)
);

CREATE TABLE IF NOT EXISTS order_items (
	id               INTEGER PRIMARY KEY AUTOINCREMENT,
	order_id         INTEGER NOT NULL,
	sku              TEXT    NOT NULL,
	quantity         INTEGER NOT NULL,
	unit_price_cents INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
	id           INTEGER PRIMARY KEY,
	customer_id  INTEGER NOT NULL,
	total_cents  INTEGER NOT NULL,
	status       TEXT    NOT NULL,
	fulfilled_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS archived_orders (
	id           INTEGER PRIMARY KEY,
	customer_id  INTEGER NOT NULL,
	total_cents  INTEGER NOT NULL,
	fulfilled_at INTEGER NOT NULL,
	archived_at  INTEGER NOT NULL
);
`

type InventoryWorker struct {
	q *Queries
}

func NewInventoryWorker(db *sql.DB, opts ...runtime.CacheOption) *InventoryWorker {
	return &InventoryWorker{
		q: New(db, opts...),
	}
}

func (w *InventoryWorker) Queries() *Queries {
	return w.q
}

func (w *InventoryWorker) Close() error {
	return w.q.Close()
}

// LookupInventoryBatch fetches inventory records for a composite slice of (warehouse_id, sku) keys
// with automatic deadlock-free presorting and heavy-hitter chunk decomposition.
func (w *InventoryWorker) LookupInventoryBatch(ctx context.Context, keys []SKUKey) ([]InventoryItem, error) {
	return w.q.GetInventoryBySKUsBatch(ctx, keys)
}

// IngestOrderItemsBatch inserts a slice of order items using adaptive chunk decomposition
// across promoted heavy-hitter statement shapes.
func (w *InventoryWorker) IngestOrderItemsBatch(ctx context.Context, items []NewOrderItem) (int64, error) {
	res, err := w.q.InsertOrderItemsBatch(ctx, items)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ArchiveFulfilledOrders streams fulfilled orders up to cutoffTS using iter.Seq2, transforms
// them via runtime.MapSeq2 into ArchivedOrderRow chunks, bulk-inserts them via InsertArchivedOrdersStream,
// and deletes the source rows via DeleteFulfilledOrdersByIDsBatch inside a single transaction.
func (w *InventoryWorker) ArchiveFulfilledOrders(
	ctx context.Context,
	cutoffTS int64,
	archivedAt int64,
) (int64, error) {
	var totalArchived int64
	err := w.q.InTx(ctx, nil, func(txQ *Queries) error {
		// Buffer fulfilled orders from the streaming query so SQLite's single connection
		// inside the transaction can execute the chunked INSERT and DELETE batches.
		fulfilled, err := txQ.StreamFulfilledOrders(ctx, cutoffTS)
		if err != nil {
			return err
		}
		if len(fulfilled) == 0 {
			return nil
		}

		idsToDelete := make([]int64, 0, len(fulfilled))
		archiveSeq := runtime.MapSeq2(runtime.FromSlice(fulfilled), func(o FulfilledOrder) (ArchivedOrderRow, error) {
			idsToDelete = append(idsToDelete, o.ID)
			return ArchivedOrderRow{
				ID:          o.ID,
				CustomerID:  o.CustomerID,
				TotalCents:  o.TotalCents,
				FulfilledAt: o.FulfilledAt,
				ArchivedAt:  archivedAt,
			}, nil
		})

		insRes, err := txQ.InsertArchivedOrdersStream(ctx, archiveSeq)
		if err != nil {
			return err
		}
		inserted, err := insRes.RowsAffected()
		if err != nil {
			return err
		}

		delRes, err := txQ.DeleteFulfilledOrdersByIDsBatch(ctx, idsToDelete)
		if err != nil {
			return err
		}
		deleted, err := delRes.RowsAffected()
		if err != nil {
			return err
		}
		if inserted != deleted {
			return sql.ErrTxDone
		}
		totalArchived = inserted
		return nil
	}, WithTxRetry(DefaultRetryPolicy()))
	return totalArchived, err
}
