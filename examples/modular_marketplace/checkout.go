// Package modular_marketplace demonstrates one SQLGen transaction spanning
// independently generated domain packages.
package modular_marketplace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/CAFxX/sqlgen/examples/ecommerce_inventory"
	"github.com/CAFxX/sqlgen/examples/fintech_ledger"
	"github.com/CAFxX/sqlgen/runtime"
)

var (
	ErrInvalidCheckout     = errors.New("modular_marketplace: invalid checkout")
	ErrCheckoutRejected    = errors.New("modular_marketplace: buyer/seller account is unavailable or balance limits would be violated")
	ErrIdempotencyConflict = errors.New("modular_marketplace: idempotency key was already used for a different transfer")
)

// Service owns the inventory module's database handle, pipeline transaction and cache.
// Other generated packages only receive typed views of that pipeline.
type Service struct {
	db        *sql.DB
	inventory *ecommerce_inventory.Queries
}

func New(db *sql.DB, opts ...runtime.CacheOption) *Service {
	return &Service{db: db, inventory: ecommerce_inventory.New(db, opts...)}
}

func (s *Service) Close() error { return s.inventory.Close() }

// CheckoutIdempotencyKey derives a globally unique transfer key from the store and
// order identity. Requiring this shape lets the receipt fence distinguish a replay
// from accidental key reuse for a different order.
func CheckoutIdempotencyKey(marketplaceID, orderID int64) string {
	return fmt.Sprintf("checkout-%d-%d", marketplaceID, orderID)
}

// CheckoutInput is a focused single-seller checkout. The wider marketplace can split a
// parent order into seller suborders while retaining the same transaction boundary.
type CheckoutInput struct {
	MarketplaceID   int64
	OrderID         int64
	BuyerAccountID  int64
	SellerAccountID int64
	IdempotencyKey  string
	Currency        string
	Items           []ecommerce_inventory.NewOrderItem
}

// Checkout persists order items and the associated financial transfer atomically.
// Inventory/order data and ledger data are generated in separate packages, but every
// statement is queued on the inventory package's single PipelineTx.
func (s *Service) Checkout(ctx context.Context, in CheckoutInput) error {
	total, err := validateAndTotal(in)
	if err != nil {
		return err
	}

	// DBOS may replay a completed business transaction if the process dies after
	// COMMIT but before the step checkpoint is persisted. The receipt is committed
	// in the same PipelineTx as the debits/credits, so it is the durable effect fence.
	done, err := s.transferAlreadyCommitted(ctx, in, total)
	if err != nil || done {
		return err
	}

	err = s.inventory.PipelineTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(p *ecommerce_inventory.Pipeline) error {
		p.InsertOrderItems(in.Items).AssertRows(int64(len(in.Items)))

		ledger := fintech_ledger.WrapPipeline(p.Pipeline)
		buyer := ledger.GetAccount(in.MarketplaceID, in.BuyerAccountID)
		seller := ledger.GetAccount(in.MarketplaceID, in.SellerAccountID)
		const maxInt64 = int64(^uint64(0) >> 1)
		canSettle := buyer.Found().
			And(buyer.WhereCol("status = 'active'")).
			And(buyer.WhereCol("currency = ?", in.Currency)).
			And(buyer.WhereCol("balance_cents >= ?", total)).
			And(seller.Found()).
			And(seller.WhereCol("status = 'active'")).
			And(seller.WhereCol("currency = ?", in.Currency)).
			And(seller.WhereCol("balance_cents <= ?", maxInt64-total))

		ledger.If(canSettle, func(approved *fintech_ledger.Pipeline) {
			approved.DebitAccount(total, in.MarketplaceID, in.BuyerAccountID).AssertRows(1)
			approved.CreditAccount(total, in.MarketplaceID, in.SellerAccountID).AssertRows(1)
			approved.InsertTransferReceipt(
				in.IdempotencyKey,
				in.MarketplaceID,
				in.BuyerAccountID,
				in.SellerAccountID,
				total,
			).AssertRows(1)
		}).Else(func(rejected *fintech_ledger.Pipeline) {
			rejected.Abort(ErrCheckoutRejected)
		})
		return nil
	}, runtime.WithTxRetry(runtime.DefaultRetryPolicy()))
	if err == nil {
		return nil
	}

	// Close the concurrent duplicate window too: if another transaction committed
	// the same transfer while this attempt was running, treat the effect as applied.
	done, checkErr := s.transferAlreadyCommitted(ctx, in, total)
	if checkErr != nil {
		return errors.Join(err, checkErr)
	}
	if done {
		return nil
	}
	return err
}

func (s *Service) transferAlreadyCommitted(ctx context.Context, in CheckoutInput, total int64) (bool, error) {
	var orgID, senderID, receiverID, amount int64
	err := s.db.QueryRowContext(ctx,
		"SELECT org_id, sender_id, receiver_id, amount_cents FROM transfer_receipts WHERE idempotency_key = ?",
		in.IdempotencyKey,
	).Scan(&orgID, &senderID, &receiverID, &amount)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if orgID != in.MarketplaceID || senderID != in.BuyerAccountID ||
		receiverID != in.SellerAccountID || amount != total {
		return false, ErrIdempotencyConflict
	}
	return true, nil
}

func validateAndTotal(in CheckoutInput) (int64, error) {
	if in.MarketplaceID <= 0 || in.OrderID <= 0 ||
		in.BuyerAccountID <= 0 || in.SellerAccountID <= 0 ||
		in.BuyerAccountID == in.SellerAccountID ||
		in.IdempotencyKey != CheckoutIdempotencyKey(in.MarketplaceID, in.OrderID) ||
		in.Currency == "" || len(in.Items) == 0 {
		return 0, ErrInvalidCheckout
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	var total int64
	for _, item := range in.Items {
		if item.OrderID != in.OrderID || item.SKU == "" ||
			item.Quantity <= 0 || item.UnitPriceCents <= 0 ||
			item.Quantity > maxInt64/item.UnitPriceCents {
			return 0, ErrInvalidCheckout
		}
		lineTotal := item.Quantity * item.UnitPriceCents
		if total > maxInt64-lineTotal {
			return 0, ErrInvalidCheckout
		}
		total += lineTotal
	}
	if total <= 0 {
		return 0, ErrInvalidCheckout
	}
	return total, nil
}
