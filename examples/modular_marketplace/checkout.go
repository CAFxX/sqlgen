// Package modular_marketplace demonstrates one SQLGen transaction spanning
// independently generated domain packages.
package modular_marketplace

import (
	"context"
	"database/sql"
	"errors"

	"github.com/CAFxX/sqlgen/examples/ecommerce_inventory"
	"github.com/CAFxX/sqlgen/examples/fintech_ledger"
	"github.com/CAFxX/sqlgen/runtime"
)

var (
	ErrInvalidCheckout = errors.New("modular_marketplace: invalid checkout")
	ErrCheckoutRejected = errors.New("modular_marketplace: buyer/seller account is unavailable or balance limits would be violated")
)

// Service owns the inventory module's database handle, pipeline transaction and cache.
// Other generated packages only receive typed views of that pipeline.
type Service struct {
	inventory *ecommerce_inventory.Queries
}

func New(db *sql.DB, opts ...runtime.CacheOption) *Service {
	return &Service{inventory: ecommerce_inventory.New(db, opts...)}
}

func (s *Service) Close() error { return s.inventory.Close() }

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
	return s.inventory.PipelineTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(p *ecommerce_inventory.Pipeline) error {
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
}

func validateAndTotal(in CheckoutInput) (int64, error) {
	if in.MarketplaceID <= 0 || in.OrderID <= 0 ||
		in.BuyerAccountID <= 0 || in.SellerAccountID <= 0 ||
		in.BuyerAccountID == in.SellerAccountID ||
		in.IdempotencyKey == "" || in.Currency == "" || len(in.Items) == 0 {
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
