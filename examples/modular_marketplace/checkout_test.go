package modular_marketplace

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/CAFxX/sqlgen/examples/ecommerce_inventory"
	"github.com/CAFxX/sqlgen/examples/fintech_ledger"
	_ "modernc.org/sqlite"
)

func TestCheckoutAcrossGeneratedPackagesIsAtomic(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()

	if _, err := db.ExecContext(ctx, ecommerce_inventory.SchemaDDL); err != nil {
		t.Fatalf("inventory schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, fintech_ledger.SchemaDDL); err != nil {
		t.Fatalf("ledger schema: %v", err)
	}
	for _, account := range []struct {
		id      int64
		balance int64
	}{
		{id: 101, balance: 50_000},
		{id: 202, balance: 2_000},
	} {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO accounts (org_id, id, currency, balance_cents, status) VALUES (?, ?, 'USD', ?, 'active')",
			7, account.id, account.balance,
		); err != nil {
			t.Fatal(err)
		}
	}

	svc := New(db)
	defer func() { _ = svc.Close() }()
	in := CheckoutInput{
		MarketplaceID:   7,
		OrderID:         1001,
		BuyerAccountID:  101,
		SellerAccountID: 202,
		IdempotencyKey:  "checkout-7-1001",
		Currency:        "USD",
		Items: []ecommerce_inventory.NewOrderItem{
			{OrderID: 1001, SKU: "BOOK-1", Quantity: 2, UnitPriceCents: 1_250},
			{OrderID: 1001, SKU: "PEN-2", Quantity: 1, UnitPriceCents: 400},
		},
	}
	if err := svc.Checkout(ctx, in); err != nil {
		t.Fatalf("checkout: %v", err)
	}

	assertInt64(t, db, "SELECT balance_cents FROM accounts WHERE org_id = 7 AND id = 101", 47_100)
	assertInt64(t, db, "SELECT balance_cents FROM accounts WHERE org_id = 7 AND id = 202", 4_900)
	assertInt64(t, db, "SELECT COUNT(*) FROM order_items WHERE order_id = 1001", 2)
	assertInt64(t, db, "SELECT COUNT(*) FROM transfer_receipts WHERE idempotency_key = 'checkout-7-1001'", 1)

	// An exact replay is a no-op: this is the crash-recovery path when the SQLGen
	// transaction committed but DBOS did not get to checkpoint its step result.
	if err := svc.Checkout(ctx, in); err != nil {
		t.Fatalf("same checkout replay must be idempotent: %v", err)
	}

	// Reusing the receipt key for a different transfer is rejected before any new
	// order item or ledger operation can be committed.
	duplicate := in
	duplicate.Items = []ecommerce_inventory.NewOrderItem{
		{OrderID: 1001, SKU: "BOOK-3", Quantity: 1, UnitPriceCents: 9_999},
	}
	if err := svc.Checkout(ctx, duplicate); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}

	assertInt64(t, db, "SELECT balance_cents FROM accounts WHERE org_id = 7 AND id = 101", 47_100)
	assertInt64(t, db, "SELECT balance_cents FROM accounts WHERE org_id = 7 AND id = 202", 4_900)
	assertInt64(t, db, "SELECT COUNT(*) FROM order_items", 2)
	assertInt64(t, db, "SELECT COUNT(*) FROM transfer_receipts", 1)

	stats := svc.inventory.CacheStats()
	if stats.SequentialPipelines != 1 || stats.WirePipelines != 0 {
		t.Fatalf("SQLite should use the sequential transactional fallback for the first run only, got %+v", stats)
	}
}

func TestCheckoutRejectsInsufficientFundsAndSellerBalanceOverflow(t *testing.T) {
	const maxInt64 = int64(^uint64(0) >> 1)
	cases := []struct {
		name   string
		buyer  int64
		seller int64
		price  int64
	}{
		{name: "insufficient buyer funds", buyer: 100, seller: 2_000, price: 250},
		{name: "seller balance overflow", buyer: 1_000, seller: maxInt64 - 100, price: 250},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, svc := newCheckoutFixture(t, tc.buyer, tc.seller)
			err := svc.Checkout(context.Background(), CheckoutInput{
				MarketplaceID: 7, OrderID: 2001, BuyerAccountID: 101, SellerAccountID: 202,
				IdempotencyKey: CheckoutIdempotencyKey(7, 2001), Currency: "USD",
				Items: []ecommerce_inventory.NewOrderItem{
					{OrderID: 2001, SKU: "BOUNDARY", Quantity: 1, UnitPriceCents: tc.price},
				},
			})
			if !errors.Is(err, ErrCheckoutRejected) {
				t.Fatalf("expected ErrCheckoutRejected, got %v", err)
			}
			assertInt64(t, db, "SELECT balance_cents FROM accounts WHERE org_id = 7 AND id = 101", tc.buyer)
			assertInt64(t, db, "SELECT balance_cents FROM accounts WHERE org_id = 7 AND id = 202", tc.seller)
			assertInt64(t, db, "SELECT COUNT(*) FROM order_items", 0)
			assertInt64(t, db, "SELECT COUNT(*) FROM transfer_receipts", 0)
		})
	}
}

func newCheckoutFixture(t *testing.T, buyerBalance, sellerBalance int64) (*sql.DB, *Service) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, ecommerce_inventory.SchemaDDL); err != nil {
		_ = db.Close()
		t.Fatalf("inventory schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, fintech_ledger.SchemaDDL); err != nil {
		_ = db.Close()
		t.Fatalf("ledger schema: %v", err)
	}
	for _, account := range []struct {
		id      int64
		balance int64
	}{
		{id: 101, balance: buyerBalance},
		{id: 202, balance: sellerBalance},
	} {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO accounts (org_id, id, currency, balance_cents, status) VALUES (?, ?, 'USD', ?, 'active')",
			7, account.id, account.balance,
		); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	svc := New(db)
	t.Cleanup(func() {
		_ = svc.Close()
		_ = db.Close()
	})
	return db, svc
}

func TestCheckoutRejectsInvalidLineBeforeStartingPipeline(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(ecommerce_inventory.SchemaDDL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fintech_ledger.SchemaDDL); err != nil {
		t.Fatal(err)
	}
	svc := New(db)
	defer func() { _ = svc.Close() }()
	err = svc.Checkout(context.Background(), CheckoutInput{
		MarketplaceID: 7, OrderID: 1, BuyerAccountID: 101, SellerAccountID: 202,
		IdempotencyKey: "bad", Items: []ecommerce_inventory.NewOrderItem{
			{OrderID: 2, SKU: "MISMATCH", Quantity: 1, UnitPriceCents: 100},
		},
	})
	if err != ErrInvalidCheckout {
		t.Fatalf("expected ErrInvalidCheckout, got %v", err)
	}
	assertInt64(t, db, "SELECT COUNT(*) FROM order_items", 0)
}

func assertInt64(t *testing.T, db *sql.DB, query string, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got != want {
		t.Fatalf("%s: got %d, want %d", query, got, want)
	}
}
