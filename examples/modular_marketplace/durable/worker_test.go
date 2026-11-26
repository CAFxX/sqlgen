package durable

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen/examples/ecommerce_inventory"
	"github.com/CAFxX/sqlgen/examples/fintech_ledger"
	marketplace "github.com/CAFxX/sqlgen/examples/modular_marketplace"
	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

type testCarrier struct {
	mu    sync.Mutex
	calls map[string]string
}

func (c *testCarrier) CreateLabel(_ context.Context, key string, orderID int64) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous, ok := c.calls[key]; ok {
		return previous, nil
	}
	tracking := fmt.Sprintf("TRACK-%d", orderID)
	c.calls[key] = tracking
	return tracking, nil
}

func (c *testCarrier) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func TestDBOSDurableCheckoutAndWorkflowDeduplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("sqlite", "file:marketplace-dbos-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })

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

	carrier := &testCarrier{calls: make(map[string]string)}
	worker, err := NewWorker(ctx, db, carrier)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	t.Cleanup(func() {
		if err := worker.Close(); err != nil {
			t.Errorf("worker close: %v", err)
		}
	})

	input := WorkflowInput{
		Checkout: marketplace.CheckoutInput{
			MarketplaceID: 7, OrderID: 3001, BuyerAccountID: 101, SellerAccountID: 202,
			IdempotencyKey: marketplace.CheckoutIdempotencyKey(7, 3001), Currency: "USD",
			Items: []ecommerce_inventory.NewOrderItem{
				{OrderID: 3001, SKU: "BOOK-9", Quantity: 2, UnitPriceCents: 1_500},
			},
		},
		FulfillmentDelay: 5 * time.Millisecond,
	}

	first, err := worker.Submit(input)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	got, err := first.GetResult(dbos.WithHandleTimeout(20 * time.Second))
	if err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if got.OrderID != 3001 || got.TrackingNumber != "TRACK-3001" {
		t.Fatalf("unexpected workflow result: %+v", got)
	}

	// A duplicate start for the same business key returns the existing workflow,
	// rather than running a second checkout or requesting another carrier label.
	second, err := worker.Submit(input)
	if err != nil {
		t.Fatalf("duplicate submit: %v", err)
	}
	again, err := second.GetResult(dbos.WithHandleTimeout(20 * time.Second))
	if err != nil {
		t.Fatalf("duplicate workflow result: %v", err)
	}
	if again != got {
		t.Fatalf("duplicate workflow returned a different result: first=%+v second=%+v", got, again)
	}
	if carrier.callCount() != 1 {
		t.Fatalf("carrier calls = %d, want 1", carrier.callCount())
	}

	assertDBInt64(t, db, "SELECT balance_cents FROM accounts WHERE org_id = 7 AND id = 101", 47_000)
	assertDBInt64(t, db, "SELECT balance_cents FROM accounts WHERE org_id = 7 AND id = 202", 5_000)
	assertDBInt64(t, db, "SELECT COUNT(*) FROM order_items WHERE order_id = 3001", 1)
	assertDBInt64(t, db, "SELECT COUNT(*) FROM transfer_receipts WHERE idempotency_key = 'checkout-3001'", 1)
}

func assertDBInt64(t *testing.T, db *sql.DB, query string, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got != want {
		t.Fatalf("%s: got %d, want %d", query, got, want)
	}
}
