package fintech_ledger

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

// GetAccount is positional (no flag blocks) and still projects columns through the trailing variadic;
// naming every column is the default projection. Queued upserts with an empty list are no-ops and an
// unsorted list is sorted for the wire.
func TestPositionalProjectionAndQueuedUpserts(t *testing.T) {
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
	if _, err := db.ExecContext(ctx, `INSERT INTO accounts (org_id, id, currency, balance_cents, status) VALUES (1, 7, 'EUR', 250, 'frozen')`); err != nil {
		t.Fatal(err)
	}
	q := New(db)

	full := Account{OrgID: 1, ID: 7, Currency: "EUR", BalanceCents: 250, Status: "frozen"}
	if got, err := q.GetAccount(ctx, 1, 7, GetAccountColsAll); err != nil || got != full {
		t.Fatalf("all columns = %+v, %v", got, err)
	}
	if got, err := q.GetAccount(ctx, 1, 7, GetAccountColBalanceCents, GetAccountColStatus); err != nil || got != (Account{BalanceCents: 250, Status: "frozen"}) {
		t.Fatalf("projected = %+v, %v", got, err)
	}

	pl := q.Pipeline(ctx)
	none := pl.UpsertAccountBalances(nil)
	some := pl.UpsertAccountBalances([]AccountSettlement{{OrgID: 2, AccountID: 1, Currency: "USD", DeltaCents: 5}, {OrgID: 1, AccountID: 7, Currency: "EUR", DeltaCents: 10}})
	if err := pl.Flush(); err != nil {
		t.Fatal(err)
	}
	if res, err := none.Result(); err != nil {
		t.Fatalf("empty upsert = %v", err)
	} else if n, _ := res.RowsAffected(); n != 0 {
		t.Fatalf("empty upsert affected %d rows", n)
	}
	if _, err := some.Result(); err != nil {
		t.Fatal(err)
	}
	if got, err := q.GetAccount(ctx, 1, 7, GetAccountColBalanceCents); err != nil || got.BalanceCents != 260 {
		t.Fatalf("after upsert = %+v, %v", got, err)
	}

	if _, err := q.UpsertAccountBalancesStream(ctx, nil); !errors.Is(err, runtime.ErrNilArgument) {
		t.Fatalf("UpsertAccountBalancesStream(nil) = %v", err)
	}
}
