package saas_multitenant

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

// An Order that repeats a column, or names a column the statement does not have, is an error, not a
// silently different sort. (Passing the tokens of another statement does not compile at all: see
// TestOrderTokensOfOtherStatementsDoNotCompile in the root package.)
func TestInvalidOrderIsAnError(t *testing.T) {
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

	page := func(sort runtime.Order) error {
		_, err := q.ListMembersPaginated(ctx, ListMembersPaginatedParams{OrgID: 1, Sort: sort, Limit: 5})
		return err
	}
	if err := page(OrderListMembersPaginated(ListMembersPaginatedOrderByScore.Desc())); err != nil {
		t.Fatalf("valid order: %v", err)
	}
	if err := page(0); err != nil {
		t.Fatalf("zero order: %v", err)
	}
	for name, sort := range map[string]runtime.Order{
		"duplicate column": OrderListMembersPaginated(ListMembersPaginatedOrderByScore.Desc(), ListMembersPaginatedOrderByScore.Asc()),
		"unknown column":   OrderListMembersPaginated(ListMembersPaginatedSortCol(13).Asc()),
		"zero term":        OrderListMembersPaginated(0),
	} {
		if err := page(sort); !errors.Is(err, runtime.ErrInvalidOrder) {
			t.Errorf("%s: err = %v, want runtime.ErrInvalidOrder", name, err)
		}
	}
}
