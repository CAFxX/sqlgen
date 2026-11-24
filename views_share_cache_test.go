package sqlgen_test

import (
	"context"
	"testing"
)

// The views of a Queries (WithPrepared, WithTx, the InTx callback's) share its statement cache: a statement
// promoted through one view is a prepared-statement hit for the others, and CacheStats of the original
// Queries counts them all.
func TestViewsShareTheCache(t *testing.T) {
	ctx := context.Background()
	db, q := setupE2EDB(t)
	if _, err := q.CreateUser(ctx, 1, 1, "a@x", "member", 1, ""); err != nil {
		t.Fatal(err)
	}
	if v := q.WithPrepared(true); v.cache != q.cache {
		t.Fatal("WithPrepared made a view with its own cache")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if v := q.WithTx(tx); v.cache != q.cache {
		t.Fatal("WithTx made a view with its own cache")
	}
	view := q.WithPrepared(true)
	for range 3 { // promote to Tier 2 through the view
		if _, err := view.GetUserByID(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	before := q.CacheStats().PreparedHits
	if _, err := q.GetUserByID(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if after := q.CacheStats().PreparedHits; after <= before {
		t.Fatalf("a statement promoted through a view is not a prepared hit for q: hits %d -> %d", before, after)
	}
}
