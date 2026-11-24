package sqlgen_test

import (
	"context"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// runtime.WithCache shares a cache between Queries of the same package and *sql.DB. Shared with Queries on
// another *sql.DB, a statement promoted to Tier 2 by one of them was run by the other through the *sql.Stmt
// prepared on the first database (reads came from the wrong database): New now refuses such a cache.
func TestWithCacheSharing(t *testing.T) {
	ctx := context.Background()
	db1, q1 := setupE2EDB(t)
	db2, _ := setupE2EDB(t)
	if _, err := q1.CreateUser(ctx, 1, 1, "db1@x", "member", 1, ""); err != nil {
		t.Fatal(err)
	}

	// Same package, same database: shared, and both see the same data.
	q1b := New(db1, runtime.WithCache(q1.Cache()))
	if q1b.Cache() != q1.Cache() {
		t.Fatal("WithCache did not share the cache")
	}
	for range 3 { // promote to Tier 2 through q1, then use it through q1b
		if u, err := q1.GetUserByID(ctx, 1); err != nil || u.Email != "db1@x" {
			t.Fatalf("q1: %+v, %v", u, err)
		}
		if u, err := q1b.GetUserByID(ctx, 1); err != nil || u.Email != "db1@x" {
			t.Fatalf("q1b: %+v, %v", u, err)
		}
	}

	// Another database: refused.
	func() {
		defer func() {
			if r, _ := recover().(string); !strings.Contains(r, "WithCache") {
				t.Fatalf("New on another *sql.DB with a shared cache: recovered %q, want a WithCache panic", r)
			}
		}()
		_ = New(db2, runtime.WithCache(q1.Cache()))
	}()
	_ = db2
}
