package runtime

import (
	"database/sql"
	"strings"
	"testing"
)

func TestWithCacheSharesTheGivenCache(t *testing.T) {
	c := NewStmtCache(WithMaxSQLVariants(64))
	if got := NewStmtCache(WithCache(c), WithMaxSQLVariants(1)); got != c {
		t.Fatal("WithCache must return the given cache, ignoring the other options")
	}
	if got := NewStmtCache(WithCache(nil)); got == nil || got == c {
		t.Fatal("WithCache(nil) must be ignored and create a new cache")
	}
}

// A cache is bound to the generated package and *sql.DB of the Queries that created it: statements are cached
// under package-local ids and Tier-2 statements are prepared on one *sql.DB, so sharing it with a Queries of
// another package or database would run another package's SQL, or a statement prepared on another database.
// NewQueriesCache refuses that loudly instead.
func TestNewQueriesCacheBindsPackageAndDB(t *testing.T) {
	var pkgA, pkgB byte
	db1, db2 := new(sql.DB), new(sql.DB)
	c := NewQueriesCache(db1, &pkgA)
	if got := NewQueriesCache(db1, &pkgA, WithCache(c)); got != c {
		t.Fatal("same package and database: the cache must be shared")
	}
	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("%s: no panic", name)
			} else if s, _ := r.(string); !strings.Contains(s, "WithCache") {
				t.Errorf("%s: panic %v does not name WithCache", name, r)
			}
		}()
		fn()
	}
	mustPanic("other package", func() { NewQueriesCache(db1, &pkgB, WithCache(c)) })
	mustPanic("other database", func() { NewQueriesCache(db2, &pkgA, WithCache(c)) })
	// A cache made by NewStmtCache has no owner yet: the first Queries using it binds it.
	free := NewStmtCache()
	if got := NewQueriesCache(db2, &pkgB, WithCache(free)); got != free {
		t.Fatal("an unbound cache must be adopted")
	}
	mustPanic("adopted cache, other database", func() { NewQueriesCache(db1, &pkgB, WithCache(free)) })
}
