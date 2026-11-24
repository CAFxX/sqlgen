package runtime

import "testing"

func TestWithCacheSharesTheGivenCache(t *testing.T) {
	c := NewStmtCache(WithMaxSQLVariants(64))
	if got := NewStmtCache(WithCache(c), WithMaxSQLVariants(1)); got != c {
		t.Fatal("WithCache must return the given cache, ignoring the other options")
	}
	if got := NewStmtCache(WithCache(nil)); got == nil || got == c {
		t.Fatal("WithCache(nil) must be ignored and create a new cache")
	}
}
