package runtime

import "testing"

// Test-only knobs: the production API exposes neither (the values are fixed), but tests shrink the
// Tier-1 byte budget and the promotion threshold to reach eviction and promotion quickly.

func withMaxPartialBytes(bytes int) CacheOption {
	return func(cfg *cacheConfig) {
		cfg.MaxPartialBytes = bytes
	}
}

func withPromoteThreshold(threshold uint8) CacheOption {
	return func(cfg *cacheConfig) {
		cfg.PromoteThreshold = min(max(threshold, 1), 15)
	}
}

// The per-statement row limit follows the parameter budget up to the wire limit of the placeholder style: a
// collection that fits one statement under the limit is one statement (DOCS section 3.2). A leftover
// 32768-row cap split a 40000-id $N IN list into two chunks (and an implicit transaction) although the
// configured budget of 65535 parameters allowed one statement.
func TestMaxRowsPerBatchFollowsTheBudgetToTheWireLimit(t *testing.T) {
	c := NewStmtCache(WithMaxBatchParams(65535))
	for _, tc := range []struct {
		dialect        Dialect
		scalars, arity int
		want           int
	}{
		{DialectDollar, 0, 1, 65535},
		{DialectDollar, 2, 1, 65533},
		{DialectDollar, 1, 2, 32767},
		{DialectQuestion, 0, 1, 32766},
		{DialectQuestion, 0, 3, 10922},
	} {
		if got := c.MaxRowsPerBatch(tc.dialect, tc.scalars, tc.arity); got != tc.want {
			t.Errorf("MaxRowsPerBatch(%v, %d, %d) = %d, want %d", tc.dialect, tc.scalars, tc.arity, got, tc.want)
		}
	}
	if got := c.PlanBatchChunk(1, 0, 0, 40000, c.MaxRowsPerBatch(DialectDollar, 0, 1), true); got != 40000 {
		t.Errorf("40000 rows under a 65535-parameter limit: first chunk %d, want the whole collection", got)
	}
}
