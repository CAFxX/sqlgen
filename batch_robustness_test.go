package sqlgen_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/CAFxX/sqlgen"
	"github.com/CAFxX/sqlgen/runtime"
)

// A panic raised by user code inside a row loop (range-over-func body, Each callback) unwinds through the
// generated iterator; the open *sql.Rows (which pins a connection) and the ManagedStmt reference must be
// released on that path too, not only on break/return.
func TestRowIteratorsReleaseResourcesWhenUserCodePanics(t *testing.T) {
	ctx := context.Background()
	ids := []int64{1, 2, 3, 4, 5}
	cases := []struct {
		name string
		f    func(q *Queries)
	}{
		{"Seq", func(q *Queries) {
			for range q.ListUsersByIDsSeq(ctx, ids) {
				panic("boom")
			}
		}},
		{"Each", func(q *Queries) {
			_ = q.ListUsersByIDsEach(ctx, ids, func(User) error { panic("boom") })
		}},
		{"BatchSeq", func(q *Queries) {
			for range q.ListUsersByIDsBatchSeq(ctx, ids) {
				panic("boom")
			}
		}},
		{"BatchEach", func(q *Queries) {
			_ = q.ListUsersByIDsBatchEach(ctx, ids, func(User) error { panic("boom") })
		}},
		{"StreamSeq", func(q *Queries) {
			rows := []BatchUserInput{{ID: 10, Name: "a", Points: 1}, {ID: 11, Name: "b", Points: 2}}
			for range q.InsertUsersBatchReturningStreamSeq(ctx, runtime.FromSlice(rows)) {
				panic("boom")
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh pool per case: leaked connections would otherwise starve later cases and hang.
			db, q := setupE2EDB(t)
			for i := int64(1); i <= 5; i++ {
				if _, err := q.CreateUser(ctx, i, 1, fmt.Sprintf("u%d@x", i), "member", i, "b"); err != nil {
					t.Fatal(err)
				}
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("expected the panic to propagate")
					}
				}()
				tc.f(q)
			}()
			if st := db.Stats(); st.InUse != 0 {
				t.Fatalf("%d connection(s) still in use after a panicking loop body (rows leaked)", st.InUse)
			}
			// The pool and the statement cache stay usable.
			if _, err := q.ListUsersByIDs(ctx, ids); err != nil {
				t.Fatalf("query after the panic: %v", err)
			}
		})
	}
}

// A parameter that appears several times in the SQL binds one argument per occurrence, so a chunk's size
// must be planned from the placeholder count, not from the number of declared parameters: with the bind
// budget raised to the engine's limit, a chunk sized for one `Score` overflowed SQLite's variable limit.
func TestBatchChunkBudgetCountsRepeatedParameterOccurrences(t *testing.T) {
	db, _ := setupE2EDB(t)
	q := New(db, runtime.WithMaxBatchParams(32766), runtime.WithPromoteThreshold(1))
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()
	if _, err := q.CreateUser(ctx, 7, 1, "a@x", "member", 10, "b"); err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 32765)
	for i := range ids {
		ids[i] = int64(i)
	}
	// Repeat so the exact-size chunk is promoted and planned at the largest size the budget allows.
	for i := 0; i < 3; i++ {
		got, err := q.UserIDsNearScoreBatch(ctx, 10, ids)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if len(got) != 1 || got[0].ID != 7 {
			t.Fatalf("call %d: got %+v, want the single user 7", i, got)
		}
	}
}

// The pooled chunk buffer of a Stream method must not keep the caller's rows (strings, byte slices,
// pointers) reachable after the stream ends: the flush leaves len(chunk) == 0, so clearing only the live
// length cleared nothing.
func TestStreamReleasesRowsHeldByPooledChunkBuffer(t *testing.T) {
	db, q := setupE2EDB(t)
	_ = db
	ctx := context.Background()
	observed := 0
	for i := 0; i < 50; i++ {
		rows := []QuadInput{{ID: int64(i*10 + 1), GroupKey: "retained-key", Priority: 1, CreatedTs: 1}, {ID: int64(i*10 + 2), GroupKey: "retained-key", Priority: 1, CreatedTs: 1}}
		if _, err := q.InsertQuadBatchStream(ctx, runtime.FromSlice(rows)); err != nil {
			t.Fatal(err)
		}
		bufPtr := quadInputChunkPool.Get().(*[]QuadInput)
		if cap(*bufPtr) > 0 {
			observed++
			for j, r := range (*bufPtr)[:cap(*bufPtr)] {
				if r != (QuadInput{}) {
					t.Fatalf("pooled chunk buffer still holds row %d: %+v", j, r)
				}
			}
		}
		quadInputChunkPool.Put(bufPtr)
	}
	if observed == 0 {
		t.Skip("sync.Pool never handed the buffer back")
	}
}

// A parameter named like a package-level identifier generated for its own statement (descFoo, stmtIDFoo,
// sqlDialect, ...) used to shadow it inside the statement's methods, so the generated file did not compile.
func TestParametersNamedLikeGeneratedIdentifiersStillCompile(t *testing.T) {
	names := []string{"desc_foo", "stmt_i_d_foo", "params_foo", "compute_foo_key", "expand_foo_params", "scan_foo_row", "sql_dialect"}
	shapes := map[string]string{
		"one":   "kind: one\n    sql: |\n      SELECT id:int64 FROM t WHERE a = :%[1]s:int64 AND b = :%[1]s:int64",
		"batch": "kind: many\n    sql: |\n      SELECT id:int64 FROM t WHERE a IN (:ids:[]int64 ASC) AND z = :%[1]s:string",
		"tuple": "kind: exec\n    sql: |\n      INSERT INTO t (a, b) VALUES :rows:[]Row(A:int64 ASC, B:string) -- :%[1]s:int64",
	}
	for _, name := range names {
		for shape, body := range shapes {
			for _, ps := range []bool{false, true} {
				y := fmt.Sprintf("package: p\ndialect: postgres\nstatements:\n  - name: Foo\n    params_struct: %v\n    %s\n", ps, fmt.Sprintf(body, name))
				src, err := sqlgen.GenerateFromYAML([]byte(y))
				if err != nil {
					t.Fatalf("%s/%s ps=%v: %v", name, shape, ps, err)
				}
				typeCheckGenerated(t, name+"_"+shape, src)
			}
		}
	}
}
