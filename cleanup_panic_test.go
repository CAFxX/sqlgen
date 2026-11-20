package sqlgen_test

import (
	"context"
	"fmt"
	"testing"

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
