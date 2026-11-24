package sqlgen_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// Allocation budget of a generated many call against hand-written database/sql (in-process SQLite: what is
// measured is the library's own overhead, not the engine). AppendTo over <Name>Seq into a reused buffer is
// the allocation-free-of-slice-growth way to read; Collect/<Name> allocate the result slice.
func benchManySetup(b *testing.B) (*Queries, *sql.DB, []int64) {
	b.Helper()
	db, q := setupE2EDB(b)
	ctx := context.Background()
	ids := make([]int64, 50)
	for i := range ids {
		ids[i] = int64(i + 1)
		if _, err := q.CreateUser(ctx, ids[i], 1, "u@example.com", "member", int64(i), "bio"); err != nil {
			b.Fatal(err)
		}
	}
	return q, db, ids
}

func BenchmarkManyGeneratedAppendTo(b *testing.B) {
	q, _, ids := benchManySetup(b)
	ctx := context.Background()
	buf := make([]User, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		if buf, err = runtime.AppendTo(buf[:0], q.ListUsersByIDsSeq(ctx, ids)); err != nil || len(buf) != 50 {
			b.Fatal(len(buf), err)
		}
	}
}

func BenchmarkManyGeneratedSlice(b *testing.B) {
	q, _, ids := benchManySetup(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if rows, err := q.ListUsersByIDs(ctx, ids); err != nil || len(rows) != 50 {
			b.Fatal(len(rows), err)
		}
	}
}

func BenchmarkManyHandWritten(b *testing.B) {
	_, db, ids := benchManySetup(b)
	ctx := context.Background()
	query := "SELECT id, org_id, email, role, score, bio FROM users WHERE id IN (?" + string(repeatComma(len(ids)-1)) + ") ORDER BY id ASC"
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	buf := make([]User, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		buf = buf[:0]
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var u User
			if err := rows.Scan(&u.ID, &u.OrgID, &u.Email, &u.Role, &u.Score, &u.Bio); err != nil {
				b.Fatal(err)
			}
			buf = append(buf, u)
		}
		if err := rows.Err(); err != nil || len(buf) != 50 {
			b.Fatal(len(buf), err)
		}
		_ = rows.Close()
	}
}

func repeatComma(n int) []byte {
	out := make([]byte, 0, 2*n)
	for range n {
		out = append(out, ',', '?')
	}
	return out
}

// Pins the allocation budget of the generated read path: reading 50 rows through AppendTo over <Name>Seq into
// a reused buffer costs about one allocation per row more than hand-written database/sql (425 vs 374 at the
// time of writing), and the slice form only adds the result slice.
func TestManyCallAllocationBudget(t *testing.T) {
	db, q := setupE2EDB(t)
	ctx := context.Background()
	ids := make([]int64, 50)
	args := make([]any, len(ids))
	for i := range ids {
		ids[i] = int64(i + 1)
		args[i] = ids[i]
		if _, err := q.CreateUser(ctx, ids[i], 1, "u@example.com", "member", int64(i), "bio"); err != nil {
			t.Fatal(err)
		}
	}
	query := "SELECT id, org_id, email, role, score, bio FROM users WHERE id IN (?" + string(repeatComma(len(ids)-1)) + ") ORDER BY id ASC"
	buf := make([]User, 0, 64)
	hand := testing.AllocsPerRun(20, func() {
		buf = buf[:0]
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var u User
			if err := rows.Scan(&u.ID, &u.OrgID, &u.Email, &u.Role, &u.Score, &u.Bio); err != nil {
				t.Fatal(err)
			}
			buf = append(buf, u)
		}
		_ = rows.Close()
	})
	appendTo := testing.AllocsPerRun(20, func() {
		var err error
		if buf, err = runtime.AppendTo(buf[:0], q.ListUsersByIDsSeq(ctx, ids)); err != nil || len(buf) != 50 {
			t.Fatal(len(buf), err)
		}
	})
	slice := testing.AllocsPerRun(20, func() {
		if rows, err := q.ListUsersByIDs(ctx, ids); err != nil || len(rows) != 50 {
			t.Fatal(len(rows), err)
		}
	})
	if appendTo > hand+60 {
		t.Errorf("AppendTo over Seq: %.0f allocs/op, hand-written %.0f: more than 60 above the baseline overhead", appendTo, hand)
	}
	if slice > appendTo+12 {
		t.Errorf("slice form: %.0f allocs/op, AppendTo into a reused buffer %.0f: the slice form may only add the growth of the result slice", slice, appendTo)
	}
}
