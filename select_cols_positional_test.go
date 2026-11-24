package sqlgen_test

import (
	"context"
	"testing"
)

// The SelectCols row projection is available in both argument styles: a struct-style statement carries the
// mask in the SelectCols field, a positional one takes the column constants as a trailing variadic argument
// (no argument: every column). These tests run the positional form against SQLite.
func TestPositionalSelectColsProjectsColumns(t *testing.T) {
	ctx := context.Background()
	_, q := setupE2EDB(t)
	q = q.WithPrepared(true) // exercises the fast slot of the statement, which a projected call must not reuse
	for id := int64(1); id <= 3; id++ {
		if _, err := q.CreateUser(ctx, id, 10+id, "u@x", "member", 100*id, "bio"); err != nil {
			t.Fatal(err)
		}
	}
	full := User{ID: 2, OrgID: 12, Email: "u@x", Role: "member", Score: 200, Bio: "bio"}

	// Interleave default and projected calls several times so a cached default statement would be reused
	// for a projected call (and the reverse) if the fast slot were not bypassed.
	for range 4 {
		if got, err := q.GetUserByID(ctx, 2); err != nil || got != full {
			t.Fatalf("no selection = %+v, %v; want %+v", got, err, full)
		}
		got, err := q.GetUserByID(ctx, 2, GetUserByIDColID, GetUserByIDColEmail)
		if want := (User{ID: 2, Email: "u@x"}); err != nil || got != want {
			t.Fatalf("id+email = %+v, %v; want %+v", got, err, want)
		}
		// Arguments combine: several constants and one OR-ed mask select the same columns.
		got2, err := q.GetUserByID(ctx, 2, GetUserByIDColID|GetUserByIDColEmail)
		if err != nil || got2 != got {
			t.Fatalf("OR-ed mask = %+v, %v; want %+v", got2, err, got)
		}
	}
	// Every column named explicitly is the default projection.
	if got, err := q.GetUserByID(ctx, 2, GetUserByIDColsAll); err != nil || got != full {
		t.Fatalf("all columns = %+v, %v; want %+v", got, err, full)
	}
	// Bits outside the statement's columns select nothing extra: the mask falls back to every column.
	if got, err := q.GetUserByID(ctx, 2, 1<<40); err != nil || got != full {
		t.Fatalf("out-of-range mask = %+v, %v; want %+v", got, err, full)
	}

	rows, err := q.ListUsersByIDs(ctx, []int64{3, 1}, ListUsersByIDsColID, ListUsersByIDsColScore)
	if err != nil || len(rows) != 2 || rows[0] != (User{ID: 1, Score: 100}) || rows[1] != (User{ID: 3, Score: 300}) {
		t.Fatalf("ListUsersByIDs projected = %+v, %v", rows, err)
	}
	var seqRows []User
	for u, err := range q.ListUsersByIDsSeq(ctx, []int64{2}, ListUsersByIDsColRole) {
		if err != nil {
			t.Fatal(err)
		}
		seqRows = append(seqRows, u)
	}
	if len(seqRows) != 1 || seqRows[0] != (User{Role: "member"}) {
		t.Fatalf("ListUsersByIDsSeq projected = %+v", seqRows)
	}

	// Pipeline form and composable query form take the same trailing argument.
	pl := q.Pipeline(ctx)
	fut := pl.GetUserByID(3, GetUserByIDColOrgID)
	futAll := pl.GetUserByID(3)
	futMany := pl.ListUsersByIDs([]int64{1, 2}, ListUsersByIDsColBio)
	if err := pl.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, err := fut.Get(); err != nil || got != (User{OrgID: 13}) {
		t.Fatalf("pipeline projected = %+v, %v", got, err)
	}
	if got, err := futAll.Get(); err != nil || got.Email != "u@x" || got.ID != 3 {
		t.Fatalf("pipeline default = %+v, %v", got, err)
	}
	if got, err := futMany.Get(); err != nil || len(got) != 2 || got[0] != (User{Bio: "bio"}) {
		t.Fatalf("pipeline many projected = %+v, %v", got, err)
	}
	if qa, qb := q.GetUserByIDQuery(1), q.GetUserByIDQuery(1, GetUserByIDColID); qa.ShapeTag == qb.ShapeTag {
		t.Fatal("projected and default queries share a shape tag")
	}
}

// The trailing variadic must not allocate: the empty call and the projected call cost the same.
func TestPositionalSelectColsAddsNoAllocation(t *testing.T) {
	ctx := context.Background()
	_, q := setupE2EDB(t)
	if _, err := q.CreateUser(ctx, 1, 1, "u@x", "member", 1, "b"); err != nil {
		t.Fatal(err)
	}
	get := func(cols ...uint64) float64 {
		return testing.AllocsPerRun(50, func() {
			if _, err := q.GetUserByID(ctx, 1, cols...); err != nil {
				t.Fatal(err)
			}
		})
	}
	base := get()
	projected := testing.AllocsPerRun(50, func() {
		if _, err := q.GetUserByID(ctx, 1, GetUserByIDColID, GetUserByIDColEmail); err != nil {
			t.Fatal(err)
		}
	})
	// A narrower scan never costs more than the full one: no heap slice for the variadic arguments.
	if projected > base {
		t.Fatalf("projected call %v allocs, default call %v", projected, base)
	}
	if spread := get(GetUserByIDColID, GetUserByIDColEmail); spread > base {
		t.Fatalf("spread call %v allocs, default call %v", spread, base)
	}
}
