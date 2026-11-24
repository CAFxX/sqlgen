package sqlgen_test

import (
	"context"
	"slices"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// A one/many statement with exactly one output column and no result_type returns the column's value itself
// (including []byte and pointer types); result_type only names a row struct, even for one column.
func TestSingleColumnStatementsReturnTheValue(t *testing.T) {
	ctx := context.Background()
	_, q := setupE2EDB(t)
	for _, u := range []struct {
		id  int64
		bio string
	}{{1, ""}, {2, "set"}, {3, ""}} {
		if _, err := q.CreateUser(ctx, u.id, 1, "u@x", "member", u.id, u.bio); err != nil {
			t.Fatal(err)
		}
	}

	raw, err := q.UserEmailBytes(ctx, 2) // one, []byte
	if err != nil || string(raw) != "u@x" {
		t.Fatalf("UserEmailBytes = %q, %v", raw, err)
	}
	bios, err := q.UserBioIfSet(ctx) // many, *string
	if err != nil || len(bios) != 3 || bios[0] != nil || bios[2] != nil || bios[1] == nil || *bios[1] != "set" {
		t.Fatalf("UserBioIfSet = %v, %v", bios, err)
	}
	var seq []int64
	for id, err := range q.UserIDsNearScoreSeq(ctx, 2, []int64{1, 2, 3}) { // many, int64, iterator form
		if err != nil {
			t.Fatal(err)
		}
		seq = append(seq, id)
	}
	if !slices.Equal(seq, []int64{1, 2}) && !slices.Equal(seq, []int64{2, 1}) {
		t.Fatalf("UserIDsNearScoreSeq = %v", seq)
	}
	ids, err := q.UserIDsNearScore(ctx, 2, []int64{1, 2, 3}) // many, int64
	if err != nil || len(ids) != 2 {
		t.Fatalf("UserIDsNearScore = %v, %v", ids, err)
	}
	// result_type: a struct, even for a single column.
	row, err := q.UserEmailOnly(ctx, 2)
	if err != nil || row != (EmailOnly{Email: "u@x"}) {
		t.Fatalf("UserEmailOnly = %+v, %v", row, err)
	}
	// Pipelines resolve to the value too.
	pl := q.Pipeline(ctx)
	fOne := pl.UserEmailBytes(2)
	fMany := pl.UserBioIfSet()
	if err := pl.Flush(); err != nil {
		t.Fatal(err)
	}
	if v, err := fOne.Get(); err != nil || string(v) != "u@x" {
		t.Fatalf("pipeline one = %q, %v", v, err)
	}
	if v, err := fMany.Get(); err != nil || len(v) != 3 {
		t.Fatalf("pipeline many = %v, %v", v, err)
	}
	// The value form composes with the runtime helpers.
	var got []int64
	if err := runtime.Each(q.UserIDsNearScoreSeq(ctx, 3, []int64{3}), func(id int64) error { got = append(got, id); return nil }); err != nil || !slices.Equal(got, []int64{3}) {
		t.Fatalf("Each over scalars = %v, %v", got, err)
	}
}
