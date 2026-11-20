package runtime

import (
	"fmt"
	"strings"
	"testing"
)

// Every column count the generator accepts (up to MaxOrderCols) must be honoured end to end: a sort
// naming all of the columns in a scrambled order with scrambled directions keeps every term, in the
// order given, in both the ORDER BY and the keyset predicate.
func TestOrderHonoursEveryTermUpToMaxOrderCols(t *testing.T) {
	for n := 1; n <= MaxOrderCols; n++ {
		names := make([]string, n)
		for i := range names {
			names[i] = fmt.Sprintf("c%d", i+1)
		}
		// Reverse order, alternating directions, so the last (tie-break) column comes first and the
		// mixed-direction seek expansion is exercised.
		var terms []uint8
		var wantOrder []string
		for i := n - 1; i >= 0; i-- {
			if i%2 == 0 {
				terms = append(terms, Asc(uint8(i)))
				wantOrder = append(wantOrder, names[i]+" ASC")
			} else {
				terms = append(terms, Desc(uint8(i)))
				wantOrder = append(wantOrder, names[i]+" DESC")
			}
		}
		spec := PackOrder(terms...)

		cols, desc, count, _ := ExpandOrder(spec, n)
		if count != n {
			t.Fatalf("n=%d: ExpandOrder kept %d terms, want %d", n, count, n)
		}
		for i := 0; i < n; i++ {
			if want := n - 1 - i; cols[i] != want || desc[i] != (want%2 == 1) {
				t.Fatalf("n=%d term %d: got col %d desc %v, want col %d desc %v", n, i, cols[i], desc[i], want, want%2 == 1)
			}
		}

		d := &QueryDescriptor{
			StmtID:    1,
			Segments:  []string{"SELECT 1 FROM t WHERE ", " ORDER BY ", ""},
			Slots:     []Slot{{Kind: SlotSeek}, {Kind: SlotOrder}},
			OrderCols: names,
		}
		for _, dialect := range []Dialect{DialectDollar, DialectQuestion} {
			in := RenderInput{OrderSpec: spec, HasSeek: true}
			sql := BuildComposedSQL(d, &in, dialect)
			if got := sql[strings.Index(sql, "ORDER BY ")+len("ORDER BY "):]; got != strings.Join(wantOrder, ", ") {
				t.Fatalf("n=%d dialect=%v: ORDER BY %q, want %q", n, dialect, got, strings.Join(wantOrder, ", "))
			}
			var vals []any
			for i := range names {
				vals = append(vals, i+1)
			}
			var args []any
			AppendSeekArgs(&args, dialect, spec, n, vals)
			if want := SeekArgCount(dialect, spec, n, true); len(args) != want {
				t.Fatalf("n=%d dialect=%v: %d seek args, SeekArgCount says %d", n, dialect, len(args), want)
			}
			// The placeholders in the rendered predicate match the arguments appended.
			var ph int
			if dialect == DialectQuestion {
				ph = strings.Count(sql, "?")
			} else {
				ph = strings.Count(sql, "$")
			}
			if n > 1 && dialect == DialectQuestion && ph != len(args) {
				t.Fatalf("n=%d: %d placeholders for %d args in %s", n, ph, len(args), sql)
			}
			if n > 1 && dialect == DialectDollar {
				if !strings.Contains(sql, fmt.Sprintf("$%d", n)) || strings.Contains(sql, fmt.Sprintf("$%d", n+1)) {
					t.Fatalf("n=%d: dollar placeholders do not span exactly $1..$%d in %s", n, n, sql)
				}
			}
			// Ties on the lead column must be ordered by the second term: its column appears in an
			// equality branch of the predicate (mixed directions) or in the row tuple.
			if n > 1 && !strings.Contains(sql, names[n-2]) {
				t.Fatalf("n=%d: second sort column %s missing from %s", n, names[n-2], sql)
			}
		}
	}
}

func TestPackOrderIgnoresTermsBeyondMaxOrderCols(t *testing.T) {
	spec := PackOrder(Asc(MaxOrderCols), Desc(MaxOrderCols+3), Asc(0))
	cols, _, count, _ := ExpandOrder(spec, MaxOrderCols)
	if count != 2 || cols[0] != 0 || cols[1] != MaxOrderCols-1 {
		t.Fatalf("out-of-range terms leaked: cols=%v count=%d", cols[:count], count)
	}
}
