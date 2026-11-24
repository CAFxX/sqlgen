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

		cols, desc, count, _ := expandOrder(spec, n)
		if count != n {
			t.Fatalf("n=%d: expandOrder kept %d terms, want %d", n, count, n)
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
			if want := seekArgCount(dialect, spec, n, true); len(args) != want {
				t.Fatalf("n=%d dialect=%v: %d seek args, seekArgCount says %d", n, dialect, len(args), want)
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

// Documented defaults (DOCS.md 2.6): no Order falls back to the last listed column ascending, a partial
// Order appends that column with the direction of the last term given, and a first page renders TRUE.
func TestOrderDefaultsAndFirstPageSeek(t *testing.T) {
	d := &QueryDescriptor{
		StmtID:    1,
		Segments:  []string{"SELECT 1 FROM t WHERE ", " ORDER BY ", ""},
		Slots:     []Slot{{Kind: SlotSeek}, {Kind: SlotOrder}},
		OrderCols: []string{"a", "b", "id"},
	}
	for _, tc := range []struct {
		name string
		spec uint64
		want string
	}{
		{"zero Order", 0, "id ASC"},
		{"partial ascending", PackOrder(Asc(1)), "b ASC, id ASC"},
		{"partial descending", PackOrder(Desc(0)), "a DESC, id DESC"},
		{"partial mixed: last given term decides", PackOrder(Desc(0), Asc(1)), "a DESC, b ASC, id ASC"},
		{"partial mixed, descending last", PackOrder(Asc(0), Desc(1)), "a ASC, b DESC, id DESC"},
		{"tie-break named explicitly", PackOrder(Desc(0), Asc(2)), "a DESC, id ASC"},
	} {
		for _, dialect := range []Dialect{DialectDollar, DialectQuestion} {
			sql := BuildComposedSQL(d, &RenderInput{OrderSpec: tc.spec}, dialect)
			if want := "SELECT 1 FROM t WHERE TRUE ORDER BY " + tc.want; sql != want {
				t.Errorf("%s dialect=%v: got %q, want %q", tc.name, dialect, sql, want)
			}
		}
	}
}

func TestMaxRowsPerBatchDefaultsAndWireLimits(t *testing.T) {
	c := NewStmtCache()
	defer func() { _ = c.Close() }()
	// Default budget (defaultMaxBatchParams) binds on both placeholder styles.
	if got := c.MaxRowsPerBatch(DialectDollar, 0, 2); got != defaultMaxBatchParams/2 {
		t.Errorf("dollar default = %d", got)
	}
	if got := c.MaxRowsPerBatch(DialectQuestion, 0, 2); got != defaultMaxBatchParams/2 {
		t.Errorf("question default = %d", got)
	}
	// A larger budget is clamped to the wire limit of the placeholder style.
	big := NewStmtCache(WithMaxBatchParams(1 << 20))
	defer func() { _ = big.Close() }()
	if got := big.MaxRowsPerBatch(DialectDollar, 0, 2); got != 65535/2 {
		t.Errorf("dollar clamped = %d, want %d", got, 65535/2)
	}
	if got := big.MaxRowsPerBatch(DialectQuestion, 0, 2); got != 32766/2 {
		t.Errorf("question clamped = %d, want %d", got, 32766/2)
	}
}

type testTerm uint8 // stands in for a generated per-statement <Stmt>SortTerm

func TestPackOrderMarksInvalidTermLists(t *testing.T) {
	valid := [][]testTerm{
		nil,
		{testTerm(Asc(0))},
		{testTerm(Desc(3)), testTerm(Asc(0))},
	}
	for _, terms := range valid {
		if !OrderValid(PackOrder(terms...)) {
			t.Errorf("%v must be valid", terms)
		}
	}
	all := make([]testTerm, MaxOrderCols)
	for i := range all {
		all[i] = testTerm(Asc(uint8(i)))
	}
	if !OrderValid(PackOrder(all...)) {
		t.Error("every column named once must be valid")
	}
	for name, terms := range map[string][]testTerm{
		"duplicate column":        {testTerm(Asc(0)), testTerm(Desc(0))},
		"zero term":               {0},
		"beyond MaxOrderCols":     {testTerm(Asc(MaxOrderCols))},
		"valid then out of range": {testTerm(Asc(0)), testTerm(Desc(60))},
	} {
		if OrderValid(PackOrder(terms...)) {
			t.Errorf("%s: must be invalid", name)
		}
	}
	if !OrderValid(0) {
		t.Error("the zero Order must be valid")
	}
	// The invalid flag must not disturb the terms that were kept.
	cols, _, count, _ := expandOrder(PackOrder(testTerm(Asc(1)), testTerm(Desc(1)), testTerm(Desc(0))), 3)
	if count != 3 || cols[0] != 1 || cols[1] != 0 || cols[2] != 2 {
		t.Errorf("expandOrder over an invalid spec: cols=%v count=%d", cols[:count], count)
	}
}

func TestPackOrderOfTypedTermsAllocatesNothing(t *testing.T) {
	a, b := testTerm(Asc(0)), testTerm(Desc(1))
	if n := testing.AllocsPerRun(100, func() { _ = PackOrder(a, b) }); n != 0 {
		t.Fatalf("PackOrder allocated %.1f objects", n)
	}
}
