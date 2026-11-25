package sqlgen

import (
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// SlotColPreds (where WhereCol columns go) sits at the end of the statement's own output column list,
// after any optional column, and is absent where a column cannot be added without changing the rows.
func TestColPredSlotPlacement(t *testing.T) {
	const wc = ", CASE WHEN (x) THEN 1 ELSE 0 END AS _sqlgen_wc1 "
	cases := []struct {
		sql  string
		want string // "" when the statement must not have the slot
	}{
		{"SELECT id:int64, name:string FROM t WHERE id = :ID:int64", "SELECT id, name " + wc + "FROM t WHERE id = $1"},
		{"SELECT id:int64 FROM t ORDER BY id LIMIT 3", "SELECT id " + wc + "FROM t ORDER BY id LIMIT 3"},
		{"SELECT 1 AS one:int64", "SELECT 1 AS one" + wc},
		{"UPDATE t SET a = 1 WHERE id = :ID:int64 RETURNING id:int64, a:int64", "UPDATE t SET a = 1 WHERE id = $1 RETURNING id, a" + wc},
		{"INSERT INTO t (a) SELECT a FROM u RETURNING id:int64", "INSERT INTO t (a) SELECT a FROM u RETURNING id" + wc},
		{"WITH c AS (SELECT id FROM t UNION SELECT id FROM u) SELECT id:int64 FROM c", "WITH c AS (SELECT id FROM t UNION SELECT id FROM u) SELECT id " + wc + "FROM c"},
		{"SELECT id:int64 FROM t WHERE a IS DISTINCT FROM b", "SELECT id " + wc + "FROM t WHERE a IS DISTINCT FROM b"},
		{"SELECT DISTINCT id:int64 FROM t", ""},
		{"SELECT DISTINCT ON (a) id:int64 FROM t", ""},
		{"SELECT id:int64 FROM t UNION ALL SELECT id FROM u", ""},
		{"SELECT id:int64 FROM t EXCEPT SELECT id FROM u", ""},
	}
	for _, c := range cases {
		cs, err := compileStatement(1, StatementSpec{Name: "S", Kind: KindMany, SQL: c.sql}, stmtDialect{known: true, style: runtime.DialectDollar, family: "postgres"})
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		desc := buildRuntimeDescriptor(cs, &cs.Root)
		in := runtime.RenderInput{FlagMask: ^uint64(0), ColPreds: []runtime.ColPred{{SQL: "x"}}}
		got := runtime.BuildComposedSQL(desc, &in, runtime.DialectDollar)
		hasSlot := false
		for _, s := range desc.Slots {
			hasSlot = hasSlot || s.Kind == runtime.SlotColPreds
		}
		if c.want == "" {
			if hasSlot || strings.Contains(got, "_sqlgen_wc") {
				t.Errorf("%s: must not take a WhereCol column:\n%s", c.sql, got)
			}
			continue
		}
		if got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.sql, got, c.want)
		}
	}
	// after an optional output column
	cs, err := compileStatement(1, StatementSpec{Name: "S", Kind: KindMany, SQL: "SELECT id:int64 {?Named: , name:string} FROM t"}, stmtDialect{known: true, style: runtime.DialectDollar, family: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []runtime.SlotKind
	for _, s := range cs.Root.Slots {
		kinds = append(kinds, s.Kind)
	}
	if len(kinds) != 2 || kinds[0] != runtime.SlotOptBlock || kinds[1] != runtime.SlotColPreds || !strings.HasPrefix(cs.Root.Segments[2], "FROM") {
		t.Fatalf("slots %v, segments %q", kinds, cs.Root.Segments)
	}
}
