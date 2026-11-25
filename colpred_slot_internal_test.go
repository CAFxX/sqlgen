package sqlgen

import (
	"errors"
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
	if len(kinds) < 2 || kinds[0] != runtime.SlotOptBlock || kinds[1] != runtime.SlotColPreds || !strings.HasPrefix(cs.Root.Segments[2], "FROM") {
		t.Fatalf("slots %v, segments %q", kinds, cs.Root.Segments)
	}
}

// On the MySQL flavor a gated main SELECT carries its gates in its own HAVING (no derived table, whose
// ORDER BY MariaDB and TiDB drop); where that is not possible an ordered statement is not applicable
// (sequential path) and an unordered one is wrapped.
func TestSelectGuardPlacement(t *testing.T) {
	cases := []struct {
		sql  string
		want string // the operation's SQL contains it; "wrap": derived table; "seq": ErrWireNotApplicable
	}{
		{"SELECT id:int64 FROM t WHERE a = :A:int64 ORDER BY id DESC", "WHERE a = 7 HAVING @sqlgen_c1=1 ORDER BY id DESC;"},
		{"SELECT k:int64, SUM(v) AS s:int64 FROM t GROUP BY k HAVING SUM(v) > :A:int64 ORDER BY s", "GROUP BY k HAVING (@sqlgen_c1=1) AND (SUM(v) > 7 ) ORDER BY s;"},
		{"SELECT k:int64 FROM t GROUP BY k HAVING COUNT(*) > :A:int64", "HAVING (@sqlgen_c1=1) AND (COUNT(*) > 7) ;"},
		{"SELECT id:int64 FROM t WHERE t.limit = :A:int64 ORDER BY id LIMIT 3 FOR UPDATE", "WHERE t.limit = 7 HAVING @sqlgen_c1=1 ORDER BY id LIMIT 3 FOR UPDATE;"},
		{"SELECT id:int64 FROM t WHERE a = :A:int64 WINDOW w AS (ORDER BY id) ORDER BY id", "= 7 HAVING @sqlgen_c1=1 WINDOW w AS (ORDER BY id) ORDER BY id;"},
		{"SELECT COUNT(*) AS n:int64 FROM t WHERE a = :A:int64", "FROM t WHERE a = 7 HAVING @sqlgen_c1=1 ;"},
		{"SELECT :A:int64 AS one:int64", "SELECT 7 AS one HAVING @sqlgen_c1=1 ;"},
		{"SELECT id:int64 FROM (SELECT id FROM u ORDER BY id LIMIT 2) AS x WHERE id > :A:int64 ORDER BY id", "WHERE id > 7 HAVING @sqlgen_c1=1 ORDER BY id;"},
		{"SELECT id:int64 FROM t WHERE a = :A:int64 UNION SELECT id FROM u", "wrap"},
		{"SELECT id:int64 FROM t WHERE a = :A:int64 {?Lim: LIMIT 3}", "wrap"},
		{"SELECT id:int64 FROM t WHERE a = :A:int64 UNION SELECT id FROM u ORDER BY id", "seq"},
		{"SELECT id:int64 FROM t WHERE a = :A:int64 {?Ord: ORDER BY id}", "seq"},
		{"SELECT k:int64 FROM t GROUP BY k {?H: HAVING COUNT(*) > 1} ORDER BY k LIMIT :A:int64", "seq"},
		{"SELECT id:int64 FROM t WHERE offset > :A:int64", "wrap"},
		{"SELECT id:int64 FROM t WHERE a = :A:int64 ORDER BY id OFFSET 2 ROWS", "= 7 HAVING @sqlgen_c1=1 ORDER BY id OFFSET 2 ROWS;"},
		{"SELECT id:int64 FROM t WHERE a = :A:int64 OFFSET 2 ROWS", "wrap"},
	}
	for _, c := range cases {
		cs, err := compileStatement(1, StatementSpec{Name: "S", Kind: KindMany, SQL: c.sql}, stmtDialect{known: true, style: runtime.DialectQuestion, family: "mysql"})
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		desc := buildRuntimeDescriptor(cs, &cs.Root)
		ops := []runtime.PipelineOp{{Desc: desc, HasResultCols: true, Args: []any{int64(7)},
			Input:      runtime.RenderInput{BranchGates: []runtime.BranchGate{{CondID: 1, WantTrue: true}}},
			Decorators: runtime.OpDecorators{AssertRows: -1}}}
		_, err = runtime.RenderMySQLBatch(ops, runtime.MySQLRenderOptions{Flavor: &runtime.MySQLFlavor{}})
		switch c.want {
		case "seq":
			if !errors.Is(err, runtime.ErrWireNotApplicable) {
				t.Errorf("%s: err = %v, want ErrWireNotApplicable", c.sql, err)
			}
			continue
		case "wrap":
			if err != nil || !strings.Contains(ops[0].SQL, ") AS _sqlgen_if WHERE @sqlgen_c1=1") {
				t.Errorf("%s: want a derived table, got %v:\n%s", c.sql, err, ops[0].SQL)
			}
			continue
		}
		if err != nil || !strings.Contains(ops[0].SQL, c.want) || strings.Contains(ops[0].SQL, "_sqlgen_if") {
			t.Errorf("%s: err %v\n got %s\nwant it to contain %q", c.sql, err, ops[0].SQL, c.want)
		}
	}
}
