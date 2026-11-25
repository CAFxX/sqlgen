package sqlgen

import (
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// TestDecomposabilityClassification pins the lexical rule that decides whether a statement with one
// collection may be chunked (DOCS section 3.2): only when its result is the concatenation of the per-chunk
// results. When unsure the answer is "no".
func TestDecomposabilityClassification(t *testing.T) {
	for _, tc := range []struct {
		kind QueryKind
		sql  string
		want bool
	}{
		// Decomposable.
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64)`, true},
		{KindMany, `SELECT id:int64, name:string FROM t WHERE org = :org:int64 AND id IN (:ids:[]int64 ASC) AND x > 1`, true},
		{KindMany, `SELECT id:int64 FROM t WHERE (org = :org:int64 AND id IN (:ids:[]int64))`, true},
		{KindMany, `SELECT id:int64 FROM t WHERE (a = 1 OR b = 2) AND t.id IN (:ids:[]int64)`, true},                                // OR in a sibling group
		{KindMany, `SELECT id:int64 FROM t WHERE x BETWEEN 1 AND 5 AND id IN (:ids:[]int64)`, true},                                 // BETWEEN's AND
		{KindMany, `SELECT id:int64, (SELECT count(*) FROM u WHERE u.t = t.id) AS n:int64 FROM t WHERE id IN (:ids:[]int64)`, true}, // per-row scalar sub-query
		{KindMany, `SELECT id:int64 FROM t WHERE EXISTS (SELECT 1 FROM u ORDER BY 1 LIMIT 1) AND id IN (:ids:[]int64)`, true},       // ORDER BY inside a sibling sub-query
		{KindMany, `WITH c AS (SELECT max(id) AS m FROM u) SELECT id:int64 FROM t, c WHERE id IN (:ids:[]int64)`, true},             // aggregate in a CTE that does not see the collection
		{KindMany, `SELECT t.id:int64 FROM t LEFT JOIN u ON u.t = t.id WHERE (t.a, t.b) IN (:rows:[]P(A:int64, B:int64))`, true},
		{KindMany, `SELECT id:int64 FROM t WHERE lower(name) IN (:names:[]string) FOR UPDATE`, true},
		{KindMany, `SELECT id:int64 FROM t WHERE name::text IN (:names:[]string)`, true},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64) -- ORDER BY id`, true}, // comment
		{KindMany, `SELECT id:int64 FROM t WHERE note = 'GROUP BY' AND id IN (:ids:[]int64)`, true},
		{KindMany, `SELECT id:int64 FROM t WHERE a IS DISTINCT FROM b AND id IN (:ids:[]int64)`, true},
		{KindMany, `SELECT id:int64 FROM t WHERE a IS NOT DISTINCT FROM :b:int64 AND id IN (:ids:[]int64)`, true},
		{KindExec, `DELETE FROM t WHERE id IN (:ids:[]int64)`, true},
		{KindMany, `DELETE FROM t WHERE id IN (:ids:[]int64) RETURNING id:int64`, true},
		{KindExec, `UPDATE t SET n = n + 1 WHERE id IN (:ids:[]int64)`, true},
		{KindExec, `INSERT INTO t (a, b) VALUES :rows:[]R(A:int64, B:int64)`, true},
		{KindMany, `INSERT INTO t (a, b) VALUES :rows:[]R(A:int64, B:int64) ON CONFLICT (a) DO UPDATE SET b = excluded.b RETURNING a:int64`, true},
		// A VALUES list joined into a top-level UPDATE / DELETE is row-wise.
		{KindExec, `WITH v(id, d) AS (VALUES :rows:[]D(ID:int64, D:int64)) UPDATE t SET n = n + v.d FROM v WHERE t.id = v.id`, true},
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE t.id = v.id`, true},
		{KindMany, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) v(id, d) WHERE v.id = t.id AND t.org = :org:int64 RETURNING t.id:int64`, true},
		{KindExec, `UPDATE t SET n = v.d FROM u, (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE u.id = t.uid AND t.id = v.id`, true},
		{KindExec, `UPDATE t SET n = (SELECT max(u.x) FROM u WHERE u.k = v.d) FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE t.id = v.id`, true},
		{KindExec, `DELETE FROM t USING (VALUES :rows:[]K(ID:int64)) AS v(id) WHERE t.id = v.id`, true},
		{KindMany, `WITH v(id) AS (VALUES :rows:[]K(ID:int64)) DELETE FROM t USING v WHERE t.id = v.id RETURNING t.id:int64`, true},
		{KindExec, `UPDATE t JOIN (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) ON t.id = v.id SET t.n = t.n + v.d`, true}, // MySQL shape

		// Not decomposable: the result depends on rows outside one chunk.
		{KindMany, `SELECT status:string, COUNT(*) AS n:int64 FROM t WHERE id IN (:ids:[]int64) GROUP BY status`, false},
		{KindMany, `SELECT COUNT(*) AS n:int64, max(id) AS m:int64 FROM t WHERE id IN (:ids:[]int64)`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64) ORDER BY id`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64) LIMIT 3`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64) OFFSET 3`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64) FETCH FIRST 3 ROWS ONLY`, false},
		{KindMany, `SELECT DISTINCT a:int64 FROM t WHERE id IN (:ids:[]int64)`, false},
		{KindMany, `SELECT DISTINCT ON (a) a:int64 FROM t WHERE id IN (:ids:[]int64)`, false},
		{KindMany, `SELECT a:int64 FROM t WHERE id IN (:ids:[]int64) GROUP BY a HAVING a > 1`, false},
		{KindMany, `SELECT id:int64, row_number() OVER (ORDER BY id) AS rn:int64 FROM t WHERE id IN (:ids:[]int64)`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64) UNION SELECT id FROM u`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64) {?Sorted: ORDER BY id }`, false}, // optional blocks count
		{KindMany, `SELECT id:int64 FROM t WHERE id NOT IN (:ids:[]int64)`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE NOT id IN (:ids:[]int64)`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE a = 1 OR id IN (:ids:[]int64)`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE (a = 1 OR id IN (:ids:[]int64))`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE NOT (a = 1 AND id IN (:ids:[]int64))`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE flag = id IN (:ids:[]int64)`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (:ids:[]int64) IS NOT TRUE`, false},
		{KindMany, `SELECT t.id:int64 FROM t LEFT JOIN u ON u.t = t.id AND u.id IN (:ids:[]int64)`, false}, // ON clause of an outer join
		{KindMany, `SELECT id:int64, CASE WHEN id IN (:ids:[]int64) THEN 1 ELSE 0 END AS hit:int64 FROM t`, false},
		{KindMany, `SELECT id:int64 FROM t WHERE id IN (SELECT tid FROM u WHERE u.id IN (:ids:[]int64))`, false}, // sub-query
		{KindMany, `SELECT n:int64 FROM (SELECT count(*) AS n FROM t WHERE id IN (:ids:[]int64)) s`, false},      // derived table
		{KindMany, `WITH s AS (SELECT id FROM t WHERE id IN (:ids:[]int64)) SELECT count(*) AS n:int64 FROM s`, false},
		{KindExec, `INSERT INTO t (a) SELECT d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d)`, false},
		{KindMany, `SELECT t.id:int64 FROM t JOIN (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) ON t.id = v.id`, false}, // only UPDATE / DELETE
		// VALUES joined into UPDATE / DELETE: only with an equality join conjunct, inner joins, no OR
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d)`, false},                                            // no join predicate
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE t.x = 1`, false},                              // v only in SET
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE v.id = :x:int64`, false},                      // a filter, not a join
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE v.id = v.d`, false},                           // v with itself
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE t.id = v.id OR t.x = 1`, false},               // OR around the join
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE (t.id = v.id OR t.x = 1)`, false},             // OR in its group
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE NOT t.id = v.id`, false},                      // negated
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE t.id = v.id = TRUE`, false},                   // part of a larger expression
		{KindExec, `UPDATE t SET n = 1 FROM u LEFT JOIN (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) ON u.id = v.id WHERE t.id = u.id`, false}, // outer join
		{KindExec, `UPDATE t JOIN (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) ON t.id = v.id SET t.n = v.d ORDER BY t.id LIMIT 5`, false},
		{KindExec, `UPDATE t SET n = v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) WHERE t.id = 1`, false},                                          // no alias
		{KindExec, `WITH v(id, d) AS (VALUES :rows:[]D(ID:int64, D:int64)) UPDATE t SET n = (SELECT sum(d) FROM v) FROM v WHERE t.id = v.id`, false}, // aggregate over v
		{KindExec, `WITH v(id, d) AS (VALUES :rows:[]D(ID:int64, D:int64)) UPDATE t SET n = 1 FROM v WHERE t.id = v.id AND EXISTS (SELECT 1 FROM v WHERE v.d > 5)`, false},
		{KindExec, `WITH v(id, d) AS (VALUES :rows:[]D(ID:int64, D:int64)), w AS (SELECT 1) UPDATE t SET n = v.d FROM v WHERE t.id = v.id`, false}, // not the only CTE
		{KindExec, `WITH RECURSIVE v(id, d) AS (VALUES :rows:[]D(ID:int64, D:int64)) UPDATE t SET n = v.d FROM v WHERE t.id = v.id`, false},
		{KindExec, `WITH v(id, d) AS (VALUES :rows:[]D(ID:int64, D:int64)) UPDATE t SET n = v.d WHERE t.id IN (SELECT id FROM v)`, false},
		{KindExec, `WITH updates(id, bonus) AS (VALUES :rows:[]D(ID:int64, D:int64)) UPDATE t SET p = t.p + (SELECT u.bonus FROM updates u WHERE u.id = t.id) WHERE t.id IN (SELECT u.id FROM updates u)`, false},
		{KindExec, `WITH v(id, d) AS (VALUES :rows:[]D(ID:int64, D:int64)) UPDATE t SET n = v.d FROM v WHERE t.id = v.id GROUP BY t.id`, false},
		{KindExec, `DELETE FROM t WHERE id NOT IN (:keep:[]int64)`, false},
		{KindExec, `UPDATE t SET n = 0 WHERE a = 1 OR id IN (:ids:[]int64)`, false},
		{KindExec, `DELETE FROM t WHERE id IN (:ids:[]int64) ORDER BY id LIMIT 10`, false}, // MySQL
		{KindExec, `UPDATE t SET n = CASE WHEN id IN (:ids:[]int64) THEN 1 ELSE 0 END`, false},
		{KindExec, `INSERT INTO t (a) SELECT a FROM u WHERE id IN (:ids:[]int64)`, false}, // only INSERT ... VALUES
	} {
		cs, err := compileStatement(1, StatementSpec{Name: "S", Kind: tc.kind, SQL: tc.sql}, stmtDialect{known: true, style: runtime.DialectDollar, family: "postgres"})
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if !cs.CanBatch {
			t.Fatalf("%s: not a one-collection statement", tc.sql)
		}
		if cs.Decomposable != tc.want {
			t.Errorf("%s: decomposable = %v (%s), want %v", tc.sql, cs.Decomposable, cs.NotDecomposableWhy, tc.want)
		}
		if cs.CanStream && !cs.Decomposable {
			t.Errorf("%s: a non-decomposable statement must not have a Stream form", tc.sql)
		}
	}
}
