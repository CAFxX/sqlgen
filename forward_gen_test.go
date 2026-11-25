package sqlgen_test

import (
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen"
)

func fwdYAML(dialect, kind, sql string) string {
	return "package: p\ndialect: " + dialect + "\nstatements:\n  - name: S\n    kind: " + kind + "\n    sql: " + sql + "\n"
}

// Arg[T] is validated when the code is generated: only scalar T, never in a statement without a Pipeline
// method, and on the $N dialect a T that travels as text needs an explicit cast.
func TestForwardGenerateErrors(t *testing.T) {
	for _, tc := range []struct {
		name, dialect, kind, sql, want string
	}{
		{"slice of Arg", "postgres", "exec", "DELETE FROM t WHERE id IN (:ids:[]Arg[int64])", "forwards one scalar value"},
		{"Arg of slice", "postgres", "exec", "UPDATE t SET v = :x:Arg[[]int64]", "not a slice"},
		{"Arg of a directive", "mysql", "exec", "UPDATE t SET v = :x:Arg[sqlgen.Query]", "not a forwardable type"},
		{"Arg of Query", "mysql", "exec", "UPDATE t SET v = :x:Arg[Query]", "not a forwardable type"},
		{"nested Arg", "mysql", "exec", "UPDATE t SET v = :x:Arg[Arg[int64]]", "invalid type in Arg"},
		{"empty Arg", "mysql", "exec", "UPDATE t SET v = :x:Arg[]", "invalid type in Arg"},
		{"unterminated", "mysql", "exec", "UPDATE t SET v = :x:Arg[int64", "unterminated Arg"},
		{"unknown type", "mysql", "exec", "UPDATE t SET v = :x:Arg[strng]", "did you mean"},
		{"string needs a cast on $N", "postgres", "exec", "UPDATE t SET v = :x:Arg[string]", "explicit SQL cast"},
		{"unmapped type needs a cast on $N", "cockroachdb", "exec", "UPDATE t SET v = :x:Arg[sql.NullInt64]", "explicit SQL cast"},
		{"repeated reference needs a cast", "postgres", "exec", "UPDATE t SET v = :x:Arg[string]::text WHERE w = :x", "repeated reference"},
		{"subquery", "postgres", "subquery", "SELECT id FROM t WHERE v = :x:Arg[int64]", "kind subquery"},
		{"plain then Arg", "mysql", "exec", "UPDATE t SET v = :x:int64 WHERE w = :x:Arg[int64]", "both as Arg"},
		{"Arg then plain", "mysql", "exec", "UPDATE t SET v = :x:Arg[int64] WHERE w = :x:int64", "both as Arg"},
		{"tuple field", "mysql", "exec", "INSERT INTO t (a) VALUES :rows:[]R(a:Arg[int64])", "not a tuple field type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlgen.GenerateFromYAML([]byte(fwdYAML(tc.dialect, tc.kind, tc.sql)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestForwardGenerateAccepts(t *testing.T) {
	for _, tc := range []struct {
		name, dialect, kind, sql string
		want                     []string
	}{
		{"cast from the Go type on $N", "postgres", "exec", "UPDATE t SET v = :x:Arg[int64] WHERE ts < :ts:Arg[time.Time]",
			[]string{`Cast: "bigint"`, `Cast: "timestamptz"`, "func (pl *Pipeline) S(x runtime.Arg[int64], ts runtime.Arg[time.Time]) *runtime.ExecFuture", "func (q *Queries) S(ctx context.Context, x int64, ts time.Time)"}},
		{"explicit casts", "postgres", "exec", "UPDATE t SET u = :u:Arg[string]::uuid, w = :u::uuid, n = :n:Arg[sql.NullInt64]::numeric",
			[]string{`Cast: "text"`, "u runtime.Arg[string], n runtime.Arg[sql.NullInt64]"}},
		{"question dialect needs no cast", "mysql", "one", "SELECT COUNT(*) AS n:int64 FROM t WHERE s = :s:Arg[string]",
			[]string{"func (pl *Pipeline) S(s runtime.Arg[string]) *runtime.RowFuture[int64]", `ResultCols: []string{"n"}`, `ResultExpr: "COUNT(*)"`}},
		{"struct style", "mariadb", "one", "INSERT INTO t (a, b) VALUES (:a:Arg[int64], :b:string) RETURNING id AS order_id:int64",
			[]string{"type SPipelineParams struct", "A runtime.Arg[int64]", `ResultExpr: "id"`, "func (pl *Pipeline) S(pa SPipelineParams)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			y := fwdYAML(tc.dialect, tc.kind, tc.sql)
			if tc.name == "struct style" {
				y += "    args: struct\n"
			}
			src, err := sqlgen.GenerateFromYAML([]byte(y))
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(string(src), w) {
					t.Errorf("generated code lacks %q", w)
				}
			}
		})
	}
}
