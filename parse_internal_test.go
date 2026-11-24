package sqlgen

import (
	"go/parser"
	"go/token"
	"testing"
)

// parseSQL compiles one annotated statement with every dialect-dependent diagnostic off and returns the
// first error, for tests of the SQL scanner alone.
func parseSQL(sqlText string) (*compiledStmt, error) {
	return compileStatement(1, StatementSpec{Name: "FuzzStmt", Kind: KindSubquery, SQL: sqlText}, stmtDialect{})
}

func TestParseSQLValidAndInvalid(t *testing.T) {
	if cs, err := parseSQL("SELECT id:int64 FROM users WHERE id = :ID:int64"); err != nil || len(cs.Root.Segments) == 0 {
		t.Fatalf("parseSQL failed: %v", err)
	}
	if _, err := parseSQL("SELECT 'unclosed FROM users"); err == nil {
		t.Fatalf("expected parseSQL error on unclosed literal")
	}

	// parseTupleBody and findConditionalBlockClose error paths, and the retired spellings.
	badSQLs := []string{
		"INSERT INTO t VALUES :Rows:[]123Bad(A:int64)",
		"INSERT INTO t VALUES :Rows:[]Tup('unclosed, A:int64)",
		"INSERT INTO t VALUES :Rows:[]Tup(\"unclosed, A:int64)",
		"INSERT INTO t VALUES :Rows:[]Tup(`unclosed, A:int64)",
		"INSERT INTO t VALUES :Rows:[]Tup(A:int64...",
		"INSERT INTO t VALUES :Rows:[]Tup(A:[]string)",
		"INSERT INTO t VALUES :Rows:[]Tup(A:int64, a:int64)",
		"INSERT INTO t VALUES :Rows:[]Tup($$unclosed)",
		"INSERT INTO t VALUES :Rows:[]Tup(/* unclosed)",
		"SELECT id:int64 FROM t WHERE 1=1 {?F: AND x = $$unclosed }",
		"SELECT id:int64 FROM t WHERE 1=1 {?F: AND x = /* unclosed }",
		"SELECT id:int64 FROM t WHERE (a, b) IN (:Pairs:[]PairItem(A:int64",
		"SELECT id:int64 FROM t WHERE (a, b) IN (:Pairs:[]PairItem(bad))",
		"SELECT id:int64 FROM t WHERE id IN (:IDs:[]int64 ASC) AND id IN (:IDs DESC)",
		// Spellings that used to exist: colon-less parameters, "..." slices, forward references, directives
		// without the leading colon or with the runtime. prefix.
		"SELECT id:int64 FROM t WHERE id = ID:int64",
		"SELECT id:int64 FROM t WHERE id IN (IDs:[]int64)",
		"SELECT id:int64 FROM t WHERE id IN (:IDs...:int64)",
		"SELECT id:int64 FROM t WHERE id IN (:IDs:int64...)",
		"SELECT id:int64 FROM t WHERE id IN (IDs...:int64)",
		"SELECT id:int64 FROM t WHERE id = :ID",
		"SELECT id:int64 FROM t WHERE Seek:sqlgen.Seek ORDER BY :Order:sqlgen.Order(id)",
		"SELECT id:int64 FROM t WHERE :Seek:sqlgen.Seek ORDER BY Order:sqlgen.Order(id)",
		"SELECT id:int64 FROM t WHERE id IN (Sub:sqlgen.Query)",
		"SELECT id:int64 FROM t WHERE id IN (:Sub:runtime.Query)",
		"SELECT id:int64 FROM t WHERE :Seek:runtime.Seek ORDER BY :Order:runtime.Order(id)",
		"SELECT id:int64 FROM t WHERE :Sub:Query",
		"INSERT INTO t VALUES Rows:[]Tup(A:int64)",
		"INSERT INTO t VALUES :Rows:[]Tup(A:int64...)",
	}
	for i, bsql := range badSQLs {
		if _, err := parseSQL(bsql); err == nil {
			t.Fatalf("badSQL[%d] expected error, got nil: %s", i, bsql)
		}
	}
}

func FuzzParseSQL(f *testing.F) {
	f.Add("SELECT id:int64, email:string FROM users WHERE id = :ID:int64")
	f.Add("SELECT u.id:int64 {?WithBio: , u.bio:string } FROM users u WHERE :Seek:sqlgen.Seek ORDER BY :Order:sqlgen.Order(id) LIMIT :Limit:int64")
	f.Add("INSERT INTO items (id, name) VALUES :Rows:[]ItemInput(ID:int64 ASC, Name:string) RETURNING id:int64")
	f.Add("SELECT id:int64 FROM t WHERE a = ID:int64 AND b IN (IDs...:int64) AND c IN (:IDs:int64...) AND d = :Undeclared")
	f.Add("SELECT id:int64 FROM t WHERE x IN (:Sub:sqlgen.Query) AND y = :Sub")
	f.Add("SELECT 'quoted :not_param {?NoFlag: 1}', $$dollar :not_param$$, /* comment :bad */ u.id::bigint AS id:int64 FROM users u")

	f.Fuzz(func(t *testing.T, sqlText string) {
		_, _ = parseSQL(sqlText)
		for _, kind := range []QueryKind{KindOne, KindMany, KindExec, KindSubquery} {
			cfg := Config{
				PackageName: "fuzzpkg",
				Dialect:     "postgres",
				Statements: []StatementSpec{
					{
						Name: "FuzzStmt",
						Kind: kind,
						SQL:  sqlText,
					},
				},
			}
			src, err := Generate(cfg)
			if err == nil && len(src) > 0 {
				fset := token.NewFileSet()
				if _, parseErr := parser.ParseFile(fset, "fuzz.go", src, parser.AllErrors); parseErr != nil {
					t.Fatalf("Generate succeeded but emitted invalid Go syntax: %v\nSQL: %q\nCode:\n%s", parseErr, sqlText, string(src))
				}
			}
		}
	})
}
