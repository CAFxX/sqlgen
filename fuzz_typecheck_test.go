package sqlgen_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen"
)

// FuzzGeneratedCodeTypeChecks feeds arbitrary statement text, names and options to the generator and, whenever
// generation succeeds, requires the output to type-check as the Go compiler would (the sibling fuzz targets
// only require it to parse). The generator may reject an input; it must never panic and must never emit code
// that does not compile.
func FuzzGeneratedCodeTypeChecks(f *testing.F) {
	seeds := []struct {
		sql, name, result string
		kind              uint8
		dialect           uint8
		args              uint8 // 0 auto, 1 positional, 2 struct
	}{
		{"SELECT id:int64, email:string FROM users WHERE id = :ID:int64", "GetUser", "", 0, 0, 1},
		{"SELECT id:int64, bio:*string, n:sql.NullInt64 FROM users WHERE id IN (:IDs:[]int64 ASC)", "ListUsers", "User", 1, 1, 1},
		{"INSERT INTO items (id, name) VALUES :Rows:[]ItemInput(ID:int64 ASC, Name:string)", "InsertItems", "", 2, 0, 0},
		{"INSERT INTO items (id, name) VALUES :Rows:[]ItemInput(ID:int64 ASC, Name:string) RETURNING id:int64, name:string", "InsertItemsRet", "", 1, 1, 2},
		{"SELECT id:int64, role:string FROM u WHERE :Seek:sqlgen.Seek {?RoleFilter: AND role = :FilterRole:string} ORDER BY :Order:sqlgen.Order(role, id) LIMIT :Limit:int64", "Page", "", 1, 1, 2},
		{"SELECT id:int64 FROM t WHERE (a = :X:int64 OR b = :X:int64) AND k IN (:Ks:[]string)", "Dup", "", 1, 0, 1},
		{"SELECT u.id:int64 {?WithBio: , u.bio:string } FROM users u WHERE u.org IN (:Sub:sqlgen.Query)", "Opt", "", 1, 1, 0},
		{"SELECT id FROM t WHERE x = :MinID:int64", "Sub", "", 3, 0, 0},
		{"DELETE FROM t WHERE (a, b) IN (:Keys:[]Key(A:int64 ASC, B:string))", "DelKeys", "", 2, 1, 0},
		{"SELECT 'ünï:cödé' AS s:string FROM t WHERE ж = Я:int64", "Üni", "Рез", 1, 0, 0},
	}
	for _, s := range seeds {
		f.Add(s.sql, s.name, s.result, s.kind, s.dialect, s.args)
	}
	styles := []string{sqlgen.ArgsAuto, sqlgen.ArgsPositional, sqlgen.ArgsStruct}
	kinds := []sqlgen.QueryKind{sqlgen.KindOne, sqlgen.KindMany, sqlgen.KindExec, sqlgen.KindSubquery}
	dialects := []string{"postgres", "sqlite", "mysql"}
	f.Fuzz(func(t *testing.T, sqlText, name, result string, kind, dialect, args uint8) {
		first := styles[int(args)%len(styles)]
		second := sqlgen.ArgsAuto // the other statement uses a different style
		if first == sqlgen.ArgsAuto {
			second = sqlgen.ArgsStruct
		}
		cfg := sqlgen.Config{
			PackageName: "fuzzpkg",
			Dialect:     dialects[int(dialect)%len(dialects)],
			Statements: []sqlgen.StatementSpec{
				{
					Name:       name,
					Kind:       kinds[int(kind)%len(kinds)],
					ResultType: result,
					Args:       first,
					SQL:        sqlText,
				},
				// A second statement sharing the fuzzed text exercises cross-statement declarations
				// (shared result and tuple types, per-statement helpers) under other names.
				{
					Name:       "Second",
					Kind:       sqlgen.KindMany,
					ResultType: result,
					Args:       second,
					SQL:        sqlText,
				},
			},
		}
		src, err := sqlgen.Generate(cfg)
		if err != nil || len(src) == 0 {
			return
		}
		errs, perr := typeCheckErrors("fuzz", src)
		if perr != nil {
			t.Fatalf("generated code does not parse: %v\n%s", perr, src)
		}
		for _, e := range errs {
			if userTypeError(e, sqlText+" "+name+" "+result) {
				return // the input names a type or package that does not exist: nothing sqlgen can know
			}
		}
		if len(errs) > 0 {
			t.Fatalf("generated code does not type-check: %v\nSQL: %q name: %q result: %q\n%s", errs[0], sqlText, name, result, src)
		}
	})
}

var undefinedRe = regexp.MustCompile(`undefined: (?:(\w+)\.)?(\w+)$`)

// userTypeError reports whether err only says that a type (or package) the statement text itself names does
// not exist, as in `:x:sql.A`; sqlgen copies type spellings verbatim and cannot resolve them.
func userTypeError(err error, userText string) bool {
	m := undefinedRe.FindStringSubmatch(err.Error())
	if m == nil {
		return false
	}
	return strings.Contains(userText, m[2]) && (m[1] == "" || strings.Contains(userText, m[1]))
}
