package sqlgen_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen"
)

// argStyleShapes are statement bodies covering every kind and the features that shape the signature of the
// generated methods; each is generated under every spelling of the args: key.
var argStyleShapes = []struct {
	name, body string
	flags      bool // has {?Flag: ...} blocks: auto picks the struct and positional is rejected
	projects   bool // has the SelectCols row projection (two or more unconditional output columns)
}{
	{"one", "kind: one\n    sql: |\n      SELECT id:int64, name:string FROM t WHERE id = :id:int64", false, true},
	{"one single column", "kind: one\n    sql: |\n      SELECT name:string FROM t WHERE id = :id:int64", false, false},
	{"one no params", "kind: one\n    sql: |\n      SELECT id:int64, name:string FROM t LIMIT 1", false, true},
	{"many slice", "kind: many\n    sql: |\n      SELECT id:int64, name:string FROM t WHERE id IN (:ids:[]int64 ASC) AND z = :z:string", false, true},
	{"many tuple slice", "kind: many\n    sql: |\n      SELECT a:int64, b:string FROM t WHERE (a, b) IN (:keys:[]Key(A:int64 ASC, B:string))", false, true},
	{"many insert returning", "kind: many\n    sql: |\n      INSERT INTO t (a, b) VALUES :rows:[]Row(A:int64 ASC, B:string) RETURNING a:int64, b:string", false, true},
	{"many seek order", "kind: many\n    sql: |\n      SELECT id:int64, score:int64 FROM t WHERE org = :org:int64 AND :After:sqlgen.Seek ORDER BY :Sort:sqlgen.Order(score, id) LIMIT :limit:int64", false, true},
	{"many flags", "kind: many\n    sql: |\n      SELECT id:int64 {?WithName: , name:string} FROM t WHERE 1=1 {?OnlyOrg: AND org = :org:int64} {?FilterRoles: AND role IN (:roles:[]string)}", true, false},
	{"many flags and columns", "kind: many\n    sql: |\n      SELECT id:int64, k:string {?WithName: , name:string} FROM t WHERE 1=1 {?OnlyOrg: AND org = :org:int64}", true, true},
	{"exec", "kind: exec\n    sql: |\n      UPDATE t SET name = :name:string WHERE id = :id:int64", false, false},
	{"exec no params", "kind: exec\n    sql: |\n      DELETE FROM t", false, false},
	{"exec slice", "kind: exec\n    sql: |\n      DELETE FROM t WHERE id IN (:ids:[]int64 ASC) AND org = :org:int64", false, false},
	{"exec tuple slice", "kind: exec\n    sql: |\n      INSERT INTO t (a, b) VALUES :rows:[]Row(A:int64 ASC, B:string)", false, false},
	{"exec flags", "kind: exec\n    sql: |\n      UPDATE t SET n = n + 1 WHERE 1=1 {?OnlyOrg: AND org = :org:int64}", true, false},
	{"subquery", "kind: subquery\n    sql: |\n      SELECT id, name FROM t WHERE x = :x:int64", false, false},
}

func argStyleYAML(args, body string) []byte {
	key := ""
	if args != "" {
		key = "    args: " + args + "\n"
	}
	return []byte("package: p\ndialect: postgres\nstatements:\n  - name: Foo\n" + key + "    " + body + "\n")
}

// Every statement kind and feature generates in every style it can have; the spellings that resolve to the
// same style generate identical code (auto is not a third style). Type-checking is done once per style on a
// file holding all the shapes, type-checking is the slow part.
func TestArgsStyleMatrix(t *testing.T) {
	var structStmts, posStmts []string
	for i, sh := range argStyleShapes {
		name := "Foo"
		t.Run(sh.name, func(t *testing.T) {
			gen := func(args string) ([]byte, error) { return sqlgen.GenerateFromYAML(argStyleYAML(args, sh.body)) }

			auto, err := gen("")
			if err != nil {
				t.Fatalf("auto: %v", err)
			}
			if explicit, err := gen(sqlgen.ArgsAuto); err != nil || !bytes.Equal(explicit, auto) {
				t.Fatalf("args: auto differs from the omitted key (err %v)", err)
			}
			structSrc, err := gen(sqlgen.ArgsStruct)
			if err != nil {
				t.Fatalf("struct: %v", err)
			}
			if !strings.Contains(string(structSrc), "type FooParams struct") {
				t.Fatal("args: struct does not generate FooParams")
			}
			if sh.projects != strings.Contains(string(structSrc), "\tSelectCols uint64\n") {
				t.Fatalf("struct style: SelectCols field present = %v, want %v", !sh.projects, sh.projects)
			}

			posSrc, err := gen(sqlgen.ArgsPositional)
			if sh.flags {
				if err == nil || !strings.Contains(err.Error(), "statement Foo: args: positional is not possible") || !strings.Contains(err.Error(), "{?Flag") {
					t.Fatalf("positional with flag blocks: err = %v", err)
				}
				if !bytes.Equal(auto, structSrc) {
					t.Fatal("auto with flag blocks must be the struct style")
				}
				return
			}
			if err != nil {
				t.Fatalf("positional: %v", err)
			}
			if !bytes.Equal(auto, posSrc) {
				t.Fatal("auto without flag blocks must be the positional style")
			}
			if strings.Contains(string(posSrc), "type FooParams struct") {
				t.Fatal("positional style generated FooParams")
			}
			if sh.projects != strings.Contains(string(posSrc), "selectCols ...uint64") {
				t.Fatalf("positional style: variadic selectCols present = %v, want %v", !sh.projects, sh.projects)
			}
		})
		_ = name
		stmt := fmt.Sprintf("  - name: S%d\n    args: %%s\n    %s\n", i, sh.body)
		structStmts = append(structStmts, fmt.Sprintf(stmt, sqlgen.ArgsStruct))
		if !sh.flags {
			posStmts = append(posStmts, fmt.Sprintf(stmt, sqlgen.ArgsPositional))
		}
	}
	for style, stmts := range map[string][]string{sqlgen.ArgsStruct: structStmts, sqlgen.ArgsPositional: posStmts} {
		src, err := sqlgen.GenerateFromYAML([]byte("package: p\ndialect: postgres\nstatements:\n" + strings.Join(stmts, "")))
		if err != nil {
			t.Fatalf("%s: %v", style, err)
		}
		typeCheckGenerated(t, "matrix_"+style, src)
	}
}

func TestArgsStyleRejectsUnknownValues(t *testing.T) {
	body := argStyleShapes[0].body
	for _, bad := range []string{"Positional", "STRUCT", "params", "positional ", "true", "named"} {
		_, err := sqlgen.GenerateFromYAML(argStyleYAML(`"`+bad+`"`, body))
		if err == nil {
			t.Fatalf("args: %q accepted", bad)
		}
		for _, want := range []string{"statement Foo", "unknown args value " + `"` + bad + `"`, "auto, positional, struct"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("args: %q: error %q lacks %q", bad, err, want)
			}
		}
	}
	// The boolean key the args: key replaced is gone, not an alias.
	old := []byte("package: p\ndialect: postgres\nstatements:\n  - name: Foo\n    params_struct: true\n    " + body + "\n")
	if _, err := sqlgen.GenerateFromYAML(old); err == nil || !strings.Contains(err.Error(), "params_struct") {
		t.Fatalf("params_struct accepted: %v", err)
	}
}

// The SelectCols projection mask is a field of the (internal or public) params struct: a parameter that maps
// to the same Go field is rejected in both styles, while a statement without the projection has no such field.
func TestArgsStyleSelectColsFieldCollision(t *testing.T) {
	multi := "kind: one\n    sql: |\n      SELECT id:int64, name:string FROM t WHERE id = :select_cols:int64"
	for _, args := range []string{"", sqlgen.ArgsPositional, sqlgen.ArgsStruct} {
		if _, err := sqlgen.GenerateFromYAML(argStyleYAML(args, multi)); err == nil || !strings.Contains(err.Error(), "SelectCols projection mask") {
			t.Fatalf("args %q: collision accepted: %v", args, err)
		}
	}
	single := "kind: one\n    sql: |\n      SELECT id:int64 FROM t WHERE id = :select_cols:int64"
	src, err := sqlgen.GenerateFromYAML(argStyleYAML(sqlgen.ArgsPositional, single))
	if err != nil {
		t.Fatal(err)
	}
	typeCheckGenerated(t, "select_cols_single", src)
}
