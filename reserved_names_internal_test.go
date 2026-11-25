package sqlgen

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"unicode"
)

func parseGenerated(t *testing.T, name string, src []byte) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
	if err != nil {
		t.Fatalf("%s: generated code does not parse: %v", name, err)
	}
	return f
}

func isLowerInitial(s string) bool {
	for _, r := range s {
		return unicode.IsLower(r) || r == '_'
	}
	return false
}

// TestReservedNamesCoverGeneratedLocals fails when generated function bodies declare a local identifier
// that reservedParamNames does not know about: a SQL parameter of that name would shadow or redeclare it.
func TestReservedNamesCoverGeneratedLocals(t *testing.T) {
	configs, err := filepath.Glob("examples/*/sqlgen.yaml")
	if err != nil || len(configs) == 0 {
		t.Fatalf("no example configs found: %v", err)
	}
	configs = append(configs, "integration/pgq/sqlgen.yaml", "integration/myq/sqlgen.yaml")
	missing := map[string][]string{}
	for _, cfgPath := range configs {
		data, err := os.ReadFile(cfgPath)
		if err != nil {
			t.Fatal(err)
		}
		src, err := GenerateFromYAML(data)
		if err != nil {
			t.Fatalf("%s: %v", cfgPath, err)
		}
		f := parseGenerated(t, cfgPath, src)
		add := func(id *ast.Ident) {
			if id == nil || !isLowerInitial(id.Name) || reservedParamNames[id.Name] {
				return
			}
			missing[id.Name] = append(missing[id.Name], cfgPath)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					if n.Tok == token.DEFINE {
						for _, l := range n.Lhs {
							if id, ok := l.(*ast.Ident); ok {
								add(id)
							}
						}
					}
				case *ast.ValueSpec:
					for _, id := range n.Names {
						add(id)
					}
				case *ast.RangeStmt:
					if n.Tok == token.DEFINE {
						if id, ok := n.Key.(*ast.Ident); ok {
							add(id)
						}
						if id, ok := n.Value.(*ast.Ident); ok {
							add(id)
						}
					}
				case *ast.FuncLit:
					for _, fl := range n.Type.Params.List {
						for _, id := range fl.Names {
							add(id)
						}
					}
				}
				return true
			})
		}
	}
	if len(missing) > 0 {
		var names []string
		for n := range missing {
			names = append(names, n)
		}
		sort.Strings(names)
		t.Errorf("generated locals missing from reservedParamNames: %s (first seen in %s)",
			strings.Join(names, " "), missing[names[0]][0])
	}
}

// TestReservedNamesCoverGeneratedDeclarations fails when the generated file declares an exported
// package-level name or a method on *Queries / *Pipeline that is not in the reserved lists, so a
// statement, result_type or tuple struct could silently collide with it.
func TestReservedNamesCoverGeneratedDeclarations(t *testing.T) {
	src, err := GenerateFromYAML([]byte(`package: zz
dialect: postgres
statements:
  - name: Zzz
    kind: exec
    sql: DELETE FROM t
`))
	if err != nil {
		t.Fatal(err)
	}
	f := parseGenerated(t, "zz", src)
	var types, methods []string
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if d.Recv != nil {
				methods = append(methods, d.Name.Name)
			} else {
				types = append(types, d.Name.Name)
			}
		case *ast.GenDecl:
			for _, s := range d.Specs {
				switch s := s.(type) {
				case *ast.TypeSpec:
					types = append(types, s.Name.Name)
				case *ast.ValueSpec:
					for _, id := range s.Names {
						types = append(types, id.Name)
					}
				}
			}
		}
	}
	var missing []string
	for _, n := range types {
		if n == "sqlDialect" || !ast.IsExported(n) || strings.Contains(n, "Zzz") || reservedTypeNames[n] {
			continue
		}
		missing = append(missing, "type/package-level "+n)
	}
	for _, n := range methods {
		if !ast.IsExported(n) || strings.Contains(n, "Zzz") || reservedStatementNames[n] {
			continue
		}
		missing = append(missing, "method "+n)
	}
	if len(missing) > 0 {
		t.Errorf("generated declarations missing from reserved name lists: %s", strings.Join(missing, ", "))
	}

	pl, err := os.ReadFile("runtime/pipeline.go")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, line := range strings.Split(string(pl), "\n") {
		rest, ok := strings.CutPrefix(line, "func (p *Pipeline) ")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(rest, "(")
		if !ast.IsExported(name) {
			continue
		}
		found++
		if !reservedStatementNames[name] {
			t.Errorf("runtime.Pipeline method %s is promoted onto the generated Pipeline but missing from reservedStatementNames", name)
		}
	}
	if found == 0 {
		t.Error("found no exported runtime.Pipeline methods; test is stale")
	}
}

// The parameters of NewSeek<Stmt> are the sort columns' fields in lowerCamelCase, never a keyword or a
// name the constructor's body uses.
func TestSeekParamNames(t *testing.T) {
	for in, want := range map[string]string{
		"ID": "id", "CreatedAt": "createdAt", "URLPath": "urlPath", "X": "x", "GroupKey": "groupKey",
		"Type": "typeVal", "Func": "funcVal", "Runtime": "runtimeVal", "Any": "anyVal", "Score2": "score2",
	} {
		if got := seekParamName(in); got != want {
			t.Errorf("seekParamName(%q) = %q, want %q", in, got, want)
		}
	}
}
