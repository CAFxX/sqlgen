package sqlgen_test

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"sync"
	"testing"
)

var (
	typeCheckMu   sync.Mutex
	typeCheckFset = token.NewFileSet()
	typeCheckImp  = importer.ForCompiler(typeCheckFset, "source", nil)
)

// typeCheckGenerated parses and fully type-checks generated Go source (imports resolved from source),
// so redeclarations, shadowing, unused variables/imports and type errors fail the test exactly as the
// Go compiler would, instead of only being caught when a user builds the generated file.
func typeCheckGenerated(t *testing.T, name string, src []byte) {
	t.Helper()
	errs, err := typeCheckErrors(name, src)
	if err != nil {
		t.Fatalf("%s: parse: %v\n%s", name, err, src)
	}
	for i, e := range errs {
		if i >= 5 {
			t.Errorf("... and %d more type errors", len(errs)-i)
			break
		}
		t.Errorf("%s: type error: %v", name, e)
	}
	if len(errs) > 0 {
		t.FailNow()
	}
}

// typeCheckErrors is typeCheckGenerated without the testing plumbing: it returns the parse error, or the
// list of type errors (empty when the source type-checks).
func typeCheckErrors(name string, src []byte) (errs []error, parseErr error) {
	typeCheckMu.Lock()
	defer typeCheckMu.Unlock()
	file, err := parser.ParseFile(typeCheckFset, name+".go", src, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	conf := types.Config{
		Importer: typeCheckImp,
		Error:    func(err error) { errs = append(errs, err) },
	}
	_, _ = conf.Check(file.Name.Name, typeCheckFset, []*ast.File{file}, nil)
	return errs, nil
}

// The sort tokens of one statement are a different type from those of another, so passing them to the
// wrong Order<Stmt> constructor is a compile error, while the right pairing type-checks.
func TestOrderTokensOfOtherStatementsDoNotCompile(t *testing.T) {
	src, err := genYAML("postgres", `  - name: A
    kind: many
    sql: |
      SELECT x:int64, id:int64 FROM a WHERE :a:sqlgen.Seek ORDER BY :s:sqlgen.Order(x, id)
  - name: B
    kind: many
    sql: |
      SELECT y:int64, id:int64 FROM b WHERE :a:sqlgen.Seek ORDER BY :s:sqlgen.Order(y, id)
`)
	if err != nil {
		t.Fatal(err)
	}
	check := func(extra string) []error {
		typeCheckMu.Lock()
		defer typeCheckMu.Unlock()
		file, err := parser.ParseFile(typeCheckFset, "p_gen.go", append(append([]byte{}, src...), extra...), parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		var errs []error
		conf := types.Config{Importer: typeCheckImp, Error: func(err error) { errs = append(errs, err) }}
		_, _ = conf.Check("p", typeCheckFset, []*ast.File{file}, nil)
		return errs
	}
	if errs := check("\nvar _ = OrderA(AOrderByX.Asc(), AOrderByID.Desc())\nvar _ = OrderB(BOrderByY.Desc())\n"); len(errs) != 0 {
		t.Fatalf("matching tokens must type-check: %v", errs)
	}
	for name, extra := range map[string]string{
		"other statement's token": "\nvar _ = OrderA(BOrderByY.Asc())\n",
		"other statement's const": "\nvar _ = OrderB(OrderAXAsc)\n",
		"plain uint8":             "\nvar _ = OrderA(uint8(2))\n",
	} {
		if errs := check(extra); len(errs) == 0 {
			t.Errorf("%s: must not compile", name)
		}
	}
}
