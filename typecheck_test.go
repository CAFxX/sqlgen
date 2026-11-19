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
	typeCheckMu.Lock()
	defer typeCheckMu.Unlock()
	file, err := parser.ParseFile(typeCheckFset, name+".go", src, parser.AllErrors)
	if err != nil {
		t.Fatalf("%s: parse: %v\n%s", name, err, src)
	}
	var errs []error
	conf := types.Config{
		Importer: typeCheckImp,
		Error:    func(err error) { errs = append(errs, err) },
	}
	_, _ = conf.Check(file.Name.Name, typeCheckFset, []*ast.File{file}, nil)
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
