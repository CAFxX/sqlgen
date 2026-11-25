package sqlgen_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen"
)

// The documentation is part of the surface: every yaml block of DOCS.md and README.md must generate, every
// Go block must parse, and every `runtime.X` they mention must exist. A block that is deliberately not a
// whole example (an elided fragment, a rejected spelling) is preceded by an HTML comment `<!-- docs:skip -->`.

type docBlock struct {
	file, lang, body string
	line             int
}

func docBlocks(t *testing.T) []docBlock {
	t.Helper()
	var out []docBlock
	for _, f := range []string{"DOCS.md", "README.md"} {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
		for i := 0; i < len(lines); i++ {
			l := strings.TrimSpace(lines[i])
			if !strings.HasPrefix(l, "```") || len(l) == 3 {
				continue
			}
			lang, start := strings.TrimPrefix(l, "```"), i
			var body []string
			for i++; i < len(lines) && !strings.HasPrefix(strings.TrimSpace(lines[i]), "```"); i++ {
				body = append(body, lines[i])
			}
			if start > 0 && strings.Contains(lines[start-1], "docs:skip") {
				continue
			}
			out = append(out, docBlock{f, lang, strings.Join(body, "\n"), start + 1})
		}
	}
	return out
}

func TestDocsYAMLBlocksGenerate(t *testing.T) {
	n := 0
	for _, b := range docBlocks(t) {
		if b.lang != "yaml" || !strings.Contains(b.body, "statements:") {
			continue
		}
		n++
		src := b.body
		if !strings.Contains(src, "package:") {
			src = "package: docs\n" + src
		}
		if !strings.Contains(src, "dialect:") {
			src = "dialect: postgres\n" + src
		}
		t.Run(b.file+":"+strconv.Itoa(b.line), func(t *testing.T) {
			if _, err := sqlgen.GenerateFromYAML([]byte(src)); err != nil {
				t.Fatalf("%v\n%s", err, b.body)
			}
		})
	}
	if n == 0 {
		t.Fatal("no yaml block found: the extraction is broken")
	}
}

// runtimeExports is the set of exported package-level identifiers of ./runtime.
func runtimeExports(t *testing.T) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	files, _ := filepath.Glob("runtime/*.go")
	names := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range af.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					names[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.TypeSpec:
						names[s.Name.Name] = true
					case *ast.ValueSpec:
						for _, n := range s.Names {
							names[n.Name] = true
						}
					}
				}
			}
		}
	}
	return names
}

var elided = regexp.MustCompile(`\{\s*\.\.\.\s*\}`)

var runtimeRef = regexp.MustCompile(`\bruntime\.([A-Z][A-Za-z0-9_]*)`)

// parseDocGo accepts what a documentation snippet is: a whole file, declarations, statements, or declarations
// followed by statements (imports are hoisted).
func parseDocGo(body string) error {
	body = elided.ReplaceAllString(body, "{}") // `if err != nil { ... }`
	if strings.HasPrefix(strings.TrimSpace(body), "package ") {
		_, err := parser.ParseFile(token.NewFileSet(), "x.go", body, parser.AllErrors)
		return err
	}
	var imports, decls, stmts []string
	lines := strings.Split(body, "\n")
	inImport, inStmts := false, false
	for _, l := range lines {
		switch {
		case inImport:
			imports = append(imports, l)
			inImport = !strings.HasPrefix(l, ")")
		case strings.HasPrefix(l, "import ("):
			imports, inImport = append(imports, l), true
		case strings.HasPrefix(l, "import "):
			imports = append(imports, l)
		case inStmts:
			stmts = append(stmts, l)
		case l == "" || strings.HasPrefix(l, " ") || strings.HasPrefix(l, "\t") || strings.HasPrefix(l, "//") ||
			strings.HasPrefix(l, "}") || strings.HasPrefix(l, ")") || strings.HasPrefix(l, "type ") ||
			strings.HasPrefix(l, "func ") || strings.HasPrefix(l, "var (") || strings.HasPrefix(l, "const ("):
			decls = append(decls, l)
		default:
			inStmts = true
			stmts = append(stmts, l)
		}
	}
	src := "package p\n" + strings.Join(imports, "\n") + "\n" + strings.Join(decls, "\n") + "\nfunc _() {\n" + strings.Join(stmts, "\n") + "\n}\n"
	_, err := parser.ParseFile(token.NewFileSet(), "x.go", src, parser.AllErrors)
	return err
}

func TestDocsGoBlocksParseAndRuntimeNamesExist(t *testing.T) {
	exports := runtimeExports(t)
	n := 0
	for _, b := range docBlocks(t) {
		if b.lang != "go" {
			continue
		}
		n++
		where := b.file + ":" + strconv.Itoa(b.line)
		if err := parseDocGo(b.body); err != nil {
			t.Errorf("%s: Go block does not parse: %v", where, err)
		}
	}
	if n == 0 {
		t.Fatal("no go block found: the extraction is broken")
	}
	// Every runtime.X the prose or a snippet mentions, in a code span or block.
	for _, f := range []string{"DOCS.md", "README.md"} {
		raw, _ := os.ReadFile(f)
		for _, m := range runtimeRef.FindAllStringSubmatch(string(raw), -1) {
			if m[1] != "X" && !exports[m[1]] { // runtime.X is the placeholder in "everything is spelled runtime.X"

				t.Errorf("%s mentions runtime.%s, which does not exist", f, m[1])
			}
		}
	}
}

// removedNames are spellings that no longer exist; the user documentation must not teach them.
var removedNames = regexp.MustCompile(`WithCache|NewQueriesCache|\bCache\(\)|WithMaxPartialBytes|WithPromoteThreshold|InTxOpts|params_struct|runtime\.Seq2|StatementKind|ErrNilQuery|RowsAffected(Eq|Gt|Ge|Lt|Le)|AssertRowsAffected|kind: *query|\.Into\(|BatchInto|BatchEach|\.Seek\(\)|\bq\.[A-Z]\w*Query\(|SQLGEN_FUZZ_SEEDS|sync-adapters|\w+RowCols?(All|[A-Z])|<Row>Col`)

func TestDocsDoNotMentionRemovedNames(t *testing.T) {
	for _, f := range []string{"DOCS.md", "README.md"} {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, l := range strings.Split(string(raw), "\n") {
			if m := removedNames.FindString(l); m != "" {
				t.Errorf("%s:%d mentions the removed name %q", f, i+1, m)
			}
		}
	}
}

// WhereCol takes the SQL predicate and its arguments only: the server evaluates it on every path. The
// documentation must not show the retired form with a Go predicate (a func literal or a goPred argument).
func TestDocsWhereColHasNoGoPredicate(t *testing.T) {
	n := 0
	for _, f := range []string{"DOCS.md", "README.md", "SPEC.md"} {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		s := string(raw)
		for i := strings.Index(s, "WhereCol("); i >= 0; {
			n++
			open := i + len("WhereCol")
			depth, j := 0, open
			for ; j < len(s); j++ {
				if s[j] == '(' {
					depth++
				} else if s[j] == ')' {
					if depth--; depth == 0 {
						break
					}
				}
			}
			call := s[i:min(j+1, len(s))]
			if strings.Contains(call, "func(") || strings.Contains(call, "goPred") {
				t.Errorf("%s:%d shows WhereCol with a Go predicate: %s", f, strings.Count(s[:i], "\n")+1, call)
			}
			k := strings.Index(s[open:], "WhereCol(")
			if k < 0 {
				break
			}
			i = open + k
		}
	}
	if n == 0 {
		t.Fatal("no WhereCol call found in the documentation: the scan is broken")
	}
}

// The pipeline If takes a condition and ONE closure; further arms are chained (.ElseIf / .Else) and Taken
// takes the arm index. The documentation must not show the retired If(cond, thenFn, elseFn) or Taken().
func TestDocsShowOnlyTheChainedIf(t *testing.T) {
	n := 0
	for _, f := range []string{"DOCS.md", "README.md", "SPEC.md"} {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		s := string(raw)
		if i := strings.Index(s, ".Taken()"); i >= 0 {
			t.Errorf("%s:%d shows Taken() without an arm index", f, strings.Count(s[:i], "\n")+1)
		}
		for off := 0; ; {
			k := strings.Index(s[off:], ".If(")
			if k < 0 {
				break
			}
			i := off + k + len(".If")
			off = i
			n++
			depth, commas := 0, 0
			for j := i; j < len(s); j++ {
				switch s[j] {
				case '(', '{', '[':
					depth++
				case ')', '}', ']':
					depth--
				case ',':
					if depth == 1 {
						commas++
					}
				}
				if depth == 0 {
					break
				}
			}
			if commas > 1 {
				t.Errorf("%s:%d shows If with more than a condition and one closure", f, strings.Count(s[:i], "\n")+1)
			}
		}
	}
	if n == 0 {
		t.Fatal("no .If( call found in the documentation: the scan is broken")
	}
}
