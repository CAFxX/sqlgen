package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("write error")
}

func TestMainFunc(t *testing.T) {
	var gotCode int
	oldExit := exitFunc
	oldArgs := os.Args
	defer func() {
		exitFunc = oldExit
		os.Args = oldArgs
	}()
	exitFunc = func(code int) {
		gotCode = code
	}
	os.Args = []string{"sqlgen", "-version"}
	main()
	if gotCode != 0 {
		t.Fatalf("expected exit code 0, got %d", gotCode)
	}
}

func TestRunCLI(t *testing.T) {
	tmpDir := t.TempDir()
	validYAML := `package: demo
out: demo_gen.go
dialect: sqlite
statements:
  - name: Ping
    kind: exec
    sql: "SELECT :id:int64"
`
	cfgFile := filepath.Join(tmpDir, "sqlgen.yaml")
	if err := os.WriteFile(cfgFile, []byte(validYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	// 1. -version
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("-version code = %d, stderr = %s", code, stderr.String())
	}
	// Test binaries carry no module version, so the fallback is printed.
	if stdout.String() != "sqlgen (devel)\n" {
		t.Fatalf("unexpected version output: %q", stdout.String())
	}

	// 2. Invalid flag -> exit 2
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-unknown-flag"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected code 2 for unknown flag, got %d", code)
	}

	// 3. The configuration is given with -config only: a positional argument -> exit 2
	for _, args := range [][]string{{"a.yaml"}, {"a.yaml", "b.yaml"}, {"-config", cfgFile, "b.yaml"}} {
		stdout.Reset()
		stderr.Reset()
		if code := run(args, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "-config") {
			t.Fatalf("run(%q): expected code 2 pointing at -config, got %d: %s", args, code, stderr.String())
		}
	}

	// 5. -stdout and -check mutually exclusive -> exit 2
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", cfgFile, "-stdout", "-check"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected code 2 for -stdout and -check, got %d", code)
	}

	// 6. -check when output file doesn't exist yet -> exit 2
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", cfgFile, "-check"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected code 2 for missing output file in -check, got %d", code)
	}

	// 7. Generate -> exit 0
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", cfgFile}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected code 0 for generation, got %d: %s", code, stderr.String())
	}
	genPath := filepath.Join(tmpDir, "demo_gen.go")
	if _, err := os.Stat(genPath); err != nil {
		t.Fatalf("expected generated file at %s: %v", genPath, err)
	}

	// 8. -check when output matches -> exit 0
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", cfgFile, "-check"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected code 0 for matching -check, got %d: %s", code, stderr.String())
	}

	// 9. -check when output drifted -> exit 2
	if err := os.WriteFile(genPath, []byte("// drifted\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", cfgFile, "-check"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected code 2 for drifted -check, got %d", code)
	}

	// 10. -stdout -> exit 0 and writes package demo
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", cfgFile, "-stdout"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected code 0 for -stdout, got %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "package demo") {
		t.Fatalf("expected generated Go source on stdout, got: %s", stdout.String())
	}

	// 11. -stdout write error -> exit 1
	stderr.Reset()
	if code := run([]string{"-config", cfgFile, "-stdout"}, errWriter{}, &stderr); code != 1 {
		t.Fatalf("expected code 1 for stdout write error, got %d", code)
	}

	// 12. -out override -> exit 0
	customOut := filepath.Join(tmpDir, "custom_gen.go")
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", cfgFile, "-out", customOut}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected code 0 for -out override, got %d: %s", code, stderr.String())
	}
	if _, err := os.Stat(customOut); err != nil {
		t.Fatalf("expected custom output file %s: %v", customOut, err)
	}

	// 13. -out write failure (a parent that is a regular file cannot become a directory) -> exit 1
	if err := os.WriteFile(filepath.Join(tmpDir, "afile"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	badOut := filepath.Join(tmpDir, "afile", "out.go")
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", cfgFile, "-out", badOut}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected code 1 for invalid -out path, got %d", code)
	}

	// 14. Missing config file (with default path & with -out) -> exit 1
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", filepath.Join(tmpDir, "missing.yaml")}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected code 1 for missing config, got %d", code)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", filepath.Join(tmpDir, "missing.yaml"), "-stdout"}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected code 1 for missing config with -stdout, got %d", code)
	}

	// 15. Invalid YAML syntax -> exit 1
	badYAMLFile := filepath.Join(tmpDir, "bad.yaml")
	if err := os.WriteFile(badYAMLFile, []byte("package: [unclosed"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", badYAMLFile, "-stdout"}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected code 1 for invalid YAML, got %d", code)
	}

	// 16. Invalid SQL in config -> exit 1
	badSQLFile := filepath.Join(tmpDir, "badsql.yaml")
	badSQLYAML := `package: demo
dialect: sqlite
statements:
  - name: Bad
    kind: exec
    sql: "SELECT 'unclosed"
`
	if err := os.WriteFile(badSQLFile, []byte(badSQLYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-config", badSQLFile, "-check"}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected code 1 for invalid SQL in -check, got %d", code)
	}

	// 17. Default "sqlgen.yaml" in working directory and empty out field
	noOutYAML := `package: demo
dialect: sqlite
statements:
  - name: Ping
    kind: exec
    sql: "SELECT :id:int64"
`
	if err := os.WriteFile(cfgFile, []byte(noOutYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(wd) }()
	stdout.Reset()
	stderr.Reset()
	if code := run(nil, &stdout, &stderr); code != 0 {
		t.Fatalf("expected code 0 for default sqlgen.yaml in cwd, got %d: %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-check"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected code 0 for -check with default sqlgen_gen.go, got %d: %s", code, stderr.String())
	}
}

func TestVersionString(t *testing.T) {
	for _, tc := range []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{"no build info", nil, "sqlgen (devel)"},
		{"empty version", &debug.BuildInfo{}, "sqlgen (devel)"},
		{"devel version", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, "sqlgen (devel)"},
		{"tagged module version", &debug.BuildInfo{Main: debug.Module{Version: "v0.3.1"}}, "sqlgen v0.3.1"},
		{"pseudo version", &debug.BuildInfo{Main: debug.Module{Version: "v0.0.0-20260101000000-abcdef123456"}}, "sqlgen v0.0.0-20260101000000-abcdef123456"},
	} {
		if got := versionString(tc.info); got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

// Diagnostics from the library already start with "sqlgen:"; the CLI must not stack a second prefix.
func TestRunCLIDiagnosticsHaveASinglePrefix(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for _, tc := range []struct{ name, yaml, want string }{
		{"bad yaml", "a: [\n", "sqlgen: parse YAML:"},
		{"removed version key", "version: \"1\"\npackage: p\nstatements: []\n", "sqlgen: parse YAML:"},
		{"missing dialect", "package: p\nstatements: []\n", "sqlgen: dialect is required; set dialect: to one of postgres, cockroachdb, mysql, mariadb, sqlite"},
		{"bad dialect", "package: p\ndialect: oracle\nstatements: []\n", `sqlgen: unsupported dialect "oracle" (valid dialects:`},
	} {
		for _, mode := range [][]string{nil, {"-check"}, {"-stdout"}} {
			var stdout, stderr bytes.Buffer
			args := append([]string{"-config", write("c.yaml", tc.yaml)}, mode...)
			if code := run(args, &stdout, &stderr); code == 0 {
				t.Fatalf("%s %v: want failure", tc.name, mode)
			}
			if got := stderr.String(); strings.Contains(got, "sqlgen: sqlgen:") || !strings.HasPrefix(got, tc.want) {
				t.Errorf("%s %v: stderr = %q, want prefix %q once", tc.name, mode, got, tc.want)
			}
		}
	}
	// Missing config file: the CLI's own message, one prefix.
	var stderr bytes.Buffer
	if code := run([]string{"-config", filepath.Join(dir, "missing.yaml")}, &bytes.Buffer{}, &stderr); code == 0 || strings.Contains(stderr.String(), "sqlgen: sqlgen:") {
		t.Errorf("missing file: code=%d stderr=%q", code, stderr.String())
	}
}
