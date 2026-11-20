package integration

import (
	"os"
	"strings"
	"testing"
)

// The fuzz fixtures exist three times (PostgreSQL protocol: fzpg, MySQL protocol: fzmy, SQLite: fzsl). They must
// stay the same statements; only the dialect and, for fzmy, the missing UPDATE ... RETURNING statement
// (MySQL/MariaDB have none) may differ. The adapters of fzmy and fzsl are mechanical copies of the one of fzpg
// (`make sync-adapters`).
func TestFuzzFixturesInSync(t *testing.T) {
	read := func(p string) string {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	norm := func(s, pkg string, dropBump bool) string {
		s = strings.ReplaceAll(s, pkg, "fzX")
		var out []string
		for _, l := range strings.Split(s, "\n") {
			if !strings.HasPrefix(l, "dialect:") && !strings.HasPrefix(l, "# Fuzz/") && !strings.HasPrefix(l, "# across ") {
				out = append(out, l)
			}
		}
		s = strings.Join(out, "\n")
		if i := strings.Index(s, "  - name: FzBumpRet"); i >= 0 && dropBump {
			s = strings.TrimRight(s[:i], "\n") + "\n"
		}
		return s
	}
	pg := read("fzpg/sqlgen.yaml")
	for _, c := range []struct {
		pkg      string
		dropBump bool
	}{{"fzmy", true}, {"fzsl", false}} {
		if norm(pg, "fzpg", c.dropBump) != norm(read(c.pkg+"/sqlgen.yaml"), c.pkg, c.dropBump) {
			t.Errorf("fzpg/sqlgen.yaml and %s/sqlgen.yaml differ beyond the dialect (and FzBumpRet for fzmy)", c.pkg)
		}
		for _, f := range []string{"fuzz_adapter_%s_test.go", "fuzz_bump_%s_test.go"} {
			if c.pkg == "fzmy" && strings.HasPrefix(f, "fuzz_bump") {
				continue // hand-written: MySQL/MariaDB have no UPDATE ... RETURNING
			}
			r := strings.NewReplacer("fzpg", c.pkg, "Fzpg", strings.ToUpper(c.pkg[:1])+c.pkg[1:])
			if r.Replace(read(strings.Replace(f, "%s", "fzpg", 1))) != read(strings.Replace(f, "%s", c.pkg, 1)) {
				t.Errorf("%s is not its fzpg original with fzpg -> %s; run `make sync-adapters`", strings.Replace(f, "%s", c.pkg, 1), c.pkg)
			}
		}
	}
}
