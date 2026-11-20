package integration

// Seeded differential ("fuzz") tests of the GENERATED code against every real engine and an in-memory
// oracle written in plain Go. Every case draws its data and parameters from a PCG stream derived from one
// seed; a failure prints it, and SQLGEN_FUZZ_SEED=<n> replays exactly the same cases:
//
//	SQLGEN_FUZZ_SEED=1234 SQLGEN_ENGINES=postgres18 go test -run TestFuzz ./integration/
//
// The oracle never calls the product: tables are filled and read back with hand-written SQL, so a bug in
// the generator or runtime cannot cancel out.
//
// Collation: engines disagree on how text sorts (MySQL's default collation is case-insensitive, PostgreSQL
// follows the locale, CockroachDB is binary). Every text column therefore holds ASCII lowercase letters
// 'a'..'c' only, no spaces and no wildcards in stored data, for which all of them agree with Go's byte-wise
// comparison; every other sort key is numeric.

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/CAFxX/sqlgen/pgxwire"
	"github.com/CAFxX/sqlgen/runtime"
)

// ---- engines, seed, rng, failure reporting ----

var sqliteEngine = engine{name: "sqlite", driver: "sqlite", family: famSQLite, dialect: runtime.DialectQuestion}

// forEachFuzzEngine runs fn on every configured server engine and on SQLite (an in-process file database, so
// it needs no DSN and also runs in the hermetic `go test`; SQLGEN_ENGINES must name it when set).
func forEachFuzzEngine(t *testing.T, fn func(t *testing.T, e engine, db *sql.DB)) {
	t.Helper()
	forEachEngine(t, fn)
	// The PostgreSQL-protocol engines once more through pgxwire's connector, whose connections encode arguments
	// and scan results themselves (the single-round-trip pipeline driver).
	for _, e := range engines {
		dsn := os.Getenv(e.env)
		if !e.isPG() || dsn == "" || (selectedEngines() != nil && !selectedEngines()[e.name]) {
			continue
		}
		t.Run(e.name+"-wire", func(t *testing.T) {
			db, err := pgxwire.OpenDB(dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			fn(t, e, db)
		})
	}
	if sel := selectedEngines(); sel != nil && !sel["sqlite"] {
		return
	}
	t.Run("sqlite", func(t *testing.T) {
		dsn := "file:" + filepath.Join(t.TempDir(), "fz.db") + "?_pragma=busy_timeout(20000)"
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		fn(t, sqliteEngine, db)
	})
}

func fuzzSeed(t *testing.T) uint64 {
	t.Helper()
	if v := strings.TrimSpace(os.Getenv("SQLGEN_FUZZ_SEED")); v != "" {
		s, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("SQLGEN_FUZZ_SEED=%q: %v", v, err)
		}
		return s
	}
	return uint64(time.Now().UnixNano())
}

type fz struct {
	t    *testing.T
	e    engine
	db   *sql.DB
	r    *rand.Rand
	seed uint64
	ctx  context.Context
	bad  int
	// noStop makes failf only record the failure (t.FailNow may only run on the test goroutine).
	noStop bool
	// extreme makes genRows draw the sort keys k1..k8 from {MinInt64, -1, 0, 1, MaxInt64} (keyset tests only:
	// aggregates and arithmetic on such values overflow by design).
	extreme bool
}

// fork returns an independent generator (own rng and failure count) for a worker goroutine.
func (f *fz) fork(i int) *fz {
	c := *f
	c.r = rand.New(rand.NewPCG(f.seed, uint64(i)+0x9e3779b97f4a7c15))
	c.noStop, c.bad = true, 0
	return &c
}

// newFz derives the per-test rng from the seed and the test name, so a test replays the same cases on
// every engine and independently of which other tests ran.
func newFz(t *testing.T, e engine, db *sql.DB, seed uint64) *fz {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.SplitN(t.Name(), "/", 2)[0]))
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("reproduce with SQLGEN_FUZZ_SEED=%d SQLGEN_ENGINES=%s go test -run '%s' ./integration/",
				seed, e.name, strings.SplitN(t.Name(), "/", 2)[0])
		}
	})
	return &fz{t: t, e: e, db: db, r: rand.New(rand.NewPCG(seed, h.Sum64())), seed: seed, ctx: fuzzCtx(t)}
}

// fuzzCtx bounds a whole test (the suite is slow under -race and with SQLGEN_FUZZ_SCALE).
func fuzzCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// failf records a failure and stops the test after a handful, so one bug does not drown the output.
func (f *fz) failf(format string, args ...any) {
	f.t.Helper()
	f.t.Errorf("[seed=%d] %s", f.seed, fmt.Sprintf(format, args...))
	if f.bad++; f.bad >= 5 && !f.noStop {
		f.t.FailNow()
	}
}

func (f *fz) intn(n int) int                     { return f.r.IntN(n) }
func (f *fz) chance(p float64) bool              { return f.r.Float64() < p }
func (f *fz) rangeI(lo, hi int) int              { return lo + f.r.IntN(hi-lo+1) }
func (f *fz) pick(xs ...int) int                 { return xs[f.r.IntN(len(xs))] }
func (f *fz) shuffle(n int, swap func(i, j int)) { f.r.Shuffle(n, swap) }

// ---- schema and data ----

const fzDDL = `CREATE TABLE fz_rows (
	id   BIGINT PRIMARY KEY,
	grp  BIGINT NOT NULL,
	k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, k3 BIGINT NOT NULL, k4 BIGINT NOT NULL,
	k5 BIGINT NOT NULL, k6 BIGINT NOT NULL, k7 BIGINT NOT NULL, k8 BIGINT NOT NULL,
	tag  VARCHAR(8) NOT NULL,
	tag2 VARCHAR(8) NOT NULL,
	note VARCHAR(16) NULL
)`

func setupFz(t *testing.T, db *sql.DB) {
	t.Helper()
	mustExec(t, db, "DROP TABLE IF EXISTS fz_rows")
	mustExec(t, db, fzDDL)
	shortGC(t, db, "fz_rows")
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS fz_rows") })
}

// shortGC lets CockroachDB drop the old versions of a fuzz table after a few seconds instead of the default
// hours: the suite deletes and reloads the table thousands of times, which would otherwise pile up in the
// compose stack's 1 GiB in-memory store until the node dies.
func shortGC(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	var version string
	if err := db.QueryRow("SELECT version()").Scan(&version); err == nil && strings.Contains(version, "CockroachDB") {
		mustExec(t, db, "ALTER TABLE "+table+" CONFIGURE ZONE USING gc.ttlseconds = 10")
	}
}

func (f *fz) ph(n int) string {
	if f.e.isPG() {
		return "$" + strconv.Itoa(n)
	}
	return "?"
}

// reset replaces the table content with rows, using hand-written SQL.
func (f *fz) reset(rows []fzRow) {
	f.t.Helper()
	mustExec(f.t, f.db, "DELETE FROM fz_rows")
	for len(rows) > 0 {
		n := min(len(rows), 40)
		var sb strings.Builder
		sb.WriteString("INSERT INTO fz_rows (id, grp, k1, k2, k3, k4, k5, k6, k7, k8, tag, tag2, note) VALUES ")
		var args []any
		for i, r := range rows[:n] {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteByte('(')
			for c := 0; c < fzNumCols; c++ {
				if c > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(f.ph(len(args) + 1))
				switch {
				case c == fzColID:
					args = append(args, r.ID)
				case c == fzColGrp:
					args = append(args, r.Grp)
				case c >= fzColK1 && c <= fzColK8:
					args = append(args, r.K[c-fzColK1])
				case c == fzColTag:
					args = append(args, r.Tag)
				case c == fzColTag2:
					args = append(args, r.Tag2)
				default:
					if r.Note == nil {
						args = append(args, nil)
					} else {
						args = append(args, *r.Note)
					}
				}
			}
			sb.WriteByte(')')
		}
		mustExec(f.t, f.db, sb.String(), args...)
		rows = rows[n:]
	}
}

// dump reads the whole table back with hand-written SQL, ordered by id.
func (f *fz) dump() []fzRow {
	f.t.Helper()
	rs, err := f.db.Query("SELECT id, grp, k1, k2, k3, k4, k5, k6, k7, k8, tag, tag2, note FROM fz_rows ORDER BY id")
	if err != nil {
		f.t.Fatal(err)
	}
	defer rs.Close()
	var out []fzRow
	for rs.Next() {
		var r fzRow
		var note sql.NullString
		if err := rs.Scan(&r.ID, &r.Grp, &r.K[0], &r.K[1], &r.K[2], &r.K[3], &r.K[4], &r.K[5], &r.K[6], &r.K[7],
			&r.Tag, &r.Tag2, &note); err != nil {
			f.t.Fatal(err)
		}
		if note.Valid {
			s := note.String
			r.Note = &s
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

func (f *fz) str(alphabet string, minLen, maxLen int) string {
	n := f.rangeI(minLen, maxLen)
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[f.intn(len(alphabet))]
	}
	return string(b)
}

// genRows draws n rows with unique, sparse ids and a random per-dataset cardinality for every sort column
// (1..6 distinct values), so keys tie heavily and sometimes a whole column is constant.
func (f *fz) genRows(n int) []fzRow {
	ids := map[int64]bool{}
	card := func(max int) int64 { return int64(f.pick(1, 2, 2, 3, 3, max)) }
	cGrp, cK := card(4), [8]int64{}
	for i := range cK {
		cK[i] = card(6)
	}
	cK[0] = int64(f.pick(3, 10))
	rows := make([]fzRow, 0, n)
	for len(rows) < n {
		id := int64(f.rangeI(1, n*4+8))
		if ids[id] {
			continue
		}
		ids[id] = true
		r := fzRow{ID: id, Grp: f.r.Int64N(cGrp), Tag: f.str("abc", 1, 3), Tag2: f.str("ab", 0, 2)}
		for i := range r.K {
			r.K[i] = f.r.Int64N(cK[i])
			if f.extreme {
				r.K[i] = []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}[r.K[i]%5]
			}
		}
		if !f.chance(0.3) {
			s := f.str("abc", 0, 3)
			r.Note = &s
		}
		rows = append(rows, r)
	}
	return rows
}

// ---- oracle ----

func (r fzRow) String() string {
	note := "NULL"
	if r.Note != nil {
		note = strconv.Quote(*r.Note)
	}
	return fmt.Sprintf("{id=%d grp=%d k=%v tag=%q tag2=%q note=%s}", r.ID, r.Grp, r.K, r.Tag, r.Tag2, note)
}

func rowEq(a, b fzRow) bool {
	if a.ID != b.ID || a.Grp != b.Grp || a.K != b.K || a.Tag != b.Tag || a.Tag2 != b.Tag2 {
		return false
	}
	return (a.Note == nil) == (b.Note == nil) && (a.Note == nil || *a.Note == *b.Note)
}

// diffRows describes the first difference between two row lists ("" when equal).
func diffRows(got, want []fzRow) string {
	for i := 0; i < min(len(got), len(want)); i++ {
		if !rowEq(got[i], want[i]) {
			return fmt.Sprintf("row %d: got %v want %v (got %d rows, want %d)", i, got[i], want[i], len(got), len(want))
		}
	}
	if len(got) != len(want) {
		return fmt.Sprintf("got %d rows, want %d (first extra/missing at index %d)", len(got), len(want), min(len(got), len(want)))
	}
	return ""
}

func sortByID(rows []fzRow) []fzRow {
	out := slices.Clone(rows)
	slices.SortFunc(out, func(a, b fzRow) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// project zeroes the columns a SelectCols mask leaves out (0 selects everything).
func project(r fzRow, mask uint64) fzRow {
	if mask == 0 {
		return r
	}
	var o fzRow
	keep := func(c int) bool { return mask&(1<<c) != 0 }
	if keep(fzColID) {
		o.ID = r.ID
	}
	if keep(fzColGrp) {
		o.Grp = r.Grp
	}
	for i := 0; i < 8; i++ {
		if keep(fzColK1 + i) {
			o.K[i] = r.K[i]
		}
	}
	if keep(fzColTag) {
		o.Tag = r.Tag
	}
	if keep(fzColTag2) {
		o.Tag2 = r.Tag2
	}
	if keep(fzColNote) {
		o.Note = r.Note
	}
	return o
}

// like implements SQL LIKE (% and _, no escapes) for the ASCII data used here.
func like(s, pat string) bool {
	if pat == "" {
		return s == ""
	}
	switch pat[0] {
	case '%':
		for i := 0; i <= len(s); i++ {
			if like(s[i:], pat[1:]) {
				return true
			}
		}
		return false
	case '_':
		return s != "" && like(s[1:], pat[1:])
	}
	return s != "" && s[0] == pat[0] && like(s[1:], pat[1:])
}

// match is the Go rendering of FzSearch's WHERE clause.
func (a fzSearchArgs) match(r fzRow) bool {
	if r.K[0] < a.Lo {
		return false
	}
	if a.ByGrp && r.Grp != a.Grp {
		return false
	}
	if a.ByGrps && !slices.Contains(a.Grps, r.Grp) {
		return false
	}
	if a.ByTags && !slices.Contains(a.Tags, r.Tag) {
		return false
	}
	if a.FilterNote && (r.Note == nil || !like(*r.Note, a.NoteLike)) { // LIKE on NULL is unknown: filtered
		return false
	}
	if a.NoteNull && r.Note != nil {
		return false
	}
	if a.ByPairs && !slices.Contains(a.Pairs, [2]int64{r.Grp, r.K[1]}) {
		return false
	}
	if a.Narrow && (r.K[2] < a.Lo || r.K[2] > a.Hi) {
		return false
	}
	if a.HiK4 && r.K[3] > a.Hi {
		return false
	}
	return true
}

// emptySlice reports whether an enabled slice parameter is empty (the statement must then fail with
// runtime.ErrEmptySliceParam without touching the database).
func (a fzSearchArgs) emptySlice() bool {
	return (a.ByGrps && len(a.Grps) == 0) || (a.ByTags && len(a.Tags) == 0) || (a.ByPairs && len(a.Pairs) == 0)
}

// subIDs evaluates a composed sub-query tree over rows.
func subIDs(rows []fzRow, s *fzSub) map[int64]bool {
	out := map[int64]bool{}
	var inner map[int64]bool
	if s.Note {
		inner = subIDs(rows, s.Inner)
	}
	for _, r := range rows {
		if s.Note {
			if inner[r.ID] && r.Note != nil && (!s.MaxK3 || r.K[2] <= s.K3Max) {
				out[r.ID] = true
			}
		} else if slices.Contains(s.Grps, r.Grp) && (!s.MinK2 || r.K[1] >= s.K2Min) {
			out[r.ID] = true
		}
	}
	return out
}

// subEmpty reports whether some leaf of the tree has an empty IN list (ErrEmptySliceParam).
func subEmpty(s *fzSub) bool {
	if s == nil {
		return false
	}
	if s.Note {
		return subEmpty(s.Inner)
	}
	return len(s.Grps) == 0
}

// pageTerms returns the effective sort of an FzPage call: the terms given (in order) plus, when the
// tie-breaker id is not named, id in the direction of the last term (ascending for an empty Order).
func pageTerms(terms []fzTerm) []fzTerm {
	idCol := len(fzOrderCols) - 1
	out := slices.Clone(terms)
	if !slices.ContainsFunc(out, func(t fzTerm) bool { return t.Col == idCol }) {
		desc := len(out) > 0 && out[len(out)-1].Desc
		out = append(out, fzTerm{idCol, desc})
	}
	return out
}

func fzColCmp(a, b fzRow, col int) int {
	switch {
	case col == fzColID:
		return cmp.Compare(a.ID, b.ID)
	case col == fzColGrp:
		return cmp.Compare(a.Grp, b.Grp)
	case col >= fzColK1 && col <= fzColK8:
		return cmp.Compare(a.K[col-fzColK1], b.K[col-fzColK1])
	case col == fzColTag:
		return strings.Compare(a.Tag, b.Tag)
	case col == fzColTag2:
		return strings.Compare(a.Tag2, b.Tag2)
	}
	panic("fzColCmp: bad column")
}

func sortRows(rows []fzRow, terms []fzTerm) []fzRow {
	eff := pageTerms(terms)
	out := slices.Clone(rows)
	slices.SortStableFunc(out, func(a, b fzRow) int {
		for _, t := range eff {
			c := fzColCmp(a, b, fzOrderCols[t.Col])
			if t.Desc {
				c = -c
			}
			if c != 0 {
				return c
			}
		}
		return 0
	})
	return out
}

// pageWant is the oracle result of an FzPage query ignoring After/Limit: filtered and totally sorted.
func pageWant(rows []fzRow, a fzPageArgs) []fzRow {
	var ids map[int64]bool
	if a.InSub {
		ids = subIDs(rows, a.Sub)
	}
	var kept []fzRow
	for _, r := range rows {
		switch {
		case r.K[0] < a.Lo:
		case a.ByGrps && !slices.Contains(a.Grps, r.Grp):
		case a.InSub && !ids[r.ID]:
		case a.SkipTag && r.Tag == a.ExTag:
		default:
			kept = append(kept, r)
		}
	}
	return sortRows(kept, a.Terms)
}

// litRows returns rows as FzInsertLit stores them: only id, grp, k1 and tag are parameters, the other columns are
// the literals of the tuple template (k2..k8 = 1..7, tag2 = 'a,)(' - with a comma and parentheses inside
// the quotes - and note = NULL).
func litRows(rows []fzRow) []fzRow {
	out := slices.Clone(rows)
	for i := range out {
		for k := 1; k < 8; k++ {
			out[i].K[k] = int64(k)
		}
		out[i].Tag2, out[i].Note = "a,)(", nil
	}
	return out
}

// ---- random parameter generation ----

func (f *fz) distinctInts(n int, gen func() int64) []int64 {
	seen := map[int64]bool{}
	var out []int64
	for len(out) < n {
		if v := gen(); !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func (f *fz) searchArgs(rows []fzRow, flags int) fzSearchArgs {
	a := fzSearchArgs{
		Lo: int64(f.intn(4)), Hi: int64(f.intn(4)),
		ByGrp: flags&1 != 0, ByGrps: flags&2 != 0, ByTags: flags&4 != 0, FilterNote: flags&8 != 0,
		NoteNull: flags&16 != 0, ByPairs: flags&32 != 0, Narrow: flags&64 != 0, HiK4: flags&128 != 0,
	}
	a.Grp = int64(f.intn(4))
	// Slice parameters are filled regardless of their flag: a disabled block must ignore them, and
	// an enabled one with an empty list must fail with ErrEmptySliceParam.
	if !f.chance(0.15) {
		a.Grps = f.distinctInts(f.rangeI(1, 3), func() int64 { return int64(f.intn(5)) })
	}
	if !f.chance(0.15) {
		seen := map[string]bool{}
		for n := f.rangeI(1, 4); len(seen) < n; {
			if t := f.str("abc", 1, 3); !seen[t] {
				seen[t] = true
				a.Tags = append(a.Tags, t)
			}
		}
	}
	a.NoteLike = []string{"a%", "%b", "%c%", "ab", "_a%", "%", "", "b_", "ca%"}[f.intn(9)]
	if !f.chance(0.15) {
		seen := map[[2]int64]bool{}
		for n := f.rangeI(1, 5); len(seen) < n; {
			p := [2]int64{int64(f.intn(4)), int64(f.intn(4))}
			if len(rows) > 0 && f.chance(0.6) {
				r := rows[f.intn(len(rows))]
				p = [2]int64{r.Grp, r.K[1]}
			}
			if !seen[p] {
				seen[p] = true
				a.Pairs = append(a.Pairs, p)
			}
		}
	}
	if f.chance(0.25) {
		a.SelectCols = uint64(f.rangeI(1, 1<<fzNumCols-1))
	}
	return a
}

func (f *fz) newSub(depth int, nonEmpty bool) *fzSub {
	if depth > 1 && f.chance(0.7) {
		return &fzSub{Note: true, Inner: f.newSub(depth-1, nonEmpty), MaxK3: f.chance(0.5), K3Max: int64(f.intn(3))}
	}
	s := &fzSub{MinK2: f.chance(0.5), K2Min: int64(f.intn(3))}
	if n := f.rangeI(1, 3); nonEmpty || !f.chance(0.1) {
		s.Grps = f.distinctInts(n, func() int64 { return int64(f.intn(5)) })
	}
	return s
}

// pageTermsRand draws a random Order spec of 0..12 terms: a random subset of the sort columns in random
// order, mostly with the tie-breaker id last (or omitted, or occasionally somewhere in the middle).
func (f *fz) pageTermsRand() []fzTerm {
	idCol := len(fzOrderCols) - 1
	n := f.rangeI(0, len(fzOrderCols))
	if n == 0 {
		return nil
	}
	cols := f.r.Perm(idCol)
	cols = cols[:min(n, idCol)]
	switch {
	case f.chance(0.6):
		cols = append(cols, idCol)
	case f.chance(0.1):
		cols = slices.Insert(cols, f.intn(len(cols)+1), idCol)
	}
	mode := f.intn(5) // 0: all asc, 1: all desc, else mixed
	terms := make([]fzTerm, len(cols))
	for i, c := range cols {
		terms[i] = fzTerm{Col: c, Desc: mode == 1 || (mode > 1 && f.chance(0.5))}
	}
	return terms
}

func flipTerms(ts []fzTerm) []fzTerm {
	out := slices.Clone(ts)
	for i := range out {
		out[i].Desc = !out[i].Desc
	}
	return out
}

func fmtTerms(ts []fzTerm) string {
	var sb strings.Builder
	for i, t := range ts {
		if i > 0 {
			sb.WriteString(", ")
		}
		d := "asc"
		if t.Desc {
			d = "desc"
		}
		fmt.Fprintf(&sb, "%d %s", t.Col, d)
	}
	return "[" + sb.String() + "]"
}
