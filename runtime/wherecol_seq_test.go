package runtime

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

// On the sequential path the WhereCol predicate is evaluated by the server too: it is one more column of
// the operation's own statement (never a Go callback), read back and hidden from the scanned row.

type wcRow struct {
	id  int64
	tag string
}

func wcSQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "wc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER, note TEXT);
		INSERT INTO t VALUES (1, 10, 'a'), (2, 20, NULL), (3, 30, 'c');`); err != nil {
		t.Fatal(err)
	}
	return db
}

var (
	// a placeholder in the output column list (before the WhereCol columns) and one after them
	wcByMin = &QueryDescriptor{Name: "ByMin", StmtID: 1, StmtVerb: VerbSelect,
		Segments: []string{"SELECT id, ", " AS tag ", "FROM t WHERE id >= ", " ORDER BY id"},
		Slots:    []Slot{{Kind: SlotScalar}, {Kind: SlotColPreds}, {Kind: SlotScalar}}}
	wcOne = &QueryDescriptor{Name: "One", StmtID: 2, StmtVerb: VerbSelect,
		Segments: []string{"SELECT score ", "FROM t WHERE id = ", ""},
		Slots:    []Slot{{Kind: SlotColPreds}, {Kind: SlotScalar}}}
	wcCount = &QueryDescriptor{Name: "Count", StmtID: 3, StmtVerb: VerbSelect,
		Segments: []string{"SELECT COUNT(*) AS n ", "FROM t WHERE id > ", ""}, Slots: []Slot{{Kind: SlotColPreds}, {Kind: SlotScalar}}}
	wcNoSlot = &QueryDescriptor{Name: "Union", StmtID: 4, StmtVerb: VerbSelect,
		Segments: []string{"SELECT id FROM t UNION SELECT id FROM t"}}
)

func wcSlice(p *Pipeline, desc *QueryDescriptor, args ...any) *SliceFuture[wcRow] {
	return QueueSlice[wcRow](p, desc.StmtID, desc, RenderInput{}, MakeVariantKey(desc.StmtID, 0, 0, 0), nil, &args, nil,
		func(s RowScanner, d *wcRow) error { return s.Scan(&d.id, &d.tag) })
}

func wcOneRow(p *Pipeline, desc *QueryDescriptor, args ...any) *RowFuture[int64] {
	return QueueRow[int64](p, desc.StmtID, desc, RenderInput{}, MakeVariantKey(desc.StmtID, 0, 0, 0), nil, &args, nil, scanInt)
}

func TestWhereColSequentialIsEvaluatedByTheServer(t *testing.T) {
	db := wcSQLite(t)
	p := NewPipeline(context.Background(), db, NewStmtCache(), nil, false, DialectQuestion)
	type check struct {
		name string
		r    *IfChain[*Pipeline]
		want bool
	}
	var checks []check
	is := func(name string, c PipelineCond, want bool) {
		checks = append(checks, check{name, p.If(c, nil), want})
	}

	s := wcSlice(p, wcByMin, "T", int64(2))
	is("slice: every row", s.WhereCol("score >= ?", int64(20)), true)
	is("slice: a NULL row is not TRUE", s.WhereCol("score >= ? AND note IS NOT NULL", int64(20)), false)
	is("slice: SQL only", s.WhereCol("length(?) = 3 AND typeof(note) IN ('text', 'null')", "abc"), true)

	g := wcOneRow(p, wcOne, int64(2))
	null := g.WhereCol("score > (SELECT score FROM t WHERE note = 'x')")
	is("row: NULL is false", null, false)
	is("row: NOT NULL leaf is true", null.Not(), true)
	is("row: two columns on one future", g.WhereCol("score = ?", int64(20)).And(g.WhereCol("score = ?", int64(21)).Not()), true)

	missing := wcOneRow(p, wcOne, int64(99))
	is("row: no row", missing.WhereCol("1 = 1"), false)
	is("row: no row, negated", missing.WhereCol("1 = 1").Not(), true)

	n := wcOneRow(p, wcCount, int64(0))
	is("aggregate in the predicate", n.WhereCol("COUNT(*) = ?", int64(3)).And(g.Found()), true)

	empty := QueueSliceNoop[wcRow](p)
	is("NoOp: no row", empty.WhereCol("1 = 1"), false)

	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, c := range checks {
		if c.r.Taken(0) != c.want {
			t.Errorf("%s: taken = %v, want %v", c.name, c.r.Taken(0), c.want)
		}
	}
	// the WhereCol columns never reach the scanned rows, and the arguments bind in order around them
	rows, err := s.Get()
	if want := []wcRow{{2, "T"}, {3, "T"}}; err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, %v; want %v", rows, err, want)
	}
	if v, err := g.Get(); err != nil || v != 20 {
		t.Fatalf("row = %v, %v", v, err)
	}
}

func TestWhereColErrors(t *testing.T) {
	db := wcSQLite(t)
	// a statement that has no place for the column fails the build: nothing runs
	p := NewPipeline(context.Background(), db, NewStmtCache(), nil, false, DialectQuestion)
	u := wcSlice(p, wcNoSlot)
	p.If(u.WhereCol("id > 0"), nil)
	if err := p.Flush(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("no slot: err = %v", err)
	}
	// the server rejects a bad predicate on the sequential path as well
	p = NewPipeline(context.Background(), db, NewStmtCache(), nil, false, DialectQuestion)
	g := wcOneRow(p, wcOne, int64(1))
	p.If(g.WhereCol("no_such_column = 1"), nil)
	if err := p.Flush(); err == nil {
		t.Fatal("a predicate over an unknown column must fail")
	}
}
