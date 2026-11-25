package runtime

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// WhereCol binds its arguments: never inlined into the SQL text on the PostgreSQL-protocol path, numbered
// after the operation's own placeholders and appended to its arguments in order.
func TestWhereColArgumentsAreBoundOnTheDollarPath(t *testing.T) {
	p, d, g, upd := compoundFixture(t)
	g.op.Args = []any{int64(7)}
	queueUpd := func() { p.QueueExec(3, upd, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil) }
	// g's own leaf, evaluated by g (the latest operation of the condition)
	p.If(d.NotFound().And(g.WhereCol("id >= ? AND note <> '?' AND id < ?", int64(3), "x")), queueUpd, nil)
	p.syncOpsFromPtrs()
	renderPipelineBatch(DialectDollar, nil, p.ops)
	get := p.ops[1]
	if want := "CASE WHEN (id >= $2 AND note <> '?' AND id < $3) THEN 1 ELSE 0 END"; !strings.Contains(get.SQL, want) {
		t.Fatalf("get SQL lacks %q:\n%s", want, get.SQL)
	}
	if want := []any{int64(7), int64(3), "x"}; !reflect.DeepEqual(get.Args, want) {
		t.Fatalf("get args = %v, want %v", get.Args, want)
	}
	if est := estimateOpBytes(get.Desc, &get.Input, &get.Decorators); est < len(get.SQL) {
		t.Fatalf("estimateOpBytes %d < rendered %d", est, len(get.SQL))
	}
	// Rendering again (a retried batch) does not append the arguments twice.
	renderPipelineBatch(DialectDollar, nil, p.ops)
	if len(p.ops[1].Args) != 3 {
		t.Fatalf("args after a second render = %v", p.ops[1].Args)
	}
	// Dropping them restores the operation's own arguments (the sequential fallback binds only those).
	dropCondArgs(p.ops)
	if !reflect.DeepEqual(p.ops[1].Args, []any{int64(7)}) {
		t.Fatalf("args after dropCondArgs = %v", p.ops[1].Args)
	}
}

// A leaf recorded by an earlier operation binds its arguments on that operation; the question dialect
// keeps '?'.
func TestWhereColArgumentsOfARecordedLeaf(t *testing.T) {
	p, d, g, upd := compoundFixture(t)
	queueUpd := func() { p.QueueExec(3, upd, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil) }
	p.If(g.WhereCol("id = ?", int64(9)).And(d.Found()), queueUpd, nil)
	// d was queued before g: the latest operation is g, so g evaluates its own leaf. Queue a later
	// operation so g's leaf is recorded by g and read by the later one.
	p2, _, g2, _ := compoundFixture(t)
	late := p2.QueueExec(1, d.op.Desc, RenderInput{}, MakeVariantKey(1, 0, 0, 0), nil, nil, nil)
	p2.If(g2.WhereCol("id = ?", int64(9)).And(late.Found()), func() {}, nil)
	for _, pl := range []*Pipeline{p, p2} {
		pl.syncOpsFromPtrs()
		renderPipelineBatch(DialectQuestion, nil, pl.ops)
		get := pl.ops[1]
		if !strings.Contains(get.SQL, "CASE WHEN (id = ?) THEN 1 ELSE 0 END") || !reflect.DeepEqual(get.Args, []any{int64(9)}) {
			t.Fatalf("get SQL/args = %s / %v", get.SQL, get.Args)
		}
		for i := range pl.ops {
			if i != 1 && len(pl.ops[i].Args) != 0 {
				t.Fatalf("op %d got the leaf's arguments: %v", i, pl.ops[i].Args)
			}
		}
	}
}

// The number of '?' markers outside quotes must match the arguments, on both futures.
func TestWhereColPlaceholderCountMismatchFailsTheBuild(t *testing.T) {
	for _, c := range []struct {
		pred string
		args []any
	}{
		{"id = ?", nil},
		{"id = ? AND v = ?", []any{1}},
		{"id = 1", []any{1}},
		{`"a?" = 'b?' AND ` + "`c?`" + ` = 1`, []any{1}},
	} {
		p, _, g, _ := compoundFixture(t)
		_ = g.WhereCol(c.pred, c.args...)
		if !errors.Is(p.buildErr, ErrInvalidArgument) || !strings.Contains(p.buildErr.Error(), "WhereCol") {
			t.Fatalf("%q with %d args: build error %v", c.pred, len(c.args), p.buildErr)
		}
		p2 := NewPipeline(context.Background(), nil, nil, nil, true, DialectDollar)
		s := QueueSlice[int64](p2, 2, g.op.Desc, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, nil, nil,
			func(s RowScanner, dst *int64) error { return s.Scan(dst) })
		_ = s.WhereCol(c.pred, c.args...)
		if !errors.Is(p2.buildErr, ErrInvalidArgument) {
			t.Fatalf("slice %q: build error %v", c.pred, p2.buildErr)
		}
	}
	p, _, g, _ := compoundFixture(t)
	_ = g.WhereCol(`name = 'it''s ?' AND id = ?`, 1)
	if p.buildErr != nil {
		t.Fatalf("a doubled quote inside a literal: %v", p.buildErr)
	}
}

// Equal leaves share a record only when neither binds arguments.
func TestWhereColLeavesWithArgumentsAreNotShared(t *testing.T) {
	p, _, g, upd := compoundFixture(t)
	late := p.QueueExec(3, upd, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil)
	p.If(g.WhereCol("id = ?", 1).And(late.Found()), func() {}, nil)
	p.If(g.WhereCol("id = ?", 2).And(late.Found()), func() {}, nil)
	if n := len(g.op.Decorators.LeafRecs); n != 2 {
		t.Fatalf("leaf records = %d, want 2", n)
	}
}

// When the pipeliner declines a prerendered batch, the sequential fallback binds only the operation's own
// arguments.
func TestWhereColArgumentsDroppedOnSequentialFallback(t *testing.T) {
	cfg := &mockDriverConfig{enableWire: true, rowsCols: []string{"id"}}
	var wireArgs int
	cfg.wireHandler = func(_ context.Context, ops []PipelineOp) error {
		wireArgs = len(ops[1].Args)
		return ErrWireNotApplicable
	}
	db := sql.OpenDB(WrapConnector(&mockConnector{cfg: cfg}))
	defer db.Close()
	cache := NewStmtCache()
	defer func() { _ = cache.Close() }()
	_, d, g, upd := compoundFixture(t)
	p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	del := p.QueueExec(1, d.op.Desc, RenderInput{}, MakeVariantKey(1, 0, 0, 0), nil, argsOf(int64(1)), nil)
	get := QueueRow[int64](p, 2, g.op.Desc, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, argsOf(int64(2)), nil,
		func(s RowScanner, dst *int64) error { return s.Scan(dst) })
	p.If(del.Found().And(get.WhereCol("id > ?", int64(0))), func() {
		p.QueueExec(3, upd, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, argsOf(int64(1), int64(2)), nil)
	}, nil)
	_ = p.Flush()
	if wireArgs != 2 {
		t.Fatalf("the pipeliner saw %d arguments on the get, want 2", wireArgs)
	}
	if n := len(p.ops[1].Args); n != 1 {
		t.Fatalf("the sequential fallback bound %d arguments, want 1", n)
	}
}

func argsOf(vals ...any) *[]any {
	ab := AcquireArgsCap(len(vals))
	*ab = append(*ab, vals...)
	return ab
}

// MySQL inlines the arguments as literals, by the same rules as the operation's own arguments.
func TestWhereColArgumentsInlinedOnMySQL(t *testing.T) {
	fl := &MySQLFlavor{Returning: true}
	var sel shapeDesc
	for _, sd := range shapeDescs() {
		if sd.name == "select" {
			sel = sd
		}
	}
	ops := []PipelineOp{mkOp(sel, nil, OpDecorators{AssertRows: -1})}
	ops[0].Args = []any{int64(5)}
	ops[0].Input.ColPreds = []ColPred{{SQL: "score > ? AND name <> ?", Args: []any{int32(1), "o'k"}}}
	ops[0].Decorators.ProduceCond = &PipelineCond{Kind: CondColPred, Op: &ops[0], Col: 1}
	ops[0].Decorators.CondID = 2
	b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := "CASE WHEN (score > 1 AND name <> 'o''k') THEN 1 ELSE 0 END"; !strings.Contains(b.SQL, want) {
		t.Fatalf("got\n%s\nwant it to contain\n%s", b.SQL, want)
	}
	// an argument that has no literal form makes the batch not applicable (sequential fallback)
	ops[0].Input.ColPreds[0].Args = []any{struct{}{}, "x"}
	if _, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: true}); !errors.Is(err, ErrWireNotApplicable) {
		t.Fatalf("unsupported argument: %v", err)
	}
}
