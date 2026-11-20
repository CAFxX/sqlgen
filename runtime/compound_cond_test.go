package runtime

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func compoundFixture(t *testing.T) (p *Pipeline, d *ExecFuture, g *RowFuture[int64], upd *QueryDescriptor) {
	t.Helper()
	get := &QueryDescriptor{
		Name: "Get", StmtID: 2, StmtVerb: VerbSelect,
		Segments:       []string{"SELECT ", " FROM t WHERE id = ", ""},
		Slots:          []Slot{{Kind: SlotSelectCols}, {Kind: SlotScalar}},
		SelectColExprs: []string{"id"},
	}
	del := &QueryDescriptor{
		Name: "Del", StmtID: 1, StmtVerb: VerbDelete,
		Segments: []string{"DELETE FROM t WHERE id = ", ""},
		Slots:    []Slot{{Kind: SlotScalar}},
	}
	upd = &QueryDescriptor{
		Name: "Upd", StmtID: 3, StmtVerb: VerbUpdate,
		Segments: []string{"UPDATE t SET v = v + ", " WHERE ", "id = ", "", ""},
		Slots:    []Slot{{Kind: SlotScalar}, {Kind: SlotBranchGuardOpen}, {Kind: SlotScalar}, {Kind: SlotBranchGuardClose}},
	}
	p = NewPipeline(context.Background(), nil, nil, nil, true, DialectDollar)
	d = p.QueueExec(1, del, RenderInput{}, MakeVariantKey(1, 0, 0, 0), nil, nil, nil)
	g = QueueRow[int64](p, 2, get, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, nil, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) }, func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) })
	return p, d, g, upd
}

// Each leaf of a compound condition must be evaluated over its own operation's rows: the earlier
// operation records its leaf server-side, and the latest operation's statement reads the record.
func TestCompoundConditionLeavesAreRecordedByTheirOwnOperation(t *testing.T) {
	p, d, g, upd := compoundFixture(t)
	cond := d.NotFound().And(g.Found())
	r := p.If(cond, func() { p.QueueExec(3, upd, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil) }, nil)
	_ = r
	p.syncOpsFromPtrs()
	RenderPipelineBatch(DialectDollar, nil, p.ops)

	del, get := p.ops[0], p.ops[1]
	if cond.Left.Recorded != 0 || cond.Right.Recorded != 0 || cond.Left.Op != d.op {
		t.Fatalf("If modified the caller's condition: %+v", cond)
	}
	if len(del.Decorators.LeafRecs) != 1 || len(get.Decorators.LeafRecs) != 0 {
		t.Fatalf("leaf records: del=%d get=%d, want 1 / 0", len(del.Decorators.LeafRecs), len(get.Decorators.LeafRecs))
	}
	id := del.Decorators.LeafRecs[0].CondID
	// the earlier operation evaluates ITS leaf over ITS rows and records it ...
	for _, want := range []string{
		"WITH _sqlgen_op AS (DELETE FROM t WHERE id = $1 RETURNING 1), _sqlgen_l2 AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.l2', CASE WHEN (COUNT(*) = 0) THEN 'true' ELSE 'false' END, true) = 'true' AS _ok FROM _sqlgen_op)",
		"SELECT (SELECT COUNT(*) FROM _sqlgen_op) AS _cnt, (SELECT _ok FROM _sqlgen_l2) AS _l2",
	} {
		if id != 2 || !strings.Contains(del.SQL, want) {
			t.Fatalf("del SQL lacks %q (record id %d):\n%s", want, id, del.SQL)
		}
	}
	// ... and the later one combines the recorded value with its own leaf; it must NOT evaluate the
	// earlier operation's leaf (COUNT(*) = 0) over its own rows
	want := "set_config('sqlgen.c1', CASE WHEN ((COALESCE(current_setting('sqlgen.l2', true) = 'true', false) AND COUNT(*) > 0)) THEN 'true' ELSE 'false' END, true)"
	if !strings.Contains(get.SQL, want) || strings.Contains(get.SQL, "COUNT(*) = 0") {
		t.Fatalf("get SQL does not combine the recorded leaf:\n%s", get.SQL)
	}
	for i, op := range p.ops[:2] {
		if est := EstimateOpBytes(op.Desc, &op.Input, &op.Decorators); est < len(op.SQL) {
			t.Fatalf("op %d: EstimateOpBytes %d < rendered %d", i, est, len(op.SQL))
		}
	}
}

func TestCompoundConditionBindingShapes(t *testing.T) {
	p, d, g, upd := compoundFixture(t)
	queueUpd := func() { p.QueueExec(3, upd, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil) }

	// equal leaves of one operation share a record; different ones do not
	p.If(d.NotFound().And(g.Found()), queueUpd, nil)
	p.If(d.NotFound().Or(g.NotFound()), queueUpd, nil)
	p.If(d.RowsAffectedEq(3).Or(g.Found()), queueUpd, nil)
	// a WhereCol with a different predicate is a different leaf
	p.If(g.WhereCol("id > 1", func(int64) bool { return true }).And(d.Found()).Not(), queueUpd, nil)
	// a leaf without an operation is FALSE, as in Go
	p.If(WhereCol[int64](nil, "id > 1", nil).Or(g.Found()), queueUpd, nil)
	p.syncOpsFromPtrs()
	RenderPipelineBatch(DialectDollar, nil, p.ops)

	del, get := p.ops[0], p.ops[1]
	if n := len(del.Decorators.LeafRecs); n != 3 { // d.NotFound (shared), d.Eq(3), d.Found
		t.Fatalf("del records %d leaves, want 3: %+v", n, del.Decorators.LeafRecs)
	}
	if n := len(get.Decorators.LeafRecs); n != 0 {
		t.Fatalf("the latest operation records %d leaves, want none", n)
	}
	if !strings.Contains(get.SQL, "NOT ((COUNT(*) > 0 AND MIN(CASE WHEN (id > 1) THEN 1 ELSE 0 END) = 1 AND COALESCE(current_setting('sqlgen.l") ||
		!strings.Contains(get.SQL, "(FALSE OR COUNT(*) > 0)") ||
		!strings.Contains(get.SQL, "MIN(CASE WHEN (id > 1) THEN 1 ELSE 0 END) = 1") {
		t.Fatalf("unexpected condition SQL:\n%s", get.SQL)
	}
	if n := strings.Count(get.SQL, "set_config("); n != 5 {
		t.Fatalf("%d recorded conditions on the latest operation, want 5:\n%s", n, get.SQL)
	}
	for i, op := range p.ops[:2] {
		if est := EstimateOpBytes(op.Desc, &op.Input, &op.Decorators); est < len(op.SQL) {
			t.Fatalf("op %d: EstimateOpBytes %d < rendered %d", i, est, len(op.SQL))
		}
	}
}

// An operation that did not run (its branch gates are closed) counts as FALSE in a condition, also under
// Not, exactly like PipelineCond.EvalGoFn: its statement must say so instead of evaluating over zero rows.
func TestConditionLeavesOfGatedOperationsAreFalseWhenTheOperationDidNotRun(t *testing.T) {
	sel := &QueryDescriptor{StmtVerb: VerbSelect, Segments: []string{"SELECT id FROM t WHERE id = ", ""}, Slots: []Slot{{Kind: SlotScalar}}}
	gates := []BranchGate{{CondID: 4, WantTrue: true}, {CondID: 5, WantTrue: false}}
	dec := OpDecorators{AssertRows: -1, ProduceCond: &PipelineCond{Kind: CondRowsEq, Op: &PipelineOp{}, Threshold: 0}, CondID: 6,
		LeafRecs: []ProducedCond{{CondID: 7, Cond: &PipelineCond{Kind: CondRowsGt, Op: &PipelineOp{}}}}}
	got := renderPipelineOp(t, DialectDollar, sel, RenderInput{BranchGates: gates}, dec)
	for _, want := range []string{
		"set_config('sqlgen.c6', CASE WHEN (CASE WHEN (current_setting('sqlgen.c4', true) = 'true' AND current_setting('sqlgen.c5', true) = 'false') THEN (COUNT(*) = 0) ELSE FALSE END)",
		"set_config('sqlgen.l7', CASE WHEN (CASE WHEN (current_setting('sqlgen.c4', true) = 'true' AND current_setting('sqlgen.c5', true) = 'false') THEN (COUNT(*) > 0) ELSE FALSE END)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	// ungated operations keep the plain form
	got = renderPipelineOp(t, DialectDollar, sel, RenderInput{}, dec)
	if !strings.Contains(got, "CASE WHEN (COUNT(*) = 0) THEN 'true'") || strings.Contains(got, "ELSE FALSE END") {
		t.Errorf("ungated condition changed:\n%s", got)
	}
}
