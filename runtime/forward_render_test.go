package runtime

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

// Hermetic shapes of value forwarding: the SQL each wire flavor renders for producers and consumers, the
// argument compaction of the PostgreSQL flavor, the translation of the guard errors and the build errors.

var (
	fwdInsertOrder = &QueryDescriptor{Name: "InsertOrder", StmtID: 101, StmtVerb: VerbInsert, HasReturning: true,
		Segments: []string{"INSERT INTO orders (customer) ", "", "", " RETURNING id"}, Slots: []Slot{{Kind: SlotInsertValuesOpen}, {Kind: SlotScalar, Cast: "text"}, {Kind: SlotInsertValuesClose}},
		ResultCols: []string{"id"}, ResultExpr: "id"}
	fwdGetOrder = &QueryDescriptor{Name: "GetOrder", StmtID: 102, StmtVerb: VerbSelect,
		Segments: []string{"SELECT id FROM orders WHERE customer = ", ""}, Slots: []Slot{{Kind: SlotScalar}},
		ResultCols: []string{"id"}, ResultExpr: "id"}
	fwdGetTwo = &QueryDescriptor{Name: "GetTwo", StmtID: 103, StmtVerb: VerbSelect,
		Segments: []string{"SELECT id, customer FROM orders"}, ResultCols: []string{"id", "customer"}}
	fwdInsertPlain = &QueryDescriptor{Name: "InsertPlain", StmtID: 104, StmtVerb: VerbInsert,
		Segments: []string{"INSERT INTO orders (customer) ", "", "", ""}, Slots: []Slot{{Kind: SlotInsertValuesOpen}, {Kind: SlotScalar}, {Kind: SlotInsertValuesClose}}}
	fwdInsertLine = &QueryDescriptor{Name: "InsertLine", StmtID: 105, StmtVerb: VerbInsert,
		Segments: []string{"INSERT INTO lines (order_id, sku, qty) ", "", ", ", ", ", "", ""},
		Slots:    []Slot{{Kind: SlotInsertValuesOpen}, {Kind: SlotScalar, Cast: "bigint"}, {Kind: SlotScalar, Cast: "text"}, {Kind: SlotScalar, Cast: "bigint"}, {Kind: SlotInsertValuesClose}}}
	fwdUpdate = &QueryDescriptor{Name: "Touch", StmtID: 106, StmtVerb: VerbUpdate,
		Segments: []string{"UPDATE orders SET a = ", ", b = ", " WHERE id = ", ""},
		Slots:    []Slot{{Kind: SlotScalar, Cast: "bigint"}, {Kind: SlotScalar}, {Kind: SlotScalar, Cast: "bigint"}}}
)

func fwdPipeline(d Dialect) *Pipeline {
	return NewPipeline(context.Background(), nil, NewStmtCache(), nil, false, d)
}

func fwdRow(p *Pipeline, desc *QueryDescriptor, args ...any) *RowFuture[int64] {
	return QueueRow[int64](p, desc.StmtID, desc, RenderInput{}, MakeVariantKey(desc.StmtID, 0, 0, 0), nil, &args, nil, nil, nil)
}

func fwdExec(p *Pipeline, desc *QueryDescriptor, args ...any) *ExecFuture {
	f := fwdPlain(p, desc, args...)
	p.BindForwarded()
	return f
}

func fwdPlain(p *Pipeline, desc *QueryDescriptor, args ...any) *ExecFuture {
	return p.QueueExec(desc.StmtID, desc, RenderInput{}, MakeVariantKey(desc.StmtID, 0, 0, 0), nil, &args, nil)
}

func fwdOps(p *Pipeline) []PipelineOp {
	p.syncOpsFromPtrs()
	return p.ops
}

const fwdSlot1 = "(CASE WHEN current_setting('sqlgen.v1', true) LIKE 'v%' THEN substr(current_setting('sqlgen.v1', true), 2) END)"

func TestForwardPostgresShapes(t *testing.T) {
	p := fwdPipeline(DialectDollar)
	order := fwdRow(p, fwdInsertOrder, "alice")
	id := order.Arg()
	if id2 := order.Arg(); id2 != id {
		t.Fatal("Arg must reuse the record")
	}
	fwdExec(p, fwdInsertLine, id.Bind(), "sku", int64(2))
	var thenLine *ExecFuture
	p.If(order.Found(), func() {
		thenLine = fwdExec(p, fwdUpdate, id.Bind(), "b", id.Bind())
	}, nil)
	ops := fwdOps(p)
	if err := renderPipelineBatch(DialectDollar, nil, ops); err != nil {
		t.Fatal(err)
	}
	want := "WITH _sqlgen_op AS (INSERT INTO orders (customer) VALUES ($1) RETURNING id), " +
		"_sqlgen_cond AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.c1', CASE WHEN (COUNT(*) > 0) THEN 'true' ELSE 'false' END, true) = 'true' AS _ok FROM _sqlgen_op), " +
		"_sqlgen_v1 AS (SELECT set_config('sqlgen.v1', CASE COUNT(*) WHEN 1 THEN COALESCE('v' || MIN((id)::text), 'n') ELSE 'x' || COUNT(*)::text END, true) AS _ok FROM _sqlgen_op) " +
		"SELECT _r.* FROM (SELECT 1 AS _o, (SELECT _ok FROM _sqlgen_cond) AS _c, (SELECT _ok FROM _sqlgen_v1) AS _v1) AS _f " +
		"CROSS JOIN LATERAL (SELECT * FROM _sqlgen_op WHERE _f._o IS NOT NULL AND _f._c IS NOT NULL AND _f._v1 IS NOT NULL) AS _r"
	if ops[0].SQL != want {
		t.Fatalf("producer:\n got %s\nwant %s", ops[0].SQL, want)
	}
	if ops[0].CheckSQL != "" {
		t.Fatal("a producer has no check")
	}
	if want := "INSERT INTO lines (order_id, sku, qty) VALUES (" + fwdSlot1 + "::bigint, $1, $2)"; ops[1].SQL != want {
		t.Fatalf("consumer:\n got %s\nwant %s", ops[1].SQL, want)
	}
	if want := "SELECT CASE WHEN COALESCE(left(current_setting('sqlgen.v1', true), 1), '') NOT IN ('v', 'n') THEN ('sqlgen:ref:1:' || COALESCE(current_setting('sqlgen.v1', true), '?'))::int ELSE 1 END"; ops[1].CheckSQL != want {
		t.Fatalf("check:\n got %s\nwant %s", ops[1].CheckSQL, want)
	}
	if len(ops[1].Args) != 2 || ops[1].Args[0] != "sku" || ops[1].Args[1] != int64(2) {
		t.Fatalf("compacted args %v", ops[1].Args)
	}
	// gated, the same value twice: one check per value, the gates first
	if want := "UPDATE orders SET a = " + fwdSlot1 + "::bigint, b = $1 WHERE id = " + fwdSlot1 + "::bigint AND current_setting('sqlgen.c1', true) = 'true'"; !strings.HasPrefix(ops[2].SQL, "UPDATE orders SET a = "+fwdSlot1+"::bigint, b = $1 WHERE ") || strings.Count(ops[2].SQL, fwdSlot1) != 2 {
		t.Fatalf("gated consumer:\n got %s\nwant like %s", ops[2].SQL, want)
	}
	if want := "SELECT CASE WHEN (current_setting('sqlgen.c1', true) = 'true') IS NOT TRUE THEN 1 WHEN COALESCE(left(current_setting('sqlgen.v1', true), 1), '') NOT IN ('v', 'n') THEN ('sqlgen:ref:1:' || COALESCE(current_setting('sqlgen.v1', true), '?'))::int ELSE 1 END"; ops[2].CheckSQL != want {
		t.Fatalf("gated check:\n got %s\nwant %s", ops[2].CheckSQL, want)
	}
	if len(ops[2].Args) != 1 || ops[2].Args[0] != "b" || !ops[2].Forwards() {
		t.Fatalf("compacted args %v", ops[2].Args)
	}
	// A declined batch runs sequentially with the original arguments; rendering again is idempotent.
	dropCondArgs(ops)
	if len(ops[1].Args) != 3 || ops[1].CheckSQL != "" {
		t.Fatalf("restored args %v", ops[1].Args)
	}
	if _, ok := ops[1].Args[0].(*argRef); !ok {
		t.Fatal("the forwarded value must be restored")
	}
	if err := renderPipelineBatch(DialectDollar, nil, ops); err != nil || len(ops[1].Args) != 2 {
		t.Fatalf("second render: %v %v", err, ops[1].Args)
	}
	_ = thenLine

	// The check's marker becomes the *ArgError the sequential path returns.
	pgErr := func(msg string) error { return &fakeSQLState{state: "22P02", msg: msg} }
	err := TranslateWireError(&ops[2], pgErr(`invalid input syntax for type integer: "sqlgen:ref:1:x2"`))
	var ae *ArgError
	if !errors.As(err, &ae) || !errors.Is(err, ErrArgUnresolved) {
		t.Fatalf("translate: %v", err)
	}
	if want := (ArgError{Statement: "Touch", Op: 2, Producer: "InsertOrder", ProducerOp: 0, Got: 2, GotKnown: true}); *ae != want {
		t.Fatalf("%+v", *ae)
	}
	if err := TranslateWireError(&ops[1], pgErr(`could not parse "sqlgen:ref:1:?" as type int`)); !errors.As(err, &ae) || ae.GotKnown {
		t.Fatalf("translate unknown count: %v", err)
	}
	for _, msg := range []string{`"sqlgen:ref:9:x0"`, `"sqlgen:ref:x"`, `something else`} {
		in := pgErr(msg)
		if err := TranslateWireError(&ops[1], in); err != in {
			t.Fatalf("%s: translated to %v", msg, err)
		}
	}
}

type fakeSQLState struct{ state, msg string }

func (e *fakeSQLState) Error() string    { return e.msg }
func (e *fakeSQLState) SQLState() string { return e.state }

func TestForwardQuestionDialectIsNotPostgresWire(t *testing.T) {
	p := fwdPipeline(DialectQuestion)
	fwdExec(p, fwdInsertLine, fwdRow(p, fwdInsertOrder, "a").Arg().Bind(), "s", int64(1))
	if err := renderPipelineBatch(DialectQuestion, nil, fwdOps(p)); !errors.Is(err, ErrWireNotApplicable) {
		t.Fatalf("err = %v", err)
	}
}

func TestForwardBuildErrors(t *testing.T) {
	p := fwdPipeline(DialectDollar)
	fwdPlain(p, fwdInsertPlain, "a").LastInsertIDArg()
	if !errors.Is(p.buildErr, ErrInvalidArgument) || !strings.Contains(p.buildErr.Error(), "RETURNING") {
		t.Fatalf("LastInsertIDArg on $N: %v", p.buildErr)
	}
	p = fwdPipeline(DialectQuestion)
	_ = fwdRow(p, fwdGetTwo).Arg()
	if !errors.Is(p.buildErr, ErrInvalidArgument) {
		t.Fatalf("two columns: %v", p.buildErr)
	}
	var nilRow *RowFuture[int64]
	var nilExec *ExecFuture
	if nilRow.Arg().Forwarded() || nilExec.LastInsertIDArg().Forwarded() {
		t.Fatal("nil futures forward nothing")
	}
	(&Pipeline{}).BindForwarded() // nothing queued: no-op
	if (&ArgError{Producer: "P", LastInsertID: true}).Error() == (&ArgError{Producer: "P"}).Error() {
		t.Fatal("messages must differ")
	}
	for _, e := range []ArgError{{LastInsertID: true, GotKnown: true, Got: 1}, {LastInsertID: true, GotKnown: true}, {LastInsertID: true}, {GotKnown: true, Label: "l", ProducerLabel: "pl"}, {}} {
		if !strings.Contains(e.Error(), "forwarded argument unresolved") {
			t.Fatal(e.Error())
		}
	}
}

func TestForwardMySQLShapes(t *testing.T) {
	fl := &MySQLFlavor{Returning: true, Forwarding: true}
	p := fwdPipeline(DialectQuestion)
	sel := fwdRow(p, fwdGetOrder, "alice").Label("lookup")
	ins := fwdRow(p, fwdInsertOrder, "bob")
	lid := fwdPlain(p, fwdInsertPlain, "carol")
	fwdExec(p, fwdInsertLine, sel.Arg().Bind(), ins.Arg().Bind(), lid.LastInsertIDArg().Bind())
	ops := fwdOps(p)
	b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"SET @sqlgen_p=-1;SET @sqlgen_f=-1;SET @sqlgen_r=0;START TRANSACTION;",
		"SET @sqlgen_p=0;SET @sqlgen_v1=NULL;SELECT _sqlgen_op.*,(@sqlgen_v1:=_sqlgen_op.id) FROM (SELECT id FROM orders WHERE customer = 'alice') AS _sqlgen_op;SET @sqlgen_n0=FOUND_ROWS();",
		"SET @sqlgen_p=1;SET @sqlgen_k=0;SET @sqlgen_v2=NULL;INSERT INTO orders (customer) VALUES ('bob') RETURNING id, (@sqlgen_k:=@sqlgen_k+1), (@sqlgen_v2:=id);SET @sqlgen_n1=@sqlgen_k;",
		"SET @sqlgen_p=2;DO LAST_INSERT_ID(0);INSERT INTO orders (customer) VALUES ('carol');SET @sqlgen_n2=ROW_COUNT();SET @sqlgen_v3=LAST_INSERT_ID();",
		"SET @sqlgen_p=3;SET @sqlgen_r=IF(@sqlgen_f<0 AND (1) AND NOT COALESCE(@sqlgen_n0=1,0),1,@sqlgen_r);SET @sqlgen_f=IF(@sqlgen_f<0 AND (1) AND NOT COALESCE(@sqlgen_n0=1,0),7,@sqlgen_f);",
		"SET @sqlgen_r=IF(@sqlgen_f<0 AND (1) AND NOT COALESCE(@sqlgen_n2=1 AND @sqlgen_v3<>0,0),3,@sqlgen_r);",
		"INSERT INTO lines (order_id, sku, qty) SELECT @sqlgen_v1, @sqlgen_v2, @sqlgen_v3 FROM (SELECT 1) AS _sqlgen_dual WHERE @sqlgen_f<0;",
		"SELECT @sqlgen_f,@sqlgen_r,@sqlgen_n0,@sqlgen_n1,@sqlgen_n2,@sqlgen_n3;",
	} {
		if !strings.Contains(b.SQL, want) {
			t.Fatalf("batch lacks\n%s\nin\n%s", want, b.SQL)
		}
	}
	if b.TrimColumns(0) != 1 || b.TrimColumns(1) != 2 || b.TrimColumns(2) != 0 {
		t.Fatalf("trim %d %d %d", b.TrimColumns(0), b.TrimColumns(1), b.TrimColumns(2))
	}
	if est := 1024 + estimateMySQLOpBytes(fl, &ops[0], ops[0].Args) + estimateMySQLOpBytes(fl, &ops[1], ops[1].Args) +
		estimateMySQLOpBytes(fl, &ops[2], ops[2].Args) + estimateMySQLOpBytes(fl, &ops[3], ops[3].Args); est < len(b.SQL) {
		t.Fatalf("estimate %d < %d", est, len(b.SQL))
	}

	// The summary of a failed consumer: @sqlgen_f = len(ops)+3, @sqlgen_r names the value.
	failed, err := b.Apply(ops, []any{int64(4 + 3), int64(1), int64(2), int64(1), int64(1), int64(0)})
	var ae *ArgError
	if failed != 3 || !errors.As(err, &ae) {
		t.Fatalf("Apply = %d, %v", failed, err)
	}
	if want := (ArgError{Statement: "InsertLine", Op: 3, Producer: "GetOrder", ProducerOp: 0, ProducerLabel: "lookup", Got: 2, GotKnown: true}); *ae != want {
		t.Fatalf("%+v", *ae)
	}
	if _, err := b.Apply(ops, []any{int64(4 + 3), int64(99), int64(2), int64(1), int64(1), int64(0)}); err == nil {
		t.Fatal("an unknown value ID must be an error")
	}
	if f, err := b.Apply(ops, []any{int64(-1), int64(0), int64(1), int64(1), int64(1), int64(1)}); f != -1 || err != nil {
		t.Fatalf("success: %d %v", f, err)
	}

	// Without the capability the batch is not applicable (it runs sequentially).
	if _, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: &MySQLFlavor{Returning: true}}); !errors.Is(err, ErrWireNotApplicable) {
		t.Fatalf("no forwarding: %v", err)
	}

	// A user transaction: the consumer's check needs no savepoint; a gated consumer checks its gates.
	p = fwdPipeline(DialectQuestion)
	o := fwdRow(p, fwdGetOrder, "x")
	p.If(o.Found(), func() { fwdExec(p, fwdInsertLine, o.Arg().Bind(), "s", int64(1)) }, nil)
	b, err = RenderMySQLBatch(fwdOps(p), MySQLRenderOptions{Flavor: fl, InTx: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := "SET @sqlgen_r=IF(@sqlgen_f<0 AND (@sqlgen_c1=1) AND NOT COALESCE(@sqlgen_n0=1,0),1,@sqlgen_r);"; !strings.Contains(b.SQL, want) || strings.Contains(b.SQL, "SAVEPOINT") {
		t.Fatalf("gated consumer in a user tx:\n%s", b.SQL)
	}
}

// Sequential resolution needs no database for the rule itself: a skipped or wrongly sized producer is an
// *ArgError naming the first unresolved value in argument order.
func TestForwardResolveRefs(t *testing.T) {
	prod := &PipelineOp{opIdx: 0, RowCount: 1, Desc: fwdGetOrder}
	ok := &argRef{op: prod, id: 1, value: func() (any, bool) { return int64(5), true }}
	bad := &argRef{op: &PipelineOp{opIdx: 1, RowCount: 3}, id: 2, value: func() (any, bool) { return int64(6), true }}
	op := &PipelineOp{opIdx: 2, Args: []any{ok, "x", bad, ok}}
	err := resolveRefs(op)
	var ae *ArgError
	if !errors.As(err, &ae) || ae.ProducerOp != 1 || ae.Got != 3 {
		t.Fatalf("err = %v", err)
	}
	if op.Args[0] != int64(5) {
		t.Fatal("the resolved value must be bound")
	}
	bad.op.RowCount = 1
	if err := resolveRefs(op); err != nil || op.Args[2] != int64(6) || op.Args[3] != int64(5) {
		t.Fatalf("%v %v", err, op.Args)
	}
	prod.Skipped = true
	op.Args[0] = ok
	if err := resolveRefs(op); !errors.As(err, &ae) || ae.GotKnown {
		t.Fatalf("skipped producer: %v", err)
	}
	_ = sql.ErrNoRows
}
