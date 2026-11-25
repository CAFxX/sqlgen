package runtime

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// A NoOp operation (an empty multi-row VALUES collection) never reaches a driver: not the wire pipeliner,
// not the sequential path, and a pipeline made only of such operations opens no connection and no
// transaction. Its futures resolve to "nothing happened".

func noopTestDB(t *testing.T, cfg *mockDriverConfig) (*sql.DB, *StmtCache) {
	t.Helper()
	return newWireTxDB(t, cfg)
}

func TestPipelineNoOpIsNotSentOnTheWire(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *mockDriverConfig
	}{
		{"wireTxPipeliner", &mockDriverConfig{enableWireTx: true}},
		{"wirePipeliner", &mockDriverConfig{enableWire: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []int
			handler := func(ops []PipelineOp) {
				for i := range ops {
					if ops[i].NoOp {
						t.Errorf("op %d: a NoOp operation reached the wire pipeliner", i)
					}
					if ops[i].SQL == "" {
						t.Errorf("op %d reached the wire pipeliner unrendered", i)
					}
					ops[i].RowCount = 3
					if ops[i].OnDone != nil {
						ops[i].OnDone(3, nil)
					}
				}
				seen = append(seen, len(ops))
			}
			tc.cfg.wireTxHandler = func(_ context.Context, _ *sql.TxOptions, ops []PipelineOp) error { handler(ops); return nil }
			tc.cfg.wireHandler = func(_ context.Context, ops []PipelineOp) error { handler(ops); return nil }
			db, cache := noopTestDB(t, tc.cfg)

			var before, noop, after *ExecFuture
			var noopRows *SliceFuture[int64]
			p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
			before = queueTestExec(p)
			noop = p.QueueExecNoop()
			noopRows = QueueSliceNoop[int64](p)
			after = queueTestExec(p)
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if len(seen) != 1 || seen[0] != 2 {
				t.Fatalf("the wire pipeliner saw batches %v, want one batch of the 2 real operations", seen)
			}
			for name, f := range map[string]*ExecFuture{"before": before, "after": after} {
				if n, err := f.RowsAffected(); err != nil || n != 3 {
					t.Fatalf("%s: RowsAffected = %d, %v", name, n, err)
				}
			}
			if n, err := noop.RowsAffected(); err != nil || n != 0 {
				t.Fatalf("no-op exec: RowsAffected = %d, %v; want 0, nil", n, err)
			}
			if res, err := noop.Result(); err != nil {
				t.Fatalf("no-op exec: Result error %v", err)
			} else if n, _ := res.RowsAffected(); n != 0 {
				t.Fatalf("no-op exec: Result affected %d rows", n)
			}
			if rows, err := noopRows.Get(); err != nil || len(rows) != 0 {
				t.Fatalf("no-op rows: %v, %v; want none", rows, err)
			}
			if st := cache.Stats(); st.WirePipelines != 1 || st.SequentialPipelines != 0 {
				t.Fatalf("stats = %+v, want one wire pipeline", st)
			}
		})
	}
}

func TestPipelineOnlyNoOpsSendNothing(t *testing.T) {
	for _, mode := range []string{"standalone", "PipelineTx"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &mockDriverConfig{enableWireTx: true}
			cfg.wireTxHandler = func(context.Context, *sql.TxOptions, []PipelineOp) error {
				t.Error("the wire pipeliner was called for a pipeline of no-ops")
				return nil
			}
			db, cache := noopTestDB(t, cfg)
			var ex *ExecFuture
			var sl *SliceFuture[int64]
			var err error
			if mode == "standalone" {
				p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
				ex, sl = p.QueueExecNoop(), QueueSliceNoop[int64](p)
				err = p.Flush()
			} else {
				err = pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
					ex, sl = p.QueueExecNoop(), QueueSliceNoop[int64](p)
					return nil
				})
			}
			if err != nil {
				t.Fatalf("flush: %v", err)
			}
			if n, err := ex.RowsAffected(); err != nil || n != 0 {
				t.Fatalf("RowsAffected = %d, %v", n, err)
			}
			if rows, err := sl.Get(); err != nil || len(rows) != 0 {
				t.Fatalf("Get = %v, %v", rows, err)
			}
			if b, e, q, w := cfg.beginCalls.Load(), cfg.execCalls.Load(), cfg.queryCalls.Load(), cfg.wireTxCalls.Load()+cfg.wireCalls.Load(); b+e+q+w != 0 {
				t.Fatalf("driver was used: begin=%d exec=%d query=%d wire=%d", b, e, q, w)
			}
			if st := cache.Stats(); st.WirePipelines+st.SequentialPipelines != 0 {
				t.Fatalf("stats = %+v, want no pipeline counted", st)
			}
		})
	}
}

func TestPipelineNoOpSequentialSkipsSQL(t *testing.T) {
	cfg := &mockDriverConfig{}
	db, cache := noopTestDB(t, cfg)
	p := NewPipeline(context.Background(), db, cache, nil, false, DialectDollar)
	a := queueTestExec(p)
	p.QueueExecNoop()
	b := queueTestExec(p)
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.RowsAffected(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RowsAffected(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.execCalls.Load(); got != 2 {
		t.Fatalf("driver executed %d statements, want 2 (the no-op sends none)", got)
	}
}

// A condition over a no-op reads zero rows; a no-op in an untaken branch is skipped like any operation.
func TestPipelineNoOpConditionsAndBranches(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	cfg.wireTxHandler = func(context.Context, *sql.TxOptions, []PipelineOp) error {
		t.Error("a pipeline whose condition depends on a no-op must run sequentially")
		return nil
	}
	db, cache := noopTestDB(t, cfg)
	p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	noop := p.QueueExecNoop()
	var thenExec, elseNoop *ExecFuture
	res := p.If(noop.NotFound(), func() { thenExec = queueTestExec(p) }, func() { elseNoop = p.QueueExecNoop() })
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if !res.Taken() {
		t.Fatal("NotFound over a no-op must hold")
	}
	if n, err := thenExec.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("then branch: %d, %v", n, err)
	}
	if _, err := elseNoop.RowsAffected(); !errors.Is(err, ErrBranchNotTaken) {
		t.Fatalf("no-op in the untaken branch: err = %v, want ErrBranchNotTaken", err)
	}
	if st := cache.Stats(); st.SequentialPipelines != 1 {
		t.Fatalf("stats = %+v, want the pipeline on the sequential path", st)
	}
}

// An assertion over a no-op is decided client-side (its row count is 0): a passing one costs nothing (a
// pipeline of only such no-ops sends nothing, not even BEGIN; with real operations it keeps its single wire
// round trip), a failing one fails the pipeline before anything is sent. Inside an If branch the assertion
// applies only if the branch is taken, so the pipeline runs on the sequential path.
func TestPipelineNoOpAssertion(t *testing.T) {
	var wireOps []int
	cfg := &mockDriverConfig{enableWireTx: true}
	cfg.wireTxHandler = func(_ context.Context, _ *sql.TxOptions, ops []PipelineOp) error {
		wireOps = append(wireOps, len(ops))
		for i := range ops {
			ops[i].RowCount = 1
			if ops[i].OnDone != nil {
				ops[i].OnDone(1, nil)
			}
		}
		return nil
	}
	db, cache := noopTestDB(t, cfg)
	sent := func() int64 {
		return cfg.beginCalls.Load() + cfg.execCalls.Load() + cfg.queryCalls.Load() + cfg.wireTxCalls.Load() + cfg.wireCalls.Load()
	}

	// Only asserted no-ops: nothing at all reaches the driver.
	p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	only := p.QueueExecNoop()
	only.AssertRows(0)
	if err := p.Flush(); err != nil {
		t.Fatalf("assertion of 0 rows over a no-op: %v", err)
	}
	if n := sent(); n != 0 {
		t.Fatalf("a pipeline of asserted no-ops used the driver %d times, want 0", n)
	}
	if n, err := only.RowsAffected(); err != nil || n != 0 {
		t.Fatalf("no-op future: %d, %v", n, err)
	}

	// With a real operation: one wire round trip carrying only the real operation.
	p = NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	real := queueTestExec(p)
	p.QueueExecNoop().AssertRows(0)
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(wireOps) != 1 || wireOps[0] != 1 || cfg.beginCalls.Load() != 0 {
		t.Fatalf("wire batches %v, begin=%d; want one wire round trip of the real operation", wireOps, cfg.beginCalls.Load())
	}
	if n, err := real.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("real op: %d, %v", n, err)
	}
	if st := cache.Stats(); st.WirePipelines != 1 || st.SequentialPipelines != 0 {
		t.Fatalf("stats = %+v, want one wire pipeline", st)
	}

	// A failing assertion: the same error the sequential path returns, nothing sent.
	before := sent()
	p = NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	queueTestExec(p)
	failing := p.QueueExecNoop().AssertFound()
	err := p.Flush()
	var ae *AssertionError
	if !errors.As(err, &ae) || !errors.Is(err, ErrAssertionFailed) || ae.Got != 0 || ae.Want != 1 || !ae.GotKnown {
		t.Fatalf("err = %v, want an assertion error (wanted 1, got 0)", err)
	}
	// The failing operation's own future carries the error, as for any other failing operation.
	if n, ferr := failing.RowsAffected(); ferr != err || n != 0 {
		t.Fatalf("failing no-op future: %d, %v; want 0, %v", n, ferr, err)
	}
	if _, ferr := failing.Result(); ferr != err {
		t.Fatalf("failing no-op future Result: %v; want %v", ferr, err)
	}
	if n := sent() - before; n != 0 {
		t.Fatalf("a failing no-op assertion used the driver %d times, want 0", n)
	}

	// Inside an If branch: sequential path (the assertion holds only if the branch is taken).
	before = sent()
	p = NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	gate := queueTestExec(p)
	p.If(gate.Found(), func() { p.QueueExecNoop().AssertRows(0) }, nil)
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(wireOps) != 1 || cfg.beginCalls.Load() != 1 || sent()-before < 2 {
		t.Fatalf("wire batches %v, begin=%d: want the gated assertion on the sequential path", wireOps, cfg.beginCalls.Load())
	}
	if cfg.rollbackCalls.Load() != 0 {
		t.Fatalf("rollbacks = %d, want none", cfg.rollbackCalls.Load())
	}

	// A failing assertion inside a taken branch (sequential path): the future carries the error too.
	p = NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	gate = queueTestExec(p)
	var inBranch *SliceFuture[int]
	p.If(gate.Found(), func() { inBranch = QueueSliceNoop[int](p).AssertRows(2) }, nil)
	err = p.Flush()
	if !errors.As(err, &ae) || ae.Want != 2 || ae.Got != 0 {
		t.Fatalf("err = %v, want an assertion error (wanted 2, got 0)", err)
	}
	if rows, ferr := inBranch.Get(); ferr != err || rows != nil {
		t.Fatalf("failing no-op slice future: %v, %v; want nil, %v", rows, ferr, err)
	}
}

// Inside a user-managed transaction on a wire-capable connection the no-ops are filtered out as well.
func TestPipelineNoOpInUserTx(t *testing.T) {
	cfg := &mockDriverConfig{enableWire: true}
	cfg.wireHandler = func(_ context.Context, ops []PipelineOp) error {
		for i := range ops {
			if ops[i].NoOp {
				t.Errorf("op %d: a NoOp operation reached the wire pipeliner", i)
			}
			ops[i].RowCount = 5
		}
		if len(ops) != 1 {
			t.Errorf("wire batch has %d ops, want 1", len(ops))
		}
		return nil
	}
	cache := NewStmtCache()
	t.Cleanup(func() { _ = cache.Close() })
	db := sql.OpenDB(WrapConnector(&mockConnector{cfg: cfg}))
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	p := NewPipeline(context.Background(), db, cache, NewTxStmtCache(tx), true, DialectDollar)
	n1 := p.QueueExecNoop()
	real := queueTestExec(p)
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if n, err := real.RowsAffected(); err != nil || n != 5 {
		t.Fatalf("real op: %d, %v", n, err)
	}
	if n, err := n1.RowsAffected(); err != nil || n != 0 {
		t.Fatalf("no-op: %d, %v", n, err)
	}
	if cfg.wireCalls.Load() != 1 {
		t.Fatalf("wire calls = %d, want 1", cfg.wireCalls.Load())
	}
}
