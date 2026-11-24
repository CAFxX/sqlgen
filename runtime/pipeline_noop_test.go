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

// An assertion over a no-op is evaluated (0 rows): it passes for 0 and fails, rolling back, for anything else.
func TestPipelineNoOpAssertion(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	cfg.wireTxHandler = func(context.Context, *sql.TxOptions, []PipelineOp) error {
		t.Error("a pipeline with an asserted no-op must run sequentially")
		return nil
	}
	db, cache := noopTestDB(t, cfg)

	p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	p.QueueExecNoop().AssertRowsAffected(0)
	if err := p.Flush(); err != nil {
		t.Fatalf("assertion of 0 rows over a no-op: %v", err)
	}

	p = NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	queueTestExec(p)
	p.QueueExecNoop().AssertFound()
	err := p.Flush()
	var ae *AssertionError
	if !errors.As(err, &ae) || !errors.Is(err, ErrAssertionFailed) || ae.Got != 0 || ae.Want != 1 {
		t.Fatalf("err = %v, want an assertion error (wanted 1, got 0)", err)
	}
	if cfg.commitCalls.Load() != 1 || cfg.rollbackCalls.Load() != 1 {
		t.Fatalf("commit/rollback = %d/%d, want the first pipeline committed and the failed one rolled back", cfg.commitCalls.Load(), cfg.rollbackCalls.Load())
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
