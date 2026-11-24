package runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// mockWireTxConn is a mockWireConn that can also run a whole transaction in one call.
type mockWireTxConn struct {
	mockWireConn
}

func (w *mockWireTxConn) ExecTxPipeline(ctx context.Context, txOpts *sql.TxOptions, ops []PipelineOp) error {
	w.cfg.wireTxCalls.Add(1)
	if h := w.cfg.wireTxHandler; h != nil {
		return h(ctx, txOpts, ops)
	}
	for i := range ops {
		ops[i].RowCount = 1
		if ops[i].Decorators.ProduceCond != nil {
			ops[i].CondVal, ops[i].CondSet = true, true
		}
	}
	return nil
}

var wireTxTestDesc = &QueryDescriptor{
	StmtID:    7,
	StmtVerb:  VerbUpdate,
	StaticSQL: "UPDATE items SET score = 1",
	Segments:  []string{"UPDATE items SET score = 1"},
}

func queueTestExec(p *Pipeline) *ExecFuture {
	return p.QueueExec(7, wireTxTestDesc, RenderInput{}, MakeVariantKey(7, 0, 0, 0), nil, nil, nil)
}

func newWireTxDB(t *testing.T, cfg *mockDriverConfig) (*sql.DB, *StmtCache) {
	t.Helper()
	cache := NewStmtCache()
	t.Cleanup(func() { _ = cache.Close() })
	return newMockDB(t, cfg), cache
}

func pipelineTx(ctx context.Context, db *sql.DB, cache *StmtCache, txOpts *sql.TxOptions, opts []TxOption, fn func(p *Pipeline) error) error {
	return RunPipelineTx(ctx, db, cache, nil, true, DialectDollar, txOpts, opts, fn)
}

func TestRunPipelineTxWireSingleCall(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	var gotOpts *sql.TxOptions
	var gotOps int
	cfg.wireTxHandler = func(ctx context.Context, txOpts *sql.TxOptions, ops []PipelineOp) error {
		gotOpts, gotOps = txOpts, len(ops)
		for i := range ops {
			if ops[i].SQL == "" {
				t.Errorf("op %d reached the wire pipeliner unrendered", i)
			}
			ops[i].RowCount = 3
		}
		return nil
	}
	db, cache := newWireTxDB(t, cfg)
	want := &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}
	var f1, f2 *ExecFuture
	err := pipelineTx(context.Background(), db, cache, want, nil, func(p *Pipeline) error {
		f1 = queueTestExec(p)
		f2 = queueTestExec(p)
		return nil
	})
	if err != nil {
		t.Fatalf("PipelineTx: %v", err)
	}
	if gotOpts != want || gotOps != 2 {
		t.Fatalf("ExecTxPipeline got opts=%v ops=%d", gotOpts, gotOps)
	}
	for i, f := range []*ExecFuture{f1, f2} {
		if n, err := f.RowsAffected(); err != nil || n != 3 {
			t.Fatalf("future %d: RowsAffected = %d, %v", i, n, err)
		}
	}
	if b, c := cfg.beginCalls.Load(), cfg.commitCalls.Load(); b != 0 || c != 0 {
		t.Fatalf("driver BeginTx/Commit must not be used on the wire path, got %d/%d", b, c)
	}
	if st := cache.Stats(); st.WirePipelines != 1 || st.SequentialPipelines != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestRunPipelineTxWireRetries(t *testing.T) {
	serialization := &mockError{msg: "could not serialize access", sqlState: "40001"}
	cases := []struct {
		name      string
		failures  []error // returned by successive ExecTxPipeline calls; then success
		wantCalls int64
		wantErr   error
		wantStrip bool // the returned error must not be a *CommitError
	}{
		{"retry on a statement error", []error{serialization, serialization}, 3, nil, false},
		{"retry on a commit-time serialization failure", []error{&CommitError{Err: serialization}}, 2, nil, false},
		{"a connection drop during COMMIT is not retried", []error{&CommitError{Err: io.ErrUnexpectedEOF}}, 1, io.ErrUnexpectedEOF, true},
		{"permanent errors are not retried", []error{errors.New("boom")}, 1, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &mockDriverConfig{enableWireTx: true}
			var n int
			cfg.wireTxHandler = func(context.Context, *sql.TxOptions, []PipelineOp) error {
				n++
				if n <= len(tc.failures) {
					return tc.failures[n-1]
				}
				return nil
			}
			db, cache := newWireTxDB(t, cfg)
			builds := 0
			err := pipelineTx(context.Background(), db, cache, nil,
				[]TxOption{WithTxRetry(TxRetryPolicy{MaxRetries: 5, BaseDelay: time.Microsecond, MaxDelay: time.Microsecond})},
				func(p *Pipeline) error { builds++; queueTestExec(p); return nil })
			if got := cfg.wireTxCalls.Load(); got != tc.wantCalls {
				t.Fatalf("ExecTxPipeline calls = %d, want %d (err=%v)", got, tc.wantCalls, err)
			}
			if int64(builds) != tc.wantCalls {
				t.Fatalf("build ran %d times, want one per attempt (%d)", builds, tc.wantCalls)
			}
			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			case tc.name == "permanent errors are not retried":
				if err == nil || err.Error() != "boom" {
					t.Fatalf("err = %v", err)
				}
			default:
				if err != nil {
					t.Fatalf("err = %v", err)
				}
			}
			var ce *CommitError
			if errors.As(err, &ce) {
				t.Fatalf("CommitError marker leaked to the caller: %v", err)
			}
			if st := cache.Stats(); int64(st.WirePipelines) != tc.wantCalls || st.SequentialPipelines != 0 {
				t.Fatalf("stats = %+v, want %d wire pipelines", st, tc.wantCalls)
			}
		})
	}
}

func TestRunPipelineTxRetryStopsWhenContextEnds(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	cfg.wireTxHandler = func(context.Context, *sql.TxOptions, []PipelineOp) error {
		return &mockError{msg: "deadlock detected", sqlState: "40P01"}
	}
	db, cache := newWireTxDB(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := pipelineTx(ctx, db, cache, nil,
		[]TxOption{WithTxRetry(TxRetryPolicy{MaxRetries: 1000, BaseDelay: time.Second, MaxDelay: time.Second})},
		func(p *Pipeline) error { queueTestExec(p); return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

func TestRunPipelineTxBuildErrorsNeverTouchTheDatabase(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	boom := errors.New("build failed")
	if err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
		queueTestExec(p)
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	// an invalid argument recorded while queuing is surfaced by the flush, still without any I/O
	err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
		p.SetBuildError(boom)
		queueTestExec(p)
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	// nothing queued: no transaction at all
	if err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error { return nil }); err != nil {
		t.Fatalf("empty PipelineTx: %v", err)
	}
	if cfg.wireTxCalls.Load() != 0 || cfg.beginCalls.Load() != 0 {
		t.Fatalf("the database was used: wireTx=%d begin=%d", cfg.wireTxCalls.Load(), cfg.beginCalls.Load())
	}
	if st := cache.Stats(); st.WirePipelines != 0 || st.SequentialPipelines != 0 {
		t.Fatalf("stats = %+v", st)
	}
	defer func() {
		if r := recover(); r != "panic in build" {
			t.Fatalf("recover() = %v", r)
		}
	}()
	_ = pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error { panic("panic in build") })
}

func TestFlushInsidePipelineTxIsRejected(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	var flushErr error
	err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
		queueTestExec(p)
		flushErr = p.Flush()
		return nil
	})
	if err != nil {
		t.Fatalf("PipelineTx: %v", err)
	}
	if !errors.Is(flushErr, ErrFlushInPipelineTx) {
		t.Fatalf("Flush inside PipelineTx = %v", flushErr)
	}
	if cfg.wireTxCalls.Load() != 1 {
		t.Fatalf("the transaction must still flush the pipeline once, calls = %d", cfg.wireTxCalls.Load())
	}
}

func TestStandalonePipelineUsesWireTxPipeliner(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	f := queueTestExec(p)
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.RowsAffected(); n != 1 || cfg.wireTxCalls.Load() != 1 || cfg.wireCalls.Load() != 0 || cfg.beginCalls.Load() != 0 {
		t.Fatalf("rows=%d wireTx=%d wire=%d begin=%d", n, cfg.wireTxCalls.Load(), cfg.wireCalls.Load(), cfg.beginCalls.Load())
	}
	if st := cache.Stats(); st.WirePipelines != 1 || st.SequentialPipelines != 0 {
		t.Fatalf("stats = %+v", st)
	}

	// A COMMIT failure is reported to a standalone caller without the internal marker.
	cfg.wireTxHandler = func(context.Context, *sql.TxOptions, []PipelineOp) error {
		return &CommitError{Err: io.ErrUnexpectedEOF}
	}
	p = NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	queueTestExec(p)
	err := p.Flush()
	var ce *CommitError
	if !errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &ce) {
		t.Fatalf("err = %v", err)
	}
}

// A driver that only implements wire.Pipeliner cannot begin/commit by itself: PipelineTx falls back to
// BeginTx + one wire call for the operations + Commit.
func TestRunPipelineTxWithPlainWirePipeliner(t *testing.T) {
	cfg := &mockDriverConfig{enableWire: true}
	db, cache := newWireTxDB(t, cfg)
	var f *ExecFuture
	if err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error { f = queueTestExec(p); return nil }); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.RowsAffected(); n != 1 {
		t.Fatalf("rows = %d", n)
	}
	if cfg.beginCalls.Load() != 1 || cfg.wireCalls.Load() != 1 || cfg.commitCalls.Load() != 1 {
		t.Fatalf("begin=%d wire=%d commit=%d", cfg.beginCalls.Load(), cfg.wireCalls.Load(), cfg.commitCalls.Load())
	}
	if st := cache.Stats(); st.WirePipelines != 1 || st.SequentialPipelines != 0 {
		t.Fatalf("stats = %+v", st)
	}

	// An error from the wire call rolls the transaction back.
	cfg.wireHandler = func(context.Context, []PipelineOp) error { return errors.New("wire failed") }
	err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error { queueTestExec(p); return nil })
	if err == nil || err.Error() != "wire failed" || cfg.rollbackCalls.Load() != 1 {
		t.Fatalf("err=%v rollbacks=%d", err, cfg.rollbackCalls.Load())
	}
}

func TestRunPipelineTxPortablePathCommitErrors(t *testing.T) {
	for _, mode := range []string{"sequential", "plain wire"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &mockDriverConfig{enableWire: mode == "plain wire"}
			db, cache := newWireTxDB(t, cfg)

			// serialization failure at COMMIT is retried, with the whole attempt re-built
			cfg.failCommit, cfg.failCommitOnce = &mockError{msg: "serialize", sqlState: "40001"}, true
			builds := 0
			err := pipelineTx(context.Background(), db, cache, nil,
				[]TxOption{WithTxRetry(TxRetryPolicy{MaxRetries: 2, BaseDelay: time.Microsecond})},
				func(p *Pipeline) error { builds++; queueTestExec(p); return nil })
			if err != nil || builds != 2 || cfg.commitCalls.Load() != 2 {
				t.Fatalf("err=%v builds=%d commits=%d", err, builds, cfg.commitCalls.Load())
			}

			// a connection drop at COMMIT is surfaced as is, and not retried
			cfg.failCommit = io.ErrUnexpectedEOF
			err = pipelineTx(context.Background(), db, cache, nil,
				[]TxOption{WithTxRetry(TxRetryPolicy{MaxRetries: 2, BaseDelay: time.Microsecond})},
				func(p *Pipeline) error { queueTestExec(p); return nil })
			var ce *CommitError
			if !errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &ce) {
				t.Fatalf("err = %v", err)
			}
			cfg.failCommit = nil

			// BeginTx failure
			cfg.failBegin = errors.New("no connection")
			err = pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error { queueTestExec(p); return nil })
			if err == nil || err.Error() != "no connection" {
				t.Fatalf("err = %v", err)
			}
			cfg.failBegin = nil

			st := cache.Stats()
			if mode == "sequential" && (st.SequentialPipelines == 0 || st.WirePipelines != 0) {
				t.Fatalf("stats = %+v", st)
			}
			if mode == "plain wire" && (st.WirePipelines == 0 || st.SequentialPipelines != 0) {
				t.Fatalf("stats = %+v", st)
			}
		})
	}
}

func TestRunPipelineTxPortablePathRollsBackOnFailureAndPanic(t *testing.T) {
	cfg := &mockDriverConfig{}
	db, cache := newWireTxDB(t, cfg)

	cfg.failExec = errors.New("exec failed")
	err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error { queueTestExec(p); return nil })
	if err == nil || cfg.rollbackCalls.Load() != 1 || cfg.commitCalls.Load() != 0 {
		t.Fatalf("err=%v rollbacks=%d commits=%d", err, cfg.rollbackCalls.Load(), cfg.commitCalls.Load())
	}
	cfg.failExec = nil

	func() {
		defer func() {
			if r := recover(); r != "scan exploded" {
				t.Fatalf("recover() = %v", r)
			}
		}()
		_ = pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
			f := queueTestExec(p)
			f.op.OnDone = func(int64, error) { panic("scan exploded") }
			return nil
		})
	}()
	if cfg.rollbackCalls.Load() != 2 {
		t.Fatalf("a panic while the transaction is open must roll it back, rollbacks = %d", cfg.rollbackCalls.Load())
	}
}

func TestRunPipelineTxInsideUserManagedTransaction(t *testing.T) {
	cfg := &mockDriverConfig{enableWire: true}
	db, cache := newWireTxDB(t, cfg)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	tc := NewTxStmtCache(tx)
	var f *ExecFuture
	err = RunPipelineTx(context.Background(), db, cache, tc, true, DialectDollar, nil, nil, func(p *Pipeline) error {
		f = queueTestExec(p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := f.RowsAffected(); n != 1 || cfg.wireCalls.Load() != 1 || cfg.beginCalls.Load() != 1 || cfg.commitCalls.Load() != 0 {
		t.Fatalf("rows=%d wire=%d begin=%d commit=%d", n, cfg.wireCalls.Load(), cfg.beginCalls.Load(), cfg.commitCalls.Load())
	}
	boom := errors.New("build failed")
	if err := RunPipelineTx(context.Background(), db, cache, tc, true, DialectDollar, nil, nil, func(*Pipeline) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if st := cache.Stats(); st.WirePipelines != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestWireTxPipelinerNeedsAPooledConnection(t *testing.T) {
	// MaxOpenConns(1): the runtime never parks the only connection in db.Conn; it runs portably.
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	db.SetMaxOpenConns(1)
	if err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error { queueTestExec(p); return nil }); err != nil {
		t.Fatal(err)
	}
	// BeginTx + the ops through the transactional wire.Pipeliner sentinel + Commit.
	if cfg.wireTxCalls.Load() != 0 || cfg.beginCalls.Load() != 1 || cfg.wireCalls.Load() != 1 {
		t.Fatalf("wireTx=%d begin=%d wire=%d", cfg.wireTxCalls.Load(), cfg.beginCalls.Load(), cfg.wireCalls.Load())
	}

	// A context that is already canceled cannot get a connection: the error surfaces from BeginTx.
	db.SetMaxOpenConns(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pipelineTx(ctx, db, cache, nil, nil, func(p *Pipeline) error { queueTestExec(p); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestWireTxPipelinerHelpers(t *testing.T) {
	if wireTxPipelinerOf(struct{}{}) != nil || wireTxPipelinerOf(&pipelineConnWrapper{Conn: &mockConn{}}) != nil {
		t.Fatal("a connection without ExecTxPipeline must not be reported as wireTxPipeliner")
	}
	raw := &mockWireTxConn{}
	if wireTxPipelinerOf(raw) == nil || wireTxPipelinerOf(&pipelineConnWrapper{Conn: raw}) == nil {
		t.Fatal("a wireTxPipeliner must be found directly and behind WrapConnector")
	}
	w := &pipelineConnWrapper{Conn: &mockConn{}}
	if err := w.ExecTxPipeline(context.Background(), nil, nil); !errors.Is(err, driver.ErrSkip) {
		t.Fatalf("err = %v", err)
	}
	var nilCache *StmtCache
	nilCache.notePipeline(true) // must not panic

	ce := &CommitError{Err: io.EOF}
	if ce.Error() != io.EOF.Error() || !errors.Is(ce, io.EOF) {
		t.Fatalf("CommitError = %v", ce)
	}
}

func TestWireResultKinds(t *testing.T) {
	cond := &PipelineCond{Kind: CondRowsGt}
	cases := []struct {
		name string
		op   PipelineOp
		want WireResultKind
	}{
		{"plain exec", PipelineOp{Decorators: OpDecorators{AssertRows: -1}}, WireResultCommandTag},
		{"exec with assertion", PipelineOp{Decorators: OpDecorators{AssertRows: 1}}, WireResultGuarded},
		{"exec with condition", PipelineOp{Decorators: OpDecorators{AssertRows: -1, ProduceCond: cond}}, WireResultGuarded},
		{"row query", PipelineOp{HasResultCols: true, Decorators: OpDecorators{AssertRows: 1, ProduceCond: cond}}, WireResultRows},
		{"abort", PipelineOp{Decorators: OpDecorators{AssertRows: -1, IsAbort: true}}, WireResultDiscard},
	}
	for _, tc := range cases {
		if got := tc.op.WireResult(); got != tc.want {
			t.Errorf("%s: WireResult = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A second If over the same future needs its own server-side recording: the branch gates of its
// operations read sqlgen.c<ID> for that ID, so it must be set by the SQL of the producing operation.
func TestExtraConditionsAreRecordedServerSide(t *testing.T) {
	get := &QueryDescriptor{
		Name: "Get", StmtID: 2, StmtVerb: VerbSelect,
		StaticSQL:      "SELECT id FROM t WHERE id = $1",
		Segments:       []string{"SELECT ", " FROM t WHERE id = ", ""},
		Slots:          []Slot{{Kind: SlotSelectCols}, {Kind: SlotScalar}},
		SelectColExprs: []string{"id"},
	}
	upd := &QueryDescriptor{
		Name: "Upd", StmtID: 3, StmtVerb: VerbUpdate,
		StaticSQL: "UPDATE t SET v = v + $1 WHERE id = $2",
		Segments:  []string{"UPDATE t SET v = v + ", " WHERE ", "id = ", "", ""},
		Slots:     []Slot{{Kind: SlotScalar}, {Kind: SlotBranchGuardOpen}, {Kind: SlotScalar}, {Kind: SlotBranchGuardClose}},
	}
	p := NewPipeline(context.Background(), nil, nil, nil, true, DialectDollar)
	f := QueueRow[int64](p, 2, get, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, nil, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) }, func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) })
	p.If(f.Found(), func() { p.QueueExec(3, upd, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil) }, nil)
	p.If(f.NotFound(), nil, func() { p.QueueExec(3, upd, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil) })
	p.If(f.RowsAffectedGe(1), nil, nil)
	p.syncOpsFromPtrs()
	renderPipelineBatch(DialectDollar, nil, p.ops)

	op := p.ops[0]
	if len(op.ExtraConds) != 2 || len(op.Decorators.ExtraConds) != 2 {
		t.Fatalf("ExtraConds = %d / %d", len(op.ExtraConds), len(op.Decorators.ExtraConds))
	}
	for _, want := range []string{
		"_sqlgen_cond AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.c1'",
		"_sqlgen_x2 AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.c2'",
		"_sqlgen_x3 AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.c3'",
		"(SELECT _ok FROM _sqlgen_x2) AS _x2",
		"AND _f._x3 IS NOT NULL",
	} {
		if !strings.Contains(op.SQL, want) {
			t.Fatalf("SQL lacks %q:\n%s", want, op.SQL)
		}
	}
	if est := estimateOpBytes(op.Desc, &op.Input, &op.Decorators); est < len(op.SQL) {
		t.Fatalf("estimateOpBytes = %d < rendered %d", est, len(op.SQL))
	}

	// an exec producer reads every guard in the target list of its final SELECT, with and without an
	// assertion (a guard the statement never reads would be pruned by the planner)
	exec := PipelineOp{Desc: upd, Decorators: OpDecorators{AssertRows: -1, ProduceCond: &PipelineCond{Kind: CondRowsGt}, CondID: 1,
		ExtraConds: []ProducedCond{{CondID: 2, Cond: &PipelineCond{Kind: CondRowsEq, Threshold: 1}}}}}
	for _, assert := range []int64{-1, 1} {
		exec.Decorators.AssertRows = assert
		ops := []PipelineOp{exec}
		renderPipelineBatch(DialectDollar, nil, ops)
		if !strings.Contains(ops[0].SQL, "SELECT (SELECT COUNT(*) FROM _sqlgen_op) AS _cnt") ||
			!strings.Contains(ops[0].SQL, "(SELECT _ok FROM _sqlgen_cond) AS _c, (SELECT _ok FROM _sqlgen_x2) AS _x2") ||
			strings.Contains(ops[0].SQL, "(SELECT _ok FROM _sqlgen_chk) AS _a") != (assert >= 0) {
			t.Fatalf("assert=%d: unexpected SQL:\n%s", assert, ops[0].SQL)
		}
		if est := estimateOpBytes(upd, &RenderInput{}, &exec.Decorators); est < len(ops[0].SQL) {
			t.Fatalf("estimateOpBytes = %d < rendered %d", est, len(ops[0].SQL))
		}
	}

	// values read back from the server win over the Go evaluation
	p2 := NewPipeline(context.Background(), nil, nil, nil, true, DialectDollar)
	f2 := QueueRow[int64](p2, 2, get, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, nil, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) }, func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) })
	r1 := p2.If(f2.Found(), nil, nil)
	r2 := p2.If(f2.NotFound(), nil, nil)
	p2.syncOpsFromPtrs()
	p2.ops[0].CondVal, p2.ops[0].CondSet = true, true
	p2.ops[0].ExtraConds[0].Val, p2.ops[0].ExtraConds[0].Set = true, true // the Go evaluation would say false
	if err := p2.executePipelineOps(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if !r1.Taken() || !r2.Taken() {
		t.Fatalf("taken = %v / %v", r1.Taken(), r2.Taken())
	}
}
