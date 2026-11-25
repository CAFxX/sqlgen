package runtime

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// Reading a future before its pipeline has flushed must be distinguishable from a real "no rows" result.
func TestFuturesReadBeforeFlushReturnErrFutureNotReady(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)

	ex := queueTestExec(p)
	row := QueueRow[int64](p, 2, wireTxTestDesc, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, nil, nil,
		func(s RowScanner, dst *int64) error { return s.Scan(dst) })
	sl := QueueSlice[int64](p, 3, wireTxTestDesc, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil,
		func(s RowScanner, dst *int64) error { return s.Scan(dst) })

	check := func(stage string) {
		t.Helper()
		if _, err := ex.Result(); !errors.Is(err, ErrFutureNotReady) {
			t.Errorf("%s: ExecFuture.Result err = %v", stage, err)
		}
		if _, err := ex.RowsAffected(); !errors.Is(err, ErrFutureNotReady) {
			t.Errorf("%s: ExecFuture.RowsAffected err = %v", stage, err)
		}
		if _, err := row.Get(); !errors.Is(err, ErrFutureNotReady) || errors.Is(err, sql.ErrNoRows) {
			t.Errorf("%s: RowFuture.Get err = %v", stage, err)
		}
		if _, err := sl.Get(); !errors.Is(err, ErrFutureNotReady) {
			t.Errorf("%s: SliceFuture.Get err = %v", stage, err)
		}
	}
	check("queued")

	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	// After the flush the futures answer for real: no ErrFutureNotReady any more.
	if n, err := ex.RowsAffected(); err != nil || n != 1 {
		t.Errorf("flushed ExecFuture: n=%d err=%v", n, err)
	}
	if _, err := ex.Result(); err != nil {
		t.Errorf("flushed ExecFuture.Result: %v", err)
	}
	if _, err := row.Get(); errors.Is(err, ErrFutureNotReady) {
		t.Errorf("flushed RowFuture still not ready: %v", err)
	}
	if _, err := sl.Get(); errors.Is(err, ErrFutureNotReady) {
		t.Errorf("flushed SliceFuture still not ready: %v", err)
	}
}

// Futures queued inside a PipelineTx callback are not ready until the transaction ran, and are after.
func TestPipelineTxFuturesReadyOnlyAfterTheTransaction(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	var f *ExecFuture
	err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
		f = queueTestExec(p)
		if _, err := f.RowsAffected(); !errors.Is(err, ErrFutureNotReady) {
			t.Errorf("inside the callback: err = %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("after PipelineTx: n=%d err=%v", n, err)
	}
}

// An operation queued on a pipeline that already ran is never silently dropped with a success report: its
// future fails with ErrPipelineRan (an assertion on it cannot be bypassed), and so does every later Flush.
func TestQueueOnAPipelineThatRanFails(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	queueTestExec(p)
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("a second Flush with nothing new queued: %v", err)
	}
	late := queueTestExec(p).AssertRows(1)
	row := QueueRow[int64](p, 2, wireTxTestDesc, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, nil, nil,
		func(s RowScanner, dst *int64) error { return s.Scan(dst) })
	sl := QueueSlice[int64](p, 3, wireTxTestDesc, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, nil, nil,
		func(s RowScanner, dst *int64) error { return s.Scan(dst) })
	var pre *PipelineRanError
	if n, err := late.RowsAffected(); !errors.As(err, &pre) || n != 0 || pre.Op != 1 {
		t.Errorf("late ExecFuture: n=%d err=%v", n, err)
	}
	if _, err := late.Result(); !errors.Is(err, ErrPipelineRan) {
		t.Errorf("late ExecFuture.Result: %v", err)
	}
	if _, err := row.Get(); !errors.Is(err, ErrPipelineRan) {
		t.Errorf("late RowFuture: %v", err)
	}
	if _, err := sl.Get(); !errors.Is(err, ErrPipelineRan) {
		t.Errorf("late SliceFuture: %v", err)
	}
	if err := p.Flush(); !errors.As(err, &pre) || pre.Op != 1 {
		t.Errorf("Flush after a late queue: %v", err)
	}
	if got := cfg.wireTxCalls.Load(); got != 1 {
		t.Errorf("the late operations reached the driver: %d pipeline calls", got)
	}
}

// The same after PipelineTx: a pipeline kept past its callback cannot queue, and the futures of a callback
// that failed report the callback's error instead of ErrFutureNotReady forever.
func TestQueueAfterPipelineTxCallbackFails(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	var kept *Pipeline
	if err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
		kept = p
		queueTestExec(p)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queueTestExec(kept).RowsAffected(); !errors.Is(err, ErrPipelineRan) {
		t.Errorf("queued after the callback returned: %v", err)
	}

	errBuild := errors.New("build failed")
	var f *ExecFuture
	err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
		kept = p
		f = queueTestExec(p)
		return errBuild
	})
	if !errors.Is(err, errBuild) {
		t.Fatalf("PipelineTx: %v", err)
	}
	if _, err := f.RowsAffected(); !errors.Is(err, errBuild) {
		t.Errorf("future of a failed callback: %v", err)
	}
	if _, err := queueTestExec(kept).RowsAffected(); !errors.Is(err, ErrPipelineRan) {
		t.Errorf("queued after a failed callback: %v", err)
	}
}

// An If queued on a pipeline that ran fails the next Flush even when its branches queue nothing.
func TestIfOnAPipelineThatRanFails(t *testing.T) {
	cfg := &mockDriverConfig{enableWireTx: true}
	db, cache := newWireTxDB(t, cfg)
	p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
	f := queueTestExec(p)
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	p.If(f.Found(), func() {}, nil)
	if err := p.Flush(); !errors.Is(err, ErrPipelineRan) {
		t.Errorf("Flush after a late If: %v", err)
	}
}
