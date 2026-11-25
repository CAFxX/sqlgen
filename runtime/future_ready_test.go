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
