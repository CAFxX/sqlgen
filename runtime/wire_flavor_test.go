package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// A wire pipeliner that answers ErrWireNotApplicable (nothing sent) makes the runtime run the pipeline
// sequentially: same results, counted in SequentialPipelines, and the pipeliner is asked once, not again
// inside the fallback transaction.
func TestWireNotApplicableFallsBackSequentially(t *testing.T) {
	t.Run("PipelineTx", func(t *testing.T) {
		cfg := &mockDriverConfig{enableWireTx: true}
		cfg.wireTxHandler = func(context.Context, *sql.TxOptions, []PipelineOp) error {
			return fmt.Errorf("batch too large: %w", ErrWireNotApplicable)
		}
		db, cache := newWireTxDB(t, cfg)
		var f *ExecFuture
		err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
			f = queueTestExec(p)
			return nil
		})
		if err != nil {
			t.Fatalf("PipelineTx: %v", err)
		}
		if _, err := f.RowsAffected(); err != nil {
			t.Fatalf("future: %v", err)
		}
		if got := cfg.wireTxCalls.Load(); got != 1 {
			t.Fatalf("ExecTxPipeline calls = %d, want 1", got)
		}
		if b, c := cfg.beginCalls.Load(), cfg.commitCalls.Load(); b != 1 || c != 1 {
			t.Fatalf("sequential fallback must use the driver's BeginTx/Commit, got %d/%d", b, c)
		}
		if st := cache.Stats(); st.WirePipelines != 0 || st.SequentialPipelines != 1 {
			t.Fatalf("stats = %+v, want 0 wire / 1 sequential", st)
		}
	})

	t.Run("user transaction", func(t *testing.T) {
		cfg := &mockDriverConfig{enableWire: true}
		cfg.wireHandler = func(context.Context, []PipelineOp) error { return ErrWireNotApplicable }
		db := newMockDB(t, cfg)
		cache := NewStmtCache()
		t.Cleanup(func() { _ = cache.Close() })
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		tc := NewTxStmtCache(tx)
		defer func() { _ = tc.Close() }()
		p := NewPipeline(context.Background(), db, cache, tc, true, DialectDollar)
		f := queueTestExec(p)
		if err := p.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if _, err := f.RowsAffected(); err != nil {
			t.Fatalf("future: %v", err)
		}
		if st := cache.Stats(); st.WirePipelines != 0 || st.SequentialPipelines != 1 {
			t.Fatalf("stats = %+v, want 0 wire / 1 sequential", st)
		}
	})
}

type flavoredWireConn struct {
	mockWireTxConn
	flavor WireFlavor
}

func (c *flavoredWireConn) WireFlavor() WireFlavor { return c.flavor }

// Only a pipeliner of the PostgreSQL flavor gets op.SQL pre-rendered by the runtime.
func TestPrerendersFollowsFlavor(t *testing.T) {
	if !prerenders(&mockWireTxConn{}) {
		t.Fatal("a pipeliner without WireFlavored must be pre-rendered")
	}
	if !prerenders(&flavoredWireConn{flavor: WireFlavorPostgres}) {
		t.Fatal("WireFlavorPostgres must be pre-rendered")
	}
	if prerenders(&flavoredWireConn{flavor: WireFlavorMySQL}) {
		t.Fatal("WireFlavorMySQL renders itself")
	}
}
