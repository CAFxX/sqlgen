package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// An If chain runs at most one arm, the first whose condition holds (else the Else arm); Taken reports
// it by index. The mock driver reports 1 row affected for every exec (sequential path; the wire flavors
// are compared with it on real engines by the integration guard matrix and differential fuzz).
func TestIfChainRunsTheFirstArmWhoseConditionHolds(t *testing.T) {
	{
		wire := false
		cfg := &mockDriverConfig{enableWireTx: wire}
		db, cache := newWireTxDB(t, cfg)
		var arms [5]*ExecFuture
		var chain *IfChain[*Pipeline]
		var nested *IfChain[*Pipeline]
		err := pipelineTx(context.Background(), db, cache, nil, nil, func(p *Pipeline) error {
			f := queueTestExec(p) // 1 row
			chain = p.If(f.RowsEq(2), func(p *Pipeline) {
				arms[0] = queueTestExec(p)
			}).ElseIf(f.RowsEq(1), func(p *Pipeline) {
				arms[1] = queueTestExec(p)
				nested = p.If(f.RowsEq(1), nil).ElseIf(f.RowsEq(1), func(p *Pipeline) { arms[4] = queueTestExec(p) })
			}).ElseIf(f.Found(), func(p *Pipeline) { // holds too, but an earlier arm ran
				arms[2] = queueTestExec(p)
			}).Else(func(p *Pipeline) {
				arms[3] = queueTestExec(p)
			})
			return nil
		})
		if err != nil {
			t.Fatalf("wire=%v: %v", wire, err)
		}
		for i, f := range arms {
			n, err := f.RowsAffected()
			ran := i == 1
			if ran != (err == nil) || !ran && !errors.Is(err, ErrBranchNotTaken) || ran && n != 1 {
				t.Errorf("wire=%v arm %d: n=%d err=%v", wire, i, n, err)
			}
			if i < 4 && chain.Taken(i) != ran {
				t.Errorf("wire=%v Taken(%d) = %v", wire, i, chain.Taken(i))
			}
		}
		if chain.Skipped() || !nested.Taken(0) || nested.Taken(1) || nested.Skipped() {
			t.Errorf("wire=%v: skipped=%v nested taken %v,%v", wire, chain.Skipped(), nested.Taken(0), nested.Taken(1))
		}
	}
}

// Taken panics for an index that names no arm; before the pipeline ran it is false for every arm.
func TestIfChainTakenIndexAndReadiness(t *testing.T) {
	p := NewPipeline(context.Background(), nil, nil, nil, true, DialectDollar)
	c := p.If(queueTestExec(p).Found(), nil)
	if c.Taken(0) || c.Skipped() {
		t.Fatal("resolved before the pipeline ran")
	}
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), "Taken(1)") {
			t.Fatalf("recover = %v", r)
		}
	}()
	c.Taken(1)
}

// The handle an arm receives is the only one that can queue while the arm's closure runs, and only then:
// every misuse fails the pipeline with ErrInvalidArgument naming the call.
func TestIfArmHandlesAreScoped(t *testing.T) {
	cases := map[string]struct {
		build func(p *Pipeline)
		want  string
	}{
		"outer handle inside the arm": {func(p *Pipeline) {
			p.If(queueTestExec(p).Found(), func(*Pipeline) { queueTestExec(p) })
		}, "operation 1) was queued on an outer pipeline handle"},
		"outer arm handle inside a nested arm": {func(p *Pipeline) {
			f := queueTestExec(p)
			p.If(f.Found(), func(b *Pipeline) {
				b.If(f.Found(), func(*Pipeline) { b.Abort(nil) })
			})
		}, "Abort (operation 1) was queued on an outer pipeline handle"},
		"stored arm handle": {func(p *Pipeline) {
			var kept *Pipeline
			p.If(queueTestExec(p).Found(), func(b *Pipeline) { kept = b })
			queueTestExec(kept)
		}, "operation 1) was queued on the handle of an If arm whose closure has returned"},
		"If on the outer handle inside the arm": {func(p *Pipeline) {
			f := queueTestExec(p)
			p.If(f.Found(), func(*Pipeline) { p.If(f.Found(), nil) })
		}, "If was queued on an outer pipeline handle"},
		"ElseIf from inside the arm": {func(p *Pipeline) {
			f := queueTestExec(p)
			var c *IfChain[*Pipeline]
			c = p.If(f.Found(), func(*Pipeline) {})
			p.If(f.Found(), func(b *Pipeline) { c.ElseIf(f.Found(), nil) })
		}, "ElseIf was queued on an outer pipeline handle"},
		"ElseIf after another operation": {func(p *Pipeline) {
			f := queueTestExec(p)
			c := p.If(f.Found(), nil)
			queueTestExec(p)
			c.ElseIf(f.Found(), nil)
		}, "ElseIf must directly follow the previous arm"},
		"Else twice": {func(p *Pipeline) {
			c := p.If(queueTestExec(p).Found(), nil)
			c.Else(nil)
			c.Else(nil)
		}, "Else after the chain's Else"},
		"Flush inside the arm": {func(p *Pipeline) {
			p.If(queueTestExec(p).Found(), func(*Pipeline) { _ = p.Flush() })
		}, "Flush was called on an If arm's handle or while"},
		"Flush on a stored arm handle": {func(p *Pipeline) {
			var kept *Pipeline
			p.If(queueTestExec(p).Found(), func(b *Pipeline) { kept = b })
			_ = kept.Flush()
		}, "Flush was called on an If arm's handle"},
	}
	for name, tc := range cases {
		cfg := &mockDriverConfig{enableWireTx: true}
		db, cache := newWireTxDB(t, cfg)
		p := NewPipeline(context.Background(), db, cache, nil, true, DialectDollar)
		tc.build(p)
		err := p.Flush()
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Flush = %v", name, err)
		}
		if cfg.wireTxCalls.Load() != 0 || cfg.execCalls.Load() != 0 {
			t.Errorf("%s: something was sent", name)
		}
	}
}
