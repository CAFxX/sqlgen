package integration

// A pipeline flushed on a transaction the caller began must leave that transaction usable after a failed
// assertion or an Abort on the single-round-trip path, as it does on the sequential path: the work done
// before the failing operation stays, the caller decides whether to commit or roll back. A database
// error (a unique violation, ...) aborts the PostgreSQL-protocol transaction on both paths.

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

type userTxOutcome struct {
	FlushErr  string
	LaterExec string // the statement the caller runs on the transaction afterwards
	Commit    string
	State     string
}

func TestRealEnginePipelineUserTransactionAfterFailure(t *testing.T) {
	scenarios := []scenario{
		{"failed assertion, nothing changed by the failing op", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			p.AddScoreOne(1, 999).AssertRowsAffected(1) // no such row
			p.AddScoreOne(1000, 2)                      // never runs
			return nil, nil
		}},
		{"failed assertion on a read", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(5, 1)
			p.GetItem(999).AssertFound()
			return nil, nil
		}},
		{"two asserted operations, the second fails", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			p.AddScoreOne(2, 2).AssertRowsAffected(1)
			p.DeleteItems([]int64{98}).AssertRowsAffected(1)
			p.AddScoreOne(4, 4).AssertRowsAffected(1)
			return nil, nil
		}},
		{"abort in a taken branch", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(1, 1)
			g := p.GetItem(1)
			return []*runtime.IfResult{p.If(g.Found(), func(p *pgq.Pipeline) { p.Abort(errStop) }, nil)}, nil
		}},
		{"assertion on an operation that feeds a condition", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(1, 1)
			u := p.AddScoreOne(1, 999).AssertRowsAffected(1)
			return []*runtime.IfResult{p.If(u.Found(), func(p *pgq.Pipeline) { p.AddScoreOne(1, 3) }, nil)}, nil
		}},
		{"successful pipeline (control)", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			return nil, nil
		}},
		{"unique violation aborts the transaction on both paths", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			p.InsertItem(2, 1, "dup", 0, "x")
			return nil, nil
		}},
		{"unique violation in an asserted operation", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(1, 1)
			p.InsertItem(2, 1, "dup", 0, "x").AssertRowsAffected(1)
			return nil, nil
		}},
	}
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("the single-round-trip pipeline SQL is only generated for the $N (PostgreSQL-protocol) dialect")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, sc := range scenarios {
			t.Run(strings.ReplaceAll(sc.name, " ", "_"), func(t *testing.T) {
				run := func(wire bool) userTxOutcome {
					mustExec(t, db, "DELETE FROM sg_items")
					for i := int64(1); i <= 4; i++ {
						mustExec(t, db, fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', %d, 'new')", i, i, i*10))
					}
					wdb := openWire(t, dsn, wire)
					q := pgq.New(wdb)
					defer q.Close()
					ctx := ctxT(t)
					tx, err := wdb.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := tx.ExecContext(ctx, "INSERT INTO sg_items VALUES (50, 5, 'before', 0, 'x')"); err != nil {
						t.Fatal(err)
					}
					p := q.WithTx(tx).Pipeline(ctx)
					sc.build(p)
					var o userTxOutcome
					o.FlushErr = errKind(p.Flush())
					_, lerr := tx.ExecContext(ctx, "INSERT INTO sg_items VALUES (60, 6, 'later', 0, 'x')")
					o.LaterExec = errKind(lerr)
					if lerr != nil && o.LaterExec == "" {
						o.LaterExec = lerr.Error()
					}
					if cerr := tx.Commit(); cerr != nil {
						o.Commit = "ERR"
					}
					o.State = snapshot(t, db)
					st := q.CacheStats()
					if wire && st.WirePipelines != 1 || !wire && st.SequentialPipelines != 1 {
						t.Fatalf("the wrong path ran (wire=%v): %+v", wire, st)
					}
					return o
				}
				seq, wire := run(false), run(true)
				t.Logf("sequential: %+v", seq)
				t.Logf("wire:       %+v", wire)
				if seq != wire {
					t.Fatalf("user-managed transaction diverges after a failed pipeline:\n  sequential %+v\n  wire       %+v", seq, wire)
				}
			})
		}
	})
}
