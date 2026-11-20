package integration

// Every combination of statement kind x assertion x recorded condition must behave on the
// single-round-trip path exactly as on the sequential fallback. The assertion and condition guards are
// CTEs with side effects (an error, set_config); a shape in which the planner may prune them lets the
// batch commit although the caller is told the assertion failed, or silently drops a recorded condition.

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

// guardOp is an operation kind seen through the parts of its future the matrix needs.
type guardOp struct {
	assert func(n int64)
	cond   func(i int) runtime.PipelineCond // distinct conditions over the same future
}

type guardKind struct {
	name  string
	queue func(p *pgq.Pipeline, id int64) guardOp
}

var guardKinds = []guardKind{
	{"exec", func(p *pgq.Pipeline, id int64) guardOp {
		f := p.AddScoreOne(100, id)
		return guardOp{func(n int64) { f.AssertRowsAffected(n) }, func(i int) runtime.PipelineCond { return guardConds(f, i) }}
	}},
	{"exec-returning", func(p *pgq.Pipeline, id int64) guardOp {
		f := p.BumpScoresReturning(5, []int64{id})
		return guardOp{func(n int64) { f.AssertRowsAffected(n) }, func(i int) runtime.PipelineCond { return guardConds(f, i) }}
	}},
	{"row", func(p *pgq.Pipeline, id int64) guardOp {
		f := p.GetItem(id)
		return guardOp{func(n int64) { f.AssertRowsAffected(n) }, func(i int) runtime.PipelineCond { return guardConds(f, i) }}
	}},
	{"slice", func(p *pgq.Pipeline, id int64) guardOp {
		f := p.ItemsByIDs([]int64{id})
		return guardOp{func(n int64) { f.AssertRowsAffected(n) }, func(i int) runtime.PipelineCond { return guardConds(f, i) }}
	}},
}

// condSource is what ExecFuture, RowFuture and SliceFuture have in common.
type condSource interface {
	Found() runtime.PipelineCond
	NotFound() runtime.PipelineCond
	RowsAffectedGe(int64) runtime.PipelineCond
	RowsAffectedEq(int64) runtime.PipelineCond
}

func guardConds(f condSource, i int) runtime.PipelineCond {
	switch i {
	case 0:
		return f.Found()
	case 1:
		return f.RowsAffectedGe(1)
	default:
		return f.NotFound().Not().And(f.RowsAffectedEq(1))
	}
}

func guardScenarios() []scenario {
	var out []scenario
	for _, k := range guardKinds {
		for _, id := range []int64{2, 99} { // present / absent
			for _, assert := range []int64{-1, 0, 1} {
				for nconds := 0; nconds <= 3; nconds++ {
					out = append(out, scenario{
						name: fmt.Sprintf("%s id=%d assert=%d conds=%d", k.name, id, assert, nconds),
						build: func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
							p.AddScoreOne(1, 1)
							op := k.queue(p, id)
							if assert >= 0 {
								op.assert(assert)
							}
							var ifs []*runtime.IfResult
							for c := 0; c < nconds; c++ {
								ifs = append(ifs, p.If(op.cond(c),
									func(p *pgq.Pipeline) { p.AddScoreOne(10, int64(2+c)) },
									func(p *pgq.Pipeline) { p.AddScoreOne(1000, int64(2+c)) }))
							}
							// a statement after the guarded one: it must not run when the batch failed
							p.DeleteItems([]int64{4})
							return ifs, nil
						},
					})
				}
			}
		}
	}
	return out
}

func TestRealEnginePipelineGuardMatrix(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("the single-round-trip pipeline SQL is only generated for the $N (PostgreSQL-protocol) dialect")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, mode := range runModes {
			for _, sc := range guardScenarios() {
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					compareWireSeq(t, db, dsn, mode, sc)
				})
			}
		}
	})
}
