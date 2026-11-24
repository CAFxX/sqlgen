package integration

// Compound conditions across different operations: every leaf must be evaluated over its own
// operation's rows on the server, so the wire path takes the same branch as the sequential fallback.

import (
	"database/sql"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

// condOps are the operations a compound condition is built from. Over the fixture rows (ids 1..4):
// d deletes nothing (0 rows), u updates one row, g finds id 3, m finds nothing, s finds two rows,
// r is a RETURNING update of two rows. skipped sits in a branch that is never taken.
type condOps struct {
	d       *runtime.ExecFuture
	u       *runtime.ExecFuture
	g, m    *runtime.RowFuture[pgq.Item]
	s       *runtime.SliceFuture[pgq.Item]
	r       *runtime.SliceFuture[pgq.Written]
	skipped *runtime.RowFuture[pgq.Item]
}

type condVariant struct {
	name string
	mk   func(o condOps) runtime.PipelineCond
}

func scoreAtLeast(n int64) (string, func(pgq.Item) bool) {
	return "score >= " + strconv.FormatInt(n, 10), func(it pgq.Item) bool { return it.Score >= n }
}

var condVariants = []condVariant{
	// single leaves
	{"d.Found", func(o condOps) runtime.PipelineCond { return o.d.Found() }},
	{"d.NotFound", func(o condOps) runtime.PipelineCond { return o.d.NotFound() }},
	{"d.Eq0", func(o condOps) runtime.PipelineCond { return o.d.RowsEq(0) }},
	{"d.Gt0", func(o condOps) runtime.PipelineCond { return o.d.RowsGt(0) }},
	{"d.Ge0", func(o condOps) runtime.PipelineCond { return o.d.RowsGe(0) }},
	{"d.Lt1", func(o condOps) runtime.PipelineCond { return o.d.RowsLt(1) }},
	{"d.Le0", func(o condOps) runtime.PipelineCond { return o.d.RowsLe(0) }},
	{"!d.Found", func(o condOps) runtime.PipelineCond { return o.d.Found().Not() }},
	// two operations, both orders
	{"d.NotFound&g.Found", func(o condOps) runtime.PipelineCond { return o.d.NotFound().And(o.g.Found()) }},
	{"g.Found&d.NotFound", func(o condOps) runtime.PipelineCond { return o.g.Found().And(o.d.NotFound()) }},
	{"get.Found&del.Eq0", func(o condOps) runtime.PipelineCond { return o.g.Found().And(o.d.RowsEq(0)) }},
	{"!(d.NotFound&g.Found)", func(o condOps) runtime.PipelineCond { return o.d.NotFound().And(o.g.Found()).Not() }},
	{"d.Found|g.Found", func(o condOps) runtime.PipelineCond { return o.d.Found().Or(o.g.Found()) }},
	{"d.Found|m.Found", func(o condOps) runtime.PipelineCond { return o.d.Found().Or(o.m.Found()) }},
	{"g.Found&m.NotFound", func(o condOps) runtime.PipelineCond { return o.g.Found().And(o.m.NotFound()) }},
	{"m.Found|g.NotFound", func(o condOps) runtime.PipelineCond { return o.m.Found().Or(o.g.NotFound()) }},
	{"u.Eq1&s.Ge2", func(o condOps) runtime.PipelineCond { return o.u.RowsEq(1).And(o.s.RowsGe(2)) }},
	{"s.Gt1&u.Lt1", func(o condOps) runtime.PipelineCond { return o.s.RowsGt(1).And(o.u.RowsLt(1)) }},
	{"s.Le1|u.Le1", func(o condOps) runtime.PipelineCond { return o.s.RowsLe(1).Or(o.u.RowsLe(1)) }},
	{"r.Eq2&d.Eq0", func(o condOps) runtime.PipelineCond { return o.r.RowsEq(2).And(o.d.RowsEq(0)) }},
	// column predicates
	{"g.WhereCol(true)", func(o condOps) runtime.PipelineCond {
		sq, gp := scoreAtLeast(30)
		return o.g.WhereCol(sq, gp)
	}},
	{"g.WhereCol(false)&d.NotFound", func(o condOps) runtime.PipelineCond {
		sq, gp := scoreAtLeast(31)
		return o.g.WhereCol(sq, gp).And(o.d.NotFound())
	}},
	{"d.NotFound&g.WhereCol(true)", func(o condOps) runtime.PipelineCond {
		sq, gp := scoreAtLeast(30)
		return o.d.NotFound().And(o.g.WhereCol(sq, gp))
	}},
	{"s.WhereCol(all)&u.Found", func(o condOps) runtime.PipelineCond {
		sq, gp := scoreAtLeast(10)
		return o.s.WhereCol(sq, gp).And(o.u.Found())
	}},
	{"!s.WhereCol(some)|d.Found", func(o condOps) runtime.PipelineCond {
		sq, gp := scoreAtLeast(20)
		return o.s.WhereCol(sq, gp).Not().Or(o.d.Found())
	}},
	{"m.WhereCol&g.Found", func(o condOps) runtime.PipelineCond {
		sq, gp := scoreAtLeast(0)
		return o.m.WhereCol(sq, gp).And(o.g.Found())
	}},
	// three operations, nested
	{"(d.NotFound&g.Found)&s.Eq2", func(o condOps) runtime.PipelineCond {
		return o.d.NotFound().And(o.g.Found()).And(o.s.RowsEq(2))
	}},
	{"d.NotFound&(g.Found|m.Found)", func(o condOps) runtime.PipelineCond {
		return o.d.NotFound().And(o.g.Found().Or(o.m.Found()))
	}},
	{"!(m.Found|d.Found)&u.Found", func(o condOps) runtime.PipelineCond {
		return o.m.Found().Or(o.d.Found()).Not().And(o.u.Found())
	}},
	{"(g.Found&m.Found)|(d.NotFound&s.Gt1)", func(o condOps) runtime.PipelineCond {
		return o.g.Found().And(o.m.Found()).Or(o.d.NotFound().And(o.s.RowsGt(1)))
	}},
	{"!(d.Found|(g.Found&!m.Found))", func(o condOps) runtime.PipelineCond {
		return o.d.Found().Or(o.g.Found().And(o.m.Found().Not())).Not()
	}},
	{"same op twice", func(o condOps) runtime.PipelineCond {
		return o.g.Found().And(o.g.RowsEq(1)).And(o.d.NotFound())
	}},
	// operations that never ran (their branch was not taken) count as false, also under Not
	{"skipped.Found|d.NotFound", func(o condOps) runtime.PipelineCond { return o.skipped.Found().Or(o.d.NotFound()) }},
	{"skipped.NotFound&g.Found", func(o condOps) runtime.PipelineCond { return o.skipped.NotFound().And(o.g.Found()) }},
	{"!skipped.Found&g.Found", func(o condOps) runtime.PipelineCond { return o.skipped.Found().Not().And(o.g.Found()) }},
	{"skipped.NotFound", func(o condOps) runtime.PipelineCond { return o.skipped.NotFound() }},
	{"!skipped.Found", func(o condOps) runtime.PipelineCond { return o.skipped.Found().Not() }},
}

// condScenarios builds one scenario per variant: the operations run, then an If on the variant picks
// between two writes; a second If over a reused sub-condition checks that binding never mutates it.
func condScenarios() []scenario {
	var out []scenario
	for _, v := range condVariants {
		out = append(out, scenario{
			name: "cond " + v.name,
			build: func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
				var o condOps
				o.d = p.DeleteItems([]int64{98, 99})
				o.u = p.AddScoreOne(1, 1)
				o.g = p.GetItem(3)
				o.m = p.GetItem(999)
				o.s = p.ItemsByIDs([]int64{1, 2})
				o.r = p.BumpScoresReturning(5, []int64{1, 2})
				// never taken: m finds nothing
				p.If(o.m.Found(), func(p *pgq.Pipeline) { o.skipped = p.GetItem(2) }, nil)
				cond := v.mk(o)
				r1 := p.If(cond,
					func(p *pgq.Pipeline) { p.AddScoreOne(1000, 2) },
					func(p *pgq.Pipeline) { p.AddScoreOne(7, 4) })
				// the same condition value again, and its negation: both must still be intact
				r2 := p.If(cond.Not(), func(p *pgq.Pipeline) { p.AddScoreOne(100000, 3) }, nil)
				return []*runtime.IfResult{r1, r2}, nil
			},
		})
	}
	return out
}

func TestRealEnginePipelineCompoundConditions(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("the single-round-trip pipeline SQL is only generated for the $N (PostgreSQL-protocol) dialect")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, mode := range runModes {
			for _, sc := range condScenarios() {
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					compareWireSeq(t, db, dsn, mode, sc)
				})
			}
		}
	})
}
