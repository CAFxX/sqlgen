package integration

// A seeded, bounded differential fuzz for the MySQL / MariaDB single-round-trip path: random pipelines
// (reads, writes, assertions, nested If/Else on simple and compound conditions over any earlier
// operation, Abort; on MariaDB also INSERT / DELETE ... RETURNING) must behave identically on mysqlwire
// and on the sequential fallback -- same error kind, same final table state, same IfResult / future
// values -- in a PipelineTx, in a standalone Flush and in a user-managed transaction. Counterpart of
// wire_fuzz_test.go (same seeds), plus the compound-condition matrix of wire_cond_test.go.

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

type myFuzzGen struct {
	rnd       *rand.Rand
	returning bool
	futs      []myFut
	kinds     []string
	ifs       []*runtime.IfResult
	trace     []string
}

func (g *myFuzzGen) tr(depth int, f string, a ...any) {
	g.trace = append(g.trace, strings.Repeat("  ", depth)+fmt.Sprintf(f, a...))
}

func (g *myFuzzGen) add(kind string, f myFut) {
	g.kinds = append(g.kinds, kind)
	g.futs = append(g.futs, f)
}

// leaf returns a random condition over f.
func (g *myFuzzGen) leaf(f myFut) runtime.PipelineCond {
	n := int64(g.rnd.IntN(3))
	if f.colPred != nil && g.rnd.IntN(5) == 0 {
		return f.colPred([]int64{20, 50}[g.rnd.IntN(2)])
	}
	c := f.src
	switch g.rnd.IntN(7) {
	case 0:
		return c.Found()
	case 1:
		return c.NotFound()
	case 2:
		return c.RowsEq(n)
	case 3:
		return c.RowsGt(n)
	case 4:
		return c.RowsGe(n)
	case 5:
		return c.RowsLt(n)
	default:
		return c.RowsLe(n)
	}
}

// cond returns a random condition: one leaf, or two to four leaves over any earlier operations combined
// with And / Or and optionally negated.
func (g *myFuzzGen) cond() runtime.PipelineCond {
	c := g.leaf(g.futs[g.rnd.IntN(len(g.futs))])
	for extra := g.rnd.IntN(3); extra > 0; extra-- {
		o := g.leaf(g.futs[g.rnd.IntN(len(g.futs))])
		switch g.rnd.IntN(3) {
		case 0:
			c = c.And(o)
		case 1:
			c = c.Or(o)
		default:
			c = o.And(c)
		}
		if g.rnd.IntN(4) == 0 {
			c = c.Not()
		}
	}
	if g.rnd.IntN(5) == 0 {
		c = c.Not()
	}
	return c
}

func (g *myFuzzGen) maybeAssertRows(depth int, f myFut, mod int) {
	if g.rnd.IntN(4) == 0 {
		x := int64(g.rnd.IntN(mod))
		f.assertRows(x)
		g.tr(depth, " assertRows %d", x)
	}
}

func (g *myFuzzGen) gen(p myPipe, depth, n int) {
	for i := 0; i < n; i++ {
		id := int64(g.rnd.IntN(5) + 1)
		k := g.rnd.IntN(13)
		if !g.returning && k >= 11 {
			k -= 11 // no RETURNING on MySQL: more plain writes instead
		}
		switch {
		case k == 0:
			f := p.InsertItem(id, 1, "n", int64(g.rnd.IntN(100)), "new")
			g.tr(depth, "insert %d", id)
			if g.rnd.IntN(3) == 0 {
				f.assertRows(1)
				g.tr(depth, " assert 1")
			}
			g.add("ins", f)
		case k == 1 || k == 2:
			f := p.AddScoreOne(int64(g.rnd.IntN(10)), id)
			g.tr(depth, "update %d", id)
			switch g.rnd.IntN(4) {
			case 0:
				f.assertRows(1)
				g.tr(depth, " assert 1")
			case 1:
				f.assertRows(0)
				g.tr(depth, " assert 0")
			}
			g.add("upd", f)
		case k == 3:
			f := p.DeleteItems([]int64{id, id + 1})
			g.tr(depth, "delete %d,%d", id, id+1)
			g.maybeAssertRows(depth, f, 3)
			g.add("del", f)
		case k == 4 || k == 5:
			f := p.GetItem(id)
			g.tr(depth, "get#%d %d", len(g.futs), id)
			if g.rnd.IntN(5) == 0 {
				f.assertFound()
				g.tr(depth, " assertFound")
			}
			g.add("get", f)
		case k == 6:
			f := p.ItemsByIDs([]int64{id, id + 1, id + 2})
			g.tr(depth, "many#%d %d..", len(g.futs), id)
			g.maybeAssertRows(depth, f, 4)
			g.add("many", f)
		case k == 7 || k == 8:
			if depth >= 3 || len(g.futs) == 0 {
				continue
			}
			cond := g.cond()
			g.tr(depth, "if (%d futures so far)", len(g.futs))
			hasElse := g.rnd.IntN(2) == 0
			r := p.If(cond, func(p myPipe) {
				g.gen(p, depth+1, 1+g.rnd.IntN(3))
			}, func() func(myPipe) {
				if !hasElse {
					return nil
				}
				return func(p myPipe) {
					g.tr(depth, "else")
					g.gen(p, depth+1, 1+g.rnd.IntN(2))
				}
			}())
			g.ifs = append(g.ifs, r)
		case k == 9 && depth > 0:
			g.tr(depth, "ABORT")
			p.Abort(errFuzzAbort)
		case k == 10:
			g.tr(depth, "setall")
			g.add("set", p.SetAllStatus([]string{"new", "active"}[g.rnd.IntN(2)]))
		case k == 11:
			var f myFut
			if g.rnd.IntN(2) == 0 {
				g.tr(depth, "insert returning %d", id)
				f = p.InsertItemReturning(id, 1, "r", int64(g.rnd.IntN(100)), "new")
			} else {
				g.tr(depth, "insert multi returning %d,%d", id, id+10)
				f = p.InsertItemsReturning([]myNew{{ID: id, Org: 1, Name: "r", Score: 5, Status: "new"}, {ID: id + 10, Org: 1, Name: "r", Score: 6, Status: "new"}})
			}
			g.maybeAssertRows(depth, f, 3)
			g.add("insret", f)
		case k == 12:
			var f myFut
			if g.rnd.IntN(2) == 0 {
				g.tr(depth, "delete returning %d,%d", id, id+1)
				f = p.DeleteReturning([]int64{id, id + 1})
			} else {
				g.tr(depth, "delete one returning %d", id)
				f = p.DeleteOneReturning(id)
			}
			g.maybeAssertRows(depth, f, 3)
			g.add("delret", f)
		}
	}
}

func myFuzzClassify(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, errFuzzAbort):
		return "abort"
	}
	return myErrKind(err)
}

// describe renders what a caller can observe after a successful flush; after a failed one the state of
// the individual futures is unspecified (which operations ran differs by design) and is not compared.
func (g *myFuzzGen) describe(err error) string {
	if err != nil {
		return ""
	}
	var sb strings.Builder
	for _, r := range g.ifs {
		fmt.Fprintf(&sb, "if(%v,%v)", r.Taken(), r.Skipped())
	}
	for i, f := range g.futs {
		fmt.Fprintf(&sb, " %s[%s]", g.kinds[i], f.desc())
	}
	return sb.String()
}

// myFuzzRun executes the pipeline generated from seed in the given mode and returns everything an
// observer can see plus the generator trace.
func myFuzzRun(t *testing.T, e engine, pr myPair, wire bool, seed uint64, mode string) (outcome, trace string) {
	t.Helper()
	mustExec(t, pr.admin, "DELETE FROM sg_items")
	mustExec(t, pr.admin, "INSERT INTO sg_items VALUES (1,1,'a',10,'new'),(2,1,'b',60,'new'),(3,1,'c',20,'new')")
	db := pr.seq
	if wire {
		db = pr.wire
	}
	q := myNewQueries(e, db)
	defer q.Close()
	var g *myFuzzGen
	build := func(p myPipe) error {
		g = &myFuzzGen{rnd: rand.New(rand.NewPCG(seed, 99)), returning: p.Returning()}
		g.gen(p, 0, 2+g.rnd.IntN(7))
		return nil
	}
	ctx := ctxT(t)
	var err error
	switch mode {
	case "PipelineTx":
		err = q.PipelineTx(ctx, build)
	case "standalone Flush":
		p, flush := q.Pipeline(ctx)
		_ = build(p)
		err = flush()
	case "user-managed tx":
		tx, berr := db.BeginTx(ctx, nil)
		if berr != nil {
			t.Fatal(berr)
		}
		p, flush := q.WithTx(tx).Pipeline(ctx)
		_ = build(p)
		err = flush()
		// A failed assertion, an Abort or a database error leaves the transaction usable (MySQL does not
		// abort a transaction on a statement error): both paths must agree. The final state is not compared:
		// the sequential path keeps the effect of an operation whose assertion failed, the wire path
		// undoes it.
		_, lerr := tx.ExecContext(ctx, "SELECT 1")
		_ = tx.Rollback()
		return myFuzzClassify(err) + "|later=" + myFuzzClassify(lerr) + "|" + g.describe(err), strings.Join(g.trace, "\n")
	}
	return myFuzzClassify(err) + "|" + mySnapshot(t, pr.admin) + "|" + g.describe(err), strings.Join(g.trace, "\n")
}

func TestRealEngineMySQLPipelineDifferentialFuzz(t *testing.T) {
	perMode := 100 * envFuzzScale()
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		pr := openMyPair(t, db, dsn)
		bad := 0
		for mi, mode := range []string{"PipelineTx", "standalone Flush", "user-managed tx"} {
			for i := 0; i < perMode; i++ {
				seed := uint64(1000*(mi+1) + i) // fixed seeds: a failure names the seed and is reproducible
				w, wtrace := myFuzzRun(t, e, pr, true, seed, mode)
				s, _ := myFuzzRun(t, e, pr, false, seed, mode)
				if w != s {
					bad++
					if bad <= 5 {
						t.Errorf("[%s seed=%d] wire diverges from sequential\n wire : %s\n plain: %s\n generated:\n%s", mode, seed, w, s, wtrace)
					}
				}
			}
		}
		if bad > 0 {
			t.Errorf("%d of %d pipelines diverged", bad, 3*perMode)
		}
	})
}

// --- compound conditions ----------------------------------------------------------------------------

// myCondOps are the operations a compound condition is built from. Over the fixture rows (ids 1..4):
// d deletes nothing (0 rows), u updates one row, g finds id 3, m finds nothing, s finds two rows, r
// returns two rows (a RETURNING insert on MariaDB, a read of two rows on MySQL, which has no RETURNING).
// skipped sits in a branch that is never taken.
type myCondOps struct {
	d, u, g, m, s, r, skipped myFut
}

type myCondVariant struct {
	name string
	mk   func(o myCondOps) runtime.PipelineCond
}

func myScoreAtLeast(f myFut, n int64) runtime.PipelineCond { return f.colPred(n) }

var myCondVariants = []myCondVariant{
	// single leaves
	{"d.Found", func(o myCondOps) runtime.PipelineCond { return o.d.src.Found() }},
	{"d.NotFound", func(o myCondOps) runtime.PipelineCond { return o.d.src.NotFound() }},
	{"d.Eq0", func(o myCondOps) runtime.PipelineCond { return o.d.src.RowsEq(0) }},
	{"d.Gt0", func(o myCondOps) runtime.PipelineCond { return o.d.src.RowsGt(0) }},
	{"d.Ge0", func(o myCondOps) runtime.PipelineCond { return o.d.src.RowsGe(0) }},
	{"d.Lt1", func(o myCondOps) runtime.PipelineCond { return o.d.src.RowsLt(1) }},
	{"d.Le0", func(o myCondOps) runtime.PipelineCond { return o.d.src.RowsLe(0) }},
	{"!d.Found", func(o myCondOps) runtime.PipelineCond { return o.d.src.Found().Not() }},
	// two operations, both orders
	{"d.NotFound&g.Found", func(o myCondOps) runtime.PipelineCond { return o.d.src.NotFound().And(o.g.src.Found()) }},
	{"g.Found&d.NotFound", func(o myCondOps) runtime.PipelineCond { return o.g.src.Found().And(o.d.src.NotFound()) }},
	{"get.Found&del.Eq0", func(o myCondOps) runtime.PipelineCond { return o.g.src.Found().And(o.d.src.RowsEq(0)) }},
	{"!(d.NotFound&g.Found)", func(o myCondOps) runtime.PipelineCond { return o.d.src.NotFound().And(o.g.src.Found()).Not() }},
	{"d.Found|g.Found", func(o myCondOps) runtime.PipelineCond { return o.d.src.Found().Or(o.g.src.Found()) }},
	{"d.Found|m.Found", func(o myCondOps) runtime.PipelineCond { return o.d.src.Found().Or(o.m.src.Found()) }},
	{"g.Found&m.NotFound", func(o myCondOps) runtime.PipelineCond { return o.g.src.Found().And(o.m.src.NotFound()) }},
	{"m.Found|g.NotFound", func(o myCondOps) runtime.PipelineCond { return o.m.src.Found().Or(o.g.src.NotFound()) }},
	{"u.Eq1&s.Ge2", func(o myCondOps) runtime.PipelineCond {
		return o.u.src.RowsEq(1).And(o.s.src.RowsGe(2))
	}},
	{"s.Gt1&u.Lt1", func(o myCondOps) runtime.PipelineCond {
		return o.s.src.RowsGt(1).And(o.u.src.RowsLt(1))
	}},
	{"s.Le1|u.Le1", func(o myCondOps) runtime.PipelineCond { return o.s.src.RowsLe(1).Or(o.u.src.RowsLe(1)) }},
	{"r.Eq2&d.Eq0", func(o myCondOps) runtime.PipelineCond {
		return o.r.src.RowsEq(2).And(o.d.src.RowsEq(0))
	}},
	{"r.Found&!d.Found", func(o myCondOps) runtime.PipelineCond { return o.r.src.Found().And(o.d.src.Found().Not()) }},
	// column predicates
	{"g.WhereCol(true)", func(o myCondOps) runtime.PipelineCond { return myScoreAtLeast(o.g, 30) }},
	{"g.WhereCol(false)&d.NotFound", func(o myCondOps) runtime.PipelineCond {
		return myScoreAtLeast(o.g, 31).And(o.d.src.NotFound())
	}},
	{"d.NotFound&g.WhereCol(true)", func(o myCondOps) runtime.PipelineCond {
		return o.d.src.NotFound().And(myScoreAtLeast(o.g, 30))
	}},
	{"s.WhereCol(all)&u.Found", func(o myCondOps) runtime.PipelineCond {
		return myScoreAtLeast(o.s, 10).And(o.u.src.Found())
	}},
	{"!s.WhereCol(some)|d.Found", func(o myCondOps) runtime.PipelineCond {
		return myScoreAtLeast(o.s, 20).Not().Or(o.d.src.Found())
	}},
	{"m.WhereCol&g.Found", func(o myCondOps) runtime.PipelineCond {
		return myScoreAtLeast(o.m, 0).And(o.g.src.Found())
	}},
	// three operations, nested
	{"(d.NotFound&g.Found)&s.Eq2", func(o myCondOps) runtime.PipelineCond {
		return o.d.src.NotFound().And(o.g.src.Found()).And(o.s.src.RowsEq(2))
	}},
	{"d.NotFound&(g.Found|m.Found)", func(o myCondOps) runtime.PipelineCond {
		return o.d.src.NotFound().And(o.g.src.Found().Or(o.m.src.Found()))
	}},
	{"!(m.Found|d.Found)&u.Found", func(o myCondOps) runtime.PipelineCond {
		return o.m.src.Found().Or(o.d.src.Found()).Not().And(o.u.src.Found())
	}},
	{"(g.Found&m.Found)|(d.NotFound&s.Gt1)", func(o myCondOps) runtime.PipelineCond {
		return o.g.src.Found().And(o.m.src.Found()).Or(o.d.src.NotFound().And(o.s.src.RowsGt(1)))
	}},
	{"!(d.Found|(g.Found&!m.Found))", func(o myCondOps) runtime.PipelineCond {
		return o.d.src.Found().Or(o.g.src.Found().And(o.m.src.Found().Not())).Not()
	}},
	{"same op twice", func(o myCondOps) runtime.PipelineCond {
		return o.g.src.Found().And(o.g.src.RowsEq(1)).And(o.d.src.NotFound())
	}},
	// operations that never ran (their branch was not taken) count as false, also under Not
	{"skipped.Found|d.NotFound", func(o myCondOps) runtime.PipelineCond { return o.skipped.src.Found().Or(o.d.src.NotFound()) }},
	{"skipped.NotFound&g.Found", func(o myCondOps) runtime.PipelineCond { return o.skipped.src.NotFound().And(o.g.src.Found()) }},
	{"!skipped.Found&g.Found", func(o myCondOps) runtime.PipelineCond { return o.skipped.src.Found().Not().And(o.g.src.Found()) }},
	{"skipped.NotFound", func(o myCondOps) runtime.PipelineCond { return o.skipped.src.NotFound() }},
	{"!skipped.Found", func(o myCondOps) runtime.PipelineCond { return o.skipped.src.Found().Not() }},
}

// myCondScenarios builds one scenario per variant: the operations run, then an If on the variant picks
// between two writes; a second If over the negated condition checks that binding never mutates it.
func myCondScenarios() []myScn {
	var out []myScn
	for _, v := range myCondVariants {
		out = append(out, myScn{
			name: "cond " + v.name,
			build: func(p myPipe) ([]*runtime.IfResult, func() string) {
				var o myCondOps
				o.d = p.DeleteItems([]int64{98, 99})
				o.u = p.AddScoreOne(1, 1)
				o.g = p.GetItem(3)
				o.m = p.GetItem(999)
				o.s = p.ItemsByIDs([]int64{1, 2})
				if p.Returning() {
					o.r = p.InsertItemsReturning([]myNew{{ID: 70, Org: 7, Name: "r1", Status: "x"}, {ID: 71, Org: 7, Name: "r2", Status: "x"}})
				} else {
					o.r = p.ItemsByIDs([]int64{3, 4})
				}
				// never taken: m finds nothing
				p.If(o.m.src.Found(), func(p myPipe) { o.skipped = p.GetItem(2) }, nil)
				cond := v.mk(o)
				r1 := p.If(cond,
					func(p myPipe) { p.AddScoreOne(1000, 2) },
					func(p myPipe) { p.AddScoreOne(7, 4) })
				// the same condition value again, and its negation: both must still be intact
				r2 := p.If(cond.Not(), func(p myPipe) { p.AddScoreOne(100000, 3) }, nil)
				return []*runtime.IfResult{r1, r2}, nil
			},
		})
	}
	return out
}

func TestRealEngineMySQLPipelineCompoundConditions(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		pr := openMyPair(t, db, dsn)
		for _, mode := range myModes {
			for _, sc := range myCondScenarios() {
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					myCompare(t, e, pr, mode, sc, false)
				})
			}
		}
	})
}
