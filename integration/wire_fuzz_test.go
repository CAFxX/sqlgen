package integration

// A seeded, bounded differential fuzz: random pipelines (reads, writes, assertions, nested If/Else on
// simple and compound conditions over any earlier operation, Abort) must behave identically on the
// single-round-trip path and on the sequential fallback -- same error kind, same final table state, same
// IfResult / future values -- in a PipelineTx, in a standalone Flush and in a user-managed transaction.

import (
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

var errFuzzAbort = errors.New("fuzz abort")

type fuzzFut struct {
	kind string
	e    *runtime.ExecFuture
	r    *runtime.RowFuture[pgq.Item]
	s    *runtime.SliceFuture[pgq.Item]
	w    *runtime.SliceFuture[pgq.Written]
	n    *runtime.RowFuture[int64]   // a producer of a forwarded value
	ns   *runtime.SliceFuture[int64] // FwdItemsAbove
}

// fuzzCondSrc is what every future offers for building conditions.
type fuzzCondSrc interface {
	Found() runtime.PipelineCond
	NotFound() runtime.PipelineCond
	RowsEq(int64) runtime.PipelineCond
	RowsGt(int64) runtime.PipelineCond
	RowsGe(int64) runtime.PipelineCond
	RowsLt(int64) runtime.PipelineCond
	RowsLe(int64) runtime.PipelineCond
}

func (f fuzzFut) src() fuzzCondSrc {
	switch {
	case f.r != nil:
		return f.r
	case f.s != nil:
		return f.s
	case f.w != nil:
		return f.w
	case f.n != nil:
		return f.n
	case f.ns != nil:
		return f.ns
	}
	return f.e
}

// fuzzIf is a generated If chain and its number of arms.
type fuzzIf struct {
	c    ifRes
	arms int
}

type fuzzGen struct {
	rnd   *rand.Rand
	futs  []fuzzFut
	ifs   []fuzzIf
	trace []string
	// avail are the forwarded values a consumer queued here may use: those produced in this If branch or
	// in an enclosing one (a value from a sibling branch is a build error by design, not fuzzed).
	avail []runtime.Arg[int64]
}

// fwdArg returns a forwarded value in scope, or (sometimes, or when there is none) a plain one.
func (g *fuzzGen) fwdArg() (runtime.Arg[int64], string) {
	if len(g.avail) == 0 || g.rnd.IntN(4) == 0 {
		v := int64(g.rnd.IntN(70))
		return runtime.Val(v), fmt.Sprint(v)
	}
	k := g.rnd.IntN(len(g.avail))
	return g.avail[k], fmt.Sprintf("v%d", k)
}

func (g *fuzzGen) tr(depth int, f string, a ...any) {
	g.trace = append(g.trace, strings.Repeat("  ", depth)+fmt.Sprintf(f, a...))
}

// leaf returns a random condition over f.
func (g *fuzzGen) leaf(f fuzzFut) runtime.PipelineCond {
	n := int64(g.rnd.IntN(3))
	if f.r != nil && g.rnd.IntN(5) == 0 {
		return f.r.WhereCol("score >= 50")
	}
	if f.s != nil && g.rnd.IntN(5) == 0 {
		return f.s.WhereCol("score >= 20")
	}
	c := f.src()
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

// cond returns a random condition: one leaf, or two or three leaves over any earlier operations
// combined with And / Or and optionally negated.
func (g *fuzzGen) cond() runtime.PipelineCond {
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

func (g *fuzzGen) gen(p *pgq.Pipeline, depth, n int) {
	for i := 0; i < n; i++ {
		id := int64(g.rnd.IntN(5) + 1)
		switch k := g.rnd.IntN(16); {
		case k == 0:
			f := p.InsertItem(id, 1, "n", int64(g.rnd.IntN(100)), "new")
			g.tr(depth, "insert %d", id)
			if g.rnd.IntN(3) == 0 {
				f.AssertRows(1)
				g.tr(depth, " assert 1")
			}
			g.futs = append(g.futs, fuzzFut{kind: "ins", e: f})
		case k == 1 || k == 2:
			f := p.AddScoreOne(int64(g.rnd.IntN(10)), id)
			g.tr(depth, "update %d", id)
			switch g.rnd.IntN(4) {
			case 0:
				f.AssertRows(1)
				g.tr(depth, " assert 1")
			case 1:
				f.AssertRows(0)
				g.tr(depth, " assert 0")
			}
			g.futs = append(g.futs, fuzzFut{kind: "upd", e: f})
		case k == 3:
			f := p.DeleteItems([]int64{id, id + 1})
			g.tr(depth, "delete %d,%d", id, id+1)
			if g.rnd.IntN(4) == 0 {
				x := int64(g.rnd.IntN(3))
				f.AssertRows(x)
				g.tr(depth, " assert %d", x)
			}
			g.futs = append(g.futs, fuzzFut{kind: "del", e: f})
		case k == 4 || k == 5:
			f := p.GetItem(id)
			g.tr(depth, "get#%d %d", len(g.futs), id)
			if g.rnd.IntN(5) == 0 {
				f.AssertFound()
				g.tr(depth, " assertFound")
			}
			g.futs = append(g.futs, fuzzFut{kind: "get", r: f})
		case k == 6:
			f := p.ItemsByIDs([]int64{id, id + 1, id + 2})
			g.tr(depth, "many#%d %d..", len(g.futs), id)
			if g.rnd.IntN(5) == 0 {
				x := int64(g.rnd.IntN(4))
				f.AssertRows(x)
				g.tr(depth, " assertRows %d", x)
			}
			g.futs = append(g.futs, fuzzFut{kind: "many", s: f})
		case k == 7 || k == 8:
			if depth >= 3 || len(g.futs) == 0 {
				continue
			}
			cond := g.cond()
			g.tr(depth, "if (%d futures so far)", len(g.futs))
			mark := len(g.avail)
			arm := func(label string, n int) func(*pgq.Pipeline) {
				return func(p *pgq.Pipeline) {
					if label != "" {
						g.tr(depth, "%s", label)
					}
					g.gen(p, depth+1, n)
					g.avail = g.avail[:mark]
				}
			}
			// a chain: If, up to two ElseIf (whose conditions may read the earlier arms' operations), an
			// optional Else
			c := p.If(cond, arm("", 1+g.rnd.IntN(3)))
			arms := 1
			for e := g.rnd.IntN(3); e > 0; e-- {
				c.ElseIf(g.cond(), arm("elseif", 1+g.rnd.IntN(2)))
				arms++
			}
			if g.rnd.IntN(2) == 0 {
				c.Else(arm("else", 1+g.rnd.IntN(2)))
				arms++
			}
			g.ifs = append(g.ifs, fuzzIf{c, arms})
		case k == 9 && depth > 0:
			g.tr(depth, "ABORT")
			p.Abort(errFuzzAbort)
		case k == 10:
			g.tr(depth, "setall")
			g.futs = append(g.futs, fuzzFut{kind: "set", e: p.SetAllStatus([]string{"new", "active"}[g.rnd.IntN(2)])})
		case k == 11:
			g.tr(depth, "bump returning %d", id)
			f := p.BumpScoresReturning(int64(g.rnd.IntN(5)), []int64{id, id + 1})
			if g.rnd.IntN(4) == 0 {
				x := int64(g.rnd.IntN(3))
				f.AssertRows(x)
				g.tr(depth, " assertRows %d", x)
			}
			g.futs = append(g.futs, fuzzFut{kind: "bump", w: f})
		case k == 12 || k == 13:
			// a producer of a forwarded value: 0 or 1 rows, several rows, exactly one row, a RETURNING id
			var f *runtime.RowFuture[int64]
			switch g.rnd.IntN(4) {
			case 0:
				f = p.FwdScoreOf(id)
				g.tr(depth, "v%d = score of %d", len(g.avail), id)
			case 1:
				st := []string{"new", "active"}[g.rnd.IntN(2)]
				f = p.FwdIDsByStatus(st)
				g.tr(depth, "v%d = ids with status %s", len(g.avail), st)
			case 2:
				f = p.CountOrgItems(1)
				g.tr(depth, "v%d = count", len(g.avail))
			default:
				f = p.FwdInsertReturningID(id+int64(g.rnd.IntN(2))*30, 1, "r")
				g.tr(depth, "v%d = insert returning", len(g.avail))
			}
			if g.rnd.IntN(6) == 0 {
				f.AssertFound()
				g.tr(depth, " assertFound")
			}
			g.avail = append(g.avail, f.Arg())
			g.futs = append(g.futs, fuzzFut{kind: "fwd", n: f})
		case k == 14 || k == 15:
			// a consumer
			a, as := g.fwdArg()
			switch g.rnd.IntN(3) {
			case 0:
				b, bs := g.fwdArg()
				f := p.FwdSetScore(a, b)
				g.tr(depth, "set score=%s where id=%s", as, bs)
				if g.rnd.IntN(3) == 0 {
					f.AssertRows(1)
					g.tr(depth, " assert 1")
				}
				g.futs = append(g.futs, fuzzFut{kind: "fwdset", e: f})
			case 1:
				g.tr(depth, "items above %s", as)
				g.futs = append(g.futs, fuzzFut{kind: "fwdabove", ns: p.FwdItemsAbove(a)})
			default:
				g.tr(depth, "insert %d score %s", id+40, as)
				g.futs = append(g.futs, fuzzFut{kind: "fwdins", e: p.FwdInsertItem(runtime.Val(id+40), "f", a)})
			}
		}
	}
}

func fuzzClassify(err error) string {
	var pe *pgconn.PgError
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, errFuzzAbort):
		return "abort"
	case errors.Is(err, runtime.ErrAssertionFailed), errors.Is(err, runtime.ErrArgUnresolved):
		return errKind(err)
	case errors.As(err, &pe):
		return "pg" + pe.Code
	}
	return "other:" + err.Error()
}

// describe renders what a caller can observe after a successful flush; after a failed one the state of
// the individual futures is unspecified (which operations ran differs by design) and is not compared.
func (g *fuzzGen) describe(err error) string {
	if err != nil {
		return ""
	}
	var sb strings.Builder
	for _, r := range g.ifs {
		fmt.Fprintf(&sb, "if(")
		for i := range r.arms {
			fmt.Fprintf(&sb, "%v,", r.c.Taken(i))
		}
		fmt.Fprintf(&sb, "%v)", r.c.Skipped())
	}
	for _, f := range g.futs {
		switch {
		case f.e != nil:
			n, e := f.e.RowsAffected()
			fmt.Fprintf(&sb, " %s[%d,%s,sk=%v]", f.kind, n, fuzzClassify(e), errors.Is(e, runtime.ErrBranchNotTaken))
		case f.r != nil:
			v, e := f.r.Get()
			fmt.Fprintf(&sb, " get[%v,%s,sk=%v]", v, fuzzClassify(e), errors.Is(e, runtime.ErrBranchNotTaken))
		case f.s != nil:
			v, e := f.s.Get()
			fmt.Fprintf(&sb, " many[%v,%s,sk=%v]", v, fuzzClassify(e), errors.Is(e, runtime.ErrBranchNotTaken))
		case f.w != nil:
			v, e := f.w.Get()
			// UPDATE ... RETURNING returns rows in the order the executor met them, which SQL leaves
			// unspecified (a CTE-wrapped UPDATE and a plain one can differ): compare them by id
			v = slices.Clone(v)
			slices.SortFunc(v, func(a, b pgq.Written) int { return cmp.Compare(a.ID, b.ID) })
			fmt.Fprintf(&sb, " bump[%v,%s,sk=%v]", v, fuzzClassify(e), errors.Is(e, runtime.ErrBranchNotTaken))
		case f.n != nil:
			v, e := f.n.Get()
			fmt.Fprintf(&sb, " fwd[%v,%s,sk=%v]", v, fuzzClassify(e), errors.Is(e, runtime.ErrBranchNotTaken))
		case f.ns != nil:
			v, e := f.ns.Get()
			// The guard CTE a statement with a recorded condition is wrapped in does not carry its ORDER BY
			// on every engine (CockroachDB): compare the ids as a set, as for bump above
			v = slices.Clone(v)
			slices.Sort(v)
			fmt.Fprintf(&sb, " above[%v,%s,sk=%v]", v, fuzzClassify(e), errors.Is(e, runtime.ErrBranchNotTaken))
		}
	}
	return sb.String()
}

// fuzzRun executes the pipeline generated from seed in the given mode and returns everything an
// observer can see plus the generator trace.
func fuzzRun(t *testing.T, db *sql.DB, seed uint64, mode string) (outcome, trace string) {
	t.Helper()
	mustExec(t, db, "DELETE FROM sg_items")
	mustExec(t, db, "INSERT INTO sg_items VALUES (1,1,'a',10,'new'),(2,1,'b',60,'new'),(3,1,'c',20,'new')")
	q := pgq.New(db)
	defer q.Close()
	var g *fuzzGen
	build := func(p *pgq.Pipeline) error {
		g = &fuzzGen{rnd: rand.New(rand.NewPCG(seed, 99))}
		g.gen(p, 0, 2+g.rnd.IntN(7))
		return nil
	}
	ctx := ctxT(t)
	var err error
	switch mode {
	case "PipelineTx":
		err = q.PipelineTx(ctx, nil, build)
	case "standalone Flush":
		p := q.Pipeline(ctx)
		build(p)
		err = p.Flush()
	case "user-managed tx":
		tx, berr := db.BeginTx(ctx, nil)
		if berr != nil {
			t.Fatal(berr)
		}
		p := q.WithTx(tx).Pipeline(ctx)
		build(p)
		err = p.Flush()
		// A failed assertion or Abort leaves the transaction usable, a database error aborts it: both
		// paths must agree. (The final state is not compared: the sequential path keeps the effect of
		// the failing operation itself, the wire path discards it.)
		_, lerr := tx.ExecContext(ctx, "SELECT 1")
		_ = tx.Rollback()
		return fuzzClassify(err) + "|later=" + fuzzClassify(lerr) + "|" + g.describe(err), strings.Join(g.trace, "\n")
	}
	return fuzzClassify(err) + "|" + snapshot(t, db) + "|" + g.describe(err), strings.Join(g.trace, "\n")
}

func TestRealEnginePipelineDifferentialFuzz(t *testing.T) {
	perMode := 100 * envFuzzScale()
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("the single-round-trip pipeline SQL is only generated for the $N (PostgreSQL-protocol) dialect")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		wire, plain := openWire(t, dsn, true), openWire(t, dsn, false)
		bad, fwd, unresolved := 0, 0, 0
		for mi, mode := range []string{"PipelineTx", "standalone Flush", "user-managed tx"} {
			for i := 0; i < perMode; i++ {
				seed := uint64(1000*(mi+1) + i) // fixed seeds: a failure names the seed and is reproducible
				w, wtrace := fuzzRun(t, wire, seed, mode)
				s, _ := fuzzRun(t, plain, seed, mode)
				if strings.Contains(wtrace, " = ") {
					fwd++
				}
				if strings.Contains(w, "ErrArgUnresolved") {
					unresolved++
				}
				if w != s {
					bad++
					if bad <= 5 {
						t.Errorf("[%s seed=%d] wire diverges from sequential\n wire : %s\n plain: %s\n generated:\n%s", mode, seed, w, s, wtrace)
					}
				}
			}
		}
		t.Logf("%d of %d pipelines forwarded values, %d failed with ErrArgUnresolved", fwd, 3*perMode, unresolved)
		if bad > 0 {
			t.Errorf("%d of %d pipelines diverged", bad, 3*perMode)
		}
	})
}
