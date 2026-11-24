package integration

// A seeded, bounded differential fuzz: random pipelines (reads, writes, assertions, nested If/Else on
// simple and compound conditions over any earlier operation, Abort) must behave identically on the
// single-round-trip path and on the sequential fallback -- same error kind, same final table state, same
// IfResult / future values -- in a PipelineTx, in a standalone Flush and in a user-managed transaction.

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
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
	}
	return f.e
}

type fuzzGen struct {
	rnd   *rand.Rand
	futs  []fuzzFut
	ifs   []*runtime.IfResult
	trace []string
}

func (g *fuzzGen) tr(depth int, f string, a ...any) {
	g.trace = append(g.trace, strings.Repeat("  ", depth)+fmt.Sprintf(f, a...))
}

// leaf returns a random condition over f.
func (g *fuzzGen) leaf(f fuzzFut) runtime.PipelineCond {
	n := int64(g.rnd.IntN(3))
	if f.r != nil && g.rnd.IntN(5) == 0 {
		return f.r.WhereCol("score >= 50", func(it pgq.Item) bool { return it.Score >= 50 })
	}
	if f.s != nil && g.rnd.IntN(5) == 0 {
		return f.s.WhereCol("score >= 20", func(it pgq.Item) bool { return it.Score >= 20 })
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
		switch k := g.rnd.IntN(13); {
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
			hasElse := g.rnd.IntN(2) == 0
			r := p.If(cond, func(p *pgq.Pipeline) {
				g.gen(p, depth+1, 1+g.rnd.IntN(3))
			}, func() func(*pgq.Pipeline) {
				if !hasElse {
					return nil
				}
				return func(p *pgq.Pipeline) {
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
	case errors.Is(err, runtime.ErrAssertionFailed):
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
		fmt.Fprintf(&sb, "if(%v,%v)", r.Taken(), r.Skipped())
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
			fmt.Fprintf(&sb, " bump[%v,%s,sk=%v]", v, fuzzClassify(e), errors.Is(e, runtime.ErrBranchNotTaken))
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
	perMode := 100
	if v := os.Getenv("SQLGEN_FUZZ_SEEDS"); v != "" {
		fmt.Sscan(v, &perMode)
	}
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("the single-round-trip pipeline SQL is only generated for the $N (PostgreSQL-protocol) dialect")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		wire, plain := openWire(t, dsn, true), openWire(t, dsn, false)
		bad := 0
		for mi, mode := range []string{"PipelineTx", "standalone Flush", "user-managed tx"} {
			for i := 0; i < perMode; i++ {
				seed := uint64(1000*(mi+1) + i) // fixed seeds: a failure names the seed and is reproducible
				w, wtrace := fuzzRun(t, wire, seed, mode)
				s, _ := fuzzRun(t, plain, seed, mode)
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
