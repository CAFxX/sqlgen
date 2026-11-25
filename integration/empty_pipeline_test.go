package integration

// Empty INSERT ... VALUES collections in pipelines, through the single-round-trip drivers (pgxwire for
// PostgreSQL / CockroachDB, mysqlwire for MySQL / MariaDB) and through the sequential fallback: the empty
// operation resolves to a no-op (0 rows / empty, never ErrBranchNotTaken unless it sits in an untaken If
// branch), the real operations around it still cost ONE network turn, the empty operation's SQL never goes
// over the wire, and a pipeline made only of such operations touches the network (and the connection) not
// at all.

import (
	"database/sql"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/CAFxX/sqlgen/integration/mdbq"
	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

// emptyOutcome is what a pipeline of real and empty operations resolved to.
type emptyOutcome struct {
	real1, real2       int64  // RowsAffected of the real updates around the empty ones
	ins                int64  // RowsAffected of the empty exec insert
	retLen             int    // rows of the empty RETURNING insert (-1: no such operation on this engine)
	got                string // name read by a real read queued after the empty operations
	thenAff            int64  // empty insert in the TAKEN If branch
	elseErr            error  // empty insert in the untaken If branch
	taken, skipped     bool
	errIns, errRet     error
	errThen, errReal12 error
}

func (o emptyOutcome) check(t *testing.T, label string, wantRet bool) {
	t.Helper()
	for name, err := range map[string]error{"empty insert": o.errIns, "empty RETURNING insert": o.errRet,
		"empty insert in the taken branch": o.errThen, "real updates": o.errReal12} {
		if err != nil {
			t.Errorf("%s: %s: %v", label, name, err)
		}
	}
	if o.real1 != 1 || o.real2 != 1 || o.ins != 0 || o.thenAff != 0 || o.got != "n2" || !o.taken || o.skipped {
		t.Errorf("%s: %+v", label, o)
	}
	if wantRet && o.retLen != 0 || !wantRet && o.retLen != -1 {
		t.Errorf("%s: empty RETURNING insert returned %d rows", label, o.retLen)
	}
	if !errors.Is(o.elseErr, runtime.ErrBranchNotTaken) {
		t.Errorf("%s: empty insert in the untaken branch: err = %v, want ErrBranchNotTaken", label, o.elseErr)
	}
}

// wireStats returns the number of pipelines of either kind that ran.
func wireStats(s runtime.CacheStats) uint64 { return s.WirePipelines + s.SequentialPipelines }

func setSnoop(t *testing.T, needle string) {
	b := []byte(needle)
	snoopNeedle.Store(&b)
	snoopHits.Store(0)
	t.Cleanup(func() { snoopNeedle.Store(nil) })
}

func TestRealEnginePGWireEmptyValuesInPipelines(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		dsn := os.Getenv(e.env)
		seedItems(t, db)
		ctx := ctxT(t)
		setSnoop(t, "INSERT INTO sg_items")

		mixed := func(q *pgq.Queries, tx bool) (o emptyOutcome) {
			var a1, a2, ins, thenIns, elseIns *runtime.ExecFuture
			var ret *runtime.SliceFuture[int64]
			var get *runtime.RowFuture[pgq.Item]
			var ifr *runtime.IfResult
			build := func(p *pgq.Pipeline) {
				a1 = p.AddScoreOne(1, 1)
				ins = p.InsertItems(nil)
				ret = p.InsertedIDs([]pgq.NewItem{})
				a2 = p.AddScoreOne(1, 3)
				g := p.GetItem(2)
				get = g
				ifr = p.If(g.Found(),
					func(p *pgq.Pipeline) { thenIns = p.InsertItems(nil) },
					func(p *pgq.Pipeline) { elseIns = p.InsertItems(nil) })
			}
			var err error
			if tx {
				err = q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { build(p); return nil })
			} else {
				p := q.Pipeline(ctx)
				build(p)
				err = p.Flush()
			}
			if err != nil {
				t.Fatalf("pipeline: %v", err)
			}
			o.real1, o.errReal12 = a1.RowsAffected()
			o.real2, _ = a2.RowsAffected()
			o.ins, o.errIns = ins.RowsAffected()
			var rows []int64
			rows, o.errRet = ret.Get()
			o.retLen = len(rows)
			it, _ := get.Get()
			o.got = it.Name
			o.thenAff, o.errThen = thenIns.RowsAffected()
			_, o.elseErr = elseIns.RowsAffected()
			o.taken, o.skipped = ifr.Taken(), ifr.Skipped()
			return o
		}
		onlyEmpty := func(q *pgq.Queries, tx bool) {
			var ins *runtime.ExecFuture
			var ret *runtime.SliceFuture[int64]
			var err error
			if tx {
				err = q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
					ins, ret = p.InsertItems(nil), p.InsertedIDs(nil)
					return nil
				})
			} else {
				p := q.Pipeline(ctx)
				ins, ret = p.InsertItems(nil), p.InsertedIDs(nil)
				err = p.Flush()
			}
			if err != nil {
				t.Fatalf("only-empty pipeline (tx=%v): %v", tx, err)
			}
			if n, err := ins.RowsAffected(); n != 0 || err != nil {
				t.Errorf("only-empty (tx=%v) insert: %d, %v", tx, n, err)
			}
			if rows, err := ret.Get(); len(rows) != 0 || err != nil {
				t.Errorf("only-empty (tx=%v) RETURNING insert: %v, %v", tx, rows, err)
			}
		}

		var results [2][2]emptyOutcome // [wire][tx]
		for wi, wire := range []bool{true, false} {
			wdb, turns, _ := countingDB(t, dsn, wire)
			q := pgq.New(wdb)
			for ti, tx := range []bool{true, false} {
				mixed(q, tx) // warm: statement texts seen by the connection
				var o emptyOutcome
				cost := measure(t, wdb, turns, func() { o = mixed(q, tx) })
				o.check(t, e.name+" wire="+map[bool]string{true: "y", false: "n"}[wire]+" tx="+map[bool]string{true: "y", false: "n"}[tx], true)
				results[wi][ti] = o
				if wire && cost != 1 {
					t.Errorf("mixed pipeline (tx=%v) with empty VALUES operations cost %d turns, want 1", tx, cost)
				}
				// A pipeline of only empty operations sends nothing and never takes a connection.
				before := wireStats(q.CacheStats())
				if c := measure(t, wdb, turns, func() { onlyEmpty(q, tx) }); c != 0 {
					t.Errorf("only-empty pipeline (wire=%v tx=%v) cost %d turns, want 0", wire, tx, c)
				}
				if after := wireStats(q.CacheStats()); after != before {
					t.Errorf("only-empty pipeline (wire=%v tx=%v) was counted as a flush: %d -> %d", wire, tx, before, after)
				}
				checkAssertedEmpty(t, wdb, turns, wire, tx, func(tx, withReal bool, want int64) error {
					build := func(p *pgq.Pipeline) {
						if withReal {
							p.AddScoreOne(0, 1)
						}
						p.InsertItems(nil).AssertRows(want)
					}
					if tx {
						return q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { build(p); return nil })
					}
					p := q.Pipeline(ctx)
					build(p)
					return p.Flush()
				})
			}
			q.Close()
		}
		if results[0] != results[1] {
			t.Errorf("wire and sequential disagree:\n  wire %+v\n  seq  %+v", results[0], results[1])
		}
		if n := snoopHits.Load(); n != 0 {
			t.Errorf("INSERT INTO sg_items went over the wire %d times: an empty VALUES operation reached the driver", n)
		}

		// A VALUES list that is NOT the INSERT row source stays an error, also in a pipeline.
		wdb, _, _ := countingDB(t, dsn, true)
		q := pgq.New(wdb)
		defer q.Close()
		if _, err := q.ApplyDeltas(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Errorf("empty VALUES in a CTE (exec): err = %v, want ErrEmptySliceParam", err)
		}
		// (the PipelineTx fails as a whole before anything is sent)
		if err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { p.ApplyDeltas(nil); return nil }); !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Errorf("empty VALUES in a CTE (pipeline): PipelineTx err = %v, want ErrEmptySliceParam", err)
		}
		if got, err := q.ItemsByIDs(ctx, nil); got != nil || !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Errorf("empty IN list: %v, %v", got, err)
		}
	})
}

// The MySQL family: myq on MySQL (exec inserts), mdbq on MariaDB (adds INSERT ... RETURNING).
func TestRealEngineMySQLWireEmptyValuesInPipelines(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		seedItems(t, db)
		ctx := ctxT(t)
		maria := e.family == famMariaDB
		setSnoop(t, "INSERT INTO sg_items")

		// The two generated packages have identical pipeline shapes; the bodies are written once per package.
		mixedMy := func(q *myq.Queries, tx bool) (o emptyOutcome) {
			var a1, a2, ins, thenIns, elseIns *runtime.ExecFuture
			var get *runtime.RowFuture[myq.Item]
			var ifr *runtime.IfResult
			build := func(p *myq.Pipeline) {
				a1 = p.AddScoreOne(1, 1)
				ins = p.InsertItems(nil)
				a2 = p.AddScoreOne(1, 3)
				g := p.GetItem(2)
				get = g
				ifr = p.If(g.Found(),
					func(p *myq.Pipeline) { thenIns = p.InsertItems(nil) },
					func(p *myq.Pipeline) { elseIns = p.InsertItems(nil) })
			}
			var err error
			if tx {
				err = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { build(p); return nil })
			} else {
				p := q.Pipeline(ctx)
				build(p)
				err = p.Flush()
			}
			if err != nil {
				t.Fatalf("pipeline: %v", err)
			}
			o.real1, o.errReal12 = a1.RowsAffected()
			o.real2, _ = a2.RowsAffected()
			o.ins, o.errIns = ins.RowsAffected()
			o.retLen = -1
			it, _ := get.Get()
			o.got = it.Name
			o.thenAff, o.errThen = thenIns.RowsAffected()
			_, o.elseErr = elseIns.RowsAffected()
			o.taken, o.skipped = ifr.Taken(), ifr.Skipped()
			return o
		}
		mixedMaria := func(q *mdbq.Queries, tx bool) (o emptyOutcome) {
			var a1, a2, ins, thenIns, elseIns *runtime.ExecFuture
			var ret *runtime.SliceFuture[int64]
			var get *runtime.RowFuture[mdbq.Item]
			var ifr *runtime.IfResult
			build := func(p *mdbq.Pipeline) {
				a1 = p.AddScoreOne(1, 1)
				ins = p.InsertItems(nil)
				ret = p.InsertedIDs([]mdbq.NewItem{})
				a2 = p.AddScoreOne(1, 3)
				g := p.GetItem(2)
				get = g
				ifr = p.If(g.Found(),
					func(p *mdbq.Pipeline) { thenIns = p.InsertItems(nil) },
					func(p *mdbq.Pipeline) { elseIns = p.InsertItems(nil) })
			}
			var err error
			if tx {
				err = q.PipelineTx(ctx, nil, func(p *mdbq.Pipeline) error { build(p); return nil })
			} else {
				p := q.Pipeline(ctx)
				build(p)
				err = p.Flush()
			}
			if err != nil {
				t.Fatalf("pipeline: %v", err)
			}
			o.real1, o.errReal12 = a1.RowsAffected()
			o.real2, _ = a2.RowsAffected()
			o.ins, o.errIns = ins.RowsAffected()
			var rows []int64
			rows, o.errRet = ret.Get()
			o.retLen = len(rows)
			it, _ := get.Get()
			o.got = it.Name
			o.thenAff, o.errThen = thenIns.RowsAffected()
			_, o.elseErr = elseIns.RowsAffected()
			o.taken, o.skipped = ifr.Taken(), ifr.Skipped()
			return o
		}
		onlyEmptyMy := func(q *myq.Queries, tx bool) {
			var ins *runtime.ExecFuture
			var err error
			if tx {
				err = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { ins = p.InsertItems(nil); return nil })
			} else {
				p := q.Pipeline(ctx)
				ins = p.InsertItems(nil)
				err = p.Flush()
			}
			if n, ierr := ins.RowsAffected(); err != nil || ierr != nil || n != 0 {
				t.Errorf("only-empty (tx=%v): %d, %v, %v", tx, n, ierr, err)
			}
		}
		onlyEmptyMaria := func(q *mdbq.Queries, tx bool) {
			var ins *runtime.ExecFuture
			var ret *runtime.SliceFuture[int64]
			var err error
			if tx {
				err = q.PipelineTx(ctx, nil, func(p *mdbq.Pipeline) error {
					ins, ret = p.InsertItems(nil), p.InsertedIDs(nil)
					return nil
				})
			} else {
				p := q.Pipeline(ctx)
				ins, ret = p.InsertItems(nil), p.InsertedIDs(nil)
				err = p.Flush()
			}
			n, ierr := ins.RowsAffected()
			rows, rerr := ret.Get()
			if err != nil || ierr != nil || rerr != nil || n != 0 || len(rows) != 0 {
				t.Errorf("only-empty (tx=%v): %d, %v, %v, %v, %v", tx, n, rows, ierr, rerr, err)
			}
		}

		var results [2][2]emptyOutcome
		for wi, wire := range []bool{true, false} {
			wdb, turns := myOpenCounted(t, dsn, wire, nil)
			var stats func() runtime.CacheStats
			var mixed func(tx bool) emptyOutcome
			var only func(tx bool)
			var asserted func(tx, withReal bool, want int64) error
			if maria {
				q := mdbq.New(wdb)
				defer q.Close()
				stats, mixed, only = q.CacheStats, func(tx bool) emptyOutcome { return mixedMaria(q, tx) }, func(tx bool) { onlyEmptyMaria(q, tx) }
				asserted = func(tx, withReal bool, want int64) error {
					build := func(p *mdbq.Pipeline) {
						if withReal {
							p.AddScoreOne(0, 1)
						}
						p.InsertItems(nil).AssertRows(want)
					}
					if tx {
						return q.PipelineTx(ctx, nil, func(p *mdbq.Pipeline) error { build(p); return nil })
					}
					p := q.Pipeline(ctx)
					build(p)
					return p.Flush()
				}
			} else {
				q := myq.New(wdb)
				defer q.Close()
				stats, mixed, only = q.CacheStats, func(tx bool) emptyOutcome { return mixedMy(q, tx) }, func(tx bool) { onlyEmptyMy(q, tx) }
				asserted = func(tx, withReal bool, want int64) error {
					build := func(p *myq.Pipeline) {
						if withReal {
							p.AddScoreOne(0, 1)
						}
						p.InsertItems(nil).AssertRows(want)
					}
					if tx {
						return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { build(p); return nil })
					}
					p := q.Pipeline(ctx)
					build(p)
					return p.Flush()
				}
			}
			for ti, tx := range []bool{true, false} {
				mixed(tx)
				var o emptyOutcome
				cost := measure(t, wdb, turns, func() { o = mixed(tx) })
				o.check(t, e.name, maria)
				results[wi][ti] = o
				if wire && cost != 1 {
					t.Errorf("mixed pipeline (tx=%v) with empty VALUES operations cost %d turns, want 1", tx, cost)
				}
				before := wireStats(stats())
				if c := measure(t, wdb, turns, func() { only(tx) }); c != 0 {
					t.Errorf("only-empty pipeline (wire=%v tx=%v) cost %d turns, want 0", wire, tx, c)
				}
				if after := wireStats(stats()); after != before {
					t.Errorf("only-empty pipeline (wire=%v tx=%v) was counted as a flush: %d -> %d", wire, tx, before, after)
				}
				checkAssertedEmpty(t, wdb, turns, wire, tx, asserted)
			}
		}
		if results[0] != results[1] {
			t.Errorf("wire and sequential disagree:\n  wire %+v\n  seq  %+v", results[0], results[1])
		}
		if n := snoopHits.Load(); n != 0 {
			t.Errorf("INSERT INTO sg_items went over the wire %d times: an empty VALUES operation reached the driver", n)
		}
	})
}

// checkAssertedEmpty: an assertion over an empty (no-op) operation is decided client-side, its row count
// being 0 (DOCS section 2.3). A passing one costs nothing (0 turns alone, the single wire turn with a real
// operation), a failing one fails the pipeline before anything is sent (0 turns, nothing applied).
func checkAssertedEmpty(t *testing.T, wdb *sql.DB, turns *atomic.Int64, wire, tx bool, run func(tx, withReal bool, want int64) error) {
	t.Helper()
	for _, c := range []struct {
		withReal bool
		want     int64
	}{{false, 0}, {true, 0}, {false, 1}, {true, 1}} {
		var err error
		cost := measure(t, wdb, turns, func() { err = run(tx, c.withReal, c.want) })
		if (c.want == 0) != (err == nil) || c.want != 0 && !errors.Is(err, runtime.ErrAssertionFailed) {
			t.Errorf("asserted empty op (wire=%v tx=%v real=%v want=%d): err = %v", wire, tx, c.withReal, c.want, err)
		}
		wantCost := int64(0)
		if c.withReal && c.want == 0 {
			wantCost = 1
		}
		if (wire || wantCost == 0) && cost != wantCost {
			t.Errorf("asserted empty op (wire=%v tx=%v real=%v want=%d) cost %d turns, want %d", wire, tx, c.withReal, c.want, cost, wantCost)
		}
	}
}
