package integration

// The single-round-trip pipeline path (pgxwire) must be observationally equivalent to the sequential
// fallback: same final table state, same error kind, same IfResult.Taken() and the same future values,
// on every PostgreSQL-protocol engine and for every way of running a pipeline (PipelineTx, a standalone
// Flush, and a Flush inside a user-managed transaction).

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/pgxwire"
	"github.com/CAFxX/sqlgen/runtime"
)

// openWire opens dsn through pgxwire (single-round-trip pipelines) or, for the sequential baseline,
// through the plain pgx stdlib driver.
func openWire(t testing.TB, dsn string, wire bool) *sql.DB {
	t.Helper()
	var (
		db  *sql.DB
		err error
	)
	if wire {
		db, err = pgxwire.OpenDB(dsn)
	} else {
		db, err = sql.Open("pgx", dsn)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type outcome struct {
	Err    string
	State  string    // every row, id-ordered
	Taken  []bool    // IfResult.Taken() for each If queued by the scenario
	Report string    // values read from futures after a successful flush
	Stats  [2]uint64 // WirePipelines, SequentialPipelines
}

func snapshot(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query("SELECT id, org_id, name, score, status FROM sg_items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var id, org, sc int64
		var name, status string
		if err := rows.Scan(&id, &org, &name, &sc, &status); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sb, "%d/%d/%s/%d/%s;", id, org, name, sc, status)
	}
	return sb.String()
}

// A scenario queues work and returns every IfResult plus a function that renders the futures' values
// (called only after a successful flush).
type scenario struct {
	name  string
	build func(p *pgq.Pipeline) (ifs []*runtime.IfResult, report func() string)
}

var errStop = errors.New("scenario abort")

func fmtSlice[T any](f *runtime.SliceFuture[T]) string {
	v, err := f.Get()
	return fmt.Sprintf("%v|%v", v, err)
}

func fmtRow[T any](f *runtime.RowFuture[T]) string {
	v, err := f.Get()
	return fmt.Sprintf("%v|%v", v, err)
}

var wireScenarios = []scenario{
	{"guarded transfer succeeds", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(-5, 1).AssertRows(1)
		p.AddScoreOne(5, 2).AssertRows(1)
		return nil, nil
	}},
	{"failed assertion rolls the whole batch back", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(-5, 1).AssertRows(1)
		p.AddScoreOne(5, 999).Label("line 2").AssertRows(1) // no such row: operation 1, labelled
		return nil, nil
	}},
	// The futures of the operations before a failed assertion stay readable after the failure, and say the
	// same thing on both paths; the operations after it never ran.
	{"futures around a failed assertion", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		g := p.GetItem(1)
		u := p.AddScoreOne(5, 2)
		s := p.ItemsByIDs([]int64{1, 2})
		p.AddScoreOne(5, 999).AssertRows(1)
		after := p.GetItem(3)
		return nil, func() string {
			n, uerr := u.RowsAffected()
			return fmtRow(g) + " " + fmt.Sprintf("%d|%v", n, uerr) + " " + fmtSlice(s) + " " + fmtRow(after)
		}
	}},
	{"AssertFound on a row-returning op that finds the row", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		g := p.GetItem(1).AssertFound()
		p.AddScoreOne(1, 1).AssertRows(1)
		return nil, func() string { return fmtRow(g) }
	}},
	{"AssertFound on a row-returning op that finds nothing", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1).AssertRows(1)
		p.GetItem(999).AssertFound() // must fire even though the op returns zero rows
		return nil, nil
	}},
	{"assertion on a many-op row count", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1)
		p.ItemsByIDs([]int64{1, 2}).AssertRows(3)
		return nil, nil
	}},
	{"zero-row assertion", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 999).AssertRows(0)
		p.DeleteItems([]int64{998, 999}).AssertRows(0)
		return nil, nil
	}},
	{"if: row found -> then branch", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *pgq.Pipeline) { p.AddScoreOne(100, 2).AssertRows(1) },
			func(p *pgq.Pipeline) { p.AddScoreOne(7, 3).AssertRows(1) })}, nil
	}},
	{"if: row missing -> else branch", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(12345)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *pgq.Pipeline) { p.AddScoreOne(100, 2).AssertRows(1) },
			func(p *pgq.Pipeline) { p.AddScoreOne(7, 3).AssertRows(1) })}, nil
	}},
	{"if: unguarded else delete", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(12345)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *pgq.Pipeline) { p.DeleteItems([]int64{1}) },
			func(p *pgq.Pipeline) { p.DeleteItems([]int64{4}) })}, nil
	}},
	{"WhereCol predicate true -> then", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(2) // score 20
		return []*runtime.IfResult{p.If(get.WhereCol("score >= 20"),
			func(p *pgq.Pipeline) { p.AddScoreOne(1, 2).AssertRows(1) },
			func(p *pgq.Pipeline) { p.AddScoreOne(2, 3).AssertRows(1) })}, nil
	}},
	{"WhereCol predicate false -> else", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(2)
		return []*runtime.IfResult{p.If(get.WhereCol("score >= 21"),
			func(p *pgq.Pipeline) { p.AddScoreOne(1, 2).AssertRows(1) },
			func(p *pgq.Pipeline) { p.AddScoreOne(2, 3).AssertRows(1) })}, nil
	}},
	{"gated single-row insert (then) / multi-row insert (else)", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *pgq.Pipeline) { p.InsertItem(50, 9, "then-row", 5, "new").AssertRows(1) },
			func(p *pgq.Pipeline) {
				p.InsertItems([]pgq.NewItem{{ID: 60, OrgID: 9, Name: "else-a"}, {ID: 61, OrgID: 9, Name: "else-b"}})
			})}, nil
	}},
	{"gated multi-row insert runs when the else branch is taken", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(777)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *pgq.Pipeline) { p.InsertItem(50, 9, "then-row", 5, "new").AssertRows(1) },
			func(p *pgq.Pipeline) {
				p.InsertItems([]pgq.NewItem{{ID: 61, OrgID: 9, Name: "else-b", Status: "x"}, {ID: 60, OrgID: 9, Name: "else-a", Status: "x"}})
			})}, nil
	}},
	{"gated UPDATE without WHERE", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *pgq.Pipeline) { p.SetAllStatus("then") },
			func(p *pgq.Pipeline) { p.SetAllStatus("else") })}, nil
	}},
	{"abort in the taken branch rolls everything back", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1).AssertRows(1)
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *pgq.Pipeline) { p.Abort(errStop) }, nil)}, nil
	}},
	{"abort in the branch NOT taken is inert", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1).AssertRows(1)
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(), nil,
			func(p *pgq.Pipeline) { p.Abort(errStop) })}, nil
	}},
	{"nested if", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		outer := p.GetItem(1)
		var inner *runtime.IfResult
		o := p.If(outer.Found(), func(p *pgq.Pipeline) {
			g2 := p.GetItem(12345)
			inner = p.If(g2.Found(),
				func(p *pgq.Pipeline) { p.AddScoreOne(1000, 1) },
				func(p *pgq.Pipeline) { p.AddScoreOne(5, 4).AssertRows(1) })
		}, func(p *pgq.Pipeline) { p.AddScoreOne(2000, 1) })
		return []*runtime.IfResult{o, inner}, nil
	}},
	{"two conditions on one future", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(1)
		i1 := p.If(get.Found(), func(p *pgq.Pipeline) { p.AddScoreOne(1, 2) }, nil)
		i2 := p.If(get.NotFound(),
			func(p *pgq.Pipeline) { p.AddScoreOne(1000, 3) },
			func(p *pgq.Pipeline) { p.AddScoreOne(7, 4) })
		return []*runtime.IfResult{i1, i2}, nil
	}},
	{"server error in the middle of the batch (duplicate key)", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1).AssertRows(1)
		p.InsertItem(2, 1, "dup", 0, "x")
		p.AddScoreOne(1, 3)
		return nil, nil
	}},
	{"RETURNING op feeds a condition", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		b := p.BumpScoresReturning(5, []int64{1, 2})
		r := p.If(b.RowsEq(2),
			func(p *pgq.Pipeline) { p.AddScoreOne(1, 3) },
			func(p *pgq.Pipeline) { p.AddScoreOne(1, 4) })
		// UPDATE ... RETURNING returns its rows in no particular order (it varies with the physical row
		// order): compare them as a set
		return []*runtime.IfResult{r}, func() string {
			v, err := b.Get()
			slices.SortFunc(v, func(x, y pgq.Written) int { return cmp.Compare(x.ID, y.ID) })
			return fmt.Sprintf("%v|%v", v, err)
		}
	}},
	{"reads return the same values", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		g := p.GetItem(2)
		missing := p.GetItem(404)
		many := p.ItemsByIDs([]int64{3, 1, 2})
		return nil, func() string { return fmtRow(g) + "#" + fmtRow(missing) + "#" + fmtSlice(many) }
	}},
	{"gated RETURNING upsert in the else branch", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(999)
		var up *runtime.SliceFuture[pgq.Written]
		var bump *runtime.SliceFuture[pgq.Written]
		r := p.If(get.Found(),
			func(p *pgq.Pipeline) { bump = p.BumpScoresReturning(1, []int64{1}) },
			func(p *pgq.Pipeline) {
				up = p.UpsertItemsReturning([]pgq.NewItem{{ID: 1, OrgID: 1, Name: "u", Score: 5, Status: "x"}, {ID: 70, OrgID: 2, Name: "v", Score: 6, Status: "y"}})
			})
		return []*runtime.IfResult{r}, func() string { return fmtSlice(up) + "#" + fmtSlice(bump) }
	}},
}

type runMode struct {
	name string
	run  func(ctx context.Context, db *sql.DB, q *pgq.Queries, sc scenario) (ifs []*runtime.IfResult, report func() string, err error)
}

var runModes = []runMode{
	{"PipelineTx", func(ctx context.Context, db *sql.DB, q *pgq.Queries, sc scenario) (ifs []*runtime.IfResult, report func() string, err error) {
		err = q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
			ifs, report = sc.build(p)
			return nil
		})
		return
	}},
	{"standalone Flush", func(ctx context.Context, db *sql.DB, q *pgq.Queries, sc scenario) (ifs []*runtime.IfResult, report func() string, err error) {
		p := q.Pipeline(ctx)
		ifs, report = sc.build(p)
		err = p.Flush()
		return
	}},
	{"user-managed tx", func(ctx context.Context, db *sql.DB, q *pgq.Queries, sc scenario) (ifs []*runtime.IfResult, report func() string, err error) {
		tx, berr := db.BeginTx(ctx, nil)
		if berr != nil {
			return nil, nil, berr
		}
		p := q.WithTx(tx).Pipeline(ctx)
		ifs, report = sc.build(p)
		if err = p.Flush(); err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
		return
	}},
}

func errKind(err error) string {
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, runtime.ErrAssertionFailed):
		// the failing operation's position and label, the wanted and the actual row count are part of
		// the contract on both paths
		var ae *runtime.AssertionError
		if errors.As(err, &ae) {
			return fmt.Sprintf("ErrAssertionFailed(%s op=%d label=%q want=%d got=%d known=%v)", ae.Statement, ae.Op, ae.Label, ae.Want, ae.Got, ae.GotKnown)
		}
		return "ErrAssertionFailed"
	case errors.Is(err, runtime.ErrArgUnresolved):
		k, _ := argErrKind(err)
		return k
	case errors.Is(err, errStop):
		return "errStop"
	case errors.As(err, &pgErr):
		return "sqlstate " + pgErr.Code
	}
	return "other: " + err.Error()
}

// compareWireSeq (the sg_items table must exist: setupItems) runs sc in mode on the sequential fallback (plain pgx) and on the single-round-trip path
// (pgxwire), each from the same fixture rows, and fails unless both observe the same outcome.
func compareWireSeq(t *testing.T, db *sql.DB, dsn string, mode runMode, sc scenario) {
	t.Helper()
	run := func(wire bool) outcome {
		mustExec(t, db, "DELETE FROM sg_items")
		for i := int64(1); i <= 4; i++ {
			mustExec(t, db, fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', %d, 'new')", i, i, i*10))
		}
		wdb := openWire(t, dsn, wire)
		q := pgq.New(wdb)
		defer q.Close()
		ifs, report, err := mode.run(ctxT(t), wdb, q, sc)
		o := outcome{State: snapshot(t, db), Err: errKind(err)}
		for _, r := range ifs {
			if r != nil {
				o.Taken = append(o.Taken, r.Taken())
			}
		}
		if report != nil { // also after a failure: the futures must agree then too
			o.Report = report()
		}
		st := q.CacheStats()
		o.Stats = [2]uint64{st.WirePipelines, st.SequentialPipelines}
		return o
	}
	seq, wire := run(false), run(true)
	t.Logf("sequential: %+v", seq)
	t.Logf("wire:       %+v", wire)
	if seq.Err != wire.Err || seq.State != wire.State || seq.Report != wire.Report || !slices.Equal(seq.Taken, wire.Taken) {
		t.Fatalf("wire pipeline diverges from sequential fallback:\n  sequential %+v\n  wire       %+v", seq, wire)
	}
	// the counters must tell the two paths apart
	if seq.Stats != [2]uint64{0, 1} || wire.Stats != [2]uint64{1, 0} {
		t.Fatalf("CacheStats{Wire,Sequential}: sequential run %v, wire run %v", seq.Stats, wire.Stats)
	}
}

func TestRealEnginePipelineWireMatchesSequential(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("the single-round-trip pipeline SQL is only generated for the $N (PostgreSQL-protocol) dialect")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, mode := range runModes {
			for _, sc := range wireScenarios {
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					compareWireSeq(t, db, dsn, mode, sc)
				})
			}
		}
	})
}
