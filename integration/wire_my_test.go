package integration

// The single-round-trip pipeline path (pgxwire) must be observationally equivalent to the sequential
// fallback: same final table state, same error kind, same IfResult.Taken() and the same future values,
// on every PostgreSQL-protocol engine and for every way of running a pipeline (PipelineTx, a standalone
// Flush, and a Flush inside a user-managed transaction).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/mysqlwire"
	"github.com/CAFxX/sqlgen/runtime"
)

// openMyWire opens dsn through pgxwire (single-round-trip pipelines) or, for the sequential baseline,
// through the plain pgx stdlib driver.
func openMyWire(t testing.TB, dsn string, wire bool) *sql.DB {
	t.Helper()
	var (
		db  *sql.DB
		err error
	)
	if wire {
		db, err = mysqlwire.OpenDB(dsn)
	} else {
		db, err = sql.Open("mysql", dsn)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type myOutcome struct {
	Err    string
	State  string    // every row, id-ordered
	Taken  []bool    // IfResult.Taken() for each If queued by the myScenario
	Report string    // values read from futures after a successful flush
	Stats  [2]uint64 // WirePipelines, SequentialPipelines
}

func mySnapshot(t *testing.T, db *sql.DB) string {
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

// A myScenario queues work and returns every IfResult plus a function that renders the futures' values
// (called only after a successful flush).
type myScenario struct {
	name  string
	build func(p *myq.Pipeline) (ifs []*runtime.IfResult, report func() string)
}

var myErrStop = errors.New("myScenario abort")

func myFmtSlice[T any](f *runtime.SliceFuture[T]) string {
	v, err := f.Get()
	return fmt.Sprintf("%v|%v", v, err)
}

func myFmtRow[T any](f *runtime.RowFuture[T]) string {
	v, err := f.Get()
	return fmt.Sprintf("%v|%v", v, err)
}

var myWireScenarios = []myScenario{
	{"guarded transfer succeeds", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(-5, 1).AssertRowsAffected(1)
		p.AddScoreOne(5, 2).AssertRowsAffected(1)
		return nil, nil
	}},
	{"failed assertion rolls the whole batch back", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(-5, 1).AssertRowsAffected(1)
		p.AddScoreOne(5, 999).AssertRowsAffected(1) // no such row
		return nil, nil
	}},
	{"AssertFound on a row-returning op that finds the row", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		g := p.GetItem(1).AssertFound()
		p.AddScoreOne(1, 1).AssertRowsAffected(1)
		return nil, func() string { return myFmtRow(g) }
	}},
	{"AssertFound on a row-returning op that finds nothing", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1).AssertRowsAffected(1)
		p.GetItem(999).AssertFound() // must fire even though the op returns zero rows
		return nil, nil
	}},
	{"assertion on a many-op row count", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1)
		p.ItemsByIDs([]int64{1, 2}).AssertRowsAffected(3)
		return nil, nil
	}},
	{"zero-row assertion", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 999).AssertRowsAffected(0)
		p.DeleteItems([]int64{998, 999}).AssertRowsAffected(0)
		return nil, nil
	}},
	{"if: row found -> then branch", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *myq.Pipeline) { p.AddScoreOne(100, 2).AssertRowsAffected(1) },
			func(p *myq.Pipeline) { p.AddScoreOne(7, 3).AssertRowsAffected(1) })}, nil
	}},
	{"if: row missing -> else branch", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(12345)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *myq.Pipeline) { p.AddScoreOne(100, 2).AssertRowsAffected(1) },
			func(p *myq.Pipeline) { p.AddScoreOne(7, 3).AssertRowsAffected(1) })}, nil
	}},
	{"if: unguarded else delete", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(12345)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *myq.Pipeline) { p.DeleteItems([]int64{1}) },
			func(p *myq.Pipeline) { p.DeleteItems([]int64{4}) })}, nil
	}},
	{"WhereCol predicate true -> then", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(2) // score 20
		return []*runtime.IfResult{p.If(myq.WhereCol(get, "score >= 20", func(it myq.Item) bool { return it.Score >= 20 }),
			func(p *myq.Pipeline) { p.AddScoreOne(1, 2).AssertRowsAffected(1) },
			func(p *myq.Pipeline) { p.AddScoreOne(2, 3).AssertRowsAffected(1) })}, nil
	}},
	{"WhereCol predicate false -> else", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(2)
		return []*runtime.IfResult{p.If(myq.WhereCol(get, "score >= 21", func(it myq.Item) bool { return it.Score >= 21 }),
			func(p *myq.Pipeline) { p.AddScoreOne(1, 2).AssertRowsAffected(1) },
			func(p *myq.Pipeline) { p.AddScoreOne(2, 3).AssertRowsAffected(1) })}, nil
	}},
	{"gated single-row insert (then) / multi-row insert (else)", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *myq.Pipeline) { p.InsertItem(50, 9, "then-row", 5, "new").AssertRowsAffected(1) },
			func(p *myq.Pipeline) {
				p.InsertItems([]myq.NewItem{{ID: 60, OrgID: 9, Name: "else-a"}, {ID: 61, OrgID: 9, Name: "else-b"}})
			})}, nil
	}},
	{"gated multi-row insert runs when the else branch is taken", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(777)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *myq.Pipeline) { p.InsertItem(50, 9, "then-row", 5, "new").AssertRowsAffected(1) },
			func(p *myq.Pipeline) {
				p.InsertItems([]myq.NewItem{{ID: 61, OrgID: 9, Name: "else-b", Status: "x"}, {ID: 60, OrgID: 9, Name: "else-a", Status: "x"}})
			})}, nil
	}},
	{"gated UPDATE without WHERE", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *myq.Pipeline) { p.SetAllStatus("then") },
			func(p *myq.Pipeline) { p.SetAllStatus("else") })}, nil
	}},
	{"abort in the taken branch rolls everything back", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1).AssertRowsAffected(1)
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(),
			func(p *myq.Pipeline) { p.Abort(myErrStop) }, nil)}, nil
	}},
	{"abort in the branch NOT taken is inert", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1).AssertRowsAffected(1)
		get := p.GetItem(1)
		return []*runtime.IfResult{p.If(get.Found(), nil,
			func(p *myq.Pipeline) { p.Abort(myErrStop) })}, nil
	}},
	{"nested if", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		outer := p.GetItem(1)
		var inner *runtime.IfResult
		o := p.If(outer.Found(), func(p *myq.Pipeline) {
			g2 := p.GetItem(12345)
			inner = p.If(g2.Found(),
				func(p *myq.Pipeline) { p.AddScoreOne(1000, 1) },
				func(p *myq.Pipeline) { p.AddScoreOne(5, 4).AssertRowsAffected(1) })
		}, func(p *myq.Pipeline) { p.AddScoreOne(2000, 1) })
		return []*runtime.IfResult{o, inner}, nil
	}},
	{"two conditions on one future", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		get := p.GetItem(1)
		i1 := p.If(get.Found(), func(p *myq.Pipeline) { p.AddScoreOne(1, 2) }, nil)
		i2 := p.If(get.NotFound(),
			func(p *myq.Pipeline) { p.AddScoreOne(1000, 3) },
			func(p *myq.Pipeline) { p.AddScoreOne(7, 4) })
		return []*runtime.IfResult{i1, i2}, nil
	}},
	{"server error in the middle of the batch (duplicate key)", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1).AssertRowsAffected(1)
		p.InsertItem(2, 1, "dup", 0, "x")
		p.AddScoreOne(1, 3)
		return nil, nil
	}},
	{"reads return the same values", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		g := p.GetItem(2)
		missing := p.GetItem(404)
		many := p.ItemsByIDs([]int64{3, 1, 2})
		return nil, func() string { return myFmtRow(g) + "#" + myFmtRow(missing) + "#" + myFmtSlice(many) }
	}},
}

type myRunMode struct {
	name string
	run  func(ctx context.Context, db *sql.DB, q *myq.Queries, sc myScenario) (ifs []*runtime.IfResult, report func() string, err error)
}

var myRunModes = []myRunMode{
	{"PipelineTx", func(ctx context.Context, db *sql.DB, q *myq.Queries, sc myScenario) (ifs []*runtime.IfResult, report func() string, err error) {
		err = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			ifs, report = sc.build(p)
			return nil
		})
		return
	}},
	{"standalone Flush", func(ctx context.Context, db *sql.DB, q *myq.Queries, sc myScenario) (ifs []*runtime.IfResult, report func() string, err error) {
		p := q.Pipeline(ctx)
		ifs, report = sc.build(p)
		err = p.Flush()
		return
	}},
	{"user-managed tx", func(ctx context.Context, db *sql.DB, q *myq.Queries, sc myScenario) (ifs []*runtime.IfResult, report func() string, err error) {
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

func myErrKind(err error) string {
	var myErr *mysql.MySQLError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, runtime.ErrAssertionFailed):
		// the wanted and the actual row count are part of the contract on both paths
		var ae *runtime.AssertionError
		if errors.As(err, &ae) {
			return fmt.Sprintf("ErrAssertionFailed(%s want=%d got=%d known=%v)", ae.Statement, ae.Want, ae.Got, ae.GotKnown)
		}
		return "ErrAssertionFailed"
	case errors.Is(err, myErrStop):
		return "myErrStop"
	case errors.As(err, &myErr):
		return fmt.Sprintf("mysql %d", myErr.Number)
	}
	return "other: " + err.Error()
}

// compareMyWireSeq (the sg_items table must exist: setupItems) runs sc in mode on the sequential fallback (plain pgx) and on the single-round-trip path
// (pgxwire), each from the same fixture rows, and fails unless both observe the same myOutcome.
func compareMyWireSeq(t *testing.T, db *sql.DB, dsn string, mode myRunMode, sc myScenario) {
	t.Helper()
	run := func(wire bool) myOutcome {
		mustExec(t, db, "DELETE FROM sg_items")
		for i := int64(1); i <= 4; i++ {
			mustExec(t, db, fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', %d, 'new')", i, i, i*10))
		}
		wdb := openMyWire(t, dsn, wire)
		q := myq.New(wdb)
		defer q.Close()
		ifs, report, err := mode.run(ctxT(t), wdb, q, sc)
		o := myOutcome{State: mySnapshot(t, db), Err: myErrKind(err)}
		for _, r := range ifs {
			if r != nil {
				o.Taken = append(o.Taken, r.Taken())
			}
		}
		if err == nil && report != nil {
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

func TestRealEngineMySQLPipelineWireMatchesSequential(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, mode := range myRunModes {
			for _, sc := range myWireScenarios {
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					compareMyWireSeq(t, db, dsn, mode, sc)
				})
			}
		}
	})
}
