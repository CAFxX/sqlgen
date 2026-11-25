package integration

// The single-round-trip pipeline path (pgxwire) must be observationally equivalent to the sequential
// fallback: same final table state, same error kind, same IfChain.Taken(i) of every arm and the same future values,
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
	Taken  []bool    // IfChain.Taken(i) of every arm of each If chain queued by the myScenario
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
	build func(p *myq.Pipeline) (ifs []ifRes, report func() string)
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
	// An ordered SELECT inside an If branch, or after an assertion, is gated: its rows must keep the
	// statement's ORDER BY (a derived table around it would not keep it on MariaDB / TiDB).
	{"if chain: the second arm runs", func(p *myq.Pipeline) ([]ifRes, func() string) {
		hit, miss := p.GetItem(1), p.GetItem(999)
		var inner ifRes
		var inArm *runtime.RowFuture[myq.Item]
		r := p.If(miss.Found(), func(p *myq.Pipeline) {
			inArm = p.GetItem(2)
			p.AddScoreOne(100, 1).AssertRows(1)
		}).ElseIf(hit.WhereCol("score >= ?", 10), func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1).AssertRows(1)
			inner = p.If(hit.RowsGt(1), func(p *myq.Pipeline) { p.AddScoreOne(1000, 2) }).
				ElseIf(hit.Found(), func(p *myq.Pipeline) { p.AddScoreOne(2, 2).AssertRows(1) }).
				Else(func(p *myq.Pipeline) { p.Abort(errFuzzAbort) })
		}).ElseIf(hit.Found(), func(p *myq.Pipeline) { // holds too, but an earlier arm ran
			p.AddScoreOne(10000, 3)
		}).Else(func(p *myq.Pipeline) { p.AddScoreOne(20000, 4) })
		after := p.If(inArm.NotFound().Not(), func(p *myq.Pipeline) { p.AddScoreOne(3, 4) }).
			ElseIf(inArm.NotFound(), func(p *myq.Pipeline) { p.AddScoreOne(4, 4) })
		return []ifRes{r, inner, after}, func() string { return myFmtRow(inArm) }
	}},
	{"if chain: no arm runs without an Else, and the Else arm runs", func(p *myq.Pipeline) ([]ifRes, func() string) {
		miss := p.GetItem(999)
		r1 := p.If(miss.Found(), func(p *myq.Pipeline) { p.AddScoreOne(1, 1) }).
			ElseIf(miss.RowsGt(0), func(p *myq.Pipeline) { p.AddScoreOne(2, 1) })
		r2 := p.If(miss.Found(), nil).ElseIf(miss.RowsGe(2), nil).Else(func(p *myq.Pipeline) { p.AddScoreOne(3, 2).AssertRows(1) })
		return []ifRes{r1, r2}, nil
	}},
	{"if chain: an assertion in the running ElseIf arm fails the batch", func(p *myq.Pipeline) ([]ifRes, func() string) {
		hit := p.GetItem(1)
		r := p.If(hit.NotFound(), nil).ElseIf(hit.Found(), func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1).AssertRows(1)
			p.AddScoreOne(1, 999).Label("in elseif").AssertRows(1)
		})
		return []ifRes{r}, nil
	}},
	{"gated SELECT keeps its ORDER BY", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(100, 1)
		g := p.GetItem(1)
		var s *runtime.SliceFuture[int64]
		var tot *runtime.SliceFuture[myq.ParityTotal]
		var n *runtime.RowFuture[int64]
		r := p.If(g.Found(), func(p *myq.Pipeline) {
			s = p.ItemsByScoreDesc(1)
			tot = p.ScoreByParity(0)
		}).Else(func(p *myq.Pipeline) { n = p.CountOrgItems(1) })
		return []ifRes{r}, func() string { return myFmtSlice(s) + " " + myFmtSlice(tot) + " " + myFmtRow(n) }
	}},
	{"SELECT after an assertion keeps its ORDER BY", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(100, 1).AssertRows(1)
		s := p.ItemsByScoreDesc(1)
		tot := p.ScoreByParity(0)
		n := p.CountOrgItems(1)
		return nil, func() string { return myFmtSlice(s) + " " + myFmtSlice(tot) + " " + myFmtRow(n) }
	}},
	{"untaken branch: an aggregate SELECT returns nothing", func(p *myq.Pipeline) ([]ifRes, func() string) {
		g := p.GetItem(999)
		var n *runtime.RowFuture[int64]
		var tot *runtime.SliceFuture[myq.ParityTotal]
		r := p.If(g.Found(), func(p *myq.Pipeline) {
			n = p.CountOrgItems(1)
			tot = p.ScoreByParity(0)
		})
		after := p.If(n.Found().Or(tot.Found()), func(p *myq.Pipeline) { p.AddScoreOne(1000, 2) })
		return []ifRes{r, after}, func() string { return myFmtRow(n) + " " + myFmtSlice(tot) }
	}},
	{"guarded transfer succeeds", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(-5, 1).AssertRows(1)
		p.AddScoreOne(5, 2).AssertRows(1)
		return nil, nil
	}},
	{"failed assertion rolls the whole batch back", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(-5, 1).AssertRows(1)
		p.AddScoreOne(5, 999).Label("line 2").AssertRows(1) // no such row: operation 1, labelled
		return nil, nil
	}},
	// The futures of the operations before a failed assertion stay readable after the failure, and say the
	// same thing on both paths; the operations after it never ran.
	{"futures around a failed assertion", func(p *myq.Pipeline) ([]ifRes, func() string) {
		g := p.GetItem(1)
		u := p.AddScoreOne(5, 2)
		s := p.ItemsByIDs([]int64{1, 2})
		p.AddScoreOne(5, 999).AssertRows(1)
		after := p.GetItem(3)
		return nil, func() string {
			n, uerr := u.RowsAffected()
			return myFmtRow(g) + " " + fmt.Sprintf("%d|%v", n, uerr) + " " + myFmtSlice(s) + " " + myFmtRow(after)
		}
	}},
	{"AssertFound on a row-returning op that finds the row", func(p *myq.Pipeline) ([]ifRes, func() string) {
		g := p.GetItem(1).AssertFound()
		p.AddScoreOne(1, 1).AssertRows(1)
		return nil, func() string { return myFmtRow(g) }
	}},
	{"AssertFound on a row-returning op that finds nothing", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(1, 1).AssertRows(1)
		p.GetItem(999).AssertFound() // must fire even though the op returns zero rows
		return nil, nil
	}},
	{"assertion on a many-op row count", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(1, 1)
		p.ItemsByIDs([]int64{1, 2}).AssertRows(3)
		return nil, nil
	}},
	{"zero-row assertion", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(1, 999).AssertRows(0)
		p.DeleteItems([]int64{998, 999}).AssertRows(0)
		return nil, nil
	}},
	{"if: row found -> then branch", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(1)
		return []ifRes{p.If(get.Found(),
			func(p *myq.Pipeline) { p.AddScoreOne(100, 2).AssertRows(1) }).Else(func(p *myq.Pipeline) { p.AddScoreOne(7, 3).AssertRows(1) })}, nil
	}},
	{"if: row missing -> else branch", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(12345)
		return []ifRes{p.If(get.Found(),
			func(p *myq.Pipeline) { p.AddScoreOne(100, 2).AssertRows(1) }).Else(func(p *myq.Pipeline) { p.AddScoreOne(7, 3).AssertRows(1) })}, nil
	}},
	{"if: unguarded else delete", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(12345)
		return []ifRes{p.If(get.Found(),
			func(p *myq.Pipeline) { p.DeleteItems([]int64{1}) }).Else(func(p *myq.Pipeline) { p.DeleteItems([]int64{4}) })}, nil
	}},
	{"WhereCol predicate true -> then", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(2) // score 20
		return []ifRes{p.If(get.WhereCol("score >= 20"),
			func(p *myq.Pipeline) { p.AddScoreOne(1, 2).AssertRows(1) }).Else(func(p *myq.Pipeline) { p.AddScoreOne(2, 3).AssertRows(1) })}, nil
	}},
	{"WhereCol predicate false -> else", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(2)
		return []ifRes{p.If(get.WhereCol("score >= 21"),
			func(p *myq.Pipeline) { p.AddScoreOne(1, 2).AssertRows(1) }).Else(func(p *myq.Pipeline) { p.AddScoreOne(2, 3).AssertRows(1) })}, nil
	}},
	{"WhereCol bound arguments true -> then", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(2)                                                                                                   // name n2, score 20
		return []ifRes{p.If(get.WhereCol("score >= ? AND name = ? AND name <> ? AND status <> '?'", int64(20), "n2", "o'k?"), // no backslash: TiDB reads its hex literal as a number (sequential fallback)
			func(p *myq.Pipeline) { p.AddScoreOne(1, 2).AssertRows(1) }).Else(func(p *myq.Pipeline) { p.AddScoreOne(2, 3).AssertRows(1) })}, nil
	}},
	{"WhereCol bound arguments false -> else", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(2)
		return []ifRes{p.If(get.WhereCol("score >= ?", int32(21)).Or(get.NotFound()),
			func(p *myq.Pipeline) { p.AddScoreOne(1, 2).AssertRows(1) }).Else(func(p *myq.Pipeline) { p.AddScoreOne(2, 3).AssertRows(1) })}, nil
	}},
	{"gated single-row insert (then) / multi-row insert (else)", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(1)
		return []ifRes{p.If(get.Found(),
			func(p *myq.Pipeline) { p.InsertItem(50, 9, "then-row", 5, "new").AssertRows(1) }).Else(func(p *myq.Pipeline) {
			p.InsertItems([]myq.NewItem{{ID: 60, OrgID: 9, Name: "else-a"}, {ID: 61, OrgID: 9, Name: "else-b"}})
		})}, nil
	}},
	{"gated multi-row insert runs when the else branch is taken", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(777)
		return []ifRes{p.If(get.Found(),
			func(p *myq.Pipeline) { p.InsertItem(50, 9, "then-row", 5, "new").AssertRows(1) }).Else(func(p *myq.Pipeline) {
			p.InsertItems([]myq.NewItem{{ID: 61, OrgID: 9, Name: "else-b", Status: "x"}, {ID: 60, OrgID: 9, Name: "else-a", Status: "x"}})
		})}, nil
	}},
	{"gated UPDATE without WHERE", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(1)
		return []ifRes{p.If(get.Found(),
			func(p *myq.Pipeline) { p.SetAllStatus("then") }).Else(func(p *myq.Pipeline) { p.SetAllStatus("else") })}, nil
	}},
	{"abort in the taken branch rolls everything back", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(1, 1).AssertRows(1)
		get := p.GetItem(1)
		return []ifRes{p.If(get.Found(),
			func(p *myq.Pipeline) { p.Abort(myErrStop) })}, nil
	}},
	{"abort in the branch NOT taken is inert", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(1, 1).AssertRows(1)
		get := p.GetItem(1)
		return []ifRes{p.If(get.Found(), nil).Else(func(p *myq.Pipeline) { p.Abort(myErrStop) })}, nil
	}},
	{"nested if", func(p *myq.Pipeline) ([]ifRes, func() string) {
		outer := p.GetItem(1)
		var inner *runtime.IfChain[*myq.Pipeline]
		o := p.If(outer.Found(), func(p *myq.Pipeline) {
			g2 := p.GetItem(12345)
			inner = p.If(g2.Found(),
				func(p *myq.Pipeline) { p.AddScoreOne(1000, 1) }).Else(func(p *myq.Pipeline) { p.AddScoreOne(5, 4).AssertRows(1) })
		}).Else(func(p *myq.Pipeline) { p.AddScoreOne(2000, 1) })
		return []ifRes{o, inner}, nil
	}},
	{"two conditions on one future", func(p *myq.Pipeline) ([]ifRes, func() string) {
		get := p.GetItem(1)
		i1 := p.If(get.Found(), func(p *myq.Pipeline) { p.AddScoreOne(1, 2) })
		i2 := p.If(get.NotFound(),
			func(p *myq.Pipeline) { p.AddScoreOne(1000, 3) }).Else(func(p *myq.Pipeline) { p.AddScoreOne(7, 4) })
		return []ifRes{i1, i2}, nil
	}},
	{"server error in the middle of the batch (duplicate key)", func(p *myq.Pipeline) ([]ifRes, func() string) {
		p.AddScoreOne(1, 1).AssertRows(1)
		p.InsertItem(2, 1, "dup", 0, "x")
		p.AddScoreOne(1, 3)
		return nil, nil
	}},
	{"reads return the same values", func(p *myq.Pipeline) ([]ifRes, func() string) {
		g := p.GetItem(2)
		missing := p.GetItem(404)
		many := p.ItemsByIDs([]int64{3, 1, 2})
		return nil, func() string { return myFmtRow(g) + "#" + myFmtRow(missing) + "#" + myFmtSlice(many) }
	}},
}

type myRunMode struct {
	name string
	run  func(ctx context.Context, db *sql.DB, q *myq.Queries, sc myScenario) (ifs []ifRes, report func() string, err error)
}

var myRunModes = []myRunMode{
	{"PipelineTx", func(ctx context.Context, db *sql.DB, q *myq.Queries, sc myScenario) (ifs []ifRes, report func() string, err error) {
		err = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			ifs, report = sc.build(p)
			return nil
		})
		return
	}},
	{"standalone Flush", func(ctx context.Context, db *sql.DB, q *myq.Queries, sc myScenario) (ifs []ifRes, report func() string, err error) {
		p := q.Pipeline(ctx)
		ifs, report = sc.build(p)
		err = p.Flush()
		return
	}},
	{"user-managed tx", func(ctx context.Context, db *sql.DB, q *myq.Queries, sc myScenario) (ifs []ifRes, report func() string, err error) {
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
				o.Taken = append(o.Taken, takenArms(r)...)
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
