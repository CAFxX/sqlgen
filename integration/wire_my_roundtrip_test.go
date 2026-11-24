package integration

// Measures real network turns of the MySQL / MariaDB single-round-trip pipelines (mysqlwire), the
// counterpart of wire_roundtrip_test.go. A "turn" is a Read from the socket that follows a Write. The
// counter wraps the go-sql-driver connection through a registered dial function, so it sees every byte
// the driver exchanges with the server and nothing else.
//
// This file also holds the helpers shared by wire_my_failure_test.go and wire_my_usertx_test.go.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/mysqlwire"
	"github.com/CAFxX/sqlgen/runtime"
)

var myNetSeq atomic.Int64

// myOpenCounted opens dsn with a dialer that counts turns. wire selects mysqlwire (single round-trip
// pipelines) or the plain go-sql-driver (the sequential baseline). mutate may adjust the parsed config
// (session parameters, ...). The pool has two connections so that the single-round-trip path is eligible
// (it needs db.Conn); one stays idle and warm so connection establishment is never measured.
func myOpenCounted(t testing.TB, dsn string, wire bool, mutate func(*mysql.Config)) (db *sql.DB, turns *atomic.Int64) {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	turns = new(atomic.Int64)
	writes := new(atomic.Int64)
	network := cfg.Net
	if network == "" {
		network = "tcp"
	}
	name := fmt.Sprintf("sqlgenturns%d_%d", os.Getpid(), myNetSeq.Add(1))
	mysql.RegisterDialContext(name, func(ctx context.Context, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &turnConn{Conn: c, turns: turns, writes: writes}, nil
	})
	t.Cleanup(func() { mysql.DeregisterDialContext(name) }) // runs after db.Close below (LIFO)
	cfg.Net = name
	if mutate != nil {
		mutate(cfg)
	}
	if wire {
		c, err := mysqlwire.Connector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		db = sql.OpenDB(c)
	} else {
		c, err := mysql.NewConnector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		db = sql.OpenDB(c)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	t.Cleanup(func() { _ = db.Close() })
	return db, turns
}

// myReseed puts the shared fixture back to its four rows (the table must exist: seedItems).
func myReseed(t testing.TB, db *sql.DB) {
	t.Helper()
	mustExec(t, db, "DELETE FROM sg_items")
	for i := 1; i <= 4; i++ {
		mustExec(t, db, fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', %d, 'new')", i, i, i*10))
	}
}

const myPristine = "1/1/n1/10/new;2/1/n2/20/new;3/1/n3/30/new;4/1/n4/40/new;"

// mySnap renders sg_items like mySnapshot, for a testing.TB.
func mySnap(t testing.TB, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query("SELECT id, org_id, name, score, status FROM sg_items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var s string
	for rows.Next() {
		var id, org, sc int64
		var name, status string
		if err := rows.Scan(&id, &org, &name, &sc, &status); err != nil {
			t.Fatal(err)
		}
		s += fmt.Sprintf("%d/%d/%s/%d/%s;", id, org, name, sc, status)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return s
}

func myConnID(t testing.TB, db *sql.DB) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow("SELECT CONNECTION_ID()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// myErrNum returns the server error number of err (0 when err is not a server error).
func myErrNum(err error) uint16 {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

// --- hand-written statement descriptors -------------------------------------------------------------

func myStaticDesc(id uint32, name string, verb runtime.StmtVerb, sql string) *runtime.QueryDescriptor {
	return &runtime.QueryDescriptor{Name: name, StmtID: id, StmtVerb: verb, StaticSQL: sql, Segments: []string{sql}}
}

var (
	// DO SLEEP(?): a long-running operation (an exec: it has no result set, which keeps the batch simple).
	mySleepDesc = &runtime.QueryDescriptor{
		Name: "Sleep", StmtID: 9001, StmtVerb: runtime.VerbOther,
		StaticSQL: "DO SLEEP(?)", Segments: []string{"DO SLEEP(", ")"}, Slots: []runtime.Slot{{Kind: runtime.SlotScalar}},
	}
	// DO SLEEP(5) alone returns 1 and no error when KILL QUERY interrupts it; as part of a larger statement
	// the interruption is error 1317.
	myKillableDesc   = myStaticDesc(9005, "Killable", runtime.VerbUpdate, "UPDATE sg_items SET score = score WHERE id = 2 AND SLEEP(5) = 0")
	myBadSyntaxDesc  = myStaticDesc(9002, "BadSyntax", runtime.VerbUpdate, "UPDATE sg_items SETT score = 1")
	myMissingDesc    = myStaticDesc(9003, "MissingTable", runtime.VerbUpdate, "UPDATE sg_missing_table SET x = 1")
	myTouchStructArg = &runtime.QueryDescriptor{
		Name: "TouchStructArg", StmtID: 9004, StmtVerb: runtime.VerbUpdate,
		StaticSQL: "UPDATE sg_items SET score = score + ? WHERE id = ?",
		Segments:  []string{"UPDATE sg_items SET score = score + ", " WHERE ", "id = ", "", ""},
		Slots: []runtime.Slot{
			{Kind: runtime.SlotScalar}, {Kind: runtime.SlotBranchGuardOpen}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotBranchGuardClose},
		},
	}
)

func myRawArgs(args ...any) *[]any {
	b := runtime.AcquireArgsCap(len(args))
	*b = append(*b, args...)
	return b
}

// myQueue queues a hand-written exec statement on p.
func myQueue(p *myq.Pipeline, desc *runtime.QueryDescriptor, args ...any) *runtime.ExecFuture {
	return p.QueueExec(desc.StmtID, desc, runtime.RenderInput{}, runtime.MakeVariantKey(desc.StmtID, 0, 0, 0), nil, myRawArgs(args...), nil)
}

// myQueueSleep queues DO SLEEP(secs).
func myQueueSleep(p *myq.Pipeline, secs int64) *runtime.ExecFuture {
	return myQueue(p, mySleepDesc, secs)
}

// --- turn counts ------------------------------------------------------------------------------------

// A pipeline workload: n guarded updates, a read, an If/Else and assertions, so that every kind of
// operation the wire path knows is represented.
type myWorkload struct {
	name   string
	seqMin int64 // fewest turns the sequential path can take: BEGIN + one per executed statement + COMMIT
	fn     func(ctx context.Context, q *myq.Queries) error
}

func myMixedN(n int) myWorkload {
	return myWorkload{fmt.Sprintf("%d guarded updates + read + If/Else + assertion", n), int64(n) + 5, func(ctx context.Context, q *myq.Queries) error {
		return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			for i := 0; i < n; i++ {
				p.AddScoreOne(1, int64(i%4)+1).AssertRows(1)
			}
			get := p.GetItem(1).AssertFound()
			p.If(get.Found(),
				func(p *myq.Pipeline) { p.AddScoreOne(-1, 1).AssertRows(1) },
				func(p *myq.Pipeline) { p.AddScoreOne(7, 3).AssertRows(1) })
			p.ItemsByIDs([]int64{1, 2, 3})
			return nil
		})
	}}
}

// The headline claim: a PipelineTx is ONE network round-trip on the single-round-trip path, however many
// operations it carries, versus BEGIN + one turn per statement + COMMIT on the sequential path.
func TestRealEngineMySQLWirePipelineTxIsOneRoundTrip(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		seedItems(t, db)
		ctx := ctxT(t)

		loads := []myWorkload{myMixedN(1), myMixedN(3), myMixedN(8), myMixedN(25)}
		type result struct{ firstSeen, warmed int64 }
		results := map[string][2]result{} // [sequential, wire]
		for wi, wire := range []bool{false, true} {
			// A fresh handle per path: the first run of each workload is "first-seen" (this connection never
			// saw the statement text), the second is "warmed".
			wdb, turns := myOpenCounted(t, dsn, wire, nil)
			q := myq.New(wdb)
			for _, w := range loads {
				var r result
				r.firstSeen = measure(t, wdb, turns, func() {
					if err := w.fn(ctx, q); err != nil {
						t.Fatalf("%s: %v", w.name, err)
					}
				})
				r.warmed = measure(t, wdb, turns, func() {
					if err := w.fn(ctx, q); err != nil {
						t.Fatalf("%s: %v", w.name, err)
					}
				})
				pair := results[w.name]
				pair[wi] = r
				results[w.name] = pair
			}
			st := q.CacheStats()
			if wire && (st.SequentialPipelines != 0 || st.WirePipelines != uint64(2*len(loads))) {
				t.Fatalf("wire stats: %+v", st)
			}
			if !wire && (st.WirePipelines != 0 || st.SequentialPipelines != uint64(2*len(loads))) {
				t.Fatalf("sequential stats: %+v", st)
			}
			q.Close()
		}
		for _, w := range loads {
			r := results[w.name]
			t.Logf("TURNS %-12s %-52s | wire first-seen=%d warmed=%d | sequential first-seen=%d warmed=%d (>= %d)",
				e.name, w.name, r[1].firstSeen, r[1].warmed, r[0].firstSeen, r[0].warmed, w.seqMin)
		}
		for _, w := range loads {
			r := results[w.name]
			if r[1].firstSeen != 1 || r[1].warmed != 1 {
				t.Errorf("%s: a PipelineTx must cost exactly one network turn (first-seen and warmed), got %d / %d", w.name, r[1].firstSeen, r[1].warmed)
			}
			if r[0].warmed < w.seqMin || r[0].firstSeen < w.seqMin {
				t.Errorf("%s: the sequential baseline should cost at least %d turns, got first-seen=%d warmed=%d", w.name, w.seqMin, r[0].firstSeen, r[0].warmed)
			}
		}
	})
}

// A turnCase is one pipeline workload whose cost in network turns is pinned. userTx cases run on a
// transaction the caller began (BEGIN is sent before the measurement window, COMMIT/ROLLBACK after it).
type myTurnCase struct {
	name    string
	userTx  bool
	wire    int64 // exact turns on the single-round-trip path
	seqMin  int64 // fewest turns on the sequential path (0: only reported)
	run     func(ctx context.Context, q *myq.Queries) error
	wantErr func(error) bool // nil: must succeed
}

func myIsAssertion(err error) bool {
	var ae *runtime.AssertionError
	return errors.As(err, &ae) && errors.Is(err, runtime.ErrAssertionFailed)
}

func myIsDup(err error) bool { return myErrNum(err) == 1062 }

var myErrAbortStop = errors.New("turn-count abort")

func myTurnCases() []myTurnCase {
	failAssert := func(p *myq.Pipeline) {
		p.AddScoreOne(1, 1).AssertRows(1)
		p.AddScoreOne(1, 999).AssertRows(1) // no such row
		p.AddScoreOne(1, 2)                 // gated off
	}
	dupKey := func(p *myq.Pipeline) {
		p.AddScoreOne(1, 1).AssertRows(1)
		p.InsertItem(2, 1, "dup", 0, "x") // id 2 exists
		p.AddScoreOne(1, 3)
	}
	abort := func(p *myq.Pipeline) {
		p.AddScoreOne(1, 1)
		g := p.GetItem(1)
		p.If(g.Found(), func(p *myq.Pipeline) { p.Abort(myErrAbortStop) }, nil)
	}
	ok := func(p *myq.Pipeline) {
		get := p.GetItem(1)
		p.If(get.Found(), func(p *myq.Pipeline) { p.AddScoreOne(1, 2).AssertRows(1) }, nil)
	}
	ptx := func(build func(*myq.Pipeline)) func(context.Context, *myq.Queries) error {
		return func(ctx context.Context, q *myq.Queries) error {
			return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { build(p); return nil })
		}
	}
	flush := func(build func(*myq.Pipeline)) func(context.Context, *myq.Queries) error {
		return func(ctx context.Context, q *myq.Queries) error {
			p := q.Pipeline(ctx)
			build(p)
			return p.Flush()
		}
	}
	isStop := func(err error) bool { return errors.Is(err, myErrAbortStop) }
	return []myTurnCase{
		{"PipelineTx", false, 1, 4, ptx(ok), nil},
		{"standalone Flush", false, 1, 4, flush(ok), nil},
		{"PipelineTx, failed assertion", false, 1, 0, ptx(failAssert), myIsAssertion},
		{"PipelineTx, Abort in a taken branch", false, 1, 0, ptx(abort), isStop},
		{"PipelineTx, duplicate key mid-batch", false, 2, 0, ptx(dupKey), myIsDup},
		{"standalone Flush, failed assertion", false, 1, 0, flush(failAssert), myIsAssertion},
		{"standalone Flush, duplicate key mid-batch", false, 2, 0, flush(dupKey), myIsDup},
		{"user-managed tx Flush", true, 1, 2, flush(ok), nil},
		{"user-managed tx Flush, failed assertion", true, 2, 0, flush(failAssert), myIsAssertion},
		{"user-managed tx Flush, Abort in a taken branch", true, 1, 0, flush(abort), isStop},
		{"user-managed tx Flush, duplicate key mid-batch", true, 2, 0, flush(dupKey), myIsDup},
	}
}

// Failure and Flush variants of the pipelines have a fixed price too: a failed assertion or an Abort in a
// transaction of the pipeline's own is still one turn (it is not a server error: the batch rolls itself
// back and says which operation failed); a database error costs the batch plus ONE recovery turn
// (ROLLBACK; SELECT @sqlgen_p). In a transaction the caller began the same holds, except that a failed
// assertion costs one more turn (ROLLBACK TO SAVEPOINT: the operations before it stay).
func TestRealEngineMySQLWireTurnCountsOfFlushAndFailurePaths(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		seedItems(t, db)
		ctx := ctxT(t)

		cost := map[string][2]int64{} // name -> [sequential, wire]
		for wi, wire := range []bool{false, true} {
			wdb, turns := myOpenCounted(t, dsn, wire, nil)
			q := myq.New(wdb)
			for _, c := range myTurnCases() {
				// run each case twice: the second run is "warmed" and must cost the same
				for round := 0; round < 2; round++ {
					myReseed(t, db)
					var tx *sql.Tx
					qq := q
					if c.userTx {
						var err error
						if tx, err = wdb.BeginTx(ctx, nil); err != nil { // BEGIN is outside the measurement window
							t.Fatal(err)
						}
						qq = q.WithTx(tx)
					}
					var runErr error
					n := measure(t, wdb, turns, func() { runErr = c.run(ctx, qq) })
					if tx != nil {
						_ = tx.Rollback()
					}
					if c.wantErr == nil && runErr != nil || c.wantErr != nil && (runErr == nil || !c.wantErr(runErr)) {
						t.Fatalf("%s (wire=%v): err = %v", c.name, wire, runErr)
					}
					if wire {
						if n != c.wire {
							t.Errorf("%s: wire path cost %d turn(s) (round %d), want exactly %d", c.name, n, round, c.wire)
						}
					} else if c.seqMin > 0 && n < c.seqMin {
						t.Errorf("%s: sequential path cost %d turn(s), want at least %d", c.name, n, c.seqMin)
					}
					r := cost[c.name]
					r[wi] = n
					cost[c.name] = r
				}
			}
			st := q.CacheStats()
			if wire && st.SequentialPipelines != 0 {
				t.Errorf("a pipeline fell back to the sequential path: %+v", st)
			}
			if !wire && st.WirePipelines != 0 {
				t.Errorf("sequential baseline used the wire path: %+v", st)
			}
			q.Close()
		}
		for _, c := range myTurnCases() {
			r := cost[c.name]
			t.Logf("TURNS %-12s %-52s | wire=%d | sequential=%d", e.name, c.name, r[1], r[0])
		}
	})
}

// A COMMIT of the caller's own transaction after the pipeline is a turn of its own (it is not part of the
// pipeline), and a connection that served a failed pipeline serves the next one in one turn again.
func TestRealEngineMySQLWireConnectionStaysOneTurnAfterFailures(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, turns := myOpenCounted(t, os.Getenv(e.env), true, nil)
		q := myq.New(wdb)
		defer q.Close()
		id := myConnID(t, wdb)
		good := myMixedN(3)
		for i, c := range myTurnCases() {
			if c.userTx {
				continue
			}
			_ = c.run(ctx, q)
			if n := measure(t, wdb, turns, func() {
				if err := good.fn(ctx, q); err != nil {
					t.Fatalf("after %q: %v", c.name, err)
				}
			}); n != 1 {
				t.Fatalf("after case %d (%s) the next pipeline cost %d turns, want 1", i, c.name, n)
			}
			if got := myConnID(t, wdb); got != id {
				t.Fatalf("after %q the connection was replaced (%d -> %d)", c.name, id, got)
			}
		}
		tx, err := wdb.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		p := q.WithTx(tx).Pipeline(ctx)
		p.AddScoreOne(1, 1).AssertRows(1)
		if err := p.Flush(); err != nil {
			t.Fatal(err)
		}
		commit := measure(t, wdb, turns, func() {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("TURNS %-12s the caller's own COMMIT after a pipeline in a user-managed tx = %d turn(s)", e.name, commit)
		if commit != 1 {
			t.Fatalf("COMMIT cost %d turns", commit)
		}
	})
}
