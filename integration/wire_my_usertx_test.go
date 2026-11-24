package integration

// A pipeline flushed on a transaction the caller began (and PipelineTx with explicit TxOptions) on the
// MySQL / MariaDB single-round-trip path: the transaction stays the caller's, a failed assertion or an
// Abort leaves it usable with the failing operation undone (SAVEPOINT) and the earlier work kept, a database
// error leaves it open exactly like the sequential path (MySQL does not abort a transaction on a statement
// error, unlike PostgreSQL). The fallbacks (a batch above max_allowed_packet, an argument that cannot be
// inlined, an isolation level without a MySQL equivalent) run on the sequential path with identical
// results. Counterpart of wire_usertx_test.go.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/runtime"
)

// myUserTxState renders the fixture plus the two rows the user transaction writes itself.
func myUserTxState(s1, s2, s3, s4 int64) string {
	return fmt.Sprintf("1/1/n1/%d/new;2/1/n2/%d/new;3/1/n3/%d/new;4/1/n4/%d/new;50/5/before/0/x;60/6/later/0/x;", s1, s2, s3, s4)
}

type myUserTxFlow struct {
	name  string
	build func(p *myq.Pipeline)
	check func(error) bool // nil: the flush must succeed
	state string           // the table after the caller's later statement and COMMIT
	// seqState, when set, is the table on the sequential path: there the failing operation has already run
	// when its assertion is checked in Go, so its own effect stays (DOCS.md, "user-managed transaction");
	// on the single-round-trip path it is undone with its savepoint.
	seqState string
}

func myUserTxFlows() []myUserTxFlow {
	stop := errors.New("user tx abort")
	return []myUserTxFlow{
		{"failed assertion: earlier work kept, the failing operation undone, later operations never run", func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1).AssertRows(1)
			p.AddScoreOne(5, 2).AssertRows(2) // affects one row
			p.AddScoreOne(1000, 3)
		}, func(err error) bool {
			var ae *runtime.AssertionError
			return errors.As(err, &ae) && ae.Statement == "AddScoreOne" && ae.Want == 2 && ae.Got == 1 && ae.GotKnown
		}, myUserTxState(11, 20, 30, 40), myUserTxState(11, 25, 30, 40)},
		{"failed assertion on a read", func(p *myq.Pipeline) {
			p.AddScoreOne(5, 1)
			p.GetItem(999).AssertFound()
		}, myIsAssertion, myUserTxState(15, 20, 30, 40), ""},
		{"failed assertion on the first operation", func(p *myq.Pipeline) {
			p.AddScoreOne(5, 999).AssertRows(1)
			p.AddScoreOne(5, 1)
		}, myIsAssertion, myUserTxState(10, 20, 30, 40), ""},
		{"two asserted operations, the second fails", func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1).AssertRows(1)
			p.AddScoreOne(2, 2).AssertRows(1)
			p.DeleteItems([]int64{98}).AssertRows(1)
			p.AddScoreOne(4, 4).AssertRows(1)
		}, myIsAssertion, myUserTxState(11, 22, 30, 40), ""},
		{"Abort in a taken branch keeps the earlier work", func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1)
			g := p.GetItem(1)
			p.If(g.Found(), func(p *myq.Pipeline) { p.Abort(stop) }, nil)
			p.AddScoreOne(1000, 3)
		}, func(err error) bool { return errors.Is(err, stop) }, myUserTxState(11, 20, 30, 40), ""},
		{"duplicate key leaves the transaction open, earlier work kept", func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1)
			p.InsertItem(2, 1, "dup", 0, "x")
			p.AddScoreOne(1000, 3) // never runs
		}, myIsDup, myUserTxState(11, 20, 30, 40), ""},
		{"duplicate key in an asserted operation", func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1)
			p.InsertItem(2, 1, "dup", 0, "x").AssertRows(1)
		}, myIsDup, myUserTxState(11, 20, 30, 40), ""},
		{"the caller's own error 1146 is returned unchanged, never mistaken for a guard", func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1)
			myQueue(p, myMissingDesc).AssertRows(1)
		}, func(err error) bool {
			return myErrNum(err) == 1146 && !errors.Is(err, runtime.ErrAssertionFailed)
		}, myUserTxState(11, 20, 30, 40), ""},
		{"syntax error", func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1).AssertRows(1)
			myQueue(p, myBadSyntaxDesc)
		}, func(err error) bool { return myErrNum(err) == 1064 }, myUserTxState(11, 20, 30, 40), ""},
		{"successful pipeline (control)", func(p *myq.Pipeline) {
			p.AddScoreOne(1, 1).AssertRows(1)
			g := p.GetItem(2)
			p.If(g.Found(), func(p *myq.Pipeline) { p.AddScoreOne(2, 3).AssertRows(1) }, nil)
		}, nil, myUserTxState(11, 20, 32, 40), ""},
	}
}

func TestRealEngineMySQLWireUserTransactionAfterFailure(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, fl := range myUserTxFlows() {
			t.Run(strings.ReplaceAll(fl.name, " ", "_"), func(t *testing.T) {
				for _, wire := range []bool{false, true} {
					myReseed(t, db)
					wdb := openMyWire(t, dsn, wire)
					wdb.SetMaxOpenConns(2)
					q := myq.New(wdb)
					ctx := ctxT(t)
					tx, err := wdb.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := tx.ExecContext(ctx, "INSERT INTO sg_items VALUES (50, 5, 'before', 0, 'x')"); err != nil {
						t.Fatal(err)
					}
					p := q.WithTx(tx).Pipeline(ctx)
					fl.build(p)
					flushErr := p.Flush()
					if fl.check == nil && flushErr != nil || fl.check != nil && (flushErr == nil || !fl.check(flushErr)) {
						_ = tx.Rollback()
						t.Fatalf("wire=%v: flush error = %v", wire, flushErr)
					}
					// the transaction is still the caller's and still usable, and sees its own work
					var before int64
					if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sg_items WHERE id = 50").Scan(&before); err != nil || before != 1 {
						_ = tx.Rollback()
						t.Fatalf("wire=%v: the transaction lost its earlier work (%d, %v)", wire, before, err)
					}
					if _, err := tx.ExecContext(ctx, "INSERT INTO sg_items VALUES (60, 6, 'later', 0, 'x')"); err != nil {
						_ = tx.Rollback()
						t.Fatalf("wire=%v: a statement after the failed flush: %v", wire, err)
					}
					if err := tx.Commit(); err != nil {
						t.Fatalf("wire=%v: commit: %v", wire, err)
					}
					want := fl.state
					if !wire && fl.seqState != "" {
						want = fl.seqState
					}
					if got := mySnap(t, db); got != want {
						t.Fatalf("wire=%v: final state\n got  %s\n want %s", wire, got, want)
					}
					st := q.CacheStats()
					if wire && (st.WirePipelines != 1 || st.SequentialPipelines != 0) || !wire && (st.SequentialPipelines != 1 || st.WirePipelines != 0) {
						t.Fatalf("wire=%v: the wrong path ran: %+v", wire, st)
					}
					if wire {
						myNoPrepared(t, wdb, "user-managed transaction")
					}
					q.Close()
				}
			})
		}
	})
}

// Several pipelines in one user-managed transaction, some failing: every one runs on the wire path, the
// savepoints of one never interfere with the next, and the caller's COMMIT persists exactly the surviving
// work.
func TestRealEngineMySQLWireTwoPipelinesInOneUserTransaction(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		dsn := os.Getenv(e.env)
		var states [2]string
		for wi, wire := range []bool{false, true} {
			myReseed(t, db)
			wdb := openMyWire(t, dsn, wire)
			wdb.SetMaxOpenConns(2)
			q := myq.New(wdb)
			ctx := ctxT(t)
			tx, err := wdb.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			tq := q.WithTx(tx)
			flush := func(build func(p *myq.Pipeline)) error {
				p := tq.Pipeline(ctx)
				build(p)
				return p.Flush()
			}
			if err := flush(func(p *myq.Pipeline) {
				p.AddScoreOne(1, 1).AssertRows(1)
				p.AddScoreOne(1, 2).AssertRows(1)
			}); err != nil {
				t.Fatalf("wire=%v first: %v", wire, err)
			}
			err = flush(func(p *myq.Pipeline) {
				p.AddScoreOne(10, 3).AssertRows(1)
				p.AddScoreOne(10, 999).AssertRows(1) // fails: row 3 keeps its +10 (earlier work), row 999 does not exist
			})
			if !myIsAssertion(err) {
				t.Fatalf("wire=%v second: %v", wire, err)
			}
			if err := flush(func(p *myq.Pipeline) { // the same statement shapes again after a failure
				p.AddScoreOne(100, 4).AssertRows(1)
				p.AddScoreOne(1, 1).AssertRows(1)
			}); err != nil {
				t.Fatalf("wire=%v third: %v", wire, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			states[wi] = mySnap(t, db)
			st := q.CacheStats()
			if wire && (st.WirePipelines != 3 || st.SequentialPipelines != 0) || !wire && st.SequentialPipelines != 3 {
				t.Fatalf("wire=%v: stats %+v", wire, st)
			}
			if wire {
				myNoPrepared(t, wdb, "two pipelines in one transaction")
			}
			q.Close()
		}
		if want := "1/1/n1/12/new;2/1/n2/21/new;3/1/n3/40/new;4/1/n4/140/new;"; states[0] != want || states[1] != want {
			t.Fatalf("final states:\n sequential %s\n wire       %s\n want       %s", states[0], states[1], want)
		}
	})
}

// --- TxOptions --------------------------------------------------------------------------------------

// myObserveIsolation runs a PipelineTx with opts that reads rows 1 and 2, sleeps, and reads them again, and
// from the outside tries to change row 1 (committed) and row 2 (left uncommitted) while it sleeps. What
// the second read sees and whether the writers are blocked tells the isolation level the transaction
// really ran at (the session variables @@transaction_isolation / @@tx_isolation only report the session's
// default, and InnoDB's transaction list is a cache that lags behind).
func myObserveIsolation(t *testing.T, admin *sql.DB, q *myq.Queries, opts *sql.TxOptions) string {
	t.Helper()
	ctx := ctxT(t)
	myReseed(t, admin)
	var first, second *runtime.SliceFuture[myq.Item]
	done := make(chan error, 1)
	go func() {
		done <- q.PipelineTx(ctx, opts, func(p *myq.Pipeline) error {
			first = p.ItemsByIDs([]int64{1, 2})
			myQueue(p, mySleepDesc, 2)
			second = p.ItemsByIDs([]int64{1, 2})
			return nil
		})
	}()
	// wait until the pipeline sleeps (the monitor's own statement mentions the text too: skip it)
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		if err := admin.QueryRow("SELECT COUNT(*) FROM information_schema.processlist WHERE id <> CONNECTION_ID() AND command = 'Query' AND info LIKE '%DO SLEEP(2)%'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the pipeline never started sleeping")
		}
		time.Sleep(10 * time.Millisecond)
	}
	writer := func(autocommit bool, stmt string) (blocked bool, finish func()) {
		c, err := admin.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 1"); err != nil {
			t.Fatal(err)
		}
		finish = func() {
			_, _ = c.ExecContext(ctx, "ROLLBACK")
			_, _ = c.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = DEFAULT")
			_ = c.Close()
		}
		if !autocommit {
			if _, err := c.ExecContext(ctx, "START TRANSACTION"); err != nil {
				t.Fatal(err)
			}
		}
		_, err = c.ExecContext(ctx, stmt)
		if err != nil && myErrNum(err) != 1205 {
			t.Fatal(err)
		}
		return err != nil, finish
	}
	b1, f1 := writer(true, "UPDATE sg_items SET score = score + 100 WHERE id = 1")
	defer f1()
	b2, f2 := writer(false, "UPDATE sg_items SET score = score + 1000 WHERE id = 2")
	defer f2()
	if err := <-done; err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	a, err := first.Get()
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.Get()
	if err != nil {
		t.Fatal(err)
	}
	if b1 || b2 {
		return "writers blocked"
	}
	return fmt.Sprintf("committed change seen=%v, uncommitted change seen=%v", a[0].Score != b[0].Score, a[1].Score != b[1].Score)
}

// TxOptions on PipelineTx: the isolation level and the read-only mode really apply to the transaction the
// batch begins (observed through the behavior of the transaction), they do not leak into the next batch on
// the same connection, and levels MySQL does not have fall back to the sequential path.
func TestRealEngineMySQLWireTxOptions(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		seedItems(t, db)
		ctx := ctxT(t)

		wdb, turns := myOpenCounted(t, dsn, true, nil)
		wdb.SetMaxOpenConns(4)
		q := myq.New(wdb)
		defer q.Close()
		cases := []struct {
			name string
			opts *sql.TxOptions
			want string
		}{
			{"serializable", &sql.TxOptions{Isolation: sql.LevelSerializable}, "writers blocked"},
			{"read uncommitted", &sql.TxOptions{Isolation: sql.LevelReadUncommitted}, "committed change seen=true, uncommitted change seen=true"},
			{"read committed", &sql.TxOptions{Isolation: sql.LevelReadCommitted}, "committed change seen=true, uncommitted change seen=false"},
			{"repeatable read", &sql.TxOptions{Isolation: sql.LevelRepeatableRead}, "committed change seen=false, uncommitted change seen=false"},
			{"default after the others (nothing leaked)", nil, "committed change seen=false, uncommitted change seen=false"},
		}
		for _, c := range cases {
			got := myObserveIsolation(t, db, q, c.opts)
			t.Logf("%s: %-42s -> %s", e.name, c.name, got)
			if got != c.want {
				t.Errorf("%s: observed %q, want %q", c.name, got, c.want)
			}
		}
		if st := q.CacheStats(); st.SequentialPipelines != 0 {
			t.Fatalf("stats %+v", st)
		}
		myReseed(t, db)

		// a read-only transaction: reads work in one turn; options do not stick to the connection
		id := myConnID(t, wdb)
		if cost := measure(t, wdb, turns, func() {
			if err := q.PipelineTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}, func(p *myq.Pipeline) error {
				p.GetItem(1).AssertFound()
				p.ItemsByIDs([]int64{1, 2})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}); cost != 1 {
			t.Fatalf("a read-only serializable PipelineTx cost %d turns, want 1", cost)
		}
		if got := myConnID(t, wdb); got != id {
			t.Fatalf("the connection was replaced (%d -> %d)", id, got)
		}

		// a read-only transaction rejects a write with 1792 on both paths, and the connection stays clean
		for _, wire := range []bool{false, true} {
			myReseed(t, db)
			wdb, _ := myOpenCounted(t, dsn, wire, nil)
			q := myq.New(wdb)
			id := myConnID(t, wdb)
			err := q.PipelineTx(ctx, &sql.TxOptions{ReadOnly: true}, func(p *myq.Pipeline) error {
				p.GetItem(1)
				p.AddScoreOne(1, 1).AssertRows(1)
				return nil
			})
			if myErrNum(err) != 1792 {
				t.Fatalf("wire=%v: write in a read-only transaction: %v", wire, err)
			}
			if got := myConnID(t, wdb); got != id {
				t.Fatalf("wire=%v: the connection was replaced", wire)
			}
			if wire {
				myNoPrepared(t, wdb, "read-only violation")
			}
			// and the connection is not stuck in read-only mode
			if err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { p.AddScoreOne(1, 1).AssertRows(1); return nil }); err != nil {
				t.Fatalf("wire=%v: write after the read-only failure: %v", wire, err)
			}
			if got := mySnap(t, db); got != "1/1/n1/11/new;2/1/n2/20/new;3/1/n3/30/new;4/1/n4/40/new;" {
				t.Fatalf("wire=%v: state %s", wire, got)
			}
			q.Close()
		}

		// an isolation level MySQL does not have is not applicable to the single-round-trip path: it falls
		// back to the sequential path, which reports the plain driver's own error
		var errs [2]error
		for wi, wire := range []bool{false, true} {
			wdb, _ := myOpenCounted(t, dsn, wire, nil)
			q := myq.New(wdb)
			errs[wi] = q.PipelineTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot}, func(p *myq.Pipeline) error { p.AddScoreOne(1, 1); return nil })
			if st := q.CacheStats(); wire && st.WirePipelines != 0 || st.SequentialPipelines > 1 {
				t.Fatalf("wire=%v: stats %+v", wire, st)
			}
			q.Close()
		}
		t.Logf("%s: LevelSnapshot: sequential %v | wire %v", e.name, errs[0], errs[1])
		if errs[0] == nil || errs[1] == nil || errs[0].Error() != errs[1].Error() {
			t.Fatalf("LevelSnapshot errors differ: %v vs %v", errs[0], errs[1])
		}
		if got := mySnap(t, db); got != "1/1/n1/11/new;2/1/n2/20/new;3/1/n3/30/new;4/1/n4/40/new;" {
			t.Fatalf("state %s", got)
		}
	})
}

// --- fallbacks --------------------------------------------------------------------------------------

func myBigPipeline(p *myq.Pipeline, n int) {
	name := strings.Repeat("x", 40)
	for i := 0; i < n; i++ {
		p.InsertItem(int64(1000+i), 2, name, int64(i), "new").AssertRows(1)
	}
}

// A batch above max_allowed_packet is not sent: an oversized packet makes the server drop the connection.
// The pipeline runs on the sequential path instead, with identical results, and the connection survives.
func TestRealEngineMySQLWirePacketLimitFallsBackToSequential(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		seedItems(t, db)
		ctx := ctxT(t)
		// The session variable max_allowed_packet is read-only on current servers, so the limit is the
		// driver's own maxAllowedPacket parameter (the connector uses the smaller of that and the server's).
		small := func(c *mysql.Config) { c.MaxAllowedPacket = 4096 }

		// the premise: the plain driver refuses a statement above the limit (control)
		plain, _ := myOpenCounted(t, dsn, false, small)
		if err := plain.QueryRowContext(ctx, "SELECT '"+strings.Repeat("y", 8000)+"'").Scan(new(string)); !errors.Is(err, mysql.ErrPktTooLarge) {
			t.Fatalf("control: an 8000 byte statement with maxAllowedPacket=4096: %v", err)
		}

		wdb, turns := myOpenCounted(t, dsn, true, small)
		q := myq.New(wdb)
		defer q.Close()
		id := myConnID(t, wdb)

		// under the limit: wire path
		if cost := measure(t, wdb, turns, func() {
			if err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
				p.AddScoreOne(1, 1).AssertRows(1)
				p.AddScoreOne(-1, 1).AssertRows(1)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}); cost != 1 {
			t.Fatalf("a batch under the limit cost %d turns", cost)
		}
		if st := q.CacheStats(); st.WirePipelines != 1 || st.SequentialPipelines != 0 {
			t.Fatalf("stats after a small pipeline: %+v", st)
		}

		// over the limit although every statement is small: sequential path, same result
		const n = 40
		if err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { myBigPipeline(p, n); return nil }); err != nil {
			t.Fatalf("oversized PipelineTx: %v", err)
		}
		if st := q.CacheStats(); st.WirePipelines != 1 || st.SequentialPipelines != 1 {
			t.Fatalf("stats after an oversized pipeline: %+v (want 1 wire, 1 sequential)", st)
		}
		var cnt, sum int64
		if err := db.QueryRow("SELECT COUNT(*), SUM(score) FROM sg_items WHERE id >= 1000").Scan(&cnt, &sum); err != nil || cnt != n || sum != n*(n-1)/2 {
			t.Fatalf("rows from the oversized pipeline: %d rows, sum %d, %v", cnt, sum, err)
		}
		// (the sequential path may open a second connection: its prepared statements live on the pool)
		alive := func(what string) {
			t.Helper()
			var n int
			if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.processlist WHERE id = ?", id).Scan(&n); err != nil || n != 1 {
				t.Fatalf("%s: the connection %d did not survive (%d, %v)", what, id, n, err)
			}
		}
		alive("oversized PipelineTx")
		// the oversized pipeline's assertion still works on the fallback
		err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			myBigPipeline(p, n) // all ids exist now: the first insert is a duplicate key
			return nil
		})
		if !myIsDup(err) {
			t.Fatalf("oversized duplicate: %v", err)
		}
		// standalone Flush and a Flush in a user-managed transaction fall back too
		p := q.Pipeline(ctx)
		for i := 0; i < n; i++ {
			p.AddScoreOne(1, int64(i%4)+1).AssertRows(1)
		}
		if err := p.Flush(); err != nil {
			t.Fatalf("oversized Flush: %v", err)
		}
		tx, err := wdb.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		up := q.WithTx(tx).Pipeline(ctx)
		for i := 0; i < n; i++ {
			up.AddScoreOne(1, int64(i%4)+1).AssertRows(1)
		}
		if err := up.Flush(); err != nil {
			_ = tx.Rollback()
			t.Fatalf("oversized Flush in a transaction: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if st := q.CacheStats(); st.WirePipelines != 1 || st.SequentialPipelines != 4 {
			t.Fatalf("stats: %+v (want 1 wire, 4 sequential)", st)
		}
		var s1 int64
		if err := db.QueryRow("SELECT score FROM sg_items WHERE id = 1").Scan(&s1); err != nil || s1 != 10+10+10 {
			t.Fatalf("score of item 1 = %d, %v (want 30: +10 from each of the two oversized flushes)", s1, err)
		}
		// ... and a small batch uses the wire path again on the very same connection
		if err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { p.AddScoreOne(1, 2).AssertRows(1); return nil }); err != nil {
			t.Fatal(err)
		}
		if st := q.CacheStats(); st.WirePipelines != 2 {
			t.Fatalf("stats: %+v", st)
		}
		alive("the end")
		if err := wdb.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// An argument the batch text cannot carry (a struct) makes the pipeline run on the sequential path, whose
// error is exactly the plain driver's.
func TestRealEngineMySQLWireUnsupportedArgumentFallsBackToSequential(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		seedItems(t, db)
		ctx := ctxT(t)
		for _, mode := range []string{"PipelineTx", "standalone Flush", "user-managed tx"} {
			var errs [2]string
			for wi, wire := range []bool{false, true} {
				myReseed(t, db)
				wdb := openMyWire(t, dsn, wire)
				wdb.SetMaxOpenConns(2)
				q := myq.New(wdb)
				build := func(p *myq.Pipeline) {
					p.AddScoreOne(1, 1).AssertRows(1)
					myQueue(p, myTouchStructArg, struct{ X int }{1}, int64(2))
				}
				var err error
				switch mode {
				case "PipelineTx":
					err = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { build(p); return nil })
				case "standalone Flush":
					p := q.Pipeline(ctx)
					build(p)
					err = p.Flush()
				default:
					var tx *sql.Tx
					if tx, err = wdb.BeginTx(ctx, nil); err != nil {
						t.Fatal(err)
					}
					p := q.WithTx(tx).Pipeline(ctx)
					build(p)
					err = p.Flush()
					_ = tx.Rollback()
				}
				if err == nil {
					t.Fatalf("%s wire=%v: expected the argument conversion error", mode, wire)
				}
				errs[wi] = err.Error()
				st := q.CacheStats()
				if st.WirePipelines != 0 || st.SequentialPipelines != 1 {
					t.Fatalf("%s wire=%v: stats %+v (want the sequential path)", mode, wire, st)
				}
				if got := mySnap(t, db); got != myPristine {
					t.Fatalf("%s wire=%v: state %s", mode, wire, got)
				}
				// the wire pool's connection is fine afterwards
				if err := wdb.PingContext(ctx); err != nil {
					t.Fatal(err)
				}
				if wire {
					myNoPrepared(t, wdb, "unsupported argument")
				}
				q.Close()
			}
			t.Logf("%s: %s: %s", e.name, mode, errs[1])
			if errs[0] != errs[1] {
				t.Fatalf("%s: wire pool error differs from the plain driver's:\n plain %s\n wire  %s", mode, errs[0], errs[1])
			}
		}
	})
}

// The cancellation semantics of a context that is already done are the plain driver's too.
func TestRealEngineMySQLWireAlreadyCancelledContext(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		for _, wire := range []bool{false, true} {
			wdb := openMyWire(t, os.Getenv(e.env), wire)
			wdb.SetMaxOpenConns(2)
			q := myq.New(wdb)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { p.AddScoreOne(1, 1); return nil })
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("wire=%v: err = %v", wire, err)
			}
			p := q.Pipeline(ctx)
			p.AddScoreOne(1, 1)
			if err := p.Flush(); !errors.Is(err, context.Canceled) {
				t.Fatalf("wire=%v: Flush err = %v", wire, err)
			}
			if got := mySnap(t, db); got != myPristine {
				t.Fatalf("wire=%v: state %s", wire, got)
			}
			q.Close()
		}
	})
}
