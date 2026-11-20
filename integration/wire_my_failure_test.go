package integration

// Robustness of the MySQL / MariaDB single-round-trip pipelines (mysqlwire): every way a pipeline can
// fail must leave the very same connection usable (no discarded connection, no transaction and no
// server-side prepared statement left behind, no session-variable state that changes the next batch), and
// the pool must stay whole under concurrency, deadlocks, lock-wait timeouts, cancellation and a killed
// connection. Counterpart of wire_failure_test.go.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/runtime"
)

// myKill kills (the statement of, with query) a connection this test created; an id that is already gone
// is fine.
func myKill(t testing.TB, admin *sql.DB, id int64, query bool) {
	t.Helper()
	stmt := "KILL "
	if query {
		stmt += "QUERY "
	}
	if _, err := admin.Exec(fmt.Sprintf("%s%d", stmt, id)); err != nil && myErrNum(err) != 1094 {
		t.Fatalf("%s%d: %v", stmt, id, err)
	}
}

// myWaitRunning waits until connection id is executing a statement whose text contains needle.
func myWaitRunning(t testing.TB, admin *sql.DB, id int64, needle string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		if err := admin.QueryRow("SELECT COUNT(*) FROM information_schema.processlist WHERE id = ? AND command = 'Query' AND info LIKE ?", id, "%"+needle+"%").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection %d never started running %q", id, needle)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// myNoPrepared asserts that the connection db (a pool with exactly one connection) has no server-side
// prepared statement named sqlgen_s: DEALLOCATE of an absent statement is error 1243.
func myNoPrepared(t testing.TB, db *sql.DB, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, "DEALLOCATE PREPARE sqlgen_s")
	if myErrNum(err) != 1243 {
		t.Fatalf("%s: a server-side prepared statement was left behind on the connection (DEALLOCATE PREPARE: %v)", what, err)
	}
}

func myPreparedCount(t testing.TB, db *sql.DB) int64 {
	t.Helper()
	var name string
	var n int64
	if err := db.QueryRow("SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'").Scan(&name, &n); err != nil {
		t.Fatal(err)
	}
	return n
}

var myRetryPolicy = runtime.TxRetryPolicy{MaxRetries: 200, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond}

// Every way a single-round-trip transaction can fail must roll back and leave the very same connection
// usable and clean.
func TestRealEngineMySQLWireFailurePathsLeaveTheConnectionClean(t *testing.T) {
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
		if wdb.Stats().OpenConnections != 1 {
			t.Fatalf("expected a single connection, pool: %+v", wdb.Stats())
		}
		baseline := myPreparedCount(t, db)

		stop := errors.New("stop")
		userTx := func(build func(p *myq.Pipeline)) func() error {
			return func() error {
				tx, err := wdb.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				p := q.WithTx(tx).Pipeline(ctx)
				build(p)
				return p.Flush()
			}
		}
		failures := []struct {
			name  string
			run   func() error
			check func(error) bool
		}{
			{"failed assertion", func() error {
				return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
					p.AddScoreOne(1, 1).AssertRowsAffected(1)
					p.AddScoreOne(1, 999).AssertRowsAffected(1)
					return nil
				})
			}, myIsAssertion},
			{"Abort in a taken branch", func() error {
				return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
					p.AddScoreOne(1, 1)
					g := p.GetItem(1)
					p.If(g.Found(), func(p *myq.Pipeline) { p.Abort(stop) }, nil)
					return nil
				})
			}, func(err error) bool { return errors.Is(err, stop) }},
			{"duplicate key in the middle of the batch", func() error {
				return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
					p.AddScoreOne(1, 1)
					p.InsertItem(1, 1, "dup", 0, "x")
					p.AddScoreOne(1, 2)
					return nil
				})
			}, myIsDup},
			{"syntax error in a hand-written statement mid-batch", func() error {
				return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
					p.AddScoreOne(1, 1).AssertRowsAffected(1)
					myQueue(p, myBadSyntaxDesc)
					p.AddScoreOne(1, 2)
					return nil
				})
			}, func(err error) bool { return myErrNum(err) == 1064 }},
			{"missing table in a hand-written statement", func() error {
				return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
					p.AddScoreOne(1, 1)
					myQueue(p, myMissingDesc).AssertRowsAffected(1)
					return nil
				})
			}, func(err error) bool { return myErrNum(err) == 1146 }},
			{"error in a standalone Flush", func() error {
				p := q.Pipeline(ctx)
				p.AddScoreOne(1, 1)
				p.InsertItem(1, 1, "dup", 0, "x")
				return p.Flush()
			}, myIsDup},
			{"failed assertion in a standalone Flush", func() error {
				p := q.Pipeline(ctx)
				p.AddScoreOne(1, 1)
				p.GetItem(999).AssertFound()
				return p.Flush()
			}, myIsAssertion},
			{"error in a user-managed transaction", userTx(func(p *myq.Pipeline) {
				p.AddScoreOne(1, 1)
				p.InsertItem(1, 1, "dup", 0, "x")
			}), myIsDup},
			{"failed assertion in a user-managed transaction", userTx(func(p *myq.Pipeline) {
				p.AddScoreOne(1, 1).AssertRowsAffected(1)
				p.AddScoreOne(1, 999).AssertRowsAffected(1)
			}), myIsAssertion},
			{"ErrWireNotApplicable fallback (argument that cannot be inlined)", func() error {
				return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
					p.AddScoreOne(1, 1)
					myQueue(p, myTouchStructArg, struct{ X int }{1}, int64(1))
					return nil
				})
			}, func(err error) bool {
				return err != nil && myErrNum(err) == 0 && !errors.Is(err, runtime.ErrAssertionFailed)
			}},
		}
		for _, f := range failures {
			err := f.run()
			if err == nil || !f.check(err) {
				t.Fatalf("%s: err = %v", f.name, err)
			}
			t.Logf("%s: %s -> %v", e.name, f.name, err)
			if st := wdb.Stats(); st.InUse != 0 || st.OpenConnections != 1 {
				t.Fatalf("%s: pool state after the failure: %+v", f.name, st)
			}
			if got := myConnID(t, wdb); got != id {
				t.Fatalf("%s: the connection was replaced (%d -> %d)", f.name, id, got)
			}
			if got := mySnap(t, db); got != myPristine {
				t.Fatalf("%s: the failed transaction left its changes behind: %s", f.name, got)
			}
			myNoPrepared(t, wdb, f.name)
			if err := wdb.PingContext(ctx); err != nil {
				t.Fatalf("%s: ping: %v", f.name, err)
			}
			var inTx int64
			if err := wdb.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id = ?", id).Scan(&inTx); err != nil || inTx != 0 {
				t.Fatalf("%s: the connection still has an open transaction (%d, %v)", f.name, inTx, err)
			}
			// ... and the next pipeline is a normal one-turn transaction again
			cost := measure(t, wdb, turns, func() {
				if err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
					p.AddScoreOne(1, 1).AssertRowsAffected(1)
					p.AddScoreOne(-1, 1).AssertRowsAffected(1)
					return nil
				}); err != nil {
					t.Fatalf("%s: pipeline after the failure: %v", f.name, err)
				}
			})
			if cost != 1 {
				t.Fatalf("%s: the pipeline after a failure cost %d turns", f.name, cost)
			}
			myNoPrepared(t, wdb, f.name+" (follow-up pipeline)")
		}

		// many failures in a row must not leak connections or statements
		for i := 0; i < 100; i++ {
			err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { p.AddScoreOne(1, 999).AssertFound(); return nil })
			if !errors.Is(err, runtime.ErrAssertionFailed) {
				t.Fatalf("iteration %d: %v", i, err)
			}
		}
		if st := wdb.Stats(); st.InUse != 0 || st.OpenConnections != 1 || st.MaxIdleClosed != 0 {
			t.Fatalf("pool after 100 failures: %+v", st)
		}
		myNoPrepared(t, wdb, "after 100 failures")
		// the server-wide counter is only reported: another client may be using the server
		t.Logf("%s: Prepared_stmt_count baseline %d, now %d", e.name, baseline, myPreparedCount(t, db))
		if st := q.CacheStats(); st.WirePipelines == 0 {
			t.Fatalf("stats = %+v", st)
		}
	})
}

// A pipeline that has been cancelled or has run out of time while the batch is in flight returns the
// context's error (never a CommitError, even though the outcome is unknown), the connection is discarded
// and the pool recovers; nothing of the cancelled batch is committed.
func TestRealEngineMySQLWireCancellationAndDeadline(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, _ := myOpenCounted(t, os.Getenv(e.env), true, nil)
		q := myq.New(wdb)
		defer q.Close()

		recovered := func(what string, oldID int64) {
			t.Helper()
			if oldID != 0 {
				myKill(t, db, oldID, false) // our own, already abandoned connection: free its row locks now
			}
			if err := wdb.PingContext(ctx); err != nil {
				t.Fatalf("%s: ping: %v", what, err)
			}
			if got := mySnap(t, db); got != myPristine {
				t.Fatalf("%s: state = %s", what, got)
			}
			if err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
				p.AddScoreOne(1, 1).AssertRowsAffected(1)
				p.AddScoreOne(-1, 1).AssertRowsAffected(1)
				return nil
			}); err != nil {
				t.Fatalf("%s: pipeline afterwards: %v", what, err)
			}
			if st := wdb.Stats(); st.InUse != 0 {
				t.Fatalf("%s: pool %+v", what, st)
			}
		}

		// cancelled while the batch runs
		id := myConnID(t, wdb)
		cctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- q.PipelineTx(cctx, nil, func(p *myq.Pipeline) error {
				p.AddScoreOne(1, 1).AssertRowsAffected(1)
				myQueueSleep(p, 2)
				return nil
			})
		}()
		myWaitRunning(t, db, id, "SLEEP")
		cancel()
		select {
		case err := <-done:
			var ce *runtime.CommitError
			if !errors.Is(err, context.Canceled) || errors.As(err, &ce) {
				t.Fatalf("cancelled pipeline: err = %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the cancelled pipeline did not return")
		}
		recovered("cancel", id)

		// deadline exceeded while the batch runs
		id = myConnID(t, wdb)
		dctx, dcancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer dcancel()
		start := time.Now()
		err := q.PipelineTx(dctx, nil, func(p *myq.Pipeline) error {
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			myQueueSleep(p, 3)
			return nil
		})
		var ce *runtime.CommitError
		if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ce) {
			t.Fatalf("deadline: err = %v", err)
		}
		if d := time.Since(start); d > 20*time.Second {
			t.Fatalf("the pipeline took %v to notice its deadline", d)
		}
		recovered("deadline", id)

		// already cancelled: nothing is sent, the connection is untouched
		id = myConnID(t, wdb)
		dead, dcancel2 := context.WithCancel(ctx)
		dcancel2()
		err = q.PipelineTx(dead, nil, func(p *myq.Pipeline) error { p.AddScoreOne(1, 1); return nil })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("already-cancelled context: err = %v", err)
		}
		if got := myConnID(t, wdb); got != id {
			t.Fatalf("an already-cancelled pipeline replaced the connection (%d -> %d): nothing should have been sent", id, got)
		}
		recovered("already cancelled", 0)
		if st := q.CacheStats(); st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v", st)
		}
	})
}

// KILL QUERY interrupts one statement of the batch with a server error (1317); that is a database error
// like any other: the batch is rolled back and the connection, which is alive, is reused.
func TestRealEngineMySQLWireKilledStatementLeavesTheConnectionClean(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, _ := myOpenCounted(t, os.Getenv(e.env), true, nil)
		q := myq.New(wdb)
		defer q.Close()
		id := myConnID(t, wdb)
		done := make(chan error, 1)
		go func() {
			done <- q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
				p.AddScoreOne(1, 1).AssertRowsAffected(1)
				myQueue(p, myKillableDesc) // SLEEP inside a larger statement: its interruption is an error
				p.AddScoreOne(1, 2)
				return nil
			})
		}()
		myWaitRunning(t, db, id, "SLEEP")
		myKill(t, db, id, true)
		if got := mySnap(t, db); got != myPristine {
			t.Fatalf("state during the kill = %s", got)
		}
		var err error
		select {
		case err = <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("the killed pipeline did not return")
		}
		t.Logf("%s: KILL QUERY mid-batch -> %T %v", e.name, err, err)
		if err == nil {
			t.Fatal("expected an error")
		}
		var ce *runtime.CommitError
		if errors.As(err, &ce) {
			t.Fatalf("a killed statement is not a commit failure: %v", err)
		}
		if got := mySnap(t, db); got != myPristine {
			t.Fatalf("state = %s", got)
		}
		if got := myConnID(t, wdb); got != id {
			t.Logf("%s: connection replaced after KILL QUERY (%d -> %d)", e.name, id, got)
		} else {
			myNoPrepared(t, wdb, "KILL QUERY")
		}
		if err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { p.AddScoreOne(1, 1).AssertRowsAffected(1); return nil }); err != nil {
			t.Fatalf("pipeline afterwards: %v", err)
		}
	})
}

// A connection that dies while the batch runs: the outcome of the transaction is unknown. The driver-level
// error IS a *runtime.CommitError (the batch carried the COMMIT), which tells the retry loop not to run the
// transaction again; the caller of PipelineTx sees the plain transport error, and WithTxRetry does not retry.
func TestRealEngineMySQLWireKilledConnection(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, _ := myOpenCounted(t, os.Getenv(e.env), true, nil)
		q := myq.New(wdb)
		defer q.Close()

		// through PipelineTx, with retries enabled
		id := myConnID(t, wdb)
		var attempts atomic.Int64
		done := make(chan error, 1)
		go func() {
			done <- q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
				attempts.Add(1)
				p.AddScoreOne(1, 1).AssertRowsAffected(1)
				myQueueSleep(p, 3)
				return nil
			}, runtime.WithTxRetry(runtime.TxRetryPolicy{MaxRetries: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}))
		}()
		myWaitRunning(t, db, id, "SLEEP")
		myKill(t, db, id, false)
		var err error
		select {
		case err = <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("the pipeline on the killed connection did not return")
		}
		t.Logf("%s: KILL CONNECTION mid-batch -> %T %v", e.name, err, err)
		var ce *runtime.CommitError
		if err == nil || errors.As(err, &ce) {
			t.Fatalf("PipelineTx on a killed connection: err = %v (a *CommitError must not reach the caller)", err)
		}
		if attempts.Load() != 1 {
			t.Fatalf("the transaction was run %d times; a failure with unknown outcome must not be retried", attempts.Load())
		}
		if got := mySnap(t, db); got != myPristine {
			t.Fatalf("state = %s", got)
		}
		if err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { p.AddScoreOne(1, 1).AssertRowsAffected(1); return nil }); err != nil {
			t.Fatalf("pipeline afterwards (the pool must have discarded the dead connection): %v", err)
		}

		// at driver level
		conn, err := wdb.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var cid int64
		if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&cid); err != nil {
			t.Fatal(err)
		}
		go func() {
			myWaitRunning(t, db, cid, "SLEEP")
			myKill(t, db, cid, false)
		}()
		var rawErr error
		if err := conn.Raw(func(dc any) error {
			wtp, ok := dc.(runtime.WireTxPipeliner)
			if !ok {
				return fmt.Errorf("%T is not a WireTxPipeliner", dc)
			}
			ops := []runtime.PipelineOp{
				{Desc: myTouchStructArg, Args: []any{int64(1), int64(1)}, Decorators: runtime.OpDecorators{AssertRows: -1}},
				{Desc: mySleepDesc, Args: []any{int64(3)}, Decorators: runtime.OpDecorators{AssertRows: -1}},
			}
			rawErr = wtp.ExecTxPipeline(ctx, nil, ops)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		t.Logf("%s: driver-level error after KILL CONNECTION: %T %v", e.name, rawErr, rawErr)
		if !errors.As(rawErr, &ce) {
			t.Fatalf("driver-level error %T %v is not a *runtime.CommitError", rawErr, rawErr)
		}
		if got := mySnap(t, db); got != "1/1/n1/11/new;2/1/n2/20/new;3/1/n3/30/new;4/1/n4/40/new;" {
			t.Fatalf("state = %s (only the successful pipeline afterwards may have committed)", got)
		}
	})
}

// Two pipelines locking the same two rows in opposite order deadlock; the server's error 1213 passes
// through the wire path unchanged and is classified as retryable, and WithTxRetry runs the victim again to
// success.
func TestRealEngineMySQLWireDeadlockIsRetried(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, _ := myOpenCounted(t, os.Getenv(e.env), true, nil)
		wdb.SetMaxOpenConns(4)
		wdb.SetMaxIdleConns(4)
		q := myq.New(wdb)
		defer q.Close()
		// warm up two connections so that both pipelines start together
		c1, err := wdb.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c2, err := wdb.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_ = c1.Close()
		_ = c2.Close()

		var attempts atomic.Int64
		pair := func(opts ...runtime.TxOption) (errs [2]error) {
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i, rows := range [2][2]int64{{1, 2}, {2, 1}} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errs[i] = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
						attempts.Add(1)
						p.AddScoreOne(1, rows[0]).AssertRowsAffected(1)
						myQueueSleep(p, 1) // both hold their first row before asking for the second
						p.AddScoreOne(1, rows[1]).AssertRowsAffected(1)
						return nil
					}, opts...)
				}()
			}
			close(start)
			wg.Wait()
			return
		}

		// without retries exactly one of them is the victim
		errs := pair()
		var victim error
		nerr := 0
		for _, err := range errs {
			if err != nil {
				nerr++
				victim = err
			}
		}
		if nerr != 1 {
			t.Fatalf("expected exactly one deadlock victim, got errors %v", errs)
		}
		var ce *runtime.CommitError
		t.Logf("%s: deadlock victim error: %T %v", e.name, victim, victim)
		if myErrNum(victim) != 1213 || errors.As(victim, &ce) || !runtime.IsRetryableTxError(victim, false) {
			t.Fatalf("victim error %v: number %d, retryable=%v", victim, myErrNum(victim), runtime.IsRetryableTxError(victim, false))
		}
		if got := mySnap(t, db); got != "1/1/n1/11/new;2/1/n2/21/new;3/1/n3/30/new;4/1/n4/40/new;" {
			t.Fatalf("state after the deadlock = %s (only the survivor may have committed)", got)
		}

		// with retries both succeed
		myReseed(t, db)
		attempts.Store(0)
		errs = pair(runtime.WithTxRetry(runtime.TxRetryPolicy{MaxRetries: 10, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}))
		if errs[0] != nil || errs[1] != nil {
			t.Fatalf("with WithTxRetry: %v", errs)
		}
		t.Logf("%s: %d attempts for 2 transactions", e.name, attempts.Load())
		if attempts.Load() < 3 {
			t.Fatalf("attempts = %d, the deadlock victim must have been retried", attempts.Load())
		}
		if got := mySnap(t, db); got != "1/1/n1/12/new;2/1/n2/22/new;3/1/n3/30/new;4/1/n4/40/new;" {
			t.Fatalf("state = %s", got)
		}
		if st := q.CacheStats(); st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v", st)
		}
		if s := wdb.Stats(); s.InUse != 0 {
			t.Fatalf("pool = %+v", s)
		}
	})
}

// A lock wait timeout (error 1205, innodb_lock_wait_timeout=1 on a dedicated pool) is retryable too.
func TestRealEngineMySQLWireLockWaitTimeoutIsRetried(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, _ := myOpenCounted(t, os.Getenv(e.env), true, func(c *mysql.Config) {
			if c.Params == nil {
				c.Params = map[string]string{}
			}
			c.Params["innodb_lock_wait_timeout"] = "1"
		})
		q := myq.New(wdb)
		defer q.Close()
		var got int64
		if err := wdb.QueryRow("SELECT @@innodb_lock_wait_timeout").Scan(&got); err != nil || got != 1 {
			t.Fatalf("session innodb_lock_wait_timeout = %d, %v", got, err)
		}

		holder, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Rollback()
		if _, err := holder.ExecContext(ctx, "UPDATE sg_items SET score = score + 100 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}
		id := myConnID(t, wdb)

		err = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			p.AddScoreOne(1, 2).AssertRowsAffected(1)
			p.AddScoreOne(1, 1).AssertRowsAffected(1) // blocked by the holder
			return nil
		})
		var ce *runtime.CommitError
		t.Logf("%s: lock wait timeout error: %T %v", e.name, err, err)
		if myErrNum(err) != 1205 || errors.As(err, &ce) || !runtime.IsRetryableTxError(err, false) {
			t.Fatalf("err = %v (number %d)", err, myErrNum(err))
		}
		if got := myConnID(t, wdb); got != id {
			t.Fatalf("a lock wait timeout replaced the connection (%d -> %d)", id, got)
		}
		myNoPrepared(t, wdb, "lock wait timeout")
		if got := mySnap(t, db); got != myPristine {
			t.Fatalf("state = %s (the whole batch must have rolled back; the holder's change is uncommitted)", got)
		}

		// with retries the pipeline succeeds once the holder is gone
		var attempts atomic.Int64
		err = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			if attempts.Add(1) == 2 {
				if err := holder.Commit(); err != nil {
					t.Error(err)
				}
			}
			p.AddScoreOne(1, 2).AssertRowsAffected(1)
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			return nil
		}, runtime.WithTxRetry(runtime.TxRetryPolicy{MaxRetries: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}))
		if err != nil || attempts.Load() != 2 {
			t.Fatalf("err = %v after %d attempts, want success on the 2nd", err, attempts.Load())
		}
		if got := mySnap(t, db); got != "1/1/n1/111/new;2/1/n2/21/new;3/1/n3/30/new;4/1/n4/40/new;" {
			t.Fatalf("state = %s", got)
		}
	})
}

// --- session state must not leak between batches ----------------------------------------------------

type myScenarioOutcome struct {
	Err    string
	State  string
	Taken  []bool
	Report string
}

// myRunScenario resets the fixture, runs sc in mode through q and observes the outcome.
func myRunScenario(t *testing.T, admin *sql.DB, wdb *sql.DB, q *myq.Queries, mode myRunMode, sc myScenario) myScenarioOutcome {
	t.Helper()
	myReseed(t, admin)
	ifs, report, err := mode.run(ctxT(t), wdb, q, sc)
	o := myScenarioOutcome{Err: myErrKind(err), State: mySnap(t, admin)}
	for _, r := range ifs {
		if r != nil {
			o.Taken = append(o.Taken, r.Taken())
		}
	}
	if err == nil && report != nil {
		o.Report = report()
	}
	return o
}

// A batch leaves @sqlgen_* user variables behind on the connection (a failed assertion leaves
// @sqlgen_f >= 0 and counts and conditions of many operations). The next batch on that connection must
// behave exactly as on a fresh connection, for every scenario and run mode.
func TestRealEngineMySQLWireSessionVariablesDoNotLeakBetweenBatches(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		dsn := os.Getenv(e.env)

		// poisons: batches that leave as much state as possible on the connection
		poisons := []func(p *myq.Pipeline){
			func(p *myq.Pipeline) { // a failed assertion in the middle of many operations
				for i := 0; i < 12; i++ {
					p.AddScoreOne(1, int64(i%4)+1).AssertRowsAffected(1)
				}
				g := p.GetItem(1)
				p.If(g.Found(), func(p *myq.Pipeline) { p.AddScoreOne(1, 2) }, func(p *myq.Pipeline) { p.AddScoreOne(1, 3) })
				p.AddScoreOne(1, 999).AssertRowsAffected(1)
				p.AddScoreOne(1, 4)
			},
			func(p *myq.Pipeline) { // an Abort in a taken branch
				g := p.GetItem(2)
				p.If(g.Found(), func(p *myq.Pipeline) { p.AddScoreOne(1, 1); p.Abort(myErrStop) }, nil)
			},
			func(p *myq.Pipeline) { // a duplicate key (DB error: @sqlgen_p points into the batch)
				p.AddScoreOne(1, 1)
				p.InsertItem(1, 1, "dup", 0, "x")
			},
		}
		fresh := openMyWire(t, dsn, true)
		fq := myq.New(fresh)
		defer fq.Close()
		dirty := openMyWire(t, dsn, true)
		dirty.SetMaxOpenConns(2)
		dq := myq.New(dirty)
		defer dq.Close()
		id := myConnID(t, dirty)

		for pi, poison := range poisons {
			for _, mode := range myRunModes {
				for _, sc := range myWireScenarios {
					name := fmt.Sprintf("poison%d/%s/%s", pi, mode.name, sc.name)
					// a brand new connection for the reference
					freshO := func() myScenarioOutcome {
						fresh.SetMaxIdleConns(0) // every run gets a new connection
						defer fresh.SetMaxIdleConns(2)
						return myRunScenario(t, db, fresh, fq, mode, sc)
					}()
					// poison the long-lived connection, then run the scenario on it
					_ = dq.PipelineTx(ctxT(t), nil, func(p *myq.Pipeline) error { poison(p); return nil })
					dirtyO := myRunScenario(t, db, dirty, dq, mode, sc)
					if freshO.Err != dirtyO.Err || freshO.State != dirtyO.State || freshO.Report != dirtyO.Report || !slices.Equal(freshO.Taken, dirtyO.Taken) {
						t.Fatalf("%s: a batch behaves differently after another batch ran on the connection:\n  fresh %+v\n  dirty %+v", name, freshO, dirtyO)
					}
				}
			}
		}
		if got := myConnID(t, dirty); got != id {
			t.Fatalf("the long-lived connection was replaced (%d -> %d)", id, got)
		}
		if st := dq.CacheStats(); st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v", st)
		}
	})
}

// --- random shapes back to back on one connection ---------------------------------------------------

type myOpSpec struct {
	kind    int
	a, b, c int64
	assert  bool
	status  string
}

func myRandomScenario(r *rand.Rand) myScenario {
	n := 1 + r.IntN(6)
	specs := make([]myOpSpec, n)
	for i := range specs {
		s := &specs[i]
		s.kind = r.IntN(8)
		s.a = int64(r.IntN(7)) - 2
		s.b = int64(1 + r.IntN(6)) // 5 and 6 do not exist: assertions on them fail
		s.c = int64(1 + r.IntN(6))
		s.assert = r.IntN(3) == 0
		s.status = []string{"new", "active", "it's", "back\\slash"}[r.IntN(4)]
		if s.kind == 1 {
			s.b = int64(5 + r.IntN(12)) // new ids, some collide with earlier inserts
		}
	}
	return myScenario{fmt.Sprintf("random%v", specs), func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		var ifs []*runtime.IfResult
		var reads []func() string
		for _, s := range specs {
			switch s.kind {
			case 0, 7:
				f := p.AddScoreOne(s.a, s.b)
				if s.assert {
					f.AssertRowsAffected(1)
				}
			case 1:
				f := p.InsertItem(s.b, 2, fmt.Sprintf("r%d", s.b), s.a, s.status)
				if s.assert {
					f.AssertRowsAffected(1)
				}
			case 2:
				f := p.DeleteItems([]int64{s.b, s.c})
				if s.assert {
					f.AssertRowsAffected(1)
				}
			case 3:
				g := p.GetItem(s.b)
				ifs = append(ifs, p.If(g.Found(),
					func(p *myq.Pipeline) { p.AddScoreOne(1, s.b).AssertRowsAffected(1) },
					func(p *myq.Pipeline) { p.AddScoreOne(2, s.c) }))
				reads = append(reads, func() string { return myFmtRow(g) })
			case 4:
				g := p.ItemsByIDs([]int64{s.b, s.c})
				reads = append(reads, func() string { return myFmtSlice(g) })
			case 5:
				f := p.AddScore(s.a, []int64{s.b, s.c}, 1)
				if s.assert {
					f.AssertRowsAffected(2)
				}
			case 6:
				if s.assert {
					g := p.GetItem(s.b)
					ifs = append(ifs, p.If(g.NotFound(), func(p *myq.Pipeline) { p.Abort(myErrStop) }, func(p *myq.Pipeline) { p.SetAllStatus(s.status) }))
				} else {
					p.SetAllStatus(s.status)
				}
			}
		}
		return ifs, func() string {
			var sb strings.Builder
			for _, f := range reads {
				sb.WriteString(f())
				sb.WriteByte('#')
			}
			return sb.String()
		}
	}}
}

// Pipelines of random shapes run back to back, on the same pooled connection, through every run mode;
// each one is compared with the sequential path run from the same table state.
func TestRealEngineMySQLWireRandomShapesMatchSequential(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		seedItems(t, db)
		dsn := os.Getenv(e.env)
		wire := openMyWire(t, dsn, true)
		wire.SetMaxOpenConns(2)
		seq := openMyWire(t, dsn, false)
		wq, sq := myq.New(wire), myq.New(seq)
		defer wq.Close()
		defer sq.Close()
		id := myConnID(t, wire)
		r := rand.New(rand.NewPCG(20260101, uint64(len(e.name))))
		iterations := 160
		ctx := ctxT(t)
		failed := 0
		for i := 0; i < iterations; i++ {
			sc := myRandomScenario(r)
			mode := myRunModes[i%len(myRunModes)]
			before := mySnap(t, db)
			rowsBefore := mySnapshotRows(t, db)
			// sequential reference
			run := func(wdb *sql.DB, q *myq.Queries) myScenarioOutcome {
				ifs, report, err := mode.run(ctx, wdb, q, sc)
				o := myScenarioOutcome{Err: myErrKind(err), State: mySnap(t, db)}
				for _, r := range ifs {
					if r != nil {
						o.Taken = append(o.Taken, r.Taken())
					}
				}
				if err == nil && report != nil {
					o.Report = report()
				}
				return o
			}
			so := run(seq, sq)
			myRestore(t, db, rowsBefore)
			if got := mySnap(t, db); got != before {
				t.Fatalf("restore failed: %s vs %s", got, before)
			}
			wo := run(wire, wq)
			if so.Err != wo.Err || so.State != wo.State || so.Report != wo.Report || !slices.Equal(so.Taken, wo.Taken) {
				t.Fatalf("iteration %d (%s) %s diverges:\n  sequential %+v\n  wire       %+v\n  from state %s", i, mode.name, sc.name, so, wo, before)
			}
			if wo.Err != "" {
				failed++
			}
		}
		t.Logf("%s: %d random pipelines, %d of them failed (identically on both paths)", e.name, iterations, failed)
		if failed == 0 || failed == iterations {
			t.Fatalf("the generator is degenerate: %d/%d failures", failed, iterations)
		}
		if got := myConnID(t, wire); got != id {
			t.Logf("%s: the long-lived connection was replaced (%d -> %d)", e.name, id, got)
		}
		if st := wq.CacheStats(); st.SequentialPipelines != 0 || st.WirePipelines != uint64(iterations) {
			t.Fatalf("wire stats = %+v", st)
		}
		myNoPrepared(t, wire, "after the random run")
	})
}

type myRow struct {
	id, org, score int64
	name, status   string
}

func mySnapshotRows(t testing.TB, db *sql.DB) []myRow {
	t.Helper()
	rows, err := db.Query("SELECT id, org_id, name, score, status FROM sg_items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []myRow
	for rows.Next() {
		var r myRow
		if err := rows.Scan(&r.id, &r.org, &r.name, &r.score, &r.status); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func myRestore(t testing.TB, db *sql.DB, rows []myRow) {
	t.Helper()
	mustExec(t, db, "DELETE FROM sg_items")
	for _, r := range rows {
		if _, err := db.Exec("INSERT INTO sg_items (id, org_id, name, score, status) VALUES (?, ?, ?, ?, ?)", r.id, r.org, r.name, r.score, r.status); err != nil {
			t.Fatal(err)
		}
	}
}

// --- concurrency ------------------------------------------------------------------------------------

// 32 goroutines x 50 pipelines on a pool of 8 connections: transfers locking rows in random order
// (deadlocks are routine and retried), inserts, deletes and conditional updates. The final table equals
// the sequential model, every pipeline went over the wire path, the pool is whole afterwards.
func TestRealEngineMySQLWireConcurrentPipelinesMatchTheModel(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		setupItems(t, db)
		const accounts, start, workers, per = 8, 100, 32, 50
		for i := 1; i <= accounts; i++ {
			mustExec(t, db, fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', %d, 'new')", i, i, start))
		}
		ctx := ctxT(t)
		wdb := openMyWire(t, os.Getenv(e.env), true)
		wdb.SetMaxOpenConns(8)
		wdb.SetMaxIdleConns(8)
		q := myq.New(wdb)
		defer q.Close()

		xid := func(g, i int) int64 { return int64(1000 + g*100 + i) }
		type step struct{ from, to, delta int64 }
		plans := make([][]step, workers)
		model := map[int64]myRow{}
		for i := int64(1); i <= accounts; i++ {
			model[i] = myRow{id: i, org: 1, name: fmt.Sprintf("n%d", i), score: start, status: "new"}
		}
		for g := 0; g < workers; g++ {
			r := rand.New(rand.NewPCG(uint64(g)+1, 99))
			for i := 0; i < per; i++ {
				s := step{from: int64(r.IntN(accounts) + 1), to: int64(r.IntN(accounts) + 1), delta: int64(r.IntN(5) + 1)}
				for s.to == s.from {
					s.to = int64(r.IntN(accounts) + 1)
				}
				plans[g] = append(plans[g], s)
				a, b := model[s.from], model[s.to]
				a.score -= s.delta
				b.score += s.delta
				model[s.from], model[s.to] = a, b
				switch i % 4 {
				case 0, 2:
					model[xid(g, i)] = myRow{id: xid(g, i), org: 2, name: "x", score: int64(i), status: "x"}
				case 1:
					delete(model, xid(g, i-1))
				case 3:
					m := model[xid(g, i-1)]
					m.score++
					model[xid(g, i-1)] = m
				}
			}
		}

		var wg sync.WaitGroup
		for g := 0; g < workers; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i, s := range plans[g] {
					err := q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
						p.AddScoreOne(-s.delta, s.from).AssertRowsAffected(1)
						p.AddScoreOne(s.delta, s.to).AssertRowsAffected(1)
						switch i % 4 {
						case 0, 2:
							p.InsertItem(xid(g, i), 2, "x", int64(i), "x").AssertRowsAffected(1)
						case 1:
							p.DeleteItems([]int64{xid(g, i-1)}).AssertRowsAffected(1)
						case 3:
							// Only a row of this goroutine is written after the read: a write to a row that other
							// transactions change would, on MariaDB 11.6+ (innodb_snapshot_isolation=ON), fail with
							// error 1020 once the read has established the snapshot.
							gt := p.GetItem(xid(g, i-1))
							p.If(gt.Found(),
								func(p *myq.Pipeline) { p.AddScoreOne(1, xid(g, i-1)).AssertRowsAffected(1) },
								func(p *myq.Pipeline) { p.AddScoreOne(1000, 1) })
						}
						return nil
					}, runtime.WithTxRetry(myRetryPolicy))
					if err != nil {
						t.Errorf("goroutine %d pipeline %d: %v", g, i, err)
						return
					}
				}
			}()
		}
		wg.Wait()

		ids := make([]int64, 0, len(model))
		for id := range model {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		var want strings.Builder
		for _, id := range ids {
			m := model[id]
			fmt.Fprintf(&want, "%d/%d/%s/%d/%s;", m.id, m.org, m.name, m.score, m.status)
		}
		if got := mySnap(t, db); got != want.String() {
			t.Fatalf("final state differs from the model:\n got  %s\n want %s", got, want.String())
		}
		st := q.CacheStats()
		t.Logf("%s: %d pipelines ran as %d attempts, pool %+v", e.name, workers*per, st.WirePipelines, wdb.Stats())
		if st.SequentialPipelines != 0 || st.WirePipelines < workers*per {
			t.Fatalf("stats = %+v", st)
		}
		if s := wdb.Stats(); s.InUse != 0 || s.OpenConnections > 8 {
			t.Fatalf("pool after the run: %+v", s)
		}
	})
}
