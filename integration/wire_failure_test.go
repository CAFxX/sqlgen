package integration

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

func backendPID(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var pid int64
	if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return pid
}

// Every way a single-round-trip transaction can fail must roll back, leave the very same connection
// usable (no discarded connection, no transaction left open) and not leak anything from the pool.
func TestRealEngineWireFailurePathsLeaveTheConnectionClean(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, turns, _ := countingDB(t, os.Getenv(e.env), true)
		q := pgq.New(wdb)
		defer q.Close()
		pid := backendPID(t, wdb)
		opened := wdb.Stats().OpenConnections

		stop := errors.New("stop")
		failures := []struct {
			name  string
			run   func() error
			check func(error) bool
		}{
			{"failed assertion", func() error {
				return q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
					p.AddScoreOne(1, 1).AssertRows(1)
					p.AddScoreOne(1, 999).AssertRows(1)
					return nil
				})
			}, func(err error) bool { return errors.Is(err, runtime.ErrAssertionFailed) }},
			{"Abort in a taken branch", func() error {
				return q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
					p.AddScoreOne(1, 1)
					g := p.GetItem(1)
					p.If(g.Found(), func(p *pgq.Pipeline) { p.Abort(stop) }, nil)
					return nil
				})
			}, func(err error) bool { return errors.Is(err, stop) }},
			{"server error in the middle of the batch", func() error {
				return q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
					p.AddScoreOne(1, 1)
					p.InsertItem(1, 1, "dup", 0, "x")
					p.AddScoreOne(1, 2)
					return nil
				})
			}, func(err error) bool {
				var pgErr *pgconn.PgError
				return errors.As(err, &pgErr) && pgErr.Code == "23505"
			}},
			{"error in a standalone Flush", func() error {
				p := q.Pipeline(ctx)
				p.AddScoreOne(1, 1)
				p.InsertItem(1, 1, "dup", 0, "x")
				return p.Flush()
			}, func(err error) bool {
				var pgErr *pgconn.PgError
				return errors.As(err, &pgErr) && pgErr.Code == "23505"
			}},
			{"error in a user-managed transaction", func() error {
				tx, err := wdb.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				p := q.WithTx(tx).Pipeline(ctx)
				p.AddScoreOne(1, 1)
				p.InsertItem(1, 1, "dup", 0, "x")
				return p.Flush()
			}, func(err error) bool {
				var pgErr *pgconn.PgError
				return errors.As(err, &pgErr) && pgErr.Code == "23505"
			}},
		}
		for _, f := range failures {
			before := turns.Load()
			err := f.run()
			cost := turns.Load() - before
			if err == nil || !f.check(err) {
				t.Fatalf("%s: err = %v", f.name, err)
			}
			t.Logf("%s: %s failed after %d turn(s)", e.name, f.name, cost)
			if cost > 3 { // the batch, one ROLLBACK for the aborted transaction, plus the user tx's BEGIN/ROLLBACK
				t.Fatalf("%s: a failure must cost the batch plus at most a rollback, got %d turns", f.name, cost)
			}
			if st := wdb.Stats(); st.InUse != 0 || st.OpenConnections != opened {
				t.Fatalf("%s: pool state after the failure: %+v (opened %d)", f.name, st, opened)
			}
			if got := backendPID(t, wdb); got != pid {
				t.Fatalf("%s: the connection was replaced (%d -> %d)", f.name, pid, got)
			}
			if got := snapshot(t, db); got != "1/1/n1/10/new;2/1/n2/20/new;3/1/n3/30/new;4/1/n4/40/new;" {
				t.Fatalf("%s: the failed transaction left its changes behind: %s", f.name, got)
			}
			// ... and the next pipeline is a normal one-turn transaction again
			cost = measure(t, wdb, turns, func() {
				if err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
					p.AddScoreOne(1, 1).AssertRows(1)
					p.AddScoreOne(-1, 1).AssertRows(1)
					return nil
				}); err != nil {
					t.Fatalf("%s: pipeline after the failure: %v", f.name, err)
				}
			})
			if cost != 1 {
				t.Fatalf("%s: the pipeline after a failure cost %d turns", f.name, cost)
			}
		}
		if e.family == famPostgres {
			var n int
			if err := db.QueryRow("SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND state LIKE 'idle in transaction%'").Scan(&n); err != nil || n != 0 {
				t.Fatalf("%d sessions are stuck in a transaction (%v)", n, err)
			}
		}

		// many failures in a row must not leak connections
		for i := 0; i < 100; i++ {
			err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { p.AddScoreOne(1, 999).AssertFound(); return nil })
			if !errors.Is(err, runtime.ErrAssertionFailed) {
				t.Fatalf("iteration %d: %v", i, err)
			}
		}
		if st := wdb.Stats(); st.InUse != 0 || st.OpenConnections != opened || st.MaxIdleClosed != 0 {
			t.Fatalf("pool after 100 failures: %+v", st)
		}
		if st := q.CacheStats(); st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v", st)
		}
	})
}

// A write-write conflict that the server detects while the batch is running (PostgreSQL: concurrent
// update at REPEATABLE READ, SQLSTATE 40001) is retried by WithTxRetry and the retry succeeds.
func TestRealEngineWireRetriesAConflictDetectedMidBatch(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, _, _ := countingDB(t, os.Getenv(e.env), true)
		wdb.SetMaxOpenConns(4)
		q := pgq.New(wdb)
		defer q.Close()

		holder, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Rollback()
		if _, err := holder.ExecContext(ctx, "UPDATE sg_items SET score = score + 1 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}

		var attempts atomic.Int64
		done := make(chan error, 1)
		go func() {
			done <- q.PipelineTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead}, func(p *pgq.Pipeline) error {
				attempts.Add(1)
				p.AddScoreOne(10, 1).AssertRows(1)
				return nil
			}, runtime.WithTxRetry(runtime.TxRetryPolicy{MaxRetries: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}))
		}()
		// wait until the pipeline's UPDATE is blocked on our row lock, then release it by committing
		if e.family == famPostgres {
			deadline := time.Now().Add(20 * time.Second)
			for {
				var waiting int
				if err := db.QueryRow("SELECT COUNT(*) FROM pg_locks WHERE NOT granted").Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the pipeline never blocked on the row lock")
				}
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			time.Sleep(300 * time.Millisecond)
		}
		if err := holder.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("PipelineTx: %v", err)
		}
		t.Logf("%s: %d attempt(s)", e.name, attempts.Load())
		if e.family == famPostgres && attempts.Load() != 2 {
			t.Fatalf("expected the concurrent-update error to cost exactly one retry, attempts = %d", attempts.Load())
		}
		if got := snapshot(t, db); got != "1/1/n1/21/new;2/1/n2/20/new;3/1/n3/30/new;4/1/n4/40/new;" {
			t.Fatalf("state = %s", got)
		}
		st := q.CacheStats()
		if st.WirePipelines != uint64(attempts.Load()) || st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v after %d attempts", st, attempts.Load())
		}
		if s := wdb.Stats(); s.InUse != 0 {
			t.Fatalf("pool = %+v", s)
		}
	})
}

// An error raised by the COMMIT itself (a deferred constraint trigger that fails with
// serialization_failure) is retried like any other retryable failure, and costs one turn per attempt.
func TestRealEngineWireRetriesAnErrorRaisedAtCommit(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if e.family != famPostgres {
			t.Skip("needs deferred constraint triggers (PostgreSQL)")
		}
		seedItems(t, db)
		seq, fn := uniqueName("sg_commit_seq"), uniqueName("sg_commit_fn")
		mustExec(t, db, "CREATE SEQUENCE "+seq)
		mustExec(t, db, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				IF nextval('%s') <= 2 THEN
					RAISE EXCEPTION 'simulated commit-time conflict' USING ERRCODE = '40001';
				END IF;
				RETURN NULL;
			END $$`, fn, seq))
		mustExec(t, db, fmt.Sprintf("CREATE CONSTRAINT TRIGGER sg_commit_trg AFTER INSERT ON sg_items DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %s()", fn))
		t.Cleanup(func() {
			_, _ = db.Exec("DROP TRIGGER IF EXISTS sg_commit_trg ON sg_items")
			_, _ = db.Exec("DROP FUNCTION IF EXISTS " + fn + "()")
			_, _ = db.Exec("DROP SEQUENCE IF EXISTS " + seq)
		})
		ctx := ctxT(t)
		wdb, turns, _ := countingDB(t, os.Getenv(e.env), true)
		q := pgq.New(wdb)
		defer q.Close()
		pid := backendPID(t, wdb)

		// without retries the caller sees the server's error, unwrapped
		err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { p.InsertItem(100, 1, "x", 0, "x"); return nil })
		var pgErr *pgconn.PgError
		var ce *runtime.CommitError
		if !errors.As(err, &pgErr) || pgErr.Code != "40001" || errors.As(err, &ce) || !runtime.IsRetryableTxError(err, false) {
			t.Fatalf("err = %v", err)
		}
		// with retries it takes one more failing attempt, then succeeds
		attempts := 0
		cost := measure(t, wdb, turns, func() {
			err = q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
				attempts++
				p.InsertItem(101, 1, "y", 0, "x").AssertRows(1)
				return nil
			}, runtime.WithTxRetry(runtime.TxRetryPolicy{MaxRetries: 5, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}))
		})
		if err != nil {
			t.Fatalf("PipelineTx with retries: %v", err)
		}
		if attempts != 2 || cost != 2 {
			t.Fatalf("attempts = %d, turns = %d; want 2 and 2 (one failing COMMIT, one good attempt)", attempts, cost)
		}
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM sg_items WHERE id IN (100, 101)").Scan(&n); err != nil || n != 1 {
			t.Fatalf("rows = %d, %v (only the committed attempt may remain)", n, err)
		}
		if got := backendPID(t, wdb); got != pid || wdb.Stats().InUse != 0 {
			t.Fatalf("connection replaced or leaked: pid %d -> %d, %+v", pid, got, wdb.Stats())
		}
	})
}

// 16 goroutines of contended transfers through the single-round-trip path: money is conserved, every
// transaction went over the wire path, and the pool is whole afterwards.
func TestRealEngineWireContendedTransfersConserveMoney(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		setupItems(t, db)
		ctx := ctxT(t)
		wdb := openWire(t, os.Getenv(e.env), true)
		wdb.SetMaxOpenConns(8)
		wdb.SetMaxIdleConns(8)
		a := newPgqAPI(wdb)
		defer a.Close()
		const accounts, start = 6, 1000
		rows := make([]item, accounts)
		for i := range rows {
			rows[i] = item{ID: int64(i + 1), OrgID: 1, Name: fmt.Sprintf("acct-%d", i), Score: start, Status: "active"}
		}
		if err := a.Insert(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if e.family == famPostgres {
			var dbName string
			if err := db.QueryRow("SELECT current_database()").Scan(&dbName); err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, fmt.Sprintf("ALTER DATABASE %q SET deadlock_timeout = '20ms'", dbName))
			t.Cleanup(func() { _, _ = db.Exec(fmt.Sprintf("ALTER DATABASE %q RESET deadlock_timeout", dbName)) })
			wdb.SetConnMaxLifetime(time.Nanosecond) // fresh sessions pick up the new setting
			time.Sleep(50 * time.Millisecond)
			wdb.SetConnMaxLifetime(0)
		}
		pol := runtime.TxRetryPolicy{MaxRetries: 200, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond}
		workers, perWorker := 16, 40
		if e.family == famCockroach {
			workers, perWorker = 8, 12 // optimistic restarts under this much contention are slow; keep CI fast
		}
		var failed, transfers atomic.Int64
		var wg sync.WaitGroup
		for g := 0; g < workers; g++ {
			wg.Add(1)
			go func(seed uint64) {
				defer wg.Done()
				r := rand.New(rand.NewPCG(seed, seed*7+1))
				for i := 0; i < perWorker; i++ {
					from, to := int64(r.IntN(accounts)+1), int64(r.IntN(accounts)+1)
					if from == to {
						continue
					}
					transfers.Add(1)
					// opposing lock orders across goroutines: deadlocks / serialization failures are routine
					if err := a.PipelineTransfer(ctx, from, to, int64(r.IntN(5)+1), pol); err != nil {
						failed.Add(1)
						t.Errorf("PipelineTransfer %d->%d: %v", from, to, err)
					}
				}
			}(uint64(g + 1))
		}
		wg.Wait()
		var sum int64
		if err := db.QueryRow("SELECT SUM(score) FROM sg_items").Scan(&sum); err != nil {
			t.Fatal(err)
		}
		if sum != accounts*start {
			t.Fatalf("money not conserved: sum = %d, want %d (failed transfers: %d)", sum, accounts*start, failed.Load())
		}
		st := a.CacheStats()
		t.Logf("%s: %d transfers, %d pipeline attempts (%d retries), pool %+v", e.name, transfers.Load(), st.WirePipelines, int64(st.WirePipelines)-transfers.Load(), wdb.Stats())
		if st.SequentialPipelines != 0 || st.WirePipelines < uint64(transfers.Load()) {
			t.Fatalf("stats = %+v for %d transfers", st, transfers.Load())
		}
		if s := wdb.Stats(); s.InUse != 0 || s.OpenConnections > 8 {
			t.Fatalf("pool after the run: %+v", s)
		}
	})
}

// CacheStats tells the two paths apart: the same pipeline counts as wire on a pgxwire handle and as
// sequential on a plain pgx handle, whether it runs in PipelineTx, standalone or in a user transaction.
func TestRealEngineCacheStatsReportTheExecutionPath(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		for _, wire := range []bool{true, false} {
			q := pgq.New(openWire(t, os.Getenv(e.env), wire))
			var want [2]uint64
			bump := func() {
				if wire {
					want[0]++
				} else {
					want[1]++
				}
			}
			if err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { p.AddScoreOne(1, 1); return nil }); err != nil {
				t.Fatal(err)
			}
			bump()
			p := q.Pipeline(ctx)
			p.AddScoreOne(1, 1)
			if err := p.Flush(); err != nil {
				t.Fatal(err)
			}
			bump()
			// a failed flush still ran on its path; an empty or unbuildable one ran on none
			if err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { p.AddScoreOne(1, 999).AssertFound(); return nil }); !errors.Is(err, runtime.ErrAssertionFailed) {
				t.Fatalf("err = %v", err)
			}
			bump()
			if err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { return errors.New("nothing queued") }); err == nil {
				t.Fatal("expected the callback's error")
			}
			st := q.CacheStats()
			if got := [2]uint64{st.WirePipelines, st.SequentialPipelines}; got != want {
				t.Fatalf("wire=%v: stats {wire, sequential} = %v, want %v", wire, got, want)
			}
			q.Close()
		}
	})
}
