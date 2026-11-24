package mysqlwire_test

// These tests run against real MySQL-protocol servers and are skipped when no DSN is configured:
//
//	. ../scripts/engines-env.sh && go test -race ./...
//
// They drive runtime.Pipeline directly with hand-written descriptors (the integration module covers the
// generated-code path and the wire-vs-sequential differential on top of this driver). internal_test.go
// covers the error paths that need a scripted server.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/mysqlwire"
	"github.com/CAFxX/sqlgen/runtime"
	"github.com/CAFxX/sqlgen/runtime/wire"
)

type testEngine struct {
	name, env string
	maria     bool
	dsn       string
}

var testEngines = []testEngine{
	{name: "mysql84", env: "SQLGEN_MYSQL84_DSN"},
	{name: "mysql97", env: "SQLGEN_MYSQL97_DSN"},
	{name: "mariadb1011", env: "SQLGEN_MARIADB1011_DSN", maria: true},
	{name: "mariadb123", env: "SQLGEN_MARIADB123_DSN", maria: true},
}

// dsnFor returns the DSN of e unless SQLGEN_ENGINES (comma separated, like the integration module) excludes it.
func dsnFor(e testEngine) string {
	if sel := strings.TrimSpace(os.Getenv("SQLGEN_ENGINES")); sel != "" && !slices.Contains(strings.Split(sel, ","), e.name) {
		return ""
	}
	return os.Getenv(e.env)
}

func forEachEngine(t *testing.T, fn func(t *testing.T, e testEngine, db *sql.DB, plain *sql.DB)) {
	t.Helper()
	ran := false
	for _, e := range testEngines {
		e.dsn = dsnFor(e)
		if e.dsn == "" {
			continue
		}
		ran = true
		t.Run(e.name, func(t *testing.T) {
			db, err := mysqlwire.OpenDB(e.dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			plain, err := sql.Open("mysql", e.dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = plain.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := db.PingContext(ctx); err != nil {
				t.Fatalf("ping: %v", err)
			}
			fn(t, e, db, plain)
		})
	}
	if !ran {
		t.Skip("no SQLGEN_MYSQL84_DSN / SQLGEN_MYSQL97_DSN / SQLGEN_MARIADB1011_DSN / SQLGEN_MARIADB123_DSN configured")
	}
}

var tableSeq atomic.Int64

func newTable(t *testing.T, db *sql.DB, ddl string) string {
	t.Helper()
	name := fmt.Sprintf("mysqlw_%d_%d", os.Getpid(), tableSeq.Add(1))
	if _, err := db.Exec(fmt.Sprintf(ddl, name)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + name) })
	return name
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// ---- hand-written descriptors over a table (id BIGINT PRIMARY KEY, v BIGINT, ts DATETIME(6)) ----

type kv struct {
	table                      string
	upd, get, ins, many, setTS *runtime.QueryDescriptor
	cache                      *runtime.StmtCache
}

func newKV(table string) *kv {
	k := &kv{table: table, cache: runtime.NewStmtCache()}
	k.upd = &runtime.QueryDescriptor{
		Name: "Upd", StmtID: 1, StmtVerb: runtime.VerbUpdate,
		StaticSQL: "UPDATE " + table + " SET v = v + ? WHERE id = ?",
		Segments:  []string{"UPDATE " + table + " SET v = v + ", " WHERE ", "id = ", "", ""},
		Slots:     []runtime.Slot{{Kind: runtime.SlotScalar}, {Kind: runtime.SlotBranchGuardOpen}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotBranchGuardClose}},
	}
	k.get = &runtime.QueryDescriptor{
		Name: "Get", StmtID: 2, StmtVerb: runtime.VerbSelect,
		StaticSQL:      "SELECT id, v FROM " + table + " WHERE id = ?",
		Segments:       []string{"SELECT ", " FROM " + table + " WHERE id = ", ""},
		Slots:          []runtime.Slot{{Kind: runtime.SlotSelectCols}, {Kind: runtime.SlotScalar}},
		SelectColExprs: []string{"id", "v"},
	}
	k.ins = &runtime.QueryDescriptor{
		Name: "Ins", StmtID: 3, StmtVerb: runtime.VerbInsert,
		StaticSQL: "INSERT INTO " + table + " (id, v) VALUES (?, ?)",
		Segments:  []string{"INSERT INTO " + table + " (id, v) ", "", ", ", "", ""},
		Slots:     []runtime.Slot{{Kind: runtime.SlotInsertValuesOpen}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotInsertValuesClose}},
	}
	k.many = &runtime.QueryDescriptor{
		Name: "All", StmtID: 4, StmtVerb: runtime.VerbSelect,
		StaticSQL:      "SELECT id, v FROM " + table + " ORDER BY id",
		Segments:       []string{"SELECT ", " FROM " + table + " ORDER BY id"},
		Slots:          []runtime.Slot{{Kind: runtime.SlotSelectCols}},
		SelectColExprs: []string{"id", "v"},
	}
	k.setTS = &runtime.QueryDescriptor{
		Name: "SetTS", StmtID: 5, StmtVerb: runtime.VerbUpdate,
		StaticSQL: "UPDATE " + table + " SET ts = ? WHERE id = ?",
		Segments:  []string{"UPDATE " + table + " SET ts = ", " WHERE ", "id = ", "", ""},
		Slots:     []runtime.Slot{{Kind: runtime.SlotScalar}, {Kind: runtime.SlotBranchGuardOpen}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotBranchGuardClose}},
	}
	return k
}

type row struct{ ID, V int64 }

func args(vs ...any) *[]any {
	b := runtime.AcquireArgsCap(len(vs))
	*b = append(*b, vs...)
	return b
}

func (k *kv) Upd(p *runtime.Pipeline, delta, id int64) *runtime.ExecFuture {
	return p.QueueExec(1, k.upd, runtime.RenderInput{}, runtime.MakeVariantKey(1, 0, 0, 0), nil, args(delta, id), nil)
}

// UpdAny is Upd with arbitrary argument values.
func (k *kv) UpdAny(p *runtime.Pipeline, delta, id any) *runtime.ExecFuture {
	return p.QueueExec(1, k.upd, runtime.RenderInput{}, runtime.MakeVariantKey(1, 0, 0, 0), nil, args(delta, id), nil)
}

func (k *kv) Ins(p *runtime.Pipeline, id, v int64) *runtime.ExecFuture {
	return p.QueueExec(3, k.ins, runtime.RenderInput{}, runtime.MakeVariantKey(3, 0, 0, 0), nil, args(id, v), nil)
}

func (k *kv) SetTS(p *runtime.Pipeline, ts time.Time, id int64) *runtime.ExecFuture {
	return p.QueueExec(5, k.setTS, runtime.RenderInput{}, runtime.MakeVariantKey(5, 0, 0, 0), nil, args(ts, id), nil)
}

func (k *kv) Get(p *runtime.Pipeline, id int64) *runtime.RowFuture[row] {
	return runtime.QueueRow[row](p, 2, k.get, runtime.RenderInput{}, runtime.MakeVariantKey(2, 0, 0, 0), nil, args(id), nil,
		func(r *sql.Row, d *row) error { return r.Scan(&d.ID, &d.V) },
		func(r *sql.Rows, d *row) error { return r.Scan(&d.ID, &d.V) })
}

func (k *kv) All(p *runtime.Pipeline) *runtime.SliceFuture[row] {
	return runtime.QueueSlice[row](p, 4, k.many, runtime.RenderInput{}, runtime.MakeVariantKey(4, 0, 0, 0), nil, args(), nil,
		func(r *sql.Rows, d *row) error { return r.Scan(&d.ID, &d.V) })
}

func (k *kv) tx(ctx context.Context, db *sql.DB, txOpts *sql.TxOptions, opts []runtime.TxOption, build func(p *runtime.Pipeline) error) error {
	return runtime.RunPipelineTx(ctx, db, k.cache, nil, true, runtime.DialectQuestion, txOpts, opts, build)
}

func (k *kv) flush(ctx context.Context, db *sql.DB, build func(p *runtime.Pipeline)) error {
	p := runtime.NewPipeline(ctx, db, k.cache, nil, true, runtime.DialectQuestion)
	build(p)
	return p.Flush()
}

// fresh returns a copy of k with its own statement cache, so that CacheStats of one pool can be told from
// another's.
func (k *kv) fresh(t *testing.T) *kv {
	t.Helper()
	c := *k
	c.cache = runtime.NewStmtCache()
	t.Cleanup(func() { _ = c.cache.Close() })
	return &c
}

func (k *kv) rows(t *testing.T, db *sql.DB) []row {
	t.Helper()
	rs, err := db.Query("SELECT id, v FROM " + k.table + " ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var out []row
	for rs.Next() {
		var r row
		if err := rs.Scan(&r.ID, &r.V); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func newKVTable(t *testing.T, db *sql.DB) *kv {
	t.Helper()
	k := newKV(newTable(t, db, "CREATE TABLE %s (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, ts DATETIME(6) NULL) ENGINE=InnoDB"))
	t.Cleanup(func() { _ = k.cache.Close() })
	for i := 1; i <= 3; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, v) VALUES (%d, %d)", k.table, i, i*10)); err != nil {
			t.Fatal(err)
		}
	}
	return k
}

var orig = []row{{1, 10}, {2, 20}, {3, 30}}

func sameRows(t *testing.T, got, want []row) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
}

func stats(t *testing.T, k *kv, wire, seq uint64) {
	t.Helper()
	if st := k.cache.Stats(); st.WirePipelines != wire || st.SequentialPipelines != seq {
		t.Fatalf("stats = wire %d / sequential %d, want %d / %d", st.WirePipelines, st.SequentialPipelines, wire, seq)
	}
}

func poolIdle(t *testing.T, db *sql.DB) {
	t.Helper()
	if st := db.Stats(); st.InUse != 0 {
		t.Fatalf("pool leak: %+v", st)
	}
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("database unusable after the failure: %v", err)
	}
}

// noPreparedLeft checks, on the single connection of db (SetMaxOpenConns(1)), that no sqlgen_s prepared
// statement is left on the server session (a DEALLOCATE of an unknown statement is error 1243).
func noPreparedLeft(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := ctxT(t)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "DEALLOCATE PREPARE sqlgen_s")
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1243 {
		t.Fatalf("a prepared statement leaked on the session (DEALLOCATE: %v)", err)
	}
}

func mysqlNumber(err error) uint16 {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

// ---- tests ----

func TestPipelineTxAndFlushRunOnTheWire(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		var up1, up2, noop *runtime.ExecFuture
		var g, miss *runtime.RowFuture[row]
		var all *runtime.SliceFuture[row]
		err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			up1 = k.Upd(p, -5, 1).AssertRowsAffected(1)
			up2 = k.Upd(p, 5, 2).AssertFound()
			noop = k.Upd(p, 5, 99) // matches nothing
			g = k.Get(p, 3)
			miss = k.Get(p, 99)
			all = k.All(p)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 5}, {2, 25}, {3, 30}})
		for i, f := range []*runtime.ExecFuture{up1, up2} {
			if n, err := f.RowsAffected(); n != 1 || err != nil {
				t.Fatalf("up%d = %d, %v", i+1, n, err)
			}
		}
		if n, err := noop.RowsAffected(); n != 0 || err != nil {
			t.Fatalf("noop = %d, %v", n, err)
		}
		if v, err := g.Get(); err != nil || v != (row{3, 30}) {
			t.Fatalf("Get = %v, %v", v, err)
		}
		if _, err := miss.Get(); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("Get(missing) = %v, want sql.ErrNoRows", err)
		}
		// the select ran after the updates of the same batch and sees them
		if vs, err := all.Get(); err != nil || !reflect.DeepEqual(vs, []row{{1, 5}, {2, 25}, {3, 30}}) {
			t.Fatalf("All = %v, %v", vs, err)
		}
		stats(t, k, 1, 0)

		// a standalone flush is atomic and on the wire too
		if err := k.flush(ctx, db, func(p *runtime.Pipeline) { k.Upd(p, 1, 1); k.Upd(p, 1, 2) }); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 6}, {2, 26}, {3, 30}})
		err = k.flush(ctx, db, func(p *runtime.Pipeline) { k.Upd(p, 1, 1); k.Ins(p, 2, 0); k.Upd(p, 1, 3) })
		if mysqlNumber(err) != 1062 {
			t.Fatalf("err = %v", err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 6}, {2, 26}, {3, 30}})
		stats(t, k, 3, 0)

		// the session is clean for the next pipeline (user variables are overwritten, not inherited)
		if err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.Upd(p, 1, 3).AssertFound(); return nil }); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 6}, {2, 26}, {3, 31}})
		poolIdle(t, db)
	})
}

func TestAssertionFailureRollsBackAndKeepsTheConnectionClean(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		db.SetMaxOpenConns(1)

		err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			k.Upd(p, 1, 1).AssertRowsAffected(1)
			k.Upd(p, 1, 99).AssertRowsAffected(1) // no such row
			k.Upd(p, 1, 2)
			return nil
		})
		var ae *runtime.AssertionError
		if !errors.Is(err, runtime.ErrAssertionFailed) || !errors.As(err, &ae) {
			t.Fatalf("err = %v", err)
		}
		if ae.Statement != "Upd" || ae.Want != 1 || ae.Got != 0 || !ae.GotKnown {
			t.Fatalf("assertion = %+v", *ae)
		}
		sameRows(t, k.rows(t, plain), orig)
		stats(t, k, 1, 0)

		// the same connection is clean: no open transaction, no prepared statement, usable
		noPreparedLeft(t, db)
		var inTx int
		if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id = CONNECTION_ID()").Scan(&inTx); err != nil || inTx != 0 {
			t.Fatalf("open InnoDB transactions of the session = %d, %v", inTx, err)
		}
		if err := db.Ping(); err != nil {
			t.Fatal(err)
		}
		if err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.Upd(p, 7, 1).AssertFound(); return nil }); err != nil {
			t.Fatalf("pipeline after the failure: %v", err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 17}, {2, 20}, {3, 30}})

		// a failed assertion on a flush of a read (select with an assertion) names the select
		err = k.flush(ctx, db, func(p *runtime.Pipeline) { k.Get(p, 99).AssertFound() })
		if !errors.As(err, &ae) || ae.Statement != "Get" {
			t.Fatalf("err = %v", err)
		}
		noPreparedLeft(t, db)
		poolIdle(t, db)
	})
}

func TestAbortAndConditionsOverTheWire(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		db.SetMaxOpenConns(1)

		// If/Else on rows found, with the skipped branch inert
		var thenRes, elseRes *runtime.IfResult
		var skipped, taken *runtime.ExecFuture
		err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			hit, miss := k.Get(p, 1), k.Get(p, 99)
			thenRes = p.If(hit.Found(), func() { taken = k.Upd(p, 1, 1) }, func() { skipped = k.Upd(p, 1000, 2) })
			elseRes = p.If(miss.Found(), func() { k.Upd(p, 1000, 3) }, nil)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		_, skippedErr := skipped.Result()
		_, takenErr := taken.Result()
		if !thenRes.Taken() || elseRes.Taken() || !errors.Is(skippedErr, runtime.ErrBranchNotTaken) || errors.Is(takenErr, runtime.ErrBranchNotTaken) {
			t.Fatalf("taken = %v / %v, branch errors = %v / %v", thenRes.Taken(), elseRes.Taken(), skippedErr, takenErr)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 20}, {3, 30}})

		// conditions on RowsAffected of an exec and on several reads of one future
		var c1, c2, c3 *runtime.IfResult
		err = k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			up := k.Upd(p, 1, 1)
			c1 = p.If(up.RowsAffectedEq(1), func() { k.Upd(p, 1, 2) }, func() { k.Upd(p, 1000, 2) })
			c2 = p.If(up.RowsAffectedGt(1).Or(up.NotFound()), func() { k.Upd(p, 1000, 2) }, func() { k.Upd(p, 1, 3) })
			hit := k.Get(p, 1)
			c3 = p.If(hit.RowsAffectedGe(1).And(hit.RowsAffectedLe(1)).And(hit.RowsAffectedLt(2)), func() { k.Upd(p, 3, 3) }, nil)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !c1.Taken() || c2.Taken() || !c3.Taken() {
			t.Fatalf("taken = %v %v %v", c1.Taken(), c2.Taken(), c3.Taken())
		}
		sameRows(t, k.rows(t, plain), []row{{1, 12}, {2, 21}, {3, 34}})

		// WhereCol (the select runs a second time, inside the aggregate) and a compound condition over two
		// operations
		var w1, w2, w3 *runtime.IfResult
		err = k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			a, b := k.Get(p, 1), k.Get(p, 2)
			w1 = p.If(a.WhereCol("v > 10", func(r row) bool { return r.V > 10 }), func() { k.Upd(p, 100, 1) }, nil)
			w2 = p.If(a.Found().And(b.WhereCol("v >= 1000", func(r row) bool { return r.V >= 1000 })), func() { k.Upd(p, 1000, 2) }, func() { k.Upd(p, 7, 2) })
			w3 = p.If(a.NotFound().Or(b.WhereCol("v = 28", func(r row) bool { return r.V == 28 })).Not(), func() { k.Upd(p, 11, 3) }, nil)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !w1.Taken() || w2.Taken() || !w3.Taken() {
			t.Fatalf("taken = %v %v %v", w1.Taken(), w2.Taken(), w3.Taken())
		}
		sameRows(t, k.rows(t, plain), []row{{1, 112}, {2, 28}, {3, 45}})

		// Abort in an untaken branch is inert
		stop := errors.New("stop here")
		err = k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			k.Upd(p, 1, 1)
			miss := k.Get(p, 99)
			p.If(miss.Found(), func() { p.Abort(stop) }, nil)
			k.Upd(p, 1, 2)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 113}, {2, 29}, {3, 45}})

		// Abort in the taken branch returns the caller's error and rolls everything back
		err = k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			k.Upd(p, 100, 1)
			hit := k.Get(p, 1)
			p.If(hit.Found(), func() { p.Abort(stop) }, nil)
			k.Upd(p, 100, 2)
			return nil
		})
		if !errors.Is(err, stop) {
			t.Fatalf("err = %v, want the abort error", err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 113}, {2, 29}, {3, 45}})
		noPreparedLeft(t, db)
		poolIdle(t, db)
	})
}

func TestServerErrorMidBatchRollsBackAndIsAttributed(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		db.SetMaxOpenConns(1)
		for _, withGuard := range []bool{false, true} { // the guard makes the batch end through a prepared COMMIT
			var failing *runtime.ExecFuture
			err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
				f := k.Upd(p, 1, 1)
				if withGuard {
					f.AssertRowsAffected(1)
				}
				failing = k.Ins(p, 1, 0) // duplicate primary key
				k.Upd(p, 1, 2)
				return nil
			})
			var me *mysql.MySQLError
			if !errors.As(err, &me) || me.Number != 1062 {
				t.Fatalf("withGuard=%v: err = %v, want *mysql.MySQLError 1062", withGuard, err)
			}
			var ce *runtime.CommitError
			if errors.As(err, &ce) {
				t.Fatalf("the CommitError marker leaked: %v", err)
			}
			_ = failing
			sameRows(t, k.rows(t, plain), orig)
			noPreparedLeft(t, db)
			poolIdle(t, db)
		}
		stats(t, k, 2, 0)

		// the first statement failing and a bad statement text are server errors too
		err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.Ins(p, 3, 0); return nil })
		if mysqlNumber(err) != 1062 {
			t.Fatalf("err = %v", err)
		}
		if err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.Upd(p, 1, 1).AssertFound(); return nil }); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 20}, {3, 30}})

		// retries: a permanent error is not retried
		attempts := 0
		err = k.tx(ctx, db, nil, []runtime.TxOption{runtime.WithTxRetry(runtime.TxRetryPolicy{MaxRetries: 3, BaseDelay: time.Millisecond})}, func(p *runtime.Pipeline) error {
			attempts++
			k.Ins(p, 3, 0)
			return nil
		})
		if mysqlNumber(err) != 1062 || attempts != 1 {
			t.Fatalf("err = %v after %d attempts", err, attempts)
		}
		poolIdle(t, db)
	})
}

func TestUserManagedTransaction(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		db.SetMaxOpenConns(1)
		newP := func(tx *sql.Tx) *runtime.Pipeline {
			return runtime.NewPipeline(ctx, db, k.cache, runtime.NewTxStmtCache(tx), true, runtime.DialectQuestion)
		}

		// commit: the ops run in the caller's transaction, in one batch, nothing is committed by the batch
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		p := newP(tx)
		k.Upd(p, 1, 1)
		g := k.Get(p, 1)
		if err := p.Flush(); err != nil {
			t.Fatal(err)
		}
		if v, _ := g.Get(); v != (row{1, 11}) {
			t.Fatalf("Get inside the tx = %v", v)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), orig) // the batch did not commit the caller's transaction
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		p = newP(tx)
		k.Upd(p, 1, 1)
		if err := p.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 20}, {3, 30}})
		stats(t, k, 2, 0)

		// a guard failure leaves the transaction usable: the work before stays, the failing operation and
		// everything after it have no effect (savepoint), the caller's own work and commit go through
		abortErr := errors.New("stop")
		if _, err := db.Exec("UPDATE " + k.table + " SET v = id * 10"); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name  string
			build func(p *runtime.Pipeline)
			check func(err error) bool
			want  []row
		}{
			{"assertion", func(p *runtime.Pipeline) {
				k.Upd(p, 1, 1).AssertRowsAffected(1)
				k.Upd(p, 5, 2)
				k.Upd(p, 1, 404).AssertRowsAffected(1) // fails
				k.Upd(p, 1000, 3)
			}, func(err error) bool { return errors.Is(err, runtime.ErrAssertionFailed) }, []row{{1, 11}, {2, 25}, {3, 130}}},
			{"abort", func(p *runtime.Pipeline) {
				k.Upd(p, 1, 1).AssertRowsAffected(1)
				g := k.Get(p, 1)
				p.If(g.Found(), func() { p.Abort(abortErr) }, nil)
				k.Upd(p, 1000, 3)
			}, func(err error) bool { return errors.Is(err, abortErr) }, []row{{1, 11}, {2, 20}, {3, 130}}},
			{"assertion on the update that fails after changing a row", func(p *runtime.Pipeline) {
				k.Upd(p, 5, 2).AssertRowsAffected(2) // changes row 2, then fails: the savepoint undoes it
			}, func(err error) bool { return errors.Is(err, runtime.ErrAssertionFailed) }, []row{{1, 10}, {2, 20}, {3, 130}}},
		} {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			p := newP(tx)
			tc.build(p)
			if err := p.Flush(); !tc.check(err) {
				t.Fatalf("%s: Flush = %v", tc.name, err)
			}
			if _, err := tx.ExecContext(ctx, "UPDATE "+k.table+" SET v = v + 100 WHERE id = 3"); err != nil {
				t.Fatalf("%s: the transaction is unusable after the failure: %v", tc.name, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("%s: commit: %v", tc.name, err)
			}
			sameRows(t, k.rows(t, plain), tc.want)
			poolIdle(t, db)
			if _, err := db.Exec("UPDATE " + k.table + " SET v = id * 10"); err != nil {
				t.Fatal(err)
			}
		}

		// a database error is not a guard failure: like the sequential path it leaves the transaction open
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		p = newP(tx)
		k.Upd(p, 1, 1).AssertRowsAffected(1)
		k.Ins(p, 1, 0).AssertRowsAffected(1)
		if err := p.Flush(); mysqlNumber(err) != 1062 {
			t.Fatalf("Flush = %v", err)
		}
		var one int
		if err := tx.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
			t.Fatalf("the transaction is unusable after a database error: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), orig)
		noPreparedLeft(t, db)
		poolIdle(t, db)
	})
}

func TestUnsupportedBatchesFallBackToTheSequentialPath(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		base := newKVTable(t, db)
		ctx := ctxT(t)

		// run the same build on the mysqlwire pool and on the plain pool: the result must be identical
		both := func(name string, run func(k *kv, db *sql.DB) error, wantSeq uint64) {
			kw, kp := base.fresh(t), base.fresh(t)
			errW, errP := run(kw, db), run(kp, plain)
			if (errW == nil) != (errP == nil) || (errW != nil && errW.Error() != errP.Error()) {
				t.Fatalf("%s: wire pool: %v, plain pool: %v", name, errW, errP)
			}
			if st := kw.cache.Stats(); st.WirePipelines != 0 || st.SequentialPipelines != wantSeq {
				t.Fatalf("%s: stats = %+v", name, st)
			}
		}

		type tree struct{ A int }
		both("NaN", func(k *kv, db *sql.DB) error {
			return k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.UpdAny(p, math.NaN(), int64(1)); return nil })
		}, 1)
		both("struct", func(k *kv, db *sql.DB) error {
			return k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.UpdAny(p, tree{1}, int64(1)); return nil })
		}, 1)
		both("snapshot isolation", func(k *kv, db *sql.DB) error {
			return k.tx(ctx, db, &sql.TxOptions{Isolation: sql.LevelSnapshot}, nil, func(p *runtime.Pipeline) error { k.Upd(p, 1, 1); return nil })
		}, 0) // the transaction cannot even begin on the sequential path
		both("far-away year", func(k *kv, db *sql.DB) error {
			return k.flush(ctx, db, func(p *runtime.Pipeline) { k.SetTS(p, time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC), 1) })
		}, 1)
		sameRows(t, base.rows(t, plain), orig)

		// a batch larger than the driver's maxAllowedPacket runs sequentially with identical results; one
		// that fits still runs on the wire
		cfg, err := mysql.ParseDSN(e.dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxAllowedPacket = 2048
		c, err := mysqlwire.Connector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		small := sql.OpenDB(c)
		defer small.Close()
		kw := base.fresh(t)
		err = kw.tx(ctx, small, nil, nil, func(p *runtime.Pipeline) error {
			for i := 0; i < 60; i++ {
				kw.Upd(p, 1, int64(1+i%3)).AssertFound()
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		stats(t, kw, 0, 1)
		sameRows(t, base.rows(t, plain), []row{{1, 30}, {2, 40}, {3, 50}})
		if err := kw.tx(ctx, small, nil, nil, func(p *runtime.Pipeline) error { kw.Upd(p, 1, 1); return nil }); err != nil {
			t.Fatal(err)
		}
		stats(t, kw, 1, 1)
		poolIdle(t, small)
	})
}

func TestTransactionOptionsOverTheWire(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)

		err := k.tx(ctx, db, &sql.TxOptions{ReadOnly: true}, nil, func(p *runtime.Pipeline) error { k.Upd(p, 1, 1); return nil })
		if mysqlNumber(err) != 1792 {
			t.Fatalf("a write in a read-only transaction: err = %v", err)
		}
		if err := k.tx(ctx, db, &sql.TxOptions{ReadOnly: true}, nil, func(p *runtime.Pipeline) error { k.Get(p, 1); return nil }); err != nil {
			t.Fatal(err)
		}

		isoVar := "@@transaction_isolation"
		if e.maria {
			isoVar = "@@tx_isolation"
		}
		// the isolation level a transaction actually runs with (read after the transaction did an InnoDB
		// read, which is when it shows up in innodb_trx)
		iso := &runtime.QueryDescriptor{
			Name: "Iso", StmtID: 9, StmtVerb: runtime.VerbSelect,
			StaticSQL: "SELECT trx_isolation_level FROM information_schema.innodb_trx WHERE trx_mysql_thread_id = CONNECTION_ID()",
			Segments:  []string{"SELECT trx_isolation_level FROM information_schema.innodb_trx WHERE trx_mysql_thread_id = CONNECTION_ID()"},
		}
		nap := &runtime.QueryDescriptor{Name: "Nap", StmtID: 10, StmtVerb: runtime.VerbSelect, StaticSQL: "SELECT SLEEP(0.15)", Segments: []string{"SELECT SLEEP(0.15)"}}
		for lvl, want := range map[sql.IsolationLevel]string{
			sql.LevelReadUncommitted: "READ UNCOMMITTED",
			sql.LevelReadCommitted:   "READ COMMITTED",
			sql.LevelRepeatableRead:  "REPEATABLE READ",
			sql.LevelSerializable:    "SERIALIZABLE",
		} {
			var got *runtime.RowFuture[string]
			err := k.tx(ctx, db, &sql.TxOptions{Isolation: lvl}, nil, func(p *runtime.Pipeline) error {
				k.Get(p, 1)
				// innodb_trx is a cache that is refreshed at most every 100ms: let it expire
				runtime.QueueRow[int64](p, 10, nap, runtime.RenderInput{}, runtime.MakeVariantKey(10, 0, 0, 0), nil, args(), nil,
					func(r *sql.Row, d *int64) error { return r.Scan(d) },
					func(r *sql.Rows, d *int64) error { return r.Scan(d) })
				got = runtime.QueueRow[string](p, 9, iso, runtime.RenderInput{}, runtime.MakeVariantKey(9, 0, 0, 0), nil, args(), nil,
					func(r *sql.Row, d *string) error { return r.Scan(d) },
					func(r *sql.Rows, d *string) error { return r.Scan(d) })
				return nil
			})
			if err != nil {
				t.Fatalf("isolation %v: %v", lvl, err)
			}
			if v, err := got.Get(); err != nil || v != want {
				t.Fatalf("isolation %v: in the batch the level is %q, %v; want %q", lvl, v, err, want)
			}
		}
		// SET TRANSACTION applies to the next transaction only: the session default is untouched
		db.SetMaxOpenConns(1)
		var level string
		if err := db.QueryRow("SELECT " + isoVar).Scan(&level); err != nil || level != "REPEATABLE-READ" {
			t.Fatalf("session isolation after the batches = %q, %v", level, err)
		}
		stats(t, k, 6, 0)
		poolIdle(t, db)
	})
}

func TestReturningOnMariaDB(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		ret := &runtime.QueryDescriptor{
			Name: "InsRet", StmtID: 8, StmtVerb: runtime.VerbInsert,
			StaticSQL:      "INSERT INTO " + k.table + " (id, v) VALUES (?, ?) RETURNING id, v",
			Segments:       []string{"INSERT INTO " + k.table + " (id, v) ", "", ", ", "", " RETURNING ", ""},
			Slots:          []runtime.Slot{{Kind: runtime.SlotInsertValuesOpen}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotInsertValuesClose}, {Kind: runtime.SlotSelectCols}},
			SelectColExprs: []string{"id", "v"},
		}
		var got *runtime.RowFuture[row]
		err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			got = runtime.QueueRow[row](p, 8, ret, runtime.RenderInput{}, runtime.MakeVariantKey(8, 0, 0, 0), nil, args(int64(10), int64(100)), nil,
				func(r *sql.Row, d *row) error { return r.Scan(&d.ID, &d.V) },
				func(r *sql.Rows, d *row) error { return r.Scan(&d.ID, &d.V) }).AssertRowsAffected(1)
			k.Upd(p, 1, 10)
			return nil
		})
		if !e.maria {
			// MySQL has no RETURNING: the batch cannot be expressed, runs sequentially, and the server
			// rejects the statement like it always did.
			if mysqlNumber(err) != 1064 {
				t.Fatalf("err = %v, want a syntax error", err)
			}
			if st := k.cache.Stats(); st.WirePipelines != 0 || st.SequentialPipelines != 1 {
				t.Fatalf("stats = %+v", st)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if v, err := got.Get(); err != nil || v != (row{10, 100}) {
			t.Fatalf("Get = %v, %v", v, err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 10}, {2, 20}, {3, 30}, {10, 101}})
		stats(t, k, 1, 0)
		poolIdle(t, db)
	})
}

func TestRawConnectionContract(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		upd := func(delta, id int64, assert int64) runtime.PipelineOp {
			return runtime.PipelineOp{Desc: k.upd, Args: []any{delta, id}, Decorators: runtime.OpDecorators{AssertRows: assert}}
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		err = conn.Raw(func(dc any) error {
			wp, ok := dc.(wire.Pipeliner)
			wtp, ok2 := dc.(wire.TxPipeliner)
			if !ok || !ok2 {
				return errors.New("mysqlwire connections must implement WirePipeliner and WireTxPipeliner")
			}
			// idle connection: ExecPipeline wraps the ops in a transaction of its own
			ops := []runtime.PipelineOp{upd(1, 1, 1), upd(1, 2, -1)}
			if err := wp.ExecPipeline(ctx, ops); err != nil {
				return err
			}
			if ops[0].RowCount != 1 || ops[1].RowCount != 1 {
				return fmt.Errorf("row counts = %d, %d", ops[0].RowCount, ops[1].RowCount)
			}
			// unsupported isolation and argument: rejected before anything is sent
			if err := wtp.ExecTxPipeline(ctx, &sql.TxOptions{Isolation: sql.LevelLinearizable}, []runtime.PipelineOp{upd(1, 1, -1)}); !errors.Is(err, runtime.ErrWireNotApplicable) {
				return fmt.Errorf("linearizable: %v", err)
			}
			bad := []runtime.PipelineOp{{Desc: k.upd, Args: []any{make(chan int), int64(1)}, Decorators: runtime.OpDecorators{AssertRows: -1}}}
			if err := wtp.ExecTxPipeline(ctx, nil, bad); !errors.Is(err, runtime.ErrWireNotApplicable) {
				return fmt.Errorf("chan argument: %v", err)
			}
			if err := wp.ExecPipeline(ctx, bad); !errors.Is(err, runtime.ErrWireNotApplicable) {
				return fmt.Errorf("chan argument: %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 21}, {3, 30}})

		// a server error is attributed to the operation that raised it, the batch is rolled back
		dup := []runtime.PipelineOp{upd(1, 1, -1), {Desc: k.ins, Args: []any{int64(1), int64(0)}, Decorators: runtime.OpDecorators{AssertRows: -1}}}
		err = conn.Raw(func(dc any) error {
			err := dc.(wire.TxPipeliner).ExecTxPipeline(ctx, nil, dup)
			if mysqlNumber(err) != 1062 || dup[0].Err != nil || mysqlNumber(dup[1].Err) != 1062 {
				return fmt.Errorf("err = %v, op errors = %v / %v", err, dup[0].Err, dup[1].Err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 21}, {3, 30}})

		// inside a transaction of the caller: ExecTxPipeline refuses, ExecPipeline runs in it
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = tx
		err = conn.Raw(func(dc any) error {
			if err := dc.(wire.TxPipeliner).ExecTxPipeline(ctx, nil, []runtime.PipelineOp{upd(1, 1, -1)}); !errors.Is(err, runtime.ErrWireNotApplicable) {
				return fmt.Errorf("ExecTxPipeline in a user transaction: %v", err)
			}
			ops := []runtime.PipelineOp{upd(1, 3, 1)}
			return dc.(wire.Pipeliner).ExecPipeline(ctx, ops)
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 21}, {3, 30}})
		// the connection is still perfectly usable
		var n int
		if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil || n != 1 {
			t.Fatalf("%d, %v", n, err)
		}
	})
}

var dialSeq atomic.Int64

// A transport error in a transaction of our own leaves the outcome unknown (the COMMIT may have been
// processed): the driver reports a *runtime.CommitError (RunPipelineTx unwraps it before the caller sees
// it, so this is tested on the connection).
func TestTransportErrorInOwnTransactionIsACommitError(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		cfg, err := mysql.ParseDSN(e.dsn)
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var conns []net.Conn
		name := fmt.Sprintf("mysqlwire_test_%d_%d", os.Getpid(), dialSeq.Add(1))
		mysql.RegisterDialContext(name, func(ctx context.Context, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err == nil {
				mu.Lock()
				conns = append(conns, c)
				mu.Unlock()
			}
			return c, err
		})
		cfg.Net = name
		c, err := mysqlwire.Connector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		cdb := sql.OpenDB(c)
		cdb.SetMaxOpenConns(1)
		defer cdb.Close()
		ctx := ctxT(t)
		conn, err := cdb.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		sleep := runtime.PipelineOp{
			Desc: &runtime.QueryDescriptor{Name: "Sleep", StmtVerb: runtime.VerbSelect, Segments: []string{"SELECT SLEEP(3)"}},
			ScanWireRows: func(rows *sql.Rows) (int64, error) {
				var n int64
				for rows.Next() {
					n++
				}
				return n, rows.Err()
			},
			HasResultCols: true,
			Decorators:    runtime.OpDecorators{AssertRows: -1},
		}
		ops := []runtime.PipelineOp{{Desc: k.upd, Args: []any{int64(1), int64(1)}, Decorators: runtime.OpDecorators{AssertRows: -1}}, sleep}
		// cut the network connection while the batch is (about to be) in flight: whether the write or the
		// read notices first, the answer is a transport error
		timer := time.AfterFunc(100*time.Millisecond, func() {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range conns {
				_ = c.Close()
			}
		})
		defer timer.Stop()
		err = conn.Raw(func(dc any) error {
			return dc.(wire.TxPipeliner).ExecTxPipeline(ctx, nil, ops)
		})
		var ce *runtime.CommitError
		if !errors.As(err, &ce) {
			t.Fatalf("err = %T %v, want *runtime.CommitError", err, err)
		}
		if mysqlNumber(err) != 0 {
			t.Fatalf("a transport error is not a server error: %v", err)
		}
		// nothing was committed (the server rolls back an unfinished transaction when the client is gone)
		deadline := time.Now().Add(10 * time.Second)
		for {
			got := k.rows(t, plain)
			if reflect.DeepEqual(got, orig) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("rows = %v after the connection was cut", got)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

func TestConnectorAndPlainBehaviour(t *testing.T) {
	if _, err := mysqlwire.OpenDB("user:pass@tcp(127.0.0.1:1)/db?parseTime=maybe"); err == nil {
		t.Fatal("OpenDB: expected a DSN error")
	}
	if _, err := mysqlwire.NewConnector("%%not a dsn"); err == nil {
		t.Fatal("NewConnector: expected a DSN error")
	}
	if _, err := mysqlwire.Connector(&mysql.Config{Net: "tcp", Addr: "x", TLSConfig: "no-such-config"}); err == nil {
		t.Fatal("Connector: expected a config error")
	}

	// no location configured: UTC (a bare Config is as good as one from a DSN)
	if _, err := mysqlwire.Connector(&mysql.Config{Net: "tcp", Addr: "127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}

	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)

		// the pool forces multiStatements; the plain pool does not have it
		if _, err := db.ExecContext(ctx, "DO 0; DO 0"); err != nil {
			t.Fatalf("multi-statement exec on the mysqlwire pool: %v", err)
		}
		if _, err := plain.ExecContext(ctx, "DO 0; DO 0"); mysqlNumber(err) != 1064 {
			t.Fatalf("multi-statement exec on the plain pool: %v", err)
		}

		// plain statements behave like the plain driver
		if _, err := db.ExecContext(ctx, "UPDATE "+k.table+" SET v = v + ? WHERE id = ?", 5, 1); err != nil {
			t.Fatal(err)
		}
		var v int64
		if err := db.QueryRowContext(ctx, "SELECT v FROM "+k.table+" WHERE id = ?", 1).Scan(&v); err != nil || v != 15 {
			t.Fatalf("v = %d, %v", v, err)
		}
		st, err := db.PrepareContext(ctx, "SELECT v FROM "+k.table+" WHERE id = ?")
		if err != nil {
			t.Fatal(err)
		}
		if err := st.QueryRowContext(ctx, 2).Scan(&v); err != nil || v != 20 {
			t.Fatalf("prepared v = %d, %v", v, err)
		}
		_ = st.Close()
		if _, err := db.ExecContext(ctx, "INSERT INTO "+k.table+" (id, v) VALUES (?, ?)", 1, 1); mysqlNumber(err) != 1062 {
			t.Fatalf("dup insert: %v", err)
		}

		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE "+k.table+" SET v = 0"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		tx, err = db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("UPDATE " + k.table + " SET v = v + 1 WHERE id = 3"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 15}, {2, 20}, {3, 31}})

		// a pipeline can follow a transaction on the same connection (the tracker is reset by Commit and
		// Rollback), and a pooled connection is reset between uses
		db.SetMaxOpenConns(1)
		if err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.Upd(p, 1, 3); return nil }); err != nil {
			t.Fatal(err)
		}
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = tx.Rollback()
		if err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.Upd(p, 1, 3); return nil }); err != nil {
			t.Fatal(err)
		}
		stats(t, k, 2, 0)
		sameRows(t, k.rows(t, plain), []row{{1, 15}, {2, 20}, {3, 33}})

		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
		if err := conn.Raw(func(dc any) error {
			if v, ok := dc.(driver.Validator); ok && !v.IsValid() {
				return errors.New("a live connection is not valid")
			}
			if p, ok := dc.(driver.Pinger); !ok || p.Ping(ctx) != nil {
				return errors.New("ping")
			}
			if r, ok := dc.(driver.SessionResetter); !ok || r.ResetSession(ctx) != nil {
				return errors.New("reset")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		poolIdle(t, db)
	})
}

func TestConnectorKeepsTheConfig(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		rome, err := time.LoadLocation("Europe/Rome")
		if err != nil {
			t.Skipf("no tz database: %v", err)
		}
		base := newKVTable(t, db)
		ctx := ctxT(t)

		cfg, err := mysql.ParseDSN(e.dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Loc = rome
		cfg.ParseTime = true
		cfg.Collation = "utf8mb4_unicode_ci"
		cfg.ClientFoundRows = true
		c, err := mysqlwire.Connector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MultiStatements {
			t.Fatal("Connector modified the caller's config")
		}
		cdb := sql.OpenDB(c)
		defer cdb.Close()
		k := base.fresh(t)

		// Loc: the time literal is rendered in the configured location, like a bound argument would be
		want := time.Date(2024, 6, 1, 12, 30, 15, 123456000, time.UTC)
		if err := k.flush(ctx, cdb, func(p *runtime.Pipeline) { k.SetTS(p, want, 1) }); err != nil {
			t.Fatal(err)
		}
		var viaWire, viaPrepared time.Time
		if err := cdb.QueryRowContext(ctx, "SELECT ts FROM "+k.table+" WHERE id = 1").Scan(&viaWire); err != nil {
			t.Fatal(err)
		}
		if !viaWire.Equal(want) {
			t.Fatalf("time written on the wire path reads back as %v, want the instant %v", viaWire, want)
		}
		if _, err := cdb.ExecContext(ctx, "UPDATE "+k.table+" SET ts = ? WHERE id = 2", want); err != nil {
			t.Fatal(err)
		}
		if err := cdb.QueryRowContext(ctx, "SELECT ts FROM "+k.table+" WHERE id = 2").Scan(&viaPrepared); err != nil || !viaPrepared.Equal(viaWire) {
			t.Fatalf("prepared path reads back %v (%v), wire path %v", viaPrepared, err, viaWire)
		}

		// Collation
		var coll string
		if err := cdb.QueryRowContext(ctx, "SELECT @@collation_connection").Scan(&coll); err != nil || coll != "utf8mb4_unicode_ci" {
			t.Fatalf("collation = %q, %v", coll, err)
		}

		// ClientFoundRows: ROW_COUNT() counts found rows, so a no-op update still has an affected row and
		// satisfies the assertion, exactly like the sequential path
		for _, db := range []*sql.DB{cdb} {
			var f *runtime.ExecFuture
			err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
				f = k.Upd(p, 0, 1).AssertRowsAffected(1)
				return nil
			})
			if err != nil {
				t.Fatalf("ClientFoundRows: %v", err)
			}
			if n, _ := f.RowsAffected(); n != 1 {
				t.Fatalf("ClientFoundRows: rows affected = %d", n)
			}
		}
		stats(t, k, 2, 0)
	})
}
