package pgxwire_test

// These tests run against real PostgreSQL-protocol servers and are skipped when no DSN is configured:
//
//	. ../scripts/engines-env.sh && go test -race ./...
//
// They drive runtime.Pipeline directly with hand-written descriptors (the integration module covers the
// generated-code path and the wire-vs-sequential differential on top of this driver).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/CAFxX/sqlgen/pgxwire"
	"github.com/CAFxX/sqlgen/runtime"
)

type testEngine struct {
	name, env string
	crdb      bool
}

var testEngines = []testEngine{
	{"postgres15", "SQLGEN_PG15_DSN", false},
	{"postgres18", "SQLGEN_PG18_DSN", false},
	{"cockroach", "SQLGEN_CRDB_DSN", true},
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
		dsn := dsnFor(e)
		if dsn == "" {
			continue
		}
		ran = true
		t.Run(e.name, func(t *testing.T) {
			db, err := pgxwire.OpenDB(dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			plain, err := sql.Open("pgx", dsn)
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
		t.Skip("no SQLGEN_PG15_DSN / SQLGEN_PG18_DSN / SQLGEN_CRDB_DSN configured")
	}
}

var tableSeq atomic.Int64

func newTable(t *testing.T, db *sql.DB, ddl string) string {
	t.Helper()
	name := fmt.Sprintf("pgxw_%d_%d", os.Getpid(), tableSeq.Add(1))
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

// ---- hand-written descriptors over a table (id BIGINT PRIMARY KEY, v BIGINT) ----

type kv struct {
	table               string
	upd, get, ins, many *runtime.QueryDescriptor
	cache               *runtime.StmtCache
}

func newKV(table string) *kv {
	k := &kv{table: table, cache: runtime.NewStmtCache()}
	k.upd = &runtime.QueryDescriptor{
		Name: "Upd", StmtID: 1, StmtVerb: runtime.VerbUpdate,
		StaticSQL: "UPDATE " + table + " SET v = v + $1 WHERE id = $2",
		Segments:  []string{"UPDATE " + table + " SET v = v + ", " WHERE ", "id = ", "", ""},
		Slots:     []runtime.Slot{{Kind: runtime.SlotScalar}, {Kind: runtime.SlotBranchGuardOpen}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotBranchGuardClose}},
	}
	k.get = &runtime.QueryDescriptor{
		Name: "Get", StmtID: 2, StmtVerb: runtime.VerbSelect,
		StaticSQL:      "SELECT id, v FROM " + table + " WHERE id = $1",
		Segments:       []string{"SELECT ", " FROM " + table + " WHERE id = ", ""},
		Slots:          []runtime.Slot{{Kind: runtime.SlotSelectCols}, {Kind: runtime.SlotScalar}},
		SelectColExprs: []string{"id", "v"},
	}
	k.ins = &runtime.QueryDescriptor{
		Name: "Ins", StmtID: 3, StmtVerb: runtime.VerbInsert,
		StaticSQL: "INSERT INTO " + table + " (id, v) VALUES ($1, $2)",
		Segments:  []string{"INSERT INTO " + table + " (id, v) ", "", ", ", "", ""},
		Slots: []runtime.Slot{{Kind: runtime.SlotInsertValuesOpen}, {Kind: runtime.SlotScalar, Cast: "bigint"},
			{Kind: runtime.SlotScalar, Cast: "bigint"}, {Kind: runtime.SlotInsertValuesClose}},
	}
	k.many = &runtime.QueryDescriptor{
		Name: "All", StmtID: 4, StmtVerb: runtime.VerbSelect,
		StaticSQL:      "SELECT id, v FROM " + table + " ORDER BY id",
		Segments:       []string{"SELECT ", " FROM " + table + " ORDER BY id"},
		Slots:          []runtime.Slot{{Kind: runtime.SlotSelectCols}},
		SelectColExprs: []string{"id", "v"},
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

func (k *kv) Ins(p *runtime.Pipeline, id, v int64) *runtime.ExecFuture {
	return p.QueueExec(3, k.ins, runtime.RenderInput{}, runtime.MakeVariantKey(3, 0, 0, 0), nil, args(id, v), nil)
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
	return runtime.RunPipelineTx(ctx, db, k.cache, nil, true, runtime.DialectDollar, txOpts, opts, build)
}

func (k *kv) flush(ctx context.Context, db *sql.DB, build func(p *runtime.Pipeline)) error {
	p := runtime.NewPipeline(ctx, db, k.cache, nil, true, runtime.DialectDollar)
	build(p)
	return p.Flush()
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
	k := newKV(newTable(t, db, "CREATE TABLE %s (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"))
	t.Cleanup(func() { _ = k.cache.Close() })
	for i := 1; i <= 3; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", k.table, i, i*10)); err != nil {
			t.Fatal(err)
		}
	}
	return k
}

func sameRows(t *testing.T, got, want []row) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
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

// ---- tests ----

func TestPipelineTxCommitsAndDeliversResults(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		var up1, up2 *runtime.ExecFuture
		var g *runtime.RowFuture[row]
		var all *runtime.SliceFuture[row]
		err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			up1 = k.Upd(p, -5, 1).AssertRowsAffected(1)
			up2 = k.Upd(p, 5, 2).AssertFound()
			g = k.Get(p, 3)
			all = k.All(p)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 5}, {2, 25}, {3, 30}})
		if n, err := up1.RowsAffected(); n != 1 || err != nil {
			t.Fatalf("up1 = %d, %v", n, err)
		}
		if n, err := up2.RowsAffected(); n != 1 || err != nil {
			t.Fatalf("up2 = %d, %v", n, err)
		}
		if v, err := g.Get(); err != nil || v != (row{3, 30}) {
			t.Fatalf("Get = %v, %v", v, err)
		}
		if vs, err := all.Get(); err != nil || len(vs) != 3 {
			t.Fatalf("All = %v, %v", vs, err)
		}
		if st := k.cache.Stats(); st.WirePipelines != 1 || st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v", st)
		}
		poolIdle(t, db)
	})
}

func TestIfElseAndAbortOverTheWire(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
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
		if !thenRes.Taken() || elseRes.Taken() {
			t.Fatalf("taken = %v / %v", thenRes.Taken(), elseRes.Taken())
		}
		if !skipped.Skipped() || taken.Skipped() {
			t.Fatalf("skipped = %v / %v", skipped.Skipped(), taken.Skipped())
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 20}, {3, 30}})

		// An Abort in the taken branch fires on the server and rolls everything back.
		stop := errors.New("stop here")
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
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 20}, {3, 30}})
		poolIdle(t, db)
	})
}

// Several conditions over one future are each recorded by the server and read back, so the branch gates
// of the second If are evaluated by the server like the first one's.
func TestSeveralConditionsOverOneFuture(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		var first, second, third *runtime.IfResult
		var skipped *runtime.ExecFuture
		err := k.tx(ctxT(t), db, nil, nil, func(p *runtime.Pipeline) error {
			hit := k.Get(p, 1)
			first = p.If(hit.Found(), func() { k.Upd(p, 1, 1) }, nil)
			second = p.If(hit.NotFound(), func() { skipped = k.Upd(p, 1000, 2) }, func() { k.Upd(p, 7, 3) })
			third = p.If(hit.RowsAffectedGe(1), func() { k.Upd(p, 3, 2) }, nil)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !first.Taken() || second.Taken() || !third.Taken() || !skipped.Skipped() {
			t.Fatalf("taken = %v %v %v, skipped = %v", first.Taken(), second.Taken(), third.Taken(), skipped.Skipped())
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 23}, {3, 37}})
	})
}

func TestAssertionAndServerErrorsRollBackAndKeepTheConnectionClean(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		db.SetMaxOpenConns(2)
		before := db.Stats().OpenConnections

		err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			k.Upd(p, 1, 1).AssertRowsAffected(1)
			k.Upd(p, 1, 99).AssertRowsAffected(1) // no such row
			k.Upd(p, 1, 2)
			return nil
		})
		var ae *runtime.AssertionError
		if !errors.Is(err, runtime.ErrAssertionFailed) || !errors.As(err, &ae) || ae.Statement != "Upd" || ae.Want != 1 {
			t.Fatalf("err = %v", err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 10}, {2, 20}, {3, 30}})

		// a server error in the middle of the batch (duplicate key) is reported as the server error
		err = k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error {
			k.Upd(p, 1, 1)
			k.Ins(p, 1, 0) // duplicate primary key
			k.Upd(p, 1, 2)
			return nil
		})
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("err = %v, want unique violation", err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 10}, {2, 20}, {3, 30}})

		// the failed transaction was rolled back and the very same connections still work
		if after := db.Stats().OpenConnections; after > before+1 {
			t.Fatalf("connections: %d -> %d", before, after)
		}
		if err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.Upd(p, 7, 1).AssertFound(); return nil }); err != nil {
			t.Fatalf("pipeline after failures: %v", err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 17}, {2, 20}, {3, 30}})
		poolIdle(t, db)
	})
}

func TestStandaloneFlushIsAtomic(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		if err := k.flush(ctx, db, func(p *runtime.Pipeline) { k.Upd(p, 1, 1); k.Upd(p, 1, 2) }); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 21}, {3, 30}})
		err := k.flush(ctx, db, func(p *runtime.Pipeline) { k.Upd(p, 1, 1); k.Ins(p, 2, 0); k.Upd(p, 1, 3) })
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("err = %v", err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 21}, {3, 30}})
		if st := k.cache.Stats(); st.WirePipelines != 2 || st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v", st)
		}
		poolIdle(t, db)
	})
}

func TestUserManagedTransactionFlushesOpsInOneBatch(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		tc := runtime.NewTxStmtCache(tx)
		p := runtime.NewPipeline(ctx, db, k.cache, tc, true, runtime.DialectDollar)
		k.Upd(p, 1, 1)
		g := k.Get(p, 1)
		if err := p.Flush(); err != nil {
			t.Fatal(err)
		}
		if v, _ := g.Get(); v != (row{1, 11}) {
			t.Fatalf("Get inside the tx = %v", v)
		}
		if !e.crdb { // CockroachDB readers block on the writer's intent instead of reading the old value
			sameRows(t, k.rows(t, plain), []row{{1, 10}, {2, 20}, {3, 30}}) // not visible before commit
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		sameRows(t, k.rows(t, plain), []row{{1, 11}, {2, 20}, {3, 30}})
		if st := k.cache.Stats(); st.WirePipelines != 1 || st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v", st)
		}

		// a failing batch inside a user transaction leaves rollback to the caller
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		p = runtime.NewPipeline(ctx, db, k.cache, runtime.NewTxStmtCache(tx), true, runtime.DialectDollar)
		k.Ins(p, 1, 0)
		if err := p.Flush(); err == nil {
			t.Fatal("expected a unique violation")
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		poolIdle(t, db)
	})
}

func TestCommitTimeErrorsAreMarkedAndReusable(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		if e.crdb {
			t.Skip("CockroachDB has no deferred constraints; commit-time restarts are covered by the contention test")
		}
		table := newTable(t, db, "CREATE TABLE %s (id BIGINT, CONSTRAINT %[1]s_u UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)")
		if _, err := db.Exec("INSERT INTO " + table + " VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		desc := &runtime.QueryDescriptor{
			Name: "DupInsert", StmtID: 9, StmtVerb: runtime.VerbInsert,
			StaticSQL: "INSERT INTO " + table + " (id) VALUES ($1)",
			Segments:  []string{"INSERT INTO " + table + " (id) ", "", "", ""},
			Slots:     []runtime.Slot{{Kind: runtime.SlotInsertValuesOpen}, {Kind: runtime.SlotScalar, Cast: "bigint"}, {Kind: runtime.SlotInsertValuesClose}},
		}
		cache := runtime.NewStmtCache()
		defer cache.Close()
		attempts := 0
		err := runtime.RunPipelineTx(ctxT(t), db, cache, nil, true, runtime.DialectDollar, nil,
			[]runtime.TxOption{runtime.WithTxRetry(runtime.TxRetryPolicy{MaxRetries: 3, BaseDelay: time.Millisecond})},
			func(p *runtime.Pipeline) error {
				attempts++
				p.QueueExec(9, desc, runtime.RenderInput{}, runtime.MakeVariantKey(9, 0, 0, 0), nil, args(int64(1)), nil)
				return nil
			})
		var pgErr *pgconn.PgError
		var ce *runtime.CommitError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("err = %v, want the deferred unique violation", err)
		}
		if errors.As(err, &ce) {
			t.Fatalf("the CommitError marker leaked to the caller")
		}
		if attempts != 1 {
			t.Fatalf("a permanent commit error was retried %d times", attempts)
		}
		var n int
		if err := plain.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("count = %d, %v", n, err)
		}
		poolIdle(t, db)
	})
}

func TestTransactionOptions(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		err := k.tx(ctx, db, &sql.TxOptions{ReadOnly: true}, nil, func(p *runtime.Pipeline) error { k.Upd(p, 1, 1); return nil })
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "25006" {
			t.Fatalf("a write in a read-only transaction: err = %v", err)
		}
		if err := k.tx(ctx, db, &sql.TxOptions{ReadOnly: true}, nil, func(p *runtime.Pipeline) error { k.Get(p, 1); return nil }); err != nil {
			t.Fatal(err)
		}
		for _, lvl := range []sql.IsolationLevel{sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSnapshot, sql.LevelSerializable} {
			if err := k.tx(ctx, db, &sql.TxOptions{Isolation: lvl}, nil, func(p *runtime.Pipeline) error { k.Get(p, 1); return nil }); err != nil {
				t.Fatalf("isolation %v: %v", lvl, err)
			}
		}
		err = k.tx(ctx, db, &sql.TxOptions{Isolation: sql.LevelLinearizable}, nil, func(p *runtime.Pipeline) error { k.Get(p, 1); return nil })
		if err == nil {
			t.Fatal("an isolation level PostgreSQL does not have must be rejected")
		}
		poolIdle(t, db)
	})
}

func TestCanceledContextDiscardsTheConnection(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := k.tx(ctx, db, nil, nil, func(p *runtime.Pipeline) error { k.Upd(p, 1, 1); return nil })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
		// a context that dies while the batch is in flight
		ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel2()
		err = k.tx(ctx2, db, nil, nil, func(p *runtime.Pipeline) error {
			p.QueueExec(5, sleepDesc(), runtime.RenderInput{}, runtime.MakeVariantKey(5, 0, 0, 0), nil, args(), nil)
			return nil
		})
		if err == nil {
			t.Fatal("expected the in-flight batch to be canceled")
		}
		var ce *runtime.CommitError
		if errors.As(err, &ce) {
			t.Fatalf("CommitError leaked: %v", err)
		}
		poolIdle(t, db)
		sameRows(t, k.rows(t, plain), []row{{1, 10}, {2, 20}, {3, 30}})
	})
}

func sleepDesc() *runtime.QueryDescriptor {
	return &runtime.QueryDescriptor{
		Name: "Sleep", StmtID: 5, StmtVerb: runtime.VerbSelect,
		StaticSQL: "SELECT pg_sleep(5)::text",
		Segments:  []string{"SELECT pg_sleep(5)::text"},
	}
}

func TestRawConnectionContract(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ctx := ctxT(t)
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		err = conn.Raw(func(dc any) error {
			wp, ok := dc.(runtime.WirePipeliner)
			wtp, ok2 := dc.(runtime.WireTxPipeliner)
			if !ok || !ok2 {
				return errors.New("pgxwire connections must implement both wire interfaces")
			}
			// unsupported isolation is rejected before anything is sent
			if err := wtp.ExecTxPipeline(ctx, &sql.TxOptions{Isolation: sql.LevelLinearizable}, nil); err == nil {
				return errors.New("expected an unsupported-isolation error")
			}
			// an argument pgx cannot encode fails before anything is sent, in both entry points
			ops := []runtime.PipelineOp{{
				SQL:        "UPDATE " + k.table + " SET v = v + $1 WHERE id = $2",
				Args:       []any{make(chan int), int64(1)},
				Decorators: runtime.OpDecorators{AssertRows: -1},
			}}
			if err := wtp.ExecTxPipeline(ctx, nil, ops); err == nil {
				return errors.New("expected an encoding error")
			}
			if err := wp.ExecPipeline(ctx, ops); err == nil {
				return errors.New("expected an encoding error")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// the connection is still perfectly usable
		var n int
		if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil || n != 1 {
			t.Fatalf("%d, %v", n, err)
		}
	})
}

func TestConnectorOptionsAndOpenDBErrors(t *testing.T) {
	if _, err := pgxwire.OpenDB("postgres://%zz"); err == nil {
		t.Fatal("expected a parse error")
	}
	cfg, err := pgx.ParseConfig("postgres://u@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	var called atomic.Bool
	c := pgxwire.Connector(cfg, stdlib.OptionBeforeConnect(func(context.Context, *pgx.ConnConfig) error {
		called.Store(true)
		return nil
	}))
	if c.Driver() == nil {
		t.Fatal("Driver() must not be nil")
	}
	if _, err := c.Connect(context.Background()); err == nil {
		t.Fatal("nothing listens on port 1")
	}
	if !called.Load() {
		t.Fatal("stdlib options must be forwarded")
	}
	var _ driver.Connector = c
}
