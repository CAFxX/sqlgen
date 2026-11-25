package integration

// Value forwarding (a parameter declared :name:Arg[T] fed by an earlier operation's RowFuture.Arg or
// ExecFuture.LastInsertIDArg) must behave on the single-round-trip paths exactly as on the sequential
// fallback: the same stored rows, the same values, the same *runtime.ArgError (operation, producer, row
// count), on every engine, in one network round trip.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen/integration/mdbq"
	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

// argErrKind renders an *runtime.ArgError for the outcome comparisons (errKind, myErrKind): every field is
// part of the contract on both paths.
func argErrKind(err error) (string, bool) {
	var ae *runtime.ArgError
	if !errors.As(err, &ae) {
		return "", false
	}
	return fmt.Sprintf("ErrArgUnresolved(%s op=%d label=%q producer=%s pop=%d plabel=%q got=%d known=%v lid=%v)",
		ae.Statement, ae.Op, ae.Label, ae.Producer, ae.ProducerOp, ae.ProducerLabel, ae.Got, ae.GotKnown, ae.LastInsertID), true
}

func fmtExec(f *runtime.ExecFuture) string {
	n, err := f.RowsAffected()
	return fmt.Sprintf("%d|%v", n, err)
}

// Forwarded values keep a PipelineTx at one network round trip (wire_roundtrip_test.go,
// wire_my_roundtrip_test.go); the sequential path pays BEGIN + one turn per statement + COMMIT.
var forwardWorkload = workload{"forwarded values", 6, func(ctx context.Context, q *pgq.Queries) error {
	return q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
		s := p.FwdScoreOf(2)
		p.FwdSetScore(s.Arg(), runtime.Val(int64(1))).AssertRows(1)
		n := p.CountOrgItems(1)
		p.FwdItemsAbove(n.Arg())
		return nil
	})
}}

var myForwardWorkload = myWorkload{"forwarded values", 6, func(ctx context.Context, q *myq.Queries) error {
	return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
		s := p.FwdScoreOf(2)
		p.FwdSetScore(s.Arg(), runtime.Val(int64(1))) // MySQL counts a row set to its own value as unaffected
		n := p.CountOrgItems(1)
		p.FwdItemsAbove(n.Arg())
		return nil
	})
}}

// ---- PostgreSQL protocol (pgq): the matrix of wire_pg_test.go ----------------------------------------

var forwardScenarios = []scenario{
	{"forward a selected value", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(2)
		u := p.FwdSetScore(s.Arg(), runtime.Val(int64(1))).AssertRows(1)
		return nil, func() string { return fmtRow(s) + "#" + fmtExec(u) }
	}},
	{"forward a RETURNING id into an update and an insert", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		r := p.FwdInsertReturningID(10, 1, "r")
		p.FwdSetScore(runtime.Val(int64(77)), r.Arg()).AssertRows(1)
		p.FwdInsertItem(runtime.Val(int64(11)), "copy", r.Arg()).AssertRows(1)
		return nil, func() string { return fmtRow(r) }
	}},
	{"forward a COUNT into a read", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		n := p.CountOrgItems(1)
		above := p.FwdItemsAbove(n.Arg())
		return nil, func() string { return fmtRow(n) + "#" + fmtSlice(above) }
	}},
	{"zero rows: the consumer fails and the batch rolls back", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1)
		s := p.FwdScoreOf(99).Label("missing")
		p.FwdSetScore(s.Arg(), runtime.Val(int64(1))).Label("consumer")
		p.AddScoreOne(1, 2)
		return nil, nil
	}},
	{"several rows: the consumer fails with the count", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		ids := p.FwdIDsByStatus("new")
		p.FwdSetScore(runtime.Val(int64(5)), ids.Arg())
		return nil, nil
	}},
	{"the first unresolved value in argument order is reported", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		a := p.FwdScoreOf(99)
		b := p.FwdIDsByStatus("new")
		p.FwdSetScore(b.Arg(), a.Arg())
		return nil, nil
	}},
	{"a consumer in an untaken branch is silent", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(99)
		r := p.If(s.Found(),
			func(p *pgq.Pipeline) { p.FwdSetScore(s.Arg(), runtime.Val(int64(1))) },
			func(p *pgq.Pipeline) { p.AddScoreOne(1, 2) })
		return []*runtime.IfResult{r}, nil
	}},
	{"a consumer in a taken nested branch (gated insert)", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(3)
		var r2 *runtime.IfResult
		r1 := p.If(s.Found(), func(p *pgq.Pipeline) {
			g := p.GetItem(4)
			r2 = p.If(g.Found(), func(p *pgq.Pipeline) { p.FwdInsertItem(runtime.Val(int64(12)), "nested", s.Arg()).AssertRows(1) }, nil)
		}, nil)
		return []*runtime.IfResult{r1, r2}, nil
	}},
	{"an unresolved value in a taken branch fails", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(99)
		r := p.If(s.NotFound(), func(p *pgq.Pipeline) { p.FwdSetScore(s.Arg(), runtime.Val(int64(1))) }, nil)
		return []*runtime.IfResult{r}, nil
	}},
	{"a failed assertion on the producer comes first", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(99).AssertFound()
		p.FwdSetScore(s.Arg(), runtime.Val(int64(1)))
		return nil, nil
	}},
	{"an assertion and a condition on the consumer", func(p *pgq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(2)
		c := p.FwdSetScore(runtime.Val(int64(5)), s.Arg()) // id 20: no such row
		r := p.If(c.Found(), func(p *pgq.Pipeline) { p.AddScoreOne(100, 1) }, func(p *pgq.Pipeline) { p.AddScoreOne(1000, 1) })
		p.FwdSetScore(runtime.Val(int64(1)), s.Arg()).AssertRows(1)
		return []*runtime.IfResult{r}, nil
	}},
}

func TestRealEngineForwardWireMatchesSequential(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol; the MySQL protocol is TestRealEngineMySQLForwardWireMatchesSequential")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, mode := range runModes {
			for _, sc := range forwardScenarios {
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					compareWireSeq(t, db, dsn, mode, sc)
				})
			}
		}
	})
}

// ---- MySQL protocol (myq; mdbq for RETURNING) ----------------------------------------------------------

var myForwardScenarios = []myScenario{
	{"forward a selected value", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(2)
		u := p.FwdSetScore(s.Arg(), runtime.Val(int64(1))).AssertRows(1)
		return nil, func() string { return myFmtRow(s) + "#" + fmtExec(u) }
	}},
	{"forward a COUNT into a read", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		n := p.CountOrgItems(1)
		above := p.FwdItemsAbove(n.Arg())
		return nil, func() string { return myFmtRow(n) + "#" + myFmtSlice(above) }
	}},
	{"zero rows: the consumer fails and the batch rolls back", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1)
		s := p.FwdScoreOf(99).Label("missing")
		p.FwdSetScore(s.Arg(), runtime.Val(int64(1))).Label("consumer")
		p.AddScoreOne(1, 2)
		return nil, nil
	}},
	{"several rows: the consumer fails with the count", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		ids := p.FwdIDsByStatus("new")
		p.FwdSetScore(runtime.Val(int64(5)), ids.Arg())
		return nil, nil
	}},
	{"the first unresolved value in argument order is reported", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		a := p.FwdScoreOf(99)
		b := p.FwdIDsByStatus("new")
		p.FwdSetScore(b.Arg(), a.Arg())
		return nil, nil
	}},
	{"a consumer in an untaken branch is silent", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(99)
		r := p.If(s.Found(),
			func(p *myq.Pipeline) { p.FwdSetScore(s.Arg(), runtime.Val(int64(1))) },
			func(p *myq.Pipeline) { p.AddScoreOne(1, 2) })
		return []*runtime.IfResult{r}, nil
	}},
	{"a consumer in a taken nested branch (gated insert)", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(3)
		var r2 *runtime.IfResult
		r1 := p.If(s.Found(), func(p *myq.Pipeline) {
			g := p.GetItem(4)
			r2 = p.If(g.Found(), func(p *myq.Pipeline) { p.FwdInsertItem(runtime.Val(int64(12)), "nested", s.Arg()).AssertRows(1) }, nil)
		}, nil)
		return []*runtime.IfResult{r1, r2}, nil
	}},
	{"an unresolved value in a taken branch fails", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(99)
		r := p.If(s.NotFound(), func(p *myq.Pipeline) { p.FwdSetScore(s.Arg(), runtime.Val(int64(1))) }, nil)
		return []*runtime.IfResult{r}, nil
	}},
	{"a failed assertion on the producer comes first", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(99).AssertFound()
		p.FwdSetScore(s.Arg(), runtime.Val(int64(1)))
		return nil, nil
	}},
	{"an assertion and a condition on the consumer", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(2)
		c := p.FwdSetScore(runtime.Val(int64(5)), s.Arg()) // id 20: no such row
		r := p.If(c.Found(), func(p *myq.Pipeline) { p.AddScoreOne(100, 1) }, func(p *myq.Pipeline) { p.AddScoreOne(1000, 1) })
		p.FwdSetScore(runtime.Val(int64(1)), s.Arg()).AssertRows(1)
		return []*runtime.IfResult{r}, nil
	}},
	{"a value forwarded into a condition's column predicate", func(p *myq.Pipeline) ([]*runtime.IfResult, func() string) {
		s := p.FwdScoreOf(2)
		g := p.GetItem(3)
		r := p.If(g.WhereCol("score > ?", int64(25)),
			func(p *myq.Pipeline) { p.FwdSetScore(s.Arg(), runtime.Val(int64(4))) }, nil)
		return []*runtime.IfResult{r}, nil
	}},
}

func TestRealEngineMySQLForwardWireMatchesSequential(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL protocol; the PostgreSQL protocol is TestRealEngineForwardWireMatchesSequential")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, mode := range myRunModes {
			for _, sc := range myForwardScenarios {
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					compareMyWireSeq(t, db, dsn, mode, sc)
				})
			}
		}
	})
}

// fwdCompare runs fn on the sequential baseline and on the wire path, each on a fresh pool after reset,
// and fails unless both report the same outcome (fn renders it) and each ran on its own path.
func fwdCompare(t *testing.T, open func(wire bool) *sql.DB, reset func(), fn func(db *sql.DB) (outcome string, wire, seq uint64)) {
	t.Helper()
	var got [2]string
	for i, w := range []bool{false, true} {
		reset()
		o, wp, sp := fn(open(w))
		got[i] = o
		if (w && (wp != 1 || sp != 0)) || (!w && (wp != 0 || sp != 1)) {
			t.Fatalf("wire=%v: CacheStats{Wire,Sequential} = %d, %d", w, wp, sp)
		}
	}
	t.Logf("outcome: %s", got[0])
	if got[0] != got[1] {
		t.Fatalf("wire pipeline diverges from sequential fallback:\n  sequential %s\n  wire       %s", got[0], got[1])
	}
}

// AUTO_INCREMENT ids (LastInsertIDArg): resolved when the INSERT affected one row and generated an id.
func TestRealEngineForwardLastInsertID(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("the $N dialect has no generated-id result (RETURNING + Arg instead)")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		mustExec(t, db, "DROP TABLE IF EXISTS sg_fwd_auto")
		mustExec(t, db, "CREATE TABLE sg_fwd_auto (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT NOT NULL)")
		t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS sg_fwd_auto") })
		reset := func() {
			mustExec(t, db, "DELETE FROM sg_items")
			mustExec(t, db, "TRUNCATE TABLE sg_fwd_auto") // restarts AUTO_INCREMENT: both paths see the same ids
			mustExec(t, db, "INSERT INTO sg_fwd_auto (id, v) VALUES (100, 0)")
			mustExec(t, db, "INSERT INTO sg_items VALUES (1, 1, 'n1', 10, 'new')")
		}
		run := func(wdb *sql.DB, build func(p *myq.Pipeline)) (string, uint64, uint64) {
			q := myq.New(wdb)
			defer q.Close()
			err := q.PipelineTx(ctxT(t), nil, func(p *myq.Pipeline) error { build(p); return nil })
			st := q.CacheStats()
			return myErrKind(err) + " " + mySnapshot(t, db), st.WirePipelines, st.SequentialPipelines
		}
		// The documented difference: an INSERT that sets the AUTO_INCREMENT column itself generates no id.
		// The server's LAST_INSERT_ID() (the single-round-trip path) does not change, so the consumer fails;
		// the driver's LastInsertId (the sequential path) reports the explicit value.
		explicit := func(p *myq.Pipeline) {
			id := p.FwdAutoInsertIgnore(200, 1).LastInsertIDArg()
			p.FwdInsertItem(id, "explicit", runtime.Val(int64(1)))
		}
		reset()
		seq, _, _ := run(openMyWire(t, dsn, false), explicit)
		reset()
		wire, _, _ := run(openMyWire(t, dsn, true), explicit)
		if !strings.HasPrefix(seq, " ") || !strings.HasPrefix(wire, "ErrArgUnresolved(FwdInsertItem op=1") {
			t.Errorf("explicit AUTO_INCREMENT value: sequential %q, wire %q", seq, wire)
		}
		for _, sc := range []struct {
			name  string
			build func(p *myq.Pipeline)
		}{
			{"generated id", func(p *myq.Pipeline) {
				id := p.FwdAutoInsert(7).AssertRows(1).LastInsertIDArg()
				p.FwdInsertItem(id, "auto", runtime.Val(int64(1))).AssertRows(1)
				p.FwdSetScore(id, runtime.Val(int64(1))).AssertRows(1)
			}},
			{"INSERT IGNORE that inserted nothing", func(p *myq.Pipeline) {
				p.FwdAutoInsert(7) // a generated id that must not leak into the next operation
				id := p.FwdAutoInsertIgnore(100, 1).Label("ignored").LastInsertIDArg()
				p.FwdInsertItem(id, "never", runtime.Val(int64(1)))
			}},
			{"a multi-row INSERT", func(p *myq.Pipeline) {
				id := p.FwdAutoInsertTwo(1, 2).LastInsertIDArg()
				p.FwdInsertItem(id, "never", runtime.Val(int64(1)))
			}},
			{"in a branch", func(p *myq.Pipeline) {
				g := p.GetItem(1)
				p.If(g.Found(), func(p *myq.Pipeline) {
					id := p.FwdAutoInsert(8).LastInsertIDArg()
					p.If(g.Found(), func(p *myq.Pipeline) { p.FwdInsertItem(id, "branch", runtime.Val(int64(2))).AssertRows(1) }, nil)
				}, nil)
			}},
		} {
			t.Run(strings.ReplaceAll(sc.name, " ", "_"), func(t *testing.T) {
				fwdCompare(t, func(w bool) *sql.DB { return openMyWire(t, dsn, w) }, reset, func(wdb *sql.DB) (string, uint64, uint64) {
					return run(wdb, sc.build)
				})
			})
		}
	})
}

// MariaDB INSERT ... RETURNING as a producer (the forwarded value is one more item of the RETURNING list).
func TestRealEngineForwardMariaDBReturning(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if e.family != famMariaDB {
			t.Skip("MariaDB only (RETURNING on INSERT)")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		reset := func() {
			mustExec(t, db, "DELETE FROM sg_items")
			mustExec(t, db, "INSERT INTO sg_items VALUES (1, 1, 'n1', 10, 'new')")
		}
		for _, sc := range []struct {
			name  string
			build func(p *mdbq.Pipeline) func() string
		}{
			{"RETURNING id", func(p *mdbq.Pipeline) func() string {
				r := p.FwdInsertReturningID(10, 1, "r")
				p.FwdSetScore(runtime.Val(int64(77)), r.Arg()).AssertRows(1)
				p.FwdInsertItem(runtime.Val(int64(11)), "copy", r.Arg()).AssertRows(1)
				return func() string { return myFmtRow(r) }
			}},
			{"gated RETURNING that inserted nothing", func(p *mdbq.Pipeline) func() string {
				g := p.GetItem(99)
				p.If(g.NotFound(), func(p *mdbq.Pipeline) {
					r := p.FwdInsertReturningID(1, 1, "dup") // duplicate key: a database error on both paths
					p.FwdSetScore(runtime.Val(int64(77)), r.Arg())
				}, nil)
				return nil
			}},
		} {
			t.Run(strings.ReplaceAll(sc.name, " ", "_"), func(t *testing.T) {
				fwdCompare(t, func(w bool) *sql.DB { return openMyWire(t, dsn, w) }, reset, func(wdb *sql.DB) (string, uint64, uint64) {
					q := mdbq.New(wdb)
					defer q.Close()
					var report func() string
					err := q.PipelineTx(ctxT(t), nil, func(p *mdbq.Pipeline) error { report = sc.build(p); return nil })
					o := myErrKind(err) + " " + mySnapshot(t, db)
					if report != nil {
						o += " " + report()
					}
					st := q.CacheStats()
					return o, st.WirePipelines, st.SequentialPipelines
				})
			})
		}
	})
}

// ---- type fidelity: one producer per type, one consumer, both paths, every engine ------------------------

const (
	fwdPgSrcDDL = `CREATE TABLE %s (k TEXT PRIMARY KEY, i8 BIGINT, i8n BIGINT, s TEXT, ts TIMESTAMPTZ, tsl TIMESTAMP,
		b BYTEA, bo BOOLEAN, f8 DOUBLE PRECISION, u UUID)`
	fwdMySrcDDL = `CREATE TABLE %s (k VARCHAR(16) PRIMARY KEY, i8 BIGINT, i8n BIGINT NULL, s VARCHAR(200) CHARACTER SET utf8mb4,
		ts DATETIME(6), tsl TIMESTAMP(6) NULL, b VARBINARY(64), bo BOOLEAN, f8 DOUBLE, u CHAR(36))`
	fwdPgText = `SELECT k, i8::text, COALESCE(i8n::text, 'NULL'), s, ts::text, tsl::text, encode(b, 'hex'), bo::text, f8::text, u::text FROM %s ORDER BY k`
	fwdMyText = `SELECT k, CAST(i8 AS CHAR), COALESCE(CAST(i8n AS CHAR), 'NULL'), HEX(s), CAST(ts AS CHAR), CAST(tsl AS CHAR), HEX(b), CAST(bo AS CHAR), CAST(f8 AS CHAR), u FROM %s ORDER BY k`
)

type fwdRow struct {
	k, i8, i8n, s, ts, tsl, b, bo, f8, u string // SQL literals
}

var fwdPgRows = []fwdRow{
	{"a", "-9223372036854775808", "NULL", "''", "'1970-01-01 00:00:00+00'", "'2024-11-03 01:30:00'", `'\x00'`, "true", "0.1", "'00000000-0000-0000-0000-000000000000'"},
	{"b", "9223372036854775807", "5", `'it''s \ back"slash -- ünïcödé 😀'`, "'2024-03-10 02:30:00.123456-05'", "'0001-01-01 00:00:00'", `'\x5c27000aff'`, "false", "1e-300", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"},
	{"c", "0", "-1", "'x'", "'9999-12-31 23:59:59.999999+00'", "'9999-12-31 23:59:59.999999'", `'\x'`, "true", "-1.7976931348623157e308", "'ffffffff-ffff-ffff-ffff-ffffffffffff'"},
	{"d", "42", "NULL", "'tab\tnl\n'", "'2000-02-29 12:00:00.5+05:30'", "'2000-02-29 12:00:00.5'", `'\xdeadbeef'`, "false", "5e-324", "'123e4567-e89b-12d3-a456-426614174000'"},
}

var fwdMyRows = []fwdRow{
	{"a", "-9223372036854775808", "NULL", "''", "'1000-01-01 00:00:00'", "'1970-01-01 00:00:01'", "0x00", "1", "0.1", "'00000000-0000-0000-0000-000000000000'"},
	{"b", "9223372036854775807", "5", `'it''s \\ back"slash -- ünïcödé 😀'`, "'2024-03-10 02:30:00.123456'", "'2038-01-19 03:14:07.999999'", "0x5c27000aff", "0", "1e-300", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"},
	{"c", "0", "-1", "'x'", "'9999-12-31 23:59:59.999999'", "'2024-11-03 01:30:00'", "''", "1", "-1.7976931348623157e308", "'ffffffff-ffff-ffff-ffff-ffffffffffff'"},
	{"d", "42", "NULL", "'tab\tnl\n'", "'2000-02-29 12:00:00.5'", "'2000-02-29 12:00:00.5'", "0xdeadbeef", "0", "4.9e-324", "'123e4567-e89b-12d3-a456-426614174000'"},
}

func fwdTable(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, 10)
		ptrs := make([]any, 10)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		var sb strings.Builder
		for _, v := range vals {
			fmt.Fprintf(&sb, "%q|", v.String)
		}
		out = append(out, sb.String())
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestRealEngineForwardTypeFidelity forwards every common Go type from a source row to a destination row,
// in one pipeline, on both paths, and requires the destination to equal the source (as the engine renders
// it as text) on both paths. PostgreSQL-protocol engines run it in the default session and under a
// non-UTC TimeZone (the forwarded text is cast back in the session's time zone).
func TestRealEngineForwardTypeFidelity(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		dsn := os.Getenv(e.env)
		srcDDL, text, rows := fwdMySrcDDL, fwdMyText, fwdMyRows
		variants := []string{"parseTime=true"}
		if e.isPG() {
			srcDDL, text, rows = fwdPgSrcDDL, fwdPgText, fwdPgRows
			variants = []string{"", "timezone=America/New_York"}
		}
		for _, tbl := range []string{"sg_fwd_src", "sg_fwd_dst"} {
			mustExec(t, db, "DROP TABLE IF EXISTS "+tbl)
			mustExec(t, db, fmt.Sprintf(srcDDL, tbl))
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + tbl) })
		}
		for _, r := range rows {
			mustExec(t, db, fmt.Sprintf("INSERT INTO sg_fwd_src VALUES ('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s)", r.k, r.i8, r.i8n, r.s, r.ts, r.tsl, r.b, r.bo, r.f8, r.u))
		}
		src := fwdTable(t, db, fmt.Sprintf(text, "sg_fwd_src"))
		dst := fmt.Sprintf(text, "sg_fwd_dst")
		for _, variant := range variants {
			vdsn := dsn
			if variant != "" {
				sep := "?"
				if strings.Contains(dsn, "?") {
					sep = "&"
				}
				vdsn += sep + variant
			}
			fwdCompare(t, func(w bool) *sql.DB {
				if e.isPG() {
					return openWire(t, vdsn, w)
				}
				return openMyWire(t, vdsn, w)
			}, func() { mustExec(t, db, "DELETE FROM sg_fwd_dst") }, func(wdb *sql.DB) (string, uint64, uint64) {
				var err error
				var st runtime.CacheStats
				if e.isPG() {
					q := pgq.New(wdb)
					err = q.PipelineTx(ctxT(t), nil, func(p *pgq.Pipeline) error {
						for _, r := range rows {
							k := r.k
							p.FwdDst(k, p.FwdSrcI8(k).Arg(), p.FwdSrcI8N(k).Arg(), p.FwdSrcS(k).Arg(), p.FwdSrcTS(k).Arg(), p.FwdSrcTSL(k).Arg(),
								p.FwdSrcB(k).Arg(), p.FwdSrcBo(k).Arg(), p.FwdSrcF8(k).Arg(), p.FwdSrcU(k).Arg()).AssertRows(1)
						}
						return nil
					})
					st = q.CacheStats()
					_ = q.Close()
				} else {
					q := myq.New(wdb)
					err = q.PipelineTx(ctxT(t), nil, func(p *myq.Pipeline) error {
						for _, r := range rows {
							k := r.k
							p.FwdDst(k, p.FwdSrcI8(k).Arg(), p.FwdSrcI8N(k).Arg(), p.FwdSrcS(k).Arg(), p.FwdSrcTS(k).Arg(), p.FwdSrcTSL(k).Arg(),
								p.FwdSrcB(k).Arg(), p.FwdSrcBo(k).Arg(), p.FwdSrcF8(k).Arg(), p.FwdSrcU(k).Arg()).AssertRows(1)
						}
						return nil
					})
					st = q.CacheStats()
					_ = q.Close()
				}
				if err != nil {
					t.Fatalf("%s: %v", variant, err)
				}
				got := fwdTable(t, db, dst)
				if strings.Join(got, "\n") != strings.Join(src, "\n") {
					t.Errorf("%s (wire=%v): forwarded values differ from the source:\n  source %q\n  stored %q", variant, st.WirePipelines > 0, src, got)
				}
				return strings.Join(got, ";"), st.WirePipelines, st.SequentialPipelines
			})
		}
	})
}

// In a transaction the caller began, an unresolved forwarded value leaves the transaction usable on both
// paths: the operations before the consumer stay, the consumer did not run.
func TestRealEngineForwardUserTxStaysUsable(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		for _, wire := range []bool{false, true} {
			mustExec(t, db, "DELETE FROM sg_items")
			mustExec(t, db, "INSERT INTO sg_items VALUES (1, 1, 'n1', 10, 'new')")
			var wdb *sql.DB
			var flush func(tx *sql.Tx) error
			if e.isPG() {
				wdb = openWire(t, dsn, wire)
				q := pgq.New(wdb)
				defer q.Close()
				flush = func(tx *sql.Tx) error {
					p := q.WithTx(tx).Pipeline(ctxT(t))
					p.AddScoreOne(5, 1).AssertRows(1)
					s := p.FwdScoreOf(99)
					p.FwdSetScore(s.Arg(), runtime.Val(int64(1))).AssertRows(1)
					return p.Flush()
				}
			} else {
				wdb = openMyWire(t, dsn, wire)
				q := myq.New(wdb)
				defer q.Close()
				flush = func(tx *sql.Tx) error {
					p := q.WithTx(tx).Pipeline(ctxT(t))
					p.AddScoreOne(5, 1).AssertRows(1)
					s := p.FwdScoreOf(99)
					p.FwdSetScore(s.Arg(), runtime.Val(int64(1))).AssertRows(1)
					return p.Flush()
				}
			}
			tx, err := wdb.BeginTx(ctxT(t), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := flush(tx); !errors.Is(err, runtime.ErrArgUnresolved) {
				_ = tx.Rollback()
				t.Fatalf("wire=%v: %v", wire, err)
			}
			var score int64
			if err := tx.QueryRowContext(ctxT(t), "SELECT score FROM sg_items WHERE id = 1").Scan(&score); err != nil || score != 15 {
				_ = tx.Rollback()
				t.Fatalf("wire=%v: the transaction must stay usable with the earlier write: %d, %v", wire, score, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("wire=%v: commit: %v", wire, err)
			}
		}
	})
}
