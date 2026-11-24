package pgxwire_test

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen/runtime"
	"github.com/CAFxX/sqlgen/runtime/wire"
)

// runOps sends ops through the connection's pipeline entry points exactly as the runtime does.
func runOps(t *testing.T, db *sql.DB, tx bool, ops []runtime.PipelineOp) error {
	t.Helper()
	ctx := ctxT(t)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var out error
	if err := conn.Raw(func(dc any) error {
		if tx {
			out = dc.(wire.TxPipeliner).ExecTxPipeline(ctx, nil, ops)
		} else {
			out = dc.(wire.Pipeliner).ExecPipeline(ctx, ops)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func readAll(dst *[][]any, n int) func(rows *sql.Rows) (int64, error) {
	return func(rows *sql.Rows) (int64, error) {
		var count int64
		for rows.Next() {
			vals := make([]any, n)
			ptrs := make([]any, n)
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return count, err
			}
			*dst = append(*dst, vals)
			count++
		}
		return count, rows.Err()
	}
}

func equalValue(a, b any) bool {
	if ta, ok := a.(time.Time); ok {
		tb, ok := b.(time.Time)
		return ok && ta.Equal(tb)
	}
	return reflect.DeepEqual(a, b)
}

// Every column type must reach database/sql as the same Go value the plain pgx driver produces, so a
// generated scan function cannot tell the wire path from the sequential one.
func TestResultValuesMatchThePlainDriver(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		cols := []string{
			"1::int2", "2::int4", "3::int8", "1.5::float4", "2.25::float8", "true", "false",
			`'\xdeadbeef'::bytea`, "'héllo'::text", "'abc'::varchar(5)", "'ab'::char(4)", "'x'::name",
			"'2024-02-29 12:34:56.789123+00'::timestamptz", "'2024-02-29 12:34:56.789123'::timestamp", "'2024-02-29'::date",
			"12.34::numeric", `'{"a": 1}'::jsonb`, `'{"a": 1}'::json`, "'123e4567-e89b-12d3-a456-426614174000'::uuid",
			"NULL::text", "NULL::int8", "5::oid", "(-9223372036854775807 - 1)::int8", "'1 day 02:03:04'::interval",
		}
		if !e.crdb {
			cols = append(cols, "'7'::xid", "'8'::cid", "'{1,2}'::int4[]", "'(1,2)'::point")
		}
		if !e.crdb && !e.yb { // YugabyteDB is built without XML support
			cols = append(cols, "'<a/>'::xml")
		}
		var want []any
		q := "SELECT " + strings.Join(cols, ", ")
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := plain.QueryRow(q).Scan(ptrs...); err != nil {
			t.Fatalf("plain driver: %v", err)
		}
		want = dest

		var got [][]any
		op := runtime.PipelineOp{SQL: q, HasResultCols: true, Decorators: runtime.OpDecorators{AssertRows: -1}, ScanWireRows: readAll(&got, len(cols))}
		for _, tx := range []bool{false, true} {
			got = nil
			if err := runOps(t, db, tx, []runtime.PipelineOp{op}); err != nil {
				t.Fatalf("tx=%v: %v", tx, err)
			}
			if len(got) != 1 {
				t.Fatalf("tx=%v: %d rows", tx, len(got))
			}
			for i := range cols {
				if !equalValue(got[0][i], want[i]) {
					t.Errorf("tx=%v %s: wire %T(%v), plain driver %T(%v)", tx, cols[i], got[0][i], got[0][i], want[i], want[i])
				}
			}
		}
	})
}

// Go values of every kind a generated query binds must be accepted and stored correctly without
// declaring parameter types.
func TestArgumentEncodings(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		table := newTable(t, db, `CREATE TABLE %s (a INT2, b INT4, c INT8, d FLOAT4, e FLOAT8, f BOOL, g BYTEA, h TEXT,
			i TIMESTAMPTZ, j TIMESTAMP, k DATE, l NUMERIC, m JSONB, n UUID, o TEXT, p TEXT, q INT8)`)
		ts := time.Date(2024, 2, 29, 12, 34, 56, 789123000, time.UTC)
		ins := runtime.PipelineOp{
			SQL: "INSERT INTO " + table + " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
			Args: []any{int16(7), int32(-4), int64(1) << 40, float32(1.5), 2.25, true, []byte{1, 2, 3}, "héllo", ts, ts, ts, "12.34",
				`{"a": 1}`, "123e4567-e89b-12d3-a456-426614174000", nil, sql.NullString{String: "nv", Valid: true}, uint32(9)},
			Decorators: runtime.OpDecorators{AssertRows: -1},
		}
		insOps := []runtime.PipelineOp{ins}
		if err := runOps(t, db, true, insOps); err != nil {
			t.Fatal(err)
		}
		if insOps[0].RowCount != 1 {
			t.Fatalf("RowCount = %d", insOps[0].RowCount)
		}
		var a int16
		var b int32
		var c int64
		var d float32
		var ee float64
		var f bool
		var g []byte
		var h string
		var i, j, k time.Time
		var l, m, n string
		var o, p sql.NullString
		var qq int64
		if err := plain.QueryRow("SELECT a,b,c,d,e,f,g,h,i,j,k,l,m::text,n::text,o,p,q FROM "+table).Scan(&a, &b, &c, &d, &ee, &f, &g, &h, &i, &j, &k, &l, &m, &n, &o, &p, &qq); err != nil {
			t.Fatal(err)
		}
		if a != 7 || b != -4 || c != 1<<40 || d != 1.5 || ee != 2.25 || !f || string(g) != "\x01\x02\x03" || h != "héllo" ||
			!i.Equal(ts) || !j.Equal(ts.Truncate(time.Microsecond)) || k.Format("2006-01-02") != "2024-02-29" ||
			l != "12.34" || m != `{"a": 1}` || n != "123e4567-e89b-12d3-a456-426614174000" || o.Valid || p.String != "nv" || qq != 9 {
			t.Fatalf("stored values differ: %v %v %v %v %v %v %q %q %v %v %v %v %v %v %v %v %v", a, b, c, d, ee, f, g, h, i, j, k, l, m, n, o, p, qq)
		}
	})
}

func TestResultCountsAndConditionReadBack(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e testEngine, db, plain *sql.DB) {
		k := newKVTable(t, db)
		ops := []runtime.PipelineOp{
			{SQL: "UPDATE " + k.table + " SET v = v + 1", Decorators: runtime.OpDecorators{AssertRows: -1}},
			{SQL: "SELECT id FROM " + k.table + " WHERE id > $1", Args: []any{int64(1)}, HasResultCols: true, Decorators: runtime.OpDecorators{AssertRows: -1},
				ScanWireRows: func(rows *sql.Rows) (int64, error) {
					var n int64
					for rows.Next() {
						n++
					}
					return n, rows.Err()
				}},
			{SQL: "SELECT pg_catalog.set_config('sqlgen.c4', 'true', true)", HasResultCols: true, Decorators: runtime.OpDecorators{AssertRows: -1, ProduceCond: &runtime.PipelineCond{}, CondID: 4},
				ScanWireRows: func(rows *sql.Rows) (int64, error) { return 1, nil }},
			{SQL: "SELECT pg_catalog.set_config('sqlgen.c5', 'false', true)", HasResultCols: true, Decorators: runtime.OpDecorators{AssertRows: -1, ProduceCond: &runtime.PipelineCond{}, CondID: 5},
				ScanWireRows: func(rows *sql.Rows) (int64, error) { return 1, nil }},
			{SQL: "WITH _sqlgen_op AS (UPDATE " + k.table + " SET v = v + 1 WHERE id = 1 RETURNING 1) SELECT (SELECT COUNT(*) FROM _sqlgen_op), 1", Decorators: runtime.OpDecorators{AssertRows: 1}},
			{SQL: "SELECT 1", Decorators: runtime.OpDecorators{IsAbort: true, AssertRows: -1}},
		}
		for _, tx := range []bool{true, false} {
			if err := runOps(t, db, tx, ops); err != nil {
				t.Fatal(err)
			}
			if ops[0].RowCount != 3 || ops[1].RowCount != 2 || ops[4].RowCount != 1 {
				t.Fatalf("tx=%v row counts = %d %d %d", tx, ops[0].RowCount, ops[1].RowCount, ops[4].RowCount)
			}
			if !ops[2].CondSet || !ops[2].CondVal || !ops[3].CondSet || ops[3].CondVal {
				t.Fatalf("tx=%v conditions = %+v / %+v", tx, ops[2], ops[3])
			}
			if ops[0].CondSet || ops[1].CondSet {
				t.Fatalf("ops without a recorded condition must not report one")
			}
			ops[2].CondSet, ops[3].CondSet = false, false
		}
		sameRows(t, k.rows(t, plain), []row{{1, 14}, {2, 22}, {3, 32}})
	})
}
