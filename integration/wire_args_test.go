package integration

// Arguments reach the server of a pipeline statement WITHOUT declared types (no describe round-trip), so
// the type the server infers for a placeholder is the type of the column it meets, not the type pgx
// assumes for the Go value. The single-round-trip path must store the same data as the prepared-statement
// path of the same driver, for the Go types people bind.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen/runtime"
)

// wireInsert runs one statement through the single-round-trip path of db (a pgxwire database).
func wireInsert(t *testing.T, db *sql.DB, q string, args ...any) error {
	t.Helper()
	ctx := ctxT(t)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var out error
	ops := []runtime.PipelineOp{{SQL: q, Args: args, Decorators: runtime.OpDecorators{AssertRows: -1}}}
	if err := conn.Raw(func(dc any) error {
		tp, ok := dc.(runtime.WireTxPipeliner)
		if !ok {
			t.Fatalf("%T is not a WireTxPipeliner", dc)
		}
		out = tp.ExecTxPipeline(ctx, nil, ops)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

type argCase struct {
	name string
	col  string
	sql  string // statement with one placeholder; default INSERT INTO t VALUES ($1)
	arg  any
	// wireErr, when set, is the substring the wire path's error must carry INSTEAD of matching the
	// prepared path: a combination that cannot work without a type hint must fail clearly.
	wireErr  string
	pgOnly   bool // CockroachDB's json is jsonb
	crdbOnly bool
}

type validValuer struct{ v any }

func (v validValuer) Value() (driver.Value, error) { return v.v, nil }

func argCases() []argCase {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		ny = time.FixedZone("EST", -5*3600)
	}
	plus5 := time.FixedZone("p5", 5*3600)
	late := time.Date(2024, 3, 1, 22, 30, 0, 123456000, ny) // 03:30 UTC of the NEXT day
	early := time.Date(2024, 3, 1, 1, 30, 0, 0, plus5)      // 20:30 UTC of the PREVIOUS day
	uuidB := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var cases []argCase
	for _, col := range []string{"timestamp", "timestamptz", "date"} {
		cases = append(cases,
			argCase{name: "late evening west of UTC into " + col, col: col, arg: late},
			argCase{name: "early morning east of UTC into " + col, col: col, arg: early},
			argCase{name: "UTC into " + col, col: col, arg: late.UTC()},
			argCase{name: "pointer into " + col, col: col, arg: &late},
			argCase{name: "nil pointer into " + col, col: col, arg: (*time.Time)(nil)},
			argCase{name: "sql.NullTime into " + col, col: col, arg: sql.NullTime{Time: late, Valid: true}},
			argCase{name: "invalid sql.NullTime into " + col, col: col, arg: sql.NullTime{}},
			argCase{name: "Valuer returning time into " + col, col: col, arg: validValuer{late}},
			argCase{name: "year 1 into " + col, col: col, arg: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
			argCase{name: "year 2200 into " + col, col: col, arg: time.Date(2200, 12, 31, 23, 59, 59, 999999000, plus5)},
		)
	}
	cases = append(cases,
		argCase{name: "time array into timestamp[]", col: "timestamp[]", arg: []time.Time{late, early}},
		argCase{name: "time array into timestamptz[]", col: "timestamptz[]", arg: []time.Time{late, early}},
		argCase{name: "time in a comparison", col: "timestamp", sql: "INSERT INTO t SELECT TIMESTAMP '2024-03-01 22:30:00' WHERE TIMESTAMP '2024-03-01 22:30:00' = $1", arg: late},

		argCase{name: "bytes into bytea", col: "bytea", arg: []byte{0, 1, 0xff, '\\', '\''}},
		argCase{name: "empty bytes into bytea", col: "bytea", arg: []byte{}},
		argCase{name: "nil bytes into bytea", col: "bytea", arg: []byte(nil)},
		argCase{name: "bytes into text", col: "text", arg: []byte("héllo")},
		argCase{name: "bytes into varchar", col: "varchar(40)", arg: []byte("héllo")},
		argCase{name: "bytes into json", col: "json", arg: []byte(`{"a": 1}`), pgOnly: true},
		argCase{name: "bytes into jsonb with an explicit cast", col: "jsonb", sql: "INSERT INTO t VALUES ($1::text::jsonb)", arg: []byte(`{"a":1}`)},
		argCase{name: "bytes into jsonb without a cast", col: "jsonb", arg: []byte(`{"a":1}`), wireErr: "[]byte"},
		argCase{name: "RawMessage into jsonb", col: "jsonb", arg: json.RawMessage(`{"a":1}`)},
		argCase{name: "string into jsonb", col: "jsonb", arg: `{"a":1}`},
		argCase{name: "map into jsonb", col: "jsonb", arg: map[string]any{"a": []int{1}, "b": "x"}},
		argCase{name: "struct into jsonb", col: "jsonb", arg: struct {
			A int    `json:"a"`
			B string `json:"b"`
		}{1, "x"}},
		argCase{name: "struct pointer into json", col: "json", arg: &struct{ A int }{1}},
		argCase{name: "bytes into json on CockroachDB needs a cast too", col: "json", arg: []byte(`{"a": 1}`), wireErr: "[]byte", crdbOnly: true},
		argCase{name: "slice of structs into jsonb", col: "jsonb", arg: []struct{ A int }{{1}, {2}}},
		argCase{name: "array into uuid", col: "uuid", arg: uuidB},
		argCase{name: "string into uuid", col: "uuid", arg: "123e4567-e89b-12d3-a456-426614174000"},
		argCase{name: "bytes into uuid", col: "uuid", arg: uuidB[:]},

		// controls: values pgx already encoded correctly must keep working
		argCase{name: "ints into int8", col: "int8", arg: int64(-1 << 63)},
		argCase{name: "string into text", col: "text", arg: "aé世"},
		argCase{name: "empty string into text", col: "text", arg: ""},
		argCase{name: "float into float8", col: "float8", arg: 1.5},
		argCase{name: "bool into boolean", col: "boolean", arg: true},
		argCase{name: "ints into int4[]", col: "int4[]", arg: []int64{1, 2, 3}},
		argCase{name: "strings into text[]", col: "text[]", arg: []string{"a,b", `q"r`, "{x}", "NULL", ""}},
		argCase{name: "nil into text", col: "text", arg: nil},
		argCase{name: "NullString into text", col: "text", arg: sql.NullString{String: "s", Valid: true}},
		argCase{name: "NullInt64 into int8", col: "int8", arg: sql.NullInt64{Int64: 7, Valid: true}},
		argCase{name: "invalid NullInt64 into int8", col: "int8", arg: sql.NullInt64{}},
	)
	return cases
}

func TestRealEnginePipelineArgumentsMatchPreparedStatements(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("pgxwire is for PostgreSQL-protocol engines")
		}
		dsn := os.Getenv(e.env)
		wire, plain := openWire(t, dsn, true), openWire(t, dsn, false)
		for _, c := range argCases() {
			t.Run(strings.ReplaceAll(c.name, " ", "_"), func(t *testing.T) {
				if c.pgOnly && e.family == famCockroach {
					t.Skip("json is jsonb on CockroachDB")
				}
				if c.crdbOnly && e.family != famCockroach {
					t.Skip("CockroachDB only")
				}
				tbl := makeTable(t, db, "sg_arg", "CREATE TABLE %s (v "+c.col+")")
				stmt := c.sql
				if stmt == "" {
					stmt = "INSERT INTO t VALUES ($1)"
				}
				stmt = strings.ReplaceAll(stmt, "INSERT INTO t ", "INSERT INTO "+tbl+" ")
				run := func(viaWire bool) string {
					mustExec(t, db, "DELETE FROM "+tbl)
					var err error
					if viaWire {
						err = wireInsert(t, wire, stmt, c.arg)
					} else {
						_, err = plain.Exec(stmt, c.arg)
					}
					if err != nil {
						return "ERR " + err.Error()
					}
					var rows []string
					rs, err := db.Query("SELECT v::text FROM " + tbl)
					if err != nil {
						t.Fatal(err)
					}
					defer rs.Close()
					for rs.Next() {
						var s sql.NullString
						if err := rs.Scan(&s); err != nil {
							t.Fatal(err)
						}
						rows = append(rows, fmt.Sprintf("%q/%v", s.String, s.Valid))
					}
					return strings.Join(rows, ";")
				}
				w, p := run(true), run(false)
				t.Logf("wire  : %s\nplain : %s", w, p)
				if c.wireErr != "" {
					if !strings.HasPrefix(w, "ERR ") || !strings.Contains(w, c.wireErr) || !strings.Contains(w, "::") {
						t.Fatalf("want a clear error mentioning %q and an explicit cast, got %s", c.wireErr, w)
					}
					return
				}
				if w != p {
					t.Fatalf("single-round-trip path stored something else:\n  wire  %s\n  plain %s", w, p)
				}
			})
		}
	})
}

func TestRealEnginePipelineUnencodableArgumentIsRejectedClearly(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("pgxwire is for PostgreSQL-protocol engines")
		}
		wire := openWire(t, os.Getenv(e.env), true)
		tbl := makeTable(t, db, "sg_arg", "CREATE TABLE %s (a text, v text)")
		err := wireInsert(t, wire, "INSERT INTO "+tbl+" VALUES ($1, $2)", "ok", make(chan int))
		if err == nil {
			t.Fatal("a channel was accepted as an argument")
		}
		for _, want := range []string{"argument $2", "chan int", "explicit cast"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %q", err, want)
			}
		}
		var n int
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+tbl).Scan(&n); err != nil || n != 0 {
			t.Fatalf("rows after a rejected statement: %d, %v", n, err)
		}
	})
}
