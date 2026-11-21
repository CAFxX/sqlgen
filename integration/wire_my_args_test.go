package integration

// The arguments of a MySQL / MariaDB single-round-trip pipeline are inlined as SQL literals by the
// renderer (MySQLFlavor.WriteLiteral): the server never sees them as parameters. The data stored by that
// path must be byte for byte what the prepared-statement path of the plain driver (binary protocol,
// interpolateParams=false) stores for the same Go value, column type and session settings, and an
// argument that must not be written as a literal (NaN, a struct, ...) must run the sequential fallback
// with the plain driver's own outcome. Nothing a caller passes may ever be read as SQL.

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/runtime"
)

// myArgVariant is a session configuration: both pools of a pair (prepared and wire) are opened with it.
type myArgVariant struct {
	name   string
	mutate func(t testing.TB, cfg *mysql.Config)
}

func myParam(k, v string) func(testing.TB, *mysql.Config) {
	return func(t testing.TB, cfg *mysql.Config) {
		if cfg.Params == nil {
			cfg.Params = map[string]string{}
		}
		cfg.Params[k] = v
	}
}

func myCharset(cs, coll string) func(testing.TB, *mysql.Config) {
	return func(t testing.TB, cfg *mysql.Config) {
		if err := cfg.Apply(mysql.Charset(cs, coll)); err != nil {
			t.Fatal(err)
		}
	}
}

var myArgVariants = []myArgVariant{
	{"base", nil},
	{"no_backslash_escapes", myParam("sql_mode", "'NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES'")},
	{"ansi_quotes", myParam("sql_mode", "'ANSI_QUOTES,STRICT_TRANS_TABLES'")},
	{"non_strict", myParam("sql_mode", "''")},
	{"latin1", myCharset("latin1", "latin1_swedish_ci")},
	{"gbk", myCharset("gbk", "gbk_chinese_ci")},
	{"binary", myCharset("binary", "")},
	{"loc_rome_tz_plus9", func(t testing.TB, cfg *mysql.Config) {
		loc, err := time.LoadLocation("Europe/Rome")
		if err != nil {
			t.Fatal(err)
		}
		cfg.Loc = loc
		myParam("time_zone", "'+09:00'")(t, cfg)
	}},
	{"utf8mb4_bin", func(t testing.TB, cfg *mysql.Config) { cfg.Collation = "utf8mb4_bin" }},
}

type myArgValuer struct{ v any }

func (v myArgValuer) Value() (driver.Value, error) { return v.v, nil }

type myArgCase struct {
	name   string
	arg    any
	reject bool // the literal encoder cannot carry it: the pipeline must run sequentially
}

func myArgCases() []myArgCase {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		rome = time.FixedZone("CET", 3600)
	}
	plus5h30 := time.FixedZone("p530", 5*3600+1800)
	base := time.Date(2024, 3, 31, 1, 30, 15, 0, time.UTC)
	cases := []myArgCase{
		// integers, bool
		{"int64 min", int64(math.MinInt64), false},
		{"int64 max", int64(math.MaxInt64), false},
		{"int64 -1", int64(-1), false},
		{"int64 0", int64(0), false},
		{"int64 127", int64(127), false},
		{"int64 128", int64(128), false},
		{"uint64 max", uint64(math.MaxUint64), false},
		{"int 7", 7, false},
		{"int32 -7", int32(-7), false},
		{"uint8 200", uint8(200), false},
		{"uint 5", uint(5), false},
		{"bool true", true, false},
		{"bool false", false, false},
		// floats
		{"float64 0.1", 0.1, false},
		{"float64 -0.0", math.Copysign(0, -1), false},
		{"float64 1.5", 1.5, false},
		{"float64 -2.5e-7", -2.5e-7, false},
		{"float64 1e300", 1e300, false},
		{"float64 smallest nonzero", math.SmallestNonzeroFloat64, false},
		{"float64 max", math.MaxFloat64, false},
		{"float64 from float32 0.1", float64(float32(0.1)), false},
		{"float32 0.1", float32(0.1), false},
		{"float32 3.4e38", float32(3.4e38), false},
		{"float64 123456789.123456789", 123456789.123456789, false},
		// strings
		{"string plain", "plain", false},
		{"string empty", "", false},
		{"string single quote", "it's", false},
		{"string doubled quotes", "a''b'''", false},
		{"string double quote", `say "hi"`, false},
		{"string backslash", `a\b`, false},
		{"string double backslash", `a\\b`, false},
		{"string backslash before quote", `\'`, false},
		{"string trailing backslash", `abc\`, false},
		{"string escapes lookalike", `\n\r\t\0\Z\%\_`, false},
		{"string NUL", "nul\x00mid", false},
		{"string ctrl-Z", "ctrlz\x1amid", false},
		{"string newlines", "line\nfeed\r\ncr\r", false},
		{"string tab and bell", "a\tb\a", false},
		{"string emoji", "emoji \U0001F600 end", false},
		{"string non-ascii", "\u00e9 \u00fc \u4e16\u754c \u20ac", false},
		{"string invalid utf8", "bad\xff\xfeend", false},
		{"string gbk trap", "\xbf'", false},
		{"string gbk trap backslash", "\xbf\\'", false},
		{"string semicolons", "a; SELECT 1; b", false},
		{"string comment markers", "x -- y /* z */ # w", false},
		{"string question mark", "what?", false},
		{"string percent s", "%s %d %v", false},
		{"string injection", "'; DROP TABLE sg_sentinel; --", false},
		{"string injection backslash", `\'; DROP TABLE sg_sentinel; --`, false},
		{"decimal string", "12345678901234.123456", false},
		{"decimal string negative", "-0.000001", false},
		{"decimal string bad", "12abc", false},
		{"json string", `{"a": [1, 2, {"b": null}], "q": "it's", "bs": "\\n"}`, false},
		{"json string invalid", `[1,2`, false},
		{"string 60KB", strings.Repeat("0123456789'\\abcdefghij", 3000), false},
		{"string 300 chars", strings.Repeat("y", 300), false},
		// bytes
		{"bytes empty", []byte{}, false},
		{"bytes nil", []byte(nil), false},
		{"bytes text", []byte("hello"), false},
		{"bytes numeric text", []byte("12"), false},
		{"bytes json", []byte(`{"a":1}`), false},
		{"bytes quote backslash NUL", []byte("a'b\\c\x00d"), false},
		{"bytes invalid utf8", []byte{0xff, 0xfe, 0x80, 'x'}, false},
		{"bytes all values", func() []byte {
			b := make([]byte, 256)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		}(), false},
		// NULL and wrappers
		{"nil", nil, false},
		{"nil *int64", (*int64)(nil), false},
		{"*string", func() *string { s := "ptr's"; return &s }(), false},
		{"NullString valid", sql.NullString{String: "ns", Valid: true}, false},
		{"NullString invalid", sql.NullString{}, false},
		{"NullInt64 valid", sql.NullInt64{Int64: 42, Valid: true}, false},
		{"Valuer returning string", myArgValuer{"valuer's"}, false},
		{"Valuer returning int64", myArgValuer{int64(9)}, false},
		{"Valuer returning bytes", myArgValuer{[]byte{1, 2, 3}}, false},
		// time.Time
		{"time UTC", base, false},
		{"time Europe/Rome", base.In(rome), false},
		{"time Rome summer", time.Date(2024, 7, 1, 12, 0, 0, 0, rome), false},
		{"time fixed zone +05:30", base.In(plus5h30), false},
		{"time nanos .123456789", base.Add(123456789 * time.Nanosecond), false},
		{"time nanos .999999999", base.Add(999999999 * time.Nanosecond), false},
		{"time nanos .0000005", base.Add(500 * time.Nanosecond), false},
		{"time nanos .000001", base.Add(1000 * time.Nanosecond), false},
		{"time micro .5", base.Add(500000 * time.Microsecond), false},
		{"time zero", time.Time{}, false},
		{"time year 1000", time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"time year 9999", time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC), false},
		{"time year 1", time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"time epoch", time.Unix(0, 0).UTC(), false},
		{"time TIMESTAMP max", time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC), false},
		{"time DST gap Rome", time.Date(2024, 3, 31, 2, 30, 0, 0, time.UTC).In(rome), false},
		{"time pointer", &base, false},
		{"time NullTime valid", sql.NullTime{Time: base, Valid: true}, false},
		// arguments the literal encoder rejects: the pipeline falls back to the sequential path
		{"float NaN", math.NaN(), true},
		{"float +Inf", math.Inf(1), true},
		{"float -Inf", math.Inf(-1), true},
		{"struct", struct{ X int }{1}, true},
		{"chan", make(chan int), true},
		{"map", map[string]int{"a": 1}, true},
		{"slice of ints", []int{1, 2}, true},
		{"time year 10000", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), true},
	}
	return cases
}

type myArgColumn struct {
	name string
	ddl  string
}

var myArgColumns = []myArgColumn{
	{"TINYINT", "TINYINT"},
	{"BIGINT", "BIGINT"},
	{"BIGINT_UNSIGNED", "BIGINT UNSIGNED"},
	{"VARCHAR200", "VARCHAR(200)"},
	{"MEDIUMTEXT", "MEDIUMTEXT"},
	{"MEDIUMBLOB", "MEDIUMBLOB"},
	{"DATETIME6", "DATETIME(6)"},
	{"TIMESTAMP6", "TIMESTAMP(6) NULL"},
	{"DECIMAL", "DECIMAL(20,6)"},
	{"JSON", "JSON"},
	{"BOOL", "BOOL"},
	{"FLOAT", "FLOAT"},
	{"DOUBLE", "DOUBLE"},
}

var myArgSeq atomic.Uint32

// myInsertDesc is INSERT INTO tbl (id, v) VALUES (?, ?).
func myInsertDesc(tbl string) *runtime.QueryDescriptor {
	return &runtime.QueryDescriptor{
		Name: "ArgInsert", StmtID: 30000 + myArgSeq.Add(1), StmtVerb: runtime.VerbInsert,
		StaticSQL: "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)",
		Segments:  []string{"INSERT INTO " + tbl + " (id, v) VALUES (", ", ", ")"},
		Slots:     []runtime.Slot{{Kind: runtime.SlotScalar}, {Kind: runtime.SlotScalar}},
	}
}

// myWireInsert runs the insert as a one-statement PipelineTx on the wire pool and reports which path
// the pipeline took.
func myWireInsert(t *testing.T, q *myq.Queries, desc *runtime.QueryDescriptor, id int64, arg any) (err error, wire bool) {
	t.Helper()
	before := q.CacheStats()
	err = q.PipelineTx(ctxT(t), nil, func(p *myq.Pipeline) error {
		myQueue(p, desc, id, arg)
		return nil
	})
	after := q.CacheStats()
	switch {
	case after.WirePipelines == before.WirePipelines+1 && after.SequentialPipelines == before.SequentialPipelines:
		return err, true
	case after.WirePipelines == before.WirePipelines && after.SequentialPipelines == before.SequentialPipelines+1:
		return err, false
	}
	t.Fatalf("pipeline counted neither as wire nor as sequential: %+v -> %+v", before, after)
	return
}

// myStored reads the stored bytes of every row of tbl: HEX(CAST(v AS BINARY)) or NULL.
func myStored(t testing.TB, db *sql.DB, tbl string) map[int64]string {
	t.Helper()
	rows, err := db.Query("SELECT id, HEX(CAST(v AS BINARY)), v IS NULL FROM " + tbl)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var hex sql.NullString
		var isNull bool
		if err := rows.Scan(&id, &hex, &isNull); err != nil {
			t.Fatal(err)
		}
		if isNull {
			out[id] = "NULL"
		} else {
			out[id] = "0x" + hex.String
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// myArgDocumentedResidual names the divergences between the two paths that are known and understood.
// Each one is matched by its exact shape (case, column kind, which side refused), so a change of
// behaviour, in either direction, fails the test instead of being absorbed. The literal rules live in
// runtime.MySQLFlavor.WriteLiteral; none of these is silent for a value of the column's own kind.
//
//   - bytes as a hex literal: 0x3132 is the NUMBER 12594 in a numeric context, while the prepared path
//     sends the text "12" (the prepared path refuses non-numeric text, the literal stores a huge number,
//     or, in a non-strict session, both store something else); and MySQL refuses a binary string for a
//     JSON column, so []byte JSON text fails there (error 3144) where the prepared path stores it. A
//     []byte meant for a numeric or JSON column must be passed as a string.
//   - a time.Time without a time of day is a bare date for the prepared path: a character or binary
//     column stores "2024-03-31" there and "2024-03-31 00:00:00" through the literal.
//   - a string with a backslash, or any string in a multi-byte character set, is written as
//     _<charset> 0x...: a string the character set cannot hold (invalid UTF-8, non-GBK text in a gbk
//     session, a lone lead byte) is refused with error 1300 where the prepared path stores the bytes
//     into a binary or numeric-converted column. The wire path refuses, never stores something else.
//   - bytes in a latin1 session: the hex literal is a binary string, which a utf8mb4 text column
//     validates (error 1366), while the prepared path converts the text from latin1.
//   - "-0.000001" as a string into BIGINT UNSIGNED: the literal is folded to a rounded DECIMAL constant
//     (0), the parameter is converted at execution (error 1264).
func myArgDocumentedResidual(e engine, variant, column string, c myArgCase, pe, we string, ps, ws string) string {
	bytesArg := strings.HasPrefix(c.name, "bytes ") || c.name == "Valuer returning bytes"
	numeric := false
	switch column {
	case "TINYINT", "BIGINT", "BIGINT_UNSIGNED", "DECIMAL", "DOUBLE", "FLOAT", "BOOL":
		numeric = true
	}
	textual := column == "VARCHAR200" || column == "MEDIUMTEXT" || column == "MEDIUMBLOB"
	switch {
	case bytesArg && numeric:
		return "bytes as a hex literal are a number in a numeric column"
	case bytesArg && column == "JSON" && e.family == famMySQL && pe == "false" && we == "true":
		return "MySQL refuses a binary string for a JSON column"
	case strings.HasPrefix(c.name, "time ") && textual && pe == "false" && we == "false":
		return "a time without a time of day is a bare date for the prepared path"
	case (c.name == "string gbk trap backslash" || c.name == "string gbk trap" || c.name == "string invalid utf8" ||
		c.name == "string non-ascii") && pe == "false" && we == "true":
		return "_charset 0x... refuses text the character set cannot hold"
	case bytesArg && variant == "latin1" && (column == "MEDIUMTEXT" || column == "VARCHAR200") && pe == "false" && we == "true":
		return "a binary string is validated as utf8mb4 by a text column"
	case c.name == "decimal string negative" && column == "BIGINT_UNSIGNED" && pe == "true" && we == "false":
		return "a string constant is rounded to 0, a parameter is refused"
	}
	return ""
}

func TestRealEngineMySQLPipelineArgumentLiterals(t *testing.T) {
	cases := myArgCases()
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		for _, v := range myArgVariants {
			t.Run(v.name, func(t *testing.T) {
				mutate := func(cfg *mysql.Config) {
					if v.mutate != nil {
						v.mutate(t, cfg)
					}
				}
				plain, _ := myOpenCounted(t, dsn, false, mutate)
				wired, _ := myOpenCounted(t, dsn, true, mutate)
				q := myq.New(wired)
				defer q.Close()
				vcfg, err := mysql.ParseDSN(dsn)
				if err != nil {
					t.Fatal(err)
				}
				mutate(vcfg)
				for _, col := range myArgColumns {
					t.Run(col.name, func(t *testing.T) {
						tbl := makeTable(t, db, "sg_arg", "CREATE TABLE %s (id INT PRIMARY KEY, v "+col.ddl+")")
						desc := myInsertDesc(tbl)
						// even ids: the prepared path, odd ids: the single-round-trip path
						errs := map[int64][2]string{}
						for i, c := range cases {
							pid, wid := int64(2*i), int64(2*i+1)
							_, perr := plain.Exec("INSERT INTO "+tbl+" (id, v) VALUES (?, ?)", pid, c.arg)
							werr, viaWire := myWireInsert(t, q, desc, wid, c.arg)
							reject := c.reject
							if tm, ok := c.arg.(time.Time); ok && !tm.IsZero() && tm.In(vcfg.Loc).Year() > 9999 {
								reject = true // in the connection's time zone the year is past 9999: no literal for it
							}
							if viaWire == reject {
								t.Errorf("%q: viaWire=%v but the argument is expected to %s", c.name, viaWire, map[bool]string{true: "fall back", false: "be inlined"}[reject])
							}
							// error-ness, not the error number: a literal and a parameter of the same invalid value
							// may be refused with different server errors (1366 vs 1264 vs 1300)
							perrS, werrS := fmt.Sprint(perr != nil), fmt.Sprint(werr != nil)
							if reject || (perr != nil && myErrNum(perr) == 0) {
								// not a server error: the sequential fallback must return the driver's own error
								perrS, werrS = fmt.Sprint(perr), fmt.Sprint(werr)
							}
							errs[int64(i)] = [2]string{perrS, werrS}
						}
						stored := myStored(t, db, tbl)
						bad := 0
						residuals := map[string]int{}
						defer func() {
							for r, n := range residuals {
								t.Logf("documented residual (%d): %s", n, r)
							}
						}()
						for i, c := range cases {
							pe, we := errs[int64(i)][0], errs[int64(i)][1]
							ps, pok := stored[int64(2*i)]
							ws, wok := stored[int64(2*i+1)]
							if !pok {
								ps = "(no row)"
							}
							if !wok {
								ws = "(no row)"
							}
							if pe == we && ps == ws {
								continue
							}
							if r := myArgDocumentedResidual(e, v.name, col.name, c, pe, we, ps, ws); r != "" {
								residuals[r]++
								continue
							}
							bad++
							if bad <= 12 {
								t.Errorf("%s/%s %q: wire path differs from the prepared path\n  prepared: err=%s stored=%.80s\n  wire    : err=%s stored=%.80s", v.name, col.name, c.name, pe, ps, we, ws)
							}
						}
						if bad > 12 {
							t.Errorf("%d divergences in total", bad)
						}
					})
				}
			})
		}
	})
}

// --- the arguments never travel unescaped -------------------------------------------------------------

var myHostile = []string{
	"'; DROP TABLE sg_sentinel; --",
	`\'; DROP TABLE sg_sentinel; --`,
	`\\'; DROP TABLE sg_sentinel; --`,
	`\`,
	`\\`,
	`\'`,
	`'`,
	`''`,
	`/*`,
	`*/ ; DROP TABLE sg_sentinel; /*`,
	`?`,
	`?, ?`,
	`' OR '1'='1`,
	`" OR "1"="1`,
	"`; DROP TABLE sg_sentinel; --",
	"x'); INSERT INTO sg_sentinel VALUES (999); --",
	"x\\'); INSERT INTO sg_sentinel VALUES (999); --",
	"\"); INSERT INTO sg_sentinel VALUES (999); --",
	"a\nb'; DROP TABLE sg_sentinel;\n-- c",
	"\x00'; DROP TABLE sg_sentinel; --",
	"\x1a'; DROP TABLE sg_sentinel; --",
	"%s%s%s",
	"@sqlgen_f=-1; SET @sqlgen_f=-1; --",
	"'; SET @sqlgen_f=-1; SET @sqlgen_p=-1; --",
	"COMMIT; DROP TABLE sg_sentinel; --",
	"'; COMMIT; DROP TABLE sg_sentinel; --",
	"' ; ROLLBACK; --",
}

// byte-level traps of the multi-byte character sets: stored bytes are compared with the prepared path only.
var myHostileBytes = []string{
	"\xbf'; DROP TABLE sg_sentinel; --",
	"\xbf\\'; DROP TABLE sg_sentinel; --",
	"\x81\\'; DROP TABLE sg_sentinel; --",
	"\xa1\\\\'; DROP TABLE sg_sentinel; --",
	"\xff'; DROP TABLE sg_sentinel; --",
}

func TestRealEngineMySQLPipelineArgumentsNeverTravelUnescaped(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		// the hostile strings name sg_sentinel: that is the table they would damage
		mustExec(t, db, "DROP TABLE IF EXISTS sg_sentinel")
		mustExec(t, db, "CREATE TABLE sg_sentinel (n INT)")
		mustExec(t, db, "INSERT INTO sg_sentinel VALUES (1), (2), (3)")
		t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS sg_sentinel") })
		intact := func(what string) {
			t.Helper()
			var n, sum int64
			if err := db.QueryRow("SELECT COUNT(*), COALESCE(SUM(n), 0) FROM sg_sentinel").Scan(&n, &sum); err != nil {
				t.Fatalf("%s: the sentinel table is gone: %v", what, err)
			}
			if n != 3 || sum != 6 {
				t.Fatalf("%s: the sentinel table changed: %d rows, sum %d", what, n, sum)
			}
		}
		for _, v := range myArgVariants {
			t.Run(v.name, func(t *testing.T) {
				mutate := func(cfg *mysql.Config) {
					if v.mutate != nil {
						v.mutate(t, cfg)
					}
				}
				plain, _ := myOpenCounted(t, dsn, false, mutate)
				wired, _ := myOpenCounted(t, dsn, true, mutate)
				q := myq.New(wired)
				defer q.Close()
				// binary columns: the stored bytes are the argument's bytes in every session
				tbl := makeTable(t, db, "sg_hostile", "CREATE TABLE %s (id INT PRIMARY KEY, v MEDIUMBLOB, w VARBINARY(200))")
				desc := &runtime.QueryDescriptor{
					Name: "HostileInsert", StmtID: 30000 + myArgSeq.Add(1), StmtVerb: runtime.VerbInsert,
					StaticSQL: "INSERT INTO " + tbl + " (id, v, w) VALUES (?, ?, ?)",
					Segments:  []string{"INSERT INTO " + tbl + " (id, v, w) VALUES (", ", ", ", ", ")"},
					Slots:     []runtime.Slot{{Kind: runtime.SlotScalar}, {Kind: runtime.SlotScalar}, {Kind: runtime.SlotScalar}},
				}
				all := append(append([]string(nil), myHostile...), myHostileBytes...)
				// even ids: the prepared path (the reference), odd ids: the wire path. Every hostile string also
				// travels in a second argument, so a break out of the first would swallow the second.
				inserted := 0
				for i, s := range all {
					pid, wid := int64(2*i), int64(2*i+1)
					_, perr := plain.Exec("INSERT INTO "+tbl+" (id, v, w) VALUES (?, ?, ?)", pid, s, "tail-"+s)
					werr := q.PipelineTx(ctxT(t), nil, func(p *myq.Pipeline) error {
						myQueue(p, desc, wid, s, "tail-"+s).AssertRowsAffected(1)
						return nil
					})
					if i < len(myHostile) {
						// plain text: both paths must store it
						if perr != nil || werr != nil {
							t.Fatalf("insert of %q: prepared %v, wire %v", s, perr, werr)
						}
					} else if werr != nil && myErrNum(werr) == 0 {
						// the byte traps of multi-byte character sets may be refused by the server (error 1300:
						// text the character set cannot hold), never by anything else
						t.Fatalf("wire insert of %q: %v", s, werr)
					}
					if perr == nil {
						inserted++
					}
					if werr == nil {
						inserted++
					}
					intact(fmt.Sprintf("after %q", s))
				}
				if st := q.CacheStats(); st.WirePipelines != uint64(len(all)) || st.SequentialPipelines != 0 {
					t.Fatalf("every hostile pipeline must run on the wire path: %+v", st)
				}
				var n int
				if err := db.QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&n); err != nil || n != inserted {
					t.Fatalf("rows in the target table: %d (want %d: nothing a string says may add a row): %v", n, inserted, err)
				}
				rows, err := db.Query("SELECT id, v, w FROM " + tbl + " ORDER BY id")
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				got := map[int64][2][]byte{}
				for rows.Next() {
					var id int64
					var a, b []byte
					if err := rows.Scan(&id, &a, &b); err != nil {
						t.Fatal(err)
					}
					got[id] = [2][]byte{a, b}
				}
				for i, s := range all {
					p, pok := got[int64(2*i)]
					w, wok := got[int64(2*i+1)]
					if wok && string(w[0]) != s && i < len(myHostile) {
						t.Errorf("%q: stored %q, not verbatim", s, w[0])
					}
					if wok && string(w[1]) != "tail-"+s && i < len(myHostile) {
						t.Errorf("%q: the second argument stored %q", s, w[1])
					}
					if pok && wok && (!bytes.Equal(p[0], w[0]) || !bytes.Equal(p[1], w[1])) {
						t.Errorf("%q: stored bytes differ from the prepared path:\n  prepared %q %q\n  wire     %q %q", s, p[0], p[1], w[0], w[1])
					}
				}
				intact("at the end")
			})
		}
	})
}
