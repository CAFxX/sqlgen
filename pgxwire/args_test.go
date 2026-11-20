package pgxwire

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type valuerFunc func() (driver.Value, error)

func (f valuerFunc) Value() (driver.Value, error) { return f() }

type selfValuer struct{ self *selfValuer }

func (s *selfValuer) Value() (driver.Value, error) { return s, nil }

func TestFormatTimeKeepsTheOffsetAndHandlesBC(t *testing.T) {
	west := time.FixedZone("EST", -5*3600)
	for _, tc := range []struct {
		t    time.Time
		want string
	}{
		{time.Date(2024, 3, 1, 22, 30, 0, 0, west), "2024-03-01 22:30:00-05:00:00"},
		{time.Date(2024, 3, 1, 22, 30, 0, 123456000, time.UTC), "2024-03-01 22:30:00.123456Z"},
		{time.Date(2024, 3, 1, 1, 30, 0, 0, time.FixedZone("x", 5*3600+30*60)), "2024-03-01 01:30:00+05:30:00"},
		{time.Date(2024, 3, 1, 1, 30, 0, 0, time.FixedZone("lmt", 53*60+28)), "2024-03-01 01:30:00+00:53:28"},
		{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), "0001-01-01 00:00:00Z"},
		{time.Date(0, 6, 15, 12, 0, 0, 0, time.UTC), "0001-06-15 12:00:00Z BC"},
		{time.Date(-99, 6, 15, 12, 0, 0, 0, west), "0100-06-15 12:00:00-05:00:00 BC"},
		{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), "10000-01-01 00:00:00Z"},
	} {
		if got := formatTime(tc.t); got != tc.want {
			t.Errorf("formatTime(%v) = %q, want %q", tc.t, got, tc.want)
		}
	}
	if got := formatTimeArray([]time.Time{time.Date(2024, 3, 1, 22, 30, 0, 0, west), time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC)}); got != `{"2024-03-01 22:30:00-05:00:00","2024-03-02 00:00:00Z"}` {
		t.Errorf("formatTimeArray = %s", got)
	}
	if got := formatUUID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0xff}); got != "01020304-0506-0708-090a-0b0c0d0e0fff" {
		t.Errorf("formatUUID = %s", got)
	}
}

func TestResolveArgs(t *testing.T) {
	m := pgtype.NewMap()
	west := time.FixedZone("EST", -5*3600)
	ts := time.Date(2024, 3, 1, 22, 30, 0, 0, west)
	var nilTime *time.Time
	var nilValuer *selfValuer
	type pair struct{ A int }
	cases := []struct {
		name string
		arg  any
		want any // a string or nil; rawBytes and untouched args are checked below
	}{
		{"time", ts, "2024-03-01 22:30:00-05:00:00"},
		{"time pointer", &ts, "2024-03-01 22:30:00-05:00:00"},
		{"nil time pointer", nilTime, nil},
		{"time slice", []time.Time{ts}, `{"2024-03-01 22:30:00-05:00:00"}`},
		{"nil time slice", []time.Time(nil), nil},
		{"NullTime", sql.NullTime{Time: ts, Valid: true}, "2024-03-01 22:30:00-05:00:00"},
		{"invalid NullTime", sql.NullTime{}, nil},
		{"nil", nil, nil},
		{"nil bytes", []byte(nil), nil},
		{"uuid array", [16]byte{1}, "01000000-0000-0000-0000-000000000000"},
		{"valuer to time", valuerFunc(func() (driver.Value, error) { return ts, nil }), "2024-03-01 22:30:00-05:00:00"},
		{"valuer to nil", valuerFunc(func() (driver.Value, error) { return nil, nil }), nil},
		{"nil valuer pointer", nilValuer, nil},
		{"map", map[string]int{"a": 1}, `{"a":1}`},
		{"struct", pair{2}, `{"A":2}`},
		{"struct pointer", &pair{3}, `{"A":3}`},
		{"struct slice", []pair{{1}, {2}}, `[{"A":1},{"A":2}]`},
	}
	for _, tc := range cases {
		out, raws, err := resolveArgs(m, []any{tc.arg})
		if err != nil || len(raws) != 0 || len(out) != 1 {
			t.Errorf("%s: out=%v raws=%v err=%v", tc.name, out, raws, err)
			continue
		}
		if out[0] != tc.want {
			t.Errorf("%s: resolved to %#v, want %#v", tc.name, out[0], tc.want)
		}
	}

	// []byte arguments are pulled out as raw parameters, in position
	b1, b2 := []byte{0, 1}, []byte{}
	out, raws, err := resolveArgs(m, []any{"x", b1, 7, b2, valuerFunc(func() (driver.Value, error) { return []byte("v"), nil })})
	if err != nil || len(raws) != 3 || raws[0].idx != 1 || raws[1].idx != 3 || raws[2].idx != 4 || string(raws[2].val) != "v" || raws[1].val == nil {
		t.Fatalf("raws = %+v err=%v", raws, err)
	}
	if out[0] != "x" || out[1] != nil || out[2] != 7 || out[3] != nil || out[4] != nil {
		t.Fatalf("out = %#v", out)
	}

	// arguments pgx encodes as they are come back untouched, without copying the slice
	in := []any{"s", int64(1), 2.5, true, []string{"a"}, []int64{1}, json2("{}")}
	out, raws, err = resolveArgs(m, in)
	if err != nil || len(raws) != 0 || &out[0] != &in[0] {
		t.Fatalf("untouched args were copied or changed: %v %v %v", out, raws, err)
	}
	if out, _, _ := resolveArgs(m, nil); out != nil {
		t.Fatalf("no args: %v", out)
	}
}

type json2 string

func (j json2) MarshalJSON() ([]byte, error) { return []byte(j), nil }

func TestResolveArgsErrors(t *testing.T) {
	m := pgtype.NewMap()
	boom := errors.New("boom")
	for name, tc := range map[string]struct {
		arg  any
		want string
	}{
		"channel":         {make(chan int), "argument $2 (chan int)"},
		"valuer error":    {valuerFunc(func() (driver.Value, error) { return nil, boom }), "boom"},
		"valuer cycle":    {&selfValuer{}, "driver.Valuer chain"},
		"unmarshalable":   {map[string]any{"f": func() {}}, "encoding/json"},
		"nil map pointer": {(*map[string]int)(nil), ""},
	} {
		_, _, err := resolveArgs(m, []any{1, tc.arg})
		if tc.want == "" { // a nil pointer is NULL for pgx, not an error
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to contain %q", name, err, tc.want)
		}
	}
}

func TestWithRawArgHint(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "XX000", Message: "unsupported jsonb version number 123"}
	got := withRawArgHint(pgErr, []int{2, 5})
	var back *pgconn.PgError
	if !errors.As(got, &back) || back != pgErr {
		t.Fatalf("hint does not unwrap to the server error: %v", got)
	}
	for _, want := range []string{"$2, $5 are a []byte", "$2::text::jsonb", "unsupported jsonb version number 123"} {
		if !strings.Contains(got.Error(), want) {
			t.Errorf("hint %q lacks %q", got, want)
		}
	}
	if !strings.Contains(withRawArgHint(pgErr, []int{3}).Error(), "$3 is a []byte") {
		t.Errorf("singular form: %v", withRawArgHint(pgErr, []int{3}))
	}
	// no hint without raw arguments, for unrelated server errors, or for non-server errors
	if withRawArgHint(pgErr, nil) != error(pgErr) {
		t.Error("hint without raw arguments")
	}
	unique := &pgconn.PgError{Code: "23505"}
	if withRawArgHint(unique, []int{1}) != error(unique) {
		t.Error("hint on a unique violation")
	}
	plain := errors.New("net down")
	if withRawArgHint(plain, []int{1}) != plain {
		t.Error("hint on a transport error")
	}
}
