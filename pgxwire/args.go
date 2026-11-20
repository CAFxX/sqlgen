package pgxwire

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// A pipeline statement is sent without declared parameter types, so that a first-seen statement does
// not cost a describe round-trip: the server infers each placeholder's type from its context. pgx's
// default Go-to-PostgreSQL mapping assumes the type of the VALUE (a time.Time is a timestamptz, a
// []byte a bytea, ...), which silently disagrees with the column the placeholder actually meets. The
// arguments are therefore resolved here, to forms the server reads correctly whatever type it infers:
//
//   - time.Time (also behind a pointer, a driver.Valuer such as sql.NullTime, or a []time.Time) is sent
//     as text that keeps the value's OWN offset. PostgreSQL ignores the offset for timestamp, date and
//     time columns (the wall clock of the Go value is stored, as with a prepared statement), and honours
//     it for timestamptz. pgx's own text encoding converts to UTC first, which stored 22:30-05:00 into
//     a timestamp column as 03:30 of the next day.
//   - []byte is sent as raw bytes in the binary format. A server that infers bytea, text, varchar, json
//     or uuid reads the bytes as they are. It is NOT valid for jsonb (the server expects a version
//     prefix); cast explicitly there ($1::text::jsonb), or pass a string / json.RawMessage / map.
//   - [16]byte (a UUID) is sent in the canonical text form.
//   - maps, structs and slices pgx has no encoding for are sent as JSON text, which json and jsonb
//     columns (and text) accept.
//   - driver.Valuer values are resolved first, as database/sql does for a prepared statement.
//
// Everything else is encoded by pgx's default type map (QueryExecModeExec semantics). A value that no
// rule can encode produces an error naming the argument; give the placeholder an explicit cast in the
// SQL (for example $2::text) or convert the value yourself.

// rawParam is a []byte argument (0-based position) that is sent in the binary format.
type rawParam struct {
	idx int
	val []byte
}

// resolveArgs returns args with every value prepared for a typeless placeholder, and the []byte
// arguments, which are replaced by nil in the returned slice (the caller patches them in after pgx built
// the text parameters). args itself is returned, without allocating, when nothing needs to change.
func resolveArgs(m *pgtype.Map, args []any) (out []any, raws []rawParam, err error) {
	out = args
	set := func(i int, v any) {
		if &out[0] == &args[0] {
			out = append([]any(nil), args...)
		}
		out[i] = v
	}
	for i, a := range args {
		v, changed, err := resolveArg(m, a, 0)
		if err != nil {
			return nil, nil, fmt.Errorf("argument $%d (%T): %w", i+1, a, err)
		}
		if b, ok := v.(rawBytes); ok {
			raws = append(raws, rawParam{i, b})
			set(i, nil)
		} else if changed {
			set(i, v)
		}
	}
	return out, raws, nil
}

// rawBytes marks a non-nil []byte on its way to becoming a rawParam.
type rawBytes []byte

const maxValuerDepth = 8

// resolveArg prepares one argument; changed is false when arg is to be encoded by pgx as it is.
func resolveArg(m *pgtype.Map, arg any, depth int) (v any, changed bool, err error) {
	switch x := arg.(type) {
	case nil:
		return nil, false, nil
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return arg, false, nil
	case time.Time:
		return formatTime(x), true, nil
	case *time.Time:
		if x == nil {
			return nil, true, nil
		}
		return formatTime(*x), true, nil
	case []time.Time:
		if x == nil {
			return nil, true, nil
		}
		return formatTimeArray(x), true, nil
	case []byte:
		if x == nil {
			return nil, true, nil
		}
		return rawBytes(x), true, nil
	case [16]byte:
		return formatUUID(x), true, nil
	case driver.Valuer:
		if rv := reflect.ValueOf(arg); rv.Kind() == reflect.Pointer && rv.IsNil() {
			return nil, true, nil
		}
		if depth >= maxValuerDepth {
			return nil, false, fmt.Errorf("driver.Valuer chain deeper than %d", maxValuerDepth)
		}
		dv, err := x.Value()
		if err != nil {
			return nil, false, err
		}
		dv, _, err = resolveArg(m, dv, depth+1)
		return dv, true, err
	}

	// Anything else: pgx's own mapping if it has one, JSON for the composite Go kinds it has none for.
	if _, err := m.Encode(0, pgtype.TextFormatCode, arg, nil); err == nil {
		return arg, false, nil
	} else if !isJSONKind(arg) {
		return nil, false, err
	}
	b, jerr := json.Marshal(arg)
	if jerr != nil {
		return nil, false, fmt.Errorf("neither pgx nor encoding/json can encode it: %w", jerr)
	}
	return string(b), true, nil
}

func isJSONKind(arg any) bool {
	rv := reflect.ValueOf(arg)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return false
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Map, reflect.Struct, reflect.Slice, reflect.Array:
		return true
	}
	return false
}

// timeLayout renders the offset with seconds, which PostgreSQL accepts (zones with a historical
// offset such as +00:53:28 exist).
const timeLayout = "2006-01-02 15:04:05.999999999Z07:00:00"

// formatTime renders t in PostgreSQL's input syntax, keeping t's own offset. Years before 1 are
// written the way PostgreSQL wants them: year 0 is 1 BC.
func formatTime(t time.Time) string {
	if y := t.Year(); y <= 0 {
		return fmt.Sprintf("%04d-", 1-y) + t.Format("01-02 15:04:05.999999999Z07:00:00") + " BC"
	}
	return t.Format(timeLayout)
}

func formatTimeArray(ts []time.Time) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, t := range ts {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(formatTime(t))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

const hexDigits = "0123456789abcdef"

func formatUUID(u [16]byte) string {
	var b [36]byte
	j := 0
	for i, c := range u {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			b[j] = '-'
			j++
		}
		b[j] = hexDigits[c>>4]
		b[j+1] = hexDigits[c&0xf]
		j += 2
	}
	return string(b[:])
}

// rawArgErrorCodes are the SQLSTATEs a server answers with when it reads raw bytes as a type that has a
// binary representation of its own (PostgreSQL: XX000 "unsupported jsonb version number"; CockroachDB:
// 08P01 "expected JSONB version 1") or as invalid text.
var rawArgErrorCodes = []string{"XX000", "08P01", "22P02", "22P03", "22021", "22023", "42804"}

// withRawArgHint explains a server error of a statement that carries []byte arguments (rawArgs, 1-based):
// they travel as raw binary parameters, which only bytea, text, varchar and uuid placeholders accept. The
// returned error unwraps to err.
func withRawArgHint(err error, rawArgs []int) error {
	var pgErr *pgconn.PgError
	if len(rawArgs) == 0 || !errors.As(err, &pgErr) || !slices.Contains(rawArgErrorCodes, pgErr.Code) {
		return err
	}
	nums := make([]string, len(rawArgs))
	for i, n := range rawArgs {
		nums[i] = "$" + strconv.Itoa(n)
	}
	return fmt.Errorf("%w (pgxwire: %s %s a []byte sent as raw binary without a declared type; that is valid for bytea, text, varchar and uuid. For json, jsonb or any other type cast the placeholder explicitly, for example %s::text::jsonb, or pass a string, json.RawMessage or a map; see DOCS.md section 3.4)",
		err, strings.Join(nums, ", "), map[bool]string{true: "is", false: "are"}[len(nums) == 1], nums[0])
}
