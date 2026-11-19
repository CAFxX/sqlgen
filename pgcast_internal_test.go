package sqlgen

import "testing"

func TestPGCastForGoType(t *testing.T) {
	for goType, want := range map[string]string{
		"int": "bigint", "int64": "bigint", "uint32": "bigint", "*int64": "bigint",
		"int32": "integer", "rune": "integer", "uint16": "integer",
		"int16": "smallint", "int8": "smallint", "uint8": "smallint", "byte": "smallint",
		"string": "text", "*string": "text", "bool": "boolean",
		"float64": "double precision", "float32": "real",
		"[]byte": "bytea", "[]uint8": "bytea", "time.Time": "timestamptz",
		"uint64": "", "sql.NullString": "", "json.RawMessage": "", "MyEnum": "",
	} {
		if got := pgCastForGoType(goType); got != want {
			t.Errorf("pgCastForGoType(%q) = %q, want %q", goType, got, want)
		}
	}
}
