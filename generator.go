package sqlgen

import (
	"bytes"
	"errors"
	"fmt"
	"go/format"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/CAFxX/sqlgen/runtime"
	"go.yaml.in/yaml/v3"
)

// Re-exported runtime types and helpers for convenience when importing the root sqlgen package.
type (
	Query           = runtime.Query
	Order           = runtime.Order
	Seek            = runtime.Seek
	Dialect         = runtime.Dialect
	StmtCache       = runtime.StmtCache
	TxStmtCache     = runtime.TxStmtCache
	ManagedStmt     = runtime.ManagedStmt
	CacheConfig     = runtime.CacheConfig
	CacheOption     = runtime.CacheOption
	CacheStats      = runtime.CacheStats
	EntryState      = runtime.EntryState
	VariantKey      = runtime.VariantKey
	QueryDescriptor = runtime.QueryDescriptor
	TupleDescriptor = runtime.TupleDescriptor
	Slot            = runtime.Slot
	SlotKind        = runtime.SlotKind
	RenderInput     = runtime.RenderInput
)

const (
	DialectPostgres = runtime.DialectPostgres
	DialectQuestion = runtime.DialectQuestion

	StateAbsent   = runtime.StateAbsent
	StatePartial  = runtime.StatePartial
	StatePrepared = runtime.StatePrepared
)

var (
	ErrEmptySliceParam = runtime.ErrEmptySliceParam
	ErrNilQuery        = runtime.ErrNilQuery
	ErrInvalidSeek     = runtime.ErrInvalidSeek
	ErrInvalidOrder    = runtime.ErrInvalidOrder

	NewStmtCache           = runtime.NewStmtCache
	NewTxStmtCache         = runtime.NewTxStmtCache
	WithMaxSQLVariants     = runtime.WithMaxSQLVariants
	WithMaxPartialBytes    = runtime.WithMaxPartialBytes
	WithMaxPreparedStmts   = runtime.WithMaxPreparedStmts
	WithPromoteThreshold   = runtime.WithPromoteThreshold
	NewSeek                = runtime.NewSeek
	PackOrder              = runtime.PackOrder
	ExpandOrder            = runtime.ExpandOrder
	Asc                    = runtime.Asc
	Desc                   = runtime.Desc
	MakeVariantKey         = runtime.MakeVariantKey
	ComputeSubqueryShape   = runtime.ComputeSubqueryShape
)

// StatementKind specifies the execution and return cardinality of a SQL statement.
type StatementKind string

const (
	KindExec     StatementKind = "exec"
	KindOne      StatementKind = "one"
	KindMany     StatementKind = "many"
	KindSubquery StatementKind = "subquery"
	KindQuery    StatementKind = "query" // alias for KindSubquery
)

// Config represents the top-level YAML configuration schema (sqlgen.yaml).
type Config struct {
	Version       string          `yaml:"version"`
	PackageName   string          `yaml:"package"`
	Out           string          `yaml:"out"`
	RuntimeImport string          `yaml:"runtime_import"`
	Dialect       string          `yaml:"dialect"`
	ExtraImports  []string        `yaml:"extra_imports"`
	Statements    []StatementSpec `yaml:"statements"`
}

// StatementSpec defines a single SQL statement in the YAML configuration.
type StatementSpec struct {
	Name         string        `yaml:"name"`
	Kind         StatementKind `yaml:"kind"`
	ResultType   string        `yaml:"result_type"`
	ParamsStruct bool          `yaml:"params_struct"`
	SQL          string        `yaml:"sql"`
	Params       []ParamSpec   `yaml:"params"`
	Columns      []ColumnSpec  `yaml:"columns"`
}

// ParamSpec defines a fallback parameter declaration in YAML.
type ParamSpec struct {
	Name  string
	Type  string
	Slice bool
}

// UnmarshalYAML supports 3 YAML forms for ParamSpec:
// 1. Mapping: { name: ID, type: int64, slice: true }
// 2. Single-key shorthand map: - ID: int64 (or - org_ids...: int64)
// 3. Compact scalar string: - "ID:int64" (or - "org_ids...:int64")
func (p *ParamSpec) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		raw := strings.TrimSpace(value.Value)
		colon := strings.IndexByte(raw, ':')
		if colon <= 0 || colon == len(raw)-1 {
			return fmt.Errorf("line %d: invalid compact param spec %q (expected \"name:type\")", value.Line, raw)
		}
		namePart := strings.TrimSpace(raw[:colon])
		typePart := strings.TrimSpace(raw[colon+1:])
		isSlice := false
		if strings.HasSuffix(namePart, "...") {
			isSlice = true
			namePart = strings.TrimSpace(strings.TrimSuffix(namePart, "..."))
		}
		if strings.HasSuffix(typePart, "...") {
			isSlice = true
			typePart = strings.TrimSpace(strings.TrimSuffix(typePart, "..."))
		}
		if namePart == "" || typePart == "" {
			return fmt.Errorf("line %d: invalid compact param spec %q", value.Line, raw)
		}
		p.Name = namePart
		p.Type = typePart
		p.Slice = isSlice
		return nil

	case yaml.MappingNode:
		if len(value.Content) == 2 {
			kNode := value.Content[0]
			vNode := value.Content[1]
			if kNode.Value != "name" && kNode.Value != "type" && kNode.Value != "slice" {
				if vNode.Kind != yaml.ScalarNode {
					return fmt.Errorf("line %d: shorthand param %q value must be a scalar type", vNode.Line, kNode.Value)
				}
				namePart := strings.TrimSpace(kNode.Value)
				typePart := strings.TrimSpace(vNode.Value)
				isSlice := false
				if strings.HasSuffix(namePart, "...") {
					isSlice = true
					namePart = strings.TrimSpace(strings.TrimSuffix(namePart, "..."))
				}
				if strings.HasSuffix(typePart, "...") {
					isSlice = true
					typePart = strings.TrimSpace(strings.TrimSuffix(typePart, "..."))
				}
				if namePart == "" || typePart == "" {
					return fmt.Errorf("line %d: shorthand param requires non-empty name and type", kNode.Line)
				}
				p.Name = namePart
				p.Type = typePart
				p.Slice = isSlice
				return nil
			}
		}

		var name, typ string
		var slice bool
		for i := 0; i+1 < len(value.Content); i += 2 {
			kNode := value.Content[i]
			vNode := value.Content[i+1]
			switch kNode.Value {
			case "name":
				if vNode.Kind != yaml.ScalarNode {
					return fmt.Errorf("line %d: param \"name\" must be a string", vNode.Line)
				}
				name = strings.TrimSpace(vNode.Value)
			case "type":
				if vNode.Kind != yaml.ScalarNode {
					return fmt.Errorf("line %d: param \"type\" must be a string", vNode.Line)
				}
				typ = strings.TrimSpace(vNode.Value)
			case "slice":
				if err := vNode.Decode(&slice); err != nil {
					return fmt.Errorf("line %d: param \"slice\" must be a boolean: %w", vNode.Line, err)
				}
			default:
				return fmt.Errorf("line %d: unknown field %q in param spec", kNode.Line, kNode.Value)
			}
		}
		if strings.HasSuffix(name, "...") {
			slice = true
			name = strings.TrimSpace(strings.TrimSuffix(name, "..."))
		}
		if strings.HasSuffix(typ, "...") {
			slice = true
			typ = strings.TrimSpace(strings.TrimSuffix(typ, "..."))
		}
		if name == "" || typ == "" {
			return fmt.Errorf("line %d: param mapping requires both \"name\" and \"type\"", value.Line)
		}
		p.Name = name
		p.Type = typ
		p.Slice = slice
		return nil

	default:
		return fmt.Errorf("line %d: invalid param spec YAML node kind", value.Line)
	}
}

// ColumnSpec defines a fallback output column declaration in YAML.
type ColumnSpec struct {
	Name string
	Type string
}

// UnmarshalYAML supports 3 YAML forms for ColumnSpec:
// 1. Mapping: { name: id, type: int64 }
// 2. Single-key shorthand map: - id: int64
// 3. Compact scalar string: - "id:int64"
func (c *ColumnSpec) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		raw := strings.TrimSpace(value.Value)
		colon := strings.IndexByte(raw, ':')
		if colon <= 0 || colon == len(raw)-1 {
			return fmt.Errorf("line %d: invalid compact column spec %q (expected \"name:type\")", value.Line, raw)
		}
		namePart := strings.TrimSpace(raw[:colon])
		typePart := strings.TrimSpace(raw[colon+1:])
		if namePart == "" || typePart == "" {
			return fmt.Errorf("line %d: invalid compact column spec %q", value.Line, raw)
		}
		c.Name = namePart
		c.Type = typePart
		return nil

	case yaml.MappingNode:
		if len(value.Content) == 2 {
			kNode := value.Content[0]
			vNode := value.Content[1]
			if kNode.Value != "name" && kNode.Value != "type" {
				if vNode.Kind != yaml.ScalarNode {
					return fmt.Errorf("line %d: shorthand column %q value must be a scalar type", vNode.Line, kNode.Value)
				}
				namePart := strings.TrimSpace(kNode.Value)
				typePart := strings.TrimSpace(vNode.Value)
				if namePart == "" || typePart == "" {
					return fmt.Errorf("line %d: shorthand column requires non-empty name and type", kNode.Line)
				}
				c.Name = namePart
				c.Type = typePart
				return nil
			}
		}

		var name, typ string
		for i := 0; i+1 < len(value.Content); i += 2 {
			kNode := value.Content[i]
			vNode := value.Content[i+1]
			switch kNode.Value {
			case "name":
				if vNode.Kind != yaml.ScalarNode {
					return fmt.Errorf("line %d: column \"name\" must be a string", vNode.Line)
				}
				name = strings.TrimSpace(vNode.Value)
			case "type":
				if vNode.Kind != yaml.ScalarNode {
					return fmt.Errorf("line %d: column \"type\" must be a string", vNode.Line)
				}
				typ = strings.TrimSpace(vNode.Value)
			default:
				return fmt.Errorf("line %d: unknown field %q in column spec", kNode.Line, kNode.Value)
			}
		}
		if name == "" || typ == "" {
			return fmt.Errorf("line %d: column mapping requires both \"name\" and \"type\"", value.Line)
		}
		c.Name = name
		c.Type = typ
		return nil

	default:
		return fmt.Errorf("line %d: invalid column spec YAML node kind", value.Line)
	}
}

// ParseYAML strictly decodes a sqlgen YAML configuration using KnownFields(true).
func ParseYAML(data []byte) (*Config, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("sqlgen: empty YAML configuration")
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("sqlgen: parse YAML: %w", err)
	}
	return &cfg, nil
}

// GenerateFromYAML parses YAML configuration bytes and generates formatted Go source code.
func GenerateFromYAML(data []byte) ([]byte, error) {
	cfg, err := ParseYAML(data)
	if err != nil {
		return nil, err
	}
	return Generate(cfg)
}

// GenerateFile reads a YAML file from cfgPath, generates Go source code, and writes it to cfg.Out.
func GenerateFile(cfgPath string) error {
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("sqlgen: read %s: %w", cfgPath, err)
	}
	cfg, err := ParseYAML(data)
	if err != nil {
		return err
	}
	src, err := Generate(cfg)
	if err != nil {
		return err
	}
	outPath := cfg.Out
	if outPath == "" {
		outPath = "sqlgen_gen.go"
	}
	if !filepath.IsAbs(outPath) {
		outPath = filepath.Join(filepath.Dir(cfgPath), outPath)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return fmt.Errorf("sqlgen: create output directory: %w", err)
	}
	if err := os.WriteFile(outPath, src, 0o644); err != nil {
		return fmt.Errorf("sqlgen: write %s: %w", outPath, err)
	}
	return nil
}

var predeclaredGoTypes = []string{
	"bool", "string",
	"int", "int8", "int16", "int32", "int64",
	"uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
	"byte", "rune",
	"float32", "float64",
	"any",
}

func isPredeclaredGoType(t string) bool {
	return slices.Contains(predeclaredGoTypes, t)
}

func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			curr[j] = min(
				prev[j]+1,
				min(curr[j-1]+1, prev[j-1]+cost),
			)
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

func validateGoType(rawType string) error {
	t := strings.TrimSpace(rawType)
	if t == "" {
		return errors.New("empty Go type")
	}
	for {
		if strings.HasPrefix(t, "[]") {
			t = strings.TrimPrefix(t, "[]")
			continue
		}
		if strings.HasPrefix(t, "*") {
			t = strings.TrimPrefix(t, "*")
			continue
		}
		break
	}
	if t == "" {
		return fmt.Errorf("invalid Go type %q", rawType)
	}
	if dot := strings.IndexByte(t, '.'); dot >= 0 {
		pkg := t[:dot]
		sel := t[dot+1:]
		if !isValidGoIdent(pkg) || !isValidGoIdent(sel) {
			return fmt.Errorf("invalid qualified Go type %q", rawType)
		}
		return nil
	}
	if !isValidGoIdent(t) {
		return fmt.Errorf("invalid Go type identifier %q", rawType)
	}
	first := t[0]
	if first >= 'a' && first <= 'z' {
		if !isPredeclaredGoType(t) {
			best := ""
			bestDist := 999
			for _, cand := range predeclaredGoTypes {
				d := levenshtein(t, cand)
				if d < bestDist {
					bestDist = d
					best = cand
				}
			}
			if best != "" && bestDist <= 3 {
				return fmt.Errorf("unknown Go predeclared type %q (did you mean %q?)", t, best)
			}
			return fmt.Errorf("unknown Go predeclared type %q", t)
		}
	}
	return nil
}

func isNullableGoType(t string) bool {
	trimmed := strings.TrimSpace(t)
	if strings.HasPrefix(trimmed, "*") || strings.HasPrefix(trimmed, "[]") {
		return true
	}
	if strings.HasPrefix(trimmed, "sql.Null") {
		return true
	}
	return false
}

func isValidGoIdent(s string) bool {
	if s == "" || token.Lookup(s).IsKeyword() {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return false
			}
		} else {
			if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
				return false
			}
		}
	}
	return true
}

var commonInitialisms = map[string]string{
	"id":    "ID",
	"ids":   "IDs",
	"url":   "URL",
	"urls":  "URLs",
	"uri":   "URI",
	"uuid":  "UUID",
	"uuids": "UUIDs",
	"sql":   "SQL",
	"api":   "API",
	"http":  "HTTP",
	"https": "HTTPS",
	"ip":    "IP",
	"json":  "JSON",
	"xml":   "XML",
	"html":  "HTML",
	"cpu":   "CPU",
}

func splitIdentWords(s string) []string {
	var words []string
	for _, part := range strings.Split(s, "_") {
		if part == "" {
			continue
		}
		runes := []rune(part)
		start := 0
		for i := 1; i < len(runes); i++ {
			prev := runes[i-1]
			curr := runes[i]
			if unicode.IsLower(prev) && unicode.IsUpper(curr) {
				words = append(words, string(runes[start:i]))
				start = i
			} else if unicode.IsUpper(prev) && unicode.IsUpper(curr) && i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
				// Keep plural 's' attached to an uppercase acronym, e.g. "IDs", "URLs", "UUIDs".
				if curr == 'S' || (runes[i+1] == 's' && (i+2 == len(runes) || unicode.IsUpper(runes[i+2]))) {
					continue
				}
				words = append(words, string(runes[start:i]))
				start = i
			}
		}
		words = append(words, string(runes[start:]))
	}
	return words
}

func toGoFieldName(s string) string {
	words := splitIdentWords(s)
	if len(words) == 0 {
		return "Field"
	}
	var b strings.Builder
	for _, w := range words {
		low := strings.ToLower(w)
		if initCap, ok := commonInitialisms[low]; ok {
			b.WriteString(initCap)
		} else {
			runes := []rune(low)
			runes[0] = unicode.ToUpper(runes[0])
			b.WriteString(string(runes))
		}
	}
	out := b.String()
	if !unicode.IsLetter(rune(out[0])) && out[0] != '_' {
		out = "F" + out
	}
	return out
}

func toGoParamName(s string) string {
	words := splitIdentWords(s)
	if len(words) == 0 {
		return "param"
	}
	var b strings.Builder
	for i, w := range words {
		low := strings.ToLower(w)
		if i == 0 {
			b.WriteString(low)
			continue
		}
		if initCap, ok := commonInitialisms[low]; ok {
			b.WriteString(initCap)
		} else {
			runes := []rune(low)
			runes[0] = unicode.ToUpper(runes[0])
			b.WriteString(string(runes))
		}
	}
	out := b.String()
	if token.Lookup(out).IsKeyword() || out == "ctx" || out == "q" || out == "dst" || out == "fn" {
		out += "Param"
	}
	return out
}

type paramKind uint8

const (
	paramScalar paramKind = iota
	paramSlice
	paramTupleSlice
	paramSubquery
	paramOrder
	paramSeek
)

type tupleFieldDef struct {
	RawName   string
	FieldName string
	GoType    string
}

type tupleStructDef struct {
	Name     string
	Fields   []tupleFieldDef
	Segments []string
}

type paramDef struct {
	RawName    string
	FieldName  string
	ParamName  string
	Kind       paramKind
	GoType     string // full parameter Go type (e.g., int64, []int64, []UserInput, runtime.Query, runtime.Order, runtime.Seek)
	ElemGoType string // element type for paramSlice or struct name for paramTupleSlice
	Tuple      *tupleStructDef
}

type colDef struct {
	RawName   string
	FieldName string
	GoType    string
	SQLExpr   string
	CondFlag  string // empty if unconditional
	UncondIdx int    // 0..len(UncondCols)-1 if unconditional, -1 if conditional
}

type optFlagDef struct {
	RawName   string
	FieldName string
	Bit       uint8
}

type slotRef struct {
	Kind      runtime.SlotKind
	ParamIdx  int // index into compiledStmt.Params (-1 for SlotSelectCols / SlotOptBlock)
	FlagBit   uint8
	Tuple     *tupleStructDef
	Inner     *parsedBlock
}

type parsedBlock struct {
	Segments  []string
	Slots     []slotRef
	NumSlices int
	NumTuples int
	NumSubs   int
}

type orderColRef struct {
	RawRef    string // e.g., "u.created_at" or "created_at"
	ColIdx    int    // index into compiledStmt.Cols
	FieldName string
	GoType    string
}

type compiledStmt struct {
	ID              uint32
	Name            string
	Kind            StatementKind
	RowTypeName     string
	ScalarResult    bool
	UseParamsStruct bool
	CanBatch        bool
	BatchParamIdx   int
	MaxRowsPerStmt  int
	IsStatic        bool

	Root            parsedBlock
	SelectColExprs  []string
	Params          []paramDef
	Cols            []colDef
	UncondCols      []colDef
	CondCols        []colDef
	Flags           []optFlagDef
	OrderCols       []orderColRef
	HasOrder        bool
	HasSeek         bool
	TotalSlices     int
	TotalTuples     int
	TotalSubs       int
}

func parseDialect(raw string) (runtime.Dialect, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "dollar", "postgres", "postgresql", "pgx", "cockroach", "cockroachdb", "$":
		return runtime.DialectPostgres, nil
	case "question", "sqlite", "sqlite3", "mysql", "mariadb", "?":
		return runtime.DialectQuestion, nil
	default:
		return runtime.DialectPostgres, fmt.Errorf("sqlgen: unsupported dialect %q", raw)
	}
}

func isIdentStartByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_'
}

func isIdentContByte(b byte) bool {
	return isIdentStartByte(b) || (b >= '0' && b <= '9')
}

func isSpaceByte(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

var projectionTerminators = map[string]bool{
	"from":      true,
	"into":      true,
	"where":     true,
	"group":     true,
	"having":    true,
	"window":    true,
	"order":     true,
	"limit":     true,
	"offset":    true,
	"fetch":     true,
	"for":       true,
	"union":     true,
	"intersect": true,
	"except":    true,
}

// matchDollarQuoteTag checks if s[pos:] starts a valid PostgreSQL dollar-quoted tag ($$ or $tag$ where tag does not start with [0-9]).
// Returns the tag string (including both '$' delimiters) and true if matched.
func matchDollarQuoteTag(s string, pos int) (string, bool) {
	if pos >= len(s) || s[pos] != '$' {
		return "", false
	}
	if pos+1 < len(s) && s[pos+1] == '$' {
		return "$$", true
	}
	if pos+1 >= len(s) || (s[pos+1] >= '0' && s[pos+1] <= '9') || !isIdentStartByte(s[pos+1]) {
		return "", false
	}
	end := pos + 2
	for end < len(s) && isIdentContByte(s[end]) {
		end++
	}
	if end < len(s) && s[end] == '$' {
		return s[pos : end+1], true
	}
	return "", false
}

// findConditionalBlockClose finds the matching '}' for a '{?Flag: ...}' block starting at openBracePos.
func findConditionalBlockClose(s string, bodyStart int) (int, error) {
	i := bodyStart
	for i < len(s) {
		ch := s[i]
		if ch == '\'' {
			i++
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '"' {
			i++
			for i < len(s) {
				if s[i] == '"' {
					if i+1 < len(s) && s[i+1] == '"' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '`' {
			i++
			for i < len(s) {
				if s[i] == '`' {
					if i+1 < len(s) && s[i+1] == '`' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '$' {
			if tag, ok := matchDollarQuoteTag(s, i); ok {
				closeIdx := strings.Index(s[i+len(tag):], tag)
				if closeIdx < 0 {
					return -1, errors.New("unclosed dollar-quoted string literal inside conditional block")
				}
				i += len(tag) + closeIdx + len(tag)
				continue
			}
		}
		if ch == '-' && i+1 < len(s) && s[i+1] == '-' {
			i += 2
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if ch == '/' && i+1 < len(s) && s[i+1] == '*' {
			closeIdx := strings.Index(s[i+2:], "*/")
			if closeIdx < 0 {
				return -1, errors.New("unclosed block comment inside conditional block")
			}
			i += 2 + closeIdx + 2
			continue
		}
		if ch == '{' && i+1 < len(s) && s[i+1] == '?' {
			return -1, errors.New("nested conditional {?...} blocks are not allowed")
		}
		if ch == '}' {
			return i, nil
		}
		i++
	}
	return -1, errors.New("unclosed conditional {?...} block")
}

// isEndOfProjectionItem implements the exact lookahead algorithm from Section 4.3.
func isEndOfProjectionItem(span string, pos int) bool {
	i := pos
	for i < len(span) {
		if isSpaceByte(span[i]) {
			i++
			continue
		}
		if span[i] == '-' && i+1 < len(span) && span[i+1] == '-' {
			i += 2
			for i < len(span) && span[i] != '\n' {
				i++
			}
			continue
		}
		if span[i] == '/' && i+1 < len(span) && span[i+1] == '*' {
			closeIdx := strings.Index(span[i+2:], "*/")
			if closeIdx < 0 {
				return false
			}
			i += 2 + closeIdx + 2
			continue
		}
		if span[i] == '{' && i+1 < len(span) && span[i+1] == '?' {
			closeBrace, err := findConditionalBlockClose(span, i+2)
			if err != nil {
				return false
			}
			i = closeBrace + 1
			continue
		}
		break
	}
	if i >= len(span) || span[i] == ',' || span[i] == ';' {
		return true
	}
	if isIdentStartByte(span[i]) {
		wEnd := i + 1
		for wEnd < len(span) && isIdentContByte(span[wEnd]) {
			wEnd++
		}
		if wEnd < len(span) && span[wEnd] == ':' {
			return false
		}
		w := strings.ToLower(span[i:wEnd])
		return projectionTerminators[w]
	}
	return false
}

// scanGoType scans a Go type annotation starting at pos: { "[]" | "*" } , [ identifier , "." ] , identifier , [ "..." ]
func scanGoType(s string, pos int) (goType string, hasEllipsis bool, nextPos int, ok bool) {
	i := pos
	for i < len(s) {
		if strings.HasPrefix(s[i:], "[]") {
			i += 2
			continue
		}
		if s[i] == '*' {
			i++
			continue
		}
		break
	}
	if i >= len(s) || !isIdentStartByte(s[i]) {
		return "", false, pos, false
	}
	id1End := i + 1
	for id1End < len(s) && isIdentContByte(s[id1End]) {
		id1End++
	}
	end := id1End
	if end < len(s) && s[end] == '.' && !strings.HasPrefix(s[end:], "...") {
		if end+1 < len(s) && isIdentStartByte(s[end+1]) {
			id2End := end + 2
			for id2End < len(s) && isIdentContByte(s[id2End]) {
				id2End++
			}
			end = id2End
		} else {
			return "", false, pos, false
		}
	}
	goType = s[pos:end]
	if strings.HasPrefix(s[end:], "...") {
		hasEllipsis = true
		end += 3
	}
	return goType, hasEllipsis, end, true
}

func normalizeScalarOrSliceType(rawType string, explicitSlice bool) (isSlice bool, fullGoType string, elemGoType string, err error) {
	t := strings.TrimSpace(rawType)
	if err := validateGoType(t); err != nil {
		return false, "", "", err
	}
	if !explicitSlice && (t == "[]byte" || t == "[]uint8") {
		return false, t, "", nil
	}
	if strings.HasPrefix(t, "[]") {
		elem := strings.TrimPrefix(t, "[]")
		if explicitSlice && (t == "[]byte" || t == "[]uint8") {
			return true, "[][]byte", t, nil
		}
		return true, t, elem, nil
	}
	if explicitSlice {
		return true, "[]" + t, t, nil
	}
	return false, t, "", nil
}

// findMatchingParen finds the matching ')' for '(' at openPos, respecting SQL quotes and comments.
func findMatchingParen(s string, openPos int) (int, error) {
	depth := 1
	i := openPos + 1
	for i < len(s) {
		ch := s[i]
		if ch == '\'' {
			i++
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '"' {
			i++
			for i < len(s) {
				if s[i] == '"' {
					if i+1 < len(s) && s[i+1] == '"' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '`' {
			i++
			for i < len(s) {
				if s[i] == '`' {
					if i+1 < len(s) && s[i+1] == '`' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if ch == '$' {
			if tag, ok := matchDollarQuoteTag(s, i); ok {
				closeIdx := strings.Index(s[i+len(tag):], tag)
				if closeIdx < 0 {
					return -1, errors.New("unclosed dollar-quoted string inside parentheses")
				}
				i += len(tag) + closeIdx + len(tag)
				continue
			}
		}
		if ch == '-' && i+1 < len(s) && s[i+1] == '-' {
			i += 2
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if ch == '/' && i+1 < len(s) && s[i+1] == '*' {
			closeIdx := strings.Index(s[i+2:], "*/")
			if closeIdx < 0 {
				return -1, errors.New("unclosed block comment inside parentheses")
			}
			i += 2 + closeIdx + 2
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				return i, nil
			}
		}
		i++
	}
	return -1, errors.New("unclosed parenthesis")
}

// parseTupleBody extracts Field:GoType tokens inside a tuple slice body while preserving
// all surrounding SQL text (casts, functions, nested parentheses, quotes) in Segments.
func parseTupleBody(structName, body string) (*tupleStructDef, error) {
	if !isValidGoIdent(structName) {
		return nil, fmt.Errorf("invalid tuple struct name %q", structName)
	}
	td := &tupleStructDef{
		Name: structName,
	}
	var seg strings.Builder
	seenFields := make(map[string]bool)
	i := 0
	for i < len(body) {
		ch := body[i]
		// Protected contexts inside tuple body
		if ch == '\'' {
			start := i
			i++
			closed := false
			for i < len(body) {
				if body[i] == '\'' {
					if i+1 < len(body) && body[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, errors.New("unclosed single-quoted literal in tuple body")
			}
			seg.WriteString(body[start:i])
			continue
		}
		if ch == '"' {
			start := i
			i++
			closed := false
			for i < len(body) {
				if body[i] == '"' {
					if i+1 < len(body) && body[i+1] == '"' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, errors.New("unclosed double-quoted identifier in tuple body")
			}
			seg.WriteString(body[start:i])
			continue
		}
		if ch == '`' {
			start := i
			i++
			closed := false
			for i < len(body) {
				if body[i] == '`' {
					if i+1 < len(body) && body[i+1] == '`' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, errors.New("unclosed backtick identifier in tuple body")
			}
			seg.WriteString(body[start:i])
			continue
		}
		if ch == ':' && i+1 < len(body) && (body[i+1] == ':' || body[i+1] == '=') {
			seg.WriteString(body[i : i+2])
			i += 2
			continue
		}
		if isIdentStartByte(ch) && (i == 0 || !isIdentContByte(body[i-1])) {
			wEnd := i + 1
			for wEnd < len(body) && isIdentContByte(body[wEnd]) {
				wEnd++
			}
			ident := body[i:wEnd]
			if wEnd < len(body) && body[wEnd] == ':' && (wEnd+1 >= len(body) || (body[wEnd+1] != ':' && body[wEnd+1] != '=')) {
				goType, hasEllipsis, nextPos, ok := scanGoType(body, wEnd+1)
				if !ok || hasEllipsis {
					return nil, fmt.Errorf("invalid tuple field type annotation for %q in %s", ident, structName)
				}
				if err := validateGoType(goType); err != nil {
					return nil, fmt.Errorf("tuple %s field %q: %w", structName, ident, err)
				}
				if strings.HasPrefix(goType, "[]") && goType != "[]byte" && goType != "[]uint8" {
					return nil, fmt.Errorf("tuple %s field %q cannot be slice type %q", structName, ident, goType)
				}
				fieldName := toGoFieldName(ident)
				if seenFields[fieldName] {
					return nil, fmt.Errorf("duplicate field %q in tuple struct %s", fieldName, structName)
				}
				seenFields[fieldName] = true
				td.Segments = append(td.Segments, seg.String())
				seg.Reset()
				td.Fields = append(td.Fields, tupleFieldDef{
					RawName:   ident,
					FieldName: fieldName,
					GoType:    goType,
				})
				i = nextPos
				continue
			}
			seg.WriteString(ident)
			i = wEnd
			continue
		}
		seg.WriteByte(ch)
		i++
	}
	td.Segments = append(td.Segments, seg.String())
	if len(td.Fields) == 0 {
		return nil, fmt.Errorf("tuple struct %s must contain at least one Field:Type annotation", structName)
	}
	return td, nil
}

type stmtParser struct {
	spec          StatementSpec
	yamlParams    map[string]ParamSpec
	paramsByName  map[string]int
	flagsByName   map[string]uint8
	rawOrderCols  []string
	inOptBlock    bool
	currentFlag   string

	parenDepth    int
	bracketDepth  int
	seenMainSel   bool
	inProjection  bool

	// Tracking for unconditional SELECT/RETURNING projection items
	projActive         bool
	projStartSegIdx    int
	projStartByteIdx   int
	projEndSegIdx      int
	projEndByteIdx     int
	projItemStartSeg   int
	projItemStartByte  int
	projHasParamSlot   bool
	projHasOptBlock    bool

	cs *compiledStmt
}

func (p *stmtParser) addOrGetParam(rawName string, kind paramKind, goType, elemGoType string, tuple *tupleStructDef) (int, error) {
	fieldName := toGoFieldName(rawName)
	for idx, existing := range p.cs.Params {
		if existing.FieldName == fieldName || existing.RawName == rawName {
			if existing.Kind != kind || existing.GoType != goType {
				return -1, fmt.Errorf("statement %s: parameter %q redeclared with conflicting types (%s vs %s)", p.spec.Name, rawName, existing.GoType, goType)
			}
			return idx, nil
		}
	}
	idx := len(p.cs.Params)
	p.cs.Params = append(p.cs.Params, paramDef{
		RawName:    rawName,
		FieldName:  fieldName,
		ParamName:  toGoParamName(rawName),
		Kind:       kind,
		GoType:     goType,
		ElemGoType: elemGoType,
		Tuple:      tuple,
	})
	p.paramsByName[rawName] = idx
	return idx, nil
}

func (p *stmtParser) addOrGetFlag(rawFlag string) (uint8, error) {
	fieldName := toGoFieldName(rawFlag)
	for _, f := range p.cs.Flags {
		if f.FieldName == fieldName || f.RawName == rawFlag {
			return f.Bit, nil
		}
	}
	if len(p.cs.Flags) >= 64 {
		return 0, fmt.Errorf("statement %s: exceeds maximum of 64 conditional flags", p.spec.Name)
	}
	bit := uint8(len(p.cs.Flags))
	p.cs.Flags = append(p.cs.Flags, optFlagDef{
		RawName:   rawFlag,
		FieldName: fieldName,
		Bit:       bit,
	})
	p.flagsByName[rawFlag] = bit
	return bit, nil
}

func (p *stmtParser) addColumn(rawName, goType, sqlExpr string) error {
	if err := validateGoType(goType); err != nil {
		return fmt.Errorf("statement %s column %q: %w", p.spec.Name, rawName, err)
	}
	fieldName := toGoFieldName(rawName)
	for _, c := range p.cs.Cols {
		if c.FieldName == fieldName || strings.EqualFold(c.RawName, rawName) {
			return fmt.Errorf("statement %s: duplicate output column %q", p.spec.Name, rawName)
		}
	}
	col := colDef{
		RawName:   rawName,
		FieldName: fieldName,
		GoType:    goType,
		SQLExpr:   strings.TrimSpace(sqlExpr),
		CondFlag:  p.currentFlag,
		UncondIdx: -1,
	}
	if p.currentFlag == "" {
		col.UncondIdx = len(p.cs.UncondCols)
		p.cs.UncondCols = append(p.cs.UncondCols, col)
	} else {
		p.cs.CondCols = append(p.cs.CondCols, col)
	}
	p.cs.Cols = append(p.cs.Cols, col)
	return nil
}

func extractCurrentItemExpr(segments []string, curSeg string, itemSegIdx, itemByteIdx int) string {
	var b strings.Builder
	allSegs := append(slices.Clone(segments), curSeg)
	for sIdx := itemSegIdx; sIdx < len(allSegs); sIdx++ {
		seg := allSegs[sIdx]
		if sIdx == itemSegIdx {
			if itemByteIdx < len(seg) {
				b.WriteString(seg[itemByteIdx:])
			}
		} else {
			b.WriteString(seg)
		}
	}
	return strings.TrimSpace(b.String())
}

func (p *stmtParser) scanSpan(span string, blk *parsedBlock, isTopLevel bool) error {
	var seg strings.Builder
	i := 0

	emitSlot := func(s slotRef) {
		if p.inProjection && !p.inOptBlock && s.Kind != runtime.SlotOptBlock && !p.projHasOptBlock {
			p.projHasParamSlot = true
		}
		blk.Segments = append(blk.Segments, seg.String())
		seg.Reset()
		blk.Slots = append(blk.Slots, s)
		switch s.Kind {
		case runtime.SlotSlice:
			blk.NumSlices++
			p.cs.TotalSlices++
		case runtime.SlotTupleSlice:
			blk.NumTuples++
			p.cs.TotalTuples++
		case runtime.SlotQuery:
			blk.NumSubs++
			p.cs.TotalSubs++
		case runtime.SlotOptBlock:
			if s.Inner != nil {
				blk.NumSlices += s.Inner.NumSlices
				blk.NumTuples += s.Inner.NumTuples
				blk.NumSubs += s.Inner.NumSubs
			}
		}
	}

	for i < len(span) {
		ch := span[i]

		// 1. Single-quoted string literal '...'
		if ch == '\'' {
			start := i
			i++
			closed := false
			for i < len(span) {
				if span[i] == '\'' {
					if i+1 < len(span) && span[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return fmt.Errorf("statement %s: unclosed single-quoted string literal", p.spec.Name)
			}
			seg.WriteString(span[start:i])
			continue
		}

		// 2. Double-quoted identifier "..."
		if ch == '"' {
			start := i
			i++
			closed := false
			for i < len(span) {
				if span[i] == '"' {
					if i+1 < len(span) && span[i+1] == '"' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return fmt.Errorf("statement %s: unclosed double-quoted identifier", p.spec.Name)
			}
			seg.WriteString(span[start:i])
			continue
		}

		// 3. Backtick-quoted identifier `...`
		if ch == '`' {
			start := i
			i++
			closed := false
			for i < len(span) {
				if span[i] == '`' {
					if i+1 < len(span) && span[i+1] == '`' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return fmt.Errorf("statement %s: unclosed backtick-quoted identifier", p.spec.Name)
			}
			seg.WriteString(span[start:i])
			continue
		}

		// 4. PostgreSQL dollar-quoted string literal $$...$$ or $tag$...$tag$
		if ch == '$' {
			if tag, ok := matchDollarQuoteTag(span, i); ok {
				closeOffset := strings.Index(span[i+len(tag):], tag)
				if closeOffset < 0 {
					return fmt.Errorf("statement %s: unclosed dollar-quoted literal %s", p.spec.Name, tag)
				}
				end := i + len(tag) + closeOffset + len(tag)
				seg.WriteString(span[i:end])
				i = end
				continue
			}
		}

		// 5. Single-line comment --...\n
		if ch == '-' && i+1 < len(span) && span[i+1] == '-' {
			start := i
			i += 2
			for i < len(span) && span[i] != '\n' {
				i++
			}
			seg.WriteString(span[start:i])
			continue
		}

		// 6. Multi-line block comment /*...*/
		if ch == '/' && i+1 < len(span) && span[i+1] == '*' {
			closeOffset := strings.Index(span[i+2:], "*/")
			if closeOffset < 0 {
				return fmt.Errorf("statement %s: unclosed block comment", p.spec.Name)
			}
			end := i + 2 + closeOffset + 2
			seg.WriteString(span[i:end])
			i = end
			continue
		}

		// 7. Bracket tracking [ ... ]
		if ch == '[' {
			p.bracketDepth++
			seg.WriteByte(ch)
			i++
			continue
		}
		if ch == ']' {
			if p.bracketDepth <= 0 {
				return fmt.Errorf("statement %s: unmatched closing bracket ']'", p.spec.Name)
			}
			p.bracketDepth--
			seg.WriteByte(ch)
			i++
			continue
		}

		// If inside brackets, copy verbatim without interpreting ':' or '{?...}'
		if p.bracketDepth > 0 {
			seg.WriteByte(ch)
			i++
			continue
		}

		// Parenthesis depth tracking
		if ch == '(' {
			p.parenDepth++
			seg.WriteByte(ch)
			i++
			continue
		}
		if ch == ')' {
			if p.parenDepth <= 0 {
				return fmt.Errorf("statement %s: unmatched closing parenthesis ')'", p.spec.Name)
			}
			p.parenDepth--
			seg.WriteByte(ch)
			i++
			continue
		}

		// Track top-level comma separating unconditional projection items
		if ch == ',' && p.inProjection && p.parenDepth == 0 && p.bracketDepth == 0 && !p.inOptBlock {
			seg.WriteByte(ch)
			i++
			p.projItemStartSeg = len(blk.Segments)
			p.projItemStartByte = seg.Len()
			continue
		}

		// 8. Colon operators :: and :=
		if ch == ':' && i+1 < len(span) && (span[i+1] == ':' || span[i+1] == '=') {
			seg.WriteString(span[i : i+2])
			i += 2
			continue
		}

		// Conditional block {?FlagName: body }
		if ch == '{' && i+1 < len(span) && span[i+1] == '?' {
			if p.inOptBlock {
				return fmt.Errorf("statement %s: nested conditional {?...} blocks are not allowed", p.spec.Name)
			}
			flagStart := i + 2
			if flagStart >= len(span) || !isIdentStartByte(span[flagStart]) {
				return fmt.Errorf("statement %s: expected flag identifier after '{?'", p.spec.Name)
			}
			flagEnd := flagStart + 1
			for flagEnd < len(span) && isIdentContByte(span[flagEnd]) {
				flagEnd++
			}
			flagName := span[flagStart:flagEnd]
			if flagEnd >= len(span) || span[flagEnd] != ':' {
				return fmt.Errorf("statement %s: expected ':' after conditional flag %q", p.spec.Name, flagName)
			}
			bodyStart := flagEnd + 1
			closeBrace, err := findConditionalBlockClose(span, bodyStart)
			if err != nil {
				return fmt.Errorf("statement %s: %w", p.spec.Name, err)
			}
			flagBit, err := p.addOrGetFlag(flagName)
			if err != nil {
				return err
			}
			if p.inProjection && p.parenDepth == 0 {
				p.projHasOptBlock = true
			}

			innerBody := span[bodyStart:closeBrace]
			innerBlk := &parsedBlock{}
			p.inOptBlock = true
			p.currentFlag = flagName
			if err := p.scanSpan(innerBody, innerBlk, false); err != nil {
				return err
			}
			p.inOptBlock = false
			p.currentFlag = ""

			emitSlot(slotRef{
				Kind:     runtime.SlotOptBlock,
				ParamIdx: -1,
				FlagBit:  flagBit,
				Inner:    innerBlk,
			})
			i = closeBrace + 1
			continue
		}

		// Annotated token with leading ':' or normal word / annotated token without leading ':'
		hasLeadingColon := false
		tokenStart := i
		identStart := i
		if ch == ':' && i+1 < len(span) && isIdentStartByte(span[i+1]) && (i == 0 || !isIdentContByte(span[i-1])) {
			hasLeadingColon = true
			identStart = i + 1
		}

		if hasLeadingColon || (isIdentStartByte(ch) && (i == 0 || !isIdentContByte(span[i-1]))) {
			identEnd := identStart + 1
			for identEnd < len(span) && isIdentContByte(span[identEnd]) {
				identEnd++
			}
			ident := span[identStart:identEnd]
			afterIdent := identEnd
			hasPreEllipsis := false
			if strings.HasPrefix(span[afterIdent:], "...") {
				hasPreEllipsis = true
				afterIdent += 3
			}

			hasTypeColon := afterIdent < len(span) && span[afterIdent] == ':' &&
				(afterIdent+1 >= len(span) || (span[afterIdent+1] != ':' && span[afterIdent+1] != '='))

			if !hasTypeColon {
				if hasLeadingColon {
					// yaml_ref_param (:ident or :ident...)
					yp, ok := p.yamlParams[ident]
					if !ok {
						if existingIdx, exists := p.paramsByName[ident]; exists {
							ep := p.cs.Params[existingIdx]
							sk := runtime.SlotScalar
							if ep.Kind == paramSlice {
								sk = runtime.SlotSlice
							}
							emitSlot(slotRef{Kind: sk, ParamIdx: existingIdx})
							i = afterIdent
							continue
						}
						return fmt.Errorf("statement %s: parameter %q referenced as :%s is not declared in params", p.spec.Name, ident, ident)
					}
					isSlice, fullType, elemType, err := normalizeScalarOrSliceType(yp.Type, yp.Slice || hasPreEllipsis)
					if err != nil {
						return fmt.Errorf("statement %s param %q: %w", p.spec.Name, ident, err)
					}
					pk := paramScalar
					sk := runtime.SlotScalar
					if isSlice {
						pk = paramSlice
						sk = runtime.SlotSlice
					}
					pIdx, err := p.addOrGetParam(ident, pk, fullType, elemType, nil)
					if err != nil {
						return err
					}
					emitSlot(slotRef{Kind: sk, ParamIdx: pIdx})
					i = afterIdent
					continue
				}

				// Plain SQL word (identifier or keyword)
				if p.parenDepth == 0 && p.bracketDepth == 0 {
					low := strings.ToLower(ident)
					if low == "select" && !p.seenMainSel {
						p.inProjection = true
						p.seenMainSel = true
						seg.WriteString(span[tokenStart:identEnd])
						i = identEnd
						for i < len(span) && isSpaceByte(span[i]) {
							seg.WriteByte(span[i])
							i++
						}
						// Handle optional DISTINCT keyword right after SELECT
						if i+8 <= len(span) && strings.EqualFold(span[i:i+8], "distinct") && (i+8 == len(span) || !isIdentContByte(span[i+8])) {
							seg.WriteString(span[i : i+8])
							i += 8
							for i < len(span) && isSpaceByte(span[i]) {
								seg.WriteByte(span[i])
								i++
							}
						}
						if isTopLevel {
							p.projActive = true
							p.projStartSegIdx = len(blk.Segments)
							p.projStartByteIdx = seg.Len()
							p.projItemStartSeg = p.projStartSegIdx
							p.projItemStartByte = p.projStartByteIdx
						}
						continue
					} else if low == "returning" {
						p.inProjection = true
						seg.WriteString(span[tokenStart:identEnd])
						i = identEnd
						for i < len(span) && isSpaceByte(span[i]) {
							seg.WriteByte(span[i])
							i++
						}
						if isTopLevel && len(p.cs.UncondCols) == 0 {
							p.projActive = true
							p.projStartSegIdx = len(blk.Segments)
							p.projStartByteIdx = seg.Len()
							p.projItemStartSeg = p.projStartSegIdx
							p.projItemStartByte = p.projStartByteIdx
						}
						continue
					} else if p.inProjection && projectionTerminators[low] {
						p.inProjection = false
					}
				}

				seg.WriteString(span[tokenStart:identEnd])
				i = identEnd
				continue
			}

			// We have ':' at afterIdent! Parse rhs starting at rhsStart = afterIdent + 1
			rhsStart := afterIdent + 1
			rhs := span[rhsStart:]

			// 1. Check tuple_slice_param: ":" "[]" go_ident "(" tuple_body ")"
			if !hasPreEllipsis && strings.HasPrefix(rhs, "[]") {
				stStart := rhsStart + 2
				if stStart < len(span) && isIdentStartByte(span[stStart]) {
					stEnd := stStart + 1
					for stEnd < len(span) && isIdentContByte(span[stEnd]) {
						stEnd++
					}
					if stEnd < len(span) && span[stEnd] == '(' {
						structName := span[stStart:stEnd]
						closeParen, err := findMatchingParen(span, stEnd)
						if err != nil {
							return fmt.Errorf("statement %s tuple %s: %w", p.spec.Name, ident, err)
						}
						tupleBody := span[stEnd+1 : closeParen]
						td, err := parseTupleBody(structName, tupleBody)
						if err != nil {
							return fmt.Errorf("statement %s: %w", p.spec.Name, err)
						}
						pIdx, err := p.addOrGetParam(ident, paramTupleSlice, "[]"+structName, structName, td)
						if err != nil {
							return err
						}
						emitSlot(slotRef{
							Kind:     runtime.SlotTupleSlice,
							ParamIdx: pIdx,
							Tuple:    td,
						})
						i = closeParen + 1
						continue
					}
				}
			}

			// 2. Check order_directive: (sqlgen.Order|runtime.Order)(col1, col2, ...)
			if !hasPreEllipsis {
				orderPrefix := ""
				if strings.HasPrefix(rhs, "sqlgen.Order(") {
					orderPrefix = "sqlgen.Order"
				} else if strings.HasPrefix(rhs, "runtime.Order(") {
					orderPrefix = "runtime.Order"
				}
				if orderPrefix != "" {
					openParen := rhsStart + len(orderPrefix)
					closeParen, err := findMatchingParen(span, openParen)
					if err != nil {
						return fmt.Errorf("statement %s Order directive: %w", p.spec.Name, err)
					}
					colListStr := strings.TrimSpace(span[openParen+1 : closeParen])
					if colListStr == "" {
						return fmt.Errorf("statement %s: sqlgen.Order(...) requires at least one column", p.spec.Name)
					}
					parts := strings.Split(colListStr, ",")
					if len(parts) > 15 {
						return fmt.Errorf("statement %s: sqlgen.Order(...) supports at most 15 columns (got %d)", p.spec.Name, len(parts))
					}
					var cols []string
					for _, pt := range parts {
						cRef := strings.TrimSpace(pt)
						if cRef == "" {
							return fmt.Errorf("statement %s: empty column in sqlgen.Order(...)", p.spec.Name)
						}
						cols = append(cols, cRef)
					}
					p.rawOrderCols = cols
					p.cs.HasOrder = true
					pIdx, err := p.addOrGetParam(ident, paramOrder, "runtime.Order", "", nil)
					if err != nil {
						return err
					}
					emitSlot(slotRef{
						Kind:     runtime.SlotOrder,
						ParamIdx: pIdx,
					})
					i = closeParen + 1
					continue
				}
			}

			// Scan go_type and optional trailing '...'
			goType, hasPostEllipsis, nextPos, ok := scanGoType(span, rhsStart)
			if !ok {
				return fmt.Errorf("statement %s: invalid type annotation after %q", p.spec.Name, ident+":")
			}

			// 3. Check seek_directive: sqlgen.Seek | runtime.Seek
			if !hasPreEllipsis && !hasPostEllipsis && (goType == "sqlgen.Seek" || goType == "runtime.Seek") {
				p.cs.HasSeek = true
				pIdx, err := p.addOrGetParam(ident, paramSeek, "runtime.Seek", "", nil)
				if err != nil {
					return err
				}
				emitSlot(slotRef{
					Kind:     runtime.SlotSeek,
					ParamIdx: pIdx,
				})
				i = nextPos
				continue
			}

			// 4. Check query_param: sqlgen.Query | runtime.Query | Query
			if !hasPreEllipsis && !hasPostEllipsis && (goType == "sqlgen.Query" || goType == "runtime.Query" || goType == "Query") {
				pIdx, err := p.addOrGetParam(ident, paramSubquery, "runtime.Query", "", nil)
				if err != nil {
					return err
				}
				emitSlot(slotRef{
					Kind:     runtime.SlotQuery,
					ParamIdx: pIdx,
				})
				i = nextPos
				continue
			}

			// 5. Disambiguate Output Column vs Input Parameter (Section 4.3)
			isOutputCol := !hasLeadingColon &&
				!hasPreEllipsis &&
				!hasPostEllipsis &&
				p.inProjection &&
				p.parenDepth == 0 &&
				p.bracketDepth == 0 &&
				isEndOfProjectionItem(span, nextPos)

			if isOutputCol {
				// Write the column identifier into the SQL segment, stripping :goType
				seg.WriteString(ident)
				var colExpr string
				if !p.inOptBlock && p.projActive {
					colExpr = extractCurrentItemExpr(blk.Segments, seg.String(), p.projItemStartSeg, p.projItemStartByte)
					p.projEndSegIdx = len(blk.Segments)
					p.projEndByteIdx = seg.Len()
				} else {
					colExpr = ident
				}
				if err := p.addColumn(ident, goType, colExpr); err != nil {
					return err
				}
				i = nextPos
				continue
			}

			// Classified as Input Parameter (SlotScalar or SlotSlice)
			isSlice, fullType, elemType, err := normalizeScalarOrSliceType(goType, hasPreEllipsis || hasPostEllipsis)
			if err != nil {
				return fmt.Errorf("statement %s param %q: %w", p.spec.Name, ident, err)
			}
			pk := paramScalar
			sk := runtime.SlotScalar
			if isSlice {
				pk = paramSlice
				sk = runtime.SlotSlice
			}
			pIdx, err := p.addOrGetParam(ident, pk, fullType, elemType, nil)
			if err != nil {
				return err
			}
			emitSlot(slotRef{
				Kind:     sk,
				ParamIdx: pIdx,
			})
			i = nextPos
			continue
		}

		seg.WriteByte(ch)
		i++
	}

	if isTopLevel {
		if p.parenDepth != 0 {
			return fmt.Errorf("statement %s: unbalanced parentheses in SQL", p.spec.Name)
		}
		if p.bracketDepth != 0 {
			return fmt.Errorf("statement %s: unbalanced brackets in SQL", p.spec.Name)
		}
	}

	blk.Segments = append(blk.Segments, seg.String())
	return nil
}

func compileStatement(id uint32, spec StatementSpec) (*compiledStmt, error) {
	if !isValidGoIdent(spec.Name) {
		return nil, fmt.Errorf("sqlgen: invalid statement name %q", spec.Name)
	}
	kind := spec.Kind
	if kind == KindQuery {
		kind = KindSubquery
	}
	switch kind {
	case KindExec, KindOne, KindMany, KindSubquery:
	default:
		return nil, fmt.Errorf("statement %s: invalid kind %q (must be exec, one, many, or subquery)", spec.Name, spec.Kind)
	}
	if spec.ResultType != "" {
		if kind == KindExec || kind == KindSubquery {
			return nil, fmt.Errorf("statement %s: result_type %q is not allowed on kind %q", spec.Name, spec.ResultType, kind)
		}
		if !isValidGoIdent(spec.ResultType) {
			return nil, fmt.Errorf("statement %s: invalid result_type identifier %q", spec.Name, spec.ResultType)
		}
	}
	trimmedSQL := strings.TrimSpace(spec.SQL)
	if trimmedSQL == "" {
		return nil, fmt.Errorf("statement %s: SQL is empty", spec.Name)
	}

	yamlParams := make(map[string]ParamSpec, len(spec.Params))
	for _, ps := range spec.Params {
		if !isValidGoIdent(ps.Name) {
			return nil, fmt.Errorf("statement %s: invalid YAML param name %q", spec.Name, ps.Name)
		}
		if _, exists := yamlParams[ps.Name]; exists {
			return nil, fmt.Errorf("statement %s: duplicate YAML param %q", spec.Name, ps.Name)
		}
		yamlParams[ps.Name] = ps
	}

	cs := &compiledStmt{
		ID:            id,
		Name:          spec.Name,
		Kind:          kind,
		BatchParamIdx: -1,
	}

	parser := &stmtParser{
		spec:         spec,
		yamlParams:   yamlParams,
		paramsByName: make(map[string]int),
		flagsByName:  make(map[string]uint8),
		cs:           cs,
	}

	if err := parser.scanSpan(trimmedSQL, &cs.Root, true); err != nil {
		return nil, err
	}

	// Fallback output columns from YAML if no inline output columns were declared
	if len(cs.Cols) == 0 && len(spec.Columns) > 0 {
		for _, cSpec := range spec.Columns {
			if err := parser.addColumn(cSpec.Name, cSpec.Type, cSpec.Name); err != nil {
				return nil, err
			}
		}
	}

	if kind == KindExec && len(cs.Cols) > 0 {
		return nil, fmt.Errorf("statement %s: kind \"exec\" cannot declare output columns (use \"one\" or \"many\" for RETURNING)", spec.Name)
	}
	if (kind == KindOne || kind == KindMany) && len(cs.Cols) == 0 {
		return nil, fmt.Errorf("statement %s: kind %q requires at least one output column", spec.Name, kind)
	}

	// Validate Order and Seek directives
	if cs.HasSeek && !cs.HasOrder {
		return nil, fmt.Errorf("statement %s: sqlgen.Seek requires a paired sqlgen.Order(...) directive in the same statement", spec.Name)
	}
	if cs.HasOrder {
		for _, rawRef := range parser.rawOrderCols {
			shortRef := rawRef
			if dot := strings.LastIndexByte(rawRef, '.'); dot >= 0 {
				shortRef = rawRef[dot+1:]
			}
			matchedIdx := -1
			for idx, col := range cs.Cols {
				if col.CondFlag != "" {
					continue
				}
				if strings.EqualFold(col.SQLExpr, rawRef) || strings.EqualFold(col.RawName, rawRef) || strings.EqualFold(col.RawName, shortRef) {
					matchedIdx = idx
					break
				}
			}
			if matchedIdx < 0 {
				return nil, fmt.Errorf("statement %s: sqlgen.Order column %q does not match any unconditional SELECT output column", spec.Name, rawRef)
			}
			matchedCol := cs.Cols[matchedIdx]
			if cs.HasSeek && isNullableGoType(matchedCol.GoType) {
				return nil, fmt.Errorf("statement %s: nullable sort column %q (%s) cannot be used with sqlgen.Seek; wrap the column in COALESCE(%s, default) AS %s:<non-null-type>", spec.Name, rawRef, matchedCol.GoType, rawRef, matchedCol.RawName)
			}
			cs.OrderCols = append(cs.OrderCols, orderColRef{
				RawRef:    rawRef,
				ColIdx:    matchedIdx,
				FieldName: matchedCol.FieldName,
				GoType:    matchedCol.GoType,
			})
		}
	}

	// Determine UseParamsStruct (forced true if conditional blocks are present)
	cs.UseParamsStruct = spec.ParamsStruct || len(cs.Flags) > 0

	// Replace unconditional projection span with SlotSelectCols when UseParamsStruct && len(UncondCols) > 1 (Section 4.4)
	if cs.UseParamsStruct && len(cs.UncondCols) > 1 && parser.projActive && !parser.projHasParamSlot &&
		parser.projStartSegIdx == parser.projEndSegIdx {
		segIdx := parser.projStartSegIdx
		origSeg := cs.Root.Segments[segIdx]
		prefix := origSeg[:parser.projStartByteIdx]
		suffix := origSeg[parser.projEndByteIdx:]

		newSegs := make([]string, 0, len(cs.Root.Segments)+1)
		newSegs = append(newSegs, cs.Root.Segments[:segIdx]...)
		newSegs = append(newSegs, prefix, suffix)
		newSegs = append(newSegs, cs.Root.Segments[segIdx+1:]...)

		newSlots := make([]slotRef, 0, len(cs.Root.Slots)+1)
		newSlots = append(newSlots, cs.Root.Slots[:segIdx]...)
		newSlots = append(newSlots, slotRef{Kind: runtime.SlotSelectCols, ParamIdx: -1})
		newSlots = append(newSlots, cs.Root.Slots[segIdx:]...)

		cs.Root.Segments = newSegs
		cs.Root.Slots = newSlots
		for _, uc := range cs.UncondCols {
			cs.SelectColExprs = append(cs.SelectColExprs, uc.SQLExpr)
		}
	}

	// Determine RowTypeName
	if kind == KindOne || kind == KindMany {
		if spec.ResultType != "" {
			if len(cs.Cols) == 1 && isPredeclaredGoType(spec.ResultType) {
				cs.RowTypeName = spec.ResultType
				cs.ScalarResult = true
			} else {
				cs.RowTypeName = spec.ResultType
			}
		} else {
			cs.RowTypeName = spec.Name + "Row"
		}
	}

	// Determine CanBatch (single unconditional SlotTupleSlice, no SlotSlice, no SlotQuery, no Order/Seek)
	if (kind == KindExec || kind == KindMany) &&
		cs.TotalTuples == 1 &&
		cs.TotalSlices == 0 &&
		cs.TotalSubs == 0 &&
		!cs.HasOrder &&
		!cs.HasSeek {
		for _, sl := range cs.Root.Slots {
			if sl.Kind == runtime.SlotTupleSlice {
				cs.CanBatch = true
				cs.BatchParamIdx = sl.ParamIdx
				numFields := len(sl.Tuple.Fields)
				scalarCount := len(cs.Params) - 1
				cs.MaxRowsPerStmt = max(1, (32766-scalarCount)/max(1, numFields))
				break
			}
		}
	}

	cs.IsStatic = cs.TotalSlices == 0 &&
		cs.TotalTuples == 0 &&
		cs.TotalSubs == 0 &&
		len(cs.Flags) == 0 &&
		len(cs.SelectColExprs) == 0 &&
		!cs.HasOrder &&
		!cs.HasSeek

	return cs, nil
}

var autoImportPrefixes = map[string]string{
	"time.":   "time",
	"json.":   "encoding/json",
	"big.":    "math/big",
	"netip.":  "net/netip",
	"url.":    "net/url",
	"driver.": "database/sql/driver",
}

func collectImports(cfg *Config, stmts []*compiledStmt, runtimePkg string) []string {
	impSet := map[string]bool{
		"context":      true,
		"database/sql": true,
		runtimePkg:     true,
	}
	hasStatic := false
	for _, s := range stmts {
		if s.IsStatic {
			hasStatic = true
		}
		if s.Kind == KindMany {
			impSet["iter"] = true
		}
		checkType := func(t string) {
			for prefix, pkg := range autoImportPrefixes {
				if strings.Contains(t, prefix) {
					impSet[pkg] = true
				}
			}
		}
		for _, p := range s.Params {
			checkType(p.GoType)
			if p.Tuple != nil {
				for _, f := range p.Tuple.Fields {
					checkType(f.GoType)
				}
			}
		}
		for _, c := range s.Cols {
			checkType(c.GoType)
		}
	}
	if hasStatic {
		impSet["sync/atomic"] = true
	}
	for _, extra := range cfg.ExtraImports {
		trimmed := strings.TrimSpace(extra)
		if trimmed != "" {
			impSet[trimmed] = true
		}
	}
	out := make([]string, 0, len(impSet))
	for k := range impSet {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Generate validates cfg and generates formatted Go source code.
func Generate(cfg *Config) ([]byte, error) {
	if cfg == nil {
		return nil, errors.New("sqlgen: nil Config")
	}
	if cfg.Version != "" && cfg.Version != "1" {
		return nil, fmt.Errorf("sqlgen: unsupported config version %q (expected \"1\")", cfg.Version)
	}
	if !isValidGoIdent(cfg.PackageName) {
		return nil, fmt.Errorf("sqlgen: invalid package name %q", cfg.PackageName)
	}
	dialect, err := parseDialect(cfg.Dialect)
	if err != nil {
		return nil, err
	}
	if len(cfg.Statements) == 0 {
		return nil, errors.New("sqlgen: configuration must contain at least one statement")
	}

	runtimeImport := strings.TrimSpace(cfg.RuntimeImport)
	if runtimeImport == "" {
		runtimeImport = "github.com/CAFxX/sqlgen/runtime"
	}

	seenStmts := make(map[string]bool, len(cfg.Statements))
	compiled := make([]*compiledStmt, 0, len(cfg.Statements))

	// Track shared row structs and tuple structs for conflict detection
	rowStructs := make(map[string][]colDef)
	var rowStructOrder []string
	tupleStructs := make(map[string]*tupleStructDef)
	var tupleStructOrder []string

	for i, spec := range cfg.Statements {
		if seenStmts[spec.Name] {
			return nil, fmt.Errorf("sqlgen: duplicate statement name %q", spec.Name)
		}
		seenStmts[spec.Name] = true

		cs, err := compileStatement(uint32(i+1), spec)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, cs)

		// Validate shared result_type structs
		if (cs.Kind == KindOne || cs.Kind == KindMany) && !cs.ScalarResult {
			if prevCols, exists := rowStructs[cs.RowTypeName]; exists {
				if len(prevCols) != len(cs.Cols) {
					return nil, fmt.Errorf("sqlgen: conflicting shared result_type %q in statement %s (column count %d vs %d)", cs.RowTypeName, cs.Name, len(prevCols), len(cs.Cols))
				}
				for cIdx := range prevCols {
					if prevCols[cIdx].FieldName != cs.Cols[cIdx].FieldName || prevCols[cIdx].GoType != cs.Cols[cIdx].GoType {
						return nil, fmt.Errorf("sqlgen: conflicting shared result_type %q in statement %s (field %d: %s %s vs %s %s)",
							cs.RowTypeName, cs.Name, cIdx,
							prevCols[cIdx].FieldName, prevCols[cIdx].GoType,
							cs.Cols[cIdx].FieldName, cs.Cols[cIdx].GoType,
						)
					}
				}
			} else {
				rowStructs[cs.RowTypeName] = cs.Cols
				rowStructOrder = append(rowStructOrder, cs.RowTypeName)
			}
		}

		// Validate shared tuple structs
		for _, p := range cs.Params {
			if p.Kind == paramTupleSlice && p.Tuple != nil {
				if prevT, exists := tupleStructs[p.Tuple.Name]; exists {
					if len(prevT.Fields) != len(p.Tuple.Fields) {
						return nil, fmt.Errorf("sqlgen: conflicting tuple struct %q in statement %s", p.Tuple.Name, cs.Name)
					}
					for fIdx := range prevT.Fields {
						if prevT.Fields[fIdx].FieldName != p.Tuple.Fields[fIdx].FieldName || prevT.Fields[fIdx].GoType != p.Tuple.Fields[fIdx].GoType {
							return nil, fmt.Errorf("sqlgen: conflicting tuple struct %q field %d in statement %s", p.Tuple.Name, fIdx, cs.Name)
						}
					}
				} else {
					tupleStructs[p.Tuple.Name] = p.Tuple
					tupleStructOrder = append(tupleStructOrder, p.Tuple.Name)
				}
			}
		}
	}

	imports := collectImports(cfg, compiled, runtimeImport)
	var b bytes.Buffer

	b.WriteString("// Code generated by sqlgen. DO NOT EDIT.\n\n")
	fmt.Fprintf(&b, "package %s\n\n", cfg.PackageName)
	b.WriteString("import (\n")
	for _, imp := range imports {
		if imp == runtimeImport && filepath.Base(imp) != "runtime" {
			fmt.Fprintf(&b, "\truntime %s\n", strconv.Quote(imp))
		} else {
			fmt.Fprintf(&b, "\t%s\n", strconv.Quote(imp))
		}
	}
	b.WriteString(")\n\n")

	// Type aliases so generated types and callers can refer to Query, Order, Seek directly
	b.WriteString("type (\n")
	b.WriteString("\tQuery = runtime.Query\n")
	b.WriteString("\tOrder = runtime.Order\n")
	b.WriteString("\tSeek  = runtime.Seek\n")
	b.WriteString(")\n\n")

	dialectConst := "runtime.DialectPostgres"
	if dialect == runtime.DialectQuestion {
		dialectConst = "runtime.DialectQuestion"
	}
	fmt.Fprintf(&b, "const sqlDialect = %s\n\n", dialectConst)

	// EmitQueries struct and lifecycle helpers
	numStatic := 0
	staticIdxByStmt := make(map[uint32]int)
	for _, cs := range compiled {
		if cs.IsStatic {
			staticIdxByStmt[cs.ID] = numStatic
			numStatic++
		}
	}

	b.WriteString("// Queries executes strongly-typed SQL statements backed by a two-tier adaptive statement cache.\n")
	b.WriteString("type Queries struct {\n")
	b.WriteString("\tdb          *sql.DB\n")
	b.WriteString("\tcache       *runtime.StmtCache\n")
	b.WriteString("\ttxCache     *runtime.TxStmtCache\n")
	b.WriteString("\tusePrepared bool\n")
	if numStatic > 0 {
		fmt.Fprintf(&b, "\tfastSlots   *[%d]atomic.Pointer[runtime.ManagedStmt]\n", numStatic)
	}
	b.WriteString("}\n\n")

	b.WriteString("// New creates a new Queries instance with a dedicated StmtCache.\n")
	b.WriteString("func New(db *sql.DB, opts ...runtime.CacheOption) *Queries {\n")
	b.WriteString("\treturn NewWithCache(db, runtime.NewStmtCache(opts...))\n")
	b.WriteString("}\n\n")

	b.WriteString("// NewWithCache creates a new Queries instance using the provided StmtCache.\n")
	b.WriteString("func NewWithCache(db *sql.DB, cache *runtime.StmtCache) *Queries {\n")
	b.WriteString("\tif cache == nil {\n")
	b.WriteString("\t\tcache = runtime.NewStmtCache()\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn &Queries{\n")
	b.WriteString("\t\tdb:          db,\n")
	b.WriteString("\t\tcache:       cache,\n")
	b.WriteString("\t\tusePrepared: true,\n")
	if numStatic > 0 {
		fmt.Fprintf(&b, "\t\tfastSlots:   new([%d]atomic.Pointer[runtime.ManagedStmt]),\n", numStatic)
	}
	b.WriteString("\t}\n")
	b.WriteString("}\n\n")

	b.WriteString("// Cache returns the underlying StmtCache.\n")
	b.WriteString("func (q *Queries) Cache() *runtime.StmtCache { return q.cache }\n\n")

	b.WriteString("// Close closes the underlying StmtCache.\n")
	b.WriteString("func (q *Queries) Close() error {\n")
	b.WriteString("\tif q.txCache != nil {\n")
	b.WriteString("\t\treturn q.txCache.Close()\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn q.cache.Close()\n")
	b.WriteString("}\n\n")

	b.WriteString("// WithPrepared returns a shallow copy of q with Tier-2 prepared statement usage toggled.\n")
	b.WriteString("func (q *Queries) WithPrepared(enabled bool) *Queries {\n")
	b.WriteString("\tcp := *q\n")
	b.WriteString("\tcp.usePrepared = enabled\n")
	b.WriteString("\treturn &cp\n")
	b.WriteString("}\n\n")

	b.WriteString("// WithTx returns a Queries view bound to tx with safe ManagedStmt reference tracking.\n")
	b.WriteString("func (q *Queries) WithTx(tx *sql.Tx) *Queries {\n")
	b.WriteString("\tcp := *q\n")
	b.WriteString("\tcp.txCache = runtime.NewTxStmtCache(tx)\n")
	b.WriteString("\treturn &cp\n")
	b.WriteString("}\n\n")

	b.WriteString("// InTx executes fn inside a database transaction, committing on nil error and rolling back otherwise.\n")
	b.WriteString("func (q *Queries) InTx(ctx context.Context, fn func(*Queries) error) error {\n")
	b.WriteString("\treturn q.InTxOpts(ctx, nil, fn)\n")
	b.WriteString("}\n\n")

	b.WriteString("// InTxOpts executes fn inside a database transaction with custom sql.TxOptions.\n")
	b.WriteString("func (q *Queries) InTxOpts(ctx context.Context, opts *sql.TxOptions, fn func(*Queries) error) error {\n")
	b.WriteString("\tif q.txCache != nil {\n")
	b.WriteString("\t\treturn fn(q)\n")
	b.WriteString("\t}\n")
	b.WriteString("\ttx, err := q.db.BeginTx(ctx, opts)\n")
	b.WriteString("\tif err != nil {\n")
	b.WriteString("\t\treturn err\n")
	b.WriteString("\t}\n")
	b.WriteString("\ttc := runtime.NewTxStmtCache(tx)\n")
	b.WriteString("\tdefer func() { _ = tc.Close() }()\n")
	b.WriteString("\ttxQ := *q\n")
	b.WriteString("\ttxQ.txCache = tc\n")
	b.WriteString("\tif err := fn(&txQ); err != nil {\n")
	b.WriteString("\t\t_ = tx.Rollback()\n")
	b.WriteString("\t\treturn err\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn tx.Commit()\n")
	b.WriteString("}\n\n")

	// Emit tuple struct definitions
	for _, tName := range tupleStructOrder {
		td := tupleStructs[tName]
		fmt.Fprintf(&b, "type %s struct {\n", td.Name)
		for _, f := range td.Fields {
			fmt.Fprintf(&b, "\t%s %s\n", f.FieldName, f.GoType)
		}
		b.WriteString("}\n\n")
	}

	// Emit result row struct definitions
	for _, rName := range rowStructOrder {
		cols := rowStructs[rName]
		fmt.Fprintf(&b, "type %s struct {\n", rName)
		for _, c := range cols {
			fmt.Fprintf(&b, "\t%s %s\n", c.FieldName, c.GoType)
		}
		b.WriteString("}\n\n")
	}

	// Track emitted Order<RowType> / Seek<RowType> helpers to avoid redeclaring if multiple statements share RowType
	emittedRowOrder := make(map[string]bool)

	for _, cs := range compiled {
		emitStatementCode(&b, cs, staticIdxByStmt, emittedRowOrder)
	}

	formatted, err := format.Source(b.Bytes())
	if err != nil {
		return nil, fmt.Errorf("sqlgen: gofmt failed: %w\n--- raw source ---\n%s", err, b.String())
	}
	return formatted, nil
}

func emitBlockLiteral(b *bytes.Buffer, blk *parsedBlock, selectColExprs []string, orderCols []orderColRef) {
	b.WriteString("&runtime.QueryDescriptor{\n")
	b.WriteString("\tSegments: []string{")
	for i, seg := range blk.Segments {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Quote(seg))
	}
	b.WriteString("},\n")
	if len(blk.Slots) > 0 {
		b.WriteString("\tSlots: []runtime.Slot{\n")
		for _, sl := range blk.Slots {
			b.WriteString("\t\t{Kind: ")
			switch sl.Kind {
			case runtime.SlotScalar:
				b.WriteString("runtime.SlotScalar")
			case runtime.SlotSlice:
				b.WriteString("runtime.SlotSlice")
			case runtime.SlotTupleSlice:
				b.WriteString("runtime.SlotTupleSlice, Tuple: &runtime.TupleDescriptor{Segments: []string{")
				for j, ts := range sl.Tuple.Segments {
					if j > 0 {
						b.WriteString(", ")
					}
					b.WriteString(strconv.Quote(ts))
				}
				fmt.Fprintf(b, "}, NumFields: %d}", len(sl.Tuple.Fields))
			case runtime.SlotOptBlock:
				fmt.Fprintf(b, "runtime.SlotOptBlock, FlagBit: %d, Inner: ", sl.FlagBit)
				emitBlockLiteral(b, sl.Inner, nil, nil)
			case runtime.SlotQuery:
				b.WriteString("runtime.SlotQuery")
			case runtime.SlotSelectCols:
				b.WriteString("runtime.SlotSelectCols")
			case runtime.SlotOrder:
				b.WriteString("runtime.SlotOrder")
			case runtime.SlotSeek:
				b.WriteString("runtime.SlotSeek")
			}
			b.WriteString("},\n")
		}
		b.WriteString("\t},\n")
	}
	if len(selectColExprs) > 0 {
		b.WriteString("\tSelectColExprs: []string{")
		for i, expr := range selectColExprs {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Quote(expr))
		}
		b.WriteString("},\n")
	}
	if len(orderCols) > 0 {
		b.WriteString("\tOrderCols: []string{")
		for i, oc := range orderCols {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Quote(oc.RawRef))
		}
		b.WriteString("},\n")
	}
	if blk.NumSlices > 0 {
		fmt.Fprintf(b, "\tNumSlices: %d,\n", blk.NumSlices)
	}
	if blk.NumTuples > 0 {
		fmt.Fprintf(b, "\tNumTuples: %d,\n", blk.NumTuples)
	}
	if blk.NumSubs > 0 {
		fmt.Fprintf(b, "\tNumSubs: %d,\n", blk.NumSubs)
	}
	b.WriteString("}")
}

func emitSlotArgExpansion(b *bytes.Buffer, cs *compiledStmt, blk *parsedBlock, paramAccess func(p paramDef) string) {
	for _, sl := range blk.Slots {
		switch sl.Kind {
		case runtime.SlotScalar:
			p := cs.Params[sl.ParamIdx]
			fmt.Fprintf(b, "\t*dst = append(*dst, %s)\n", paramAccess(p))
		case runtime.SlotSlice:
			p := cs.Params[sl.ParamIdx]
			acc := paramAccess(p)
			fmt.Fprintf(b, "\tif len(%s) == 0 {\n\t\treturn runtime.ErrEmptySliceParam\n\t}\n", acc)
			fmt.Fprintf(b, "\tfor _, v := range %s {\n\t\t*dst = append(*dst, v)\n\t}\n", acc)
		case runtime.SlotTupleSlice:
			p := cs.Params[sl.ParamIdx]
			acc := paramAccess(p)
			fmt.Fprintf(b, "\tif len(%s) == 0 {\n\t\treturn runtime.ErrEmptySliceParam\n\t}\n", acc)
			fmt.Fprintf(b, "\tfor i := range %s {\n", acc)
			for _, f := range sl.Tuple.Fields {
				fmt.Fprintf(b, "\t\t*dst = append(*dst, %s[i].%s)\n", acc, f.FieldName)
			}
			b.WriteString("\t}\n")
		case runtime.SlotQuery:
			p := cs.Params[sl.ParamIdx]
			acc := paramAccess(p)
			fmt.Fprintf(b, "\tif err := %s.AppendArgs(dst); err != nil {\n\t\treturn err\n\t}\n", acc)
		case runtime.SlotSeek:
			p := cs.Params[sl.ParamIdx]
			acc := paramAccess(p)
			orderExpr := "runtime.Order(0)"
			for _, op := range cs.Params {
				if op.Kind == paramOrder {
					orderExpr = paramAccess(op)
					break
				}
			}
			fmt.Fprintf(b, "\tif err := runtime.AppendSeekArgs(dst, %s, %s, %d); err != nil {\n\t\treturn err\n\t}\n", acc, orderExpr, len(cs.OrderCols))
		case runtime.SlotOrder:
			p := cs.Params[sl.ParamIdx]
			acc := paramAccess(p)
			fmt.Fprintf(b, "\tif _, _, err := runtime.ExpandOrder(%s, %d); err != nil {\n\t\treturn err\n\t}\n", acc, len(cs.OrderCols))
		case runtime.SlotOptBlock:
			flag := cs.Flags[sl.FlagBit]
			fmt.Fprintf(b, "\tif p.%s {\n", flag.FieldName)
			emitSlotArgExpansion(b, cs, sl.Inner, paramAccess)
			b.WriteString("\t}\n")
		}
	}
}

func emitStatementCode(b *bytes.Buffer, cs *compiledStmt, staticIdxByStmt map[uint32]int, emittedRowOrder map[string]bool) {
	fmt.Fprintf(b, "const stmtID%s uint32 = %d\n\n", cs.Name, cs.ID)

	// Emit SelectCols bitmask constants when UseParamsStruct && len(UncondCols) > 0
	if cs.UseParamsStruct && len(cs.UncondCols) > 0 {
		b.WriteString("const (\n")
		for i, uc := range cs.UncondCols {
			fmt.Fprintf(b, "\t%sCol%s uint64 = 1 << %d\n", cs.Name, uc.FieldName, i)
		}
		fmt.Fprintf(b, "\t%sColsAll uint64 = (1 << %d) - 1\n", cs.Name, len(cs.UncondCols))
		b.WriteString(")\n\n")
	}

	// Emit Order and Seek helpers for sortable columns
	if cs.HasOrder {
		typeTag := cs.RowTypeName
		if typeTag == "" || cs.ScalarResult || emittedRowOrder[typeTag] {
			typeTag = cs.Name
		}
		emittedRowOrder[typeTag] = true

		b.WriteString("const (\n")
		for idx, oc := range cs.OrderCols {
			ascVal := runtime.Asc(uint8(idx))
			descVal := runtime.Desc(uint8(idx))
			fmt.Fprintf(b, "\tOrder%s%sAsc uint8 = %d\n", typeTag, oc.FieldName, ascVal)
			fmt.Fprintf(b, "\tOrder%s%sDesc uint8 = %d\n", typeTag, oc.FieldName, descVal)
			if typeTag != cs.Name {
				fmt.Fprintf(b, "\tOrder%s%sAsc_%s uint8 = %d\n", cs.Name, oc.FieldName, cs.Name, ascVal)
				fmt.Fprintf(b, "\t%sOrder%sAsc uint8 = %d\n", typeTag, oc.FieldName, ascVal)
				fmt.Fprintf(b, "\t%sOrder%sDesc uint8 = %d\n", typeTag, oc.FieldName, descVal)
			}
		}
		b.WriteString(")\n\n")

		fmt.Fprintf(b, "func Order%s(terms ...uint8) runtime.Order {\n", typeTag)
		b.WriteString("\treturn runtime.PackOrder(terms...)\n")
		b.WriteString("}\n\n")

		if typeTag != cs.Name {
			fmt.Fprintf(b, "func Order%sStmt(terms ...uint8) runtime.Order {\n", cs.Name)
			b.WriteString("\treturn runtime.PackOrder(terms...)\n")
			b.WriteString("}\n\n")
		}

		if cs.HasSeek && !cs.ScalarResult && cs.RowTypeName != "" {
			fmt.Fprintf(b, "func Seek%s(row %s) runtime.Seek {\n", typeTag, cs.RowTypeName)
			b.WriteString("\treturn runtime.NewSeek(")
			for idx, oc := range cs.OrderCols {
				if idx > 0 {
					b.WriteString(", ")
				}
				fmt.Fprintf(b, "row.%s", oc.FieldName)
			}
			b.WriteString(")\n}\n\n")
		}
	}

	// Emit Params struct (either public <Name>Params when UseParamsStruct is true, or internal <name>Args for Query currying)
	paramsStructName := cs.Name + "Params"
	if !cs.UseParamsStruct {
		paramsStructName = "params" + cs.Name
	}
	fmt.Fprintf(b, "type %s struct {\n", paramsStructName)
	for _, p := range cs.Params {
		fmt.Fprintf(b, "\t%s %s\n", p.FieldName, p.GoType)
	}
	for _, f := range cs.Flags {
		fmt.Fprintf(b, "\t%s bool\n", f.FieldName)
	}
	if cs.UseParamsStruct && len(cs.UncondCols) > 0 {
		b.WriteString("\tSelectCols uint64\n")
	}
	b.WriteString("}\n\n")

	// Emit static QueryDescriptor variable
	fmt.Fprintf(b, "var desc%s = ", cs.Name)
	emitBlockLiteral(b, &cs.Root, cs.SelectColExprs, cs.OrderCols)
	b.WriteString("\n\n")

	// Emit static non-capturing expander function expand<Name>Params and expand<Name>Args
	fmt.Fprintf(b, "func expand%sParams(dst *[]any, p %s) error {\n", cs.Name, paramsStructName)
	b.WriteString("\t_ = p\n")
	emitSlotArgExpansion(b, cs, &cs.Root, func(p paramDef) string {
		return "p." + p.FieldName
	})
	b.WriteString("\treturn nil\n")
	b.WriteString("}\n\n")

	fmt.Fprintf(b, "func expand%sArgs(dst *[]any, q *runtime.Query) error {\n", cs.Name)
	if len(cs.Params) > 0 || len(cs.Flags) > 0 {
		fmt.Fprintf(b, "\treturn expand%sParams(dst, q.Params.(%s))\n", cs.Name, paramsStructName)
	} else {
		fmt.Fprintf(b, "\treturn expand%sParams(dst, %s{})\n", cs.Name, paramsStructName)
	}
	b.WriteString("}\n\n")

	// Helper to format method parameter signature and call arguments
	var sigParams strings.Builder
	var callArgs strings.Builder
	if cs.UseParamsStruct {
		fmt.Fprintf(&sigParams, "p %s", paramsStructName)
		callArgs.WriteString("p")
	} else {
		for i, p := range cs.Params {
			if i > 0 {
				sigParams.WriteString(", ")
				callArgs.WriteString(", ")
			}
			fmt.Fprintf(&sigParams, "%s %s", p.ParamName, p.GoType)
			callArgs.WriteString(p.ParamName)
		}
	}
	sigStr := sigParams.String()
	callStr := callArgs.String()
	sigWithLeadingComma := ""
	callWithLeadingComma := ""
	if sigStr != "" {
		sigWithLeadingComma = ", " + sigStr
		callWithLeadingComma = ", " + callStr
	}

	// Emit zero-allocation compute<Name>Key helper for direct query execution
	fmt.Fprintf(b, "func compute%sKey(p %s) (runtime.VariantKey, uint64, uint64, int) {\n", cs.Name, paramsStructName)
	b.WriteString("\t_ = p\n")
	b.WriteString("\tvar colMask uint64\n")
	if cs.UseParamsStruct && len(cs.SelectColExprs) > 0 {
		fmt.Fprintf(b, "\tcolMask = p.SelectCols & %sColsAll\n", cs.Name)
		fmt.Fprintf(b, "\tif colMask == 0 {\n\t\tcolMask = %sColsAll\n\t}\n", cs.Name)
	}
	b.WriteString("\tvar flagMask uint64\n")
	for _, f := range cs.Flags {
		fmt.Fprintf(b, "\tif p.%s {\n\t\tflagMask |= 1 << %d\n\t}\n", f.FieldName, f.Bit)
	}
	b.WriteString("\tvar orderSpec runtime.Order\n")
	b.WriteString("\tvar hasSeek bool\n")
	for _, p := range cs.Params {
		if p.Kind == paramOrder {
			fmt.Fprintf(b, "\torderSpec = p.%s\n", p.FieldName)
		}
		if p.Kind == paramSeek {
			fmt.Fprintf(b, "\thasSeek = p.%s.IsActive()\n", p.FieldName)
		}
	}
	b.WriteString("\torderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)\n")
	fmt.Fprintf(b, "\tapproxArgs := %d\n", len(cs.Params)+4)

	totalDims := cs.TotalSlices + cs.TotalTuples + cs.TotalSubs
	dimIdx := 0
	var emitFastDims func(blk *parsedBlock, activeCond string)
	emitFastDims = func(blk *parsedBlock, activeCond string) {
		for _, sl := range blk.Slots {
			switch sl.Kind {
			case runtime.SlotSlice:
				p := cs.Params[sl.ParamIdx]
				dVar := fmt.Sprintf("d%d", dimIdx)
				dimIdx++
				fmt.Fprintf(b, "\tvar %s uint64\n", dVar)
				if activeCond == "" {
					fmt.Fprintf(b, "\t%s = uint64(len(p.%s))\n", dVar, p.FieldName)
					fmt.Fprintf(b, "\tapproxArgs += len(p.%s)\n", p.FieldName)
				} else {
					fmt.Fprintf(b, "\tif %s {\n\t\t%s = uint64(len(p.%s))\n\t\tapproxArgs += len(p.%s)\n\t}\n", activeCond, dVar, p.FieldName, p.FieldName)
				}
			case runtime.SlotTupleSlice:
				p := cs.Params[sl.ParamIdx]
				dVar := fmt.Sprintf("d%d", dimIdx)
				dimIdx++
				nf := len(sl.Tuple.Fields)
				fmt.Fprintf(b, "\tvar %s uint64\n", dVar)
				if activeCond == "" {
					fmt.Fprintf(b, "\t%s = uint64(len(p.%s))\n", dVar, p.FieldName)
					fmt.Fprintf(b, "\tapproxArgs += len(p.%s) * %d\n", p.FieldName, nf)
				} else {
					fmt.Fprintf(b, "\tif %s {\n\t\t%s = uint64(len(p.%s))\n\t\tapproxArgs += len(p.%s) * %d\n\t}\n", activeCond, dVar, p.FieldName, p.FieldName, nf)
				}
			case runtime.SlotQuery:
				p := cs.Params[sl.ParamIdx]
				dVar := fmt.Sprintf("d%d", dimIdx)
				dimIdx++
				fmt.Fprintf(b, "\tvar %s uint64\n", dVar)
				if activeCond == "" {
					fmt.Fprintf(b, "\t%s = p.%s.ShapeTag\n", dVar, p.FieldName)
				} else {
					fmt.Fprintf(b, "\tif %s {\n\t\t%s = p.%s.ShapeTag\n\t}\n", activeCond, dVar, p.FieldName)
				}
			case runtime.SlotOptBlock:
				flag := cs.Flags[sl.FlagBit]
				emitFastDims(sl.Inner, "p."+flag.FieldName)
			}
		}
	}
	emitFastDims(&cs.Root, "")

	fmt.Fprintf(b, "\tvk := runtime.MakeVariantKey(stmtID%s, colMask, flagMask, orderKey", cs.Name)
	for di := 0; di < totalDims; di++ {
		fmt.Fprintf(b, ", d%d", di)
	}
	b.WriteString(")\n")
	b.WriteString("\treturn vk, colMask, flagMask, approxArgs\n")
	b.WriteString("}\n\n")

	// Emit prepare<Name> helper that builds runtime.Query for currying and SQL rendering on cache miss
	fmt.Fprintf(b, "func prepare%s(%s) (runtime.Query, runtime.VariantKey, int) {\n", cs.Name, sigStr)
	if !cs.UseParamsStruct {
		fmt.Fprintf(b, "\tp := %s{\n", paramsStructName)
		for _, p := range cs.Params {
			fmt.Fprintf(b, "\t\t%s: %s,\n", p.FieldName, p.ParamName)
		}
		b.WriteString("\t}\n")
	}
	fmt.Fprintf(b, "\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
	b.WriteString("\tvar orderSpec runtime.Order\n")
	b.WriteString("\tvar hasSeek bool\n")
	for _, p := range cs.Params {
		if p.Kind == paramOrder {
			fmt.Fprintf(b, "\torderSpec = p.%s\n", p.FieldName)
		}
		if p.Kind == paramSeek {
			fmt.Fprintf(b, "\thasSeek = p.%s.IsActive()\n", p.FieldName)
		}
	}
	b.WriteString("\torderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)\n")

	if cs.TotalSlices > 0 {
		fmt.Fprintf(b, "\tsliceLens := make([]int, %d)\n", cs.TotalSlices)
	} else {
		b.WriteString("\tvar sliceLens []int\n")
	}
	if cs.TotalTuples > 0 {
		fmt.Fprintf(b, "\ttupleLens := make([]int, %d)\n", cs.TotalTuples)
	} else {
		b.WriteString("\tvar tupleLens []int\n")
	}
	if cs.TotalSubs > 0 {
		fmt.Fprintf(b, "\tsubs := make([]runtime.Query, %d)\n", cs.TotalSubs)
	} else {
		b.WriteString("\tvar subs []runtime.Query\n")
	}

	sliceCursor := 0
	tupleCursor := 0
	subCursor := 0
	var emitCoordCollection func(blk *parsedBlock, activeCond string)
	emitCoordCollection = func(blk *parsedBlock, activeCond string) {
		for _, sl := range blk.Slots {
			switch sl.Kind {
			case runtime.SlotSlice:
				p := cs.Params[sl.ParamIdx]
				idx := sliceCursor
				sliceCursor++
				if activeCond == "" {
					fmt.Fprintf(b, "\tsliceLens[%d] = len(p.%s)\n", idx, p.FieldName)
				} else {
					fmt.Fprintf(b, "\tif %s {\n\t\tsliceLens[%d] = len(p.%s)\n\t}\n", activeCond, idx, p.FieldName)
				}
			case runtime.SlotTupleSlice:
				p := cs.Params[sl.ParamIdx]
				idx := tupleCursor
				tupleCursor++
				if activeCond == "" {
					fmt.Fprintf(b, "\ttupleLens[%d] = len(p.%s)\n", idx, p.FieldName)
				} else {
					fmt.Fprintf(b, "\tif %s {\n\t\ttupleLens[%d] = len(p.%s)\n\t}\n", activeCond, idx, p.FieldName)
				}
			case runtime.SlotQuery:
				p := cs.Params[sl.ParamIdx]
				idx := subCursor
				subCursor++
				if activeCond == "" {
					fmt.Fprintf(b, "\tsubs[%d] = p.%s\n", idx, p.FieldName)
				} else {
					fmt.Fprintf(b, "\tif %s {\n\t\tsubs[%d] = p.%s\n\t}\n", activeCond, idx, p.FieldName)
				}
			case runtime.SlotOptBlock:
				flag := cs.Flags[sl.FlagBit]
				emitCoordCollection(sl.Inner, "p."+flag.FieldName)
			}
		}
	}
	emitCoordCollection(&cs.Root, "")

	hasSubsLiteral := "false"
	if cs.TotalSubs > 0 {
		hasSubsLiteral = "true"
	}
	if cs.TotalSlices+cs.TotalTuples == 0 {
		fmt.Fprintf(b, "\tshapeTag := runtime.ComputeSubqueryShape(stmtID%s, colMask, flagMask, orderKey, %s)\n", cs.Name, hasSubsLiteral)
	} else {
		fmt.Fprintf(b, "\tshapeLens := make([]int, 0, %d)\n", cs.TotalSlices+cs.TotalTuples)
		if cs.TotalSlices > 0 {
			b.WriteString("\tshapeLens = append(shapeLens, sliceLens...)\n")
		}
		if cs.TotalTuples > 0 {
			b.WriteString("\tshapeLens = append(shapeLens, tupleLens...)\n")
		}
		fmt.Fprintf(b, "\tshapeTag := runtime.ComputeSubqueryShape(stmtID%s, colMask, flagMask, orderKey, %s, shapeLens...)\n", cs.Name, hasSubsLiteral)
	}

	fmt.Fprintf(b, "\tqVal := runtime.Query{\n")
	fmt.Fprintf(b, "\t\tStmtID: stmtID%s,\n", cs.Name)
	fmt.Fprintf(b, "\t\tDesc:   desc%s,\n", cs.Name)
	b.WriteString("\t\tInput: runtime.RenderInput{\n")
	b.WriteString("\t\t\tColMask:   colMask,\n")
	b.WriteString("\t\t\tFlagMask:  flagMask,\n")
	b.WriteString("\t\t\tOrderSpec: orderSpec,\n")
	b.WriteString("\t\t\tHasSeek:   hasSeek,\n")
	b.WriteString("\t\t\tSliceLens: sliceLens,\n")
	b.WriteString("\t\t\tTupleLens: tupleLens,\n")
	b.WriteString("\t\t\tSubs:      subs,\n")
	b.WriteString("\t\t},\n")
	b.WriteString("\t\tParams:   p,\n")
	fmt.Fprintf(b, "\t\tExpand:   expand%sArgs,\n", cs.Name)
	b.WriteString("\t\tShapeTag: shapeTag,\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn qVal, vk, approxArgs\n")
	b.WriteString("}\n\n")

	// 1. Currying Constructor (<Name>Query) for all kinds except exec
	if cs.Kind != KindExec {
		fmt.Fprintf(b, "// %sQuery constructs a composable runtime.Query without executing it.\n", cs.Name)
		fmt.Fprintf(b, "func %sQuery(%s) runtime.Query {\n", cs.Name, sigStr)
		fmt.Fprintf(b, "\tqVal, _, _ := prepare%s(%s)\n", cs.Name, callStr)
		b.WriteString("\treturn qVal\n")
		b.WriteString("}\n\n")

		fmt.Fprintf(b, "// %sQuery constructs a composable runtime.Query without executing it.\n", cs.Name)
		fmt.Fprintf(b, "func (q *Queries) %sQuery(%s) runtime.Query {\n", cs.Name, sigStr)
		fmt.Fprintf(b, "\treturn %sQuery(%s)\n", cs.Name, callStr)
		b.WriteString("}\n\n")
	}

	if cs.Kind == KindSubquery {
		return
	}

	fastSlotExpr := "nil"
	if slotIdx, ok := staticIdxByStmt[cs.ID]; ok {
		fastSlotExpr = fmt.Sprintf("&q.fastSlots[%d]", slotIdx)
	}

	emitLocalParamsInit := func(indent string) {
		if !cs.UseParamsStruct {
			fmt.Fprintf(b, "%sp := %s{\n", indent, paramsStructName)
			for _, p := range cs.Params {
				fmt.Fprintf(b, "%s\t%s: %s,\n", indent, p.FieldName, p.ParamName)
			}
			fmt.Fprintf(b, "%s}\n", indent)
		}
	}

	// 2. KindExec methods
	if cs.Kind == KindExec {
		fmt.Fprintf(b, "func (q *Queries) %s(ctx context.Context%s) (sql.Result, error) {\n", cs.Name, sigWithLeadingComma)
		emitLocalParamsInit("\t")
		fmt.Fprintf(b, "\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
		b.WriteString("\targBuf := runtime.AcquireArgs(approxArgs)\n")
		b.WriteString("\tdefer runtime.ReleaseArgs(argBuf)\n")
		fmt.Fprintf(b, "\tif err := expand%sParams(argBuf, p); err != nil {\n", cs.Name)
		b.WriteString("\t\treturn nil, err\n")
		b.WriteString("\t}\n")
		fmt.Fprintf(b, "\tbuildSQL := func() string {\n\t\tqVal, _, _ := prepare%s(%s)\n\t\treturn runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)\n\t}\n", cs.Name, callStr)
		fmt.Fprintf(b, "\treturn q.cache.Exec(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, %s, buildSQL, (*argBuf)...)\n", cs.Name, fastSlotExpr)
		b.WriteString("}\n\n")

		if cs.CanBatch {
			emitExecBatchMethod(b, cs, sigWithLeadingComma, callStr, paramsStructName)
		}
		return
	}

	// Helper to emit scanDest setup before rows.Next() / row.Scan()
	emitScanSetup := func(indent string) {
		fmt.Fprintf(b, "%svar item %s\n", indent, cs.RowTypeName)
		if cs.ScalarResult {
			fmt.Fprintf(b, "%sscanDest := [1]any{&item}\n", indent)
			fmt.Fprintf(b, "%sscanSlice := scanDest[:1]\n", indent)
			return
		}
		fmt.Fprintf(b, "%svar scanBuf [%d]any\n", indent, len(cs.Cols))
		if cs.UseParamsStruct && len(cs.SelectColExprs) > 0 {
			fmt.Fprintf(b, "%sscanCount := 0\n", indent)
			fmt.Fprintf(b, "%seffMask := colMask\n", indent)
			for i, uc := range cs.UncondCols {
				fmt.Fprintf(b, "%sif (effMask & (1 << %d)) != 0 {\n", indent, i)
				fmt.Fprintf(b, "%s\tscanBuf[scanCount] = &item.%s\n", indent, uc.FieldName)
				fmt.Fprintf(b, "%s\tscanCount++\n", indent)
				fmt.Fprintf(b, "%s}\n", indent)
			}
			for _, cc := range cs.CondCols {
				flagField := toGoFieldName(cc.CondFlag)
				fmt.Fprintf(b, "%sif p.%s {\n", indent, flagField)
				fmt.Fprintf(b, "%s\tscanBuf[scanCount] = &item.%s\n", indent, cc.FieldName)
				fmt.Fprintf(b, "%s\tscanCount++\n", indent)
				fmt.Fprintf(b, "%s}\n", indent)
			}
			fmt.Fprintf(b, "%sscanSlice := scanBuf[:scanCount]\n", indent)
		} else if len(cs.CondCols) > 0 {
			fmt.Fprintf(b, "%sscanCount := %d\n", indent, len(cs.UncondCols))
			for i, uc := range cs.UncondCols {
				fmt.Fprintf(b, "%sscanBuf[%d] = &item.%s\n", indent, i, uc.FieldName)
			}
			for _, cc := range cs.CondCols {
				flagField := toGoFieldName(cc.CondFlag)
				fmt.Fprintf(b, "%sif p.%s {\n", indent, flagField)
				fmt.Fprintf(b, "%s\tscanBuf[scanCount] = &item.%s\n", indent, cc.FieldName)
				fmt.Fprintf(b, "%s\tscanCount++\n", indent)
				fmt.Fprintf(b, "%s}\n", indent)
			}
			fmt.Fprintf(b, "%sscanSlice := scanBuf[:scanCount]\n", indent)
		} else {
			for i, c := range cs.Cols {
				fmt.Fprintf(b, "%sscanBuf[%d] = &item.%s\n", indent, i, c.FieldName)
			}
			fmt.Fprintf(b, "%sscanSlice := scanBuf[:%d]\n", indent, len(cs.Cols))
		}
	}

	// 3. KindOne method
	if cs.Kind == KindOne {
		fmt.Fprintf(b, "func (q *Queries) %s(ctx context.Context%s) (%s, error) {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
		fmt.Fprintf(b, "\tvar zero %s\n", cs.RowTypeName)
		emitLocalParamsInit("\t")
		fmt.Fprintf(b, "\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
		b.WriteString("\t_ = colMask\n")
		b.WriteString("\targBuf := runtime.AcquireArgs(approxArgs)\n")
		b.WriteString("\tdefer runtime.ReleaseArgs(argBuf)\n")
		fmt.Fprintf(b, "\tif err := expand%sParams(argBuf, p); err != nil {\n", cs.Name)
		b.WriteString("\t\treturn zero, err\n")
		b.WriteString("\t}\n")
		fmt.Fprintf(b, "\tbuildSQL := func() string {\n\t\tqVal, _, _ := prepare%s(%s)\n\t\treturn runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)\n\t}\n", cs.Name, callStr)
		fmt.Fprintf(b, "\tsqlRow, ms, err := q.cache.QueryRow(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, %s, buildSQL, (*argBuf)...)\n", cs.Name, fastSlotExpr)
		b.WriteString("\tif err != nil {\n")
		b.WriteString("\t\treturn zero, err\n")
		b.WriteString("\t}\n")
		emitScanSetup("\t")
		b.WriteString("\terr = sqlRow.Scan(scanSlice...)\n")
		b.WriteString("\tif ms != nil {\n")
		b.WriteString("\t\t_ = ms.Release()\n")
		b.WriteString("\t}\n")
		b.WriteString("\tif err != nil {\n")
		b.WriteString("\t\treturn zero, err\n")
		b.WriteString("\t}\n")
		b.WriteString("\treturn item, nil\n")
		b.WriteString("}\n\n")
		return
	}

	// 4. KindMany methods: <Name>, <Name>Into, <Name>Seq, <Name>Each
	fmt.Fprintf(b, "func (q *Queries) %s(ctx context.Context%s) ([]%s, error) {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\treturn q.%sInto(ctx, nil%s)\n", cs.Name, callWithLeadingComma)
	b.WriteString("}\n\n")

	fmt.Fprintf(b, "func (q *Queries) %sInto(ctx context.Context, dst []%s%s) ([]%s, error) {\n", cs.Name, cs.RowTypeName, sigWithLeadingComma, cs.RowTypeName)
	emitLocalParamsInit("\t")
	fmt.Fprintf(b, "\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
	b.WriteString("\t_ = colMask\n")
	b.WriteString("\targBuf := runtime.AcquireArgs(approxArgs)\n")
	b.WriteString("\tdefer runtime.ReleaseArgs(argBuf)\n")
	fmt.Fprintf(b, "\tif err := expand%sParams(argBuf, p); err != nil {\n", cs.Name)
	b.WriteString("\t\treturn nil, err\n")
	b.WriteString("\t}\n")
	fmt.Fprintf(b, "\tbuildSQL := func() string {\n\t\tqVal, _, _ := prepare%s(%s)\n\t\treturn runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)\n\t}\n", cs.Name, callStr)
	fmt.Fprintf(b, "\tsqlRows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, %s, buildSQL, (*argBuf)...)\n", cs.Name, fastSlotExpr)
	b.WriteString("\tif err != nil {\n")
	b.WriteString("\t\treturn nil, err\n")
	b.WriteString("\t}\n")
	emitScanSetup("\t")
	b.WriteString("\tfor sqlRows.Next() {\n")
	if !cs.ScalarResult {
		fmt.Fprintf(b, "\t\titem = %s{}\n", cs.RowTypeName)
	}
	b.WriteString("\t\tif err := sqlRows.Scan(scanSlice...); err != nil {\n")
	b.WriteString("\t\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\t\tif ms != nil {\n\t\t\t\t_ = ms.Release()\n\t\t\t}\n")
	b.WriteString("\t\t\treturn nil, err\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t\tdst = append(dst, item)\n")
	b.WriteString("\t}\n")
	b.WriteString("\terr = sqlRows.Err()\n")
	b.WriteString("\t_ = sqlRows.Close()\n")
	b.WriteString("\tif ms != nil {\n\t\t_ = ms.Release()\n\t}\n")
	b.WriteString("\tif err != nil {\n")
	b.WriteString("\t\treturn nil, err\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn dst, nil\n")
	b.WriteString("}\n\n")

	// <Name>Seq returning iter.Seq2[Row, error]
	fmt.Fprintf(b, "func (q *Queries) %sSeq(ctx context.Context%s) iter.Seq2[%s, error] {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	b.WriteString("\treturn func(yield func(" + cs.RowTypeName + ", error) bool) {\n")
	fmt.Fprintf(b, "\t\tvar zero %s\n", cs.RowTypeName)
	emitLocalParamsInit("\t\t")
	fmt.Fprintf(b, "\t\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
	b.WriteString("\t\t_ = colMask\n")
	b.WriteString("\t\targBuf := runtime.AcquireArgs(approxArgs)\n")
	b.WriteString("\t\tdefer runtime.ReleaseArgs(argBuf)\n")
	fmt.Fprintf(b, "\t\tif err := expand%sParams(argBuf, p); err != nil {\n", cs.Name)
	b.WriteString("\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\treturn\n")
	b.WriteString("\t\t}\n")
	fmt.Fprintf(b, "\t\tbuildSQL := func() string {\n\t\t\tqVal, _, _ := prepare%s(%s)\n\t\t\treturn runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)\n\t\t}\n", cs.Name, callStr)
	fmt.Fprintf(b, "\t\tsqlRows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, %s, buildSQL, (*argBuf)...)\n", cs.Name, fastSlotExpr)
	b.WriteString("\t\tif err != nil {\n")
	b.WriteString("\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\treturn\n")
	b.WriteString("\t\t}\n")
	emitScanSetup("\t\t")
	b.WriteString("\t\tfor sqlRows.Next() {\n")
	if !cs.ScalarResult {
		fmt.Fprintf(b, "\t\t\titem = %s{}\n", cs.RowTypeName)
	}
	b.WriteString("\t\t\tif err := sqlRows.Scan(scanSlice...); err != nil {\n")
	b.WriteString("\t\t\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\t\t\tif ms != nil {\n\t\t\t\t\t_ = ms.Release()\n\t\t\t\t}\n")
	b.WriteString("\t\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\t\treturn\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t\tif !yield(item, nil) {\n")
	b.WriteString("\t\t\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\t\t\tif ms != nil {\n\t\t\t\t\t_ = ms.Release()\n\t\t\t\t}\n")
	b.WriteString("\t\t\t\treturn\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t\terr = sqlRows.Err()\n")
	b.WriteString("\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\tif ms != nil {\n\t\t\t_ = ms.Release()\n\t\t}\n")
	b.WriteString("\t\tif err != nil {\n")
	b.WriteString("\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("}\n\n")

	// <Name>Each
	fmt.Fprintf(b, "func (q *Queries) %sEach(ctx context.Context%s, fn func(%s) error) error {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\tfor row, err := range q.%sSeq(ctx%s) {\n", cs.Name, callWithLeadingComma)
	b.WriteString("\t\tif err != nil {\n\t\t\treturn err\n\t\t}\n")
	b.WriteString("\t\tif err := fn(row); err != nil {\n\t\t\treturn err\n\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn nil\n")
	b.WriteString("}\n\n")

	if cs.CanBatch {
		emitManyBatchMethods(b, cs, sigWithLeadingComma, callWithLeadingComma, callStr, paramsStructName, emitScanSetup)
	}
}

func emitExecBatchMethod(b *bytes.Buffer, cs *compiledStmt, sigWithLeadingComma, callStr, paramsStructName string) {
	batchParam := cs.Params[cs.BatchParamIdx]
	tupleAccess := batchParam.ParamName
	if cs.UseParamsStruct {
		tupleAccess = "p." + batchParam.FieldName
	}

	fmt.Fprintf(b, "func (q *Queries) %sBatch(ctx context.Context%s) (sql.Result, error) {\n", cs.Name, sigWithLeadingComma)
	fmt.Fprintf(b, "\ttotalRows := len(%s)\n", tupleAccess)
	b.WriteString("\tif totalRows == 0 {\n\t\treturn nil, runtime.ErrEmptySliceParam\n\t}\n")
	if !cs.UseParamsStruct {
		fmt.Fprintf(b, "\tp := %s{\n", paramsStructName)
		for _, p := range cs.Params {
			fmt.Fprintf(b, "\t\t%s: %s,\n", p.FieldName, p.ParamName)
		}
		b.WriteString("\t}\n")
	}
	fmt.Fprintf(b, "\t_, colMask, flagMask, _ := compute%sKey(p)\n", cs.Name)
	b.WriteString("\tshapeTag := runtime.ComputeBatchShapeTag(colMask, flagMask)\n")
	fmt.Fprintf(b, "\tfirstChunk := q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows, %d, true)\n", cs.Name, cs.MaxRowsPerStmt)
	b.WriteString("\ttxCache := q.txCache\n")
	b.WriteString("\tvar autoTx *sql.Tx\n")
	b.WriteString("\tif txCache == nil && firstChunk < totalRows {\n")
	b.WriteString("\t\tvar err error\n")
	b.WriteString("\t\tautoTx, err = q.db.BeginTx(ctx, nil)\n")
	b.WriteString("\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
	b.WriteString("\t\ttxCache = runtime.NewTxStmtCache(autoTx)\n")
	b.WriteString("\t\tdefer func() { _ = txCache.Close() }()\n")
	b.WriteString("\t}\n")
	b.WriteString("\tvar batchRes runtime.BatchExecResult\n")
	b.WriteString("\tfor offset := 0; offset < totalRows; {\n")
	b.WriteString("\t\tchunkSize := firstChunk\n")
	b.WriteString("\t\tif offset > 0 {\n")
	fmt.Fprintf(b, "\t\t\tchunkSize = q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows-offset, %d, false)\n", cs.Name, cs.MaxRowsPerStmt)
	b.WriteString("\t\t}\n")
	fmt.Fprintf(b, "\t\tchunkRows := %s[offset : offset+chunkSize]\n", tupleAccess)
	b.WriteString("\t\tchunkP := p\n")
	fmt.Fprintf(b, "\t\tchunkP.%s = chunkRows\n", batchParam.FieldName)
	fmt.Fprintf(b, "\t\tvk, _, _, approxArgs := compute%sKey(chunkP)\n", cs.Name)
	b.WriteString("\t\targBuf := runtime.AcquireArgs(approxArgs)\n")
	fmt.Fprintf(b, "\t\tif err := expand%sParams(argBuf, chunkP); err != nil {\n", cs.Name)
	b.WriteString("\t\t\truntime.ReleaseArgs(argBuf)\n")
	b.WriteString("\t\t\tif autoTx != nil {\n\t\t\t\t_ = autoTx.Rollback()\n\t\t\t}\n")
	b.WriteString("\t\t\treturn nil, err\n")
	b.WriteString("\t\t}\n")
	var buildCall string
	if cs.UseParamsStruct {
		buildCall = "chunkP"
	} else {
		var chunkCall strings.Builder
		for i, p := range cs.Params {
			if i > 0 {
				chunkCall.WriteString(", ")
			}
			if i == cs.BatchParamIdx {
				chunkCall.WriteString("chunkRows")
			} else {
				chunkCall.WriteString(p.ParamName)
			}
		}
		buildCall = chunkCall.String()
	}
	fmt.Fprintf(b, "\t\tbuildSQL := func() string {\n\t\t\tqVal, _, _ := prepare%s(%s)\n\t\t\treturn runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)\n\t\t}\n", cs.Name, buildCall)
	fmt.Fprintf(b, "\t\tres, err := q.cache.Exec(ctx, q.db, txCache, q.usePrepared, vk, stmtID%s, shapeTag, chunkSize, nil, buildSQL, (*argBuf)...)\n", cs.Name)
	b.WriteString("\t\truntime.ReleaseArgs(argBuf)\n")
	b.WriteString("\t\tif err != nil {\n")
	b.WriteString("\t\t\tif autoTx != nil {\n\t\t\t\t_ = autoTx.Rollback()\n\t\t\t}\n")
	b.WriteString("\t\t\treturn nil, err\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t\tbatchRes.Add(res)\n")
	b.WriteString("\t\toffset += chunkSize\n")
	b.WriteString("\t}\n")
	b.WriteString("\tif autoTx != nil {\n")
	b.WriteString("\t\tif err := autoTx.Commit(); err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn batchRes, nil\n")
	b.WriteString("}\n\n")
}

func emitManyBatchMethods(b *bytes.Buffer, cs *compiledStmt, sigWithLeadingComma, callWithLeadingComma, callStr, paramsStructName string, emitScanSetup func(indent string)) {
	batchParam := cs.Params[cs.BatchParamIdx]
	tupleAccess := batchParam.ParamName
	if cs.UseParamsStruct {
		tupleAccess = "p." + batchParam.FieldName
	}

	var buildCall string
	if cs.UseParamsStruct {
		buildCall = "chunkP"
	} else {
		var chunkCall strings.Builder
		for i, p := range cs.Params {
			if i > 0 {
				chunkCall.WriteString(", ")
			}
			if i == cs.BatchParamIdx {
				chunkCall.WriteString("chunkRows")
			} else {
				chunkCall.WriteString(p.ParamName)
			}
		}
		buildCall = chunkCall.String()
	}

	// <Name>Batch
	fmt.Fprintf(b, "func (q *Queries) %sBatch(ctx context.Context%s) ([]%s, error) {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\treturn q.%sBatchInto(ctx, nil%s)\n", cs.Name, callWithLeadingComma)
	b.WriteString("}\n\n")

	// <Name>BatchInto (starts internal transaction if q.txCache == nil && firstChunk < totalRows)
	fmt.Fprintf(b, "func (q *Queries) %sBatchInto(ctx context.Context, dst []%s%s) ([]%s, error) {\n", cs.Name, cs.RowTypeName, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\ttotalRows := len(%s)\n", tupleAccess)
	b.WriteString("\tif totalRows == 0 {\n\t\treturn nil, runtime.ErrEmptySliceParam\n\t}\n")
	if !cs.UseParamsStruct {
		fmt.Fprintf(b, "\tp := %s{\n", paramsStructName)
		for _, p := range cs.Params {
			fmt.Fprintf(b, "\t\t%s: %s,\n", p.FieldName, p.ParamName)
		}
		b.WriteString("\t}\n")
	}
	fmt.Fprintf(b, "\t_, colMask, flagMask, _ := compute%sKey(p)\n", cs.Name)
	b.WriteString("\tshapeTag := runtime.ComputeBatchShapeTag(colMask, flagMask)\n")
	fmt.Fprintf(b, "\tfirstChunk := q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows, %d, true)\n", cs.Name, cs.MaxRowsPerStmt)
	b.WriteString("\ttxCache := q.txCache\n")
	b.WriteString("\tvar autoTx *sql.Tx\n")
	b.WriteString("\tif txCache == nil && firstChunk < totalRows {\n")
	b.WriteString("\t\tvar err error\n")
	b.WriteString("\t\tautoTx, err = q.db.BeginTx(ctx, nil)\n")
	b.WriteString("\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
	b.WriteString("\t\ttxCache = runtime.NewTxStmtCache(autoTx)\n")
	b.WriteString("\t\tdefer func() { _ = txCache.Close() }()\n")
	b.WriteString("\t}\n")
	b.WriteString("\tfor offset := 0; offset < totalRows; {\n")
	b.WriteString("\t\tchunkSize := firstChunk\n")
	b.WriteString("\t\tif offset > 0 {\n")
	fmt.Fprintf(b, "\t\t\tchunkSize = q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows-offset, %d, false)\n", cs.Name, cs.MaxRowsPerStmt)
	b.WriteString("\t\t}\n")
	fmt.Fprintf(b, "\t\tchunkRows := %s[offset : offset+chunkSize]\n", tupleAccess)
	b.WriteString("\t\tchunkP := p\n")
	fmt.Fprintf(b, "\t\tchunkP.%s = chunkRows\n", batchParam.FieldName)
	fmt.Fprintf(b, "\t\tvk, _, _, approxArgs := compute%sKey(chunkP)\n", cs.Name)
	b.WriteString("\t\targBuf := runtime.AcquireArgs(approxArgs)\n")
	fmt.Fprintf(b, "\t\tif err := expand%sParams(argBuf, chunkP); err != nil {\n", cs.Name)
	b.WriteString("\t\t\truntime.ReleaseArgs(argBuf)\n")
	b.WriteString("\t\t\tif autoTx != nil {\n\t\t\t\t_ = autoTx.Rollback()\n\t\t\t}\n")
	b.WriteString("\t\t\treturn nil, err\n")
	b.WriteString("\t\t}\n")
	fmt.Fprintf(b, "\t\tbuildSQL := func() string {\n\t\t\tqVal, _, _ := prepare%s(%s)\n\t\t\treturn runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)\n\t\t}\n", cs.Name, buildCall)
	fmt.Fprintf(b, "\t\tsqlRows, ms, err := q.cache.Query(ctx, q.db, txCache, q.usePrepared, vk, stmtID%s, shapeTag, chunkSize, nil, buildSQL, (*argBuf)...)\n", cs.Name)
	b.WriteString("\t\truntime.ReleaseArgs(argBuf)\n")
	b.WriteString("\t\tif err != nil {\n")
	b.WriteString("\t\t\tif autoTx != nil {\n\t\t\t\t_ = autoTx.Rollback()\n\t\t\t}\n")
	b.WriteString("\t\t\treturn nil, err\n")
	b.WriteString("\t\t}\n")
	emitScanSetup("\t\t")
	b.WriteString("\t\tfor sqlRows.Next() {\n")
	if !cs.ScalarResult {
		fmt.Fprintf(b, "\t\t\titem = %s{}\n", cs.RowTypeName)
	}
	b.WriteString("\t\t\tif err := sqlRows.Scan(scanSlice...); err != nil {\n")
	b.WriteString("\t\t\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\t\t\tif ms != nil {\n\t\t\t\t\t_ = ms.Release()\n\t\t\t\t}\n")
	b.WriteString("\t\t\t\tif autoTx != nil {\n\t\t\t\t\t_ = autoTx.Rollback()\n\t\t\t\t}\n")
	b.WriteString("\t\t\t\treturn nil, err\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t\tdst = append(dst, item)\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t\terr = sqlRows.Err()\n")
	b.WriteString("\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\tif ms != nil {\n\t\t\t_ = ms.Release()\n\t\t}\n")
	b.WriteString("\t\tif err != nil {\n")
	b.WriteString("\t\t\tif autoTx != nil {\n\t\t\t\t_ = autoTx.Rollback()\n\t\t\t}\n")
	b.WriteString("\t\t\treturn nil, err\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t\toffset += chunkSize\n")
	b.WriteString("\t}\n")
	b.WriteString("\tif autoTx != nil {\n")
	b.WriteString("\t\tif err := autoTx.Commit(); err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn dst, nil\n")
	b.WriteString("}\n\n")

	// <Name>BatchSeq (executes chunks using q.txCache as-is, without opening a hidden transaction)
	fmt.Fprintf(b, "func (q *Queries) %sBatchSeq(ctx context.Context%s) iter.Seq2[%s, error] {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	b.WriteString("\treturn func(yield func(" + cs.RowTypeName + ", error) bool) {\n")
	fmt.Fprintf(b, "\t\tvar zero %s\n", cs.RowTypeName)
	fmt.Fprintf(b, "\t\ttotalRows := len(%s)\n", tupleAccess)
	b.WriteString("\t\tif totalRows == 0 {\n\t\t\tyield(zero, runtime.ErrEmptySliceParam)\n\t\t\treturn\n\t\t}\n")
	if !cs.UseParamsStruct {
		fmt.Fprintf(b, "\t\tp := %s{\n", paramsStructName)
		for _, p := range cs.Params {
			fmt.Fprintf(b, "\t\t\t%s: %s,\n", p.FieldName, p.ParamName)
		}
		b.WriteString("\t\t}\n")
	}
	fmt.Fprintf(b, "\t\t_, colMask, flagMask, _ := compute%sKey(p)\n", cs.Name)
	b.WriteString("\t\tshapeTag := runtime.ComputeBatchShapeTag(colMask, flagMask)\n")
	fmt.Fprintf(b, "\t\tfirstChunk := q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows, %d, true)\n", cs.Name, cs.MaxRowsPerStmt)
	b.WriteString("\t\tfor offset := 0; offset < totalRows; {\n")
	b.WriteString("\t\t\tchunkSize := firstChunk\n")
	b.WriteString("\t\t\tif offset > 0 {\n")
	fmt.Fprintf(b, "\t\t\t\tchunkSize = q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows-offset, %d, false)\n", cs.Name, cs.MaxRowsPerStmt)
	b.WriteString("\t\t\t}\n")
	fmt.Fprintf(b, "\t\t\tchunkRows := %s[offset : offset+chunkSize]\n", tupleAccess)
	b.WriteString("\t\t\tchunkP := p\n")
	fmt.Fprintf(b, "\t\t\tchunkP.%s = chunkRows\n", batchParam.FieldName)
	fmt.Fprintf(b, "\t\t\tvk, _, _, approxArgs := compute%sKey(chunkP)\n", cs.Name)
	b.WriteString("\t\t\targBuf := runtime.AcquireArgs(approxArgs)\n")
	fmt.Fprintf(b, "\t\t\tif err := expand%sParams(argBuf, chunkP); err != nil {\n", cs.Name)
	b.WriteString("\t\t\t\truntime.ReleaseArgs(argBuf)\n")
	b.WriteString("\t\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\t\treturn\n")
	b.WriteString("\t\t\t}\n")
	fmt.Fprintf(b, "\t\t\tbuildSQL := func() string {\n\t\t\t\tqVal, _, _ := prepare%s(%s)\n\t\t\t\treturn runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)\n\t\t\t}\n", cs.Name, buildCall)
	fmt.Fprintf(b, "\t\t\tsqlRows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, shapeTag, chunkSize, nil, buildSQL, (*argBuf)...)\n", cs.Name)
	b.WriteString("\t\t\truntime.ReleaseArgs(argBuf)\n")
	b.WriteString("\t\t\tif err != nil {\n")
	b.WriteString("\t\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\t\treturn\n")
	b.WriteString("\t\t\t}\n")
	emitScanSetup("\t\t\t")
	b.WriteString("\t\t\tfor sqlRows.Next() {\n")
	if !cs.ScalarResult {
		fmt.Fprintf(b, "\t\t\t\titem = %s{}\n", cs.RowTypeName)
	}
	b.WriteString("\t\t\t\tif err := sqlRows.Scan(scanSlice...); err != nil {\n")
	b.WriteString("\t\t\t\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\t\t\t\tif ms != nil {\n\t\t\t\t\t\t_ = ms.Release()\n\t\t\t\t\t}\n")
	b.WriteString("\t\t\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\t\t\treturn\n")
	b.WriteString("\t\t\t\t}\n")
	b.WriteString("\t\t\t\tif !yield(item, nil) {\n")
	b.WriteString("\t\t\t\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\t\t\t\tif ms != nil {\n\t\t\t\t\t\t_ = ms.Release()\n\t\t\t\t\t}\n")
	b.WriteString("\t\t\t\t\treturn\n")
	b.WriteString("\t\t\t\t}\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t\terr = sqlRows.Err()\n")
	b.WriteString("\t\t\t_ = sqlRows.Close()\n")
	b.WriteString("\t\t\tif ms != nil {\n\t\t\t\t_ = ms.Release()\n\t\t\t}\n")
	b.WriteString("\t\t\tif err != nil {\n")
	b.WriteString("\t\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\t\treturn\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t\toffset += chunkSize\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("}\n\n")

	// <Name>BatchEach
	fmt.Fprintf(b, "func (q *Queries) %sBatchEach(ctx context.Context%s, fn func(%s) error) error {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\tfor row, err := range q.%sBatchSeq(ctx%s) {\n", cs.Name, callWithLeadingComma)
	b.WriteString("\t\tif err != nil {\n\t\t\treturn err\n\t\t}\n")
	b.WriteString("\t\tif err := fn(row); err != nil {\n\t\t\treturn err\n\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn nil\n")
	b.WriteString("}\n\n")
}
