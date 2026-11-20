package sqlgen

import (
	"bytes"
	"errors"
	"fmt"
	"go/format"
	"go/token"
	"io"
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
	StmtVerb        = runtime.StmtVerb
	RenderInput     = runtime.RenderInput
	OpDecorators    = runtime.OpDecorators
	BranchGate      = runtime.BranchGate
	Pipeline        = runtime.Pipeline
	PipelineOp      = runtime.PipelineOp
	PipelineCond    = runtime.PipelineCond
	ExecFuture      = runtime.ExecFuture
	IfResult        = runtime.IfResult
	WirePipeliner   = runtime.WirePipeliner
	WireTxPipeliner = runtime.WireTxPipeliner
	CommitError     = runtime.CommitError
	TxRetryPolicy   = runtime.TxRetryPolicy
	TxOption        = runtime.TxOption
)

const (
	StateAbsent   = runtime.StateAbsent
	StatePartial  = runtime.StatePartial
	StatePrepared = runtime.StatePrepared
)

var (
	ErrEmptySliceParam    = runtime.ErrEmptySliceParam
	ErrUninitializedQuery = runtime.ErrUninitializedQuery
	ErrNilQuery           = runtime.ErrNilQuery
	ErrAssertionFailed    = runtime.ErrAssertionFailed
	ErrBranchNotTaken     = runtime.ErrBranchNotTaken
	ErrFlushInPipelineTx  = runtime.ErrFlushInPipelineTx
	ErrInvalidSeek        = runtime.ErrInvalidSeek
	ErrInvalidOrder       = runtime.ErrInvalidOrder

	NewStmtCache         = runtime.NewStmtCache
	NewTxStmtCache       = runtime.NewTxStmtCache
	WithMaxSQLVariants   = runtime.WithMaxSQLVariants
	WithMaxPartialBytes  = runtime.WithMaxPartialBytes
	WithMaxPreparedStmts = runtime.WithMaxPreparedStmts
	WithPromoteThreshold = runtime.WithPromoteThreshold
	WithMaxBatchParams   = runtime.WithMaxBatchParams
	NewSeek              = runtime.NewSeek
	PackOrder            = runtime.PackOrder
	ExpandOrder          = runtime.ExpandOrder
	Asc                  = runtime.Asc
	Desc                 = runtime.Desc
	MakeVariantKey       = runtime.MakeVariantKey
	ComputeSubqueryShape = runtime.ComputeSubqueryShape
	DefaultRetryPolicy   = runtime.DefaultRetryPolicy
	WithTxRetry          = runtime.WithTxRetry
	WrapConnector        = runtime.WrapConnector
	IsRetryableTxError   = runtime.IsRetryableTxError
)

// QueryKind specifies the execution and return cardinality of a SQL statement.
type QueryKind string

// StatementKind is an alias for QueryKind.
type StatementKind = QueryKind

const (
	KindExec     QueryKind = "exec"
	KindOne      QueryKind = "one"
	KindMany     QueryKind = "many"
	KindSubquery QueryKind = "subquery"
	KindQuery    QueryKind = "query" // alias for KindSubquery
)

// Dialect specifies the SQL placeholder syntax in sqlgen.yaml.
type Dialect string

const (
	DialectQuestion Dialect = "question" // ? (SQLite, MySQL, MariaDB)
	DialectDollar   Dialect = "dollar"   // $1, $2, ... (PostgreSQL, CockroachDB)
	DialectPostgres Dialect = "dollar"
)

// ColumnSpec defines a fallback output column declaration in YAML.
type ColumnSpec struct {
	Name     string `yaml:"name"`
	GoType   string `yaml:"type"`
	SQLExpr  string `yaml:"-"`
	FlagName string `yaml:"-"`
	FlagBit  uint64 `yaml:"-"`
}

// ParamSpec defines a fallback parameter declaration in YAML.
type ParamSpec struct {
	Name    string `yaml:"name"`
	GoType  string `yaml:"type"`
	Slice   bool   `yaml:"slice"`
	HasSort bool   `yaml:"-"`
	Desc    bool   `yaml:"-"`
}

// StatementSpec defines a single SQL statement in the YAML configuration.
type StatementSpec struct {
	Name         string       `yaml:"name"`
	Kind         QueryKind    `yaml:"kind"`
	ResultType   string       `yaml:"result_type"`
	ParamsStruct bool         `yaml:"params_struct"`
	SQL          string       `yaml:"sql"`
	Params       []ParamSpec  `yaml:"params"`
	Columns      []ColumnSpec `yaml:"columns"`
}

// Config represents the top-level YAML configuration schema (sqlgen.yaml).
type Config struct {
	Version       string          `yaml:"version"`
	PackageName   string          `yaml:"package"`
	Out           string          `yaml:"out"`
	RuntimeImport string          `yaml:"runtime_import"`
	ExtraImports  []string        `yaml:"extra_imports"`
	Dialect       Dialect         `yaml:"dialect"`
	Statements    []StatementSpec `yaml:"statements"`
}

func parseParamScalarString(line int, raw string) (ParamSpec, error) {
	raw = strings.TrimSpace(raw)
	colon := strings.IndexByte(raw, ':')
	if colon <= 0 || colon == len(raw)-1 {
		return ParamSpec{}, fmt.Errorf("line %d: invalid compact param spec %q (expected \"name:type\")", line, raw)
	}
	namePart := strings.TrimSpace(raw[:colon])
	typePart := strings.TrimSpace(raw[colon+1:])
	isSlice := false
	hasSort := false
	desc := false

	if strings.HasSuffix(namePart, "...") {
		isSlice = true
		namePart = strings.TrimSpace(strings.TrimSuffix(namePart, "..."))
	}
	// Check for trailing ASC / DESC modifier on typePart
	if fields := strings.Fields(typePart); len(fields) == 2 {
		mod := strings.ToLower(fields[1])
		if mod == "asc" || mod == "desc" {
			hasSort = true
			desc = mod == "desc"
			typePart = fields[0]
		}
	}
	if strings.HasSuffix(typePart, "...") {
		isSlice = true
		typePart = strings.TrimSpace(strings.TrimSuffix(typePart, "..."))
	}
	if namePart == "" || typePart == "" {
		return ParamSpec{}, fmt.Errorf("line %d: invalid compact param spec %q", line, raw)
	}
	return ParamSpec{
		Name:    namePart,
		GoType:  typePart,
		Slice:   isSlice,
		HasSort: hasSort,
		Desc:    desc,
	}, nil
}

// UnmarshalYAML supports 3 YAML forms for ParamSpec:
// 1. Mapping: { name: ID, type: int64, slice: true }
// 2. Single-key shorthand map: - ID: int64 (or - org_ids...: int64)
// 3. Compact scalar string: - "ID:int64" (or - "org_ids...:int64")
func (p *ParamSpec) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		ps, err := parseParamScalarString(value.Line, value.Value)
		if err != nil {
			return err
		}
		*p = ps
		return nil

	case yaml.MappingNode:
		if len(value.Content) == 2 {
			kNode := value.Content[0]
			vNode := value.Content[1]
			if kNode.Value != "name" && kNode.Value != "type" && kNode.Value != "slice" {
				if vNode.Kind != yaml.ScalarNode {
					return fmt.Errorf("line %d: shorthand param %q value must be a scalar type", vNode.Line, kNode.Value)
				}
				ps, err := parseParamScalarString(kNode.Line, kNode.Value+":"+vNode.Value)
				if err != nil {
					return err
				}
				*p = ps
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
				if vNode.Kind != yaml.ScalarNode || (vNode.Value != "true" && vNode.Value != "false") {
					return fmt.Errorf("line %d: param \"slice\" must be a boolean", vNode.Line)
				}
				slice = vNode.Value == "true"
			default:
				return fmt.Errorf("line %d: unknown field %q in param spec", kNode.Line, kNode.Value)
			}
		}
		if name == "" || typ == "" {
			return fmt.Errorf("line %d: param mapping requires both \"name\" and \"type\"", value.Line)
		}
		ps, err := parseParamScalarString(value.Line, name+":"+typ)
		if err != nil {
			return err
		}
		if slice {
			ps.Slice = true
		}
		*p = ps
		return nil

	default:
		return fmt.Errorf("line %d: invalid param spec YAML node kind", value.Line)
	}
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
		c.GoType = typePart
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
				c.GoType = typePart
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
		c.GoType = typ
		return nil

	default:
		return fmt.Errorf("line %d: invalid column spec YAML node kind", value.Line)
	}
}

// ParseYAML strictly decodes a sqlgen YAML configuration using KnownFields(true).
func ParseYAML(data []byte) (Config, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return Config{}, errors.New("sqlgen: empty YAML configuration")
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("sqlgen: parse YAML: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Config{}, errors.New("sqlgen: trailing YAML document is not allowed")
		}
		return Config{}, fmt.Errorf("sqlgen: trailing YAML content: %w", err)
	}
	if cfg.Version != "" && cfg.Version != "1" {
		return Config{}, fmt.Errorf("sqlgen: unsupported config version %q (expected \"1\")", cfg.Version)
	}
	return cfg, nil
}

// GenerateFromYAML parses YAML configuration bytes and generates formatted Go source code.
func GenerateFromYAML(data []byte) ([]byte, error) {
	cfg, err := ParseYAML(data)
	if err != nil {
		return nil, err
	}
	return Generate(cfg)
}

// WriteFileAtomic writes data to targetPath atomically using a temporary file in the same directory.
func WriteFileAtomic(targetPath string, data []byte) error {
	dir := filepath.Dir(targetPath)
	tmp, err := os.CreateTemp(dir, ".sqlgen-*.tmp")
	if err != nil {
		return fmt.Errorf("sqlgen: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("sqlgen: write temp file: %w", err)
	}
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("sqlgen: chmod temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("sqlgen: close temp file: %w", err)
	}
	if err := os.Rename(tmpName, targetPath); err != nil {
		cleanup()
		return fmt.Errorf("sqlgen: rename %s: %w", targetPath, err)
	}
	return nil
}

// GenerateFromYAMLFile reads a YAML file from cfgPath, generates Go source code, and writes it atomically.
func GenerateFromYAMLFile(cfgPath, outOverride string) error {
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("sqlgen: read %s: %w", cfgPath, err)
	}
	cfg, err := ParseYAML(data)
	if err != nil {
		return err
	}
	if outOverride != "" {
		cfg.Out = outOverride
	}
	src, err := Generate(cfg)
	if err != nil {
		return err
	}
	outPath := cfg.Out
	if outPath == "" {
		outPath = "sqlgen_gen.go"
	}
	if !filepath.IsAbs(outPath) && outOverride == "" {
		outPath = filepath.Join(filepath.Dir(cfgPath), outPath)
	}
	return WriteFileAtomic(outPath, src)
}

// GenerateFile reads a YAML file from cfgPath, generates Go source code, and writes it to cfg.Out.
func GenerateFile(cfgPath string) error {
	return GenerateFromYAMLFile(cfgPath, "")
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

func isOrderableGoType(t string) bool {
	switch t {
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
		"byte", "rune", "float32", "float64", "string",
		"time.Time", "[]byte", "[]uint8":
		return true
	default:
		return false
	}
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
	"sku":   "SKU",
	"skus":  "SKUs",
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
	var buf []byte
	for _, w := range words {
		low := strings.ToLower(w)
		if initCap, ok := commonInitialisms[low]; ok {
			buf = append(buf, initCap...)
		} else {
			runes := []rune(low)
			runes[0] = unicode.ToUpper(runes[0])
			buf = append(buf, string(runes)...)
		}
	}
	out := string(buf)
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
	var buf []byte
	for i, w := range words {
		low := strings.ToLower(w)
		if i == 0 {
			buf = append(buf, low...)
			continue
		}
		if initCap, ok := commonInitialisms[low]; ok {
			buf = append(buf, initCap...)
		} else {
			runes := []rune(low)
			runes[0] = unicode.ToUpper(runes[0])
			buf = append(buf, string(runes)...)
		}
	}
	out := string(buf)
	if token.Lookup(out).IsKeyword() || reservedParamNames[out] {
		out += "Param"
	}
	return out
}

// reservedParamNames are identifiers a generated function declares or uses in its own scope: a SQL
// parameter with one of these names becomes a Go parameter that would redeclare or shadow it (for
// example `:in:string` vs the generated `in := runtime.RenderInput{}`). TestReservedNamesCoverGeneratedLocals
// derives the locals from real generated code, so this list cannot silently go stale.
var reservedParamNames = func() map[string]bool {
	m := map[string]bool{}
	for _, n := range strings.Fields(`
		_ approxArgs argBuf autoTx batchRes bufPtr chunk chunkCap chunkP chunkRows chunkSize colMask cp d0 d1
		dst eFn effMask err expandErr firstChunk flagMask flushChunk hasSeek i idx idxBuf in item j k maxRows ms
		offset orderKey orderSpec out p r res row rs scanBuf scanCount scanDest scanSlice shapeLens shapeTag
		sliceLensBuf sortedIdx sqlRow sqlRows sub subsBuf tFn targetQ tc totalRows tupleLensBuf txCache txQ v vk
		yield zero
		ctx q pl fn seq
		sql runtime context iter slices sync atomic time json big netip url driver errors strconv
		append cap clear close complete copy delete imag len make max min new panic print println real recover
		any bool byte comparable complex128 complex64 error float32 float64 int int16 int32 int64 int8 rune string
		uint uint16 uint32 uint64 uint8 uintptr true false iota nil`) {
		m[n] = true
	}
	return m
}()

// reservedTypeNames are package-level names every generated file declares; a result_type or tuple struct
// with one of these names would redeclare it. reservedStatementNames are the methods generated on
// *Queries and *Pipeline (including those promoted from runtime.Pipeline).
// TestReservedNamesCoverGeneratedDeclarations checks both against real generated output.
var (
	reservedTypeNames = setOf("DefaultRetryPolicy", "ExecFuture", "IfResult", "New", "NewSeek", "NewWithCache", "Order",
		"Pipeline", "PipelineCond", "Queries", "Query", "Seek", "TxOption", "TxRetryPolicy", "WhereCol", "WithTxRetry")
	reservedStatementNames = setOf("Cache", "CacheStats", "Close", "If", "InTx", "InTxOpts", "Pipeline", "PipelineTx",
		"WithPrepared", "WithTx", "SetBuildError", "QueueExec", "Abort", "Flush", "Exec")
)

func setOf(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

func lowerFirstIdent(s string) string {
	if s == "" {
		return "v"
	}
	runes := []rune(s)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
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
	HasSort   bool
	Desc      bool
}

type tupleStructDef struct {
	Name     string
	Fields   []tupleFieldDef
	Segments []string
	HasSort  bool
}

type paramDef struct {
	RawName    string
	FieldName  string
	ParamName  string
	Kind       paramKind
	GoType     string
	ElemGoType string
	Tuple      *tupleStructDef
	HasSort    bool
	Desc       bool
	InOptBlock bool
}

type colDef struct {
	RawName   string
	FieldName string
	GoType    string
	SQLExpr   string
	CondFlag  string
	UncondIdx int
}

type optFlagDef struct {
	RawName   string
	FieldName string
	Bit       uint8
}

type slotRef struct {
	Kind     runtime.SlotKind
	ParamIdx int
	FlagBit  uint8
	Tuple    *tupleStructDef
	Inner    *parsedBlock
}

type parsedBlock struct {
	Segments  []string
	Slots     []slotRef
	NumSlices int
	NumTuples int
	NumSubs   int
}

type orderColRef struct {
	RawRef    string
	ColIdx    int
	FieldName string
	GoType    string
}

// rowOrder records the sqlgen.Order column list first declared for a shared result type.
type rowOrder struct {
	stmt string
	cols []string
}

type compiledStmt struct {
	ID              uint32
	Name            string
	Kind            QueryKind
	StmtVerb        runtime.StmtVerb
	HasReturning    bool
	RowTypeName     string
	ScalarResult    bool
	UseParamsStruct bool
	CanBatch        bool
	CanStream       bool
	BatchParamIdx   int
	BatchIsTuple    bool
	ScalarParamCnt  int
	BatchArity      int
	MaxRowsPerStmt  int
	IsStatic        bool
	StaticSQL       string
	// PGCasts is set for INSERT statements under the dollar dialect: their value placeholders get a
	// type cast in the descriptor so that branch-gated INSERTs (rewritten to INSERT ... SELECT) type-check.
	PGCasts bool

	Root           parsedBlock
	SelectColExprs []string
	Params         []paramDef
	Cols           []colDef
	UncondCols     []colDef
	CondCols       []colDef
	Flags          []optFlagDef
	OrderCols      []orderColRef
	HasOrder       bool
	HasSeek        bool
	TotalSlices    int
	TotalTuples    int
	TotalSubs      int
}

// ParsedSQL holds the parsed segments and slot occurrences of a SQL statement for testing/fuzzing.
type ParsedSQL struct {
	Segments    []string
	Occurrences []slotRef
}

// ParseSQL parses a raw annotated SQL query string and returns its segments and occurrences.
func ParseSQL(sqlStr string) (*ParsedSQL, error) {
	cs, err := compileStatement(1, StatementSpec{
		Name: "FuzzStmt",
		Kind: KindSubquery,
		SQL:  sqlStr,
	})
	if err != nil {
		return nil, err
	}
	return &ParsedSQL{
		Segments:    cs.Root.Segments,
		Occurrences: cs.Root.Slots,
	}, nil
}

func parseDialect(raw Dialect) (runtime.Dialect, error) {
	switch strings.ToLower(strings.TrimSpace(string(raw))) {
	case "", "dollar", "postgres", "postgresql", "pgx", "cockroach", "cockroachdb", "$":
		return runtime.DialectDollar, nil
	case "question", "sqlite", "sqlite3", "mysql", "mariadb", "?":
		return runtime.DialectQuestion, nil
	default:
		return runtime.DialectDollar, fmt.Errorf("sqlgen: unsupported dialect %q", raw)
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

// scanOptionalSortModifier checks if s[pos:] has horizontal whitespace followed by ASC or DESC.
func scanOptionalSortModifier(s string, pos int) (hasSort bool, desc bool, nextPos int) {
	i := pos
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i == pos || i >= len(s) || !isIdentStartByte(s[i]) {
		return false, false, pos
	}
	wEnd := i + 1
	for wEnd < len(s) && isIdentContByte(s[wEnd]) {
		wEnd++
	}
	if wEnd < len(s) && s[wEnd] == ':' {
		return false, false, pos
	}
	word := strings.ToLower(s[i:wEnd])
	if word != "asc" && word != "desc" {
		return false, false, pos
	}
	return true, word == "desc", wEnd
}

// scanOptionalTypecastSuffix scans an optional PostgreSQL ::type[(...)] suffix starting at s[pos:].
func scanOptionalTypecastSuffix(s string, pos int) (suffix string, nextPos int) {
	if pos+2 >= len(s) || s[pos] != ':' || s[pos+1] != ':' || !isIdentStartByte(s[pos+2]) {
		return "", pos
	}
	end := pos + 3
	for end < len(s) && isIdentContByte(s[end]) {
		end++
	}
	if end < len(s) && s[end] == '(' {
		closeParen := strings.IndexByte(s[end:], ')')
		if closeParen > 0 {
			end += closeParen + 1
		}
	}
	if strings.HasPrefix(s[end:], "[]") {
		end += 2
	}
	return s[pos:end], end
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

func parseTupleBody(structName, body string) (*tupleStructDef, error) {
	if !isValidGoIdent(structName) {
		return nil, fmt.Errorf("invalid tuple struct name %q", structName)
	}
	if reservedTypeNames[structName] {
		return nil, fmt.Errorf("tuple struct name %q collides with a type or function declared in every generated file; rename it", structName)
	}
	td := &tupleStructDef{
		Name: structName,
	}
	var seg []byte
	seenFields := make(map[string]bool)
	i := 0
	for i < len(body) {
		ch := body[i]
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
			seg = append(seg, body[start:i]...)
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
			seg = append(seg, body[start:i]...)
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
			seg = append(seg, body[start:i]...)
			continue
		}
		if ch == '-' && i+1 < len(body) && body[i+1] == '-' {
			// Line comment: copied verbatim (SPEC 4.1) and always terminated, so a comment at the very end
			// of the body cannot swallow the ")" that closes each rendered row.
			start := i
			for i < len(body) && body[i] != '\n' {
				i++
			}
			if i < len(body) {
				i++
			}
			seg = append(seg, body[start:i]...)
			if body[i-1] != '\n' {
				seg = append(seg, '\n')
			}
			continue
		}
		if ch == '/' && i+1 < len(body) && body[i+1] == '*' {
			end := strings.Index(body[i+2:], "*/")
			if end < 0 {
				return nil, errors.New("unclosed block comment in tuple body")
			}
			seg = append(seg, body[i:i+2+end+2]...)
			i += 2 + end + 2
			continue
		}
		if ch == '$' {
			if tag, ok := matchDollarQuoteTag(body, i); ok {
				end := strings.Index(body[i+len(tag):], tag)
				if end < 0 {
					return nil, errors.New("unclosed dollar-quoted string in tuple body")
				}
				seg = append(seg, body[i:i+len(tag)+end+len(tag)]...)
				i += len(tag) + end + len(tag)
				continue
			}
		}
		if ch == ':' && i+1 < len(body) && (body[i+1] == ':' || body[i+1] == '=') {
			seg = append(seg, body[i:i+2]...)
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

				// Check for optional ::typecast suffix and optional ASC / DESC modifier
				castSuffix, afterCast := scanOptionalTypecastSuffix(body, nextPos)
				hasSort, desc, afterSort := scanOptionalSortModifier(body, afterCast)
				if hasSort {
					if !isOrderableGoType(goType) {
						return nil, fmt.Errorf("tuple %s field %q of type %q is not orderable for ASC/DESC presorting", structName, ident, goType)
					}
					td.HasSort = true
					nextPos = afterSort
				} else {
					nextPos = afterCast
				}

				td.Segments = append(td.Segments, string(seg))
				seg = seg[:0]
				if castSuffix != "" {
					seg = append(seg, castSuffix...)
				}
				td.Fields = append(td.Fields, tupleFieldDef{
					RawName:   ident,
					FieldName: fieldName,
					GoType:    goType,
					HasSort:   hasSort,
					Desc:      desc,
				})
				i = nextPos
				continue
			}
			seg = append(seg, ident...)
			i = wEnd
			continue
		}
		seg = append(seg, ch)
		i++
	}
	td.Segments = append(td.Segments, string(seg))
	if len(td.Fields) == 0 {
		return nil, fmt.Errorf("tuple struct %s must contain at least one Field:Type annotation", structName)
	}
	return td, nil
}

type stmtParser struct {
	spec          StatementSpec
	prevWordLow   string // previous plain SQL word, lower-cased (for IS [NOT] DISTINCT FROM)
	yamlParams    map[string]ParamSpec
	paramsByCanon map[string]int
	flagsByName   map[string]uint8
	rawOrderCols  []string
	inOptBlock    bool
	currentFlag   string

	parenDepth   int
	bracketDepth int
	seenMainSel  bool
	inProjection bool

	hasTopWhere          bool
	needBranchGuardClose bool
	inSingleInsertValues bool

	projActive        bool
	projStartSegIdx   int
	projStartByteIdx  int
	projEndSegIdx     int
	projEndByteIdx    int
	projItemStartSeg  int
	projItemStartByte int
	projHasParamSlot  bool
	projHasOptBlock   bool

	cs *compiledStmt
}

func (p *stmtParser) addOrGetParam(rawName string, kind paramKind, goType, elemGoType string, tuple *tupleStructDef, hasSort, desc bool) (int, error) {
	canonKey := strings.ToLower(toGoParamName(rawName))
	if idx, exists := p.paramsByCanon[canonKey]; exists {
		existing := &p.cs.Params[idx]
		if existing.Kind != kind || existing.GoType != goType {
			return -1, fmt.Errorf("statement %s: parameter %q redeclared with conflicting types (%s vs %s)", p.spec.Name, rawName, existing.GoType, goType)
		}
		if existing.HasSort && hasSort && existing.Desc != desc {
			return -1, fmt.Errorf("statement %s: parameter %q redeclared with conflicting sort directions", p.spec.Name, rawName)
		}
		if hasSort {
			existing.HasSort = true
			existing.Desc = desc
		}
		if !p.inOptBlock {
			existing.InOptBlock = false
		}
		return idx, nil
	}
	if hasSort && kind != paramTupleSlice {
		targetType := elemGoType
		if targetType == "" {
			targetType = goType
		}
		if !isOrderableGoType(targetType) {
			return -1, fmt.Errorf("statement %s: parameter %q of type %q is not orderable for ASC/DESC presorting", p.spec.Name, rawName, targetType)
		}
	}
	fieldName := toGoFieldName(rawName)
	idx := len(p.cs.Params)
	p.cs.Params = append(p.cs.Params, paramDef{
		RawName:    rawName,
		FieldName:  fieldName,
		ParamName:  toGoParamName(rawName),
		Kind:       kind,
		GoType:     goType,
		ElemGoType: elemGoType,
		Tuple:      tuple,
		HasSort:    hasSort,
		Desc:       desc,
		InOptBlock: p.inOptBlock,
	})
	p.paramsByCanon[canonKey] = idx
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
		if len(p.cs.CondCols) > 0 {
			return fmt.Errorf("statement %s: unconditional output column %q follows a conditional {?...: ...} column; put every conditional column after all unconditional ones (the unconditional projection is spliced as one unit and scanned first)", p.spec.Name, rawName)
		}
		if len(p.cs.UncondCols) >= 64 {
			return fmt.Errorf("statement %s: more than 64 unconditional output columns (the SelectCols projection mask is 64 bits wide); split the query or return a narrower row", p.spec.Name)
		}
		col.UncondIdx = len(p.cs.UncondCols)
		p.cs.UncondCols = append(p.cs.UncondCols, col)
	} else {
		p.cs.CondCols = append(p.cs.CondCols, col)
	}
	p.cs.Cols = append(p.cs.Cols, col)
	return nil
}

func extractCurrentItemExpr(segments []string, curSeg string, itemSegIdx, itemByteIdx int) string {
	var buf []byte
	allSegs := append(slices.Clone(segments), curSeg)
	for sIdx := itemSegIdx; sIdx < len(allSegs); sIdx++ {
		seg := allSegs[sIdx]
		if sIdx == itemSegIdx {
			if itemByteIdx < len(seg) {
				buf = append(buf, seg[itemByteIdx:]...)
			}
		} else {
			buf = append(buf, seg...)
		}
	}
	return strings.TrimSpace(string(buf))
}

func (p *stmtParser) scanSpan(span string, blk *parsedBlock, isTopLevel bool) error {
	var seg []byte
	i := 0

	emitSlot := func(s slotRef) {
		if p.inProjection && !p.inOptBlock && s.Kind != runtime.SlotOptBlock && !p.projHasOptBlock {
			p.projHasParamSlot = true
		}
		blk.Segments = append(blk.Segments, string(seg))
		seg = seg[:0]
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

	flushBranchGuardBeforeEnd := func() {
		if !isTopLevel || p.inOptBlock {
			return
		}
		if p.cs.StmtVerb == runtime.VerbUpdate || p.cs.StmtVerb == runtime.VerbDelete {
			if p.needBranchGuardClose {
				p.needBranchGuardClose = false
				emitSlot(slotRef{Kind: runtime.SlotBranchGuardClose, ParamIdx: -1})
			} else if !p.hasTopWhere {
				p.hasTopWhere = true
				emitSlot(slotRef{Kind: runtime.SlotBranchGuardWhere, ParamIdx: -1})
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
			seg = append(seg, span[start:i]...)
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
			seg = append(seg, span[start:i]...)
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
			seg = append(seg, span[start:i]...)
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
				seg = append(seg, span[i:end]...)
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
			seg = append(seg, span[start:i]...)
			continue
		}

		// 6. Multi-line block comment /*...*/
		if ch == '/' && i+1 < len(span) && span[i+1] == '*' {
			closeOffset := strings.Index(span[i+2:], "*/")
			if closeOffset < 0 {
				return fmt.Errorf("statement %s: unclosed block comment", p.spec.Name)
			}
			end := i + 2 + closeOffset + 2
			seg = append(seg, span[i:end]...)
			i = end
			continue
		}

		// 7. Bracket tracking [ ... ]
		if ch == '[' {
			p.bracketDepth++
			seg = append(seg, ch)
			i++
			continue
		}
		if ch == ']' {
			if p.bracketDepth <= 0 {
				return fmt.Errorf("statement %s: unmatched closing bracket ']'", p.spec.Name)
			}
			p.bracketDepth--
			seg = append(seg, ch)
			i++
			continue
		}

		if p.bracketDepth > 0 {
			seg = append(seg, ch)
			i++
			continue
		}

		// Parenthesis depth tracking
		if ch == '(' {
			p.parenDepth++
			seg = append(seg, ch)
			i++
			continue
		}
		if ch == ')' {
			if p.parenDepth <= 0 {
				return fmt.Errorf("statement %s: unmatched closing parenthesis ')'", p.spec.Name)
			}
			p.parenDepth--
			if isTopLevel && p.inSingleInsertValues && p.parenDepth == 0 {
				p.inSingleInsertValues = false
				emitSlot(slotRef{Kind: runtime.SlotInsertValuesClose, ParamIdx: -1})
				i++
				continue
			}
			seg = append(seg, ch)
			i++
			continue
		}

		// Top-level semicolon
		if ch == ';' && isTopLevel && p.parenDepth == 0 && p.bracketDepth == 0 {
			flushBranchGuardBeforeEnd()
			seg = append(seg, ch)
			i++
			continue
		}

		// Track top-level comma separating unconditional projection items
		if ch == ',' && p.inProjection && p.parenDepth == 0 && p.bracketDepth == 0 && !p.inOptBlock {
			seg = append(seg, ch)
			i++
			p.projItemStartSeg = len(blk.Segments)
			p.projItemStartByte = len(seg)
			continue
		}

		// 8. Colon operators :: and :=
		if ch == ':' && i+1 < len(span) && (span[i+1] == ':' || span[i+1] == '=') {
			seg = append(seg, span[i:i+2]...)
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
					// yaml_ref_param (:ident or :ident... [ASC|DESC])
					canonKey := strings.ToLower(toGoParamName(ident))
					yp, ok := p.yamlParams[canonKey]
					if !ok {
						if existingIdx, exists := p.paramsByCanon[canonKey]; exists {
							ep := p.cs.Params[existingIdx]
							sk := runtime.SlotScalar
							if ep.Kind == paramSlice {
								sk = runtime.SlotSlice
							}
							hasSort, desc, nextAfterSort := scanOptionalSortModifier(span, afterIdent)
							if hasSort && sk == runtime.SlotSlice {
								if _, err := p.addOrGetParam(ident, ep.Kind, ep.GoType, ep.ElemGoType, nil, hasSort, desc); err != nil {
									return err
								}
								afterIdent = nextAfterSort
							}
							emitSlot(slotRef{Kind: sk, ParamIdx: existingIdx})
							i = afterIdent
							continue
						}
						if p.inProjection && p.parenDepth == 0 && p.bracketDepth == 0 && i > 0 && span[i-1] == ')' {
							// "COUNT(*):int64": a type annotation can only follow a plain column name or alias.
							expr := strings.TrimSpace(string(seg))
							if !p.inOptBlock && p.projActive {
								expr = extractCurrentItemExpr(blk.Segments, string(seg), p.projItemStartSeg, p.projItemStartByte)
							}
							typ := ident
							if gt, _, _, okT := scanGoType(span, identStart); okT {
								typ = gt
							}
							return fmt.Errorf("statement %s: output column expression %q needs an alias: write %s AS name:%s", p.spec.Name, expr, expr, typ)
						}
						return fmt.Errorf("statement %s: parameter %q referenced as :%s is not declared in params; declare its type inline (:%s:int64) or list it under params:", p.spec.Name, ident, ident, ident)
					}
					isSlice, fullType, elemType, err := normalizeScalarOrSliceType(yp.GoType, yp.Slice || hasPreEllipsis)
					if err != nil {
						return fmt.Errorf("statement %s param %q: %w", p.spec.Name, ident, err)
					}
					pk := paramScalar
					sk := runtime.SlotScalar
					if isSlice {
						pk = paramSlice
						sk = runtime.SlotSlice
					}
					hasSort := yp.HasSort
					desc := yp.Desc
					if isSlice {
						if hs, d, nextAfterSort := scanOptionalSortModifier(span, afterIdent); hs {
							hasSort = true
							desc = d
							afterIdent = nextAfterSort
						}
					}
					pIdx, err := p.addOrGetParam(ident, pk, fullType, elemType, nil, hasSort, desc)
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
					if p.cs.StmtVerb == runtime.VerbOther {
						switch low {
						case "select":
							p.cs.StmtVerb = runtime.VerbSelect
						case "insert":
							p.cs.StmtVerb = runtime.VerbInsert
						case "update":
							p.cs.StmtVerb = runtime.VerbUpdate
						case "delete":
							p.cs.StmtVerb = runtime.VerbDelete
						}
					}

					if low == "select" && p.cs.StmtVerb == runtime.VerbSelect && !p.seenMainSel {
						p.inProjection = true
						p.seenMainSel = true
						seg = append(seg, span[tokenStart:identEnd]...)
						i = identEnd
						for i < len(span) && isSpaceByte(span[i]) {
							seg = append(seg, span[i])
							i++
						}
						if i+8 <= len(span) && strings.EqualFold(span[i:i+8], "distinct") && (i+8 == len(span) || !isIdentContByte(span[i+8])) {
							seg = append(seg, span[i:i+8]...)
							i += 8
							for i < len(span) && isSpaceByte(span[i]) {
								seg = append(seg, span[i])
								i++
							}
							// PostgreSQL "DISTINCT ON (expr, ...)" is part of the projection prefix: without this
							// the first output column's expression would become "ON (a) a" and a SelectCols mask
							// that omits it would silently drop the DISTINCT ON.
							if i+2 <= len(span) && strings.EqualFold(span[i:i+2], "on") && (i+2 == len(span) || !isIdentContByte(span[i+2])) {
								j := i + 2
								for j < len(span) && isSpaceByte(span[j]) {
									j++
								}
								if j < len(span) && span[j] == '(' {
									if closeParen, err := findMatchingParen(span, j); err == nil {
										seg = append(seg, span[i:closeParen+1]...)
										i = closeParen + 1
										for i < len(span) && isSpaceByte(span[i]) {
											seg = append(seg, span[i])
											i++
										}
									}
								}
							}
						}
						if isTopLevel {
							p.projActive = true
							p.projStartSegIdx = len(blk.Segments)
							p.projStartByteIdx = len(seg)
							p.projItemStartSeg = p.projStartSegIdx
							p.projItemStartByte = p.projStartByteIdx
						}
						continue
					} else if low == "returning" {
						if isTopLevel && !p.inOptBlock {
							flushBranchGuardBeforeEnd()
						}
						p.inProjection = true
						p.cs.HasReturning = true
						seg = append(seg, span[tokenStart:identEnd]...)
						i = identEnd
						for i < len(span) && isSpaceByte(span[i]) {
							seg = append(seg, span[i])
							i++
						}
						if isTopLevel && len(p.cs.UncondCols) == 0 {
							p.projActive = true
							p.projStartSegIdx = len(blk.Segments)
							p.projStartByteIdx = len(seg)
							p.projItemStartSeg = p.projStartSegIdx
							p.projItemStartByte = p.projStartByteIdx
						}
						continue
					} else if isTopLevel && !p.inOptBlock && low == "where" && (p.cs.StmtVerb == runtime.VerbUpdate || p.cs.StmtVerb == runtime.VerbDelete) && !p.hasTopWhere {
						p.inProjection = false
						p.hasTopWhere = true
						p.needBranchGuardClose = true
						seg = append(seg, span[tokenStart:identEnd]...)
						i = identEnd
						for i < len(span) && isSpaceByte(span[i]) {
							seg = append(seg, span[i])
							i++
						}
						emitSlot(slotRef{Kind: runtime.SlotBranchGuardOpen, ParamIdx: -1})
						continue
					} else if isTopLevel && !p.inOptBlock && low == "values" && p.cs.StmtVerb == runtime.VerbInsert {
						p.inProjection = false
						j := identEnd
						for j < len(span) && isSpaceByte(span[j]) {
							j++
						}
						if j < len(span) && span[j] == '(' {
							if closeParen, err := findMatchingParen(span, j); err == nil {
								k := closeParen + 1
								for k < len(span) && isSpaceByte(span[k]) {
									k++
								}
								if k >= len(span) || span[k] != ',' {
									emitSlot(slotRef{Kind: runtime.SlotInsertValuesOpen, ParamIdx: -1})
									p.parenDepth = 1
									p.inSingleInsertValues = true
									i = j + 1
									continue
								}
							}
						} else if j < len(span) && (span[j] == ':' || isIdentStartByte(span[j])) {
							// Check if VALUES is followed by a tuple_slice_param: [":"] ident ":" "[]" go_ident "(" ... ")"
							tStart := j
							if span[tStart] == ':' {
								tStart++
							}
							tEnd := tStart
							for tEnd < len(span) && isIdentContByte(span[tEnd]) {
								tEnd++
							}
							if tEnd > tStart && strings.HasPrefix(span[tEnd:], ":[]") {
								stStart := tEnd + 3
								stEnd := stStart
								for stEnd < len(span) && isIdentContByte(span[stEnd]) {
									stEnd++
								}
								if stEnd > stStart && stEnd < len(span) && span[stEnd] == '(' {
									if closeParen, err := findMatchingParen(span, stEnd); err == nil {
										tIdent := span[tStart:tEnd]
										structName := span[stStart:stEnd]
										tupleBody := span[stEnd+1 : closeParen]
										td, err := parseTupleBody(structName, tupleBody)
										if err != nil {
											return fmt.Errorf("statement %s: %w", p.spec.Name, err)
										}
										pIdx, err := p.addOrGetParam(tIdent, paramTupleSlice, "[]"+structName, structName, td, td.HasSort, false)
										if err != nil {
											return err
										}
										emitSlot(slotRef{Kind: runtime.SlotInsertTupleValuesOpen, ParamIdx: -1})
										emitSlot(slotRef{Kind: runtime.SlotTupleSlice, ParamIdx: pIdx, Tuple: td})
										emitSlot(slotRef{Kind: runtime.SlotInsertTupleValuesClose, ParamIdx: -1})
										i = closeParen + 1
										continue
									}
								}
							}
						}
					} else if p.inProjection && projectionTerminators[low] &&
						!(low == "from" && p.prevWordLow == "distinct") && // IS [NOT] DISTINCT FROM
						!(tokenStart > 0 && span[tokenStart-1] == '.') { // t.limit, t.offset, ...
						p.inProjection = false
					}
				}

				p.prevWordLow = strings.ToLower(ident)
				seg = append(seg, span[tokenStart:identEnd]...)
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
						pIdx, err := p.addOrGetParam(ident, paramTupleSlice, "[]"+structName, structName, td, td.HasSort, false)
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
					if len(parts) > runtime.MaxOrderCols {
						return fmt.Errorf("statement %s: sqlgen.Order(...) supports at most %d columns (got %d)", p.spec.Name, runtime.MaxOrderCols, len(parts))
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
					pIdx, err := p.addOrGetParam(ident, paramOrder, "runtime.Order", "", nil, false, false)
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
				pIdx, err := p.addOrGetParam(ident, paramSeek, "runtime.Seek", "", nil, false, false)
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
				pIdx, err := p.addOrGetParam(ident, paramSubquery, "runtime.Query", "", nil, false, false)
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

			// Check if this is a 1D slice parameter or has a sort modifier
			isSliceCandidate := hasPreEllipsis || hasPostEllipsis || (strings.HasPrefix(goType, "[]") && goType != "[]byte" && goType != "[]uint8")
			hasSort := false
			desc := false
			if isSliceCandidate {
				if hs, d, afterSort := scanOptionalSortModifier(span, nextPos); hs {
					hasSort = true
					desc = d
					nextPos = afterSort
				}
			}

			// 5. Disambiguate Output Column vs Input Parameter (Section 4.3)
			isOutputCol := !hasLeadingColon &&
				!hasPreEllipsis &&
				!hasPostEllipsis &&
				!hasSort &&
				p.inProjection &&
				p.parenDepth == 0 &&
				p.bracketDepth == 0 &&
				isEndOfProjectionItem(span, nextPos)

			if isOutputCol {
				seg = append(seg, ident...)
				var colExpr string
				if !p.inOptBlock && p.projActive {
					colExpr = extractCurrentItemExpr(blk.Segments, string(seg), p.projItemStartSeg, p.projItemStartByte)
					p.projEndSegIdx = len(blk.Segments)
					p.projEndByteIdx = len(seg)
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
			pIdx, err := p.addOrGetParam(ident, pk, fullType, elemType, nil, hasSort, desc)
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

		seg = append(seg, ch)
		i++
	}

	if isTopLevel {
		if p.parenDepth != 0 {
			return fmt.Errorf("statement %s: unbalanced parentheses in SQL", p.spec.Name)
		}
		if p.bracketDepth != 0 {
			return fmt.Errorf("statement %s: unbalanced brackets in SQL", p.spec.Name)
		}
		flushBranchGuardBeforeEnd()
	}

	blk.Segments = append(blk.Segments, string(seg))
	return nil
}

func compileStatement(id uint32, spec StatementSpec) (*compiledStmt, error) {
	if !isValidGoIdent(spec.Name) {
		return nil, fmt.Errorf("sqlgen: invalid statement name %q", spec.Name)
	}
	if reservedStatementNames[spec.Name] {
		return nil, fmt.Errorf("statement name %q collides with a method generated on *Queries / *Pipeline; rename it", spec.Name)
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
		if reservedTypeNames[spec.ResultType] {
			return nil, fmt.Errorf("statement %s: result_type %q collides with a type or function declared in every generated file; rename it", spec.Name, spec.ResultType)
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
		canon := strings.ToLower(toGoParamName(ps.Name))
		if _, exists := yamlParams[canon]; exists {
			return nil, fmt.Errorf("statement %s: duplicate YAML param %q", spec.Name, ps.Name)
		}
		yamlParams[canon] = ps
	}

	cs := &compiledStmt{
		ID:            id,
		Name:          spec.Name,
		Kind:          kind,
		BatchParamIdx: -1,
	}

	parser := &stmtParser{
		spec:          spec,
		yamlParams:    yamlParams,
		paramsByCanon: make(map[string]int),
		flagsByName:   make(map[string]uint8),
		cs:            cs,
	}

	if err := parser.scanSpan(trimmedSQL, &cs.Root, true); err != nil {
		return nil, err
	}

	if len(cs.Cols) == 0 && len(spec.Columns) > 0 {
		for _, cSpec := range spec.Columns {
			if err := parser.addColumn(cSpec.Name, cSpec.GoType, cSpec.Name); err != nil {
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

	cs.UseParamsStruct = spec.ParamsStruct || len(cs.Flags) > 0

	// Fields of the generated <Name>Params struct share one namespace: parameters, optional-block flags
	// and the synthetic SelectCols mask. A collision would otherwise surface as a Go compile error in
	// the generated file.
	if cs.UseParamsStruct {
		owners := map[string]string{"SelectCols": "the generated SelectCols projection mask"}
		for _, prm := range cs.Params {
			if prev, dup := owners[prm.FieldName]; dup {
				return nil, fmt.Errorf("statement %s: parameter %q maps to Go field %q, which collides with %s; rename it", spec.Name, prm.RawName, prm.FieldName, prev)
			}
			owners[prm.FieldName] = fmt.Sprintf("parameter %q", prm.RawName)
		}
		for _, fl := range cs.Flags {
			if prev, dup := owners[fl.FieldName]; dup {
				return nil, fmt.Errorf("statement %s: conditional flag {?%s: ...} maps to Go field %q, which collides with %s; rename the flag or the parameter", spec.Name, fl.RawName, fl.FieldName, prev)
			}
			owners[fl.FieldName] = fmt.Sprintf("flag %q", fl.RawName)
		}
	}

	// Section 4.4: Unconditional SlotSelectCols splicing whenever len(UncondCols) > 1 && len(spec.Columns) == 0
	if len(cs.UncondCols) > 1 && len(spec.Columns) == 0 && parser.projActive && !parser.projHasParamSlot &&
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

	// Section 4.6.3: Exact CanBatch and CanStream predicates
	collectionOccs := cs.TotalSlices + cs.TotalTuples
	if (kind == KindExec || kind == KindMany) &&
		collectionOccs == 1 &&
		cs.TotalSubs == 0 &&
		!cs.HasOrder &&
		!cs.HasSeek {
		for _, sl := range cs.Root.Slots {
			if sl.Kind == runtime.SlotTupleSlice {
				cs.CanBatch = true
				cs.CanStream = true
				cs.BatchParamIdx = sl.ParamIdx
				cs.BatchIsTuple = true
				cs.BatchArity = len(sl.Tuple.Fields)
				cs.ScalarParamCnt = len(cs.Params) - 1
				cs.MaxRowsPerStmt = max(1, (32766-cs.ScalarParamCnt)/max(1, cs.BatchArity))
				break
			}
			if sl.Kind == runtime.SlotSlice {
				cs.CanBatch = true
				cs.CanStream = false
				cs.BatchParamIdx = sl.ParamIdx
				cs.BatchIsTuple = false
				cs.BatchArity = 1
				cs.ScalarParamCnt = len(cs.Params) - 1
				cs.MaxRowsPerStmt = max(1, 32766-cs.ScalarParamCnt)
				break
			}
		}
	}

	cs.IsStatic = cs.TotalSlices == 0 &&
		cs.TotalTuples == 0 &&
		cs.TotalSubs == 0 &&
		len(cs.Flags) == 0 &&
		!cs.UseParamsStruct &&
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

// importPackageNames returns the package names an import path can plausibly declare: every path segment,
// without a ".vN" suffix and without a "go-" prefix ("github.com/x/go-uuid", "gopkg.in/x/uuid.v1",
// "github.com/gofrs/uuid/v5" all provide "uuid").
func importPackageNames(path string) []string {
	var names []string
	for _, seg := range strings.Split(path, "/") {
		if i := strings.LastIndex(seg, ".v"); i > 0 {
			seg = seg[:i]
		}
		names = append(names, seg, strings.TrimPrefix(seg, "go-"))
	}
	return names
}

// qualifiedPackages lists the package qualifiers ("uuid" in "[]uuid.UUID") a Go type expression refers to.
func qualifiedPackages(t string) []string {
	var out []string
	for i := 0; i < len(t); i++ {
		if !isIdentStartByte(t[i]) || (i > 0 && isIdentContByte(t[i-1])) {
			continue
		}
		j := i + 1
		for j < len(t) && isIdentContByte(t[j]) {
			j++
		}
		if j+1 < len(t) && t[j] == '.' && isIdentStartByte(t[j+1]) {
			out = append(out, t[i:j])
		}
		i = j - 1
	}
	return out
}

func resolveImports(cfg *Config, stmts []*compiledStmt, runtimePkg string) ([]string, error) {
	impSet := map[string]bool{
		"context":      true,
		"database/sql": true,
		runtimePkg:     true,
	}
	hasStatic := false
	hasSort := false
	hasByteSliceSort := false
	hasStream := false
	hasIter := false

	type usedType struct{ stmt, goType string }
	var used []usedType

	for _, s := range stmts {
		if s.IsStatic && s.Kind != KindSubquery {
			hasStatic = true
		}
		if s.Kind == KindMany || s.CanStream {
			hasIter = true
		}
		if s.CanStream {
			hasStream = true
		}
		checkType := func(t string) {
			used = append(used, usedType{s.Name, t})
			for prefix, pkg := range autoImportPrefixes {
				search := t
				for {
					idx := strings.Index(search, prefix)
					if idx < 0 {
						break
					}
					if idx == 0 || !isIdentContByte(search[idx-1]) {
						impSet[pkg] = true
						break
					}
					search = search[idx+len(prefix):]
				}
			}
		}
		for _, p := range s.Params {
			checkType(p.GoType)
			if p.HasSort {
				hasSort = true
				if p.ElemGoType == "[]byte" || p.ElemGoType == "[]uint8" {
					hasByteSliceSort = true
				}
			}
			if p.Tuple != nil {
				if p.Tuple.HasSort {
					hasSort = true
				}
				for _, f := range p.Tuple.Fields {
					checkType(f.GoType)
					if f.HasSort && (f.GoType == "[]byte" || f.GoType == "[]uint8") {
						hasByteSliceSort = true
					}
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
	if hasIter {
		impSet["iter"] = true
	}
	if hasStream {
		impSet["sync"] = true
	}
	if hasSort {
		impSet["slices"] = true
	}
	if hasByteSliceSort {
		impSet["bytes"] = true
	}
	for _, extra := range cfg.ExtraImports {
		trimmed := strings.TrimSpace(extra)
		if trimmed != "" {
			impSet[trimmed] = true
		}
	}
	// A qualified type whose package is not imported would only fail later, in the generated file.
	known := map[string]bool{"sql": true}
	for path := range impSet {
		for _, n := range importPackageNames(path) {
			known[n] = true
		}
	}
	for _, u := range used {
		for _, pkg := range qualifiedPackages(u.goType) {
			if !known[pkg] {
				return nil, fmt.Errorf("statement %s: type %q uses package %q which is not imported; add its import path to extra_imports", u.stmt, u.goType, pkg)
			}
		}
	}
	out := make([]string, 0, len(impSet))
	for k := range impSet {
		out = append(out, k)
	}
	slices.Sort(out)
	return out, nil
}

// pgCastForGoType maps a scalar Go parameter type to the PostgreSQL type used to type its placeholder
// inside branch-gated INSERTs. It returns "" when no safe mapping exists (the placeholder is then left
// untyped and the user can add an explicit ::type in the SQL).
func pgCastForGoType(goType string) string {
	switch strings.TrimPrefix(strings.TrimSpace(goType), "*") {
	case "int", "int64", "uint32":
		return "bigint"
	case "int32", "rune", "uint16":
		return "integer"
	case "int16", "int8", "uint8", "byte":
		return "smallint"
	case "string":
		return "text"
	case "bool":
		return "boolean"
	case "float64":
		return "double precision"
	case "float32":
		return "real"
	case "[]byte", "[]uint8":
		return "bytea"
	case "time.Time":
		return "timestamptz"
	}
	return ""
}

func buildRuntimeDescriptor(cs *compiledStmt, blk *parsedBlock) *runtime.QueryDescriptor {
	d := &runtime.QueryDescriptor{
		Name:           cs.Name,
		StmtID:         cs.ID,
		StmtVerb:       cs.StmtVerb,
		HasReturning:   cs.HasReturning,
		SelectColExprs: cs.SelectColExprs,
		Segments:       blk.Segments,
	}
	for _, sl := range blk.Slots {
		d.Slots = append(d.Slots, runtime.Slot{Kind: sl.Kind})
	}
	return d
}

func sanitizeTypeIdent(t string) string {
	var buf []byte
	for i := 0; i < len(t); i++ {
		c := t[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			buf = append(buf, c)
		} else {
			buf = append(buf, '_')
		}
	}
	return string(buf)
}

func emitCompareExpr(b *bytes.Buffer, goType, lhs, rhs string, desc bool) {
	a, bExpr := lhs, rhs
	if desc {
		a, bExpr = rhs, lhs
	}
	switch goType {
	case "time.Time":
		fmt.Fprintf(b, "if c := %s.Compare(%s); c != 0 {\n\t\treturn c\n\t}\n", a, bExpr)
	case "[]byte", "[]uint8":
		fmt.Fprintf(b, "if c := bytes.Compare(%s, %s); c != 0 {\n\t\treturn c\n\t}\n", a, bExpr)
	default:
		fmt.Fprintf(b, "if %s < %s {\n\t\treturn -1\n\t}\n\tif %s > %s {\n\t\treturn 1\n\t}\n", a, bExpr, a, bExpr)
	}
}

// Generate validates cfg and generates formatted Go source code.
func Generate(cfg Config) ([]byte, error) {
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

	rowStructs := make(map[string][]colDef)
	rowOrders := make(map[string]rowOrder)
	var rowStructOrder []string
	tupleStructs := make(map[string]*tupleStructDef)
	var tupleStructOrder []string
	streamTupleStructs := make(map[string]bool)

	for i, spec := range cfg.Statements {
		if seenStmts[spec.Name] {
			return nil, fmt.Errorf("sqlgen: duplicate statement name %q", spec.Name)
		}
		seenStmts[spec.Name] = true

		cs, err := compileStatement(uint32(i+1), spec)
		if err != nil {
			return nil, err
		}
		cs.PGCasts = dialect == runtime.DialectDollar && cs.StmtVerb == runtime.VerbInsert
		if cs.IsStatic {
			rtDesc := buildRuntimeDescriptor(cs, &cs.Root)
			cs.StaticSQL = runtime.BuildComposedSQL(rtDesc, &runtime.RenderInput{}, dialect)
		}
		compiled = append(compiled, cs)

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
			// The <Row>OrderBy* constants, Order<Row>() and Row.Seek() are emitted once per result type
			// and index into the sqlgen.Order(...) column list, so every statement sharing the type
			// must declare the identical list or its sort/seek columns would silently be the wrong ones.
			if cs.HasOrder {
				cur := make([]string, len(cs.OrderCols))
				for i, oc := range cs.OrderCols {
					cur[i] = strings.ToLower(oc.RawRef)
				}
				if prev, seen := rowOrders[cs.RowTypeName]; seen {
					if !slices.Equal(prev.cols, cur) {
						return nil, fmt.Errorf("statements %s and %s share result_type %q but declare different sqlgen.Order column lists (%s vs %s); use a distinct result_type or the same list",
							prev.stmt, cs.Name, cs.RowTypeName, strings.Join(prev.cols, ", "), strings.Join(cur, ", "))
					}
				} else {
					rowOrders[cs.RowTypeName] = rowOrder{stmt: cs.Name, cols: cur}
				}
			}
		}

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
					if p.Tuple.HasSort {
						prevT.HasSort = true
						for fIdx := range p.Tuple.Fields {
							if p.Tuple.Fields[fIdx].HasSort {
								prevT.Fields[fIdx].HasSort = true
								prevT.Fields[fIdx].Desc = p.Tuple.Fields[fIdx].Desc
							}
						}
					}
				} else {
					tupleStructs[p.Tuple.Name] = p.Tuple
					tupleStructOrder = append(tupleStructOrder, p.Tuple.Name)
				}
				if cs.CanStream {
					streamTupleStructs[p.Tuple.Name] = true
				}
			}
		}
	}

	imports, err := resolveImports(&cfg, compiled, runtimeImport)
	if err != nil {
		return nil, err
	}
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

	b.WriteString("type (\n")
	b.WriteString("\tQuery         = runtime.Query\n")
	b.WriteString("\tOrder         = runtime.Order\n")
	b.WriteString("\tSeek          = runtime.Seek\n")
	b.WriteString("\tPipelineCond  = runtime.PipelineCond\n")
	b.WriteString("\tExecFuture    = runtime.ExecFuture\n")
	b.WriteString("\tIfResult      = runtime.IfResult\n")
	b.WriteString("\tTxOption      = runtime.TxOption\n")
	b.WriteString("\tTxRetryPolicy = runtime.TxRetryPolicy\n")
	b.WriteString(")\n\n")

	b.WriteString("var (\n")
	b.WriteString("\tNewSeek            = runtime.NewSeek\n")
	b.WriteString("\tWithTxRetry        = runtime.WithTxRetry\n")
	b.WriteString("\tDefaultRetryPolicy = runtime.DefaultRetryPolicy\n")
	b.WriteString(")\n\n")

	b.WriteString("func WhereCol[T any](f *runtime.RowFuture[T], sqlPred string, goPred func(T) bool) runtime.PipelineCond {\n")
	b.WriteString("\treturn runtime.WhereCol(f, sqlPred, goPred)\n")
	b.WriteString("}\n\n")

	dialectConst := "runtime.DialectDollar"
	if dialect == runtime.DialectQuestion {
		dialectConst = "runtime.DialectQuestion"
	}
	fmt.Fprintf(&b, "const sqlDialect = %s\n\n", dialectConst)

	numStatic := 0
	staticIdxByStmt := make(map[uint32]int)
	for _, cs := range compiled {
		if cs.IsStatic && cs.Kind != KindSubquery {
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

	b.WriteString("// CacheStats returns aggregated telemetry across all 16 statement cache shards.\n")
	b.WriteString("func (q *Queries) CacheStats() runtime.CacheStats { return q.cache.Stats() }\n\n")

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

	b.WriteString("// InTx executes fn inside a database transaction with optional retry policy.\n")
	b.WriteString("func (q *Queries) InTx(ctx context.Context, txOpts *sql.TxOptions, fn func(*Queries) error, opts ...runtime.TxOption) error {\n")
	b.WriteString("\treturn runtime.RunTxWithRetry(ctx, q.db, q.txCache, txOpts, opts, func(tc *runtime.TxStmtCache) error {\n")
	b.WriteString("\t\ttxQ := *q\n")
	b.WriteString("\t\ttxQ.txCache = tc\n")
	b.WriteString("\t\treturn fn(&txQ)\n")
	b.WriteString("\t})\n")
	b.WriteString("}\n\n")

	b.WriteString("// InTxOpts is an alias for InTx.\n")
	b.WriteString("func (q *Queries) InTxOpts(ctx context.Context, txOpts *sql.TxOptions, fn func(*Queries) error, opts ...runtime.TxOption) error {\n")
	b.WriteString("\treturn q.InTx(ctx, txOpts, fn, opts...)\n")
	b.WriteString("}\n\n")

	// Emit Pipeline wrapper type
	b.WriteString("// Pipeline queues strongly-typed SQL operations for single-roundtrip execution or sequential fallback.\n")
	b.WriteString("type Pipeline struct {\n")
	b.WriteString("\t*runtime.Pipeline\n")
	b.WriteString("\tq *Queries\n")
	b.WriteString("}\n\n")

	b.WriteString("// Pipeline creates a new Pipeline bound to ctx.\n")
	b.WriteString("func (q *Queries) Pipeline(ctx context.Context) *Pipeline {\n")
	b.WriteString("\treturn &Pipeline{\n")
	b.WriteString("\t\tPipeline: runtime.NewPipeline(ctx, q.db, q.cache, q.txCache, q.usePrepared, sqlDialect),\n")
	b.WriteString("\t\tq:        q,\n")
	b.WriteString("\t}\n")
	b.WriteString("}\n\n")

	b.WriteString("// PipelineTx executes a transactional pipeline batch with optional retry policy.\n")
	b.WriteString("func (q *Queries) PipelineTx(ctx context.Context, txOpts *sql.TxOptions, fn func(*Pipeline) error, opts ...runtime.TxOption) error {\n")
	b.WriteString("\treturn runtime.RunPipelineTx(ctx, q.db, q.cache, q.txCache, q.usePrepared, sqlDialect, txOpts, opts, func(p *runtime.Pipeline) error {\n")
	b.WriteString("\t\treturn fn(&Pipeline{Pipeline: p, q: q})\n")
	b.WriteString("\t})\n")
	b.WriteString("}\n\n")

	b.WriteString("// If queues conditional thenFn and optional elseFn branches gated on cond.\n")
	b.WriteString("func (p *Pipeline) If(cond runtime.PipelineCond, thenFn func(*Pipeline), elseFn func(*Pipeline)) *runtime.IfResult {\n")
	b.WriteString("\tvar tFn, eFn func()\n")
	b.WriteString("\tif thenFn != nil {\n")
	b.WriteString("\t\ttFn = func() { thenFn(p) }\n")
	b.WriteString("\t}\n")
	b.WriteString("\tif elseFn != nil {\n")
	b.WriteString("\t\teFn = func() { elseFn(p) }\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn p.Pipeline.If(cond, tFn, eFn)\n")
	b.WriteString("}\n\n")

	// Emit tuple struct definitions, chunk pools, and presort comparators
	for _, tName := range tupleStructOrder {
		td := tupleStructs[tName]
		fmt.Fprintf(&b, "type %s struct {\n", td.Name)
		for _, f := range td.Fields {
			fmt.Fprintf(&b, "\t%s %s\n", f.FieldName, f.GoType)
		}
		b.WriteString("}\n\n")

		if streamTupleStructs[tName] {
			poolName := lowerFirstIdent(td.Name) + "ChunkPool"
			fmt.Fprintf(&b, "var %s = sync.Pool{\n", poolName)
			fmt.Fprintf(&b, "\tNew: func() any {\n\t\ts := make([]%s, 0, 256)\n\t\treturn &s\n\t},\n", td.Name)
			b.WriteString("}\n\n")
		}

		if td.HasSort {
			fmt.Fprintf(&b, "func compare%s(a, b %s) int {\n", td.Name, td.Name)
			for _, f := range td.Fields {
				if !f.HasSort {
					continue
				}
				b.WriteString("\t")
				emitCompareExpr(&b, f.GoType, "a."+f.FieldName, "b."+f.FieldName, f.Desc)
			}
			b.WriteString("\treturn 0\n}\n\n")

			fmt.Fprintf(&b, "func is%sSorted(rows []%s) bool {\n", td.Name, td.Name)
			b.WriteString("\tfor i := 1; i < len(rows); i++ {\n")
			fmt.Fprintf(&b, "\t\tif compare%s(rows[i-1], rows[i]) > 0 {\n\t\t\treturn false\n\t\t}\n", td.Name)
			b.WriteString("\t}\n\treturn true\n}\n\n")
		}
	}

	// Emit 1D slice presort helpers for each unique (elemGoType, desc) pair
	emittedSliceSort := make(map[string]bool)
	for _, cs := range compiled {
		for _, p := range cs.Params {
			if p.Kind == paramSlice && p.HasSort {
				dirTag := "Asc"
				if p.Desc {
					dirTag = "Desc"
				}
				typeIdent := sanitizeTypeIdent(p.ElemGoType)
				key := typeIdent + "_" + dirTag
				if emittedSliceSort[key] {
					continue
				}
				emittedSliceSort[key] = true

				fmt.Fprintf(&b, "func compare%s%s(a, b %s) int {\n\t", typeIdent, dirTag, p.ElemGoType)
				emitCompareExpr(&b, p.ElemGoType, "a", "b", p.Desc)
				b.WriteString("\treturn 0\n}\n\n")

				fmt.Fprintf(&b, "func is%s%sSorted(s []%s) bool {\n", typeIdent, dirTag, p.ElemGoType)
				b.WriteString("\tfor i := 1; i < len(s); i++ {\n")
				fmt.Fprintf(&b, "\t\tif compare%s%s(s[i-1], s[i]) > 0 {\n\t\t\treturn false\n\t\t}\n", typeIdent, dirTag)
				b.WriteString("\t}\n\treturn true\n}\n\n")
			}
		}
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

	emittedRowOrder := make(map[string]bool)
	emittedRowCols := make(map[string]bool)
	emittedRowSeek := make(map[string]bool)

	for _, cs := range compiled {
		emitStatementCode(&b, cs, staticIdxByStmt, emittedRowOrder, emittedRowCols, emittedRowSeek)
	}

	formatted, err := format.Source(b.Bytes())
	if err != nil {
		return nil, fmt.Errorf("sqlgen: gofmt failed: %w\n--- raw source ---\n%s", err, b.String())
	}
	return formatted, nil
}

func emitBlockLiteral(b *bytes.Buffer, cs *compiledStmt, blk *parsedBlock, isRoot bool) {
	b.WriteString("&runtime.QueryDescriptor{\n")
	if isRoot {
		fmt.Fprintf(b, "\tName: %s,\n", strconv.Quote(cs.Name))
		fmt.Fprintf(b, "\tStmtID: %d,\n", cs.ID)
		switch cs.StmtVerb {
		case runtime.VerbSelect:
			b.WriteString("\tStmtVerb: runtime.VerbSelect,\n")
		case runtime.VerbInsert:
			b.WriteString("\tStmtVerb: runtime.VerbInsert,\n")
		case runtime.VerbUpdate:
			b.WriteString("\tStmtVerb: runtime.VerbUpdate,\n")
		case runtime.VerbDelete:
			b.WriteString("\tStmtVerb: runtime.VerbDelete,\n")
		}
		if cs.HasReturning {
			b.WriteString("\tHasReturning: true,\n")
		}
		if cs.StaticSQL != "" {
			fmt.Fprintf(b, "\tStaticSQL: %s,\n", strconv.Quote(cs.StaticSQL))
		}
	}
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
				if cs.PGCasts && sl.ParamIdx >= 0 && sl.ParamIdx < len(cs.Params) {
					if c := pgCastForGoType(cs.Params[sl.ParamIdx].GoType); c != "" {
						fmt.Fprintf(b, ", Cast: %s", strconv.Quote(c))
					}
				}
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
				fmt.Fprintf(b, "}, NumFields: %d", len(sl.Tuple.Fields))
				if cs.PGCasts {
					var casts []string
					hasCast := false
					for _, f := range sl.Tuple.Fields {
						c := pgCastForGoType(f.GoType)
						hasCast = hasCast || c != ""
						casts = append(casts, strconv.Quote(c))
					}
					if hasCast {
						b.WriteString(", Casts: []string{" + strings.Join(casts, ", ") + "}")
					}
				}
				b.WriteString("}")
			case runtime.SlotOptBlock:
				fmt.Fprintf(b, "runtime.SlotOptBlock, FlagBit: %d, Inner: ", sl.FlagBit)
				emitBlockLiteral(b, cs, sl.Inner, false)
			case runtime.SlotQuery:
				b.WriteString("runtime.SlotQuery")
			case runtime.SlotSelectCols:
				b.WriteString("runtime.SlotSelectCols")
			case runtime.SlotOrder:
				b.WriteString("runtime.SlotOrder")
			case runtime.SlotSeek:
				b.WriteString("runtime.SlotSeek")
			case runtime.SlotBranchGuardOpen:
				b.WriteString("runtime.SlotBranchGuardOpen")
			case runtime.SlotBranchGuardClose:
				b.WriteString("runtime.SlotBranchGuardClose")
			case runtime.SlotBranchGuardWhere:
				b.WriteString("runtime.SlotBranchGuardWhere")
			case runtime.SlotInsertValuesOpen:
				b.WriteString("runtime.SlotInsertValuesOpen")
			case runtime.SlotInsertValuesClose:
				b.WriteString("runtime.SlotInsertValuesClose")
			case runtime.SlotInsertTupleValuesOpen:
				b.WriteString("runtime.SlotInsertTupleValuesOpen")
			case runtime.SlotInsertTupleValuesClose:
				b.WriteString("runtime.SlotInsertTupleValuesClose")
			}
			b.WriteString("},\n")
		}
		b.WriteString("\t},\n")
	}
	if isRoot && len(cs.SelectColExprs) > 0 {
		b.WriteString("\tSelectColExprs: []string{")
		for i, expr := range cs.SelectColExprs {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Quote(expr))
		}
		b.WriteString("},\n")
	}
	if isRoot && len(cs.OrderCols) > 0 {
		b.WriteString("\tOrderCols: []string{")
		for i, oc := range cs.OrderCols {
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
			if p.HasSort {
				dirTag := "Asc"
				if p.Desc {
					dirTag = "Desc"
				}
				typeIdent := sanitizeTypeIdent(p.ElemGoType)
				fmt.Fprintf(b, "\tif is%s%sSorted(%s) {\n", typeIdent, dirTag, acc)
				fmt.Fprintf(b, "\t\tfor _, v := range %s {\n\t\t\t*dst = append(*dst, v)\n\t\t}\n", acc)
				b.WriteString("\t} else {\n")
				fmt.Fprintf(b, "\t\tidxBuf := runtime.AcquireIndices(len(%s))\n", acc)
				b.WriteString("\t\tidx := *idxBuf\n")
				fmt.Fprintf(b, "\t\tslices.SortFunc(idx, func(i, j int) int { return compare%s%s(%s[i], %s[j]) })\n", typeIdent, dirTag, acc, acc)
				fmt.Fprintf(b, "\t\tfor _, k := range idx {\n\t\t\t*dst = append(*dst, %s[k])\n\t\t}\n", acc)
				b.WriteString("\t\truntime.ReleaseIndices(idxBuf)\n")
				b.WriteString("\t}\n")
			} else {
				fmt.Fprintf(b, "\tfor _, v := range %s {\n\t\t*dst = append(*dst, v)\n\t}\n", acc)
			}
		case runtime.SlotTupleSlice:
			p := cs.Params[sl.ParamIdx]
			acc := paramAccess(p)
			fmt.Fprintf(b, "\tif len(%s) == 0 {\n\t\treturn runtime.ErrEmptySliceParam\n\t}\n", acc)
			if sl.Tuple.HasSort {
				fmt.Fprintf(b, "\tif is%sSorted(%s) {\n", sl.Tuple.Name, acc)
				fmt.Fprintf(b, "\t\tfor i := range %s {\n", acc)
				for _, f := range sl.Tuple.Fields {
					fmt.Fprintf(b, "\t\t\t*dst = append(*dst, %s[i].%s)\n", acc, f.FieldName)
				}
				b.WriteString("\t\t}\n\t} else {\n")
				fmt.Fprintf(b, "\t\tidxBuf := runtime.AcquireIndices(len(%s))\n", acc)
				b.WriteString("\t\tidx := *idxBuf\n")
				fmt.Fprintf(b, "\t\tslices.SortFunc(idx, func(i, j int) int { return compare%s(%s[i], %s[j]) })\n", sl.Tuple.Name, acc, acc)
				b.WriteString("\t\tfor _, k := range idx {\n")
				for _, f := range sl.Tuple.Fields {
					fmt.Fprintf(b, "\t\t\t*dst = append(*dst, %s[k].%s)\n", acc, f.FieldName)
				}
				b.WriteString("\t\t}\n\t\truntime.ReleaseIndices(idxBuf)\n\t}\n")
			} else {
				fmt.Fprintf(b, "\tfor i := range %s {\n", acc)
				for _, f := range sl.Tuple.Fields {
					fmt.Fprintf(b, "\t\t*dst = append(*dst, %s[i].%s)\n", acc, f.FieldName)
				}
				b.WriteString("\t}\n")
			}
		case runtime.SlotQuery:
			p := cs.Params[sl.ParamIdx]
			acc := paramAccess(p)
			fmt.Fprintf(b, "\tif err := %s.AppendArgs(dst, dialect); err != nil {\n\t\treturn err\n\t}\n", acc)
		case runtime.SlotSeek:
			p := cs.Params[sl.ParamIdx]
			acc := paramAccess(p)
			orderExpr := "uint64(0)"
			for _, op := range cs.Params {
				if op.Kind == paramOrder {
					orderExpr = paramAccess(op)
					break
				}
			}
			fmt.Fprintf(b, "\tif %s.IsActive() {\n", acc)
			fmt.Fprintf(b, "\t\tif len(%s.Values) != %d {\n\t\t\treturn runtime.ErrInvalidSeek\n\t\t}\n", acc, len(cs.OrderCols))
			fmt.Fprintf(b, "\t\truntime.AppendSeekArgs(dst, dialect, %s, %d, %s.Values)\n", orderExpr, len(cs.OrderCols), acc)
			b.WriteString("\t}\n")
		case runtime.SlotOptBlock:
			flag := cs.Flags[sl.FlagBit]
			fmt.Fprintf(b, "\tif p.%s {\n", flag.FieldName)
			emitSlotArgExpansion(b, cs, sl.Inner, paramAccess)
			b.WriteString("\t}\n")
		}
	}
}

func emitSortHelperGroup(b *bytes.Buffer, typeTag string, orderCols []orderColRef) {
	fmt.Fprintf(b, "type %sSortCol uint8\n\n", typeTag)
	fmt.Fprintf(b, "func (c %sSortCol) Asc() uint8  { return runtime.Asc(uint8(c)) }\n", typeTag)
	fmt.Fprintf(b, "func (c %sSortCol) Desc() uint8 { return runtime.Desc(uint8(c)) }\n\n", typeTag)

	b.WriteString("const (\n")
	for idx, oc := range orderCols {
		ascVal := runtime.Asc(uint8(idx))
		descVal := runtime.Desc(uint8(idx))
		fmt.Fprintf(b, "\t%sOrderBy%s %sSortCol = %d\n", typeTag, oc.FieldName, typeTag, idx)
		fmt.Fprintf(b, "\tOrder%s%sAsc uint8 = %d\n", typeTag, oc.FieldName, ascVal)
		fmt.Fprintf(b, "\tOrder%s%sDesc uint8 = %d\n", typeTag, oc.FieldName, descVal)
		fmt.Fprintf(b, "\t%sOrder%sAsc uint8 = %d\n", typeTag, oc.FieldName, ascVal)
		fmt.Fprintf(b, "\t%sOrder%sDesc uint8 = %d\n", typeTag, oc.FieldName, descVal)
	}
	b.WriteString(")\n\n")

	fmt.Fprintf(b, "func Order%s(terms ...uint8) runtime.Order {\n", typeTag)
	b.WriteString("\treturn runtime.PackOrder(terms...)\n")
	b.WriteString("}\n\n")
}

func emitColMaskConstants(b *bytes.Buffer, prefix string, uncondCols []colDef) {
	b.WriteString("const (\n")
	for i, uc := range uncondCols {
		fmt.Fprintf(b, "\t%sCol%s uint64 = 1 << %d\n", prefix, uc.FieldName, i)
	}
	fmt.Fprintf(b, "\t%sColsAll uint64 = (1 << %d) - 1\n", prefix, len(uncondCols))
	b.WriteString(")\n\n")
}

func emitStatementCode(
	b *bytes.Buffer,
	cs *compiledStmt,
	staticIdxByStmt map[uint32]int,
	emittedRowOrder map[string]bool,
	emittedRowCols map[string]bool,
	emittedRowSeek map[string]bool,
) {
	fmt.Fprintf(b, "const stmtID%s uint32 = %d\n\n", cs.Name, cs.ID)

	// Emit SelectCols bitmask constants on both <RowType> and <StmtName>
	if len(cs.UncondCols) > 0 {
		if !cs.ScalarResult && cs.RowTypeName != "" && !emittedRowCols[cs.RowTypeName] {
			emittedRowCols[cs.RowTypeName] = true
			emitColMaskConstants(b, cs.RowTypeName, cs.UncondCols)
		}
		if cs.RowTypeName != cs.Name && !emittedRowCols[cs.Name] {
			emittedRowCols[cs.Name] = true
			emitColMaskConstants(b, cs.Name, cs.UncondCols)
		}
	}

	// Emit Order and Seek helpers
	if cs.HasOrder {
		primaryTag := cs.RowTypeName
		if primaryTag == "" || cs.ScalarResult {
			primaryTag = cs.Name
		}
		if !emittedRowOrder[primaryTag] {
			emittedRowOrder[primaryTag] = true
			emitSortHelperGroup(b, primaryTag, cs.OrderCols)
		}
		if primaryTag != cs.Name && !emittedRowOrder[cs.Name] {
			emittedRowOrder[cs.Name] = true
			emitSortHelperGroup(b, cs.Name, cs.OrderCols)
		}

		if cs.HasSeek && !cs.ScalarResult && cs.RowTypeName != "" {
			if !emittedRowSeek[cs.RowTypeName] {
				emittedRowSeek[cs.RowTypeName] = true
				fmt.Fprintf(b, "func (r %s) Seek() runtime.Seek {\n\treturn runtime.NewSeek(", cs.RowTypeName)
				for idx, oc := range cs.OrderCols {
					if idx > 0 {
						b.WriteString(", ")
					}
					fmt.Fprintf(b, "r.%s", oc.FieldName)
				}
				b.WriteString(")\n}\n\n")

				fmt.Fprintf(b, "func Seek%s(row %s) runtime.Seek {\n\treturn row.Seek()\n}\n\n", cs.RowTypeName, cs.RowTypeName)
			}
			if cs.RowTypeName != cs.Name && !emittedRowSeek[cs.Name] {
				emittedRowSeek[cs.Name] = true
				fmt.Fprintf(b, "func Seek%s(row %s) runtime.Seek {\n\treturn row.Seek()\n}\n\n", cs.Name, cs.RowTypeName)
			}
		}
	}

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

	fmt.Fprintf(b, "var desc%s = ", cs.Name)
	emitBlockLiteral(b, cs, &cs.Root, true)
	b.WriteString("\n\n")

	fmt.Fprintf(b, "func expand%sParams(dst *[]any, dialect runtime.Dialect, p %s) error {\n", cs.Name, paramsStructName)
	b.WriteString("\t_ = dialect\n\t_ = p\n")
	emitSlotArgExpansion(b, cs, &cs.Root, func(p paramDef) string {
		return "p." + p.FieldName
	})
	b.WriteString("\treturn nil\n")
	b.WriteString("}\n\n")

	if cs.Kind != KindExec {
		fmt.Fprintf(b, "func expand%sArgs(dst *[]any, dialect runtime.Dialect, q *runtime.Query) error {\n", cs.Name)
		if len(cs.Params) > 0 || len(cs.Flags) > 0 {
			fmt.Fprintf(b, "\treturn expand%sParams(dst, dialect, q.Params.(%s))\n", cs.Name, paramsStructName)
		} else {
			fmt.Fprintf(b, "\treturn expand%sParams(dst, dialect, %s{})\n", cs.Name, paramsStructName)
		}
		b.WriteString("}\n\n")
	}

	var sigParams []byte
	var callArgs []byte
	if cs.UseParamsStruct {
		sigParams = append(sigParams, ("p " + paramsStructName)...)
		callArgs = append(callArgs, 'p')
	} else {
		for i, p := range cs.Params {
			if i > 0 {
				sigParams = append(sigParams, ", "...)
				callArgs = append(callArgs, ", "...)
			}
			sigParams = append(sigParams, (p.ParamName + " " + p.GoType)...)
			callArgs = append(callArgs, p.ParamName...)
		}
	}
	sigStr := string(sigParams)
	callStr := string(callArgs)
	sigWithLeadingComma := ""
	callWithLeadingComma := ""
	if sigStr != "" {
		sigWithLeadingComma = ", " + sigStr
		callWithLeadingComma = ", " + callStr
	}

	// Zero-allocation compute<Name>Key helper
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
	canFailExpand := totalDims > 0 || cs.HasSeek
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

	// Helper to emit stack-allocated RenderInput construction (zero heap allocations on cache hits)
	emitStackRenderInput := func(indent, pVar string) {
		var orderField, seekField string
		for _, p := range cs.Params {
			if p.Kind == paramOrder {
				orderField = pVar + "." + p.FieldName
			}
			if p.Kind == paramSeek {
				seekField = pVar + "." + p.FieldName + ".IsActive()"
			}
		}
		if cs.TotalSlices > 0 {
			fmt.Fprintf(b, "%svar sliceLensBuf [%d]int\n", indent, cs.TotalSlices)
		}
		if cs.TotalTuples > 0 {
			fmt.Fprintf(b, "%svar tupleLensBuf [%d]int\n", indent, cs.TotalTuples)
		}
		if cs.TotalSubs > 0 {
			fmt.Fprintf(b, "%svar subsBuf [%d]runtime.Query\n", indent, cs.TotalSubs)
		}
		sc := 0
		tc := 0
		subc := 0
		var fillCoords func(blk *parsedBlock, cond string)
		fillCoords = func(blk *parsedBlock, cond string) {
			for _, sl := range blk.Slots {
				switch sl.Kind {
				case runtime.SlotSlice:
					p := cs.Params[sl.ParamIdx]
					idx := sc
					sc++
					if cond == "" {
						fmt.Fprintf(b, "%ssliceLensBuf[%d] = len(%s.%s)\n", indent, idx, pVar, p.FieldName)
					} else {
						fmt.Fprintf(b, "%sif %s {\n%s\tsliceLensBuf[%d] = len(%s.%s)\n%s}\n", indent, cond, indent, idx, pVar, p.FieldName, indent)
					}
				case runtime.SlotTupleSlice:
					p := cs.Params[sl.ParamIdx]
					idx := tc
					tc++
					if cond == "" {
						fmt.Fprintf(b, "%stupleLensBuf[%d] = len(%s.%s)\n", indent, idx, pVar, p.FieldName)
					} else {
						fmt.Fprintf(b, "%sif %s {\n%s\ttupleLensBuf[%d] = len(%s.%s)\n%s}\n", indent, cond, indent, idx, pVar, p.FieldName, indent)
					}
				case runtime.SlotQuery:
					p := cs.Params[sl.ParamIdx]
					idx := subc
					subc++
					if cond == "" {
						fmt.Fprintf(b, "%ssubsBuf[%d] = %s.%s\n", indent, idx, pVar, p.FieldName)
					} else {
						fmt.Fprintf(b, "%sif %s {\n%s\tsubsBuf[%d] = %s.%s\n%s}\n", indent, cond, indent, idx, pVar, p.FieldName, indent)
					}
				case runtime.SlotOptBlock:
					flag := cs.Flags[sl.FlagBit]
					fillCoords(sl.Inner, pVar+"."+flag.FieldName)
				}
			}
		}
		fillCoords(&cs.Root, "")

		fmt.Fprintf(b, "%sin := runtime.RenderInput{\n", indent)
		fmt.Fprintf(b, "%s\tColMask:  colMask,\n", indent)
		fmt.Fprintf(b, "%s\tFlagMask: flagMask,\n", indent)
		if orderField != "" {
			fmt.Fprintf(b, "%s\tOrderSpec: %s,\n", indent, orderField)
		}
		if seekField != "" {
			fmt.Fprintf(b, "%s\tHasSeek: %s,\n", indent, seekField)
		}
		if cs.TotalSlices > 0 {
			fmt.Fprintf(b, "%s\tSliceLens: sliceLensBuf[:],\n", indent)
		}
		if cs.TotalTuples > 0 {
			fmt.Fprintf(b, "%s\tTupleLens: tupleLensBuf[:],\n", indent)
		}
		if cs.TotalSubs > 0 {
			fmt.Fprintf(b, "%s\tSubs: subsBuf[:],\n", indent)
		}
		fmt.Fprintf(b, "%s}\n", indent)
	}

	// 1. Currying Constructor (<Name>Query) for all kinds except exec
	if cs.Kind != KindExec {
		fmt.Fprintf(b, "// %sQuery constructs a composable runtime.Query without executing it.\n", cs.Name)
		fmt.Fprintf(b, "func %sQuery(%s) runtime.Query {\n", cs.Name, sigStr)
		if !cs.UseParamsStruct {
			fmt.Fprintf(b, "\tp := %s{\n", paramsStructName)
			for _, p := range cs.Params {
				fmt.Fprintf(b, "\t\t%s: %s,\n", p.FieldName, p.ParamName)
			}
			b.WriteString("\t}\n")
		}
		fmt.Fprintf(b, "\t_, colMask, flagMask, _ := compute%sKey(p)\n", cs.Name)
		emitStackRenderInput("\t", "p")
		b.WriteString("\torderKey := runtime.EncodeOrderKey(in.OrderSpec, in.HasSeek)\n")

		hasSubsLiteral := "false"
		if cs.TotalSubs > 0 {
			hasSubsLiteral = "true"
		}
		if totalDims == 0 {
			fmt.Fprintf(b, "\tshapeTag := runtime.ComputeSubqueryShape(stmtID%s, colMask, flagMask, orderKey, %s)\n", cs.Name, hasSubsLiteral)
		} else {
			fmt.Fprintf(b, "\tvar shapeLens [%d]int\n", totalDims)
			if cs.TotalSlices > 0 {
				b.WriteString("\tcopy(shapeLens[:], in.SliceLens)\n")
			}
			if cs.TotalTuples > 0 {
				fmt.Fprintf(b, "\tcopy(shapeLens[%d:], in.TupleLens)\n", cs.TotalSlices)
			}
			if cs.TotalSubs > 0 {
				fmt.Fprintf(b, "\tfor i, sub := range in.Subs {\n\t\tshapeLens[%d+i] = int(sub.ShapeTag)\n\t}\n", cs.TotalSlices+cs.TotalTuples)
			}
			fmt.Fprintf(b, "\tshapeTag := runtime.ComputeSubqueryShape(stmtID%s, colMask, flagMask, orderKey, %s, shapeLens[:]...)\n", cs.Name, hasSubsLiteral)
		}

		fmt.Fprintf(b, "\treturn runtime.Query{\n")
		fmt.Fprintf(b, "\t\tStmtID:   stmtID%s,\n", cs.Name)
		fmt.Fprintf(b, "\t\tDesc:     desc%s,\n", cs.Name)
		b.WriteString("\t\tInput:    in,\n")
		b.WriteString("\t\tParams:   p,\n")
		fmt.Fprintf(b, "\t\tExpand:   expand%sArgs,\n", cs.Name)
		b.WriteString("\t\tShapeTag: shapeTag,\n")
		b.WriteString("\t}\n")
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
	pipelineFastSlotExpr := "nil"
	if slotIdx, ok := staticIdxByStmt[cs.ID]; ok {
		fastSlotExpr = fmt.Sprintf("&q.fastSlots[%d]", slotIdx)
		pipelineFastSlotExpr = fmt.Sprintf("&pl.q.fastSlots[%d]", slotIdx)
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
		b.WriteString("\targBuf := runtime.AcquireArgsCap(approxArgs)\n")
		b.WriteString("\tdefer runtime.ReleaseArgs(argBuf)\n")
		if !canFailExpand {
			fmt.Fprintf(b, "\t_ = expand%sParams(argBuf, sqlDialect, p)\n", cs.Name)
		} else {
			fmt.Fprintf(b, "\tif err := expand%sParams(argBuf, sqlDialect, p); err != nil {\n", cs.Name)
			b.WriteString("\t\treturn nil, err\n")
			b.WriteString("\t}\n")
		}
		emitStackRenderInput("\t", "p")
		fmt.Fprintf(b, "\treturn q.cache.Exec(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, %s, sqlDialect, desc%s, &in, (*argBuf)...)\n", cs.Name, fastSlotExpr, cs.Name)
		b.WriteString("}\n\n")

		// Pipeline queuing method for KindExec
		fmt.Fprintf(b, "func (pl *Pipeline) %s(%s) *runtime.ExecFuture {\n", cs.Name, sigStr)
		emitLocalParamsInit("\t")
		fmt.Fprintf(b, "\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
		b.WriteString("\targBuf := runtime.AcquireArgsCap(approxArgs)\n")
		fmt.Fprintf(b, "\texpandErr := expand%sParams(argBuf, sqlDialect, p)\n", cs.Name)
		emitStackRenderInput("\t", "p")
		fmt.Fprintf(b, "\treturn pl.Pipeline.QueueExec(stmtID%s, desc%s, in, vk, %s, argBuf, expandErr)\n", cs.Name, cs.Name, pipelineFastSlotExpr)
		b.WriteString("}\n\n")

		if cs.CanBatch {
			emitExecBatchAndStreamMethods(b, cs, sigWithLeadingComma, paramsStructName, emitStackRenderInput)
		}
		return
	}

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

	// Emit single unified scan<Name>Row helper (accepts both *sql.Row and *sql.Rows)
	fmt.Fprintf(b, "func scan%sRow(scanner interface{ Scan(...any) error }, dst *%s, colMask uint64, p %s) error {\n", cs.Name, cs.RowTypeName, paramsStructName)
	b.WriteString("\t_ = colMask\n\t_ = p\n")
	emitScanSetup("\t")
	b.WriteString("\tif err := scanner.Scan(scanSlice...); err != nil {\n\t\treturn err\n\t}\n")
	b.WriteString("\t*dst = item\n\treturn nil\n}\n\n")

	// 3. KindOne method
	if cs.Kind == KindOne {
		fmt.Fprintf(b, "func (q *Queries) %s(ctx context.Context%s) (%s, error) {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
		fmt.Fprintf(b, "\tvar zero %s\n", cs.RowTypeName)
		emitLocalParamsInit("\t")
		fmt.Fprintf(b, "\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
		b.WriteString("\t_ = colMask\n")
		b.WriteString("\targBuf := runtime.AcquireArgsCap(approxArgs)\n")
		b.WriteString("\tdefer runtime.ReleaseArgs(argBuf)\n")
		if !canFailExpand {
			fmt.Fprintf(b, "\t_ = expand%sParams(argBuf, sqlDialect, p)\n", cs.Name)
		} else {
			fmt.Fprintf(b, "\tif err := expand%sParams(argBuf, sqlDialect, p); err != nil {\n", cs.Name)
			b.WriteString("\t\treturn zero, err\n")
			b.WriteString("\t}\n")
		}
		emitStackRenderInput("\t", "p")
		fmt.Fprintf(b, "\tsqlRow, ms, err := q.cache.QueryRow(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, %s, sqlDialect, desc%s, &in, (*argBuf)...)\n", cs.Name, fastSlotExpr, cs.Name)
		b.WriteString("\tif err != nil {\n")
		b.WriteString("\t\treturn zero, err\n")
		b.WriteString("\t}\n")
		fmt.Fprintf(b, "\tvar item %s\n", cs.RowTypeName)
		fmt.Fprintf(b, "\terr = scan%sRow(sqlRow, &item, colMask, p)\n", cs.Name)
		b.WriteString("\t_ = ms.Release()\n")
		b.WriteString("\tif err != nil {\n")
		b.WriteString("\t\treturn zero, err\n")
		b.WriteString("\t}\n")
		b.WriteString("\treturn item, nil\n")
		b.WriteString("}\n\n")

		// Pipeline queuing method for KindOne
		fmt.Fprintf(b, "func (pl *Pipeline) %s(%s) *runtime.RowFuture[%s] {\n", cs.Name, sigStr, cs.RowTypeName)
		emitLocalParamsInit("\t")
		fmt.Fprintf(b, "\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
		b.WriteString("\targBuf := runtime.AcquireArgsCap(approxArgs)\n")
		fmt.Fprintf(b, "\texpandErr := expand%sParams(argBuf, sqlDialect, p)\n", cs.Name)
		emitStackRenderInput("\t", "p")
		fmt.Fprintf(b, "\treturn runtime.QueueRow(pl.Pipeline, stmtID%s, desc%s, in, vk, %s, argBuf, expandErr,\n", cs.Name, cs.Name, pipelineFastSlotExpr)
		fmt.Fprintf(b, "\t\tfunc(r *sql.Row, dst *%s) error { return scan%sRow(r, dst, colMask, p) },\n", cs.RowTypeName, cs.Name)
		fmt.Fprintf(b, "\t\tfunc(rs *sql.Rows, dst *%s) error { return scan%sRow(rs, dst, colMask, p) },\n", cs.RowTypeName, cs.Name)
		b.WriteString("\t)\n}\n\n")
		return
	}

	// 4. KindMany methods: <Name>, <Name>Into, <Name>Seq, <Name>Each
	fmt.Fprintf(b, "func (q *Queries) %s(ctx context.Context%s) ([]%s, error) {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\treturn q.%sInto(ctx, nil%s)\n", cs.Name, callWithLeadingComma)
	b.WriteString("}\n\n")

	fmt.Fprintf(b, "func (q *Queries) %sInto(ctx context.Context, dst []%s%s) ([]%s, error) {\n", cs.Name, cs.RowTypeName, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\tfor row, err := range q.%sSeq(ctx%s) {\n", cs.Name, callWithLeadingComma)
	b.WriteString("\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
	b.WriteString("\t\tdst = append(dst, row)\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn dst, nil\n")
	b.WriteString("}\n\n")

	fmt.Fprintf(b, "func (q *Queries) %sSeq(ctx context.Context%s) iter.Seq2[%s, error] {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	b.WriteString("\treturn func(yield func(" + cs.RowTypeName + ", error) bool) {\n")
	fmt.Fprintf(b, "\t\tvar zero %s\n", cs.RowTypeName)
	emitLocalParamsInit("\t\t")
	fmt.Fprintf(b, "\t\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
	b.WriteString("\t\t_ = colMask\n")
	b.WriteString("\t\targBuf := runtime.AcquireArgsCap(approxArgs)\n")
	b.WriteString("\t\tdefer runtime.ReleaseArgs(argBuf)\n")
	if !canFailExpand {
		fmt.Fprintf(b, "\t\t_ = expand%sParams(argBuf, sqlDialect, p)\n", cs.Name)
	} else {
		fmt.Fprintf(b, "\t\tif err := expand%sParams(argBuf, sqlDialect, p); err != nil {\n", cs.Name)
		b.WriteString("\t\t\tyield(zero, err)\n")
		b.WriteString("\t\t\treturn\n")
		b.WriteString("\t\t}\n")
	}
	emitStackRenderInput("\t\t", "p")
	fmt.Fprintf(b, "\t\tsqlRows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, %s, sqlDialect, desc%s, &in, (*argBuf)...)\n", cs.Name, fastSlotExpr, cs.Name)
	b.WriteString("\t\tif err != nil {\n")
	b.WriteString("\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\treturn\n")
	b.WriteString("\t\t}\n")
	fmt.Fprintf(b, "\t\tvar item %s\n", cs.RowTypeName)
	b.WriteString("\t\tfor sqlRows.Next() {\n")
	fmt.Fprintf(b, "\t\t\tif err := scan%sRow(sqlRows, &item, colMask, p); err != nil {\n", cs.Name)
	b.WriteString("\t\t\t\truntime.CloseRows(sqlRows, ms)\n")
	b.WriteString("\t\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\t\treturn\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t\tif !yield(item, nil) {\n")
	b.WriteString("\t\t\t\truntime.CloseRows(sqlRows, ms)\n")
	b.WriteString("\t\t\t\treturn\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t\terr = sqlRows.Err()\n")
	b.WriteString("\t\truntime.CloseRows(sqlRows, ms)\n")
	b.WriteString("\t\tif err != nil {\n")
	b.WriteString("\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("}\n\n")

	fmt.Fprintf(b, "func (q *Queries) %sEach(ctx context.Context%s, fn func(%s) error) error {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\tfor row, err := range q.%sSeq(ctx%s) {\n", cs.Name, callWithLeadingComma)
	b.WriteString("\t\tif err != nil {\n\t\t\treturn err\n\t\t}\n")
	b.WriteString("\t\tif err := fn(row); err != nil {\n\t\t\treturn err\n\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn nil\n")
	b.WriteString("}\n\n")

	// Pipeline queuing method for KindMany
	fmt.Fprintf(b, "func (pl *Pipeline) %s(%s) *runtime.SliceFuture[%s] {\n", cs.Name, sigStr, cs.RowTypeName)
	emitLocalParamsInit("\t")
	fmt.Fprintf(b, "\tvk, colMask, flagMask, approxArgs := compute%sKey(p)\n", cs.Name)
	b.WriteString("\targBuf := runtime.AcquireArgsCap(approxArgs)\n")
	fmt.Fprintf(b, "\texpandErr := expand%sParams(argBuf, sqlDialect, p)\n", cs.Name)
	emitStackRenderInput("\t", "p")
	fmt.Fprintf(b, "\treturn runtime.QueueSlice(pl.Pipeline, stmtID%s, desc%s, in, vk, %s, argBuf, expandErr,\n", cs.Name, cs.Name, pipelineFastSlotExpr)
	fmt.Fprintf(b, "\t\tfunc(rs *sql.Rows, dst *%s) error { return scan%sRow(rs, dst, colMask, p) },\n", cs.RowTypeName, cs.Name)
	b.WriteString("\t)\n}\n\n")

	if cs.CanBatch {
		emitManyBatchAndStreamMethods(b, cs, sigWithLeadingComma, callWithLeadingComma, paramsStructName, emitStackRenderInput)
	}
}

// emitBatchGlobalPresortSetup emits global index sorting before chunk slicing when the batched collection has HasSort == true.
func emitBatchGlobalPresortSetup(b *bytes.Buffer, cs *compiledStmt, indent, collAccess string) {
	batchParam := cs.Params[cs.BatchParamIdx]
	if cs.BatchIsTuple && batchParam.Tuple != nil && batchParam.Tuple.HasSort {
		fmt.Fprintf(b, "%svar sortedIdx *[]int\n", indent)
		fmt.Fprintf(b, "%sif !is%sSorted(%s) {\n", indent, batchParam.Tuple.Name, collAccess)
		fmt.Fprintf(b, "%s\tsortedIdx = runtime.AcquireIndices(totalRows)\n", indent)
		fmt.Fprintf(b, "%s\tdefer runtime.ReleaseIndices(sortedIdx)\n", indent)
		fmt.Fprintf(b, "%s\tslices.SortFunc(*sortedIdx, func(i, j int) int { return compare%s(%s[i], %s[j]) })\n", indent, batchParam.Tuple.Name, collAccess, collAccess)
		fmt.Fprintf(b, "%s}\n", indent)
	} else if !cs.BatchIsTuple && batchParam.HasSort {
		dirTag := "Asc"
		if batchParam.Desc {
			dirTag = "Desc"
		}
		typeIdent := sanitizeTypeIdent(batchParam.ElemGoType)
		fmt.Fprintf(b, "%svar sortedIdx *[]int\n", indent)
		fmt.Fprintf(b, "%sif !is%s%sSorted(%s) {\n", indent, typeIdent, dirTag, collAccess)
		fmt.Fprintf(b, "%s\tsortedIdx = runtime.AcquireIndices(totalRows)\n", indent)
		fmt.Fprintf(b, "%s\tdefer runtime.ReleaseIndices(sortedIdx)\n", indent)
		fmt.Fprintf(b, "%s\tslices.SortFunc(*sortedIdx, func(i, j int) int { return compare%s%s(%s[i], %s[j]) })\n", indent, typeIdent, dirTag, collAccess, collAccess)
		fmt.Fprintf(b, "%s}\n", indent)
	}
}

// emitBatchChunkExpansion emits argument expansion for a single batch chunk [offset : offset+chunkSize],
// using sortedIdx if non-nil so global presort order is preserved across chunks without mutating caller memory.
func emitBatchChunkExpansion(b *bytes.Buffer, cs *compiledStmt, indent, collAccess, _ string) {
	batchParam := cs.Params[cs.BatchParamIdx]
	hasSort := (cs.BatchIsTuple && batchParam.Tuple != nil && batchParam.Tuple.HasSort) || (!cs.BatchIsTuple && batchParam.HasSort)
	if !hasSort {
		fmt.Fprintf(b, "%s_ = expand%sParams(argBuf, sqlDialect, chunkP)\n", indent, cs.Name)
		return
	}

	// When HasSort is true and sortedIdx == nil, chunkP is already sorted so expand<Name>Params handles it directly.
	fmt.Fprintf(b, "%sif sortedIdx == nil {\n", indent)
	fmt.Fprintf(b, "%s\t_ = expand%sParams(argBuf, sqlDialect, chunkP)\n", indent, cs.Name)
	fmt.Fprintf(b, "%s} else {\n", indent)
	for _, sl := range cs.Root.Slots {
		switch sl.Kind {
		case runtime.SlotScalar:
			p := cs.Params[sl.ParamIdx]
			fmt.Fprintf(b, "%s\t*argBuf = append(*argBuf, chunkP.%s)\n", indent, p.FieldName)
		case runtime.SlotSlice:
			fmt.Fprintf(b, "%s\tidx := (*sortedIdx)[offset : offset+chunkSize]\n", indent)
			fmt.Fprintf(b, "%s\tfor _, k := range idx {\n%s\t\t*argBuf = append(*argBuf, %s[k])\n%s\t}\n", indent, indent, collAccess, indent)
		case runtime.SlotTupleSlice:
			fmt.Fprintf(b, "%s\tidx := (*sortedIdx)[offset : offset+chunkSize]\n", indent)
			fmt.Fprintf(b, "%s\tfor _, k := range idx {\n", indent)
			for _, f := range sl.Tuple.Fields {
				fmt.Fprintf(b, "%s\t\t*argBuf = append(*argBuf, %s[k].%s)\n", indent, collAccess, f.FieldName)
			}
			fmt.Fprintf(b, "%s\t}\n", indent)
		case runtime.SlotOptBlock:
			flag := cs.Flags[sl.FlagBit]
			fmt.Fprintf(b, "%s\tif chunkP.%s {\n", indent, flag.FieldName)
			for _, innerSl := range sl.Inner.Slots {
				if innerSl.Kind == runtime.SlotScalar {
					ip := cs.Params[innerSl.ParamIdx]
					fmt.Fprintf(b, "%s\t\t*argBuf = append(*argBuf, chunkP.%s)\n", indent, ip.FieldName)
				}
			}
			fmt.Fprintf(b, "%s\t}\n", indent)
		}
	}
	fmt.Fprintf(b, "%s}\n", indent)
}

func buildStreamSignatures(cs *compiledStmt, paramsStructName string) (sigStream, callBatchArgs string) {
	batchParam := cs.Params[cs.BatchParamIdx]
	seqType := "iter.Seq2[" + batchParam.ElemGoType + ", error]"
	if cs.UseParamsStruct {
		return ", p " + paramsStructName + ", seq " + seqType, ", chunkP"
	}
	var sig []byte
	var call []byte
	for i, p := range cs.Params {
		sig = append(sig, ", "...)
		call = append(call, ", "...)
		if i == cs.BatchParamIdx {
			sig = append(sig, ("seq " + seqType)...)
			call = append(call, "chunk"...)
		} else {
			sig = append(sig, (p.ParamName + " " + p.GoType)...)
			call = append(call, p.ParamName...)
		}
	}
	return string(sig), string(call)
}

func emitExecBatchAndStreamMethods(
	b *bytes.Buffer,
	cs *compiledStmt,
	sigWithLeadingComma, paramsStructName string,
	emitStackRenderInput func(indent, pVar string),
) {
	batchParam := cs.Params[cs.BatchParamIdx]
	collAccess := batchParam.ParamName
	if cs.UseParamsStruct {
		collAccess = "p." + batchParam.FieldName
	}

	fmt.Fprintf(b, "func (q *Queries) %sBatch(ctx context.Context%s) (sql.Result, error) {\n", cs.Name, sigWithLeadingComma)
	fmt.Fprintf(b, "\ttotalRows := len(%s)\n", collAccess)
	b.WriteString("\tif totalRows == 0 {\n\t\treturn nil, runtime.ErrEmptySliceParam\n\t}\n")
	if !cs.UseParamsStruct {
		fmt.Fprintf(b, "\tp := %s{\n", paramsStructName)
		for _, p := range cs.Params {
			fmt.Fprintf(b, "\t\t%s: %s,\n", p.FieldName, p.ParamName)
		}
		b.WriteString("\t}\n")
	}
	emitBatchGlobalPresortSetup(b, cs, "\t", collAccess)
	fmt.Fprintf(b, "\t_, colMask, flagMask, _ := compute%sKey(p)\n", cs.Name)
	b.WriteString("\tshapeTag := runtime.ComputeBatchShapeTag(colMask, flagMask)\n")
	fmt.Fprintf(b, "\tmaxRows := q.cache.MaxRowsPerBatch(sqlDialect, %d, %d)\n", cs.ScalarParamCnt, cs.BatchArity)
	fmt.Fprintf(b, "\tfirstChunk := q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows, maxRows, true)\n", cs.Name)
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
	fmt.Fprintf(b, "\t\t\tchunkSize = q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows-offset, maxRows, false)\n", cs.Name)
	b.WriteString("\t\t}\n")
	fmt.Fprintf(b, "\t\tchunkRows := %s[offset : offset+chunkSize]\n", collAccess)
	b.WriteString("\t\tchunkP := p\n")
	fmt.Fprintf(b, "\t\tchunkP.%s = chunkRows\n", batchParam.FieldName)
	fmt.Fprintf(b, "\t\tvk, _, _, approxArgs := compute%sKey(chunkP)\n", cs.Name)
	b.WriteString("\t\targBuf := runtime.AcquireArgsCap(approxArgs)\n")
	emitBatchChunkExpansion(b, cs, "\t\t", collAccess, "\t\t\truntime.ReleaseArgs(argBuf)\n\t\t\tif autoTx != nil {\n\t\t\t\t_ = autoTx.Rollback()\n\t\t\t}\n\t\t\treturn nil, err\n")
	emitStackRenderInput("\t\t", "chunkP")
	fmt.Fprintf(b, "\t\tres, err := q.cache.Exec(ctx, q.db, txCache, q.usePrepared, vk, stmtID%s, shapeTag, chunkSize, nil, sqlDialect, desc%s, &in, (*argBuf)...)\n", cs.Name, cs.Name)
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

	if cs.CanStream {
		sigStream, callBatchArgs := buildStreamSignatures(cs, paramsStructName)
		poolName := lowerFirstIdent(batchParam.ElemGoType) + "ChunkPool"

		fmt.Fprintf(b, "func (q *Queries) %sStream(ctx context.Context%s) (sql.Result, error) {\n", cs.Name, sigStream)
		fmt.Fprintf(b, "\tchunkCap := min(256, q.cache.MaxRowsPerBatch(sqlDialect, %d, %d))\n", cs.ScalarParamCnt, cs.BatchArity)
		fmt.Fprintf(b, "\tbufPtr := %s.Get().(*[]%s)\n", poolName, batchParam.ElemGoType)
		b.WriteString("\tchunk := (*bufPtr)[:0]\n")
		b.WriteString("\tdefer func() {\n\t\tclear(chunk)\n\t\t*bufPtr = chunk[:0]\n")
		fmt.Fprintf(b, "\t\t%s.Put(bufPtr)\n\t}()\n", poolName)
		b.WriteString("\tvar batchRes runtime.BatchExecResult\n")
		b.WriteString("\tflushChunk := func() error {\n")
		b.WriteString("\t\tif len(chunk) == 0 {\n\t\t\treturn nil\n\t\t}\n")
		if cs.UseParamsStruct {
			b.WriteString("\t\tchunkP := p\n")
			fmt.Fprintf(b, "\t\tchunkP.%s = chunk\n", batchParam.FieldName)
		}
		fmt.Fprintf(b, "\t\tres, err := q.%sBatch(ctx%s)\n", cs.Name, callBatchArgs)
		b.WriteString("\t\tif err != nil {\n\t\t\treturn err\n\t\t}\n")
		b.WriteString("\t\tbatchRes.Add(res)\n")
		b.WriteString("\t\tchunk = chunk[:0]\n\t\treturn nil\n\t}\n")
		b.WriteString("\tfor item, err := range seq {\n")
		b.WriteString("\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
		b.WriteString("\t\tchunk = append(chunk, item)\n")
		b.WriteString("\t\tif len(chunk) >= chunkCap {\n")
		b.WriteString("\t\t\tif err := flushChunk(); err != nil {\n\t\t\t\treturn nil, err\n\t\t\t}\n")
		b.WriteString("\t\t}\n\t}\n")
		b.WriteString("\tif err := flushChunk(); err != nil {\n\t\treturn nil, err\n\t}\n")
		b.WriteString("\treturn batchRes, nil\n")
		b.WriteString("}\n\n")

		fmt.Fprintf(b, "func (q *Queries) %sStreamSeq(ctx context.Context%s) (sql.Result, error) {\n", cs.Name, sigStream)
		if cs.UseParamsStruct {
			fmt.Fprintf(b, "\treturn q.%sStream(ctx, p, seq)\n", cs.Name)
		} else {
			var forwardArgs []byte
			for i, p := range cs.Params {
				forwardArgs = append(forwardArgs, ", "...)
				if i == cs.BatchParamIdx {
					forwardArgs = append(forwardArgs, "seq"...)
				} else {
					forwardArgs = append(forwardArgs, p.ParamName...)
				}
			}
			fmt.Fprintf(b, "\treturn q.%sStream(ctx%s)\n", cs.Name, string(forwardArgs))
		}
		b.WriteString("}\n\n")
	}
}

func emitManyBatchAndStreamMethods(
	b *bytes.Buffer,
	cs *compiledStmt,
	sigWithLeadingComma, callWithLeadingComma, paramsStructName string,
	emitStackRenderInput func(indent, pVar string),
) {
	batchParam := cs.Params[cs.BatchParamIdx]
	collAccess := batchParam.ParamName
	if cs.UseParamsStruct {
		collAccess = "p." + batchParam.FieldName
	}

	// <Name>Batch
	fmt.Fprintf(b, "func (q *Queries) %sBatch(ctx context.Context%s) ([]%s, error) {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\treturn q.%sBatchInto(ctx, nil%s)\n", cs.Name, callWithLeadingComma)
	b.WriteString("}\n\n")

	// <Name>BatchInto
	fmt.Fprintf(b, "func (q *Queries) %sBatchInto(ctx context.Context, dst []%s%s) ([]%s, error) {\n", cs.Name, cs.RowTypeName, sigWithLeadingComma, cs.RowTypeName)
	fmt.Fprintf(b, "\ttotalRows := len(%s)\n", collAccess)
	b.WriteString("\tif totalRows == 0 {\n\t\treturn nil, runtime.ErrEmptySliceParam\n\t}\n")
	if !cs.UseParamsStruct {
		fmt.Fprintf(b, "\tp := %s{\n", paramsStructName)
		for _, p := range cs.Params {
			fmt.Fprintf(b, "\t\t%s: %s,\n", p.FieldName, p.ParamName)
		}
		b.WriteString("\t}\n")
	}
	fmt.Fprintf(b, "\t_, colMask, flagMask, _ := compute%sKey(p)\n", cs.Name)
	fmt.Fprintf(b, "\tmaxRows := q.cache.MaxRowsPerBatch(sqlDialect, %d, %d)\n", cs.ScalarParamCnt, cs.BatchArity)
	fmt.Fprintf(b, "\tfirstChunk := q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows, maxRows, true)\n", cs.Name)
	b.WriteString("\ttargetQ := q\n")
	b.WriteString("\tvar autoTx *sql.Tx\n")
	b.WriteString("\tif q.txCache == nil && firstChunk < totalRows {\n")
	b.WriteString("\t\tvar err error\n")
	b.WriteString("\t\tautoTx, err = q.db.BeginTx(ctx, nil)\n")
	b.WriteString("\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
	b.WriteString("\t\ttargetQ = q.WithTx(autoTx)\n")
	b.WriteString("\t\tdefer func() { _ = targetQ.Close() }()\n")
	b.WriteString("\t}\n")
	fmt.Fprintf(b, "\tfor row, err := range targetQ.%sBatchSeq(ctx%s) {\n", cs.Name, callWithLeadingComma)
	b.WriteString("\t\tif err != nil {\n")
	b.WriteString("\t\t\tif autoTx != nil {\n\t\t\t\t_ = autoTx.Rollback()\n\t\t\t}\n")
	b.WriteString("\t\t\treturn nil, err\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t\tdst = append(dst, row)\n")
	b.WriteString("\t}\n")
	b.WriteString("\tif autoTx != nil {\n")
	b.WriteString("\t\tif err := autoTx.Commit(); err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn dst, nil\n")
	b.WriteString("}\n\n")

	// <Name>BatchSeq
	fmt.Fprintf(b, "func (q *Queries) %sBatchSeq(ctx context.Context%s) iter.Seq2[%s, error] {\n", cs.Name, sigWithLeadingComma, cs.RowTypeName)
	b.WriteString("\treturn func(yield func(" + cs.RowTypeName + ", error) bool) {\n")
	fmt.Fprintf(b, "\t\tvar zero %s\n", cs.RowTypeName)
	fmt.Fprintf(b, "\t\ttotalRows := len(%s)\n", collAccess)
	b.WriteString("\t\tif totalRows == 0 {\n\t\t\tyield(zero, runtime.ErrEmptySliceParam)\n\t\t\treturn\n\t\t}\n")
	if !cs.UseParamsStruct {
		fmt.Fprintf(b, "\t\tp := %s{\n", paramsStructName)
		for _, p := range cs.Params {
			fmt.Fprintf(b, "\t\t\t%s: %s,\n", p.FieldName, p.ParamName)
		}
		b.WriteString("\t\t}\n")
	}
	emitBatchGlobalPresortSetup(b, cs, "\t\t", collAccess)
	fmt.Fprintf(b, "\t\t_, colMask, flagMask, _ := compute%sKey(p)\n", cs.Name)
	b.WriteString("\t\tshapeTag := runtime.ComputeBatchShapeTag(colMask, flagMask)\n")
	fmt.Fprintf(b, "\t\tmaxRows := q.cache.MaxRowsPerBatch(sqlDialect, %d, %d)\n", cs.ScalarParamCnt, cs.BatchArity)
	fmt.Fprintf(b, "\t\tfirstChunk := q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows, maxRows, true)\n", cs.Name)
	b.WriteString("\t\tfor offset := 0; offset < totalRows; {\n")
	b.WriteString("\t\t\tchunkSize := firstChunk\n")
	b.WriteString("\t\t\tif offset > 0 {\n")
	fmt.Fprintf(b, "\t\t\t\tchunkSize = q.cache.PlanBatchChunk(stmtID%s, colMask, flagMask, totalRows-offset, maxRows, false)\n", cs.Name)
	b.WriteString("\t\t\t}\n")
	fmt.Fprintf(b, "\t\t\tchunkRows := %s[offset : offset+chunkSize]\n", collAccess)
	b.WriteString("\t\t\tchunkP := p\n")
	fmt.Fprintf(b, "\t\t\tchunkP.%s = chunkRows\n", batchParam.FieldName)
	fmt.Fprintf(b, "\t\t\tvk, _, _, approxArgs := compute%sKey(chunkP)\n", cs.Name)
	b.WriteString("\t\t\targBuf := runtime.AcquireArgsCap(approxArgs)\n")
	emitBatchChunkExpansion(b, cs, "\t\t\t", collAccess, "\t\t\t\truntime.ReleaseArgs(argBuf)\n\t\t\t\tyield(zero, err)\n\t\t\t\treturn\n")
	emitStackRenderInput("\t\t\t", "chunkP")
	fmt.Fprintf(b, "\t\t\tsqlRows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtID%s, shapeTag, chunkSize, nil, sqlDialect, desc%s, &in, (*argBuf)...)\n", cs.Name, cs.Name)
	b.WriteString("\t\t\truntime.ReleaseArgs(argBuf)\n")
	b.WriteString("\t\t\tif err != nil {\n")
	b.WriteString("\t\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\t\treturn\n")
	b.WriteString("\t\t\t}\n")
	fmt.Fprintf(b, "\t\t\tvar item %s\n", cs.RowTypeName)
	b.WriteString("\t\t\tfor sqlRows.Next() {\n")
	fmt.Fprintf(b, "\t\t\t\tif err := scan%sRow(sqlRows, &item, colMask, chunkP); err != nil {\n", cs.Name)
	b.WriteString("\t\t\t\t\truntime.CloseRows(sqlRows, ms)\n")
	b.WriteString("\t\t\t\t\tyield(zero, err)\n")
	b.WriteString("\t\t\t\t\treturn\n")
	b.WriteString("\t\t\t\t}\n")
	b.WriteString("\t\t\t\tif !yield(item, nil) {\n")
	b.WriteString("\t\t\t\t\truntime.CloseRows(sqlRows, ms)\n")
	b.WriteString("\t\t\t\t\treturn\n")
	b.WriteString("\t\t\t\t}\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t\terr = sqlRows.Err()\n")
	b.WriteString("\t\t\truntime.CloseRows(sqlRows, ms)\n")
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

	// <Name>Stream and <Name>StreamSeq
	if cs.CanStream {
		sigStream, callBatchArgs := buildStreamSignatures(cs, paramsStructName)
		poolName := lowerFirstIdent(batchParam.ElemGoType) + "ChunkPool"

		fmt.Fprintf(b, "func (q *Queries) %sStreamSeq(ctx context.Context%s) iter.Seq2[%s, error] {\n", cs.Name, sigStream, cs.RowTypeName)
		b.WriteString("\treturn func(yield func(" + cs.RowTypeName + ", error) bool) {\n")
		fmt.Fprintf(b, "\t\tvar zero %s\n", cs.RowTypeName)
		fmt.Fprintf(b, "\t\tchunkCap := min(256, q.cache.MaxRowsPerBatch(sqlDialect, %d, %d))\n", cs.ScalarParamCnt, cs.BatchArity)
		fmt.Fprintf(b, "\t\tbufPtr := %s.Get().(*[]%s)\n", poolName, batchParam.ElemGoType)
		b.WriteString("\t\tchunk := (*bufPtr)[:0]\n")
		b.WriteString("\t\tdefer func() {\n\t\t\tclear(chunk)\n\t\t\t*bufPtr = chunk[:0]\n")
		fmt.Fprintf(b, "\t\t\t%s.Put(bufPtr)\n\t\t}()\n", poolName)
		b.WriteString("\t\tflushChunk := func() bool {\n")
		b.WriteString("\t\t\tif len(chunk) == 0 {\n\t\t\t\treturn true\n\t\t\t}\n")
		if cs.UseParamsStruct {
			b.WriteString("\t\t\tchunkP := p\n")
			fmt.Fprintf(b, "\t\t\tchunkP.%s = chunk\n", batchParam.FieldName)
		}
		fmt.Fprintf(b, "\t\t\tfor r, err := range q.%sBatchSeq(ctx%s) {\n", cs.Name, callBatchArgs)
		b.WriteString("\t\t\t\tif err != nil {\n\t\t\t\t\tyield(zero, err)\n\t\t\t\t\treturn false\n\t\t\t\t}\n")
		b.WriteString("\t\t\t\tif !yield(r, nil) {\n\t\t\t\t\treturn false\n\t\t\t\t}\n")
		b.WriteString("\t\t\t}\n")
		b.WriteString("\t\t\tchunk = chunk[:0]\n\t\t\treturn true\n\t\t}\n")
		b.WriteString("\t\tfor item, err := range seq {\n")
		b.WriteString("\t\t\tif err != nil {\n\t\t\t\tyield(zero, err)\n\t\t\t\treturn\n\t\t\t}\n")
		b.WriteString("\t\t\tchunk = append(chunk, item)\n")
		b.WriteString("\t\t\tif len(chunk) >= chunkCap {\n")
		b.WriteString("\t\t\t\tif !flushChunk() {\n\t\t\t\t\treturn\n\t\t\t\t}\n")
		b.WriteString("\t\t\t}\n\t\t}\n")
		b.WriteString("\t\t_ = flushChunk()\n")
		b.WriteString("\t}\n")
		b.WriteString("}\n\n")

		fmt.Fprintf(b, "func (q *Queries) %sStream(ctx context.Context%s) ([]%s, error) {\n", cs.Name, sigStream, cs.RowTypeName)
		fmt.Fprintf(b, "\tvar out []%s\n", cs.RowTypeName)
		var forwardArgs []byte
		if cs.UseParamsStruct {
			forwardArgs = append(forwardArgs, ", p, seq"...)
		} else {
			for i, p := range cs.Params {
				forwardArgs = append(forwardArgs, ", "...)
				if i == cs.BatchParamIdx {
					forwardArgs = append(forwardArgs, "seq"...)
				} else {
					forwardArgs = append(forwardArgs, p.ParamName...)
				}
			}
		}
		fmt.Fprintf(b, "\tfor r, err := range q.%sStreamSeq(ctx%s) {\n", cs.Name, string(forwardArgs))
		b.WriteString("\t\tif err != nil {\n\t\t\treturn nil, err\n\t\t}\n")
		b.WriteString("\t\tout = append(out, r)\n")
		b.WriteString("\t}\n")
		b.WriteString("\treturn out, nil\n")
		b.WriteString("}\n\n")
	}
}
