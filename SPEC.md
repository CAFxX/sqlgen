# `sqlgen` — Complete Production Architecture, Ergonomics & Verification Specification (`SPEC.md`)

Note: the spec comes from the spec workshop on https://share.gemini.google/Nm8OXGBJD6LD

## 1. System Overview & The Ten Non-Negotiable Architectural Laws

`sqlgen` is a production-grade Go SQL code generator and zero-reflection runtime library that compiles a declarative YAML configuration (`sqlgen.yaml`) containing annotated SQL statements into strongly-typed, idiomatic Go code backed by `database/sql`.

### 1.1. The Ten Core Invariants

1. **The Single-`strings.Builder` Law & Provable Upper-Bound Pre-Sizing**:
   - **Standalone Statement Execution**: Rendering any SQL statement at runtime—regardless of how many nested `sqlgen.Query` CTEs, `{?Flag: ...}` conditional blocks, `SelectCols` bitmasks, `Seek`/`Order` clauses, or multi-row `VALUES` tuples it contains—MUST instantiate **at most one `strings.Builder`** (`0` on a Tier-1 or Tier-2 cache hit; `1` on a cache miss).
   - **Provable Upper-Bound Pre-Sizing (`EstimateOpBytes`)**: Before streaming SQL into a `strings.Builder`, `b.Grow(EstimateOpBytes(...))` MUST be called where `EstimateOpBytes` is a guaranteed mathematical upper bound (`EstimateOpBytes >= len(renderedSQL)`). Reallocating a `strings.Builder`'s internal `[]byte` slice mid-render is a specification violation.
   - **Pipeline / `PipelineTx` Execution**: Rendering an entire $M$-statement `Pipeline` or `PipelineTx`—including all `AssertRowsAffected` CTE guards (`_sqlgen_chk`), `If/Then/Else` condition producers (`_sqlgen_cond`), and inline branch guards—MUST instantiate **at most one `strings.Builder` for the entire batch**. All uncached or decorated operations stream sequentially into that single builder, `master := b.String()` is called **once** after all operations are written, and each `ops[i].SQL` is assigned via zero-allocation string slicing `master[span.start:span.end]`.
   - **Zero String Formatting in Combinators or Helpers**: Constructing curried `sqlgen.Query` values, `Order` specifications, or `PipelineCond` boolean AST trees (`.RowsAffectedEq(1).And(...)`) MUST NEVER call `fmt.Sprintf`, `strings.Join`, `+` string concatenation, or instantiate `strings.Builder`. All integer formatting (`$1`, `COUNT(*) = N`, `'sqlgen.cN'`) MUST use stack-allocated `[20]byte` buffers with `strconv.AppendInt`.
2. **Zero-Closure Cache Resolution & Zero-Reflection Execution**:
   - Zero `reflect` usage anywhere in `runtime` or generated code.
   - `StmtCache` methods (`Resolve`, `Exec`, `Query`, `QueryRow`) MUST accept `(dialect Dialect, desc *QueryDescriptor, in *RenderInput)` rather than a `func() string` closure so local variables never escape to the heap on cache hits.
   - Flattened `[]any` query arguments MUST use pooled `*[]any` slice pointers (`AcquireArgs`, `AcquireArgsCap`, `ReleaseArgs`) cleared via `clear(s)` before returning to `sync.Pool`.
   - Dynamic column scanning MUST bind field pointers into a stack-allocated `[NumCols]any` buffer once *before* `for rows.Next()`, zeroing the target struct in-place (`item = RowType{}`) on each iteration.
3. **100% `database/sql` Compatibility**:
   - Works seamlessly with `*sql.DB` (implicit transactions) and `*sql.Tx` (explicit transactions via `q.WithTx(tx)` and `q.InTx(...)`) across PostgreSQL, CockroachDB, SQLite, MySQL, and MariaDB, including when `db.SetMaxOpenConns(1)`.
4. **Everything-in-SQL Typed DSL**:
   - Output columns (`col:GoType`), input parameters (`Param:GoType`), 1D slices (`IDs:[]int64`), multi-row tuple slices (`Rows:[]Patch(ID:int64, ...)`), deadlock-prevention sort modifiers (`ASC`/`DESC`), conditional blocks (`{?Flag: ...}`), keyset pagination (`Seek:sqlgen.Seek` and `Order:sqlgen.Order(...)`), and composable subqueries (`Sub:sqlgen.Query`) are expressed inline in the SQL string.
5. **16-Shard Two-Tier Adaptive Statement Cache (W-TinyLFU + SLRU)**:
   - **Tier 1 (Partially Prepared / Staged SQL)**: Stores pre-rendered `sqlStr` strings in memory (`MaxSQLVariants`, `MaxPartialBytes`) using Segmented LRU (20% Probation, 80% Protected), W-TinyLFU admission filtering (via a 2 KB 4-bit Count-Min Sketch per shard), and in-place `cacheEntry` struct recycling.
   - **Tier 2 (Fully Prepared `*sql.Stmt`)**: Stores reference-counted `ManagedStmt` handles (`MaxPreparedStmts`). Variants promote from Tier 1 to Tier 2 (using singleflight coordination via `entry.preparing`) when their decayed TinyLFU frequency reaches `PromoteThreshold` and beats the Tier-2 victim, and demote back to Tier 1 Protected (closing `*sql.Stmt` when `refs == 0` while keeping `sqlStr` warm) when superseded.
6. **Dual Resultset Consumption (Slice & Streaming)**:
   - Every `kind: many` query generates `<Name>` (slice), `<Name>Into` (reuses caller slice capacity), `<Name>Seq` (lazy `iter.Seq2[Row, error]` iterator), and `<Name>Each` (callback).
7. **Arity-Scaled Batching, Heavy-Hitter Promotion & Iterator Streaming Writes**:
   - Per-statement power-of-2 chunk ceiling $N_{\text{chunk\_max}} \in \{2^0 \dots 2^{15}\}$ (`1` to `32,768`) computed from scalar count $S$, tuple arity $F$, dialect parameter limit (`65,535` or `32,766`), and `WithMaxBatchParams` (default `16,384`).
   - Recurring non-power-of-2 batch sizes (e.g., `1000` or `900` rows) are promoted to dedicated Tier-2 `*sql.Stmt` handles, greedily reused for larger batches (`2900 = 1000 + 1000 + 900`), and fall back to powers of 2 when cold.
   - Tuple-slice statements also generate `<Name>Stream` and `<Name>StreamSeq` accepting `iter.Seq2[TupleStruct, error]` for $O(1)$-memory streaming writes.
8. **Deadlock-Prevention Global Presorting (`ASC` / `DESC`)**:
   - Slices and tuple-slices with `ASC`/`DESC` modifiers run an $O(N)$ `isSorted` check first (0 allocs). If unsorted, they sort a pooled `*[]int` index permutation (`AcquireIndices`) globally **before** chunk slicing—never mutating the caller's slice and guaranteeing monotonic lock acquisition across all chunks.
9. **Single-Roundtrip `Pipeline` & `PipelineTx` with Server-Side Bail-Out & `If/Then/Else`**:
   - Flushes `[BEGIN + Q1..Qn + COMMIT + Sync]` in **1 network round-trip** on drivers implementing `WirePipeliner` (and on `*sql.Tx` via `WrapConnector`), falling back automatically to sequential Tier-2 prepared statements on standard drivers.
   - Supports `.AssertRowsAffected(n)`, `.AssertFound()`, and `p.If(cond, thenFn, elseFn)` with `p.Abort(err)`.
10. **Automatic Transaction Retries (`TxRetryPolicy`)**:
    - `InTx` and `PipelineTx` retry with exponential backoff and full jitter (`math/rand/v2`) on deterministic transient deadlocks and serialization failures (`40P01`, `40001`, `55P03`, MySQL `1213`/`1205`, SQLite `SQLITE_BUSY`/`SQLITE_LOCKED`), while never retrying ambiguous network disconnects (`driver.ErrBadConn`, `io.EOF`, `net.Error`) during `Commit`.

---

## 2. Repository Structure & Module Layout

Target Go version: **Go 1.27+** (matches `go.mod`)

```text
.
├── go.mod
├── DOCS.md                             # Complete, freestanding user documentation & cookbook
├── runtime/
│   ├── runtime.go                      # Core types, pools, VariantKey, Order/Seek, iterator helpers
│   ├── sql_stream.go                   # Single-strings.Builder SQL renderer (BuildComposedSQL, RenderPipelineBatch)
│   ├── cache.go                        # 16-shard Two-Tier W-TinyLFU + SLRU StmtCache, ManagedStmt, TxStmtCache
│   ├── pipeline.go                     # Pipeline, PipelineTx, Futures, If/Then/Else, WrapConnector, TxRetryPolicy
│   └── runtime_test.go                 # AST audit, mockdriver faults, PBT, fuzzing, concurrency & benchmark tests
├── generator.go                        # YAML parser, lexical SQL scanner, validator, single-buffer Go emitter
├── generator_test.go                   # Compilation matrix, parser fuzzing, PBT pagination, and SQLite e2e tests
├── cmd/
│   └── sqlgen/
│       ├── main.go                     # CLI binary (-config, -out, -stdout, -check, -version)
│       └── main_test.go                # CLI exit-code, drift-check, and stderr diagnostic tests
└── examples/                           # Clean-room dogfooding applications built strictly from DOCS.md
    ├── fintech_ledger/
    │   ├── sqlgen.yaml
    │   ├── ledger_gen.go
    │   ├── service.go
    │   └── service_test.go
    ├── saas_multitenant/
    │   ├── sqlgen.yaml
    │   ├── portal_gen.go
    │   ├── portal.go
    │   └── portal_test.go
    ├── ecommerce_inventory/
    │   ├── sqlgen.yaml
    │   ├── inventory_gen.go
    │   ├── worker.go
    │   └── worker_test.go
    └── clearing_exchange/
        ├── sqlgen.yaml
        ├── exchange_gen.go
        ├── engine.go
        └── engine_test.go
```

External dependencies in `go.mod`:
- `go.yaml.in/yaml/v3`
- `modernc.org/sqlite` (test-only dependency)

---

## 3. YAML Configuration Schema (`sqlgen.yaml`) & CLI Contract (`cmd/sqlgen`)

### 3.1. Configuration Types (`generator.go`)
```go
type QueryKind string

const (
	KindExec     QueryKind = "exec"
	KindOne      QueryKind = "one"
	KindMany     QueryKind = "many"
	KindSubquery QueryKind = "subquery"
)

type Dialect string

const (
	DialectQuestion Dialect = "question" // ? (SQLite, MySQL, MariaDB)
	DialectDollar   Dialect = "dollar"   // $1, $2, ... (PostgreSQL, CockroachDB)
)

// The `dialect` YAML key is case-insensitive and also accepts aliases (parseDialect):
//   dollar:   "", "postgres", "postgresql", "pgx", "cockroach", "cockroachdb", "$"  (default)
//   question: "sqlite", "sqlite3", "mysql", "mariadb", "?"
// Any other value fails with `sqlgen: unsupported dialect "<v>"`.

type ColumnSpec struct {
	Name     string `yaml:"name"`
	GoType   string `yaml:"type"`
	SQLExpr  string `yaml:"-"`
	FlagName string `yaml:"-"`
	FlagBit  uint64 `yaml:"-"`
}

type ParamSpec struct {
	Name    string `yaml:"name"`
	GoType  string `yaml:"type"`
	Slice   bool   `yaml:"slice"`
	HasSort bool   `yaml:"-"`
	Desc    bool   `yaml:"-"`
}

type StatementSpec struct {
	Name         string       `yaml:"name"`
	Kind         QueryKind    `yaml:"kind"`
	ResultType   string       `yaml:"result_type"`
	ParamsStruct bool         `yaml:"params_struct"`
	SQL          string       `yaml:"sql"`
	Params       []ParamSpec  `yaml:"params"`
	Columns      []ColumnSpec `yaml:"columns"`
}

type Config struct {
	Version       string          `yaml:"version"`
	PackageName   string          `yaml:"package"`
	Out           string          `yaml:"out"`
	RuntimeImport string          `yaml:"runtime_import"`
	ExtraImports  []string        `yaml:"extra_imports"`
	Dialect       Dialect         `yaml:"dialect"`
	Statements    []StatementSpec `yaml:"statements"`
}
```

### 3.2. Strict YAML Unmarshaling & Import Resolution Rules
1. `ParseYAML(data []byte) (Config, error)` MUST use `dec := yaml.NewDecoder(bytes.NewReader(data))` with `dec.KnownFields(true)`. Empty input (`io.EOF`), trailing YAML documents after the first document, or `Version` values other than `""` or `"1"` MUST return an error.
2. `ColumnSpec.UnmarshalYAML(value *yaml.Node)` and `ParamSpec.UnmarshalYAML(value *yaml.Node)` MUST inspect `*yaml.Node` directly (never calling `value.Decode` into a struct, which drops `KnownFields(true)`) and support three notations:
   - Verbose mapping: `{ name: ID, type: int64 }` (and optional `slice: true` on `ParamSpec`). Any unknown mapping key MUST fail with `fmt.Errorf("line %d: unknown field %q", kNode.Line, kNode.Value)`.
   - Single-key shorthand mapping: `- ID: int64` (or `- org_ids...: int64`).
   - Compact scalar string: `- "ID:int64"` (or `- "org_ids...:int64"`).
3. **Strict Unused-Import Prevention (`resolveImports`)**:
   - `context` and `database/sql` are imported when at least one non-`subquery` statement exists (or in `Queries` methods).
   - `sync` is imported ONLY if at least one statement emits a `<tupleStruct>ChunkPool` (`CanStream == true`).
   - `sync/atomic` is imported ONLY if at least one statement emits a static `Fast` atomic pointer slot (`DynamicSlots == 0 && Kind != KindSubquery`).
   - `iter` is imported ONLY if at least one statement has `Kind == KindMany` or `CanStream == true`.
   - `slices` is imported ONLY if at least one 1D slice parameter or tuple field has `HasSort == true`.
   - Standard library packages (`time`, `encoding/json`, `math/big`, `net/netip`, `net/url`, `database/sql/driver`) are auto-imported ONLY when their package selector (`time.`, `json.`, etc.) appears in a parameter, tuple field, or output column type.
4. `GenerateFromYAMLFile` MUST write the formatted output file atomically via `os.CreateTemp` in the target directory, `Chmod(0644)`, `Close()`, and `os.Rename` (cleaning up the temp file via `os.Remove` on any error).

### 3.3. CLI Binary Contract (`cmd/sqlgen/main.go`)
`cmd/sqlgen` MUST expose a testable `run(args []string, stdout, stderr io.Writer) int` function called by `main()`:
- **Flags**:
  - `-config <path>`: Path to YAML configuration file (default `"sqlgen.yaml"`). Also accepts a single positional argument `<path>` if `-config` is not explicitly passed.
  - `-out <path>`: Overrides the `out` path in `sqlgen.yaml`.
  - `-stdout`: Writes generated Go code to `stdout` instead of disk.
  - `-check`: CI verification mode. Generates the Go code in memory and compares it byte-for-byte against the existing output file on disk without modifying disk files. If the file is missing or differs, prints an actionable diff/drift message to `stderr` and exits with code `2`.
  - `-version`: Prints `"sqlgen v1.0.0\n"` to `stdout` and exits with code `0`.
- **Exit Codes**:
  - `0`: Success.
  - `1`: Configuration, file I/O, SQL parsing, or validation error.
  - `2`: Invalid CLI flag usage or `-check` drift detected.

---

## 4. Formal SQL DSL Grammar & Lexical Scanner (`generator.go`)

### 4.1. Protected Lexical Contexts (Copied Verbatim)
In `scanSpan`, before recognizing any `sqlgen` annotation or keyword, the scanner MUST check for and copy the following constructs verbatim into the current SQL segment:
1. **Single-Quoted String Literals**: `'...'` (handling `''` escapes). Unclosed `'...'` MUST return a compile-time error.
2. **Double-Quoted Identifiers**: `"..."` (handling `""` escapes). Unclosed `"..."` MUST return a compile-time error.
3. **Backtick-Quoted Identifiers**: `` `...` `` (handling `` `` `` escapes). Unclosed `` `...` `` MUST return a compile-time error.
4. **PostgreSQL Dollar-Quoted String Literals**: Begins at `$` when `i+1 < n` and either `span[i+1] == '$'` (`$$...$$`) or `isIdentStart(span[i+1])` followed by zero or more `isIdentChar` and a closing `$` (`$tag$...$tag$`). Because `isIdentStart` excludes digits `'0'..'9'`, positional parameters like `$1` are never matched. Copy verbatim through the matching closing `$tag$`; unclosed dollar quotes MUST return a compile-time error.
5. **Single-Line Comments**: `--` through `\n` or `EOF`.
6. **Multi-Line Block Comments**: `/*` through `*/`. Unclosed block comments MUST return a compile-time error.
7. **Square-Bracketed Expressions**: `[` through matching `]`, tracked via `bracketDepth` (e.g., PostgreSQL array slice `arr[1:3]`). Unclosed `[` (`bracketDepth != 0` at end of span) MUST return a compile-time error.
8. **Two-Character Colon Operators**: `::` (PostgreSQL typecast) and `:=` (assignment) consumed atomically (2 bytes).

### 4.2. Formal EBNF Grammar (Outside Protected Contexts)
```ebnf
sql_statement      = { top_level_element } ;
top_level_element  = protected_token | conditional_block | annotated_token | raw_sql_char ;

conditional_block  = "{?" , identifier , ":" , { inner_element } , "}" ;
inner_element      = protected_token | annotated_token | raw_sql_char ;

annotated_token    = tuple_slice_param
                   | order_directive
                   | seek_directive
                   | query_param
                   | typed_token
                   | yaml_ref_param ;

tuple_slice_param  = [ ":" ] , identifier , ":" , "[]" , go_ident , "(" , tuple_body , ")" ;
tuple_body         = { protected_token | tuple_field | "(" , tuple_body , ")" | raw_sql_char } ;
tuple_field        = identifier , ":" , scalar_go_type , [ typecast_suffix ] , [ sort_dir ] ;
typecast_suffix    = "::" , identifier , [ "(" , { digit | "," | " " } , ")" ] ;

order_directive    = [ ":" ] , identifier , ":" , ( "sqlgen.Order" | "runtime.Order" ) , "(" , order_col_list , ")" ;
order_col_list     = sql_col_ref , { "," , sql_col_ref } ;
sql_col_ref        = [ identifier , "." ] , identifier ;

seek_directive     = [ ":" ] , identifier , ":" , ( "sqlgen.Seek" | "runtime.Seek" ) ;
query_param        = [ ":" ] , identifier , ":" , ( "sqlgen.Query" | "runtime.Query" | "Query" ) ;
typed_token        = [ ":" ] , identifier , [ "..." ] , ":" , go_type , [ "..." ] , [ sort_dir ] ;
yaml_ref_param     = ":" , identifier , [ "..." ] , [ sort_dir ] ;

sort_dir           = ( "ASC" | "asc" | "DESC" | "desc" ) ;
identifier         = ( letter | "_" ) , { letter | digit | "_" } ;
go_ident           = identifier ;
go_type            = { "[]" | "*" } , [ identifier , "." ] , identifier ;
scalar_go_type     = [ "[]" | "*" ] , [ identifier , "." ] , identifier ;
```

### 4.3. Context-Sensitive Disambiguation (`SELECT` / `RETURNING` vs. Input Parameter)
1. **Projection & Statement Verb State Tracking**:
   At `parenDepth == 0 && bracketDepth == 0`, when scanning an unquoted SQL word `W` (no leading `:`, not followed by `:`):
   - Track the primary statement verb `stmtVerb` (`VerbSelect`, `VerbInsert`, `VerbUpdate`, `VerbDelete`, `VerbOther`): the first top-level keyword that is `select`, `insert`, `update`, or `delete` sets `stmtVerb` (ignoring leading `WITH cte AS (...)` headers, where the CTE body is inside `parenDepth >= 1`).
   - If `lower(W) == "select"` and `stmtVerb == VerbSelect` and `!seenMainSel`: set `inProjection = true`, `seenMainSel = true`. *(Crucial: if `stmtVerb` is `VerbInsert`, e.g., `INSERT INTO t SELECT ... RETURNING ...`, `SELECT` MUST NOT set `inProjection = true`; only `RETURNING` is the output projection of an `INSERT`/`UPDATE`/`DELETE`.)*
   - If `lower(W) == "returning"`: set `inProjection = true`, `hasReturning = true`.
   - If `inProjection == true` and `lower(W)` is in the **Projection Terminator Set** `{"from", "into", "where", "group", "having", "window", "order", "limit", "offset", "fetch", "for", "union", "intersect", "except"}`: set `inProjection = false`.
2. **Classification Rule**:
   A `typed_token` ending at offset `pos` is classified as an **Output Column** if and only if ALL 6 conditions hold:
   - No leading `:` before `identifier`.
   - No `...` before or after `go_type`.
   - No trailing `ASC` or `DESC` modifier.
   - `go_type` is not `Query`, `Seek`, or `Order`.
   - `inProjection == true`, `parenDepth == 0`, and `bracketDepth == 0`.
   - **`isEndOfProjectionItem(span, pos)` returns `true`**:
     Starting at `pos`, loop to skip whitespace, `--` comments, `/*...*/` comments, and **any `{?Flag: ...}` conditional blocks** (scanning balanced `{...}` while respecting `'...'` strings). After skipping those, return `true` if and only if `i >= len(span)`, `span[i] == ','`, `span[i] == ';'`, or `span[i]` starts an unannotated identifier word `W` whose lowercase form is in the **Projection Terminator Set**.
   - Otherwise, classify the `typed_token` as an **Input Parameter**.

### 4.4. Unconditional `SlotSelectCols` Splicing (Single-Pass Safe)
Whenever a statement has `len(UncondCols) > 1` and `len(spec.Columns) == 0` (inline output columns in `SELECT` or `RETURNING`), **regardless of whether `UseParamsStruct` is already true during `SELECT` scanning**:
1. Each unconditional output column records its full trimmed SQL projection expression in `QueryDescriptor.SelectColExprs` (e.g., `"u.id"`, `"COALESCE(o.display_name, o.slug) AS org_name"`).
2. In the top-level `QueryDescriptor`, the entire span of unconditional column expressions (from after the `SELECT ` / `RETURNING ` whitespace to the end of the last unconditional column) is replaced by a single `SlotSelectCols` slot in `QueryDescriptor.Slots`.
3. Any conditional output columns (`{?WithOrg: , o.name AS org_name:string}`) remain as `SlotOptBlock` occurrences immediately following `SlotSelectCols`.
4. When `UseParamsStruct == false` (or `p.SelectCols == 0`), `in.ColMask` is `0`, which `SlotSelectCols` renders as all unconditional columns (`effectiveMask = validMask`). When `DynamicSlots == 0`, `StaticSQL` also expands `SlotSelectCols` with `ColMask == 0`. This guarantees that discovering a `{?Flag: ...}` block later in `WHERE` never invalidates the already-scanned `SELECT` clause.

### 4.5. Inline Branch-Guard Slots (`SlotBranchGuard*` & `SlotInsertValues*`)
To allow any statement inside a pipelined `p.If(...)` branch to be gated on the server in a **single streaming pass** with **one `strings.Builder`** across both PostgreSQL and SQLite:
1. **`SELECT` Statements (`desc.StmtVerb == VerbSelect`)**:
   - Handled directly in `streamOpSQL`: when `len(in.BranchGates) > 0`, `streamOpSQL` writes `"SELECT * FROM ("` before streaming `desc` and writes `") AS _sqlgen_if WHERE "` + `<branch_gates>` after streaming `desc`.
2. **`INSERT` Statements (`desc.StmtVerb == VerbInsert`)**:
   - **Subcase 2A: Single-Row `INSERT INTO tbl (c1, c2) VALUES (e1, e2)`** (where `VALUES` is followed by `(`):
     - Replace `VALUES (` (including the opening parenthesis) with `SlotInsertValuesOpen`, and replace the matching closing `)` of the `VALUES (...)` tuple with `SlotInsertValuesClose`.
     - When `len(in.BranchGates) == 0`: `SlotInsertValuesOpen` writes `"VALUES ("` and `SlotInsertValuesClose` writes `")"`.
     - When `len(in.BranchGates) > 0`: `SlotInsertValuesOpen` writes `"SELECT "` and `SlotInsertValuesClose` writes `" FROM (SELECT 1) AS _sqlgen_dual WHERE "` + `<branch_gates>`.
     - *(Emitting `INSERT INTO tbl (c1, c2) SELECT $1, $2 FROM (SELECT 1) AS _sqlgen_dual WHERE <gates> [ON CONFLICT ...] [RETURNING ...]` is 100% valid syntax in both PostgreSQL and SQLite and preserves PostgreSQL's target-column type inference for `$1, $2`.)*
   - **Subcase 2B: Multi-Row Tuple Slice `INSERT INTO tbl (c1, c2) VALUES Rows:[]Tuple(...)`** (where `VALUES` is followed by a `SlotTupleSlice` token):
     - Replace the `VALUES` keyword with `SlotInsertTupleValuesOpen`, and place `SlotInsertTupleValuesClose` immediately after the `SlotTupleSlice` occurrence (before `ON CONFLICT`, `RETURNING`, `;`, or `EOF`).
     - When `len(in.BranchGates) == 0`: `SlotInsertTupleValuesOpen` writes `"VALUES "` and `SlotInsertTupleValuesClose` writes `""`.
     - When `len(in.BranchGates) > 0`: `SlotInsertTupleValuesOpen` writes `"WITH _sqlgen_v AS (VALUES "` and `SlotInsertTupleValuesClose` writes `") SELECT * FROM _sqlgen_v WHERE "` + `<branch_gates>`.
3. **`UPDATE` and `DELETE` Statements (`desc.StmtVerb == VerbUpdate` or `VerbDelete`)**:
   - In `scanSpan` at `parenDepth == 0`:
     - If a top-level `WHERE` keyword is present: emit `SlotBranchGuardOpen` immediately after `"WHERE "`, and emit `SlotBranchGuardClose` immediately before top-level `RETURNING`, `;`, or `EOF`.
       - When `len(in.BranchGates) == 0`: both write `""`.
       - When `len(in.BranchGates) > 0`: `SlotBranchGuardOpen` writes `"("` + `<branch_gates>` + `") AND ("`, and `SlotBranchGuardClose` writes `")"`.
     - If no top-level `WHERE` keyword is present: emit `SlotBranchGuardWhere` immediately before top-level `RETURNING`, `;`, or `EOF`.
       - When `len(in.BranchGates) == 0`: writes `""`.
       - When `len(in.BranchGates) > 0`: writes `" WHERE "` + `<branch_gates>`.

### 4.6. Parameter Deduplication, Presort Modifiers (`ASC` / `DESC`) & Diagnostics
1. **Parameter Deduplication Invariant**:
   - Within a single statement, input parameters are deduplicated by `canonicalKey = strings.ToLower(toGoParamName(rawName))`.
   - If `OrgID:int64` and `org_id:int64` both appear in the same statement, they resolve to a **single** Go function parameter (`orgID int64`) or `<Name>Params` struct field (`OrgID int64`), while binding separate positional SQL placeholders (`$1`, `$3`) at each occurrence in `Occurrences`.
   - If the same `canonicalKey` is redeclared in the same statement with conflicting Go types, slot kinds, or sort directions, `parseSQL` MUST fail with a compile-time error.
2. **Presort Modifiers (`ASC` / `DESC`)**:
   - After a `SlotSlice` token (`IDs:[]int64`) or a tuple field (`ID:int64` or `ID:int64::bigint`), look ahead across horizontal whitespace (`' '`, `'\t'`). If the next word is `ASC`, `asc`, `DESC`, or `desc` (followed by whitespace, `,`, `)`, `\n`, or `EOF`), consume it and set `HasSort = true, Desc = (lower == "desc")`.
   - Validate that the element/field Go type is orderable (`int*`, `uint*`, `float*`, `uintptr`, `byte`, `rune`, `string`, `time.Time`, `[]byte`, `[]uint8`); otherwise return a compile-time error.
3. **Exact `CanBatch` and `CanStream` Predicates**:
   - Let `collectionOccs := ps.SliceOccs + ps.TupleOccs`.
   - `CanBatch` is `true` if and only if:
     `(ps.Spec.Kind == KindExec || ps.Spec.Kind == KindMany) && collectionOccs == 1 && collectionParam.FlagBit == 0 && ps.QueryOccs == 0 && !ps.HasOrder && !ps.HasSeek`.
   - `CanStream` is `true` if and only if:
     `ps.CanBatch && ps.TupleOccs == 1`.

---

## 5. Core Runtime Primitives, `VariantKey`, `Order`/`Seek` & Pools (`runtime/runtime.go`)

```go
package runtime

import (
	"bytes"
	"database/sql"
	"errors"
	"iter"
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Dialect uint8

const (
	DialectQuestion Dialect = iota
	DialectDollar
)

type StmtVerb uint8

const (
	VerbOther StmtVerb = iota
	VerbSelect
	VerbInsert
	VerbUpdate
	VerbDelete
)

type SlotKind uint8

const (
	SlotScalar                 SlotKind = iota // ? or $N
	SlotSlice                                  // ?, ? or $N, $N+1
	SlotTupleSlice                             // (?, ?), (?, ?)
	SlotQuery                                  // Composed runtime.Query
	SlotSelectCols                             // Dynamic SELECT column projection (ColMask)
	SlotOptBlock                               // Conditional block {?Flag: ...} (FlagMask)
	SlotSeek                                   // Keyset pagination WHERE predicate
	SlotOrder                                  // Custom multi-column ORDER BY
	SlotBranchGuardOpen                        // "(" + gates + ") AND (" after WHERE in UPDATE/DELETE
	SlotBranchGuardClose                       // ")" at end of WHERE in UPDATE/DELETE
	SlotBranchGuardWhere                       // " WHERE " + gates in UPDATE/DELETE without WHERE
	SlotInsertValuesOpen                       // "VALUES (" or "SELECT "
	SlotInsertValuesClose                      // ")" or " FROM (SELECT 1) AS _sqlgen_dual WHERE " + gates
	SlotInsertTupleValuesOpen                  // "VALUES " or "WITH _sqlgen_v AS (VALUES "
	SlotInsertTupleValuesClose                 // "" or ") SELECT * FROM _sqlgen_v WHERE " + gates
)

const (
	DefaultMaxPreparedStmts = 256
	DefaultMaxSQLVariants   = 4096
	DefaultMaxPartialBytes  = 16 << 20 // 16 MB
	DefaultPromoteThreshold = 2
	DefaultMaxBatchParams   = 16384
	MaxPowerOfTwoChunk      = 32768
	InexactKeyBit           = uint64(1) << 63
)

var (
	ErrEmptySliceParam    = errors.New("sqlgen: slice parameter cannot be empty")
	ErrUninitializedQuery = errors.New("sqlgen: curried Query argument is uninitialized")
	ErrAssertionFailed    = errors.New("sqlgen: pipeline row-count assertion failed")
	ErrBranchNotTaken     = errors.New("sqlgen: pipeline if/else branch was not taken")
)
```

### 5.1. 256-Bit `VariantKey` & Bijective `ComputeSubqueryShape`
```go
// VariantKey is a 256-bit comparable value type that uniquely identifies a query variation
// across StmtID, ColMask, FlagMask, OrderKey, and up to 4 slice/tuple/subquery dimensions.
type VariantKey [4]uint64

func MakeVariantKey(stmtID uint32, colMask, flagMask, orderKey uint64, dims ...uint64) VariantKey {
	inexact := stmtID == 0 || stmtID > 0x7FFF || colMask > 0xFFFFFF || flagMask > 0xFFFFFF || len(dims) > 4
	for _, d := range dims {
		if (d & InexactKeyBit) != 0 {
			inexact = true
			break
		}
	}
	if !inexact {
		canPackDims := true
		var w2, w3 uint64
		switch len(dims) {
		case 0:
		case 1:
			w2 = dims[0]
		case 2:
			w2 = dims[0]
			w3 = dims[1]
		case 3:
			if dims[0] <= 0xFFFFFFFF && dims[1] <= 0xFFFFFFFF && dims[2] <= 0xFFFFFFFF {
				w2 = (dims[0] << 32) | dims[1]
				w3 = dims[2]
			} else {
				canPackDims = false
			}
		case 4:
			if dims[0] <= 0xFFFFFFFF && dims[1] <= 0xFFFFFFFF && dims[2] <= 0xFFFFFFFF && dims[3] <= 0xFFFFFFFF {
				w2 = (dims[0] << 32) | dims[1]
				w3 = (dims[2] << 32) | dims[3]
			} else {
				canPackDims = false
			}
		}
		if canPackDims {
			// Encode len(dims) in bits 47..45 so (dims=[0]) and (dims=[]) or (dims=[1,0]) are bijective:
			// Word 0: [Bit 63: 0][Bits 62..48: stmtID (15b)][Bits 47..45: len(dims) (3b)][Bits 44..22: colMask (23b)][Bits 21..0: flagMask (22b)]
			if colMask <= 0x7FFFFF && flagMask <= 0x3FFFFF {
				w0 := (uint64(stmtID) << 48) | (uint64(len(dims)) << 45) | ((colMask & 0x7FFFFF) << 22) | (flagMask & 0x3FFFFF)
				return VariantKey{w0, orderKey, w2, w3}
			}
		}
	}
	return VariantKey{InexactKeyBit | uint64(stmtID), colMask ^ flagMask, orderKey, uint64(len(dims))}
}

func (k VariantKey) IsExact() bool { return (k[0] & InexactKeyBit) == 0 }

func (k VariantKey) hash() uint64 {
	h := k[0]*0x9e3779b97f4a7c15 + k[1]*0xbf58476d1ce4e5b9 + k[2]*0x94d049bb133111eb + k[3]
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	return h ^ (h >> 31)
}

// ComputeSubqueryShape packs a subquery's coordinates bijectively into 63 bits (InexactKeyBit == 0)
// when all fields fit, or sets InexactKeyBit so the outer query falls back to exact SQL string keying.
func ComputeSubqueryShape(stmtID uint32, colMask, flagMask, orderKey uint64, hasNestedSubs bool, sliceOrTupleLens ...int) uint64 {
	if !hasNestedSubs && orderKey == 0 && stmtID > 0 && stmtID <= 0x3FFF &&
		colMask <= 0xFF && flagMask <= 0xFF && len(sliceOrTupleLens) <= 2 {
		var s0, s1 uint64
		fits := true
		if len(sliceOrTupleLens) >= 1 {
			if sliceOrTupleLens[0] < 0 || sliceOrTupleLens[0] > 0xFFFF {
				fits = false
			}
			s0 = uint64(sliceOrTupleLens[0])
		}
		if len(sliceOrTupleLens) == 2 {
			if sliceOrTupleLens[1] < 0 || sliceOrTupleLens[1] > 0xFFFF {
				fits = false
			}
			s1 = uint64(sliceOrTupleLens[1])
		}
		if fits {
			return (uint64(stmtID) << 49) | (uint64(len(sliceOrTupleLens)) << 47) |
				((colMask & 0x7F) << 40) | ((flagMask & 0xFF) << 32) | (s0 << 16) | s1
		}
	}
	vk := MakeVariantKey(stmtID, colMask, flagMask, orderKey)
	return InexactKeyBit | (vk.hash() & ^InexactKeyBit)
}
```

### 5.2. Complete Keyset Pagination (`Order` & `Seek`) Algorithms
```go
// PackOrder packs up to 7 sort column terms (each encoded as ((colIdx1Based) << 1) | descBit)
// into a single collision-free uint64 with zero heap allocations. Duplicate columns are ignored.
func PackOrder(terms ...uint8) uint64 {
	var (
		packed uint64
		seen   uint64
		shift  uint
	)
	for _, t := range terms {
		col1 := t >> 1
		if col1 == 0 || col1 > 63 {
			continue
		}
		bit := uint64(1) << col1
		if (seen & bit) != 0 {
			continue
		}
		seen |= bit
		packed |= uint64(t) << shift
		shift += 8
		if shift >= 56 {
			break
		}
	}
	return packed
}

// ExpandOrder unpacks a packed uint64 order specification into column indices (0..numCols-1)
// and direction flags (true == DESC), automatically appending the tie-breaker column (numCols-1)
// if not already present (inheriting the preceding sort column's direction so single-direction
// DESC sorts use uniform tuple comparison).
func ExpandOrder(orderSpec uint64, numCols int) (cols [8]int, desc [8]bool, count int, allSameDir bool) {
	if numCols <= 0 {
		return cols, desc, 0, true
	}
	var seen uint64
	for shift := uint(0); shift < 56; shift += 8 {
		t := uint8((orderSpec >> shift) & 0xFF)
		if t == 0 {
			break
		}
		colIdx := int(t>>1) - 1
		if colIdx < 0 || colIdx >= numCols {
			continue
		}
		bit := uint64(1) << uint(colIdx)
		if (seen & bit) != 0 {
			continue
		}
		seen |= bit
		cols[count] = colIdx
		desc[count] = (t & 1) != 0
		count++
		if count == 7 {
			break
		}
	}
	tieIdx := numCols - 1
	tieBit := uint64(1) << uint(tieIdx)
	if (seen & tieBit) == 0 {
		cols[count] = tieIdx
		if count > 0 {
			desc[count] = desc[count-1]
		} else {
			desc[count] = false
		}
		count++
	}
	allSameDir = true
	for i := 1; i < count; i++ {
		if desc[i] != desc[0] {
			allSameDir = false
			break
		}
	}
	return cols, desc, count, allSameDir
}

// SeekArgCount returns the number of positional parameters consumed by SlotSeek.
func SeekArgCount(dialect Dialect, orderSpec uint64, numCols int, hasSeek bool) int {
	if !hasSeek || numCols <= 0 {
		return 0
	}
	_, _, count, allSameDir := ExpandOrder(orderSpec, numCols)
	if dialect == DialectDollar || count == 1 || allSameDir {
		return count
	}
	// DialectQuestion (?) with mixed ASC/DESC directions expands to:
	// 1 bounding-box param + count*(count+1)/2 lexicographical branch params.
	return 1 + (count*(count+1))/2
}

// AppendSeekArgs appends cursor column values into dst in the exact order expected by formatSeekSQL.
func AppendSeekArgs(dst *[]any, dialect Dialect, orderSpec uint64, numCols int, colValues []any) {
	cols, _, count, allSameDir := ExpandOrder(orderSpec, numCols)
	if dialect == DialectDollar || count == 1 || allSameDir {
		for i := 0; i < count; i++ {
			*dst = append(*dst, colValues[cols[i]])
		}
		return
	}
	*dst = append(*dst, colValues[cols[0]])
	for i := 0; i < count; i++ {
		for j := 0; j <= i; j++ {
			*dst = append(*dst, colValues[cols[j]])
		}
	}
}

func formatOrderSQL(b *strings.Builder, orderCols []string, orderSpec uint64) {
	cols, desc, count, _ := ExpandOrder(orderSpec, len(orderCols))
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(orderCols[cols[i]])
		if desc[i] {
			b.WriteString(" DESC")
		} else {
			b.WriteString(" ASC")
		}
	}
}

func formatSeekSQL(b *strings.Builder, dialect Dialect, paramIndex *int, numBuf *[20]byte, orderCols []string, orderSpec uint64, hasSeek bool) {
	if !hasSeek || len(orderCols) == 0 {
		b.WriteString("TRUE")
		return
	}
	cols, desc, count, allSameDir := ExpandOrder(orderSpec, len(orderCols))
	writeParam := func(idx int) {
		if dialect == DialectQuestion {
			b.WriteByte('?')
		} else {
			b.WriteByte('$')
			b.Write(strconv.AppendInt((*numBuf)[:0], int64(idx), 10))
		}
	}
	if count == 1 {
		b.WriteString(orderCols[cols[0]])
		if desc[0] {
			b.WriteString(" < ")
		} else {
			b.WriteString(" > ")
		}
		writeParam(*paramIndex)
		*paramIndex++
		return
	}
	if allSameDir {
		b.WriteByte('(')
		for i := 0; i < count; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(orderCols[cols[i]])
		}
		if desc[0] {
			b.WriteString(") < (")
		} else {
			b.WriteString(") > (")
		}
		for i := 0; i < count; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			writeParam(*paramIndex)
			*paramIndex++
		}
		b.WriteByte(')')
		return
	}
	// Mixed ASC/DESC directions: emit index-friendly bounding-box + lexicographical OR branches
	baseParam := *paramIndex
	if dialect == DialectDollar {
		*paramIndex += count
	}
	nextColParam := func(colPos int) int {
		if dialect == DialectDollar {
			return baseParam + colPos
		}
		p := *paramIndex
		*paramIndex++
		return p
	}
	b.WriteByte('(')
	b.WriteString(orderCols[cols[0]])
	if desc[0] {
		b.WriteString(" <= ")
	} else {
		b.WriteString(" >= ")
	}
	writeParam(nextColParam(0))
	b.WriteString(" AND (")
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteString(" OR (")
		}
		for j := 0; j < i; j++ {
			b.WriteString(orderCols[cols[j]])
			b.WriteString(" = ")
			writeParam(nextColParam(j))
			b.WriteString(" AND ")
		}
		b.WriteString(orderCols[cols[i]])
		if desc[i] {
			b.WriteString(" < ")
		} else {
			b.WriteString(" > ")
		}
		writeParam(nextColParam(i))
		if i > 0 {
			b.WriteByte(')')
		}
	}
	b.WriteString("))")
}
```

---

## 6. Single-`strings.Builder` Streaming SQL Renderer (`runtime/sql_stream.go`)

### 6.1. Dialect-Agnostic Condition & Assertion SQL Generation
1. **`CondColPred` (`WhereCol`)**:
   Stream `"COUNT(*) > 0 AND MIN(CASE WHEN ("` + `c.SQLPred` + `") THEN 1 ELSE 0 END) = 1"`. (Valid ANSI SQL across PostgreSQL, CockroachDB, SQLite, and MySQL without requiring `BOOL_AND`).
2. **`_sqlgen_cond` CTE**:
   Stream `", _sqlgen_cond AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.c"` + `condID` + `"', CASE WHEN ("` + `cond` + `") THEN 'true' ELSE 'false' END, true) = 'true' AS _ok FROM _sqlgen_op)"`.
3. **`_sqlgen_chk` CTE & `IsAbort` Overflow Expression**:
   - In `DialectDollar`: `"ABS(-9223372036854775808::bigint)"`
   - In `DialectQuestion`: `"ABS(-9223372036854775808)"`

### 6.2. Provable Upper-Bound Byte Estimator (`EstimateOpBytes`)
`EstimateOpBytes(desc *QueryDescriptor, in *RenderInput, dec *OpDecorators) int` MUST compute a strict upper bound (`EstimateOpBytes >= len(renderedSQL)`):
- Per `BranchGate`: `56` bytes.
- Per `SlotScalar`: `12` bytes.
- Per `SlotSlice` of length $L$: `12 * L` bytes.
- Per `SlotTupleSlice` of length $R$ with tuple segments $S_0 \dots S_F$: `R * (4 + sum(len(S_j) + 12))` bytes.
- Per `SlotSelectCols`: `sum(len(expr) + 2)` across all `desc.SelectColExprs`.
- Per `SlotOrder`: `sum(len(col) + 6)` across all `desc.OrderCols`.
- Per `SlotSeek`: if `!in.HasSeek`, `4` bytes; if `in.HasSeek` with $K = \text{len}(\text{desc.OrderCols})$ and `maxColLen = max(len(col))`, `(1 + K*(K+1)/2) * (maxColLen + 24) + 32` bytes.
- Per `SlotInsertValuesOpen` / `Close` and `SlotInsertTupleValuesOpen` / `Close`: when `len(in.BranchGates) > 0`, `96 + 56 * len(in.BranchGates)` bytes; when `len(in.BranchGates) == 0`, `16` bytes.
- Per `SlotBranchGuardOpen` / `Close` / `Where` when `len(in.BranchGates) > 0`: `16 + 56 * len(in.BranchGates)` bytes.
- For `VerbSelect` wrapper when `len(in.BranchGates) > 0`: `48 + 56 * len(in.BranchGates)` bytes.
- For `dec.AssertRows >= 0`: `220` bytes.
- For `dec.ProduceCond != nil`: `260 + dec.ProduceCond.estimateBytes()` bytes.

---

## 7. 16-Shard Two-Tier W-TinyLFU + SLRU Statement Cache (`runtime/cache.go`)

### 7.1. 4-Bit `tinyLFUSketch` Implementation
```go
type tinyLFUSketch struct {
	table        [256]uint64 // 256 * 16 nibbles = 4,096 4-bit counters (2 KB inline)
	additions    uint32
	sampleWindow uint32
}

func (s *tinyLFUSketch) init(sampleWindow uint32) {
	if sampleWindow < 512 {
		sampleWindow = 4096
	}
	s.sampleWindow = sampleWindow
}

func (s *tinyLFUSketch) increment(h uint64) uint8 {
	s.additions++
	if s.additions >= s.sampleWindow {
		for i := range s.table {
			s.table[i] = (s.table[i] >> 1) & 0x7777777777777777
		}
		s.additions = 0
	}
	var minVal uint8 = 15
	for i := uint(0); i < 4; i++ {
		idx := ((h >> (i * 12)) ^ (h * (uint64(i)*0x517cc1b727220a95 + 1))) & 4095
		wordIdx := idx >> 4
		shift := (idx & 15) << 2
		val := uint8((s.table[wordIdx] >> shift) & 0xF)
		if val < 15 {
			val++
			s.table[wordIdx] = (s.table[wordIdx] & ^(uint64(0xF) << shift)) | (uint64(val) << shift)
		}
		if val < minVal {
			minVal = val
		}
	}
	return minVal
}

func (s *tinyLFUSketch) estimate(h uint64) uint8 {
	var minVal uint8 = 15
	for i := uint(0); i < 4; i++ {
		idx := ((h >> (i * 12)) ^ (h * (uint64(i)*0x517cc1b727220a95 + 1))) & 4095
		wordIdx := idx >> 4
		shift := (idx & 15) << 2
		val := uint8((s.table[wordIdx] >> shift) & 0xF)
		if val < minVal {
			minVal = val
		}
	}
	return minVal
}
```

### 7.2. Strict `partialBytes` Accounting Invariant & Singleflight Promotion
1. **`partialBytes` Accounting Invariant**:
   For every shard, `shard.partialBytes` MUST equal the exact sum of `len(e.sqlStr)` across all entries currently residing in `shard.partialProb` or `shard.partialProt`:
   $$\text{shard.partialBytes} \equiv \sum_{e \in \text{partialProb} \cup \text{partialProt}} \text{len}(e.\text{sqlStr})$$
   - Linking an entry into `partialProb` or `partialProt` (on Tier-1 admission or Tier-2 $\rightarrow$ Tier-1 demotion) adds `len(e.sqlStr)` to `shard.partialBytes`.
   - Unlinking an entry from `partialProb` or `partialProt` (on Tier-1 $\rightarrow$ Tier-2 promotion or Tier-1 eviction) subtracts `len(e.sqlStr)` from `shard.partialBytes`.
   - Moving an entry between `partialProb` and `partialProt` leaves `shard.partialBytes` unchanged.
2. **Singleflight Promotion (`entry.preparing`)**:
   - When a Tier-1 entry qualifies for promotion to Tier 2 (`freq >= promoteThreshold` and beats `preparedVictim`), the first goroutine sets `entry.preparing = true` under `shard.mu` before unlocking to call `db.PrepareContext(ctx, cachedSQL)`.
   - Any concurrent goroutine hitting the same `entry` while `entry.preparing == true` does NOT call `db.PrepareContext`, increments `partialHits`, and executes `cachedSQL` immediately without blocking.
   - When `promoteThreshold == 1` (prepare on first sight), a cache miss inserts `entry` into `byKey`/`bySQL` and `partialProb` with `entry.preparing = true` under `shard.mu` *before* calling `db.PrepareContext(ctx, sqlStr)` so concurrent misses on the same cold key never duplicate `PrepareContext` calls.
3. **`ManagedStmt` Reference-Counted Lifecycle**:
   - `newManagedStmt(stmt, 2)` initializes `refs = 2` when promoted (`1` for Tier-2 residency + `1` for the active caller).
   - `StmtCache.Query` returns `(*sql.Rows, *ManagedStmt, error)`. If `QueryContext` returns an error, `StmtCache.Query` calls `ms.Release()` immediately. Otherwise, the caller closes `rows` first and then calls `if ms != nil { _ = ms.Release() }`.
   - `TxStmtCache` holds an internal `*txState` with `heldMS []*ManagedStmt`. Calling `StmtFromManaged(ctx, ms)` acquires `+1` ref on `ms` on first bind in that transaction. All held refs are released deterministically in `txCache.Close()` AND via `runtime.AddCleanup(tc, (*txState).releaseAll, state)`.

---

## 8. Single-Roundtrip `Pipeline`, `PipelineTx`, `If/Then/Else` & `TxRetryPolicy` (`runtime/pipeline.go`)

1. **Wire-Pipelined Mode (`WirePipeliner`)**:
   - Detected via `conn.Raw` (when `txCache == nil`) or via `WrapConnector` sentinel execution `tx.ExecContext(withPipelineOps(ctx, ops), "--sqlgen:pipeline")` (when `txCache != nil`).
   - Calls `RenderPipelineBatch(dialect, cache, ops)` to populate `ops[i].SQL` using at most 1 `strings.Builder`.
   - Translates any integer-overflow error (`SQLSTATE 22003` or `"integer overflow"` / `"bigint out of range"`) on an operation with `Decorators.IsAbort == true` into `op.AbortErr`, and on an operation with `Decorators.AssertRows >= 0` into `runtime.ErrAssertionFailed`.
2. **Sequential Fallback Mode (`*sql.DB` / `*sql.Tx`)**:
   - Executes operations sequentially through `StmtCache` (using Tier-2 prepared statements).
   - Evaluates `BranchGates` in Go via `cond.EvalGoFn()`: if any gate is false, calls `op.OnSkipped()` (`future.skipped = true`) and skips database execution for that operation.
   - If an active `IsAbort` operation is reached, returns `op.AbortErr` (triggering `tx.Rollback()`).
   - If an operation has `Decorators.AssertRows >= 0` and the actual affected/returned row count does not equal `AssertRows`, sets `op.Err = runtime.ErrAssertionFailed` and returns `runtime.ErrAssertionFailed` (triggering `tx.Rollback()`).
3. **`IsRetryableTxError(err error, duringCommit bool) bool`**:
   - Returns `false` if `err == nil`, `errors.Is(err, context.Canceled)`, `errors.Is(err, context.DeadlineExceeded)`, or `errors.Is(err, ErrAssertionFailed)`.
   - If `duringCommit == true` and `err` is a transport/connection drop (`errors.Is(err, driver.ErrBadConn)`, `errors.Is(err, io.EOF)`, `errors.Is(err, io.ErrUnexpectedEOF)`, or `net.Error`), returns `false`.
   - Returns `true` for SQLSTATE `"40001"`, `"40P01"`, `"55P03"`, MySQL codes `1213` / `1205`, SQLite codes `(code & 0xFF) == 5` or `6`, and matching error substrings.

---

## 9. Code Generator Specification (`generator.go`)

For each statement `<Name>` in `sqlgen.yaml`, `Generate` emits into a single `bytes.Buffer`:
1. Tuple structs (`<TupleStruct>`), `<tupleStruct>ChunkPool`, `compare<TupleStruct>`, and `is<TupleStruct>Sorted` (when `HasSort == true`).
2. 1D slice presort helpers `is<Type>AscSorted` / `is<Type>DescSorted` (when `HasSort == true`).
3. Output row struct (`<RowType>`), `<RowType>Col` bitmask constants (`1 << iota`), and `<RowType>Order` / `<RowType>SortCol` / `Order<RowType>(...)` helpers.
4. `<Name>Params` struct (when `UseParamsStruct == true`).
5. Static `QueryDescriptor` (`<name>Desc`) and non-capturing `expand<Name>Args` function.
6. Currying constructors: `func <Name>Query(...) Query` and `func (q *Queries) <Name>Query(...) Query`.
7. Direct execution methods on `*Queries`:
   - `kind: exec`: `<Name>`, `<Name>Batch` (if `CanBatch`), `<Name>Stream` (if `CanStream`).
   - `kind: one`: `<Name>`.
   - `kind: many`: `<Name>`, `<Name>Into`, `<Name>Seq`, `<Name>Each`, `<Name>Batch`, `<Name>BatchInto`, `<Name>BatchSeq`, `<Name>BatchEach` (if `CanBatch`), `<Name>Stream`, `<Name>StreamSeq` (if `CanStream`).
8. Pipeline queuing method on `*Pipeline`: `func (p *Pipeline) <Name>(...) *runtime.ExecFuture` / `*runtime.RowFuture[<RowType>]` / `*runtime.SliceFuture[<RowType>]`.

---

## 10. User Documentation Specification (`DOCS.md`)

Because the Clean-Room Dogfooding Protocol (Section 11) requires users/sub-agents to build complex applications relying **exclusively on `DOCS.md` and the `sqlgen` CLI**, `DOCS.md` MUST be authored as a comprehensive, accurate, self-contained user manual containing:
1. **Quickstart & CLI Reference**: Exact `go run ./cmd/sqlgen -config sqlgen.yaml` and `//go:generate` usage, `-check` mode for CI, and full `sqlgen.yaml` schema reference.
2. **Complete SQL DSL Cookbook**: Copy-pasteable examples and exact rules for:
   - Inline output columns (`SELECT id:int64, expr AS alias:string`) and single-column vs. struct return types.
   - Scalar parameters (`ID:int64`, `Payload:[]byte`), parameter deduplication when the same parameter name appears multiple times in one statement, 1D slices (`IDs:[]int64`), and multi-row tuple slices (`Users:[]NewUser(OrgID:int64, Email:string, NOW())`).
   - Deadlock-prevention presorting (`IDs:[]int64 ASC`, `OrgID:int64 ASC, Email:string DESC`), PostgreSQL `::type` casts inside `VALUES` tuples (`ID:int64::bigint ASC`), and a clear note that `RETURNING` rows from a presorted batch are returned in the presorted execution order while the caller's input slice remains unmutated.
   - Conditional blocks (`{?FlagName: ...}`) across `SELECT`, `JOIN`, and `WHERE`, and `SelectCols` bitmask projection (`UserColID | UserColEmail`).
   - Keyset pagination (`After:sqlgen.Seek` and `Sort:sqlgen.Order(col1, col2, id)`) and constructing sort orders via `Order<RowType>(<RowType>OrderBy<Col>.Asc(), ...)`.
   - Runtime query composition (`<Name>Query(...)` passed into `Sub:sqlgen.Query` slots in `WITH` CTEs or `WHERE IN`).
3. **Generated Go API & Execution Modes Reference**:
   - Reading: `<Name>`, `<Name>Into`, `<Name>Seq`, `<Name>Each`.
   - Batching & Streaming: `<Name>Batch`, `<Name>BatchInto`, `<Name>BatchSeq`, `<Name>BatchEach`, `<Name>Stream`, `<Name>StreamSeq`, and `runtime.Seq2` / `FromSeq` / `MapSeq2`.
   - Transactions & Retries: `q.WithTx(tx)`, `q.InTx(ctx, txOpts, fn, WithTxRetry(runtime.DefaultRetryPolicy()))`.
   - Pipelining & Conditional Control Flow: `q.Pipeline(ctx)`, `q.PipelineTx(...)`, futures (`ExecFuture`, `RowFuture[T]`, `SliceFuture[T]`), `.AssertRowsAffected(n)`, `.AssertFound()`, `p.If(cond, thenFn, elseFn)`, `WhereCol`, `p.Abort(err)`, and `IfResult.Taken()`.
   - Cache Tuning & Observability: `WithMaxPreparedStmts`, `WithMaxSQLVariants`, `WithMaxPartialBytes`, `WithPromoteThreshold`, `WithMaxBatchParams`, and `q.CacheStats()`.

---

## 11. Multi-Persona Clean-Room Dogfooding Protocol (`examples/`)

To validate ergonomics, documentation completeness, and end-to-end reliability under realistic production workloads, four complete applications MUST be built in `examples/` and tested end-to-end.

### 11.1. The Clean-Room Information Barrier & Ergonomics Feedback Loop
1. **Information Barrier**:
   - When building each dogfooding package under `examples/`, the developer/sub-agent acting as the "sqlgen user" MUST rely **exclusively on `DOCS.md` and the compiled `cmd/sqlgen` binary** (generating `*_gen.go` by invoking `cmd/sqlgen` on the example's `sqlgen.yaml`).
   - The dogfooding packages MUST NOT import internal generator symbols or rely on undocumented runtime internals.
2. **Zero-Friction Ergonomics Loop**:
   - Any ambiguity in `DOCS.md`, unintuitive generated method signature, confusing compile-time error message, or awkward call-site boilerplate discovered while writing the 4 dogfooding applications MUST be treated as a defect: fix `generator.go`, `runtime/`, and `DOCS.md`, regenerate `*_gen.go`, and re-verify.

### 11.2. Dogfooding Persona 1: High-Concurrency Double-Entry Fintech Ledger (`examples/fintech_ledger`)
- **Domain**: Multi-currency account ledger supporting atomic transfers, overdraft protection, idempotency keys, audit logging, and bulk settlement.
- **Features Road-Tested**:
  - `PipelineTx` with `.AssertRowsAffected(1)`, `.AssertFound()`, and `p.If(cond, thenFn, elseFn)` (on sufficient balance: debit sender, credit receiver, insert transfer receipt; on insufficient balance: `elseFn` records a `rejected_insufficient_funds` audit event and commits without moving funds; on frozen account via `WhereCol`: `thenP.Abort(ErrAccountFrozen)` rolls back the entire transaction).
  - Deadlock-free bulk settlement via `UpsertAccountBalancesBatch` using composite presorting (`OrgID:int64 ASC, AccountID:int64 ASC`).
  - Contending concurrent transfers under `WithTxRetry(runtime.DefaultRetryPolicy())`.
- **Mandatory End-to-End Test (`examples/fintech_ledger/service_test.go`)**:
  - Initialize 20 accounts with `$200,000` total supply (`$10,000` each).
  - Spawn 16 goroutines concurrently executing random bidirectional transfers (`A -> B` and `B -> A`), bulk settlements, and frozen-account attempts.
  - Assert **strict conservation of money** ($\sum \text{balances} == \$200,000$), zero unhandled deadlock errors, and identical state transitions on both `WirePipeliner` and sequential `*sql.Tx`.

### 11.3. Dogfooding Persona 2: Multi-Tenant B2B SaaS Analytics & Admin Portal (`examples/saas_multitenant`)
- **Domain**: Multi-tenant user/organization management and invoice cohort reporting portal.
- **Features Road-Tested**:
  - Dynamic search query (`SearchMembers`) combining 4 `{?Flag: ...}` conditional blocks (`WithOrgDetails`, `FilterRole`, `FilterStatuses`, `FilterMinScore`) and `SelectCols` bitmask projection (compact list view vs. full profile view).
  - Multi-column keyset pagination (`ListMembersPaginated`) with `After:sqlgen.Seek` and `Sort:sqlgen.Order(role, score, created_at, id)`.
  - 3-level curried `sqlgen.Query` composition: composing `ActiveOrgsQuery` + `SearchMembersQuery` into `AggregateCohortInvoices`.
- **Mandatory End-to-End Test (`examples/saas_multitenant/portal_test.go`)**:
  - Seed 5 organizations, 300 members, and 1,000 invoices.
  - Road-test dynamic filter combinations, partial column projections (`SelectCols`), full keyset pagination across all pages with mixed `ASC`/`DESC` sort orders, and curried CTE cohort reporting.

### 11.4. Dogfooding Persona 3: E-Commerce Fulfillment & Streaming Archival Engine (`examples/ecommerce_inventory`)
- **Domain**: High-volume order ingestion, bulk SKU lookup, and $O(1)$-memory historical order archival pipeline.
- **Features Road-Tested**:
  - Bulk composite-key `SELECT` lookup (`GetInventoryBySKUsBatch`).
  - Heavy-hitter batch promotion on `InsertOrderItemsBatch` (repeated `500`-row and `400`-row batches) followed by greedy decomposition of a `1400`-row spike (`500 + 500 + 400`).
  - Streaming ETL pipeline: `StreamFulfilledOrdersSeq` $\rightarrow$ `runtime.MapSeq2` $\rightarrow$ `InsertArchivedOrdersStreamSeq` $\rightarrow$ `DeleteFulfilledOrdersByIDsBatch` (with `IDs:[]int64 ASC`).
- **Mandatory End-to-End Test (`examples/ecommerce_inventory/worker_test.go`)**:
  - Ingest 3,000 order items across recurring batch sizes, verify `q.CacheStats().Promotions` and `PreparedHits`, stream-archive 2,000 fulfilled orders in $O(1)$ memory, and verify `cmd/sqlgen -check` passes on all 4 example directories.

### 11.5. Dogfooding Persona 4: Multi-Asset Derivatives & Spot Clearing Exchange (`examples/clearing_exchange`)
- **Domain**: Institutional multi-asset clearing house, real-time margin & risk surveillance engine, atomic bilateral trade settlement with maker/taker fee rebates and liquidation protection, and regulatory trade settlement archival.
- **Features Road-Tested (Full Combinatorial Stress Test)**:
  - **3-Level Nested Curried `sqlgen.Query` Composition + Coupled Conditional Projections/JOINs/WHEREs + Bitmask `SelectCols` + 4-Column Keyset Pagination**:
    - Level 1 (`EligibleAccountsSubquery` with `{?FilterTiers: tier IN (Tiers:[]string ASC)}`, `{?FilterJurisdictions: jurisdiction IN (Jurisdictions:[]string DESC)}`, `{?ExcludeFrozen: status != 'FROZEN'}`, `{?RequireKYC: kyc_verified = 1}`) and Level 1 (`ActiveInstrumentsSubquery` with `{?FilterAssetClasses: asset_class IN (AssetClasses:[]string ASC)}` and `{?MaxInitialMarginBps: initial_margin_bps <= MaxMarginBps:int64}`).
    - Level 2 (`FilteredPositionsSubquery` composing both Level 1 subqueries via `WITH eligible_accts AS (...), active_insts AS (...)` plus `{?OnlyNonZero: p.net_quantity != 0}` and `{?MinNotionalExposure: ABS(p.net_quantity * i.mark_price_cents) >= MinNotionalCents:int64}`).
    - Level 3 (`QueryRiskPositionsPage` composing Level 2 with coupled `{?IncludeAccountMeta: ...}` and `{?IncludeMarginMetrics: ...}` across `SELECT`, `JOIN`, and `WHERE`, bitmask `SelectCols`, `After:sqlgen.Seek`, and 4-column `Sort:sqlgen.Order(maintenance_margin_cents, unrealized_pnl_cents, account_id, position_id)`).
  - **3-Level Nested `PipelineTx` Control Flow (`If`/`Then`/`Else`, `RowFuture`, `SliceFuture.WhereCol`, `ExecFuture`, `Abort`, `UPDATE`/`DELETE`/`INSERT` Branch Guards)**:
    - Validates active instrument (`RowFuture`), checks pre-trade risk flags on both counterparties (`SliceFuture.WhereCol` $\rightarrow$ `Abort(ErrKillSwitchActive)`), verifies both counterparties' margin sufficiency (`ExecFuture` $\rightarrow$ `If/Else` recording `REJECTED_SELLER_MARGIN` or `REJECTED_BUYER_MARGIN`), and in the 3rd-level `Then` branch atomically debits/credits collateral, upserts buyer/seller derivative positions, deletes consumed resting orders, and inserts the immutable trade fill and audit log.
  - **Multi-Dimensional Presorted Batching & Streaming with Extra Scalar Parameters and `RETURNING`**:
    - `UpsertInstrumentMarksBatch` (`Marks:[]InstrumentMarkInput(Symbol:string ASC, ...)` + scalar `UpdatedAt:int64` + `RETURNING` via `Batch`, `BatchInto`, `BatchSeq`, `BatchEach`, `Stream`, `StreamSeq`).
    - `BatchAdjustCollateralBatch` (`Adjustments:[]CollateralAdjustment(DeskID:int64 ASC, AccountID:int64 DESC, DeltaCollateralCents:int64, DeltaLockedCents:int64)` + scalar `UpdatedAt:int64` + `RETURNING`).
    - `LookupPositionsByKeysBatch` (`Keys:[]PositionKey(AccountID:int64 ASC, Symbol:string DESC)` + `{?ExcludeFlat: p.net_quantity != 0}`).
    - `BulkCancelOrdersByIDsBatch` (`OrderIDs:[]int64 ASC` + scalar `CancelReason:string` + `RETURNING`).
    - Streaming settlement archival (`StreamUnarchivedFills` / `Seq` / `Each` $\rightarrow$ `runtime.MapSeq2` $\rightarrow$ `ArchiveSettlementBatchStreamSeq` $\rightarrow$ `MarkFillsArchivedByIDsBatch`).
- **Mandatory End-to-End Test (`examples/clearing_exchange/engine_test.go`)**:
  - Runs identically against both `SequentialTx` and `WirePipeliner`, verifying 12goroutine concurrent bilateral clearing + mark-to-market shocks + collateral sweeps, strict **zero-sum net position conservation** ($\sum \text{net\_quantity} \equiv 0$ per symbol), **exact collateral & fee conservation** ($\sum \text{available\_collateral\_cents} == \text{Initial} + \sum \text{Adjustments} - \sum \text{NetFees}$), and 16-direction 4-column keyset pagination equivalence over the 3-level nested subquery hierarchy.

---

## 12. Exhaustive Testing Discipline: Unit, Fault-Injection, PBT & Fuzzing

### 12.1. Six Engineering Verification Oracles
1. **AST & Static Invariant Gate (`TestSingleStringsBuilderASTAudit`)**:
   - Parse `runtime/*.go`, `generator.go`, and all generated `*_gen.go` files using `go/parser` and `go/ast`.
   - Assert `reflect` is never imported.
   - Assert `strings.Builder` is instantiated ONLY inside `BuildComposedSQL` (1) and `RenderPipelineBatch` (1).
   - Assert `fmt.Sprintf`, `strings.Join`, and `bytes.Buffer` are never called in `runtime/sql_stream.go`.
2. **12-Schema Compilation Isolation Gate (`TestCompilationIsolationMatrix`)**:
   - Generate and type-check (`go/types`) 12 minimal orthogonal schemas (1 statement each: static `exec` 0 params, static `exec` 2 params, static `one` 1 col, static `one` 3 cols, `many` with `SelectCols` only, `many` with `WHERE` flag only, `many` with `SELECT`+`WHERE` flags, `exec` with 1D `ASC` slice, `exec` with tuple `ASC` slice, `many` with `Order` only, `many` with `Seek`+`Order`, `subquery` only) to guarantee zero unused imports or variables in any feature subset.
3. **Property-Based Testing (PBT) Suite (`runtime_test.go` & `generator_test.go`)**:
   - **PBT 1 (`TestPropertyEstimateOpBytesStrictUpperBound`)**: Across 2,000 randomized descriptors, slice/tuple lengths, flags, sort specs, and pipeline decorators, assert `len(renderedSQL) <= EstimateOpBytes(...)` and `testing.AllocsPerRun == 1.0`.
   - **PBT 2 (`TestPropertyKeysetPaginationEquivalence`)**: Across 250 duplicate-heavy rows, all 16 sort-direction masks ($\{\text{ASC}, \text{DESC}\}^4$), and page sizes `1, 2, 7, 25, 250, 500`, assert keyset pagination returns the exact same row sequence as unpaginated SQL `ORDER BY`.
   - **PBT 3 (`TestPropertyPresortImmutabilityAndMonotonicity`)**: Across 500 randomized slice/tuple inputs, assert the caller's slice is never mutated and the emitted parameter sequence is strictly monotonic under the declared `ASC`/`DESC` comparator.
   - **PBT 4 (`TestPropertyBatchChunkDecompositionConservation`)**: Across batch sizes `1..25,000` and randomized promoted Tier-2 sizes, assert $\sum \text{chunk}_i == N$ and every $\text{chunk}_i \times F + S \le \text{MaxWireParams}$.
   - **PBT 5 (`TestPropertyPipelineWireVsSequentialParity`)**:
     - Note on SQLite vs. PostgreSQL CTE Grammar: PostgreSQL/CockroachDB (`DialectDollar`) supports data-modifying CTEs (`WITH _sqlgen_op AS (UPDATE ...)`), whereas SQLite's grammar forbids `UPDATE`/`INSERT`/`DELETE` inside a `WITH` clause.
     - Therefore: (a) Test the exact PostgreSQL `RenderPipelineBatch` CTE SQL generation (`WITH _sqlgen_op AS (...)`, `_sqlgen_chk`, `_sqlgen_cond`, `SlotBranchGuard`, `SlotInsertValuesOpen`) and wire-scanning state machine using the `DialectDollar` `mockdriver` `WirePipeliner`, and (b) For live SQLite differential parity between `WirePipeliner` and sequential `*sql.Tx`, implement a SQLite `WirePipeliner` test adapter that executes the queued `PipelineOp` batch over the SQLite connection (evaluating `SlotBranchGuard` / `SlotInsertValuesOpen` directly in SQLite SQL and recording `_sqlgen_cond` via connection-scoped SQLite custom functions `set_config` and `current_setting`), asserting identical database state and future outcomes between `WirePipeliner` and sequential `*sql.Tx`.
4. **Native Go Fuzzing Suite (`testing.F`)**:
   - `FuzzParseSQL`: Seeded with 25 valid and malformed SQL queries; asserts zero panics and `len(Segments) == len(Occurrences) + 1`.
   - `FuzzParseYAML`: Fuzzes `ParseYAML` across arbitrary byte inputs; asserts zero panics.
   - `FuzzEstimateOpBytesUpperBound`: Fuzzes SQL rendering inputs; asserts `len(sql) <= EstimateOpBytes`.
   - `FuzzVariantKeyAndSubqueryShapeBijectivity`: Fuzzes `MakeVariantKey` and `ComputeSubqueryShape`; asserts zero collisions whenever `IsExact() == true`.
   - `FuzzStmtCacheAdversarialStateTransitions`: Fuzzes cache operations (hits, misses, promotions, demotions, byte-budget overflows); asserts internal doubly-linked list invariants (`partialProb.len + partialProt.len == count(partialItems)`, `preparedProb.len + preparedProt.len <= maxPrepared`, and `shard.partialBytes == sum(len(e.sqlStr))` for all Tier-1 entries).
5. **Deterministic Fault-Injecting & Handle-Counting Driver Suite (`mockdriver` in `runtime_test.go`)**:
   - Inject faults at `PrepareContext`, `BeginTx`, `ExecContext`, `QueryContext`, `Rows.Next` (mid-stream), `Rows.Close`, `Stmt.Close`, `Tx.Commit` (`40001` retryable vs. `driver.ErrBadConn`/`io.EOF` non-retryable), and user closure `panic` inside `InTx` / `PipelineTx`.
   - Assert `mockdriver.OpenStmts.Load() == 0` after demotions, GC cleanup of orphaned `q.WithTx(tx)` (`runtime.AddCleanup`), and `q.Close()`.
   - Assert singleflight promotion under a 64-goroutine thundering herd calls `PrepareContext` **exactly once**.
6. **Resource & Concurrency Gates**:
   - **`MaxOpenConns(1)` Deadlock Gate**: Run all read, batch, streaming (`Seq`/`BatchSeq`/`StreamSeq` with early `break`), `InTx`, and `PipelineTx` operations on a live SQLite DB with `db.SetMaxOpenConns(1)`.
   - **Zero-Allocation Hot-Path Gate**: Verify via `testing.AllocsPerRun` that `MakeVariantKey`, `PackOrder`/`ExpandOrder`, `AcquireArgs`/`ReleaseArgs`, `AcquireIndices`/`ReleaseIndices`, and Tier-1/Tier-2 cache hits allocate `0` heap objects on the cache/pool path.

---

## 13. Mandatory Acceptance Gates, Coverage Thresholds & Verification Commands

The implementation is NOT complete until all of the following commands execute cleanly with zero errors:

```bash
# 1. Verify CLI codegen & -check idempotency across all 4 dogfooding projects
go run ./cmd/sqlgen -config examples/fintech_ledger/sqlgen.yaml -check
go run ./cmd/sqlgen -config examples/saas_multitenant/sqlgen.yaml -check
go run ./cmd/sqlgen -config examples/ecommerce_inventory/sqlgen.yaml -check
go run ./cmd/sqlgen -config examples/clearing_exchange/sqlgen.yaml -check

# 2. Static analysis gate
go vet ./...

# 3. Full test & dogfooding suite with race detector and coverage profile
#    Requirement: >= 95% statement coverage on root package, runtime/, cmd/sqlgen/, and examples/
go test -race -v -count=1 -coverprofile=coverage.out ./...
go tool cover -func=coverage.out

# 4. Native Fuzzing targets
go test -fuzz=FuzzParseSQL -fuzztime=5s .
go test -fuzz=FuzzParseYAML -fuzztime=5s .
go test -fuzz=FuzzEstimateOpBytesUpperBound -fuzztime=5s ./runtime
go test -fuzz=FuzzVariantKeyAndSubqueryShapeBijectivity -fuzztime=5s ./runtime
go test -fuzz=FuzzStmtCacheAdversarialStateTransitions -fuzztime=5s ./runtime

# 5. Zero-allocation and throughput benchmarks
go test -bench=. -benchmem ./runtime
```