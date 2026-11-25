# `sqlgen` — Complete Production Architecture, Ergonomics & Verification Specification (`SPEC.md`)

Note: the spec comes from the spec workshop on https://share.gemini.google/Nm8OXGBJD6LD

## 1. System Overview & The Ten Non-Negotiable Architectural Laws

`sqlgen` is a production-grade Go SQL code generator and zero-reflection runtime library that compiles a declarative YAML configuration (`sqlgen.yaml`) containing annotated SQL statements into strongly-typed, idiomatic Go code backed by `database/sql`.

### 1.1. The Ten Core Invariants

1. **The Single-`strings.Builder` Law & Provable Upper-Bound Pre-Sizing**:
   - **Standalone Statement Execution**: Rendering any SQL statement at runtime—regardless of how many nested `sqlgen.Query` CTEs, `{?Flag: ...}` conditional blocks, `SelectCols` bitmasks, `Seek`/`Order` clauses, or multi-row `VALUES` tuples it contains—MUST instantiate **at most one `strings.Builder`** (`0` on a Tier-1 or Tier-2 cache hit; `1` on a cache miss).
   - **Provable Upper-Bound Pre-Sizing (`estimateOpBytes`)**: Before streaming SQL into a `strings.Builder`, `b.Grow(estimateOpBytes(...))` MUST be called where `estimateOpBytes` is a guaranteed mathematical upper bound (`estimateOpBytes >= len(renderedSQL)`). Reallocating a `strings.Builder`'s internal `[]byte` slice mid-render is a specification violation.
   - **Pipeline / `PipelineTx` Execution**: Rendering an entire $M$-statement `Pipeline` or `PipelineTx`—including all `AssertRows` CTE guards (`_sqlgen_chk`), `If/Then/Else` condition producers (`_sqlgen_cond`), and inline branch guards—MUST instantiate **at most one `strings.Builder` for the entire batch**. All uncached or decorated operations stream sequentially into that single builder, `master := b.String()` is called **once** after all operations are written, and each `ops[i].SQL` is assigned via zero-allocation string slicing `master[span.start:span.end]`.
   - **Zero String Formatting in Combinators or Helpers**: Constructing curried `sqlgen.Query` values, `Order` specifications, or `PipelineCond` boolean AST trees (`.RowsEq(1).And(...)`) MUST NEVER call `fmt.Sprintf`, `strings.Join`, `+` string concatenation, or instantiate `strings.Builder`. All integer formatting (`$1`, `COUNT(*) = N`, `'sqlgen.cN'`) MUST use stack-allocated `[20]byte` buffers with `strconv.AppendInt`.
2. **Zero-Closure Cache Resolution & Zero-Reflection Execution**:
   - Zero `reflect` usage anywhere in `runtime` or generated code.
   - `StmtCache` methods (`Resolve`, `Exec`, `Query`, `QueryRow`) MUST accept `(dialect Dialect, desc *QueryDescriptor, in *RenderInput)` rather than a `func() string` closure so local variables never escape to the heap on cache hits.
   - Flattened `[]any` query arguments MUST use pooled `*[]any` slice pointers (`acquireArgs`, `AcquireArgsCap`, `ReleaseArgs`) cleared via `clear(s)` before returning to `sync.Pool`.
   - Dynamic column scanning MUST bind field pointers into a stack-allocated `[NumCols]any` buffer once *before* `for rows.Next()`, zeroing the target struct in-place (`item = RowType{}`) on each iteration.
3. **100% `database/sql` Compatibility**:
   - Works seamlessly with `*sql.DB` (implicit transactions) and `*sql.Tx` (explicit transactions via `q.WithTx(tx)` and `q.InTx(...)`) across PostgreSQL, CockroachDB, YugabyteDB, SQLite, MySQL, MariaDB and TiDB, including when `db.SetMaxOpenConns(1)`.
4. **Everything-in-SQL Typed DSL**:
   - Output columns (`col:GoType`), input parameters (`:Param:GoType`), 1D slices (`:IDs:[]int64`), multi-row tuple slices (`:Rows:[]Patch(ID:int64, ...)`), deadlock-prevention sort modifiers (`ASC`/`DESC`), conditional blocks (`{?Flag: ...}`), keyset pagination (`:Seek:sqlgen.Seek` and `:Order:sqlgen.Order(...)`), and composable subqueries (`:Sub:sqlgen.Query`) are expressed inline in the SQL string. Every parameter starts with a colon; there is no other spelling, and no out-of-line `params:` / `columns:` lists.
5. **16-Shard Two-Tier Adaptive Statement Cache (W-TinyLFU + SLRU)**:
   - **Tier 1 (Partially Prepared / Staged SQL)**: Stores pre-rendered `sqlStr` strings in memory (`MaxSQLVariants`, `MaxPartialBytes`) using Segmented LRU (20% Probation, 80% Protected), W-TinyLFU admission filtering (via a 2 KB 4-bit Count-Min Sketch per shard), and in-place `cacheEntry` struct recycling.
   - **Tier 2 (Fully Prepared `*sql.Stmt`)**: Stores reference-counted `ManagedStmt` handles (`MaxPreparedStmts`). Variants promote from Tier 1 to Tier 2 (using singleflight coordination via `entry.preparing`) when their decayed TinyLFU frequency reaches `PromoteThreshold` and beats the Tier-2 victim, and demote back to Tier 1 Protected (closing `*sql.Stmt` when `refs == 0` while keeping `sqlStr` warm) when superseded.
6. **Dual Resultset Consumption (Slice & Streaming)**:
   - Every `kind: many` query generates exactly `<Name>` (slice, `nil` when there are no rows) and `<Name>Seq` (lazy `iter.Seq2[Row, error]` iterator). The other consumption styles are the generic helpers `runtime.Collect(seq)`, `runtime.AppendTo(dst, seq)` (reuses the caller's slice capacity; on error returns `dst` exactly as passed) and `runtime.Each(seq, fn func(T) error)` (`fn` returns `runtime.ErrStop`, or an error wrapping it, to end early without an error; a nil `seq` or `fn` is `ErrNilArgument`); none allocates beyond the appended elements.
7. **Arity-Scaled Chunking, Heavy-Hitter Promotion & Iterator Streaming Writes**:
   - A statement with exactly one top-level collection parameter is chunked by its plain method `<Name>` (and `<Name>Seq` for `kind: many`); no generated name carries a `Batch` suffix. More than one chunk runs in an implicit transaction unless the caller's `*Queries` is already bound to one.
   - Per-statement row ceiling $N_{\text{chunk\_max}} \in [1, 32{,}768]$ computed from scalar count $S$, tuple arity $F$, dialect parameter limit (`65,535` or `32,766`), and `WithMaxBatchParams` (default `16,384`).
   - A collection of $N \le N_{\text{chunk\_max}}$ rows is always ONE statement of exactly $N$ rows (no implicit transaction, one round trip, statement-level atomicity), on the first call with a size and on every later call. Only $N > N_{\text{chunk\_max}}$ is chunked: full-limit chunks, the remainder last as one exact chunk. A size that recurs (`>= promoteThreshold`) is promoted to a dedicated Tier-2 `*sql.Stmt`; a one-off size stays a Tier-1 SQL string under the admission filter.
   - Tuple-slice statements also generate `<Name>Stream` (and, for `kind: many`, `<Name>StreamSeq`) accepting `iter.Seq2[TupleStruct, error]` for $O(1)$-memory streaming writes.
8. **Deadlock-Prevention Global Presorting (`ASC` / `DESC`)**:
   - Slices and tuple-slices with `ASC`/`DESC` modifiers run an $O(N)$ `isSorted` check first (0 allocs). If unsorted, they sort a pooled `*[]int` index permutation (`AcquireIndices`) globally **before** chunk slicing—never mutating the caller's slice and guaranteeing that the *bound parameters* are globally monotonic across all chunks. Monotonic **lock acquisition** follows only where the engine processes keys in bound order (multi-row `INSERT`/upsert `VALUES`); for `UPDATE`/`DELETE ... WHERE col IN (...)` the engine's plan decides the order rows are visited, locked and returned (PostgreSQL scans an `IN` list in ascending index order, so `DESC` has no effect there).
9. **Single-Roundtrip `Pipeline` & `PipelineTx` with Server-Side Bail-Out & `If/Then/Else`**:
   - Flushes `[BEGIN + Q1..Qn + COMMIT + Sync]` in **1 network round-trip** on connections implementing `wire.TxPipeliner` (`PipelineTx` and a standalone `Pipeline.Flush`), and `[Q1..Qn + Sync]` in 1 round-trip inside a caller-owned `*sql.Tx` on connections implementing `wire.Pipeliner` (via `WrapConnector`). The shipped driver is the nested module `pgxwire` (pgx `ExecBatch` with `QueryExecModeExec` semantics, so even first-seen statements need no describe round-trip); it is verified on PostgreSQL 15/18 and CockroachDB by counting real network turns (`integration/wire_roundtrip_test.go`). The second shipped driver is the nested module `mysqlwire` (go-sql-driver/mysql, `multiStatements`): the same contract on MySQL 8.4/9.7 and MariaDB 10.11/12.3, where one operation renders as a statement SEQUENCE over session user variables instead of one CTE statement (the "wire flavor", see §6.1 and "Wire flavors" below), verified by counting real network turns (`integration/wire_my_roundtrip_test.go`). A wire pipeliner may refuse a batch BEFORE sending anything by returning `runtime.ErrWireNotApplicable` (packet limit, argument it cannot inline, failed capability probe); the runtime then runs that pipeline sequentially and counts it in `SequentialPipelines`. Everything else falls back automatically to sequential Tier-2 prepared statements, and `CacheStats.WirePipelines` / `SequentialPipelines` report which path ran.
   - Supports `.AssertRows(n)`, `.AssertFound()`, and `p.If(cond, thenFn, elseFn)` with `p.Abort(err)`.
10. **Automatic Transaction Retries (`TxRetryPolicy`)**:
    - `InTx` and `PipelineTx` retry with exponential backoff and full jitter (`math/rand/v2`) on deterministic transient deadlocks and serialization failures (`40P01`, `40001`, `55P03`, MySQL `1213`/`1205`, TiDB `9007`/`8022`, SQLite `SQLITE_BUSY`/`SQLITE_LOCKED`), while never retrying ambiguous network disconnects (`driver.ErrBadConn`, `io.EOF`, `net.Error`) during `Commit`.

---

## 2. Repository Structure & Module Layout

Target Go version: **Go 1.27+** (matches `go.mod`)

```text
.
├── go.mod
├── DOCS.md                             # Complete, freestanding user documentation & cookbook
├── runtime/
│   ├── runtime.go                      # Core types, pools, VariantKey, Order/Seek, iterator helpers
│   ├── sql_stream.go                   # Single-strings.Builder SQL renderer (BuildComposedSQL, renderPipelineBatch)
│   ├── cache.go                        # 16-shard Two-Tier W-TinyLFU + SLRU StmtCache, ManagedStmt, TxStmtCache
│   ├── pipeline.go                     # Pipeline, PipelineTx, Futures, If/Then/Else, WrapConnector, TxRetryPolicy
│   ├── wire/wire.go                    # Driver SPI interfaces (Pipeliner, TxPipeliner, Flavored) and RowsFromDriverRows; imported only by wire drivers
│   └── runtime_test.go                 # AST audit, mockdriver faults, PBT, fuzzing, concurrency & benchmark tests
├── generator.go                        # YAML parser, lexical SQL scanner, validator, single-buffer Go emitter
├── generator_test.go                   # Compilation matrix, parser fuzzing, PBT pagination, and SQLite e2e tests
├── cmd/
│   └── sqlgen/
│       ├── main.go                     # CLI binary (-config, -out, -stdout, -check, -version)
│       └── main_test.go                # CLI exit-code, drift-check, and stderr diagnostic tests
├── pgxwire/                            # NESTED MODULE (pgx/v5): single-round-trip wire.Pipeliner / wire.TxPipeliner for PostgreSQL, CockroachDB & YugabyteDB
│   ├── pgxwire.go                      # OpenDB / Connector, ExecPipeline / ExecTxPipeline (pgconn ExecBatch, QueryExecModeExec semantics)
│   ├── args.go                         # argument resolution for typeless placeholders: time.Time keeps its offset, []byte -> raw binary, [16]byte -> uuid text, map/struct -> JSON, Valuers resolved (DOCS.md 3.4)
│   ├── rows.go                         # text-format results -> database/sql driver values
│   └── *_test.go                       # engine tests (skipped without SQLGEN_*_DSN)
├── mysqlwire/                          # NESTED MODULE (go-sql-driver/mysql): single-round-trip wire.Pipeliner / wire.TxPipeliner for MySQL, MariaDB & TiDB (WireFlavorMySQL)
│   ├── mysqlwire.go                    # OpenDB / Connector (forces multiStatements), conn.ExecPipeline / ExecTxPipeline (one QueryContext of the multi-statement text), capability probe, max_allowed_packet check, failure recovery turn
│   └── *_test.go                       # engine tests (skipped without SQLGEN_*_DSN) and hermetic fake-connection tests
├── integration/                        # NESTED MODULE: real-engine suite incl. wire-vs-sequential differential, network-turn counts, failure/concurrency tests
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

The root module depends on nothing else. `pgxwire/` (module `github.com/CAFxX/sqlgen/pgxwire`, requires `pgx/v5` and `sqlgen`), `mysqlwire/` (module `github.com/CAFxX/sqlgen/mysqlwire`, requires `go-sql-driver/mysql` and `sqlgen`) and `integration/` are separate modules, each with `replace github.com/CAFxX/sqlgen => ../`, so pgx and the database drivers never enter the root module's dependency graph.

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

type StatementSpec struct {
	Name       string    `yaml:"name"`
	Kind       QueryKind `yaml:"kind"`
	ResultType string    `yaml:"result_type"`
	Args       string    `yaml:"args"` // "" or "auto" | "positional" | "struct"
	SQL        string    `yaml:"sql"`
}

type Config struct {
	PackageName  string          `yaml:"package"`
	Out          string          `yaml:"out"`
	ExtraImports []string        `yaml:"extra_imports"`
	Dialect      string          `yaml:"dialect"`
	Statements   []StatementSpec `yaml:"statements"`
}

// `dialect` is REQUIRED and is exactly one of the five names below (parseDialect). There are no aliases and
// no case folding; a missing or unknown value fails with
//   sqlgen: dialect is required; set dialect: to one of postgres, cockroachdb, mysql, mariadb, sqlite
//   sqlgen: unsupported dialect "<v>" (valid dialects: postgres, cockroachdb, mysql, mariadb, sqlite)
// The name alone decides the placeholder style (engineStyle: `$N` for postgres and cockroachdb, `?` for mysql,
// mariadb and sqlite) and the engine-specific diagnostics (mysql rejects RETURNING; mariadb allows it).
// Generated code carries only the style, as `const sqlDialect = runtime.DialectDollar | runtime.DialectQuestion`.
```

### 3.2. Strict YAML Unmarshaling & Import Resolution Rules
1. `ParseYAML(data []byte) (Config, error)` MUST use `dec := yaml.NewDecoder(bytes.NewReader(data))` with `dec.KnownFields(true)`. Empty input (`io.EOF`), trailing YAML documents after the first document, or any key not in `Config` / `StatementSpec` (in particular the removed `version`, `runtime_import`, `params` and `columns`) MUST return an error.
2. Output columns and parameters are declared only inline in the SQL (section 4); `generator.go` has no `ColumnSpec` / `ParamSpec` types and no custom `UnmarshalYAML`.
3. **Strict Unused-Import Prevention (`resolveImports`)**:
   - `context` and `database/sql` are imported when at least one non-`subquery` statement exists (or in `Queries` methods).
   - `sync` is imported ONLY if at least one statement emits a `<tupleStruct>ChunkPool` (`CanStream == true`).
   - `sync/atomic` is imported ONLY if at least one statement emits a static `Fast` atomic pointer slot (`DynamicSlots == 0 && Kind != KindSubquery`).
   - `iter` is imported ONLY if at least one statement has `Kind == KindMany` or `CanStream == true`.
   - `slices` is imported ONLY if at least one 1D slice parameter or tuple field has `HasSort == true`.
   - Standard library packages (`time`, `encoding/json`, `math/big`, `net/netip`, `net/url`, `database/sql/driver`) are auto-imported ONLY when their package selector (`time.`, `json.`, etc.) appears in a parameter, tuple field, or output column type.
4. `GenerateFromYAMLFile` (the single file-level entry point; `cmd/sqlgen` calls it) MUST write the formatted output file atomically through `WriteFileAtomic`: a same-directory temporary file (mode `0666` filtered by the umask, or the existing file's permission bits), `Sync()`, `Close()` and `os.Rename`, removing the temp file on any error.

### 3.3. CLI Binary Contract (`cmd/sqlgen/main.go`)
`cmd/sqlgen` MUST expose a testable `run(args []string, stdout, stderr io.Writer) int` function called by `main()`:
- **Flags**:
  - `-config <path>`: Path to YAML configuration file (default `"sqlgen.yaml"`). It is the only way to name the configuration: a positional argument is rejected with exit code `2`.
  - `-out <path>`: Overrides the `out` path in `sqlgen.yaml`.
  - `-stdout`: Writes generated Go code to `stdout` instead of disk.
  - `-check`: CI verification mode. Generates the Go code in memory and compares it byte-for-byte against the existing output file on disk without modifying disk files. If the file is missing or differs, prints an actionable diff/drift message to `stderr` and exits with code `2`.
  - `-version`: Prints `sqlgen <version>\n` to `stdout` and exits with code `0`, where `<version>` is the main module version from `runtime/debug.ReadBuildInfo` (for example `v0.3.1`), or `(devel)` when it is empty or no build information is available.
- **Exit Codes**:
  - `0`: Success.
  - `1`: Configuration, file I/O, SQL parsing, or validation error.
  - `2`: Invalid CLI flag usage or `-check` drift detected.
- **Output Safety**: The output file is written atomically (same-directory temporary file, `fsync`, rename). A symbolic link at the output path is followed (the link is preserved); an existing output keeps its permission bits and a new one gets `0666` filtered by the umask. The generator MUST refuse (exit `1`) to overwrite the configuration file, or any non-empty file that does not begin with `// Code generated by sqlgen.`, and MUST reject an output path containing a backslash when the path separator is `/`. A relative `-out` is relative to the working directory; a relative `out:` is relative to the configuration file.

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
                   | param_decl
                   | param_ref
                   | output_column ;

tuple_slice_param  = ":" , identifier , ":" , "[]" , go_ident , "(" , tuple_body , ")" ;
tuple_body         = { protected_token | tuple_field | "(" , tuple_body , ")" | raw_sql_char } ;
tuple_field        = identifier , ":" , scalar_go_type , [ typecast_suffix ] , [ sort_dir ] ;
typecast_suffix    = "::" , identifier , [ "(" , { digit | "," | " " } , ")" ] ;

order_directive    = ":" , identifier , ":" , "sqlgen.Order" , "(" , order_col_list , ")" ;
order_col_list     = sql_col_ref , { "," , sql_col_ref } ;
sql_col_ref        = [ identifier , "." ] , identifier ;

seek_directive     = ":" , identifier , ":" , "sqlgen.Seek" ;
query_param        = ":" , identifier , ":" , "sqlgen.Query" ;
param_decl         = ":" , identifier , ":" , go_type , [ sort_dir ] ;          (* scalar ":name:T", slice ":name:[]T" [ASC|DESC] *)
param_ref          = ":" , identifier , [ sort_dir ] ;                          (* repeat of an earlier declaration *)
output_column      = identifier , ":" , go_type ;                               (* only at the end of a top-level SELECT / RETURNING item *)

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
   There is exactly one spelling per concept, so classification is by the leading colon:
   - A token that starts with `:` is a **parameter**: `:name:Type` declares one (a `[]T` type other than `[]byte` is a slice and may be followed by `ASC` / `DESC`; `*T` is nullable), `:name:[]Struct(...)` a tuple slice, `:name:sqlgen.Order(...)` / `:name:sqlgen.Seek` / `:name:sqlgen.Query` the directives, and a bare `:name` repeats a parameter declared earlier in the statement (an undeclared `:name` is an error: declare the type at the first use). Referencing a non-scalar, non-slice parameter again is an error.
   - A token without a leading `:` followed by `:Type` is an **Output Column** if and only if `inProjection == true`, `parenDepth == 0`, `bracketDepth == 0` and **`isEndOfProjectionItem(span, pos)` returns `true`**:
     Starting at `pos`, loop to skip whitespace, `--` comments, `/*...*/` comments, and **any `{?Flag: ...}` conditional blocks** (scanning balanced `{...}` while respecting `'...'` strings). After skipping those, return `true` if and only if `i >= len(span)`, `span[i] == ','`, `span[i] == ';'`, or `span[i]` starts an unannotated identifier word `W` whose lowercase form is in the **Projection Terminator Set**.
   - Any other colon-less `name:Type` is a generate-time error that names the token and the corrected spelling (`:name:Type`).
   - The retired spellings are errors too: `name...` / `Type...` slices (declare `:name:[]Type`), the `runtime.` and bare `Query` / `Seek` / `Order` directive prefixes (only `sqlgen.Order`, `sqlgen.Seek`, `sqlgen.Query` exist), and YAML `params:` / `columns:`.

### 4.4. Unconditional `SlotSelectCols` Splicing (Single-Pass Safe)
Whenever a statement has `len(UncondCols) > 1` (inline output columns in `SELECT` or `RETURNING`), **regardless of the argument style (`StructArgs`), which is decided after `SELECT` scanning**:
1. Each unconditional output column records its full trimmed SQL projection expression in `QueryDescriptor.SelectColExprs` (e.g., `"u.id"`, `"COALESCE(o.display_name, o.slug) AS org_name"`).
2. In the top-level `QueryDescriptor`, the entire span of unconditional column expressions (from after the `SELECT ` / `RETURNING ` whitespace to the end of the last unconditional column) is replaced by a single `SlotSelectCols` slot in `QueryDescriptor.Slots`.
3. Any conditional output columns (`{?WithOrg: , o.name AS org_name:string}`) remain as `SlotOptBlock` occurrences immediately following `SlotSelectCols`.
4. When no columns are selected (`p.SelectCols & ColsAll` is `0` or every column; the canonical value is `0`, in both argument styles), `in.ColMask` is `0`, which `SlotSelectCols` renders as all unconditional columns (`effectiveMask = validMask`). When `DynamicSlots == 0`, `StaticSQL` also expands `SlotSelectCols` with `ColMask == 0`. This guarantees that discovering a `{?Flag: ...}` block later in `WHERE` never invalidates the already-scanned `SELECT` clause.
5. In a statement with `sqlgen.Order`, a non-zero mask is OR-ed with the bits of every `Order(...)` column before the "all columns" canonicalisation (`compute<Name>Key`): the sort/cursor columns are always selected, since `Seek<Name>(row)` reads them.

### 4.5. Inline Branch-Guard Slots (`SlotBranchGuard*` & `SlotInsertValues*`)
To allow any statement inside a pipelined `p.If(...)` branch to be gated on the server in a **single streaming pass** with **one `strings.Builder`** across both PostgreSQL and SQLite:
1. **`SELECT` Statements (`desc.StmtVerb == VerbSelect`)**:
   - Handled directly in `streamOpSQL`: when `len(in.BranchGates) > 0`, `streamOpSQL` writes `"SELECT * FROM ("` before streaming `desc` and writes `") AS _sqlgen_if WHERE "` + `<branch_gates>` after streaming `desc`.
2. **`INSERT` Statements (`desc.StmtVerb == VerbInsert`)**:
   - **Subcase 2A: Single-Row `INSERT INTO tbl (c1, c2) VALUES (e1, e2)`** (where `VALUES` is followed by `(`):
     - Replace `VALUES (` (including the opening parenthesis) with `SlotInsertValuesOpen`, and replace the matching closing `)` of the `VALUES (...)` tuple with `SlotInsertValuesClose`.
     - When `len(in.BranchGates) == 0`: `SlotInsertValuesOpen` writes `"VALUES ("` and `SlotInsertValuesClose` writes `")"`.
     - When `len(in.BranchGates) > 0`: `SlotInsertValuesOpen` writes `"SELECT "` and `SlotInsertValuesClose` writes `" FROM (SELECT 1) AS _sqlgen_dual WHERE "` + `<branch_gates>`.
     - *(Emitting `INSERT INTO tbl (c1, c2) SELECT $1, $2 FROM (SELECT 1) AS _sqlgen_dual WHERE <gates> [ON CONFLICT ...] [RETURNING ...]` is valid syntax in PostgreSQL, CockroachDB and SQLite, but it does NOT preserve PostgreSQL's target-column type inference: a placeholder in a SELECT list is not typed from the target column (PostgreSQL resolves it to `text` and fails with `column "id" is of type bigint but expression is of type text`; CockroachDB fails with `could not determine data type of placeholder $1`). Therefore, under `DialectDollar`, every placeholder rendered inside a branch-gated INSERT's value list is typed explicitly: `Slot.Cast` / `TupleDescriptor.Casts[i]` (emitted by the generator for `INSERT` statements from the parameter's Go type: `int`/`int64`/`uint32`→`bigint`, `int32`/`rune`/`uint16`→`integer`, `int16`/`int8`/`uint8`/`byte`→`smallint`, `string`→`text`, `bool`→`boolean`, `float64`→`double precision`, `float32`→`real`, `[]byte`→`bytea`, `time.Time`→`timestamptz`; no cast for other types, where the user adds an explicit `::type`) renders as `$1::bigint`. Un-gated INSERTs keep plain `VALUES (...)`, where inference works, and carry no casts.)*
   - **Subcase 2B: Multi-Row Tuple Slice `INSERT INTO tbl (c1, c2) VALUES :Rows:[]Tuple(...)`** (where `VALUES` is followed by a `SlotTupleSlice` token):
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
   - After a `SlotSlice` token (`:IDs:[]int64`) or a tuple field (`ID:int64` or `ID:int64::bigint`), look ahead across horizontal whitespace (`' '`, `'\t'`). If the next word is `ASC`, `asc`, `DESC`, or `desc` (followed by whitespace, `,`, `)`, `\n`, or `EOF`), consume it and set `HasSort = true, Desc = (lower == "desc")`.
   - Validate that the element/field Go type is orderable (`int*`, `uint*`, `float*`, `uintptr`, `byte`, `rune`, `string`, `time.Time`, `[]byte`, `[]uint8`); otherwise return a compile-time error.
4. **Generator input restrictions** (each is rejected at generation time rather than emitting code that does not compile or silently misbehaves):
   - A conditional flag name, a parameter name and a `SelectCols` Go field of one statement MUST NOT collide.
   - A 1D slice parameter (first occurrence or repeat) MUST be the whole list of an `IN (...)` / `NOT IN (...)`: the SQL text before it ends with `IN (` and the next non-space byte is `)` (an empty preceding segment, after another slot, cannot be judged and is accepted, like the tuple-slice rule).
   - An unconditional output column MUST NOT follow a conditional one, and a statement MUST NOT have more than 64 unconditional output columns.
   - Statements sharing a `result_type` may declare different `sqlgen.Order` lists: the sort and seek helpers are per statement.
   - **Result type (scalar vs struct).** For `kind: one` / `kind: many`: when the statement has exactly one output column, that column is unconditional and `result_type` is empty, `RowTypeName` is the column's Go type and the statement returns the value itself (`ScalarResult == true`: `(T, error)`, `([]T, error)`, `iter.Seq2[T, error]`, `*RowFuture[T]` / `*SliceFuture[T]`), for any `T` the column may be declared as (`time.Time`, `[]byte`, `*string`, `pkg.Type`). Otherwise `RowTypeName` is `result_type` when set, and `<Name>Row` when not; the row struct is then generated, even for one column. `result_type` MUST be a valid Go identifier that is not declared in the Go universe scope (`go/types.Universe.Lookup(name) == nil`: `int64`, `string`, `any`, `error`, `nil`, `len`, ... are rejected with an error naming the predeclared identifier).
   - Statement names, `result_type` names and tuple struct names MUST NOT equal a generated declaration (`reservedStatementNames`, `reservedTypeNames`).
   - A parameter whose Go name equals a generated local (`reservedParamNames`) is renamed `<name>Param`.
   - Tuple bodies accept `--` and `/* */` comments and `$tag$` strings. `DISTINCT ON (...)` stays in the projection prefix, and neither `IS [NOT] DISTINCT FROM` nor a qualified word such as `t.limit` ends the projection.
3. **Exact `CanBatch` and `CanStream` Predicates**:
   - Let `collectionOccs := ps.SliceOccs + ps.TupleOccs`.
   - `CanBatch` is `true` if and only if:
     `(ps.Spec.Kind == KindExec || ps.Spec.Kind == KindMany) && collectionOccs == 1 && collectionParam.FlagBit == 0 && ps.QueryOccs == 0 && !ps.HasOrder && !ps.HasSeek`.
   - `Decomposable` (only meaningful when `CanBatch`) is the lexical, conservative verdict of `decomposable()` (`decompose.go`): the collection is the `VALUES` list of a top-level `INSERT`, or a positive `IN (...)` list that is a whole conjunct of the top-level `WHERE` of a `SELECT`/`UPDATE`/`DELETE` (also inside parenthesised `AND` groups that are not sub-queries), and the top level (plus the enclosing groups) has no aggregate call, `GROUP BY`, `HAVING`, `DISTINCT`, `ORDER BY`, `LIMIT`, `OFFSET`, `FETCH`, `OVER`, `WINDOW`, `UNION`, `INTERSECT`, `EXCEPT` and, for an `IN` list, no `OR`; optional blocks are inlined, the SelectCols projection is scanned through its column expressions, sub-queries that do not contain the collection are ignored. When unsure: not decomposable.
   - A `CanBatch && !Decomposable` statement never chunks: its plain method and `<Name>Seq` return `*runtime.CollectionTooLargeError` (`errors.Is(err, runtime.ErrCollectionTooLarge)`) before sending anything when `len(collection) > MaxRowsPerBatch`, and otherwise run the single statement.
   - `CanStream` is `true` if and only if:
     `ps.CanBatch && ps.Decomposable && ps.TupleOccs == 1`.
   - A struct-style `<Name>Stream` / `<Name>StreamSeq` returns `runtime.InvalidArgument("<Name>Stream: p.<Field> must be empty: ...")` (matching `runtime.ErrInvalidArgument`) when the collection field of `p` is non-empty, before pulling `seq`. An empty `seq` is a no-op when `ValuesParamIdx == BatchParamIdx` and `runtime.ErrEmptySliceParam` otherwise (counted with `totalRows`).
5. **Empty collections (`ValuesParamIdx`).** `ValuesParamIdx` is the parameter index of the tuple slice parsed as the row source of `INSERT ... VALUES :rows:[]T(...)` (Subcase 2B of 4.5: top level, not inside a `{?Flag:}` block, `StmtVerb == VerbInsert`), or `-1`. Emptiness is judged by that structure, never by the statement kind:
   - A statement with `ValuesParamIdx >= 0` and `Kind` `KindExec` or `KindMany` treats `len(collection) == 0` as a successful no-op, checked before anything else (before argument expansion, chunk planning, transaction start, cache access): `<Name>` returns `runtime.BatchExecResult{}` (`RowsAffected() == 0`, nil error) / a nil slice, `<Name>Seq` and `<Name>StreamSeq` yield nothing, `<Name>Stream` of an empty iterator likewise, and the `*Pipeline` method returns `QueueExecNoop()` / `QueueSliceNoop[T]()` (section 8). No SQL, no transaction.
   - Every other empty collection stays `runtime.ErrEmptySliceParam` (emitted in `expand<Name>Params` and the chunked paths): a 1D slice, a tuple slice used inside `IN (...)`, a `VALUES` list that is not the row source of an `INSERT` (a CTE or derived table: what emptiness means depends on its use), and any collection of a `KindOne` statement. In a pipeline the error is recorded as the build error and returned by the flush, nothing is sent.

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
	defaultMaxPreparedStmts = 256
	defaultMaxSQLVariants   = 4096
	defaultMaxPartialBytes  = 16 << 20 // 16 MB
	defaultPromoteThreshold = 2
	defaultMaxBatchParams   = 16384
	maxPowerOfTwoChunk      = 32768
	inexactKeyBit           = uint64(1) << 63
)

var (
	ErrEmptySliceParam    = errors.New("sqlgen: slice parameter cannot be empty")
	ErrUninitializedQuery = errors.New("sqlgen: curried Query argument is uninitialized")
	ErrAssertionFailed    = errors.New("sqlgen: pipeline row-count assertion failed")
	ErrBranchNotTaken     = errors.New("sqlgen: pipeline if/else branch was not taken")
	ErrInvalidOrder       = errors.New("sqlgen: invalid order specification")
	ErrNilArgument        = errors.New("sqlgen: nil iterator or callback argument")
	ErrStop               = errors.New("sqlgen: stop iteration") // returned by an Each callback to end early without an error
	ErrFutureNotReady     = errors.New("sqlgen: pipeline future read before the pipeline was flushed")
)

// AssertionError is what a failed AssertRows / AssertFound returns. It matches
// errors.Is(err, ErrAssertionFailed) via Unwrap. GotKnown is false on the wire path, where the
// server only signals the violation and the actual count is unavailable.
type AssertionError struct {
	Statement string // QueryDescriptor.Name; empty when unknown
	Want, Got int64
	GotKnown  bool
}
// Error(): `sqlgen: pipeline assertion failed on statement "S": expected N rows affected[, got M]`
```

`ErrAssertionFailed` is the sentinel; assertion failures are always returned as `*AssertionError` wrapping it. The generator emits the statement name into `QueryDescriptor.Name` for every statement so the runtime can report it.

### 5.1. 256-Bit `VariantKey` & Bijective `ComputeSubqueryShape`
```go
// VariantKey is a 256-bit comparable value type that uniquely identifies a query variation
// across StmtID, ColMask, FlagMask, OrderKey, and up to 4 slice/tuple/subquery dimensions.
type VariantKey [4]uint64

func MakeVariantKey(stmtID uint32, colMask, flagMask, orderKey uint64, dims ...uint64) VariantKey {
	inexact := stmtID == 0 || stmtID > 0x7FFF || colMask > 0xFFFFFF || flagMask > 0xFFFFFF || len(dims) > 4
	for _, d := range dims {
		if (d & inexactKeyBit) != 0 {
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
	return VariantKey{inexactKeyBit | uint64(stmtID), colMask ^ flagMask, orderKey, uint64(len(dims))}
}

func (k VariantKey) IsExact() bool { return (k[0] & inexactKeyBit) == 0 }

func (k VariantKey) hash() uint64 {
	h := k[0]*0x9e3779b97f4a7c15 + k[1]*0xbf58476d1ce4e5b9 + k[2]*0x94d049bb133111eb + k[3]
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	return h ^ (h >> 31)
}

// ComputeSubqueryShape packs a subquery's coordinates bijectively into 63 bits (inexactKeyBit == 0)
// when all fields fit, or sets inexactKeyBit so the outer query falls back to exact SQL string keying.
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
	return inexactKeyBit | (vk.hash() & ^inexactKeyBit)
}
```

### 5.2. Complete Keyset Pagination (`Order` & `Seek`) Algorithms
```go
// MaxOrderCols is the largest number of columns a sqlgen.Order(...) directive may list, and so the
// largest number of terms a packed Order can carry: every term is 5 bits wide (a 1-based column index
// of 1..12 in the upper four bits, the DESC flag in the lowest), so twelve terms fill bits 0..59 and
// leave bit 63 for the Seek flag of EncodeOrderKey.
const MaxOrderCols = 12

const (
	orderTermBits = 5
	orderTermMask = 1<<orderTermBits - 1
)

// orderInvalidBit marks a packed Order built from an invalid term list (see PackOrder). It sits above the
// 60 term bits and below the Seek flag, and is ignored by expandOrder.
const orderInvalidBit = uint64(1) << 62

// PackOrder packs up to MaxOrderCols sort column terms (each encoded as ((colIdx1Based) << 1) | descBit)
// into a single collision-free uint64 with zero heap allocations. It accepts any type whose underlying
// type is uint8, so the generated per-statement term types (which make mixing the tokens of two
// statements a compile error) pack without conversion. A term that is zero, names a column beyond
// MaxOrderCols, or repeats a column already named marks the result invalid: OrderValid reports false
// and the generated statement methods return ErrInvalidOrder instead of running a sort the caller did
// not ask for.
func PackOrder[T ~uint8](terms ...T) uint64 {
	var (
		packed uint64
		seen   uint64
		shift  uint
	)
	for _, term := range terms {
		t := uint8(term)
		col1 := t >> 1
		if col1 == 0 || col1 > MaxOrderCols {
			packed |= orderInvalidBit
			continue
		}
		bit := uint64(1) << col1
		if (seen & bit) != 0 {
			packed |= orderInvalidBit
			continue
		}
		seen |= bit
		packed |= uint64(t) << shift
		shift += orderTermBits
	}
	return packed
}

// OrderValid reports whether orderSpec was packed from a valid term list (see PackOrder). The zero Order
// (no terms) is valid.
func OrderValid(orderSpec Order) bool { return orderSpec&orderInvalidBit == 0 }

// expandOrder unpacks a packed uint64 order specification into column indices (0..numCols-1)
// and direction flags (true == DESC), automatically appending the tie-breaker column (numCols-1)
// if not already present (inheriting the preceding sort column's direction so single-direction
// DESC sorts use uniform tuple comparison). numCols must not exceed MaxOrderCols (the generator
// rejects larger Order directives); count is at most numCols.
func expandOrder(orderSpec uint64, numCols int) (cols [MaxOrderCols]int, desc [MaxOrderCols]bool, count int, allSameDir bool) {
	if numCols <= 0 {
		return cols, desc, 0, true
	}
	if numCols > MaxOrderCols {
		numCols = MaxOrderCols
	}
	var seen uint64
	for shift := uint(0); shift < MaxOrderCols*orderTermBits; shift += orderTermBits {
		t := uint8((orderSpec >> shift) & orderTermMask)
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

// seekArgCount returns the number of positional parameters consumed by SlotSeek.
func seekArgCount(dialect Dialect, orderSpec uint64, numCols int, hasSeek bool) int {
	if !hasSeek || numCols <= 0 {
		return 0
	}
	_, _, count, allSameDir := expandOrder(orderSpec, numCols)
	if dialect == DialectDollar || count == 1 || allSameDir {
		return count
	}
	// DialectQuestion (?) with mixed ASC/DESC directions expands to:
	// 1 bounding-box param + count*(count+1)/2 lexicographical branch params.
	return 1 + (count*(count+1))/2
}

// AppendSeekArgs appends cursor column values into dst in the exact order expected by formatSeekSQL.
func AppendSeekArgs(dst *[]any, dialect Dialect, orderSpec uint64, numCols int, colValues []any) {
	cols, _, count, allSameDir := expandOrder(orderSpec, numCols)
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
	cols, desc, count, _ := expandOrder(orderSpec, len(orderCols))
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
	cols, desc, count, allSameDir := expandOrder(orderSpec, len(orderCols))
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
2. **`_sqlgen_cond` CTE** (and the `_sqlgen_chk` assertion CTE):
   *Evaluation guarantee:* both CTEs have side effects (an overflow error, `set_config`) and MUST run whenever the batch runs, even when `_sqlgen_op` yields zero rows. For operations with result columns the final statement is therefore `SELECT _r.* FROM (SELECT 1 AS _o[, (SELECT _ok FROM _sqlgen_chk) AS _a][, (SELECT _ok FROM _sqlgen_cond) AS _c]) AS _f CROSS JOIN LATERAL (SELECT * FROM _sqlgen_op WHERE _f._o IS NOT NULL[ AND _f._a IS NOT NULL][ AND _f._c IS NOT NULL]) AS _r`. A plain `FROM (SELECT 1, (SELECT _ok FROM ...)) AS _f JOIN _sqlgen_op ON TRUE` is forbidden: PostgreSQL prunes the unused `_f` columns and never evaluates the CTEs, so conditions are never recorded and assertions never fire. Verified on PostgreSQL 15/18 and CockroachDB by `integration/wire_pg_test.go`.
   For operations WITHOUT result columns (plain exec) the final statement is `SELECT (SELECT COUNT(*) FROM _sqlgen_op) AS _cnt[, (SELECT _ok FROM _sqlgen_chk) AS _a][, (SELECT _ok FROM _sqlgen_cond) AS _c][, (SELECT _ok FROM _sqlgen_x<ID>) AS _x<ID> ...]`; the first column is the number of affected rows. *Every* guard CTE (`_sqlgen_chk`, `_sqlgen_cond`, every `_sqlgen_x<ID>`) MUST be read by the target list of the final SELECT in EVERY statement shape: PostgreSQL removes a CTE whose output is unused, and a `CROSS JOIN` against it that never references one of its columns counts as unused (an assertion that also fed an `If` condition used to commit although the caller was told it failed). Pinned structurally by `TestEveryGuardCTEIsReferencedByTheFinalSelect` (verb x result columns x assertion x conditions x gates) and proved on real engines by `TestRealEnginePipelineGuardMatrix`.
   Stream `", _sqlgen_cond AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.c"` + `condID` + `"', CASE WHEN ("` + `cond` + `") THEN 'true' ELSE 'false' END, true) = 'true' AS _ok FROM _sqlgen_op)"`.
   *Compound conditions across operations:* `Pipeline.If` binds the condition (`bindCond`, a copy; the caller's value is never modified). The condition is rendered by its root operation, the latest operation of its leaves. A leaf over a DIFFERENT operation is not evaluated by the root's statement (that would count the root's rows); its own operation records it in an extra CTE `_sqlgen_l<ID>` (`OpDecorators.LeafRecs`, one record per distinct leaf and operation, IDs drawn from the same counter as condition IDs) as `set_config('sqlgen.l<ID>', CASE WHEN (<leaf>) THEN 'true' ELSE 'false' END, true)`, read through the usual guard column `(SELECT _ok FROM _sqlgen_l<ID>) AS _l<ID>`. The root renders such a leaf as `COALESCE(current_setting('sqlgen.l<ID>', true) = 'true', false)`; a leaf of the root itself uses its own `_sqlgen_op`. A leaf over an operation with branch gates is rendered `CASE WHEN (<gates>) THEN (<leaf>) ELSE FALSE END` (NULL gates count as closed) so that an operation that did not run is FALSE, also under `NOT`, exactly like `PipelineCond.EvalGoFn`; a leaf without an operation is `FALSE`. Proved against the sequential path by `TestRealEnginePipelineCompoundConditions` and the differential fuzz.
3. **`_sqlgen_chk` CTE & `IsAbort` guard error**:
   - A guard fails its statement with an error that can be told apart from every error the caller's own statement may raise (a genuine integer overflow, SQLSTATE 22003, must surface as the database error, exactly as on the sequential path). In `DialectDollar` the guard casts a text carrying a marker to an integer: assertion `('sqlgen:assert:' || COUNT(*)::text)::int` as the `ELSE` branch of the `_sqlgen_chk` CASE, abort `('sqlgen:abort:' || (SELECT COUNT(*))::text)::int` (constant `wireAssertMarker` / `wireAbortMarker`). PostgreSQL answers `invalid input syntax for type integer: "sqlgen:assert:3"` (22P02), CockroachDB `could not parse "sqlgen:assert:3" as type int ...` (22P02); the marker names the guard and an assertion's marker carries the actual row count.
   - This MUST NOT be a constant expression: PostgreSQL constant-folds constants at plan time even inside an unreached `CASE` branch, which would make every guarded statement fail unconditionally (an earlier constant overflow guard did, and `-9223372036854775808::bigint` parses as `-(9223372036854775808::bigint)`, whose cast already overflows). The aggregate (`COUNT(*)`, or the scalar subquery `(SELECT COUNT(*))` for an abort, which has no FROM) defers evaluation to the moment the branch is taken. Verified against real PostgreSQL 15/18 and CockroachDB (`integration/wire_guard_test.go`, `integration/wire_pg_test.go`).
   - `TranslateWireError(op, err)` translates ONLY errors whose SQLSTATE (when the driver exposes one) is 22P02 and whose message carries the operation's own marker: `sqlgen:abort:` on an `IsAbort` operation becomes `op.AbortErr`; `sqlgen:assert:<n>` on an operation with `AssertRows >= 0` becomes `*AssertionError{Want, Got: n, GotKnown: true}`. Every other error, a genuine overflow included, is returned unchanged.
   - In `DialectQuestion`: `"ABS(-9223372036854775808)"`, a constant integer overflow (SQLite; MySQL/MariaDB have no wire path since the pipeline SQL also relies on PostgreSQL-only `set_config`/`current_setting`). Neither can carry a marker in an error message, so `renderPipelineBatch` flags every guarded operation it rendered for a non-`$N` dialect (`PipelineOp.overflowGuard`) and `TranslateWireError` then recognises that overflow (`SQLSTATE 22003`, `integer overflow`, `bigint out of range`) on such an operation as the guard. This is what the example suites' SQLite-backed simulated wire driver relies on; operations rendered for `$N` never do.

### 6.2. Provable Upper-Bound Byte Estimator (`estimateOpBytes`)
`estimateOpBytes(desc *QueryDescriptor, in *RenderInput, dec *OpDecorators) int` MUST compute a strict upper bound (`estimateOpBytes >= len(renderedSQL)`):
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
- For `dec.IsAbort`: `144 + 56 * len(in.BranchGates)` bytes (the whole statement).
- For `dec.AssertRows >= 0`: `400 + 56 * len(in.BranchGates)` bytes (the CTE, its guard error and the guard's column in the final SELECT).
- For `dec.ProduceCond != nil`: `480 + dec.ProduceCond.estimateBytes(len(in.BranchGates))` bytes, and the same for every `dec.ExtraConds[i]` and `dec.LeafRecs[i]` (a leaf costs `80 + len(SQLPred)`, plus `48 + 56 * len(in.BranchGates)` when gated).
- Per `SlotScalar` / tuple field when `len(in.BranchGates) > 0`: `len(Cast) + 2` extra bytes (see §4.5).
- These constants are verified by `TestEstimateOpBytesCoversEveryPipelineOpShape`, which renders every verb × assertion × condition × gate-count × dialect shape and requires `estimateOpBytes >= len(rendered)`.

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

1. **Wire-Pipelined Mode (`wire.TxPipeliner` / `wire.Pipeliner`)**:
   - `wire.TxPipeliner.ExecTxPipeline(ctx, txOpts, ops)` runs `BEGIN [isolation / read-only from txOpts]`, every operation and `COMMIT` in one round-trip. It is detected via `db.Conn` + `conn.Raw` (unwrapping `WrapConnector`'s wrapper) when the runtime owns the transaction: `PipelineTx` (via `RunPipelineTx`, which only *queues* in the user callback and begins the transaction when it flushes, retrying the whole attempt under `WithTxRetry`) and a standalone `Pipeline.Flush`. It is skipped when `MaxOpenConnections == 1`. On any error the implementation leaves no transaction open on the connection (an extra `ROLLBACK` round-trip is allowed on the error path only) and reports a broken connection to the pool; an error raised by the `COMMIT` is wrapped in `*CommitError` so `IsRetryableTxError(err, duringCommit=true)` applies, and the wrapper is removed before the error reaches the caller. A transport error in the batch has an unknown outcome and is reported as a commit-time failure (never retried).
   - `wire.Pipeliner.ExecPipeline(ctx, ops)` runs the operations only, inside the transaction the connection is in (a caller-owned `*sql.Tx`, reached via `WrapConnector` sentinel execution `tx.ExecContext(withPipelinePayload(ctx, payload), "--sqlgen:pipeline")` when `txCache != nil`; the caller commits or rolls back). On an idle connection the `pgxwire` implementation wraps the batch in `BEGIN ... COMMIT` itself, because the transaction-local `set_config` the condition recording relies on is rejected by CockroachDB outside an explicit transaction. A driver implementing only `wire.Pipeliner` gets `BeginTx` + one wire call + `Commit` for `PipelineTx`.
   - **Wire flavors.** A pipeliner that implements `wire.Flavored` (`WireFlavor() WireFlavor`) says how its batch must be rendered; one without it is `WireFlavorPostgres`. For `WireFlavorMySQL` the runtime does NOT pre-render `ops[i].SQL`: the connection calls `runtime.RenderMySQLBatch(ops, MySQLRenderOptions{Flavor, InTx, TxOpts})` (it knows the time zone and character set of the literals, whether the caller's transaction is open, and `max_allowed_packet`). That renders ONE multi-statement text per batch (`MySQLBatch.SQL`; `ops[i].SQL` is the part of it that belongs to operation i): `SET @sqlgen_p=-1;` then, per operation, `SET @sqlgen_p=<i>;`, the core statement with the arguments INLINED as literals by the renderer (`MySQLFlavor.WriteLiteral`; the placeholders are slots of the descriptor, so no text is ever searched for `?`) and every branch gate / failure gate rendered as `@sqlgen_c<ID>=1|0` / `@sqlgen_f<0`, `SET @sqlgen_n<i>=ROW_COUNT()` (or `FOUND_ROWS()` after a SELECT, or a `(@sqlgen_k:=@sqlgen_k+1)` counter column after a MariaDB `RETURNING`), an assertion as `SET @sqlgen_f=IF(@sqlgen_f<0 AND (gates) AND NOT (@sqlgen_n<i><=>want), i, @sqlgen_f)`, an Abort as the same without the count test, every condition as `SET @sqlgen_c<ID>=COALESCE(expr,0)` (leaves recorded for a compound condition as `@sqlgen_l<ID>`; a `WhereCol` leaf re-runs the SELECT inside an aggregate), one assignment per `SET`, and every `@sqlgen_` variable assigned earlier in the SAME batch (session variables outlive a batch on a pooled connection). The batch ends with `COMMIT` (own transaction, no guard in the batch) or `SET @sqlgen_q=IF(@sqlgen_f<0,'COMMIT','ROLLBACK'); PREPARE/EXECUTE/DEALLOCATE`, then the summary row `SELECT @sqlgen_f, @sqlgen_n.., @sqlgen_c..`. The k-th row-returning operation is the k-th result set of the answer (`MySQLBatch.ResultSetOps`), the summary row follows. A failed assertion or Abort is NOT a server error: later operations are gated off, the transaction rolls back and `MySQLBatch.Apply(ops, summaryRow)` builds the same `*AssertionError` (with Got) / `op.AbortErr` the sequential path returns and fills `op.RowCount` / `CondVal` / `ExtraConds[].Val`. A database error of a statement is a real error; the recovery turn is `ROLLBACK; SELECT @sqlgen_p` (own transaction) and tells which operation it was (`@sqlgen_p == len(ops)` is the COMMIT: `*CommitError`). Inside the caller's transaction an assertion operation runs after `SAVEPOINT sqlgen_sp<i>` and a failed assertion costs one more turn, `ROLLBACK TO SAVEPOINT` (MySQL cannot do that inside a prepared statement), which undoes the failing operation and leaves the transaction usable. `RenderMySQLBatch` returns an error wrapping `ErrWireNotApplicable` for what the flavor cannot express: an argument that cannot be inlined (unsupported Go type, NaN, Inf), `RETURNING` on a server without it (MySQL), a column condition over a RETURNING statement, an isolation level without MySQL equivalent. The renderer pre-sizes its single `strings.Builder` with an upper bound of what it writes (literal sizes included), pinned by `FuzzEstimateMySQLOpBytesUpperBound` and the shape matrix.
   - For `WireFlavorPostgres`, calls `renderPipelineBatch(dialect, cache, ops)` to populate `ops[i].SQL` using at most 1 `strings.Builder`. `PipelineOp.WireResult()` tells a driver how to read each answer (command tag, scanned rows, guarded `(affected, ok)` row, discarded abort row). Conditions recorded by `set_config('sqlgen.c<ID>', ..., true)` are read back inside the same batch with `current_setting('sqlgen.c<ID>', true)`; several `If`s over one future record one condition each (`OpDecorators.ExtraConds`, CTE `_sqlgen_x<ID>`), and the driver reads each back (`ProducedCond.Val/Set`).
   - `Pipeline.Flush`/`Exec` called inside a `PipelineTx` callback returns `ErrFlushInPipelineTx`. A flush counts toward `CacheStats.WirePipelines` when a wire pipeliner ran it and `SequentialPipelines` when it ran statement by statement; an empty flush or one that fails while being built counts as neither.
   - `TranslateWireError` translates ONLY the guard's own marked error (see §6.1 item 3): `sqlgen:abort:` on an operation with `Decorators.IsAbort == true` into `op.AbortErr`, and `sqlgen:assert:<n>` on an operation with `Decorators.AssertRows >= 0` into a `*runtime.AssertionError` (matching `runtime.ErrAssertionFailed`) carrying the statement name (`op.Desc.Name`), the expected count and the actual count (`Got == n`, `GotKnown == true`). A genuine integer overflow (SQLSTATE 22003) or any other error of the caller's own statement passes through unchanged, identical to the sequential path.
2. **Sequential Fallback Mode (`*sql.DB` / `*sql.Tx`)**:
   - Executes operations sequentially through `StmtCache` (using Tier-2 prepared statements).
   - Evaluates `BranchGates` in Go via `cond.EvalGoFn()`: if any gate is false, calls `op.OnSkipped()` (the future then reports `ErrBranchNotTaken`: that error is the only way a future says "my operation did not run", there is no `Skipped()` accessor) and skips database execution for that operation.
   - **NoOp operations** (`PipelineOp.NoOp`, queued by `Pipeline.QueueExecNoop` / `QueueSliceNoop[T]`: an empty `INSERT ... VALUES` collection). They take part in branch gating and conditions like any operation (zero rows affected, `Skipped` when gated off) but have no statement. At flush, first the assertion of every NoOp outside any branch (`len(Input.BranchGates) == 0`) is decided client-side against `RowCount == 0`: a passing one is cleared (`AssertRows = -1`), a failing one returns its `*AssertionError` (`Got: 0, GotKnown: true`) before anything is sent. Then: if every operation is a *plain* NoOp (no `AssertRows`, no `ProduceCond`/`ExtraConds`/`LeafRecs`) the pipeline resolves them without touching the database (no connection, no transaction, nothing counted in `CacheStats`); otherwise a NoOp that still carries an assertion (inside a branch) or a condition forces the whole pipeline onto the sequential path (`noWire`), where it resolves with `RowCount == 0` in order; otherwise the NoOps are removed from the slice handed to `wire.Pipeliner` / `wire.TxPipeliner` (`wireOps`) and the results the driver wrote are copied back. A wire driver therefore never sees a NoOp.
   - If an active `IsAbort` operation is reached, returns `op.AbortErr` (triggering `tx.Rollback()`).
   - If an operation has `Decorators.AssertRows >= 0` and the actual affected/returned row count does not equal `AssertRows`, sets `op.Err` to a `*runtime.AssertionError{Statement, Want: AssertRows, Got: RowCount, GotKnown: true}` (matching `runtime.ErrAssertionFailed`) and returns it. The implicit transaction of a standalone `Pipeline`, and `PipelineTx`, roll back; a pipeline bound to a user-managed `*sql.Tx` (`WithTx(tx).Pipeline(ctx)`) does not: the error is returned, the transaction stays usable, the writes of the operations before the failing one stay in it, and the caller owns the rollback. A `wire.Pipeliner` must provide the same for a failed assertion or `Abort` inside a transaction it did not begin: `pgxwire` takes a `SAVEPOINT` before each guarded operation in the same batch, releases the first one at the end, and on a guard failure (not on a database error of the caller's statement, which aborts a PostgreSQL-protocol transaction on every path) rolls back to the failing operation's savepoint (one extra round-trip, error path only). The failing operation's own effect is discarded there, whereas the sequential path has already run it.
3. **`IsRetryableTxError(err error, duringCommit bool) bool`**:
   - Returns `false` if `err == nil`, `errors.Is(err, context.Canceled)`, `errors.Is(err, context.DeadlineExceeded)`, or `errors.Is(err, ErrAssertionFailed)`.
   - If `duringCommit == true` and `err` is a transport/connection drop (`errors.Is(err, driver.ErrBadConn)`, `errors.Is(err, io.EOF)`, `errors.Is(err, io.ErrUnexpectedEOF)`, or `net.Error`), returns `false`.
   - Returns `true` for SQLSTATE `"40001"`, `"40P01"`, `"55P03"`, MySQL codes `1213` / `1205`, TiDB codes `9007` (write conflict) / `8022` (KV error safe to retry, at commit), SQLite codes `(code & 0xFF) == 5` or `6`, and matching error substrings.

---

## 9. Code Generator Specification (`generator.go`)

For each statement `<Name>` in `sqlgen.yaml`, `Generate` emits into a single `bytes.Buffer`:
1. Tuple structs (`<TupleStruct>`), `<tupleStruct>ChunkPool`, `compare<TupleStruct>`, and `is<TupleStruct>Sorted` (when `HasSort == true`).
2. 1D slice presort helpers `is<Type>AscSorted` / `is<Type>DescSorted` (when `HasSort == true`).
3. Output row struct (`<RowType>`) and, per statement with unconditional output columns, the `<Stmt>Col<X>` bitmask constants (`1 << iota`) and `<Stmt>ColsAll`; per statement that declares `sqlgen.Order`, the helpers `<Stmt>SortCol` / `<Stmt>SortTerm` / `<Stmt>OrderBy<Col>` / `Order<Stmt>(...)` and (with `sqlgen.Seek`) `Seek<Stmt>(row <RowType>) runtime.Seek` (`Order<Stmt>` takes `<Stmt>SortTerm`, so the sort tokens of another statement do not compile; a repeated or out-of-range term yields `runtime.ErrInvalidOrder`). Nothing is keyed by the row type: statements sharing a `result_type` may declare different `sqlgen.Order` lists, each with its own `Seek<Stmt>`.
4. `<Name>Params` struct (when `StructArgs == true`, i.e. `args: struct`, or `args: auto` with `{?Flag: ...}` blocks).
5. Static `QueryDescriptor` (`<name>Desc`) and non-capturing `expand<Name>Args` function.
6. Currying constructors: `func <Name>Query(...) runtime.Query` (package-level only; stateless, so no `*Queries` method form). The generated file never declares aliases or wrappers of runtime symbols (no `type Query = runtime.Query`, no `WhereCol` helper): signatures spell `runtime.X`, and applications import `runtime` for the user-facing API. The only package-level names it declares are `Queries`, `Pipeline`, `New` and the per-statement names (`reservedTypeNames`).
7. Direct execution methods on `*Queries`:
   - `kind: exec`: `<Name>` (chunked when `CanBatch`), `<Name>Stream` (if `CanStream`).
   - `kind: one`: `<Name>`.
   - `kind: many`: `<Name>` and `<Name>Seq` (both chunked when `CanBatch`), `<Name>Stream`, `<Name>StreamSeq` (if `CanStream`; a `kind: exec` statement has `<Name>Stream` only). There are no other consumption or batch method variants: the base method chunks, and `runtime.Collect` / `AppendTo` / `Each` consume any iterator.
8. Pipeline queuing method on `*Pipeline`: `func (p *Pipeline) <Name>(...) *runtime.ExecFuture` / `*runtime.RowFuture[<RowType>]` / `*runtime.SliceFuture[<RowType>]`.

---

## 10. User Documentation Specification (`DOCS.md`)

Because the Clean-Room Dogfooding Protocol (Section 11) requires users/sub-agents to build complex applications relying **exclusively on `DOCS.md` and the `sqlgen` CLI**, `DOCS.md` MUST be authored as a comprehensive, accurate, self-contained user manual containing:
1. **Quickstart & CLI Reference**: Exact `go run ./cmd/sqlgen -config sqlgen.yaml` and `//go:generate` usage, `-check` mode for CI, and full `sqlgen.yaml` schema reference.
2. **Complete SQL DSL Cookbook**: Copy-pasteable examples and exact rules for:
   - Inline output columns (`SELECT id:int64, expr AS alias:string`) and single-column vs. struct return types.
   - Scalar parameters (`:ID:int64`, `:Payload:[]byte`), parameter deduplication when the same parameter name appears multiple times in one statement, 1D slices (`:IDs:[]int64`), and multi-row tuple slices (`:Users:[]NewUser(OrgID:int64, Email:string, NOW())`).
   - Deadlock-prevention presorting (`:IDs:[]int64 ASC`, `OrgID:int64 ASC, Email:string DESC`), PostgreSQL `::type` casts inside `VALUES` tuples (`ID:int64::bigint ASC`), and a clear note that `RETURNING` rows of a presorted multi-row `INSERT`/upsert are returned in presorted order (verified on PostgreSQL/CockroachDB by `integration/features_test.go`) while the caller's input slice remains unmutated, and that the returned order of `UPDATE`/`DELETE ... WHERE col IN (...) RETURNING` is unspecified.
   - Conditional blocks (`{?FlagName: ...}`) across `SELECT`, `JOIN`, and `WHERE`, and `SelectCols` bitmask projection (`UserColID | UserColEmail`).
   - Keyset pagination (`After:sqlgen.Seek` and `Sort:sqlgen.Order(col1, col2, id)`) and constructing sort orders via `Order<Stmt>(<Stmt>OrderBy<Col>.Asc(), ...)`.
   - Runtime query composition (`<Name>Query(...)` passed into `Sub:sqlgen.Query` slots in `WITH` CTEs or `WHERE IN`).
3. **Generated Go API & Execution Modes Reference**:
   - Reading: `<Name>`, `<Name>Seq`, and the `runtime.Collect` / `AppendTo` / `Each` helpers.
   - Chunking & Streaming: automatic chunking of the plain `<Name>`, `<Name>Stream`, `<Name>StreamSeq`, and `runtime.FromSeq` / `runtime.MapSeq2`; empty-collection rules (2.3).
   - Transactions & Retries: `q.WithTx(tx)`, `q.InTx(ctx, txOpts, fn, runtime.WithTxRetry(runtime.DefaultRetryPolicy()))`.
   - Pipelining & Conditional Control Flow: `q.Pipeline(ctx)`, `q.PipelineTx(...)`, futures (`ExecFuture`, `RowFuture[T]`, `SliceFuture[T]`), `.AssertRows(n)`, `.AssertFound()`, `p.If(cond, thenFn, elseFn)`, `WhereCol`, `p.Abort(err)`, and `IfResult.Taken()`.
   - Cache Tuning & Observability: `WithMaxPreparedStmts`, `WithMaxSQLVariants`, `WithMaxBatchParams`, `q.WithPrepared(false)` (the one way to disable Tier-2 prepared statements; a non-positive `WithMaxPreparedStmts` is ignored), and `q.CacheStats()`. The Tier-1 byte budget (`defaultMaxPartialBytes`, 16 MiB) and the promotion threshold (`defaultPromoteThreshold`, 2) are fixed constants; the runtime tests reach them through test-only options (`runtime/options_test.go`).

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
  - `PipelineTx` with `.AssertRows(1)`, `.AssertFound()`, and `p.If(cond, thenFn, elseFn)` (on sufficient balance: debit sender, credit receiver, insert transfer receipt; on insufficient balance: `elseFn` records a `rejected_insufficient_funds` audit event and commits without moving funds; on frozen account via `WhereCol`: `thenP.Abort(ErrAccountFrozen)` rolls back the entire transaction).
  - Deadlock-free bulk settlement via `UpsertAccountBalances` using composite presorting (`OrgID:int64 ASC, AccountID:int64 ASC`).
  - Contending concurrent transfers under `runtime.WithTxRetry(runtime.DefaultRetryPolicy())`.
- **Mandatory End-to-End Test (`examples/fintech_ledger/service_test.go`)**:
  - Initialize 20 accounts with `$200,000` total supply (`$10,000` each).
  - Spawn 16 goroutines concurrently executing random bidirectional transfers (`A -> B` and `B -> A`), bulk settlements, and frozen-account attempts.
  - Assert **strict conservation of money** ($\sum \text{balances} == \$200,000$), zero unhandled deadlock errors, and identical state transitions on both `wire.Pipeliner` and sequential `*sql.Tx`.

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
  - Bulk composite-key `SELECT` lookup (`GetInventoryBySKUs`).
  - Heavy-hitter batch promotion on `InsertOrderItems` (repeated `500`-row and `400`-row batches) followed by a `1400`-row spike that runs as one exact statement and is not promoted.
  - Streaming ETL pipeline: `StreamFulfilledOrdersSeq` $\rightarrow$ `runtime.MapSeq2` $\rightarrow$ `InsertArchivedOrdersStream` $\rightarrow$ `DeleteFulfilledOrdersByIDs` (with `:IDs:[]int64 ASC`).
- **Mandatory End-to-End Test (`examples/ecommerce_inventory/worker_test.go`)**:
  - Ingest 3,000 order items across recurring batch sizes, verify `q.CacheStats().Promotions` and `PreparedHits`, stream-archive 2,000 fulfilled orders in $O(1)$ memory, and verify `cmd/sqlgen -check` passes on all 4 example directories.

### 11.5. Dogfooding Persona 4: Multi-Asset Derivatives & Spot Clearing Exchange (`examples/clearing_exchange`)
- **Domain**: Institutional multi-asset clearing house, real-time margin & risk surveillance engine, atomic bilateral trade settlement with maker/taker fee rebates and liquidation protection, and regulatory trade settlement archival.
- **Features Road-Tested (Full Combinatorial Stress Test)**:
  - **3-Level Nested Curried `sqlgen.Query` Composition + Coupled Conditional Projections/JOINs/WHEREs + Bitmask `SelectCols` + 4-Column Keyset Pagination**:
    - Level 1 (`EligibleAccountsSubquery` with `{?FilterTiers: tier IN (:Tiers:[]string ASC)}`, `{?FilterJurisdictions: jurisdiction IN (:Jurisdictions:[]string DESC)}`, `{?ExcludeFrozen: status != 'FROZEN'}`, `{?RequireKYC: kyc_verified = 1}`) and Level 1 (`ActiveInstrumentsSubquery` with `{?FilterAssetClasses: asset_class IN (:AssetClasses:[]string ASC)}` and `{?MaxInitialMarginBps: initial_margin_bps <= :MaxMarginBps:int64}`).
    - Level 2 (`FilteredPositionsSubquery` composing both Level 1 subqueries via `WITH eligible_accts AS (...), active_insts AS (...)` plus `{?OnlyNonZero: p.net_quantity != 0}` and `{?MinNotionalExposure: ABS(p.net_quantity * i.mark_price_cents) >= :MinNotionalCents:int64}`).
    - Level 3 (`QueryRiskPositionsPage` composing Level 2 with coupled `{?IncludeAccountMeta: ...}` and `{?IncludeMarginMetrics: ...}` across `SELECT`, `JOIN`, and `WHERE`, bitmask `SelectCols`, `:After:sqlgen.Seek`, and 4-column `:Sort:sqlgen.Order(maintenance_margin_cents, unrealized_pnl_cents, account_id, position_id)`).
  - **3-Level Nested `PipelineTx` Control Flow (`If`/`Then`/`Else`, `RowFuture`, `SliceFuture.WhereCol`, `ExecFuture`, `Abort`, `UPDATE`/`DELETE`/`INSERT` Branch Guards)**:
    - Validates active instrument (`RowFuture`), checks pre-trade risk flags on both counterparties (`SliceFuture.WhereCol` $\rightarrow$ `Abort(ErrKillSwitchActive)`), verifies both counterparties' margin sufficiency (`ExecFuture` $\rightarrow$ `If/Else` recording `REJECTED_SELLER_MARGIN` or `REJECTED_BUYER_MARGIN`), and in the 3rd-level `Then` branch atomically debits/credits collateral, upserts buyer/seller derivative positions, deletes consumed resting orders, and inserts the immutable trade fill and audit log.
  - **Multi-Dimensional Presorted Batching & Streaming with Extra Scalar Parameters and `RETURNING`**:
    - `UpsertInstrumentMarks` (`:Marks:[]InstrumentMarkInput(Symbol:string ASC, ...)` + scalar `:UpdatedAt:int64` + `RETURNING` via the chunked plain method, `Seq`, `Stream`, `StreamSeq`).
    - `BatchAdjustCollateral` (`:Adjustments:[]CollateralAdjustment(DeskID:int64 ASC, AccountID:int64 DESC, DeltaCollateralCents:int64, DeltaLockedCents:int64)` + scalar `:UpdatedAt:int64` + `RETURNING`).
    - `LookupPositionsByKeys` (`:Keys:[]PositionKey(AccountID:int64 ASC, Symbol:string DESC)` + `{?ExcludeFlat: p.net_quantity != 0}`).
    - `BulkCancelOrdersByIDs` (`:OrderIDs:[]int64 ASC` + scalar `:CancelReason:string` + `RETURNING`).
    - Streaming settlement archival (`StreamUnarchivedFills` / `Seq` / `Each` $\rightarrow$ `runtime.MapSeq2` $\rightarrow$ `ArchiveSettlementStream` $\rightarrow$ `MarkFillsArchivedByIDs`).
- **Mandatory End-to-End Test (`examples/clearing_exchange/engine_test.go`)**:
  - Runs identically against both `SequentialTx` and `wire.Pipeliner`, verifying 12goroutine concurrent bilateral clearing + mark-to-market shocks + collateral sweeps, strict **zero-sum net position conservation** ($\sum \text{net\_quantity} \equiv 0$ per symbol), **exact collateral & fee conservation** ($\sum \text{available\_collateral\_cents} == \text{Initial} + \sum \text{Adjustments} - \sum \text{NetFees}$), and 16-direction 4-column keyset pagination equivalence over the 3-level nested subquery hierarchy.

---

## 12. Exhaustive Testing Discipline: Unit, Fault-Injection, PBT & Fuzzing

### 12.1. Six Engineering Verification Oracles
1. **AST & Static Invariant Gate (`TestSingleStringsBuilderASTAudit`)**:
   - Parse `runtime/*.go`, `generator.go`, and all generated `*_gen.go` files using `go/parser` and `go/ast`.
   - Assert `reflect` is never imported.
   - Assert `strings.Builder` is instantiated ONLY inside `BuildComposedSQL` (1) and `renderPipelineBatch` (1).
   - Assert `fmt.Sprintf`, `strings.Join`, and `bytes.Buffer` are never called in `runtime/sql_stream.go`.
2. **12-Schema Compilation Isolation Gate (`TestCompilationIsolationMatrix`)**:
   - Generate and type-check (`go/types`) 12 minimal orthogonal schemas (1 statement each: static `exec` 0 params, static `exec` 2 params, static `one` 1 col, static `one` 3 cols, `many` with `SelectCols` only, `many` with `WHERE` flag only, `many` with `SELECT`+`WHERE` flags, `exec` with 1D `ASC` slice, `exec` with tuple `ASC` slice, `many` with `Order` only, `many` with `Seek`+`Order`, `subquery` only) to guarantee zero unused imports or variables in any feature subset.
3. **Property-Based Testing (PBT) Suite (`runtime_test.go` & `generator_test.go`)**:
   - **PBT 1 (`TestPropertyEstimateOpBytesStrictUpperBound`)**: Across 2,000 randomized descriptors, slice/tuple lengths, flags, sort specs, and pipeline decorators, assert `len(renderedSQL) <= estimateOpBytes(...)` and `testing.AllocsPerRun == 1.0`.
   - **PBT 2 (`TestPropertyKeysetPaginationEquivalence`)**: Across 250 duplicate-heavy rows, all 16 sort-direction masks ($\{\text{ASC}, \text{DESC}\}^4$), and page sizes `1, 2, 7, 25, 250, 500`, assert keyset pagination returns the exact same row sequence as unpaginated SQL `ORDER BY`.
   - **PBT 3 (`TestPropertyPresortImmutabilityAndMonotonicity`)**: Across 500 randomized slice/tuple inputs, assert the caller's slice is never mutated and the emitted parameter sequence is strictly monotonic under the declared `ASC`/`DESC` comparator.
   - **PBT 4 (`TestPropertyBatchChunkDecompositionConservation`)**: Across batch sizes `1..25,000` and randomized promoted Tier-2 sizes (which must not influence the plan), assert $\sum \text{chunk}_i == N$, $\text{chunk}_0 == N$ when $N \le N_{\text{chunk\_max}}$, and every $\text{chunk}_i \times F + S \le \text{MaxWireParams}$.
   - **PBT 5 (`TestPropertyPipelineWireVsSequentialParity`)**:
     - Note on SQLite vs. PostgreSQL CTE Grammar: PostgreSQL/CockroachDB (`DialectDollar`) supports data-modifying CTEs (`WITH _sqlgen_op AS (UPDATE ...)`), whereas SQLite's grammar forbids `UPDATE`/`INSERT`/`DELETE` inside a `WITH` clause.
     - Therefore: (a) Test the exact PostgreSQL `renderPipelineBatch` CTE SQL generation (`WITH _sqlgen_op AS (...)`, `_sqlgen_chk`, `_sqlgen_cond`, `SlotBranchGuard`, `SlotInsertValuesOpen`) and wire-scanning state machine using the `DialectDollar` `mockdriver` `wire.Pipeliner`, and (b) For live SQLite differential parity between `wire.Pipeliner` and sequential `*sql.Tx`, implement a SQLite `wire.Pipeliner` test adapter that executes the queued `PipelineOp` batch over the SQLite connection (evaluating `SlotBranchGuard` / `SlotInsertValuesOpen` directly in SQLite SQL and recording `_sqlgen_cond` via connection-scoped SQLite custom functions `set_config` and `current_setting`), asserting identical database state and future outcomes between `wire.Pipeliner` and sequential `*sql.Tx`.
   - **Real-engine differentials (`integration/`, PostgreSQL 15/18 + CockroachDB, pgxwire vs plain pgx, in `PipelineTx`, standalone `Flush` and a user-managed tx)**: `TestRealEnginePipelineWireMatchesSequential` (named scenarios), `TestRealEnginePipelineGuardMatrix` (statement kind x assertion x 0-3 conditions x row present/absent), `TestRealEnginePipelineCompoundConditions` (conditions over several operations), `TestRealEnginePipelineUserOverflowIsNotAnAssertion`, `TestRealEnginePipelineUserTransactionAfterFailure`, `TestRealEnginePipelineArgumentsMatchPreparedStatements` (Go type x column type), and the seeded, bounded `TestRealEnginePipelineDifferentialFuzz` (`SQLGEN_FUZZ_SEEDS`, default 100 fixed seeds per mode and engine, about 300 random pipelines per engine in well under a minute; a failure prints the seed and the generated pipeline). The oracle is always the sequential path. The MySQL-protocol counterparts (`integration/wire_my_*_test.go`, MySQL 8.4/9.7 and MariaDB 10.11/12.3, mysqlwire vs plain go-sql-driver) are `TestRealEngineMySQLPipelineWireMatchesSequential`, `TestRealEngineMySQLPipelineGuardMatrix`, `TestRealEngineMySQLPipelineCompoundConditions`, `TestRealEngineMySQLPipelineArgumentLiterals` (Go type x column type x 9 session variants incl. NO_BACKSLASH_ESCAPES, ANSI_QUOTES, latin1, gbk, binary, a non-UTC loc, utf8mb4_bin) and `TestRealEngineMySQLPipelineDifferentialFuzz` (statements limited to what the engine supports: no RETURNING on MySQL, INSERT/DELETE only on MariaDB).
   - **Seeded differential tests of the generated code against an in-memory oracle (`integration/fuzz_*_test.go`, all seven server engines, each also through `pgxwire` on the PostgreSQL protocol, plus in-process SQLite)**: three generated fixtures with the same statements (`fzpg`, `fzmy`, `fzsl`; rendered with their Go adapters from one template each by `integration/fixtures`, so they cannot drift (`make check` verifies)) over one table, driven through the engine-agnostic `fzAPI`. The oracle is plain Go over rows the test wrote and reads back with hand-written SQL, never through the product. Every case derives from one seed (`SQLGEN_FUZZ_SEED=<n>` replays it; a failure prints it; `SQLGEN_FUZZ_SCALE=<n>` multiplies the case counts for soak runs). Covered: all 256 combinations of eight `{?Flag:}` blocks (shared scalars, 1D and tuple slices, NULL-able columns, enabled-but-empty lists -> `ErrEmptySliceParam`, disabled blocks ignoring their parameters) on the default, prepare-early and tiny evicting caches; every `SelectCols` subset; multi-row inserts (presorted ASC/DESC, unsorted, literal tuple templates; base method, `Stream`) and `IN` / tuple-`IN` lookups at sizes around the chunk boundary with small `WithMaxBatchParams` budgets; keyset pagination over random `Order` specs of 0..12 columns (mixed directions, tie-breaker named, implied or mid-list, int64 extremes, direction-flipped twin, random cursors) walked page by page against the oracle's total sort; composed sub-queries to depth 3 with flags, a slot referenced twice, conditional JOIN/columns; `UPDATE` / `DELETE` with `IN` lists (base method, chunked) and every supported `RETURNING` form; user transactions (`WithTx`) with commit and rollback; `Seq` iterators with `runtime.Collect` / `AppendTo` / `Each` including early exit and cancelled contexts (no connection left checked out); concurrent readers sharing one `Queries`; SQL-looking and non-ASCII strings round-tripping. Text columns hold lowercase ASCII only because engines collate text differently; every other sort key is numeric.
4. **Native Go Fuzzing Suite (`testing.F`)**:
   - `FuzzParseSQL` (`parse_internal_test.go`): seeded with valid and malformed annotated SQL, including every retired spelling; asserts zero panics and that whatever `Generate` accepts is syntactically valid Go.
   - `FuzzParseYAML`: Fuzzes `ParseYAML` across arbitrary byte inputs; asserts zero panics.
   - `FuzzEstimateOpBytesUpperBound`: Fuzzes SQL rendering inputs; asserts `len(sql) <= estimateOpBytes`.
   - `FuzzVariantKeyAndSubqueryShapeBijectivity`: Fuzzes `MakeVariantKey` and `ComputeSubqueryShape`; asserts zero collisions whenever `IsExact() == true`.
   - `FuzzStmtCacheAdversarialStateTransitions`: Fuzzes cache operations (hits, misses, promotions, demotions, byte-budget overflows); asserts internal doubly-linked list invariants (`partialProb.len + partialProt.len == count(partialItems)`, `preparedProb.len + preparedProt.len <= maxPrepared`, and `shard.partialBytes == sum(len(e.sqlStr))` for all Tier-1 entries).
5. **Deterministic Fault-Injecting & Handle-Counting Driver Suite (`mockdriver` in `runtime_test.go`)**:
   - Inject faults at `PrepareContext`, `BeginTx`, `ExecContext`, `QueryContext`, `Rows.Next` (mid-stream), `Rows.Close`, `Stmt.Close`, `Tx.Commit` (`40001` retryable vs. `driver.ErrBadConn`/`io.EOF` non-retryable), and user closure `panic` inside `InTx` / `PipelineTx`.
   - Assert `mockdriver.OpenStmts.Load() == 0` after demotions, GC cleanup of orphaned `q.WithTx(tx)` (`runtime.AddCleanup`), and `q.Close()`.
   - Assert singleflight promotion under a 64-goroutine thundering herd calls `PrepareContext` **exactly once**.
6. **Resource & Concurrency Gates**:
   - **`MaxOpenConns(1)` Deadlock Gate**: Run all read, batch, streaming (`Seq`/`StreamSeq` with early `break`), `InTx`, and `PipelineTx` operations on a live SQLite DB with `db.SetMaxOpenConns(1)`.
   - **Zero-Allocation Hot-Path Gate**: Verify via `testing.AllocsPerRun` that `MakeVariantKey`, `PackOrder`/`expandOrder`, `acquireArgs`/`ReleaseArgs`, `AcquireIndices`/`ReleaseIndices`, and Tier-1/Tier-2 cache hits allocate `0` heap objects on the cache/pool path.

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

# 6. Nested modules (real engines; `. scripts/engines-env.sh` exports the DSNs of `make engines-up`)
(cd pgxwire && go vet ./... && go test -race -count=1 -cover ./...)        # >= 90% with the PG/CRDB/YugabyteDB engines up
(cd mysqlwire && go vet ./... && go test -race -count=1 -cover ./...)      # >= 90% with the MySQL/MariaDB/TiDB engines up
(cd integration && SQLGEN_REQUIRE_ENGINES=1 go test -race -count=1 -timeout 15m ./...)
# Real engines (docker-compose.yml, scripts/engines-env.sh, CI matrix): postgres15 postgres18 cockroach yugabyte
# mysql84 mysql97 mariadb1011 mariadb123 tidb. YugabyteDB (PostgreSQL family, dialect postgres) runs the full suite
# per push (SMOKE=1 is only a quick local subset); .github/workflows/nightly.yml adds a larger-scale fuzz soak;
# TiDB (MySQL family, dialect mysql, no RETURNING) runs the full suite per push.
```