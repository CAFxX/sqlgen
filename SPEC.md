# `sqlgen` — Production Specification (`SPEC.md`)

## 1. Objective & Core Design Principles

`sqlgen` is a Go SQL code generator and runtime library that compiles a YAML specification of SQL statements into strongly-typed, zero-reflection Go methods backed by `database/sql`.

### 1.1. Non-Negotiable Requirements
1. **100% `database/sql` Compatibility**: Works seamlessly with any standard `database/sql` driver (PostgreSQL, SQLite, MySQL, MariaDB, CockroachDB) across both implicit transactions (`*sql.DB`) and explicit transactions (`*sql.Tx`).
2. **Inline Typed SQL DSL**: All column types, input parameter types, 1D slices, multi-row tuple slices, conditional SQL blocks, keyset pagination predicates, custom multi-column ordering, and composable subqueries are expressed directly inside the SQL text.
3. **Sharded Two-Tier Adaptive Statement Cache (W-TinyLFU + SLRU)**:
   - **Tier 1 (Partially Prepared / Staged SQL)**: Caches dynamically constructed SQL strings in memory (`MaxSQLVariants`, `MaxPartialBytes`). Avoids repeated SQL string formatting without consuming database server resources.
   - **Tier 2 (Fully Prepared `*sql.Stmt`)**: Caches reference-counted `*sql.Stmt` handles (`MaxPreparedStmts`). Frequently and recently used query variations (across slice arities, batch sizes, conditional flags, column subsets, sort orders, and composed subqueries) are adaptively promoted from Tier 1 to Tier 2, and demoted back to Tier 1 (closing the `*sql.Stmt` when its reference count hits zero while retaining the warm SQL string) when superseded.
4. **Dual Resultset Consumption**: Every multi-row query supports both slice materialization (`<Name>`, `<Name>Into`) and zero-allocation lazy row streaming (`<Name>Seq` returning `iter.Seq2[Row, error]`, and `<Name>Each`).
5. **Runtime Query Composition**: Every statement generates a non-executing currying constructor (`<Name>Query(...) sqlgen.Query`) that binds and validates typed arguments and can be passed into outer queries (e.g., `WITH cte AS (Sub:sqlgen.Query)` or `WHERE id IN (Sub:sqlgen.Query)`).
6. **Adaptive Multi-Row Batch Execution**: Statements with a tuple-slice parameter (`VALUES Rows:[]RowStruct(...)`) generate `<Name>Batch`, `<Name>BatchInto`, `<Name>BatchSeq`, and `<Name>BatchEach` methods that combine heavy-hitter exact-size promotion (e.g., preparing `1000`-row or `900`-row batches once recurring), greedy reuse of prepared Tier-2 sizes, and power-of-2 fallback chunking (`256, 128, 64, 32, 16, 8, 4, 2, 1`).
7. **Minimal Runtime Allocations**:
   - Zero `reflect` usage in generated code or `sqlgen/runtime`.
   - Pooled `*[]any` argument buffers (`sync.Pool` of slice pointers) cleared via `clear()` on release.
   - Stack-allocated `[NumCols]any` scan destination buffers initialized once before `for rows.Next()`.
   - Value-type 256-bit `VariantKey` (`[4]uint64`) for zero-allocation cache lookups.

---

## 2. Repository & Package Layout

Target Go version: **Go 1.24+**

```text
.
├── go.mod
├── runtime/
│   ├── runtime.go          # Core types, VariantKey, ArgPool, BuildComposedSQL, Seek/Order
│   ├── cache.go            # Sharded Two-Tier W-TinyLFU + SLRU StmtCache, ManagedStmt, TxStmtCache
│   └── runtime_test.go     # Unit, concurrency, promotion/demotion, and benchmark tests
├── generator.go            # YAML config loader, lexical SQL parser, validator, Go code emitter
├── generator_test.go       # Parser, validator, codegen, and end-to-end SQLite integration tests
└── cmd/
    └── sqlgen/
        └── main.go         # CLI entrypoint for //go:generate
```

External dependencies:
- `go.yaml.in/yaml/v3` (for strict YAML parsing)
- `modernc.org/sqlite` (test-only dependency for end-to-end execution and benchmarks)

---

## 3. YAML Configuration Schema (`sqlgen.yaml`)

### 3.1. Structure
```yaml
version: "1"
package: store
out: store_gen.go
runtime_import: [example.com/sqlgen/runtime](https://example.com/sqlgen/runtime)
dialect: postgres # Aliases: dollar, postgres, postgresql, pgx, cockroach, cockroachdb, $ | question, sqlite, sqlite3, mysql, mariadb, ?
extra_imports:
  - time

statements:
  - name: GetUserByID
    kind: one               # One of: exec, one, many, subquery (alias: query)
    result_type: User       # Optional custom struct/type name (invalid on kind: exec or subquery)
    params_struct: false    # Optional; forced true automatically if statement has {?Flag: ...} blocks
    sql: |
      SELECT id:int64, email:string, role:string, created_at:time.Time
      FROM users
      WHERE id = ID:int64
    params: []              # Optional fallback parameter declarations
    columns: []             # Optional fallback output column declarations
```

### 3.2. Strict Validation Rules
1. `ParseYAML` MUST use `yaml.NewDecoder` with `dec.KnownFields(true)`.
2. `Version` MUST be `""` or `"1"`.
3. `PackageName` and each statement `Name` MUST be valid Go identifiers. Duplicate statement names MUST be rejected.
4. `ColumnSpec` and `ParamSpec` MUST support 3 YAML forms while rejecting unknown keys with line numbers:
   - Mapping: `{ name: ID, type: int64 }` (and `slice: true` for `ParamSpec`)
   - Single-key shorthand map: `- ID: int64` (or `- org_ids...: int64`)
   - Compact scalar string: `- "ID:int64"` (or `- "org_ids...:int64"`)
5. Standard library imports (`time`, `encoding/json`, `math/big`, `net/netip`, `net/url`, `database/sql/driver`) MUST be auto-detected when their package prefix (`time.`, `json.`, etc.) appears in any parameter, tuple field, or column type, and deduplicated alongside `extra_imports`. `iter` MUST be auto-imported whenever any statement has `kind: many`.

---

## 4. Formal SQL DSL Grammar & Lexical Scanner

### 4.1. Protected Lexical Contexts (Copied Verbatim)
The scanner processes the trimmed SQL string in a single pass. The following constructs MUST be recognized first and copied verbatim into the current SQL segment without interpreting any `:` or `{?...}` inside them:
1. **Single-Quoted String Literals**: `'...'` (with `''` escaped quotes).
2. **Double-Quoted Identifiers**: `"..."` (with `""` escaped quotes).
3. **Backtick-Quoted Identifiers**: `` `...` `` (with `` `` `` escaped backticks).
4. **PostgreSQL Dollar-Quoted String Literals**: `$$...$$` or `$tag$...$tag$`, where `tag` consists of zero or more `[a-zA-Z0-9_]` characters and **does not start with a digit `[0-9]`** (so positional `$1` parameters are unaffected). An unclosed dollar-quoted literal MUST return a parse error.
5. **Single-Line Comments**: `--` up to `\n` or `EOF`.
6. **Multi-Line Block Comments**: `/*` up to `*/`. An unclosed block comment MUST return a parse error.
7. **Bracketed Expressions**: `[` up to matching `]`, tracked via `bracketDepth`. Colons inside brackets (e.g., PostgreSQL array slice `arr[1:3]`) MUST NOT be parsed as type annotations.
8. **Colon Operators**: `::` (PostgreSQL typecast) and `:=` (assignment) MUST be consumed atomically (2 bytes) and written verbatim to the current SQL segment.

### 4.2. EBNF Grammar (Outside Protected Contexts)
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
tuple_field        = identifier , ":" , scalar_go_type ;

order_directive    = [ ":" ] , identifier , ":" , ( "sqlgen.Order" | "runtime.Order" ) , "(" , order_col_list , ")" ;
order_col_list     = sql_col_ref , { "," , sql_col_ref } ;
sql_col_ref        = [ identifier , "." ] , identifier ;

seek_directive     = [ ":" ] , identifier , ":" , ( "sqlgen.Seek" | "runtime.Seek" ) ;
query_param        = [ ":" ] , identifier , ":" , ( "sqlgen.Query" | "runtime.Query" | "Query" ) ;
typed_token        = [ ":" ] , identifier , [ "..." ] , ":" , go_type , [ "..." ] ;
yaml_ref_param     = ":" , identifier , [ "..." ] ;

identifier         = ( letter | "_" ) , { letter | digit | "_" } ;
go_ident           = identifier ;
go_type            = { "[]" | "*" } , [ identifier , "." ] , identifier ;
scalar_go_type     = [ "[]" | "*" ] , [ identifier , "." ] , identifier ;
```

### 4.3. Context-Sensitive Disambiguation of `typed_token` (`SELECT` / `RETURNING` vs. Input Parameter)
The scanner tracks:
- `parenDepth` (incremented on `(`, decremented on `)`)
- `bracketDepth` (incremented on `[`, decremented on `]`)
- `seenMainSel` (`bool`, initially `false`)
- `inProjection` (`bool`, initially `false`)

**Keyword State Transitions** (when scanning an unquoted word `W` with no leading `:`, not followed by `:`, at `parenDepth == 0` and `bracketDepth == 0`):
- `lower(W) == "select"` and `!seenMainSel` $\rightarrow$ `inProjection = true`, `seenMainSel = true`.
- `lower(W) == "returning"` $\rightarrow$ `inProjection = true`.
- `inProjection == true` and `lower(W)` is in `{"from", "into", "where", "group", "having", "window", "order", "limit", "offset", "fetch", "for", "union", "intersect", "except"}` $\rightarrow$ `inProjection = false`.

**Classification Rule**:
A `typed_token` ending at offset `pos` is classified as an **Output Column** if and only if ALL of the following hold:
1. No leading `:` before `identifier`.
2. No `...` before or after `go_type`.
3. `go_type` is not a directive (`Query`, `Seek`, `Order`).
4. `inProjection == true`, `parenDepth == 0`, and `bracketDepth == 0`.
5. `isEndOfProjectionItem(span, pos)` returns `true`.

**`isEndOfProjectionItem(span, pos)` Algorithm**:
Starting at `pos`, loop to skip:
- Whitespace (`' '`, `'\t'`, `'\r'`, `'\n'`)
- Line comments `--...\n`
- Block comments `/*...*/`
- **Conditional blocks `{?...}`**: If `span[i] == '{'` and `span[i+1] == '?'`, advance `i` past the matching closing `}` (respecting single-quoted strings inside the block) and continue skipping whitespace/comments/blocks.
After skipping all of the above:
- Return `true` if `i >= len(span)`, `span[i] == ','`, or `span[i] == ';'`.
- If `span[i]` starts an identifier word `W` (and `W` is not followed by `:`), return `true` if `lower(W)` is in the projection terminator keyword set (`from`, `into`, `where`, etc.).
- Otherwise return `false` (meaning the token is followed by an operator or `AS`, e.g., `SELECT price * Tax:float64 AS total:float64`, so `Tax:float64` is an **Input Parameter**).

### 4.4. Dynamic Projection Slot (`SlotSelectCols`) for Unconditional Output Columns
When a statement has `UseParamsStruct == true`, `len(UncondCols) > 1`, and inline unconditional columns in `SELECT` (or `RETURNING`):
1. The unconditional columns MUST NOT be baked into the static `Segments` string.
2. Instead, each unconditional column's full SQL projection expression (e.g., `u.id`, `COALESCE(o.display_name, o.slug) AS org_name`) is stored in `QueryDescriptor.SelectColExprs` (in declaration order `0 .. len(UncondCols)-1`).
3. In the top-level `QueryDescriptor`, the entire span of unconditional column expressions between `SELECT ` (or `RETURNING `) and the first conditional column block `{?...}` or terminator keyword (`FROM`, etc.) is replaced by a single `SlotSelectCols` slot.
4. At runtime in `writeDescriptorSQL`:
   - Compute `validMask := (uint64(1) << uint(len(desc.SelectColExprs))) - 1`.
   - Compute `effectiveMask := in.ColMask & validMask`.
   - If `effectiveMask == 0`, set `effectiveMask = validMask` (all unconditional columns are selected).
   - Emit the comma-separated `desc.SelectColExprs[i]` for every bit `i` set in `effectiveMask`.
5. At runtime in `scanDest` setup (before `rows.Next()` or `row.Scan`):
   - Compute the exact same `effectiveMask`: if `(p.SelectCols & AllUncondColsMask) == 0`, include `&item.<Col>` for all unconditional columns; otherwise include `&item.<Col>` only for bits set in `p.SelectCols & AllUncondColsMask`.
   - This guarantees the number of projected SQL columns and the number of `rows.Scan` destination pointers are always identical.

### 4.5. Construct-Specific Rules
1. **Scalar vs. 1D Slice Normalization**:
   - `[]byte` and `[]uint8` (without `...`) are **scalar** parameters (`SlotScalar`, Go type `[]byte` / `[]uint8`).
   - Any other `[]T` (including `[][]byte`, `[]int64`, `[]string`) or any type annotated with `...` is a **1D Slice** parameter (`SlotSlice`, element type `T`, Go parameter type `[]T`).
   - Passing an empty slice (`len(s) == 0`) to an active `SlotSlice` or `SlotTupleSlice` MUST immediately return `runtime.ErrEmptySliceParam` without querying the database.
2. **Multi-Row Tuple Slice (`Param:[]StructName(body)`)**:
   - `parseTupleBody` extracts `Field:GoType` tokens inside `body` as fields of `StructName`, preserving all surrounding SQL text (including PostgreSQL casts like `::bigint` and SQL functions like `NOW()`) in `TupleDescriptor.Segments` (`len(Segments) == len(Fields) + 1`).
   - Nested parentheses inside `body` (such as `COALESCE(Name:*string, 'default')` or `NOW()`) MUST be preserved inside the current tuple segment.
3. **Explicit Conditional Blocks (`{?FlagName: body }`)**:
   - Nested `{?...}` blocks inside `{?...}` MUST be rejected at compile time.
   - Multiple `{?FlagName: ...}` blocks with the same `FlagName` in a statement share the same bit in `flagMask` and the same `FlagName bool` field on `<StmtName>Params`.
   - When `p.<FlagName> == false` at runtime:
     - The block's SQL segments and inner slots are skipped during SQL rendering (`cur.sliceIdx`, `cur.tupleIdx`, `cur.subIdx` are advanced past skipped inner slots).
     - Inner parameters are NOT validated (e.g., an empty slice or uninitialized `sqlgen.Query` inside an inactive block MUST NOT return an error), are NOT appended to `argBuf`, and contribute `0` to `VariantKey` dimensions.
     - Inner output columns are NOT appended to `scanDest`.
4. **Paired Keyset Pagination (`Seek:sqlgen.Seek`) & Custom Ordering (`Order:sqlgen.Order(...)`)**:
   - Every column reference in `sqlgen.Order(c_1, ..., c_k)` MUST match an output column in `SELECT` (by `SQLExpr` or unaliased identifier).
   - **Nullable Sort Column Restriction**: If a statement uses `sqlgen.Seek`, every column listed in `sqlgen.Order(...)` MUST have a non-pointer, non-nullable Go type (i.e., types starting with `*` or `sql.Null` MUST be rejected at compile time with an error instructing the user to wrap the column in `COALESCE(col, default) AS alias:type`).
   - Up to 15 sortable columns may be declared in `sqlgen.Order(...)`; up to 7 sort terms may be packed per call via `Order<RowType>(terms ...uint8)`. The final column `c_k` is the unique tie-breaker and is automatically appended by `ExpandOrder` if omitted by the caller (inheriting the direction of the preceding sort column, or `ASC` if `OrderSpec == 0`).

### 4.6. Identifier Casing & Type Validation
- `toGoFieldName`: Converts `snake_case` or `camelCase` to exported `PascalCase`, uppercasing common initialisms (`ID`, `IDs`, `URL`, `URLs`, `URI`, `UUID`, `UUIDs`, `SQL`, `API`, `HTTP`, `HTTPS`, `IP`, `JSON`, `XML`, `HTML`, `CPU`).
- `toGoParamName`: Same word splitting, with the first word in all-lowercase (`OrgIDs` $\rightarrow$ `orgIDs`, `ID` $\rightarrow$ `id`).
- Unqualified Go types starting with a lowercase ASCII letter MUST be validated against Go's 18 predeclared types (`bool`, `string`, `int`, `int8`, `int16`, `int32`, `int64`, `uint`, `uint8`, `uint16`, `uint32`, `uint64`, `uintptr`, `byte`, `rune`, `float32`, `float64`, `any`). Unknown lowercase types MUST fail code generation with a Levenshtein distance suggestion (e.g., `stirng` $\rightarrow$ `did you mean "string"?`).

---

## 5. Runtime Engine & Sharded Two-Tier Adaptive Cache (`sqlgen/runtime`)

### 5.1. Sharded Cache Architecture (`StmtCache`)
To eliminate mutex contention under high concurrency, `StmtCache` MUST be partitioned into **16 shards** (`const numCacheShards = 16`), selected by `shardIdx = hash & (numCacheShards - 1)`.

Each shard (`cacheShard`) independently maintains:
- `mu sync.Mutex`
- `sketch tinyLFUSketch` (4-bit Count-Min Sketch, `[256]uint64` = 4,096 counters with periodic bitwise halving `(w >> 1) & 0x7777777777777777`)
- `byKey map[VariantKey]*cacheEntry` (for exact keys where `key.IsExact() == true`)
- `bySQL map[string]*cacheEntry` (for inexact keys where `key.IsExact() == false`)
- **Tier 1 (Partially Prepared SLRU)**: `partialProb` (20% capacity) and `partialProt` (80% capacity), bounded by per-shard `maxPartial` entry count AND per-shard `maxPartialBytes` string memory budget (default 16 MB total / 1 MB per shard; single SQL strings larger than `256 KB` bypass Tier 1 storage unless promoted to Tier 2).
- **Tier 2 (Fully Prepared SLRU)**: `preparedProb` (20% capacity) and `preparedProt` (80% capacity), bounded by per-shard `maxPrepared` entry count (default 256 total / 16 per shard).

### 5.2. W-TinyLFU Admission, Promotion, Demotion, and Recycling Rules
1. **Cache Hit in Tier 2 (`entry.ms.Load() != nil`)**:
   - Increment `sketch.increment(entry.hash)`.
   - Call `ms.Acquire()` (+1 reference for the caller).
   - Move `entry` to `preparedProt` MRU head (if in `preparedProb`, promote to `preparedProt` and step down `preparedProt.tail` to `preparedProb.head` if `preparedProt` exceeds 80%).
   - Increment `preparedHits`. Return `(ms, "", nil)`.
2. **Cache Hit in Tier 1 (`entry.ms.Load() == nil`)**:
   - Increment `freq := sketch.increment(entry.hash)`.
   - Move `entry` to `partialProt` MRU head (stepping down `partialProt.tail` to `partialProb.head` if needed).
   - **Tier 1 $\rightarrow$ Tier 2 Promotion Check**:
     - Let `canPrepareOnDB := txCache == nil || db.Stats().MaxOpenConnections != 1`.
     - If `canPrepareOnDB && !entry.preparing && freq >= promoteThreshold`:
       - If Tier 2 is full (`preparedProb.len + preparedProt.len >= shard.maxPrepared`), check whether `freq > sketch.estimate(preparedVictim.hash)`.
       - If eligible: set `entry.preparing = true`, unlock shard, call `db.PrepareContext(ctx, entry.sqlStr)`, wrap in `newMS := newManagedStmt(prepared, 2)` (`1` for Tier-2 residency + `1` for current caller), relock shard, demote `preparedVictim` to `partialProt.head` (via `demoteToPartialLocked`), store `newMS` in `entry.ms` (and `fastSlot` if non-nil), move `entry` from Tier 1 to `preparedProb.head` (decrementing `partialBytes` while in Tier 2), unlock shard, and call `demotedMS.Release()` outside the lock.
   - Otherwise, unlock shard, increment `partialHits`, and return `(nil, entry.sqlStr, nil)` (0 SQL string building).
3. **Cache Miss (Unseen Variation)**:
   - Increment `freq := sketch.increment(h)`.
   - Render `sqlStr = buildSQL()` outside the shard lock.
   - If `promoteThreshold == 1 && canPrepareOnDB`: immediately prepare on `db` and insert into Tier 2 (`preparedProb.head`), demoting a Tier-2 victim if full.
   - Otherwise, run **Tier-1 W-TinyLFU Admission**:
     - If Tier 1 is at capacity (`count >= shard.maxPartial` or `partialBytes + len(sqlStr) > shard.maxPartialBytes`), compare `freq` against `victimFreq := sketch.estimate(partialVictim.hash)`.
     - If `freq < victimFreq`: **reject admission** and return `(nil, sqlStr, nil)` directly (0 map churn, 0 node allocation).
     - If `freq >= victimFreq`: evict `partialVictim`, **recycle its `*cacheEntry` struct in-place**, insert into `byKey`/`bySQL` and `partialProb.head`, and evict any additional tail entries if needed to satisfy `maxPartialBytes`.

### 5.3. Bijective `VariantKey` & `ComputeSubqueryShape` (Zero Hash-Collision Guarantee)
1. `MakeVariantKey(stmtID uint32, colMask, flagMask, orderKey uint64, dims ...uint64) VariantKey`:
   - Packs `stmtID` (15 bits), `colMask` (24 bits), `flagMask` (24 bits) into `Word 0`; `orderKey` into `Word 1`; and up to two 64-bit `dims` (or four 32-bit `dims`) into `Word 2` and `Word 3`.
   - If any input exceeds its bit width, `len(dims) > 4`, or any `dim` has `InexactKeyBit` (`1 << 63`) set, `Word 0` MUST have `InexactKeyBit` set (`key.IsExact() == false`), forcing `StmtCache` to key by `sqlStr` in `bySQL`.
2. `ComputeSubqueryShape(stmtID uint32, colMask, flagMask, orderKey uint64, hasNestedSubs bool, sliceOrTupleLens ...int) uint64`:
   - If `hasNestedSubs == true`, `orderKey != 0`, `stmtID == 0 || stmtID > 0x7FFF`, `colMask > 0xFF`, `flagMask > 0xFF`, or `len(sliceOrTupleLens) > 2`, return `InexactKeyBit | (hash & ^InexactKeyBit)`.
   - If `len(sliceOrTupleLens)` has 1 or 2 lengths and any length is `< 0 || > 0xFFFF`, return `InexactKeyBit | (hash & ^InexactKeyBit)`.
   - Otherwise, bijectively pack all fields into 63 bits with `InexactKeyBit == 0`:
     `uint64(stmtID)<<48 | (colMask&0xFF)<<40 | (flagMask&0xFF)<<32 | uint64(s0)<<16 | uint64(s1)`.

### 5.4. Reference-Counted `ManagedStmt` & `TxStmtCache` Lifetime Rules
1. **`StmtCache.Query` Signature**:
   ```go
   func (c *StmtCache) Query(
       ctx context.Context,
       db *sql.DB,
       txCache *TxStmtCache,
       usePrepared bool,
       key VariantKey,
       stmtID uint32,
       shapeTag uint64,
       batchSize int,
       fastSlot *atomic.Pointer[ManagedStmt],
       buildSQL func() string,
       args ...any,
   ) (*sql.Rows, *ManagedStmt, error)
   ```
   - `StmtCache.Query` MUST NOT call `defer ms.Release()` before returning `*sql.Rows`.
   - If `ms.stmt.QueryContext` fails with an error, `StmtCache.Query` calls `ms.Release()` immediately and returns `(nil, nil, err)`.
   - On success, `StmtCache.Query` returns `(rows, ms, nil)`. The generated caller (`<Name>Into`, `<Name>Seq`, `<Name>Each`, `<Name>Batch*`) MUST close `rows` first and then call `if ms != nil { _ = ms.Release() }`.
2. **`TxStmtCache` Safe Reference Management**:
   - When `TxStmtCache.StmtFromManaged(ctx, ms)` binds a `ManagedStmt` to `tc.tx` via `tc.tx.StmtContext(ctx, ms.stmt)` for the first time in that transaction, it caches the resulting `*sql.Stmt` in `tc.byBase[ms]`.
   - To ensure `ms.stmt` is never closed while the transaction is open—even if the user called `q.WithTx(tx)` manually without `q.InTx`—`NewTxStmtCache(tx)` MUST allocate an internal `txState` holding `heldMS []*ManagedStmt` and register a `runtime.AddCleanup(tc, func(st *txState) { st.releaseAll() }, state)` (in addition to `tc.Close()` called deterministically by `q.InTx`).

---

## 6. Code Generation Specification (`sqlgen`)

### 6.1. Generated Methods per Statement Kind
For every statement `<Name>`:
1. **Currying Constructor (all kinds except `exec`)**:
   - Package function `func <Name>Query(...) Query` and method `func (q *Queries) <Name>Query(...) Query`.
   - Populates `runtime.RenderInput` (including `ColMask`, `FlagMask`, `OrderSpec`, `HasSeek`, `SliceLens`, `TupleLens`, `Subs`) and binds a non-capturing static expander function `expand<Name>Args(dst *[]any, q *runtime.Query) error` that handles `SlotScalar`, `SlotSlice`, `SlotTupleSlice`, `SlotQuery`, `SlotOptBlock`, and `SlotSeek`.
2. **`kind: exec`**:
   - `func (q *Queries) <Name>(ctx context.Context, ...) (sql.Result, error)`
   - If `CanBatch`: `func (q *Queries) <Name>Batch(ctx context.Context, ...) (sql.Result, error)`
3. **`kind: one`**:
   - `func (q *Queries) <Name>(ctx context.Context, ...) (<RowType>, error)`
4. **`kind: many`**:
   - `func (q *Queries) <Name>(ctx context.Context, ...) ([]<RowType>, error)`
   - `func (q *Queries) <Name>Into(ctx context.Context, dst []<RowType>, ...) ([]<RowType>, error)`
   - `func (q *Queries) <Name>Seq(ctx context.Context, ...) iter.Seq2[<RowType>, error]`
   - `func (q *Queries) <Name>Each(ctx context.Context, ..., fn func(<RowType>) error) error`
   - If `CanBatch` (single `SlotTupleSlice`, no `SlotSlice`, no `SlotQuery`, no `Order`/`Seek`):
     - `func (q *Queries) <Name>Batch(ctx context.Context, ...) ([]<RowType>, error)`
     - `func (q *Queries) <Name>BatchInto(ctx context.Context, dst []<RowType>, ...) ([]<RowType>, error)`
     - `func (q *Queries) <Name>BatchSeq(ctx context.Context, ...) iter.Seq2[<RowType>, error]`
     - `func (q *Queries) <Name>BatchEach(ctx context.Context, ..., fn func(<RowType>) error) error`

### 6.2. Batch Transaction Semantics
- **`<Name>Batch` and `<Name>BatchInto`** (non-yielding):
  - Evaluate `firstChunk := q.cache.PlanBatchChunk(stmtID, colMask, flagMask, totalRows, maxRowsPerStmt, true)`.
  - If `q.txCache == nil && firstChunk < totalRows`, automatically start an internal transaction (`tx, err := q.db.BeginTx(ctx, nil)`), execute all chunks against `txCache = runtime.NewTxStmtCache(tx)` (with `defer txCache.Close()`), and commit at the end (rolling back on error).
  - If `firstChunk == totalRows` (e.g., power-of-2 $\le 256$ or promoted heavy-hitter size like `1000`), execute directly on `q.db` in a single atomic statement without `BeginTx`/`Commit`.
- **`<Name>BatchSeq` and `<Name>BatchEach`** (yielding streaming iterators):
  - Execute chunks using `q.txCache` as-is (do NOT open a hidden transaction when `q.txCache == nil`). This ensures early `break` from a `for range` loop never silently rolls back already-yielded rows and nested queries inside the loop body never deadlock `MaxOpenConns(1)`. Callers wanting multi-chunk atomic streaming wrap the loop in `q.InTx(...)`.

---

## 7. Verification, Testing & Benchmark Suite Requirements

The implementation is not complete until all of the following tests and benchmarks pass under `go test -race -v ./...`:

1. **Parser & Validator Unit Tests (`generator_test.go`)**:
   - Protected context tests: colons and `{?Flag:...}` inside `'...'`, `"..."`, `` `...` ``, `$$...$$`, `$tag$...$tag$`, `--`, `/*...*/`, `arr[1:3]`, `::bigint`, and `:=`.
   - Projection disambiguation tests: `SELECT` columns immediately preceding `{?Flag: , col:type}`, arithmetic expressions with input parameters before `AS alias:type`, CTE `WITH q AS (SELECT ...)` followed by main `SELECT`.
   - Compile-time error tests: typoed types (`stirng` $\rightarrow$ suggests `string`), nullable `*string` column in `sqlgen.Order` when paired with `sqlgen.Seek`, duplicate column names, conflicting shared `result_type` structs, nested `{?...}` blocks.
2. **End-to-End SQLite Integration Tests (`generator_test.go`)**:
   - Fixed-arity `exec`, `one`, `many` (`Slice`, `Into`, `Seq` with early `break`, `Each`) in both PostgreSQL (`$1`) SQL-generation tests and live SQLite (`?`) execution tests.
   - `SelectCols` bitmask subsetting: verify that selecting 2 of 4 unconditional columns plus 1 `{?WithExtra: ...}` conditional column emits the exact 3-column `SELECT` list and scans all 3 fields without error.
   - Coupled `{?WithOrg: ...}` blocks across `SELECT`, `LEFT JOIN`, and `WHERE` (including inactive slice and inactive `sqlgen.Query` parameters inside disabled blocks).
   - Keyset pagination (`Seek` + `Order`) across multiple pages with uniform (`ASC, ASC` and `DESC, DESC`) and mixed (`ASC, DESC, ASC`) sort orders, verifying zero duplicate or skipped rows across page boundaries.
   - Curried `sqlgen.Query` composition: CTEs (`WITH q AS (...)`), `WHERE id IN (...)`, and curried queries that themselves use `Order`, `Seek`, and `{?Flag: ...}`.
   - Multi-row batch `INSERT ... RETURNING` and `UPDATE ... FROM (VALUES ...)`: verify `1000`-row and `900`-row heavy-hitter promotion, greedy chunking for `2900` rows (`1000 + 1000 + 900`), and implicit vs. explicit transaction behavior.
3. **Cache & Concurrency Stress Tests (`runtime/runtime_test.go`)**:
   - W-TinyLFU scan-flood resistance: flood `StmtCache` with 20,000 one-off variations while running 50 hot variations; verify hot variations remain in Tier 2 (`StatePrepared`) and `ActivePartial` / `partialBytes` never exceed configured caps.
   - Concurrent execution + demotion safety under `-race`: 32 goroutines streaming `Seq` rows while Tier-2 capacity (`maxPrepared = 4`) forces continuous demotions; verify `ManagedStmt` reference counting prevents `sql: statement is closed` errors.
   - `WithTx` cleanup verification: verify manual `q.WithTx(tx)` releases held `ManagedStmt` references upon GC cleanup without leaking open statements.
4. **Allocation Benchmarks (`runtime/runtime_test.go`)**:
   - `BenchmarkStaticQueryHit`: verify `0` SQL string allocations on Tier-2 hit.
   - `BenchmarkDynamicSliceHit`: verify `0` SQL string allocations and pooled `argBuf` reuse on Tier-2 hit.
   - `BenchmarkSelectColsScan`: verify `0` per-row scan pointer allocations across `Seq` iteration.