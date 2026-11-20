# `sqlgen` User Manual & SQL DSL Cookbook

`sqlgen` is a compile-time Go 1.27+ SQL code generator and zero-reflection execution runtime. Write standard SQL in a YAML configuration file (`sqlgen.yaml`) using lightweight inline type annotations (`:type`, `{?Flag: ...}`, `[]Tuple(...)`, `sqlgen.Seek`, `sqlgen.Order`, `sqlgen.Query`), and `sqlgen` generates strongly-typed, zero-allocation Go methods for direct execution, batching, `iter.Seq2` streaming, retryable transactions, and single-roundtrip speculative pipelines.

---

## 1. Quickstart & CLI Reference

### Running the CLI

Generate code from `sqlgen.yaml` in the current directory:

```bash
go run github.com/CAFxX/sqlgen/cmd/sqlgen -config sqlgen.yaml
```

Or add a `//go:generate` directive to any Go file in your package:

```go
//go:generate go run github.com/CAFxX/sqlgen/cmd/sqlgen -config sqlgen.yaml
```

### CLI Flags & Exit Codes

| Flag | Description |
| :--- | :--- |
| `-config <path>` | Path to YAML configuration file (default `"sqlgen.yaml"`). You may also pass `<path>` as a single positional argument if `-config` is not explicitly provided. |
| `-out <path>` | Overrides the `out` path configured in `sqlgen.yaml`. |
| `-stdout` | Writes generated Go code to `stdout` instead of disk. |
| `-check` | CI verification mode. Generates the Go code in memory and compares it byte-for-byte against the existing output file on disk without modifying any files. Exits with code `2` if the file is missing or out of date. |
| `-version` | Prints `sqlgen <version>` to `stdout` (the module version recorded in the binary's build information, for example `sqlgen v0.3.1`; `sqlgen (devel)` for a local build) and exits with code `0`. |

**Exit Codes**:
- `0`: Success.
- `1`: Configuration, file I/O, SQL parsing, or validation error.
- `2`: Invalid CLI flag usage or `-check` drift detected.

---

### `sqlgen.yaml` Schema Reference

```yaml
version: "1"                                    # Optional: must be "" or "1"
package: ledger                                 # Target Go package name (required)
out: ledger_gen.go                              # Output Go file path relative to YAML file (default: "sqlgen_gen.go")
dialect: postgres                               # "postgres" | "postgresql" | "pgx" ($1, $2)
                                                # "sqlite" | "sqlite3" | "mysql" | "mariadb" (?)
                                                # "sqlserver" | "mssql" (@p1, @p2)
                                                # "oracle" (:1, :2)
runtime_import: "github.com/CAFxX/sqlgen/runtime" # Optional override for runtime import path
extra_imports:                                  # Optional extra Go import paths
  - "github.com/google/uuid"

statements:
  - name: GetAccount                            # Exported Go identifier (required)
    kind: one                                   # "exec" | "one" | "many" | "subquery" (default: "many")
    result_type: Account                        # Optional custom struct name (or scalar type like "int64") for "one"/"many" (default: "<Name>Row")
    params_struct: false                        # Optional: set true to always generate <Name>Params struct instead of positional arguments
    params:                                     # Optional out-of-line parameter type declarations
      - org_id: int64                           # Shorthand mapping
      - "account_id:int64"                      # Compact scalar string
      - { name: tags, type: string, slice: true } # Verbose mapping
    columns:                                    # Optional explicit output columns (used when SQL omits inline :type)
      - id: int64
      - "balance:int64"
      - { name: status, type: string }
    sql: |
      SELECT id, balance, status
      FROM accounts
      WHERE org_id = :org_id AND id = :account_id
```

> **Strict YAML Validation**: `sqlgen` decodes `sqlgen.yaml` with `KnownFields(true)`. Unknown keys at any level, empty YAML files, or trailing YAML documents produce an immediate compile-time error.

---

## 2. Complete SQL DSL Cookbook

### 2.1. Inline Output Columns (`SELECT col:type`)

In `kind: one` and `kind: many` queries, annotate top-level `SELECT` (or `RETURNING`) expressions with `:GoType`:

```yaml
statements:
  # Single output column with result_type: int64 -> returns the scalar type directly (int64, not a struct!)
  - name: GetAccountBalance
    kind: one
    result_type: int64
    sql: |
      SELECT balance:int64
      FROM accounts
      WHERE id = :account_id:int64

  # Multiple output columns -> generates struct AccountSummary { ID int64; Email string; TotalCents int64 }
  - name: ListAccountSummaries
    kind: many
    result_type: AccountSummary
    sql: |
      SELECT
        u.id:int64,
        u.email:string,
        COALESCE(SUM(t.amount_cents), 0) AS total_cents:int64
      FROM users u
      LEFT JOIN transfers t ON t.user_id = u.id
      GROUP BY u.id, u.email
```

- **Column Name Inference**:
  - If an `AS alias:type` clause is present, the Go struct field name is `PascalCase(alias)` (with standard Go initialisms uppercased: `id` $\rightarrow$ `ID`, `url` $\rightarrow$ `URL`, `uuid` $\rightarrow$ `UUID`, `sku` $\rightarrow$ `SKU`, `api` $\rightarrow$ `API`, `http` $\rightarrow$ `HTTP`, `ip` $\rightarrow$ `IP`, `json` $\rightarrow$ `JSON`, `sql` $\rightarrow$ `SQL`, `cpu` $\rightarrow$ `CPU`, `ttl` $\rightarrow$ `TTL`, `uri` $\rightarrow$ `URI`, `uid` $\rightarrow$ `UID`, `acl` $\rightarrow$ `ACL`, `html` $\rightarrow$ `HTML`, `xml` $\rightarrow$ `XML`, `db` $\rightarrow$ `DB`, `io` $\rightarrow$ `IO`, `os` $\rightarrow$ `OS`, `ui` $\rightarrow$ `UI`).
  - Otherwise, if the expression is a table-qualified column `u.created_at:time.Time`, the `u.` prefix is stripped and the field name is `CreatedAt`.
  - Complex expressions without a trailing identifier (e.g., `COUNT(*):int64` without `AS alias`) MUST include `AS <alias>:type`.
- **Column and parameter restrictions**:
  - Conditional output columns must come after all unconditional ones, and a statement may have at most 64 unconditional output columns; `sqlgen` rejects anything else.
  - A parameter whose Go name would shadow a generated local (e.g. `:in:string`) is renamed with a `Param` suffix (`inParam`).
- **Single-Column vs. Struct Return Types**:
  - By default (`result_type: ""`), `sqlgen` generates a struct `<Name>Row` (or your custom `result_type: MyStruct` name, which can be shared across multiple statements that return the same columns).
  - When a `kind: one` or `kind: many` statement has **1 output column** and sets `result_type` to a predeclared Go scalar type (e.g., `result_type: int64` or `result_type: string`), the generated methods return the scalar type directly (`(int64, error)` or `([]int64, error)`).

---

### 2.2. Scalar Parameters, Parameter Deduplication & 1D Slices

Parameters outside `SELECT` projection lists may be written with or without a leading colon (`:OrgID:int64` or `OrgID:int64`), or referenced as `:org_id` when declared in the statement's `params:` list.

```yaml
statements:
  # Scalar parameters + automatic deduplication when the same parameter appears multiple times
  - name: RecordTransferAudit
    kind: exec
    sql: |
      INSERT INTO audit_logs (actor_id, target_id, payload)
      VALUES (:actor_id:int64, COALESCE(:target_id:*int64, :actor_id:int64), :payload:[]byte)

  # 1D slice parameter (IN clause)
  - name: GetUsersByIDs
    kind: many
    sql: |
      SELECT id:int64, email:string
      FROM users
      WHERE org_id = :org_id:int64
        AND id IN (:user_ids:[]int64)
```

- **Parameter Deduplication**: If `:actor_id:int64` appears twice in the SQL query, the generated Go function accepts `actorID int64` **once** and binds its value to both SQL placeholders automatically.
- **Byte Slices**: `[]byte` (and `json.RawMessage`) is always treated as a **scalar** binary/JSON parameter, never expanded into comma-separated placeholders.
- **Empty 1D Slice Protection**: Passing an empty slice (`len(userIDs) == 0`) to an active 1D slice or tuple slice parameter returns `runtime.ErrEmptySliceParam` before hitting the database, preventing invalid `IN ()` SQL syntax errors.
- **Function Signature Arity (`params_struct`)**:
  - By default, if a statement has no `{?Flag: ...}` conditional blocks and `params_struct` is not `true`, parameters are passed inline as positional arguments: `q.GetUsersByIDs(ctx, orgID, userIDs)`.
  - If a statement has any `{?Flag: ...}` conditional blocks or sets `params_struct: true`, a `<Name>Params` struct is generated (including `SelectCols uint64` for multi-column `SELECT` statements): `q.SearchUsers(ctx, SearchUsersParams{...})`.

---

### 2.3. Multi-Row Tuple Slices (`[]StructName(...)`)

For bulk `INSERT ... VALUES` or composite-key `WHERE (a, b) IN (...)` queries, use a tuple slice parameter:

```yaml
statements:
  - name: InsertOrderItems
    kind: exec
    sql: |
      INSERT INTO order_items (order_id, sku, quantity, created_at)
      VALUES :items:[]NewOrderItem(OrderID:int64, SKU:string, Quantity:int64, CURRENT_TIMESTAMP)

  - name: GetInventoryBySKUs
    kind: many
    result_type: InventoryItem
    sql: |
      SELECT warehouse_id:int64, sku:string, available_qty:int64
      FROM inventory
      WHERE (warehouse_id, sku) IN (:keys:[]SKUKey(WarehouseID:int64, SKU:string))
```

- Literal SQL expressions inside the tuple template (such as `CURRENT_TIMESTAMP`, `NOW()`, `'active'`, or `0`) are preserved verbatim in every expanded tuple row, while `FieldName:GoType` slots are extracted into the generated `NewOrderItem` struct and bound as positional parameters.
- Statements with **exactly 1 tuple slice parameter** and **0 1D slice parameters** automatically gain `*Batch` and `*Stream` methods (`CanBatch == true`, `CanStream == true`).
- Statements with **0 tuple slice parameters** and **1 1D slice parameter** automatically gain `*Batch` methods (`CanBatch == true`).

---

### 2.4. Deadlock-Prevention Presorting (`ASC` / `DESC`) & PostgreSQL `::type` Casts

Concurrent transactions updating or deleting multiple rows in arbitrary order can cause B-tree page/row lock deadlocks. Append `ASC` or `DESC` to any 1D slice or tuple field to instruct `sqlgen` to presort the keys before binding parameters:

```yaml
statements:
  # 1D slice presorting
  - name: DeleteFulfilledOrdersByIDs
    kind: exec
    sql: |
      DELETE FROM orders
      WHERE id IN (:ids:[]int64 ASC)

  # Composite multi-column tuple presorting (with optional PostgreSQL ::type casts)
  - name: UpsertAccountBalances
    kind: exec
    sql: |
      INSERT INTO accounts (org_id, id, balance_cents)
      VALUES :items:[]AccountDelta(OrgID:int64 ASC, AccountID:int64 ASC, DeltaCents:int64)
      ON CONFLICT (org_id, id)
      DO UPDATE SET balance_cents = accounts.balance_cents + EXCLUDED.balance_cents
```

Need PostgreSQL explicit type casts inside `VALUES` or `UNNEST` tuples? Place `::pg_type` immediately before `ASC`/`DESC`:

```sql
VALUES :items:[]AccountPatch(ID:int64::bigint ASC, Delta:int64::bigint)
```

**Important Presorting Guarantees**:
1. **Caller Slice Immutability**: `sqlgen` never mutates the caller's input slice! It sorts a pooled index permutation (`[]int`) in $O(N \log N)$ (or $O(N)$ fast-path when already sorted).
2. **Global Sort Before Batch Chunking**: In `<Name>Batch`, `sqlgen` sorts the entire $N$-item batch globally *before* splitting it into wire-safe chunks, guaranteeing globally monotonic lock acquisition across all chunks.
3. **`RETURNING` Row Order**: For `INSERT ... VALUES` / upserts driven by a presorted tuple slice, the database processes and returns rows in the **presorted order** (verified on PostgreSQL and CockroachDB, including across batch chunks), while your input slice remains in its original order. For `UPDATE` / `DELETE ... WHERE col IN (:ids ASC|DESC) ... RETURNING` the presort fixes the order of the *bound parameters* only: the database visits, locks and returns rows in the order its query plan chooses (PostgreSQL turns an `IN` list into a sorted index scan, so a `DESC` list has no effect). Treat the returned order of such statements as unspecified; if you need one, wrap the statement in a CTE and `ORDER BY` the result.
4. **What presorting does and does not buy you**: it makes lock acquisition monotonic where the engine processes keys in the order they are bound (multi-row `INSERT`/upsert `VALUES`). For `IN` lists the engine decides, so prefer `ASC` there (it matches the ascending index order engines use) and use `SELECT ... FOR UPDATE ... ORDER BY` first if you need a guaranteed lock order.

---

### 2.5. Conditional Blocks (`{?FlagName: ...}`) & `SelectCols` Bitmask Projection

Wrap optional SQL clauses in `{?FlagName: ...}`. `sqlgen` adds a `FlagName bool` field to `<Name>Params`:

```yaml
statements:
  - name: SearchMembers
    kind: many
    result_type: MemberRow
    sql: |
      SELECT
        m.id:int64,
        m.org_id:int64,
        m.email:string,
        m.role:string,
        m.status:string,
        m.score:int64
        {?WithOrgDetails: , o.name AS org_name:string, o.plan_tier AS org_plan:string }
      FROM members m
      {?WithOrgDetails: JOIN organizations o ON o.id = m.org_id }
      WHERE m.org_id = :org_id:int64
        {?FilterRole: AND m.role = :role:string }
        {?FilterStatuses: AND m.status IN (:statuses:[]string) }
        {?FilterMinScore: AND m.score >= :min_score:int64 }
      ORDER BY m.id ASC
```

- When a flag is `false`, its SQL block is omitted and any parameters exclusively inside disabled blocks are not bound.
- If a `{?Flag: ...}` block wraps columns in the `SELECT` clause (`WithOrgDetails` above), those struct fields (`OrgName`, `OrgPlan`) are left at their zero values when `WithOrgDetails == false`, and scanned normally when `WithOrgDetails == true`.

#### Runtime Column Projection (`SelectCols`)

Every multi-column `kind: one` or `kind: many` statement with a `<Name>Params` struct (`params_struct: true` or at least one `{?Flag: ...}`) exposes a `SelectCols uint64` bitmask field on `<Name>Params`:

```go
// Generated bitmask constants for MemberRow (and SearchMembers):
// MemberRowColID, MemberRowColOrgID, MemberRowColEmail, MemberRowColRole,
// MemberRowColStatus, MemberRowColScore, MemberRowColsAll

// Compact list view: only fetch ID, Email, and Role over the wire!
rows, err := q.SearchMembers(ctx, SearchMembersParams{
    OrgID:      orgID,
    SelectCols: MemberRowColID | MemberRowColEmail | MemberRowColRole,
})
```

- Setting `SelectCols: 0` (the default) or `MemberRowColsAll` selects all unconditional columns.
- Setting specific bits omits unselected unconditional columns from the rendered `SELECT` list and skips scanning them, saving network bandwidth and allocation for wide rows.

---

### 2.6. Keyset Pagination (`sqlgen.Seek` & `sqlgen.Order`)

Avoid slow `OFFSET` pagination on deep pages using `After:sqlgen.Seek` (in `WHERE`) and `Sort:sqlgen.Order(col1, col2, ...)` (in `ORDER BY`).

**Requirements for correct pages.** The sort columns must be `NOT NULL`, and the last column listed must be unique (a primary key, say) so that it breaks every tie. `sqlgen` compares each column with `<` / `>` / `=`, and any comparison with `NULL` is unknown, so a row whose sort column is `NULL` is skipped by every page after the first; engines also disagree on where `NULL`s sort. If a column can be `NULL`, sort on `COALESCE(col, sentinel)` in a subquery or generated column instead. The pagination test suite (`integration/suite_test.go`) checks every ASC/DESC combination and page size against an in-memory oracle on every supported engine, on duplicate-heavy but non-null data.

```yaml
statements:
  - name: ListMembersPaginated
    kind: many
    result_type: MemberItem
    params_struct: true
    sql: |
      SELECT
        id:int64,
        role:string,
        score:int64,
        created_at:int64
      FROM members
      WHERE org_id = :org_id:int64
        AND :after:sqlgen.Seek
      ORDER BY :sort:sqlgen.Order(role, score, created_at, id)
      LIMIT :limit:int64
```

**Important Rules for `Seek` & `Order`**:
1. Every column listed in `sqlgen.Order(col1, col2, ...)` must match an unconditional selected column name (or alias) in the `SELECT` list, and at most 15 columns may be listed.
2. `sqlgen` generates strongly-typed sort column tokens (`<Stmt>OrderBy<Field>.Asc()` and `.Desc()`), a type-safe constructor `Order<Stmt>(...)`, and cursor helpers:
   - `row.Seek()` (a method on `<RowType>`), `Seek<Stmt>(row)` or `Seek<RowType>(row)` $\rightarrow$ returns a `runtime.Seek` cursor populated from `row` for the next page.
   - `Order<Stmt>(ListMembersPaginatedOrderByScore.Desc(), ListMembersPaginatedOrderByID.Asc())` $\rightarrow$ builds the `runtime.Order` value.

   **Which names to use.** Every statement gets its own set (`Order<Stmt>`, `<Stmt>OrderBy<Col>`, `Seek<Stmt>`) and, in addition, a set keyed by its row type (`Order<RowType>`, `<RowType>OrderBy<Col>`, `Seek<RowType>`). Prefer the per-statement names: they are always unambiguous. The row-type names exist for statements that share one `result_type` (they then share one set, which is why such statements must list identical `sqlgen.Order` columns). With the default `result_type` (`<Stmt>Row`) the row-type names carry an extra `Row` (`Order<Stmt>Row`, `<Stmt>RowOrderBy<Col>`). Besides the typed tokens there are raw `uint8` constants for each column and direction (`Order<Stmt><Col>Asc`, `Order<Stmt><Col>Desc`, and the equivalent `<Stmt>Order<Col>Asc` / `<Stmt>Order<Col>Desc`) that can be passed to `Order<Stmt>(...)` directly; the typed tokens are the recommended spelling.
3. **First Page vs. Subsequent Pages**:
   - On Page 1, pass a zero `runtime.Seek{}` (`After: runtime.Seek{}`). `sqlgen` renders `1=1` and binds `0` cursor parameters.
   - On Page 2+, pass `After: lastRow.Seek()`.
   - If all sort columns share the same direction (all `ASC` or all `DESC`), `sqlgen` renders compact row-value comparison `(col1, col2) > (?, ?)` (or `<`).
   - If sort directions are **mixed** (e.g. `score DESC, id ASC`), `sqlgen` automatically renders a B-tree index-friendly bounding-box prefix (`col1 <= ?`) combined with exact lexicographical tie-breaking `OR` branches!

```go
// Per-statement names (preferred): the statement is ListMembersPaginated.
sortSpec := OrderListMembersPaginated(
    ListMembersPaginatedOrderByRole.Asc(),
    ListMembersPaginatedOrderByScore.Desc(),
    ListMembersPaginatedOrderByCreatedAt.Asc(),
    ListMembersPaginatedOrderByID.Asc(),
)
// The same value through the row-type names (result_type: MemberItem), useful when several
// statements share MemberItem: OrderMemberItem(MemberItemOrderByRole.Asc(), ...).

// Page 1:
page1, err := q.ListMembersPaginated(ctx, ListMembersPaginatedParams{
    OrgID: orgID,
    After: runtime.Seek{}, // first page
    Sort:  sortSpec,
    Limit: 25,
})

// Page 2 (using the last row from Page 1):
if len(page1) > 0 {
    last := page1[len(page1)-1]
    page2, err := q.ListMembersPaginated(ctx, ListMembersPaginatedParams{
        OrgID: orgID,
        After: last.Seek(),
        Sort:  sortSpec,
        Limit: 25,
    })
    _ = page2
}
```

---

### 2.7. Runtime Query Composition (`sqlgen.Query` Currying)

Every `one`, `many`, or `subquery` statement `<Name>` generates a zero-DB currying constructor `<Name>Query(...) runtime.Query` (both as a package-level function and as a method on `*Queries`). You can compose curried queries into parent queries using `:sub:sqlgen.Query` slots inside `WITH` CTEs, `FROM (...)`, or `WHERE col IN (:sub:sqlgen.Query)`:

```yaml
statements:
  - name: ActiveOrgs
    kind: subquery
    sql: |
      SELECT id
      FROM organizations
      WHERE plan_tier IN (:tiers:[]string) AND is_active = 1

  - name: CohortMemberIDs
    kind: subquery
    sql: |
      SELECT m.id
      FROM members m
      WHERE m.org_id IN (:active_orgs:sqlgen.Query)
        {?FilterRole: AND m.role = :role:string }
        AND m.score >= :min_score:int64

  - name: AggregateCohortInvoices
    kind: many
    result_type: CohortSummary
    params_struct: true
    sql: |
      WITH target_members AS (:member_query:sqlgen.Query)
      SELECT
        i.currency:string,
        COUNT(*) AS invoice_count:int64,
        COALESCE(SUM(i.amount_cents), 0) AS total_cents:int64
      FROM invoices i
      WHERE i.member_id IN (SELECT id FROM target_members)
        AND i.status = :status:string
      GROUP BY i.currency
      ORDER BY i.currency ASC
```

Usage in Go:

```go
orgsQ := q.ActiveOrgsQuery([]string{"enterprise", "pro"})
membersQ := q.CohortMemberIDsQuery(CohortMemberIDsParams{
    ActiveOrgs: orgsQ,
    FilterRole: true,
    Role:       "admin",
    MinScore:   50,
})
summaries, err := q.AggregateCohortInvoices(ctx, AggregateCohortInvoicesParams{
    MemberQuery: membersQ,
    Status:      "paid",
})
```

- `sqlgen` flattens nested `runtime.Query` trees up to depth 3 into a collision-free `VariantKey` so composed queries still enjoy $O(1)$ Tier-2 prepared statement caching!

---

## 3. Generated Go API & Execution Modes Reference

### Constructing `*Queries`

```go
q := New(db) // uses default cache configuration
// Or with custom cache & batch options:
q := New(db,
    runtime.WithMaxPreparedStmts(512),
    runtime.WithMaxSQLVariants(4096),
    runtime.WithMaxPartialBytes(4<<20),
    runtime.WithPromoteThreshold(2),
    runtime.WithMaxBatchParams(4000),
)
defer q.Close()
```

### 3.1. Reading Modes (`kind: one` & `kind: many`)

For a `kind: exec` statement `<Name>`:
- `q.<Name>(ctx, ...) (sql.Result, error)`

For a `kind: one` statement `<Name>`:
- `q.<Name>(ctx, ...) (<RowType>, error)` — returns `sql.ErrNoRows` if no row matches.

For a `kind: many` statement `<Name>`:
- `q.<Name>(ctx, ...) ([]<RowType>, error)` — returns all matching rows as a slice.
- `q.<Name>Into(ctx, dst []<RowType>, ...) ([]<RowType>, error)` — appends matching rows to the caller-provided slice `dst` (like `append`: use the returned slice, which reuses `dst`'s backing array while its capacity suffices). Pass `buf[:0]` to reuse a buffer across calls.
- `q.<Name>Seq(ctx, ...) iter.Seq2[<RowType>, error]` — Go 1.23+ range-over-func iterator that streams rows in $O(1)$ memory and automatically closes `*sql.Rows` (and releases the cached `*ManagedStmt`) even if the caller exits the `for range` loop early with `break` or `return`.
- `q.<Name>Each(ctx, ..., fn func(<RowType>) error) error` — invokes `fn` for each row.

---

### 3.2. Batching & $O(1)$-Memory Streaming (`*Batch`, `*Stream`, `MapSeq2`)

#### Automatic Wire-Safe Batching (`*Batch`)

Database drivers enforce hard limits on parameters per statement (e.g., 65,535 in PostgreSQL, 32,766 in SQLite). When a `kind: exec` or `kind: many` statement has exactly one collection parameter (a tuple slice `[]Tuple` or a 1D slice `[]T`) and no `sqlgen.Order`, `sqlgen.Seek` or composed `sqlgen.Query` parameter, `sqlgen` generates `*Batch` methods:

- `kind: exec`:
  - `q.<Name>Batch(ctx, ...) (sql.Result, error)` — the result sums `RowsAffected()` over all chunks (its concrete type is `runtime.BatchExecResult`; `LastInsertId()` is the one of the final chunk).
- `kind: many`:
  - `q.<Name>Batch(ctx, ...) ([]<RowType>, error)`
  - `q.<Name>BatchInto(ctx, dst []<RowType>, ...) ([]<RowType>, error)`
  - `q.<Name>BatchSeq(ctx, ...) iter.Seq2[<RowType>, error]`
  - `q.<Name>BatchEach(ctx, ..., fn func(<RowType>) error) error`

**How Adaptive Batch Chunking (`PlanBatchChunk`) Works**:
- When a batch size $N$ exceeds the driver's wire parameter limit (`MaxRowsPerBatch`), `sqlgen` automatically splits the slice into wire-safe chunks.
- **Heavy-Hitter Promotion & Greedy Reuse**: When recurring batch sizes (e.g., `500` rows or `400` rows) execute repeatedly (`>= promoteThreshold`), `StmtCache` promotes those exact batch sizes to Tier-2 prepared statements. If a larger spike arrives (e.g., `1400` rows), `PlanBatchChunk` greedily decomposes it into your already-prepared Tier-2 statement sizes (`500 + 500 + 400`) before falling back to cold power-of-2 chunks (`512, 256, ...`)!

#### $O(1)$-Memory Streaming Ingestion (`*Stream` & `*StreamSeq`)

When a statement qualifies for `*Batch` **and** its collection parameter is a tuple slice (`[]Tuple`; 1D slices get `*Batch` only), `sqlgen` also generates `<Name>Stream` and `<Name>StreamSeq`. They take the lazy `iter.Seq2[<TupleType>, error]` **in place of** the tuple-slice parameter (same position in the argument list), so an arbitrarily large or lazy source is ingested in $O(1)$ memory:

- `kind: exec`:
  - `q.<Name>Stream(ctx, ..., seq iter.Seq2[<TupleType>, error], ...) (sql.Result, error)` — pulls items from `seq` into a pooled chunk buffer (`256` items or `MaxRowsPerBatch`, whichever is smaller), flushes each chunk via `<Name>Batch`, and returns the summed result (`res.RowsAffected()` is the total).
  - `q.<Name>StreamSeq(...)` — same signature and behaviour as `<Name>Stream` (an alias kept so exec and many statements expose the same names).
- `kind: many`:
  - `q.<Name>Stream(ctx, ..., seq iter.Seq2[<TupleType>, error], ...) ([]<RowType>, error)`
  - `q.<Name>StreamSeq(ctx, ..., seq iter.Seq2[<TupleType>, error], ...) iter.Seq2[<RowType>, error]`

An error yielded by `seq` stops the stream and is returned (or yielded) as-is. `runtime.Seq2[T]` is a convenience alias for `iter.Seq2[T, error]`, so values of either spelling are accepted.

**Iterator Adapters in `runtime`** (all return plain `iter.Seq2[T, error]`):
- `runtime.FromSlice(items []T) iter.Seq2[T, error]` — wraps an in-memory slice.
- `runtime.FromSeq(seq iter.Seq[T]) iter.Seq2[T, error]` — wraps a non-error `iter.Seq[T]`.
- `runtime.MapSeq2(seq iter.Seq2[In, error], fn func(In) (Out, error)) iter.Seq2[Out, error]` — transforms a streaming query (`q.StreamOrdersSeq(...)`) directly into the input stream of `<Name>Stream` or `<Name>StreamSeq` in $O(1)$ memory; the first error from `seq` or `fn` ends the stream.
- `runtime.MapSeq(seq iter.Seq2[In, error], fn func(In) Out) iter.Seq2[Out, error]` — infallible mapping helper.

```go
// Insert a slice through the streaming path:
res, err := q.InsertArchivedOrdersStream(ctx, runtime.FromSlice(orders))
```

```go
// Example: Stream fulfilled orders, transform them via MapSeq2, and stream-insert into archive!
orderStream := q.StreamFulfilledOrdersSeq(ctx, cutoffTimestamp)
archiveStream := runtime.MapSeq2(orderStream, func(o FulfilledOrderRow) (NewArchivedOrder, error) {
    return NewArchivedOrder{
        OrderID:    o.ID,
        CustomerID: o.CustomerID,
        TotalCents: o.TotalCents,
        ArchivedAt: nowUnix,
    }, nil
})
res, err := q.InsertArchivedOrdersStream(ctx, archiveStream)
insertedCount, _ := res.RowsAffected()
```

---

### 3.3. Transactions & Automatic Retries (`WithTx`, `InTx`, `WithTxRetry`)

#### Manual `*sql.Tx` Binding

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()

txQ := q.WithTx(tx)
defer txQ.Close() // deterministically releases transaction-scoped prepared statements
```

#### Managed Closure Transactions (`InTx`) & Serialization Retries

`q.InTx` manages `BeginTx`, `txQ.Close()`, panic recovery (guaranteed `Rollback` before re-panicking), and `Commit`:

```go
err := q.InTx(ctx, nil, func(txQ *Queries) error {
    // All queries on txQ run inside the transaction and reuse q's Tier-2 prepared statements!
    return txQ.DebitAccount(ctx, senderID, amountCents)
}, WithTxRetry(runtime.DefaultRetryPolicy()))
```

- `WithTxRetry(policy runtime.TxRetryPolicy)` automatically retries the entire transaction closure with exponential backoff + full jitter when the database returns a transient serialization failure (`SQLSTATE 40001`), deadlock (`SQLSTATE 40P01`, MySQL `1213`), or lock timeout (`SQLSTATE 55P03`, MySQL `1205`, SQLite `SQLITE_BUSY` / `SQLITE_LOCKED`).
- **Ambiguous Commit Protection**: If a network drop (`driver.ErrBadConn`, `io.EOF`, `net.Error`) occurs *during* `tx.Commit()`, `InTx` **never** retries automatically because the transaction may have already committed on the server.

---

### 3.4. Single-Roundtrip Speculative Pipelining (`Pipeline` & `PipelineTx`)

`Pipeline` and `PipelineTx` allow you to queue multiple reads, writes, row-count assertions, and data-dependent `If/Then/Else` branches and flush them together.
- With the `pgxwire` driver (below) on PostgreSQL or CockroachDB, the entire conditional workflow, `BEGIN` and `COMMIT` included, is sent to the server in **one network round-trip** (one write, one `Sync`).
- On every other driver and engine (plain `*sql.DB`, MySQL, MariaDB, SQLite, ...), `Pipeline` seamlessly falls back to sequential execution while preserving identical branching, assertion, and rollback semantics. Nothing to configure, and nothing silently pretends to be faster than it is: see "Verifying which path ran" below.

**Engine support.** The CI matrix runs the full suite against PostgreSQL 15 and 18, CockroachDB, MySQL 8.4 and 9.7, MariaDB 10.11 and 12.3 and SQLite, with real drivers (`pgx`, `go-sql-driver/mysql`). The single-roundtrip batch SQL relies on PostgreSQL's `set_config` / `current_setting` and data-modifying CTEs, so it exists only for the `$N` dialect (PostgreSQL and CockroachDB); on MySQL, MariaDB and SQLite a pipeline always runs through the sequential fallback (same semantics, one round trip per statement).

#### Wiring the single-round-trip path (`pgxwire`)

`pgxwire` is a separate Go module (`github.com/CAFxX/sqlgen/pgxwire`, depends on `pgx/v5`), so the main `sqlgen` module keeps depending only on `yaml` and `sqlite`. It returns an ordinary `*sql.DB`; everything that is not a sqlgen pipeline behaves exactly as with pgx's stdlib driver.

```go
import (
    "github.com/CAFxX/sqlgen/pgxwire"
    "github.com/CAFxX/sqlgen/runtime"
)

db, err := pgxwire.OpenDB("postgres://user:pass@host:5432/app?sslmode=require")
if err != nil { ... }
db.SetMaxOpenConns(16) // must not be 1: the wire path borrows a pooled connection (see the conditions below)
q := store.New(db)

err = q.PipelineTx(ctx, nil, func(p *store.Pipeline) error {
    p.AddScore(-5, from).AssertRowsAffected(1)
    p.AddScore(5, to).AssertRowsAffected(1)
    return nil
}, runtime.WithTxRetry(runtime.DefaultRetryPolicy()))

fmt.Println(q.CacheStats().WirePipelines) // 1: the whole transaction was one network round-trip
```

Use `pgxwire.Connector(cfg *pgx.ConnConfig, opts ...stdlib.OptionOpenDB)` with `sql.OpenDB` when you build the `pgx.ConnConfig` yourself (custom dialer, TLS, `AfterConnect`, ...); the options are those of `stdlib.GetConnector`. The connector is already wrapped with `runtime.WrapConnector`, so do not wrap it again.

**When the single-round-trip path is used.** The condition is always the same: the *pool* hands out a pgxwire connection, and sqlgen owns (or is told about) the transaction boundary.

| How the pipeline runs | Network round-trips on pgxwire | Notes |
| :--- | :--- | :--- |
| `q.PipelineTx(ctx, txOpts, fn)` | **1**: `BEGIN` (with the isolation level and read-only mode of `txOpts`) + every operation + `COMMIT` + `Sync` | `fn` only *queues*: the transaction begins when the batch is sent, so a failing `fn` never touches the database. Retries (`WithTxRetry`) re-run `fn` and cost one round-trip per attempt. |
| `q.Pipeline(ctx)` ... `p.Flush()` (no transaction) | **1**: `BEGIN` + operations + `COMMIT` + `Sync`, atomic: all or nothing | |
| `q.WithTx(tx).Pipeline(ctx)` ... `p.Flush()` (a `*sql.Tx` you began) | **1** for the operations only | `BEGIN` and `COMMIT`/`ROLLBACK` are yours (`db.BeginTx`, `tx.Commit`): 2 more round-trips you chose to make. A failing batch leaves the transaction aborted; roll it back. |
| `q.InTx(...)`, plain query methods | unchanged (pgx's usual behaviour) | Only pipelines are batched. |
| any other driver, `db.SetMaxOpenConns(1)`, or a `*sql.DB` not opened through pgxwire | sequential fallback (`BEGIN`, one per statement, `COMMIT`) | `CacheStats().SequentialPipelines` counts these. With one open connection the runtime cannot park a connection for the batch without risking a self-deadlock, so it runs sequentially. |

An empty pipeline, or one whose `fn` returned an error, sends nothing. Calling `Flush()`/`Exec()` inside a `PipelineTx` callback returns `runtime.ErrFlushInPipelineTx`; the transaction flushes the pipeline itself.

**Failure semantics.** Any failure (a failed assertion, an `Abort`, a server error in the middle of the batch, a serialization failure or deadlock, a dropped connection) rolls the transaction back and surfaces the same error value the sequential path returns (`runtime.ErrAssertionFailed`, your `Abort` error, the `*pgconn.PgError`). When a statement fails the server skips the rest of the batch and leaves the transaction aborted, so pgxwire sends one extra `ROLLBACK` round-trip (error path only) and the connection goes back to the pool clean; if even that fails the connection is closed and the pool replaces it. A failure raised by the `COMMIT` itself (a deferred constraint, a serialization failure under `SERIALIZABLE`) is reported like any other and is retried by `WithTxRetry` when `runtime.IsRetryableTxError` says so; a dropped connection during the batch has an unknown outcome (the `COMMIT` may have been processed) and is never retried.

**How a first-seen pipeline stays at one round-trip.** pgx's usual query modes (`QueryExecModeCacheStatement`, the default, and `CacheDescribe`) first *describe* every statement text they have not seen, which is a separate round-trip, before running the batch. pgxwire therefore sends each pipeline statement the way `pgx.QueryExecModeExec` does: unnamed Parse/Bind/Describe/Execute messages, parameters in text format with no declared types (the server infers them from context), results in text format decoded locally into the same Go values pgx's stdlib driver produces. Consequences, all measured below:
- A first-seen pipeline costs exactly as many round-trips as a warmed one: one.
- pgx's statement cache is not used for pipelines, so the server parses each statement of each flush (server CPU, never a round-trip). A code path that gains nothing from batching can stay on plain query methods, which still use prepared statements.
- Parameter types are inferred by the server from the SQL, as a describe would have inferred them. A placeholder the server cannot type from context (a bare `SELECT $1`) fails with "could not determine data type of parameter" on either path and needs a cast in the SQL; sqlgen already adds casts where it generates such contexts (see "Typed placeholders" below). Because the server, not pgx, decides the type, the Go arguments are encoded so that they mean the same as on a prepared statement; see "Argument types on the single-round-trip path" below.
- pgx query tracers do not see pipeline batches (they run below `pgx.Conn`); use the counters below.
- If your connection goes through a transaction-pooling proxy (PgBouncer in transaction mode), this is the safe mode: nothing is prepared server-side.

**Writing a driver for another engine or protocol.** Implement `runtime.WirePipeliner` (`ExecPipeline(ctx, ops)`: the operations only) and, if you can fold the transaction boundary in too, `runtime.WireTxPipeliner` (`ExecTxPipeline(ctx, txOpts, ops)`). The runtime has already rendered `ops[i].SQL` and `ops[i].Args`; per op, `op.WireResult()` says how to read the answer: set `op.RowCount` from the command tag, from a guarded `(affected, ok)` row, or by feeding result rows to `op.ScanWireRows` (wrap a `driver.Rows` with `runtime.RowsFromDriverRows`); for ops with `Decorators.ProduceCond` (and each `ExtraConds` entry) read the condition back with `SELECT current_setting('sqlgen.c<CondID>', true)` inside the same batch into `CondVal`/`CondSet`; map server errors with `runtime.TranslateWireError(op, err)`; wrap an error raised by the `COMMIT` in `*runtime.CommitError`; and leave no transaction open on the connection when you return an error. Wrap your connector with `runtime.WrapConnector` so user-managed transactions reach `ExecPipeline`. `pgxwire/pgxwire.go` is a complete example.

**Verifying which path ran.** `q.CacheStats()` has two counters, incremented once per flushed pipeline (once per `PipelineTx` attempt):
- `WirePipelines`: the flush went out in a single round-trip through a `WirePipeliner` / `WireTxPipeliner` connection.
- `SequentialPipelines`: the flush executed statement by statement.

A flush that fails while being built or that queued nothing counts as neither; a flush that fails on the server still counts on the path it ran. A test asserting `WirePipelines > 0 && SequentialPipelines == 0` fails loudly when a misconfigured pool (for example `SetMaxOpenConns(1)`, or a `*sql.DB` opened with plain `sql.Open("pgx", ...)`) silently loses the fast path.

To count real network turns instead of trusting a counter, wrap the dialer: a *turn* is a `Read` that follows a `Write`.

```go
type turnConn struct {
    net.Conn
    turns *atomic.Int64
    wrote atomic.Bool
}
func (c *turnConn) Write(p []byte) (int, error) { c.wrote.Store(true); return c.Conn.Write(p) }
func (c *turnConn) Read(p []byte) (int, error) {
    if c.wrote.Swap(false) { c.turns.Add(1) }
    return c.Conn.Read(p)
}

cfg, _ := pgx.ParseConfig(dsn)
cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
    c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
    return &turnConn{Conn: c, turns: &turns}, err
}
db := sql.OpenDB(pgxwire.Connector(cfg))
```

`integration/wire_roundtrip_test.go` is exactly this, run against PostgreSQL 15, 18 and CockroachDB. Warm the connection first (the startup/authentication handshake is turns too), and note that pgx's stdlib driver pings a pooled connection that was idle for more than a second before reuse (one extra turn that is not sqlgen's; `stdlib.OptionShouldPing` controls it).

**Measured round-trips** (network turns per `PipelineTx`, established connection; `integration/wire_roundtrip_test.go`):

| Workload | Sequential (plain pgx) first-seen / warmed | pgxwire first-seen / warmed |
| :--- | :--- | :--- |
| 1 guarded update | 4 / 8 on PG 15 and 18, 4 / 6 on CockroachDB | **1 / 1** on PG 15, PG 18, CockroachDB |
| 3 guarded updates | 5 / 5 | **1 / 1** on all three |
| 10 guarded updates | 12 / 12 | **1 / 1** on all three |
| read + `If`/`Else` + 3 assertions | 8 / 10 | **1 / 1** on all three |
| standalone `Flush()` | n/a | **1** on all three |
| `Flush()` in a user-managed tx | n/a | **1** (+1 for your `COMMIT`) on all three |
| failure (assertion, `Abort`, server error) | n/a | 2 (the batch + the `ROLLBACK`); the pipeline after it is back to 1 |

The sequential figures include the extra `Prepare` round-trips of statement-cache promotion and vary by run; the lower bound is `BEGIN` + one per executed statement + `COMMIT`. CockroachDB reaches the same single round-trip as PostgreSQL. Wall-clock effect, measured with `go test -bench PipelineTx` on a loopback server (`ENGINES=postgres18`, 10 guarded updates): about 2.5 ms for pgxwire against 16 ms sequential; with 5 ms of simulated network latency per turn 5.7 ms against 69 ms. The win is the round-trips removed, so it grows with the latency to your database and with the number of statements.

#### Argument types on the single-round-trip path

Pipeline statements carry no declared parameter types, so the type the server reads an argument as is the type of the column or expression its placeholder meets. pgxwire resolves the Go values first so that the stored data is the same as on a prepared statement (the `integration` suite compares both paths on PostgreSQL 15, 18 and CockroachDB):

| Go argument | Sent as | Works for |
|---|---|---|
| `time.Time`, `*time.Time`, `sql.NullTime`, a `driver.Valuer` returning a time | text keeping the value's **own** UTC offset (`2024-03-01 22:30:00-05:00:00`; years before 1 as `... BC`) | `timestamp` / `date` / `time` columns store the Go wall clock (PostgreSQL ignores the offset for them, exactly as a prepared statement stores it); `timestamptz` stores the instant. pgx's own mode converts to UTC first, which would store `22:30-05:00` into a `timestamp` column as `03:30` of the next day. |
| `[]time.Time` | an array literal with the same element format | `timestamp[]`, `timestamptz[]` |
| `[]byte` | raw bytes, binary format | `bytea`, `text`, `varchar`, `uuid` (16 bytes). **Not `json`/`jsonb`** (the server expects a version prefix): the statement fails with an error naming the `[]byte` argument; write `$N::text::jsonb` in the SQL, or pass a `string`, `json.RawMessage` or a map/struct instead. A nil slice is `NULL`. |
| `[16]byte` (a UUID) | canonical `xxxxxxxx-xxxx-...` text | `uuid`, `text` |
| `map`, `struct`, pointer to struct, slice of structs/maps (anything pgx cannot encode) | JSON text | `json`, `jsonb`, `text` |
| `driver.Valuer` (`sql.Null*`, `uuid.UUID`, ...) | the resolved `driver.Value`, then as above | as for the resolved value |
| everything else (`string`, integers, floats, `bool`, `[]string`, `[]int64`, `json.RawMessage`, ...) | pgx's default text encoding | as before |

A value no rule can encode is rejected before anything is sent, with an error that names the argument (`argument $2 (chan int)`) and suggests an explicit cast in the SQL (`$2::text`) or converting the value yourself. Known differences from a prepared statement that are deliberately not emulated: a `time.Time` bound to a `text` column is stored in PostgreSQL's date-time syntax rather than Go's `Time.String()` form, and `float32` is sent in its shortest decimal form (`0.1`, not `0.10000000149011612`).

#### Typed placeholders in gated inserts (`$N` dialect)

An `INSERT` queued inside a `p.If` branch is sent as `INSERT ... SELECT ... WHERE <gates>`, and PostgreSQL does not infer placeholder types from the target columns there. `sqlgen` therefore casts each placeholder from its Go type (`int`/`int64` to `bigint`, `string` to `text`, `bool` to `boolean`, `float64` to `double precision`, `[]byte` to `bytea`, `time.Time` to `timestamptz`, and so on). For any other Go type (for example a `uuid` or `jsonb` column fed by a `string`), add an explicit cast in the SQL, such as `:id:string::uuid`.

```go
err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
    // 1. Queue a read or update and attach assertions:
    senderFut := p.GetAccount(orgID, senderID).AssertFound()

    // 2. Branch on the future's result and columns:
    p.If(
        senderFut.Found().And(
            WhereCol(senderFut, fmt.Sprintf("balance_cents >= %d", transferAmount), func(a Account) bool {
                return a.BalanceCents >= transferAmount
            }),
        ),
        func(thenP *Pipeline) {
            // Nested check: abort and roll back if account is frozen!
            thenP.If(
                WhereCol(senderFut, "status = 'frozen'", func(a Account) bool {
                    return a.Status == "frozen"
                }),
                func(frozenP *Pipeline) {
                    frozenP.Abort(ErrAccountFrozen)
                },
                func(activeP *Pipeline) {
                    activeP.DebitAccount(orgID, senderID, transferAmount).AssertRowsAffected(1)
                    activeP.CreditAccount(orgID, receiverID, transferAmount).AssertRowsAffected(1)
                    activeP.InsertTransferReceipt(idempotencyKey, orgID, senderID, receiverID, transferAmount).AssertRowsAffected(1)
                },
            )
        },
        func(elseP *Pipeline) {
            // Insufficient funds: record rejection audit log and commit cleanly!
            elseP.InsertAuditEvent(orgID, senderID, "rejected_insufficient_funds", transferAmount).AssertRowsAffected(1)
        },
    )
    return nil
}, WithTxRetry(runtime.DefaultRetryPolicy()))
```

#### Pipeline Futures, Assertions & Branch Combinators

1. **Future Types**:
   - `*runtime.ExecFuture`: returned by `kind: exec` pipeline methods.
     - `.AssertRowsAffected(n int64) *ExecFuture`: fails the pipeline with an error matching `runtime.ErrAssertionFailed` if rows affected $\ne n$.
     - `.AssertFound() *ExecFuture`: fails the pipeline with an error matching `runtime.ErrAssertionFailed` if rows affected $\ne 1$.
     - `.RowsAffectedEq(n)`, `.RowsAffectedGt(n)`, `.RowsAffectedGe(n)`, `.RowsAffectedLt(n)`, `.RowsAffectedLe(n)`, `.Found()`, `.NotFound()`: returns a `PipelineCond` for `p.If(...)`.
     - `.Result() (sql.Result, error)`, `.RowsAffected() (int64, error)`, and `.Skipped() bool` (valid after `Flush` / `PipelineTx` completes).
   - `*runtime.RowFuture[T]`: returned by `kind: one` pipeline methods.
     - `.AssertFound() *RowFuture[T]`: fails the pipeline with an error matching `runtime.ErrAssertionFailed` if the row count is not 1.
     - `.AssertRowsAffected(n int64) *RowFuture[T]`: fails the pipeline with an error matching `runtime.ErrAssertionFailed` if row count $\ne n$.
     - `.Found()`, `.NotFound()`, `.RowsAffectedEq(n)`, `.RowsAffectedGt(n)`, `.RowsAffectedGe(n)`, `.RowsAffectedLt(n)`, `.RowsAffectedLe(n)`: returns a `PipelineCond`.
     - `.WhereCol(sqlPred string, goPred func(T) bool) PipelineCond` (or package helper `WhereCol(fut, sqlPred, goPred)`): evaluates `sqlPred` on the wire in `WirePipeliner` mode and `goPred(row)` in Go fallback mode.
     - `.Get() (T, error)` (or `.Value() (T, error)`) and `.Skipped() bool` (valid after flush).
   - `*runtime.SliceFuture[T]`: returned by `kind: many` pipeline methods.
     - `.AssertRowsAffected(n int64) *SliceFuture[T]`, `.AssertFound() *SliceFuture[T]`
     - `.Found()`, `.NotFound()`, `.RowsAffectedEq(n)`, `.RowsAffectedGt(n)`, `.RowsAffectedGe(n)`, `.RowsAffectedLt(n)`, `.RowsAffectedLe(n)`, `.WhereCol(sqlPred, goPred func(T) bool)`
     - `.Get() ([]T, error)` (or `.Values() ([]T, error)`) and `.Skipped() bool` (valid after flush).
2. **Boolean Combinators & Control Flow**:
   - `condA.And(condB)`, `condA.Or(condB)`, `condA.Not()` combine `PipelineCond` values.
     The operands may come from different futures (`del.NotFound().And(get.Found())`): every leaf is evaluated over its own operation's rows, also on the single-round-trip path, where each earlier operation records its leaf server-side and the latest one combines the records, still in one network round-trip. A leaf over an operation that did not run (its branch was not taken) is false, so `Not` of it is true, identically on both paths.
   - `ifRes := p.If(cond, thenFn, elseFn)`: `ifRes.Taken()` and `ifRes.Skipped()` report whether the `then` branch was taken after flush.
   - `p.Abort(err)`: when reached inside an active branch, aborts the pipeline, returning `err` from `PipelineTx` / `Flush` (the transaction is rolled back only inside `PipelineTx` or a standalone `Pipeline`; see below).
3. **Assertion errors**: a failed assertion returns a `*runtime.AssertionError` (`errors.Is(err, runtime.ErrAssertionFailed)` is true; use `errors.As` to read the fields `Statement`, `Want`, `Got`, `GotKnown`). The message names the statement and the expected count, for example `sqlgen: pipeline assertion failed on statement "InsertAudit": expected 5 rows affected, got 3`. On the single-roundtrip wire path the server only signals that the assertion was violated, so the actual count is unknown (`GotKnown` is false) and the message ends after the expected count.
4. **Who rolls back**:
   - `PipelineTx(ctx, opts, fn)` and a standalone `q.Pipeline(ctx)` (which runs in its own implicit transaction): a failed assertion, `Abort`, or any other error rolls the transaction back automatically; nothing the pipeline wrote is committed.
   - A pipeline on a **user-managed transaction** (`q.WithTx(tx).Pipeline(ctx).Flush()`): the pipeline never rolls back. A failed assertion makes `Flush` return the error, but earlier writes of the pipeline (and everything else done on `tx`) stay in the still-open transaction, and the transaction remains usable. The caller must call `tx.Rollback()` (or deliberately `tx.Commit()`) itself.

---

### 3.5. Cache Tuning & Observability (`StmtCache`)

`sqlgen` includes a 16-shard, Two-Tier W-TinyLFU + Segmented LRU (`StmtCache`) that caches rendered SQL strings in Tier 1 (bounded by both variant count and byte budget) and only promotes recurring heavy-hitter shapes to Tier-2 database prepared statements (`*sql.Stmt`).

| Option | Default | Description |
| :--- | :--- | :--- |
| `runtime.WithMaxPreparedStmts(n)` | `512` (`32`/shard) | Maximum Tier-2 `*sql.Stmt` handles across all 16 shards (`0` disables Tier 2). |
| `runtime.WithMaxSQLVariants(n)` | `4096` (`256`/shard) | Maximum Tier-1 rendered SQL strings cached across all 16 shards (`0` disables Tier 1+2). |
| `runtime.WithMaxPartialBytes(bytes)` | `4 MiB` (`256 KiB`/shard) | Hard memory budget in bytes for Tier-1 cached SQL strings. |
| `runtime.WithPromoteThreshold(hits)` | `2` | Sketch frequency required before promoting a Tier-1 SQL variant (or recurring batch size) to a Tier-2 `*sql.Stmt`. |
| `runtime.WithMaxBatchParams(params)` | Dialect default (`65535` PG, `32766` SQLite/MySQL) | Overrides the maximum wire parameters per batch chunk. |

Inspect live cache telemetry at any time via `stats := q.CacheStats()`:
- `stats.PreparedHits`: Tier-2 `*sql.Stmt` cache hits.
- `stats.PartialHits`: Tier-1 SQL string cache hits (0 SQL formatting overhead).
- `stats.Misses`: Cold cache misses.
- `stats.Promotions`: Number of variants promoted from Tier 1 to Tier 2 `*sql.Stmt`.
- `stats.Demotions`: Number of Tier-2 `*sql.Stmt` handles demoted back to Tier 1.
- `stats.Evictions`: Number of Tier-1 SQL strings evicted.
- `stats.RejectedAdmits`: Tier-1 admissions refused by the TinyLFU admission filter.
- `stats.ActivePrepared`: Current Tier-2 `*sql.Stmt` count.
- `stats.ActivePartial`: Current Tier-1 SQL string count.
- `stats.PartialBytes`: Exact byte sum of all Tier-1 SQL strings currently in cache.
- `stats.WirePipelines`: Pipeline flushes (one per `Pipeline.Flush`, one per `PipelineTx` attempt) that ran in a single network round-trip through a `WirePipeliner` / `WireTxPipeliner` connection (see section 3.4).
- `stats.SequentialPipelines`: Pipeline flushes that executed one statement at a time (the portable fallback). A flush that fails while being built, or that queued nothing, counts as neither.

---

### 3.6. Reserved Identifiers

Every generated file declares the same fixed set of top-level names in your package, so no statement, `result_type` or tuple struct may use them.

**Package-level names declared in every generated file** (`reservedTypeNames` in `generator.go`):

| Kind | Names |
| :--- | :--- |
| Type aliases | `Query`, `Order`, `Seek`, `PipelineCond`, `ExecFuture`, `IfResult`, `TxOption`, `TxRetryPolicy` |
| Function values | `NewSeek`, `WithTxRetry`, `DefaultRetryPolicy` |
| Functions | `WhereCol`, `New`, `NewWithCache` |
| Types | `Queries`, `Pipeline` |

**Why the unqualified aliases exist.** The aliases (`Query = runtime.Query`, `WithTxRetry = runtime.WithTxRetry`, and so on) re-export the runtime types and options that appear in generated signatures, so application code that only calls the generated API never has to import the `runtime` package. For example `q.InTx(ctx, nil, fn, WithTxRetry(DefaultRetryPolicy()))` and `After: NewSeek(...)` compile with just the generated package. They are aliases, not new types: a `Query` and a `runtime.Query` are identical and interchangeable.

**Method names generated on `*Queries` and `*Pipeline`** that a statement name must avoid (`reservedStatementNames`): `Cache`, `CacheStats`, `Close`, `If`, `InTx`, `InTxOpts`, `Pipeline`, `PipelineTx`, `WithPrepared`, `WithTx`, plus `SetBuildError`, `QueueExec`, `Abort`, `Flush` and `Exec`, which the generated `Pipeline` inherits from `runtime.Pipeline`. A statement becomes a method with its own name on both `*Queries` and `*Pipeline`, so it must not shadow any of them.

**What happens on a clash.** Generation fails with a clear error instead of emitting code that does not compile:

- a statement named like one of the methods above: `statement name "Close" collides with a method generated on *Queries / *Pipeline; rename it`;
- a `result_type` (or a tuple struct name such as `[]Query(...)`) equal to a package-level name above: `statement Q: result_type "Order" collides with a type or function declared in every generated file; rename it`.

Rename the statement or `result_type` (for example `Order` to `OrderRecord`).
