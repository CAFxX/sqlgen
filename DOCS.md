# `sqlgen` User Manual & SQL DSL Cookbook

`sqlgen` is a compile-time Go 1.27+ SQL code generator and zero-reflection execution runtime. Write standard SQL in a YAML configuration file (`sqlgen.yaml`) using lightweight inline type annotations (`:type`, `{?Flag: ...}`, `[]Tuple(...)`, `sqlgen.Seek`, `sqlgen.Order`, `sqlgen.Query`), and `sqlgen` generates strongly-typed, zero-allocation Go methods for direct execution, batching, `iter.Seq2` streaming, retryable transactions, and single-roundtrip speculative pipelines.

---

## Surface at a glance

Everything below is the whole surface; each item has one spelling and the sections that follow detail it.

- **YAML file keys**: `package`, `out`, `dialect` (`postgres` | `cockroachdb` | `mysql` | `mariadb` | `sqlite`, required), `extra_imports`, `statements`.
- **Statement keys**: `name`, `kind` (`exec` | `one` | `many` | `subquery`), `sql`, `result_type` (names the row struct: an exported identifier, never a predeclared type), `args` (`auto` | `positional` | `struct`).
- **Token grammar inside `sql`** (placeholders are never written by hand):
  - output column `col:Type`, `expr AS alias:Type`, nullable `*Type`; one unconditional column and no `result_type` returns the value itself.
  - parameter `:name:Type`, repeat of an earlier parameter `:name`, 1D slice `:name:[]Type` (optionally `ASC` / `DESC` to presort), tuple slice `:rows:[]Struct(Field:Type [ASC|DESC] [::cast], ...)`.
  - directives `:name:sqlgen.Seek`, `:name:sqlgen.Order(col, ...)`, `:name:sqlgen.Query`; optional block `{?Flag: ... }` (switched by a `Flag` bool; its columns and parameters join the statement).
- **Generated per statement kind** (`q` is a `*Queries`, `p` a `*Pipeline`):
  - `exec`: `q.Name(ctx, ...) (sql.Result, error)`; when chunked (a decomposable one-collection statement) also `q.NameStream(ctx, seq, ...)`. A one-collection statement is chunked by the base method.
  - `one`: `q.Name(ctx, ...) (Row, error)`. `many`: `q.Name(ctx, ...) ([]Row, error)` and `q.NameSeq(ctx, ...) iter.Seq2[Row, error]`; when chunked also `NameStream` / `NameStreamSeq`.
  - `subquery` statements and `one` / `many` statements that are a `SELECT` (also `WITH ... SELECT`): package-level `NameQuery(...) runtime.Query` (compose into `:sub:sqlgen.Query` slots; a data-modifying `INSERT`/`UPDATE`/`DELETE ... RETURNING` gets none, engines reject it inside `IN (...)`); every non-`subquery` kind: `p.Name(...)` returning a future (`ExecFuture`, `RowFuture[T]`, `SliceFuture[T]`).
  - types and helpers: `Row` struct, `NameParams` (struct style), `NameCol<X>` / `NameColsAll` projection masks (trailing `selectCols ...uint64`, or a `SelectCols` field; only for statements that have the projection, i.e. two or more unconditional output columns), and per statement with `sqlgen.Order`: `NameOrderBy<Col>.Asc()/.Desc()`, `OrderName(...)`; with a `sqlgen.Seek` also `NewSeekName(<one typed value per Order column>)` and `SeekName(row)`.
  - on every `*Queries`: `WithTx(tx)`, `InTx(ctx, opts, fn, ...runtime.TxOption)`, `Pipeline(ctx)`, `PipelineTx(ctx, opts, fn, ...)`, `WithPrepared(bool)`, `CacheStats()`, `Close()`; package-level `New(db, ...runtime.CacheOption)`.
- **Runtime user API** (import `github.com/CAFxX/sqlgen/runtime`; the generated file declares no aliases):
  - reading: `Collect`, `AppendTo`, `Each` (+ `ErrStop`), `FromSlice`, `FromSeq`, `MapSeq2`.
  - values: `Query`, `Order`, `Seek` (the zero value is the first page), `OrCols`.
  - transactions: `WithTxRetry`, `DefaultRetryPolicy`, `TxRetryPolicy`.
  - pipelines: futures (`ExecFuture`, `RowFuture`, `SliceFuture`: `Rows{Eq,Gt,Ge,Lt,Le}`, `Found`, `NotFound`, `AssertRows`, `AssertFound`, `Label`, `WhereCol`, `Result`, `Get`), `PipelineCond` (`And`, `Or`, `Not`), `p.If`, `p.Abort`, `p.Flush`, `AssertionError`.
  - cache: `WithMaxPreparedStmts`, `WithMaxSQLVariants`, `WithMaxBatchParams`, `CacheStats`; drivers: `WrapConnector` and the `pgxwire` / `mysqlwire` modules.

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
| `-config <path>` | Path to YAML configuration file (default `"sqlgen.yaml"`). It is given with `-config` only; positional arguments are rejected. |
| `-out <path>` | Overrides the `out` path configured in `sqlgen.yaml`; unlike `out`, a relative `-out` is relative to the working directory. |
| `-stdout` | Writes generated Go code to `stdout` instead of disk. |
| `-check` | CI verification mode. Generates the Go code in memory and compares it byte-for-byte against the existing output file on disk without modifying any files. Exits with code `2` if the file is missing or out of date. |
| `-version` | Prints `sqlgen <version>` to `stdout` and exits with code `0`. The version is the one Go records for the sqlgen module in the binary's build information: `go install github.com/CAFxX/sqlgen/cmd/sqlgen@v0.3.1` prints `sqlgen v0.3.1` (an untagged commit prints its pseudo-version, `v0.0.0-<date>-<commit>`); a build inside the sqlgen repository (`go run ./cmd/sqlgen`) prints `sqlgen (devel)`; run as a dependency of your module (`go run github.com/CAFxX/sqlgen/cmd/sqlgen`, a `tool` directive) it prints the version your `go.mod` requires, and with a `replace` pointing at a local checkout that is still the required version (often `v0.0.0`), not the checkout's state. |

**Exit Codes**:
- `0`: Success.
- `1`: Configuration, file I/O, SQL parsing, or validation error.
- `2`: Invalid CLI flag usage or `-check` drift detected.

**Output safety**: the output file is written atomically (temporary file in the same directory, synced, then renamed). A symbolic link at the output path is followed, so the link survives; an existing output keeps its permission bits and a new one gets `0666` filtered by the umask. sqlgen refuses (exit `1`) to overwrite the configuration file or any non-empty file that does not start with the `// Code generated by sqlgen.` header, and rejects an output path containing a backslash on systems whose path separator is `/`.

---

### `sqlgen.yaml` Schema Reference

```yaml
package: ledger                                 # Target Go package name (required)
out: ledger_gen.go                              # Output Go file path relative to YAML file (default: "sqlgen_gen.go")
dialect: postgres                               # Required, exactly one of: postgres | cockroachdb ($1, $2) | mysql | mariadb | sqlite (?)
                                                # (no aliases; a missing or unknown value is an error listing the five; SQL Server and Oracle are not supported)
extra_imports:                                  # Optional extra Go import paths for types outside the standard library prefixes
  - "github.com/google/uuid"                    # (time, encoding/json, math/big, net/netip, net/url and database/sql/driver are imported automatically)

statements:
  - name: GetAccount                            # Exported Go identifier (required)
    kind: one                                   # Required: "exec" | "one" | "many" | "subquery"
    result_type: Account                        # Optional exported row struct name for "one"/"many" (default: "<Name>Row"); never a predeclared type
    args: auto                                  # Optional argument style: auto (default) | positional | struct (section 2.2)
    sql: |                                      # Output columns and parameters are typed inline (section 2)
      SELECT id:int64, balance:int64, status:string
      FROM accounts
      WHERE org_id = :org_id:int64 AND id = :account_id:int64
```

Statement names are exported Go identifiers: `name: getAccount` is rejected at generate time (`must be an exported Go identifier`), as are names that collide with generated methods. The `out:` directory is created if it does not exist.

> **Strict YAML Validation**: `sqlgen` decodes `sqlgen.yaml` with `KnownFields(true)`. Unknown keys at any level, empty YAML files, or trailing YAML documents produce an immediate compile-time error. Every type is declared inline in the SQL: there are no `params:` / `columns:` lists, and no `version:` or `runtime_import:` keys. A removed key is reported together with the key that replaces it.

> **Write `sql:` as a block scalar (`sql: |`).** A plain (unquoted) YAML scalar may not contain `: ` or ` #` and may not start with an indicator (`{`, `[`, `*`, `&`, `!`, `'`, `"`, ...): a `{?Flag: ...}` block contains `: `, ` #` starts a YAML comment that silently truncates the SQL, and a multi-line plain scalar is folded into one line, so a `-- comment` line would swallow the rest of the statement. A one-line plain `sql:` works only when it avoids all of these; `sql: |` (a literal block) passes the text through verbatim.

#### Placeholders are generated, never hand-written

Write parameters as `:name:type` and let `sqlgen` emit the engine's placeholders. A hand-written placeholder would collide with the generated numbering and arguments, so it is a generate-time error when it can be recognised without ambiguity (placeholder-looking text inside string literals, quoted identifiers, dollar-quoted strings and comments is never flagged):
- `dialect: postgres` / `cockroachdb` (`$N`): a literal `$1`, `$2`, ... is rejected. A bare `?` is left alone, because PostgreSQL's `jsonb` operators (`?`, `?|`, `?&`) use it.
- `dialect: mysql` / `mariadb` / `sqlite` (`?`): a literal `?` is rejected (none of these engines has a `?` operator, so outside literals it can only be a placeholder). `$1` text is left alone.
- A tuple slice (`:rows:[]T(...)`) expands to a comma-separated list of parenthesised rows, so it is only accepted after `VALUES` or inside `IN (...)`; anywhere else (`WHERE x = :rows:[]T(...)`) is an error.

### Engine support

| Feature | PostgreSQL | CockroachDB | YugabyteDB | SQLite | MySQL | MariaDB | TiDB |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dialect:` | `postgres` | `cockroachdb` | `postgres` | `sqlite` | `mysql` | `mariadb` | `mysql` |
| Placeholders | `$N` | `$N` | `$N` | `?` | `?` | `?` | `?` |
| `RETURNING` (`kind: many` on `INSERT` / `UPDATE` / `DELETE`) | yes | yes | yes | yes | **no** (generate-time error) | `INSERT` and `DELETE` only (no `UPDATE ... RETURNING`) | **no** (as MySQL) |
| Tested versions | 15, 18 | v25.2 | 2025.2 (YSQL) | `modernc.org/sqlite` | 8.4, 9.7 | 10.11, 12.3 | v8.5 |
| Keyset `Seek` / `Order`, `SelectCols`, `{?Flag:}` blocks, automatic chunking / `Stream`, `InTx` | yes | yes | yes | yes | yes | yes | yes |
| Single-round-trip pipelines | yes (`pgxwire`) | yes (`pgxwire`) | yes (`pgxwire`) | no (sequential) | yes (`mysqlwire`) | yes (`mysqlwire`) | yes (`mysqlwire`) |
| Typed `::type` placeholder casts in gated inserts | yes | yes | yes | n/a | n/a | n/a | n/a |
| Batch chunk limit (parameters per statement, see `WithMaxBatchParams`) | 16384 default, at most 65535 | same | same | 16384 default, at most 32766 | same as SQLite | same as SQLite | same as SQLite |

Notes:
- **`RETURNING`.** MySQL has no `RETURNING` clause; a statement containing one under `dialect: mysql` fails at generate time with a hint. MariaDB 10.5+ supports it for `INSERT` and `DELETE` (not `UPDATE`): pick `dialect: mariadb` for it (the two dialects generate the same `?` placeholders, so `mariadb` merely allows `RETURNING`). On MySQL read the rows back with a `SELECT`.
- **`time.Time` on MySQL and MariaDB.** `go-sql-driver/mysql` returns `DATE` / `DATETIME` / `TIMESTAMP` columns as `[]byte` unless the DSN has `parseTime=true`; scanning them into a `time.Time` result field fails without it (`user:pass@tcp(host)/db?parseTime=true`).
- **`time.Time` ordering on SQLite.** `modernc.org/sqlite` stores a `time.Time` as text in the value's own zone (`2026-01-01 23:29:00.123456789 +0100 CET`), and SQLite compares the text, so `ORDER BY` and the keyset comparisons of `sqlgen.Seek` follow the wall-clock text, not the instant, as soon as values with different UTC offsets are mixed (and a value in a zone without a name, such as a `time.FixedZone("", ...)`, cannot be scanned back). Write times as UTC (`t.UTC()`), or store Unix times in an `INTEGER` column; with UTC values pages are in time order (checked by paging 60 sub-second-spread values against Go's order).
- **`time.Time` from SQLite expressions.** `modernc.org/sqlite` turns a value into `time.Time` only for a column whose declared type is `DATE`, `DATETIME` or `TIMESTAMP`. An expression has no declared type, so an aggregate over such a column (`MAX(created_at)`, `MIN(...)`, also `CAST(... AS DATETIME)`) comes back as the stored text and scanning it into `time.Time` fails (`unsupported Scan, storing driver.Value type string into type *time.Time`). Declare it as `MAX(created_at) AS last:string` and parse it, or select the column itself (`SELECT created_at:time.Time FROM e ORDER BY created_at DESC LIMIT 1`).
- **YugabyteDB** speaks the PostgreSQL protocol and dialect (`dialect: postgres`, `pgx`, `pgxwire`); it is not CockroachDB. Things that differ from PostgreSQL: its `SERIALIZABLE` takes read locks (two serializable transactions that read a row and then both write it wait on each other: use `REPEATABLE READ`, which is snapshot isolation and fails the second writer with SQLSTATE 40001); most write conflicts are resolved inside the server by waiting or by a transparent statement retry, so fewer of them reach `WithTxRetry` than on PostgreSQL; `CREATE CONSTRAINT TRIGGER` does not exist; and the server wants about 4 GB of memory.
- **TiDB** speaks the MySQL protocol and dialect (`dialect: mysql`, `go-sql-driver/mysql`, `mysqlwire`; no `RETURNING`). Things that differ from MySQL: the default collation is `utf8mb4_bin` (binary comparison and ordering of text); transactions are pessimistic by default, and in optimistic mode (`tidb_txn_mode = 'optimistic'`) a write conflict surfaces at `COMMIT` as error 9007 (`Write conflict ... [try again later]`) or 8022, which `IsRetryableTxError` classifies as retryable (`WithTxRetry` re-runs the transaction); `SERIALIZABLE` and `READ UNCOMMITTED` are refused (error 8048) and so are read-only transactions (`sql.TxOptions{ReadOnly: true}`, error 1235), unless the corresponding `tidb_skip_isolation_level_check` / `tidb_enable_noop_functions` variables are set; `AUTO_INCREMENT` values are unique but not consecutive. On the `mysqlwire` path TiDB parses the whole multi-statement batch before it runs any of it, so a syntax error in a hand-written statement means no operation of that pipeline ran (on MySQL the operations before it did, and are kept or rolled back as described below), and a string argument that would need the `_<charset> 0x...` literal form is not inlined (see "Argument literals").
- **First page of a keyset query.** The `Seek` slot renders the literal `TRUE` (a valid boolean expression on every engine above) when no cursor is given.

---

## 2. Complete SQL DSL Cookbook

### 2.1. Inline Output Columns (`SELECT col:type`)

In `kind: one` and `kind: many` queries, annotate top-level `SELECT` (or `RETURNING`) expressions with `:GoType`:

```yaml
statements:
  # Single output column and no result_type -> returns the column's value directly ((int64, error), not a struct)
  - name: GetAccountBalance
    kind: one
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
  - If an `AS alias:type` clause is present, the Go struct field name is `PascalCase(alias)` (with these initialisms uppercased, singular and plural: `id` $\rightarrow$ `ID`, `ids` $\rightarrow$ `IDs`, `sku` $\rightarrow$ `SKU`, `url` $\rightarrow$ `URL`, `uri` $\rightarrow$ `URI`, `uuid` $\rightarrow$ `UUID`; and `sql` $\rightarrow$ `SQL`, `api` $\rightarrow$ `API`, `http` $\rightarrow$ `HTTP`, `https` $\rightarrow$ `HTTPS`, `ip` $\rightarrow$ `IP`, `json` $\rightarrow$ `JSON`, `xml` $\rightarrow$ `XML`, `html` $\rightarrow$ `HTML`, `cpu` $\rightarrow$ `CPU`; any other word is simply capitalized).
  - Otherwise, if the expression is a table-qualified column `u.created_at:time.Time`, the `u.` prefix is stripped and the field name is `CreatedAt`.
  - Complex expressions without a trailing identifier (e.g., `COUNT(*):int64` without `AS alias`) MUST include `AS <alias>:type`.
- **Column and parameter restrictions**:
  - Conditional output columns must come after all unconditional ones, and a statement may have at most 64 unconditional output columns; `sqlgen` rejects anything else.
  - A parameter whose Go name would shadow a generated local (e.g. `:in:string`) is renamed with a `Param` suffix (`inParam`).
- **Single-Column vs. Struct Return Types**:
  - A `kind: one` or `kind: many` statement with **exactly 1 output column and no `result_type`** returns that column's value directly: `(T, error)` for `one`, `([]T, error)` and `iter.Seq2[T, error]` for `many`, and `*runtime.RowFuture[T]` / `*runtime.SliceFuture[T]` in a pipeline. `T` is whatever the column is declared as: `int64`, `time.Time`, `[]byte`, a pointer or nullable type (`*string`), a named type from another package.
  - With several output columns (and no `result_type`) `sqlgen` generates a struct `<Name>Row`.
  - `result_type: MyStruct` only ever **names a row struct**, one that can be shared across statements that return the same columns, and it makes even a single column a struct (`MyStruct{ID int64}`). It must be a valid Go identifier that is not predeclared: `result_type: int64` (or `string`, `any`, ...) is rejected with an error, because it used to mean "the scalar" and now never does.

---

### 2.2. Scalar Parameters, Parameter Deduplication & 1D Slices

A parameter is declared where it is first used, always with a leading colon and its Go type: `:name:Type` for a scalar, `:name:[]Type` for a slice (optionally followed by `ASC` / `DESC`, section 2.4), `:name:*Type` for a nullable value. Later uses repeat just `:name` (or `:name ASC|DESC` for a slice). Without the leading colon, `name:Type` is only an output column (section 2.1), and a `name:Type` anywhere else is an error that tells you to add the colon. The `Type...` / `name...` slice spellings do not exist.

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
- **Where a 1D slice may appear**: only as the whole list of an `IN (...)` / `NOT IN (...)` (`id IN (:ids:[]int64)`, also when repeated as `:ids`). It expands to comma-separated placeholders, so anywhere else (`x = :x:[]int64`, a function argument, next to other items in the list) is a generate-time error; use a scalar type for a single value.
- **Byte Slices**: `[]byte` (and `json.RawMessage`) is always treated as a **scalar** binary/JSON parameter, never expanded into comma-separated placeholders.
- **Empty collections** (the rule is structural: it depends on what the collection is used for, not on the statement kind):
  - An empty 1D slice (`len(userIDs) == 0`) in an active `IN (...)`/`NOT IN (...)`, and an empty tuple slice anywhere but as the row source of an `INSERT`, returns `runtime.ErrEmptySliceParam` before hitting the database: `IN ()` is invalid SQL, and what an empty list *means* (`NOT IN ()` is true for every row) is a decision for the caller, not the generator.
  - The one exception is the multi-row `VALUES` list of an `INSERT` (section 2.3): inserting nothing is a **successful no-op** (details there).
- **Argument style (`args:`)**: the `args:` key of a statement selects how its generated methods take the parameters. The value is an enum, one of:

  | Value | Call shape |
  |---|---|
  | `auto` (default when the key is omitted) | `struct` when the statement has `{?Flag: ...}` blocks, `positional` otherwise |
  | `positional` | each parameter is a Go argument: `q.GetUsersByIDs(ctx, orgID, userIDs)` |
  | `struct` | one generated `<Name>Params` struct: `q.GetUsersByIDs(ctx, GetUsersByIDsParams{OrgID: orgID, UserIDs: userIDs})` |

  Any other value is a generate-time error that lists the valid ones. The style only changes the signature: `Name`, `NameQuery`, the `Pipeline` method, `NameSeq`/`NameStream` and the SQL and caching behaviour are the same in every style, so switching a statement is a mechanical call-site change. `struct` is always legal. `positional` is rejected (`args: positional is not possible: ...`) for a statement with `{?Flag: ...}` blocks, because the blocks are switched by boolean fields that only the struct carries; `Seek` / `Order` parameters, slices, tuple slices and `sqlgen.Query` parameters are ordinary parameters and work in both styles. Choose `struct` explicitly for a statement with many parameters of the same type, where named fields read better than a long positional list. The argument styles are an extension point: a future style is a new value of this key, not a new key.

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

- A tuple template holds only fields and literal SQL: a statement parameter inside it (`(ID:int64, :batch:int64)`) is a generate-time error. Make the value a field of the tuple (`Batch:int64`) and set it in every row.
- Literal SQL expressions inside the tuple template (such as `CURRENT_TIMESTAMP`, `NOW()`, `'active'`, or `0`) are preserved verbatim in every expanded tuple row, while `FieldName:GoType` slots are extracted into the generated `NewOrderItem` struct and bound as positional parameters.
- Statements with **exactly 1 collection parameter** (a tuple slice or a 1D slice, at the top level of the statement, no `sqlgen.Order` / `sqlgen.Seek` / composed `sqlgen.Query`) whose result is the concatenation of per-chunk results (*decomposable*, section 3.2: no aggregate, `GROUP BY`, `DISTINCT`, `ORDER BY`, `LIMIT`, ...) are **chunked automatically**: the plain method splits the collection into wire-safe chunks and runs them in an implicit transaction (section 3.2). A decomposable statement (tuple or 1D slice) also gains `*Stream` (and, for `many`, `*StreamSeq`). A non-decomposable one runs as one statement and returns `runtime.ErrCollectionTooLarge` when the collection does not fit.
- **An empty `VALUES` list is a no-op.** For `INSERT ... VALUES :items:[]NewOrderItem(...)` (an `exec` statement, or a `many` one with `RETURNING`), `len(items) == 0` inserts nothing: `exec` returns a `sql.Result` with `RowsAffected() == 0` and a nil error, `many` returns no rows (`len(rows) == 0`; the slice is `nil`), `*Stream` of an empty iterator likewise. No SQL is sent and no transaction is opened. In a `Pipeline` / `PipelineTx` the future resolves to zero rows / an empty slice (not an error), the empty operation is never handed to a wire driver (`pgxwire`, `mysqlwire`), and a pipeline made only of such operations sends nothing, not even `BEGIN`. Its row count is known client-side (0), so an assertion on it outside any `If` branch is decided before anything is sent: `p.InsertEvents(nil).AssertRows(0)` costs nothing (the pipeline keeps its single round trip, or sends nothing if it has no other operation), and a failing one (`AssertFound()`, `AssertRows(n)` with `n > 0`) fails the pipeline with the usual `*runtime.AssertionError` before anything is sent (nothing is committed, as on the rollback of the sequential path). An assertion on an empty operation inside an `If` branch (it applies only if the branch is taken) and a condition over an empty operation (`p.If(empty.Found(), ...)`) are resolved on the sequential path (one statement at a time): correct, but not a single round trip. A tuple slice inside `IN (...)`, or a `VALUES` list that feeds a CTE or derived table rather than an `INSERT`, is not covered by this rule and stays `runtime.ErrEmptySliceParam` (what an empty list means there depends on how the rows are used).

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
2. **Global Sort Before Chunking**: In the plain `<Name>` method of a chunked statement, `sqlgen` sorts the entire $N$-item batch globally *before* splitting it into wire-safe chunks, guaranteeing globally monotonic lock acquisition across all chunks.
3. **`RETURNING` Row Order**: For `INSERT ... VALUES` / upserts driven by a presorted tuple slice, the database processes and returns rows in the **presorted order** (verified on PostgreSQL and CockroachDB, including across batch chunks), while your input slice remains in its original order. For `UPDATE` / `DELETE ... WHERE col IN (:ids ASC|DESC) ... RETURNING` the presort fixes the order of the *bound parameters* only: the database visits, locks and returns rows in the order its query plan chooses (PostgreSQL turns an `IN` list into a sorted index scan, so a `DESC` list has no effect). Treat the returned order of such statements as unspecified; if you need one, wrap the statement in a CTE and `ORDER BY` the result.
4. **What presorting does and does not buy you**: it makes lock acquisition monotonic where the engine processes keys in the order they are bound (multi-row `INSERT`/upsert `VALUES`). For `IN` lists the engine decides, so prefer `ASC` there (it matches the ascending index order engines use) and use `SELECT ... FOR UPDATE ... ORDER BY` first if you need a guaranteed lock order.

---

### 2.5. Conditional Blocks (`{?FlagName: ...}`) & `SelectCols` Bitmask Projection

Wrap optional SQL clauses in `{?FlagName: ...}`. `sqlgen` adds a `FlagName bool` field to `<Name>Params`, so a statement with flag blocks always takes the params struct (`args: auto` picks it; `args: positional` is an error, section 2.2):

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
- Flag blocks work anywhere in the statement, including the `SET` list of an `UPDATE`, which gives a partial update (PATCH) in one statement: `UPDATE members SET {?SetRole: role = :role:string, }{?SetScore: score = :score:int64, }updated_at = CURRENT_TIMESTAMP WHERE id = :id:int64`. A block is plain text, so put each assignment's comma inside its block and keep one unconditional assignment last (here `updated_at`), so that every combination of flags is valid SQL.
- If a `{?Flag: ...}` block wraps columns in the `SELECT` clause (`WithOrgDetails` above), those struct fields (`OrgName`, `OrgPlan`) are left at their zero values when `WithOrgDetails == false`, and scanned normally when `WithOrgDetails == true`.

#### Runtime Column Projection (`SelectCols`)

Every `kind: one` or `kind: many` statement with at least two unconditional output columns can return a subset of them, in either argument style. The column constants are generated per statement, `<Name>Col<X>` and `<Name>ColsAll` (statements sharing a `result_type` each get their own set, since each projects its own columns):

```go
// Generated bitmask constants for SearchMembers (returning MemberRow):
// SearchMembersColID, SearchMembersColOrgID, SearchMembersColEmail, SearchMembersColRole,
// SearchMembersColStatus, SearchMembersColScore, SearchMembersColsAll

// Struct style (SearchMembers has {?Flag: ...} blocks): the SelectCols field of <Name>Params.
// Compact list view: only fetch ID, Email, and Role over the wire!
rows, err := q.SearchMembers(ctx, SearchMembersParams{
    OrgID:      orgID,
    SelectCols: SearchMembersColID | SearchMembersColEmail | SearchMembersColRole,
})

// Positional style (a kind: one statement GetMember returning MemberRow): a trailing variadic argument, selectCols ...uint64. Several constants are
// combined (an already OR-ed mask works too); no argument returns every column.
m, err := q.GetMember(ctx, memberID, GetMemberColID, GetMemberColEmail)
```

- Selecting nothing (`SelectCols: 0`, or no variadic argument) or `SearchMembersColsAll` selects all unconditional columns.
- Setting specific bits omits unselected unconditional columns from the rendered `SELECT` list and skips scanning them, saving network bandwidth and allocation for wide rows; unselected fields keep their zero values.
- **Sort/cursor columns are always selected.** In a statement with `sqlgen.Order` (and `sqlgen.Seek`), every column of the `Order(...)` list (which includes the tie-break column) is added to any non-zero mask, because `Seek<Name>(row)` builds the next page's cursor from those fields: `SelectCols: PageItemsColName` returns `name` plus the sort columns.
- The projection exists whenever the statement has two or more unconditional output columns whose list is a plain span of the `SELECT` / `RETURNING` clause; it does not depend on the argument style. The variadic is the same final parameter of `<Name>`, `<Name>Query`, the `Pipeline` method, `<Name>Seq` and `<Name>Stream`, costs no allocation (the arguments live on the caller's stack), and never changes the default path: the call without it uses the same cached statement and fast slot as before. A positional parameter that maps to the Go field `SelectCols` (`:select_cols:...`) is rejected, as in the struct style.
- Why a trailing variadic: it keeps every existing positional call valid, keeps the parameters in the order of the SQL, and needs no second method per statement; the alternative (a leading mask argument) would make the common call carry a meaningless `0`.

---

### 2.6. Keyset Pagination (`sqlgen.Seek` & `sqlgen.Order`)

Avoid slow `OFFSET` pagination on deep pages using `After:sqlgen.Seek` (in `WHERE`) and `Sort:sqlgen.Order(col1, col2, ...)` (in `ORDER BY`).

**Requirements for correct pages.** The sort columns must be `NOT NULL`, and the last column listed must be unique (a primary key, say) so that it breaks every tie. `sqlgen` compares each column with `<` / `>` / `=`, and any comparison with `NULL` is unknown, so a row whose sort column is `NULL` is skipped by every page after the first; engines also disagree on where `NULL`s sort. If a column can be `NULL`, sort on `COALESCE(col, sentinel)` in a subquery or generated column instead. The pagination test suite (`integration/suite_test.go`) checks every ASC/DESC combination and page size against an in-memory oracle on every supported engine, on duplicate-heavy but non-null data. A seeded differential suite (`integration/fuzz_*_test.go`) additionally pages random `Order` specs of 1 to 12 columns, with mixed directions, tied keys and `int64` extremes, over every engine and SQLite (`SQLGEN_FUZZ_SEED` replays a failure).

```yaml
statements:
  - name: ListMembersPaginated
    kind: many
    result_type: MemberItem
    args: struct
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
1. Every column listed in `sqlgen.Order(col1, col2, ...)` must match an unconditional selected column name (or alias) in the `SELECT` list, and at most 12 columns may be listed (`runtime.MaxOrderCols`: the sort selection is packed into 5 bits per term, so every column of a valid directive can be named in one sort and none is silently dropped).
2. `sqlgen` generates strongly-typed sort column tokens (`<Stmt>OrderBy<Field>.Asc()` and `.Desc()`), a type-safe constructor `Order<Stmt>(...)`, and cursor helpers:
   - `Seek<Stmt>(row)` $\rightarrow$ returns a `runtime.Seek` cursor populated from `row` (a `<RowType>`) for the next page.
   - `NewSeek<Stmt>(...)` $\rightarrow$ the same cursor from the values of the `sqlgen.Order` columns, typed and in their declared order (`NewSeekListMembersPaginated(role string, score int64, createdAt int64, id int64)` for the statement above): the way to rebuild a cursor from a decoded page token. A token decoded into a value of the wrong type does not compile.
   - `Order<Stmt>(ListMembersPaginatedOrderByScore.Desc(), ListMembersPaginatedOrderByID.Asc())` $\rightarrow$ builds the `runtime.Order` value.

   **One set per statement.** Every statement with a `sqlgen.Order` gets `Order<Stmt>`, `<Stmt>OrderBy<Col>`, `<Stmt>SortCol` / `<Stmt>SortTerm` and (with a `sqlgen.Seek`) `Seek<Stmt>`; nothing is keyed by the row type. Statements that share one `result_type` may therefore declare different `sqlgen.Order` lists, each with its own tokens and its own `Seek<Stmt>` (there is no cursor method on the row type: a row type shared by statements with different sort lists has no single cursor).

   **Misuse is caught.** `Order<Stmt>` takes `<Stmt>SortTerm` values, a type of its own per statement, so passing the tokens of another statement, a column without a direction, or a plain `uint8`, does not compile. A term list that repeats a column, or contains a term for a column the statement does not have (only reachable by converting an arbitrary number, `<Stmt>SortCol(40).Asc()`), makes the statement method return `runtime.ErrInvalidOrder` instead of running a different sort. A cursor (`Seek<Stmt>(row)`, or `NewSeek<Stmt>(...)`) always carries every `sqlgen.Order` column of its statement, with its Go type, so it cannot disagree with the sort, and a value of the wrong type does not compile (a wrongly typed value would otherwise bind silently, and SQLite, which compares across types, would return a wrong page). A hand-built `runtime.Seek{Values: ...}` with the wrong number of values returns `runtime.ErrInvalidSeek`; do not build one.
3. **First Page vs. Subsequent Pages**:
   - On Page 1, pass a zero `runtime.Seek{}` (`After: runtime.Seek{}`). `sqlgen` renders `TRUE` and binds `0` cursor parameters.
   - On Page 2+, pass `After: Seek<Stmt>(lastRow)`.
   - If all sort columns share the same direction (all `ASC` or all `DESC`), `sqlgen` renders compact row-value comparison `(col1, col2) > (?, ?)` (or `<`).
   - If sort directions are **mixed** (e.g. `score DESC, id ASC`), `sqlgen` automatically renders a B-tree index-friendly bounding-box prefix (`col1 <= ?`) combined with exact lexicographical tie-breaking `OR` branches!

4. **Defaults for a short or empty `Sort`.** The last column listed in `sqlgen.Order(...)` (the unique tie-breaker) is always part of the sort. A zero `runtime.Order` (an unset `Sort` field, or `Order<Stmt>()`) sorts by that column ascending only: `ORDER BY id ASC`. A partial `Order` keeps the terms you gave, in your order, and appends the tie-breaker if you did not name it, using the direction of the **last term you gave** (so `Desc(score)` gives `score DESC, id DESC`, and `Desc(score), Asc(role)` gives `score DESC, role ASC, id ASC`); name the tie-breaker yourself to choose its direction.

```go
sortSpec := OrderListMembersPaginated(
    ListMembersPaginatedOrderByRole.Asc(),
    ListMembersPaginatedOrderByScore.Desc(),
    ListMembersPaginatedOrderByCreatedAt.Asc(),
    ListMembersPaginatedOrderByID.Asc(),
)

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
        After: SeekListMembersPaginated(last),
        Sort:  sortSpec,
        Limit: 25,
    })
    _ = page2
}
```

---

### 2.7. Runtime Query Composition (`sqlgen.Query` Currying)

Every `subquery` statement, and every `one` or `many` statement that is a `SELECT` (including `WITH ... SELECT`), `<Name>` generates a currying constructor `<Name>Query(...) runtime.Query`, a package-level function (there is no `*Queries` method form: it is stateless, composition happens before any DB handle exists, and a method would only ignore its receiver). You can compose curried queries into parent queries using `:sub:sqlgen.Query` slots inside `WITH` CTEs, `FROM (...)`, or `WHERE col IN (:sub:sqlgen.Query)`:

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
    args: struct
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
orgsQ := ActiveOrgsQuery([]string{"enterprise", "pro"})
membersQ := CohortMemberIDsQuery(CohortMemberIDsParams{
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
    runtime.WithMaxBatchParams(4000),
)
defer q.Close()
```

### 3.1. Reading Modes (`kind: one` & `kind: many`)

For a `kind: exec` statement `<Name>`:
- `q.<Name>(ctx, ...) (sql.Result, error)`

For a `kind: one` statement `<Name>`:
- `q.<Name>(ctx, ...) (<RowType>, error)` — returns `sql.ErrNoRows` if no row matches.

For a `kind: many` statement `<Name>` there are exactly two methods:
- `q.<Name>(ctx, ...) ([]<RowType>, error)` — returns all matching rows as a slice. A query that matches nothing returns a `nil` slice and no error: test `len(rows) == 0`, not `rows == nil`.
- `q.<Name>Seq(ctx, ...) iter.Seq2[<RowType>, error]` — Go 1.23+ range-over-func iterator that streams rows in $O(1)$ memory and automatically closes `*sql.Rows` (and releases the cached `*ManagedStmt`) even if the caller exits the `for range` loop early with `break` or `return`, and when the loop body panics.

`<RowType>` is the row struct (`<Name>Row`, or your `result_type`), or, for a statement with a single output column and no `result_type`, the column's own type (`int64`, `time.Time`, `[]byte`, `*string`, ...): `q.CountUsers(ctx) (int64, error)`, `q.UserEmails(ctx) ([]string, error)`.

The other consumption styles are helpers over the iterator, in `runtime`, and work for every `iter.Seq2[T, error]` (every `<Name>Seq`, `<Name>StreamSeq`, `runtime.MapSeq2(...)`):
- `runtime.Collect(seq) ([]T, error)` — the rows as a new slice (what `q.<Name>` does). On an error it returns `(nil, err)`, never a partial slice.
- `runtime.AppendTo(dst []T, seq) ([]T, error)` — appends the rows to the caller's slice (like `append`: use the returned slice, which reuses `dst`'s backing array while its capacity suffices; pass `buf[:0]` to reuse a buffer across calls and avoid growing a new slice per call). On an error it returns `(dst, err)` with `dst` exactly as you passed it, so the data you already held is never dropped. It allocates nothing beyond what `append` grows.
- `runtime.Each(seq, fn func(T) error) error` — invokes `fn` for each row, in order. The first error of the sequence or of `fn` stops the iteration and is returned unchanged; `fn` returns `runtime.ErrStop` (or an error wrapping it) to end the iteration early without an error (`Each` then returns `nil`). A nil `seq` or `fn` is `runtime.ErrNilArgument`. A panic in `fn` still releases the rows.

```go
rows, err := runtime.AppendTo(buf[:0], q.ListOpenOrdersSeq(ctx, symbol))        // reuse a buffer
err = runtime.Each(q.ListOpenOrdersSeq(ctx, symbol), func(o OpenOrderRow) error { // callback style
    return process(o)
})
```

---

### 3.2. Automatic Chunking & $O(1)$-Memory Streaming (`<Name>`, `*Stream`, `MapSeq2`)

#### Automatic Wire-Safe Chunking (the plain `<Name>` method)

Database drivers enforce hard limits on parameters per statement (e.g., 65,535 in PostgreSQL, 32,766 in SQLite). `sqlgen` starts a chunk at no more than `16384` parameters by default (see `WithMaxBatchParams`). When a `kind: exec` or `kind: many` statement has exactly one collection parameter (a tuple slice `[]Tuple` or a 1D slice `[]T`) and no `sqlgen.Order`, `sqlgen.Seek` or composed `sqlgen.Query` parameter, the plain method `<Name>` (and, for `many`, `<Name>Seq`) splits the collection into chunks that fit; there is no separate "batch" method to remember to call:

- `kind: exec`:
  - `q.<Name>(ctx, ...) (sql.Result, error)` — the result sums `RowsAffected()` over all chunks (its concrete type is `runtime.BatchExecResult`; `LastInsertId()` is the one of the final chunk).
- `kind: many`:
  - `q.<Name>(ctx, ...) ([]<RowType>, error)` — the rows of all chunks, concatenated in chunk order (so `RETURNING` rows keep the presorted order across chunks).
  - `q.<Name>Seq(ctx, ...) iter.Seq2[<RowType>, error]` — the same rows as an iterator. It runs the chunks one after the other but does not open a transaction of its own (a lazily consumed iterator cannot own one): run it on a `WithTx`/`InTx` query when a multi-chunk `INSERT ... RETURNING` must be all or nothing.

The pipeline method (`pl.<Name>(...)`) queues the statement as one operation: it does not chunk, so split the collection yourself if it can exceed the parameter limit. A collection that cannot fit one statement at all (the wire limit: 65,535 placeholders on the `$N` dialects, 32,766 on the `?` ones, less the statement's other parameters, over the tuple width; `WithMaxBatchParams` does not apply, since nothing is chunked) fails the pipeline with a `*runtime.CollectionTooLargeError` (its `Pipeline` field is true) before anything is sent: nothing reaches the database, no transaction is begun.

**Only decomposable statements are chunked.** Chunking is correct only when the statement's result is the concatenation of the per-chunk results. `sqlgen` decides this lexically and conservatively at generate time; when it cannot be sure, the statement is *not decomposable*. A statement is decomposable when:

- its collection is the `VALUES` list of a top-level `INSERT` (with or without `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` / `RETURNING`), or
- its collection is a positive `IN (...)` list (1D or tuple slice) that is a whole conjunct of the top-level `WHERE` of a `SELECT`, `UPDATE` or `DELETE` (`... WHERE org = :org AND id IN (:ids:[]int64)`, also inside parenthesised `AND` groups), with or without `RETURNING`, or
- its collection is a `VALUES` list joined as a row source into a top-level `UPDATE` or `DELETE` (a bulk update), with or without `RETURNING`: `UPDATE t SET n = n + v.d FROM (VALUES :rows:[]D(ID:int64, D:int64)) AS v(id, d) WHERE t.id = v.id`, the same with `WITH v(id, d) AS (VALUES ...) UPDATE ... FROM v WHERE ...` (the statement's only CTE, named only by that `FROM`), `DELETE FROM t USING (VALUES ...) AS v(id) WHERE t.id = v.id`, or `UPDATE t JOIN (VALUES ...) AS v(id, d) ON t.id = v.id SET ...`. The join must be an inner join with an equality between a column of `v` and a column of another table as a whole conjunct of the top-level `WHERE` / `ON` (no `OR` at the top level), and `v` may appear elsewhere only as `v.col` (not as a table in a sub-query: `(SELECT sum(d) FROM v)` is not row-wise). Each chunk then changes the rows its own `v` rows match; as with `IN` lists, a target row matched by `v` rows in several chunks is changed once per chunk (de-duplicate the keys first). (MySQL accepts a `VALUES` table only as `VALUES ROW(...), ROW(...)`, which a tuple slice does not generate: there, write bulk updates as `INSERT ... ON DUPLICATE KEY UPDATE` or an `IN` list.)

and the top level of the statement (plus any parenthesised group around the `IN` list) contains none of: an aggregate function (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `array_agg`, `string_agg`, `GROUP_CONCAT`, `json_agg`, ...), `GROUP BY`, `HAVING`, `DISTINCT` (incl. `DISTINCT ON`; the comparison `IS [NOT] DISTINCT FROM` does not count), `ORDER BY` (a global order is not the concatenation of per-chunk orders), `LIMIT` / `OFFSET` / `FETCH` (incl. MySQL `UPDATE`/`DELETE ... ORDER BY ... LIMIT`), a window function (`OVER`, `WINDOW`), a set operation (`UNION`, `INTERSECT`, `EXCEPT`) or, for an `IN` list, `OR`. Optional `{?Flag: ...}` blocks count as if they were on. An `IN` list under `NOT` (`NOT IN`, `NOT (... IN ...)`), inside `CASE`, a comparison or `IS ...`, in an `ON`/`SET`/projection clause, or inside a sub-query, CTE body or derived table (including `WITH v AS (VALUES :rows:[]T(...)) UPDATE ...`), and an `INSERT ... SELECT`, are not decomposable. Sub-queries that do *not* contain the collection (a scalar sub-query in the projection, even with `COUNT(*)`; an `EXISTS` in another conjunct; a CTE that does not read the collection) are evaluated per row or independently of the chunking, so they do not matter. User-defined aggregate functions are not recognised.

A non-decomposable statement is never chunked and has no `<Name>Stream` / `<Name>StreamSeq` form. A collection that fits one statement runs exactly as before (one statement); one that does not fit returns an error matching `runtime.ErrCollectionTooLarge` (a `*runtime.CollectionTooLargeError` naming the statement, the collection size and the limit) **before anything is sent**: no statement, no transaction. Raise `WithMaxBatchParams` (up to the wire limit) or split the work yourself when a larger collection is legitimate (for example, run a `GROUP BY` per slice of ids and merge the groups in Go). Pipeline operations never chunk any statement (see above).

**How Adaptive Chunking (`PlanBatchChunk`) Works**:
- When a collection of size $N$ exceeds the driver's wire parameter limit (`MaxRowsPerBatch`), `sqlgen` automatically splits the slice into wire-safe chunks.
- **Repeated values in an `IN` list.** The chunks are separate statements, so a value repeated in a collection that spans several chunks is matched once per chunk: an idempotent statement (`DELETE ... WHERE id IN (...)`, a `SELECT`) is unaffected, but `UPDATE ... SET n = n + 1 WHERE id IN (...)` applies the increment once per chunk that contains the id. De-duplicate the ids first when that matters.
- **A collection that fits runs as ONE statement.** When all $N$ rows fit one statement under the limit, the method sends exactly one statement of exactly $N$ rows: one round trip, statement-level atomicity, no implicit transaction, and identical behaviour on the first call with a size and on every later call (`3` rows are one 3-row statement, never `2 + 1`). Only a collection above the limit is chunked: full-limit chunks, then the remainder as one exact chunk, all inside the implicit transaction (or the caller's).
- **Heavy-Hitter Promotion**: When a batch size (e.g., `500` rows or `400` rows) recurs (`>= promoteThreshold`), `StmtCache` promotes that exact size to a Tier-2 prepared statement on its next miss. A size that does not recur stays a plain Tier-1 SQL string under the usual admission filter, so one-off sizes (a `1400`-row spike) run as one exact statement without displacing prepared statements.

#### $O(1)$-Memory Streaming Ingestion (`*Stream` & `*StreamSeq`)

When a statement is chunked (it is decomposable, see above), whether its collection parameter is a tuple slice (`[]Tuple`) or a 1D slice (`[]T`, say the ids of `DELETE ... WHERE id IN (:ids:[]int64)`), `sqlgen` also generates `<Name>Stream` (and, for `many` statements, `<Name>StreamSeq`). They take the lazy `iter.Seq2[<Elem>, error]` (`<Elem>` is the tuple type or `T`) **in place of** the collection parameter (same position in the argument list), so an arbitrarily large or lazy source is ingested in $O(1)$ memory. An empty source fails like an empty `IN` list (`runtime.ErrEmptySliceParam`; only an empty `INSERT ... VALUES` source is a no-op), and each chunk is presorted on its own when the collection has `ASC` / `DESC`:

- `kind: exec`:
  - `q.<Name>Stream(ctx, ..., seq iter.Seq2[<Elem>, error], ...) (sql.Result, error)` — pulls items from `seq` into a pooled chunk buffer (`256` items or `MaxRowsPerBatch`, whichever is smaller), flushes each chunk via `<Name>`, and returns the summed result (`res.RowsAffected()` is the total).
- `kind: many`:
  - `q.<Name>Stream(ctx, ..., seq iter.Seq2[<Elem>, error], ...) ([]<RowType>, error)`
  - `q.<Name>StreamSeq(ctx, ..., seq iter.Seq2[<Elem>, error], ...) iter.Seq2[<RowType>, error]`

`seq` is the only source of rows. With the struct argument style (`q.<Name>Stream(ctx, p, seq)`) the collection field of `p` must be empty: setting it returns an error matching `runtime.ErrInvalidArgument` that names the field (it used to be silently ignored). Emptiness follows the same structural rule as the plain method: an empty `seq` for an `INSERT ... VALUES` statement is a successful no-op, for a tuple `IN (...)` list it is `runtime.ErrEmptySliceParam` (nothing is sent).

**Atomicity.** `<Name>` of more than one chunk runs in its own transaction (all or nothing, unless called on a `WithTx`/`InTx` query, which uses the caller's): when a later chunk fails, the earlier ones are rolled back. A stream is not one transaction: every flush (up to `256` items) is atomic, but an error or a cancelled context part-way leaves the earlier flushes committed. Run the stream inside `q.InTx` for all-or-nothing ingestion. An error yielded by `seq` stops the stream and is returned (or yielded) as-is. A nil `seq` (and a nil callback passed to `runtime.Each`, `InTx` or `PipelineTx`) returns `runtime.ErrNilArgument` instead of panicking. A stream of an empty iterator inserts nothing (section 2.3).

**Iterator Adapters in `runtime`** (all return plain `iter.Seq2[T, error]`):
- `runtime.FromSlice(items []T) iter.Seq2[T, error]` — wraps an in-memory slice.
- `runtime.FromSeq(seq iter.Seq[T]) iter.Seq2[T, error]` — wraps a non-error `iter.Seq[T]`.
- `runtime.MapSeq2(seq iter.Seq2[In, error], fn func(In) (Out, error)) iter.Seq2[Out, error]` — transforms a streaming query (`q.StreamOrdersSeq(...)`) directly into the input stream of a `<Name>Stream` or `<Name>StreamSeq` in $O(1)$ memory; the first error from `seq` or `fn` ends the stream.
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
}, runtime.WithTxRetry(runtime.DefaultRetryPolicy()))
```

- `runtime.WithTxRetry(policy runtime.TxRetryPolicy)` automatically retries the entire transaction closure with exponential backoff + full jitter when the database returns a transient serialization failure (`SQLSTATE 40001`), deadlock (`SQLSTATE 40P01`, MySQL `1213`), a TiDB optimistic write conflict (`9007`, `8022`), or lock timeout (`SQLSTATE 55P03`, MySQL `1205`, SQLite `SQLITE_BUSY` / `SQLITE_LOCKED`).
- **Ambiguous Commit Protection**: If a network drop (`driver.ErrBadConn`, `io.EOF`, `net.Error`) occurs *during* `tx.Commit()`, `InTx` **never** retries automatically because the transaction may have already committed on the server.

---

### 3.4. Single-Roundtrip Speculative Pipelining (`Pipeline` & `PipelineTx`)

`Pipeline` and `PipelineTx` allow you to queue multiple reads, writes, row-count assertions, and data-dependent `If/Then/Else` branches and flush them together.
- With the `pgxwire` driver (below) on PostgreSQL, CockroachDB or YugabyteDB, or the `mysqlwire` driver (below) on MySQL, MariaDB or TiDB, the entire conditional workflow, `BEGIN` and `COMMIT` included, is sent to the server in **one network round-trip** (PostgreSQL protocol: one write, one `Sync`; MySQL protocol: one multi-statement text).
- On every other driver and engine (plain `*sql.DB`, SQLite, ...), and for any batch a wire driver declines (see "Falling back" below), `Pipeline` seamlessly falls back to sequential execution while preserving identical branching, assertion, and rollback semantics. Nothing to configure, and nothing silently pretends to be faster than it is: see "Verifying which path ran" below.

**Engine support.** The CI matrix runs the full suite against PostgreSQL 15 and 18, CockroachDB, MySQL 8.4 and 9.7, MariaDB 10.11 and 12.3, TiDB 8.5, YugabyteDB 2025.2 and SQLite per push, with real drivers (`pgx`, `go-sql-driver/mysql`). The two single-roundtrip batch renderings are different programs for the same contract: on PostgreSQL, CockroachDB and YugabyteDB (`$N` dialect) one statement per operation built on `set_config` / `current_setting` and data-modifying CTEs (`pgxwire`); on MySQL, MariaDB and TiDB (`?` dialect) a short statement sequence per operation over session user variables, `ROW_COUNT()` and a conditional `COMMIT` / `ROLLBACK`, with the arguments inlined as literals (`mysqlwire`, see "The MySQL and MariaDB path" below). SQLite has neither and always runs through the sequential fallback (same semantics, one round trip per statement).

#### Wiring the single-round-trip path (`pgxwire`)

`pgxwire` is a separate Go module (`github.com/CAFxX/sqlgen/pgxwire`, depends on `pgx/v5`), so the main `sqlgen` module keeps depending only on `yaml` and `sqlite`. It returns an ordinary `*sql.DB`; everything that is not a sqlgen pipeline behaves exactly as with pgx's stdlib driver.

```go
import (
    "github.com/CAFxX/sqlgen/pgxwire"
    "github.com/CAFxX/sqlgen/runtime"
)

db, err := pgxwire.OpenDB("postgres://user:pass@host:5432/app?sslmode=require")
if err != nil { ... }
db.SetMaxOpenConns(16) // 1 also works, but loses the one-turn transaction (see the conditions below)
q := store.New(db)

err = q.PipelineTx(ctx, nil, func(p *store.Pipeline) error {
    p.AddScore(-5, from).AssertRows(1)
    p.AddScore(5, to).AssertRows(1)
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
| `q.WithTx(tx).Pipeline(ctx)` ... `p.Flush()` (a `*sql.Tx` you began) | **1** for the operations only | `BEGIN` and `COMMIT`/`ROLLBACK` are yours (`db.BeginTx`, `tx.Commit`): 2 more round-trips you chose to make. A failed assertion or `Abort` leaves the transaction usable, exactly as on the sequential path (see "Who rolls back" below); a database error of one of your statements aborts it, as PostgreSQL does on every path: roll it back. |
| `q.InTx(...)`, plain query methods | unchanged (pgx's usual behaviour) | Only pipelines are batched. |
| `db.SetMaxOpenConns(1)` (pgxwire) | **3**: `BEGIN`, the operations in one batch, `COMMIT` | With one open connection the runtime never borrows a second one for the batch (that would deadlock against itself): it begins an ordinary transaction on the only connection and sends the operations through that connection's pipeliner. No deadlock, same results and errors, but `BEGIN` and `COMMIT` are separate turns. Counted in `WirePipelines`, not `SequentialPipelines`. |
| any other driver, or a `*sql.DB` not opened through pgxwire | sequential fallback (`BEGIN`, one per statement, `COMMIT`) | `CacheStats().SequentialPipelines` counts these. |

An empty pipeline, or one whose `fn` returned an error, sends nothing. Calling `Flush()` inside a `PipelineTx` callback returns `runtime.ErrFlushInPipelineTx`; the transaction flushes the pipeline itself.

**Failure semantics.** Any failure (a failed assertion, an `Abort`, a server error in the middle of the batch, a serialization failure or deadlock, a dropped connection) rolls the transaction back and surfaces the same error value the sequential path returns (`runtime.ErrAssertionFailed`, your `Abort` error, the `*pgconn.PgError`). When a statement fails the server skips the rest of the batch and leaves the transaction aborted, so pgxwire sends one extra `ROLLBACK` round-trip (error path only) and the connection goes back to the pool clean; if even that fails the connection is closed and the pool replaces it. A failure raised by the `COMMIT` itself (a deferred constraint, a serialization failure under `SERIALIZABLE`) is reported like any other and is retried by `WithTxRetry` when `runtime.IsRetryableTxError` says so; a dropped connection during the batch has an unknown outcome (the `COMMIT` may have been processed) and is never retried.

**How a first-seen pipeline stays at one round-trip.** pgx's usual query modes (`QueryExecModeCacheStatement`, the default, and `CacheDescribe`) first *describe* every statement text they have not seen, which is a separate round-trip, before running the batch. pgxwire therefore sends each pipeline statement the way `pgx.QueryExecModeExec` does: unnamed Parse/Bind/Describe/Execute messages, parameters in text format with no declared types (the server infers them from context), results in text format decoded locally into the same Go values pgx's stdlib driver produces. Consequences, all measured below:
- A first-seen pipeline costs exactly as many round-trips as a warmed one: one.
- pgx's statement cache is not used for pipelines, so the server parses each statement of each flush (server CPU, never a round-trip). A code path that gains nothing from batching can stay on plain query methods, which still use prepared statements.
- Parameter types are inferred by the server from the SQL, as a describe would have inferred them. A placeholder the server cannot type from context (a bare `SELECT $1`) fails with "could not determine data type of parameter" on either path and needs a cast in the SQL; sqlgen already adds casts where it generates such contexts (see "Typed placeholders" below). Because the server, not pgx, decides the type, the Go arguments are encoded so that they mean the same as on a prepared statement; see "Argument types on the single-round-trip path" below.
- pgx query tracers do not see pipeline batches (they run below `pgx.Conn`); use the counters below.
- If your connection goes through a transaction-pooling proxy (PgBouncer in transaction mode), this is the safe mode: nothing is prepared server-side.

**Writing a driver for another engine or protocol.** The interfaces a driver connection implements live in `github.com/CAFxX/sqlgen/runtime/wire` (`wire.Pipeliner`, `wire.TxPipeliner`, `wire.Flavored`, plus the helper `wire.RowsFromDriverRows`); the runtime finds them by structural type assertion, and applications never import that package. The data a driver receives and fills in (`runtime.PipelineOp`, `runtime.ProducedCond`, `runtime.CommitError`, `runtime.ErrWireNotApplicable`, `runtime.TranslateWireError`, `runtime.RenderMySQLBatch` and friends) is owned by the pipeline core and stays in `runtime`. Implement `wire.Pipeliner` (`ExecPipeline(ctx, ops)`: the operations only) and, if you can fold the transaction boundary in too, `wire.TxPipeliner` (`ExecTxPipeline(ctx, txOpts, ops)`). First decide the *flavor*: a connection that also implements `wire.Flavored` (`WireFlavor() runtime.WireFlavor`) tells the runtime how its batch must be rendered. `WireFlavorPostgres` (the default, and what a connection without `wire.Flavored` is): the runtime renders one self-contained statement per operation into `ops[i].SQL` / `ops[i].Args`, as described next. `WireFlavorMySQL`: the runtime does not pre-render; your connection calls `runtime.RenderMySQLBatch(ops, runtime.MySQLRenderOptions{Flavor: &runtime.MySQLFlavor{Loc, Charset, Returning}, InTx, TxOpts})` (it needs to know the time zone and character set of the inlined literals, whether the caller's transaction is open, and whether the server has `RETURNING`; a driver for another MySQL-protocol server, TiDB for instance, fills `MySQLFlavor` accordingly), sends the one multi-statement text `MySQLBatch.SQL`, reads the answer as laid out by `MySQLBatch.ResultSetOps()` (result set k belongs to that operation; the summary row follows) and hands the summary row to `MySQLBatch.Apply`, which fills in counts and conditions and builds the assertion / `Abort` error. `mysqlwire/mysqlwire.go` is the complete example of that flavor. Either way, a driver that finds out BEFORE sending anything that it cannot run this particular batch in one round-trip (it is larger than the server's packet limit, an argument cannot be represented, a capability probe failed) returns an error wrapping `runtime.ErrWireNotApplicable`; the runtime then runs the whole pipeline sequentially and counts it in `SequentialPipelines`. For the PostgreSQL flavor: per op, `op.WireResult()` says how to read the answer: set `op.RowCount` from the command tag, from a guarded `(affected, ok)` row, or by feeding result rows to `op.ScanWireRows` (wrap a `driver.Rows` with `wire.RowsFromDriverRows`); for ops with `Decorators.ProduceCond` (and each `ExtraConds` entry) read the condition back with `SELECT current_setting('sqlgen.c<CondID>', true)` inside the same batch into `CondVal`/`CondSet`; map server errors with `runtime.TranslateWireError(op, err)`; wrap an error raised by the `COMMIT` in `*runtime.CommitError`; and leave no transaction open on the connection when you return an error. Wrap your connector with `runtime.WrapConnector` so user-managed transactions reach `ExecPipeline`. `pgxwire/pgxwire.go` is a complete example.

**Verifying which path ran.** `q.CacheStats()` has two counters, incremented once per flushed pipeline (once per `PipelineTx` attempt):
- `WirePipelines`: the flush went out in a single round-trip through a `wire.Pipeliner` / `wire.TxPipeliner` connection.
- `SequentialPipelines`: the flush executed statement by statement.

A flush that fails while being built or that queued nothing counts as neither; a flush that fails on the server still counts on the path it ran. A test asserting `WirePipelines > 0 && SequentialPipelines == 0` fails loudly when a `*sql.DB` opened with plain `sql.Open("pgx", ...)` silently loses the fast path. To assert the one-turn transaction itself (not just the batched operations), count network turns as below: `SetMaxOpenConns(1)` keeps `WirePipelines` counting but costs `BEGIN` and `COMMIT` as extra turns.

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

#### The MySQL and MariaDB path (`mysqlwire`)

`mysqlwire` (`github.com/CAFxX/sqlgen/mysqlwire`, a nested module that depends on `go-sql-driver/mysql`) gives MySQL 8.4 / 9.7, MariaDB 10.11 / 12.3 and TiDB 8.5 the same single-round-trip `Pipeline` / `PipelineTx`: `START TRANSACTION`, every operation and `COMMIT` go out as ONE multi-statement text. Wiring and call sites are identical to `pgxwire`:

```go
db, err := mysqlwire.OpenDB("user:pass@tcp(host:3306)/app?parseTime=true")   // or sql.OpenDB(mysqlwire.Connector(cfg))
q := store.New(db)
err = q.PipelineTx(ctx, nil, func(p *store.Pipeline) error {
    p.AddScore(-5, from).AssertRows(1)
    p.AddScore(5, to).AssertRows(1)
    return nil
})
fmt.Println(q.CacheStats().WirePipelines) // 1
```

The table above ("When the single-round-trip path is used") applies unchanged, with `START TRANSACTION` for `BEGIN`; the measured numbers are below.

**What the connector changes, and what it requires.**
- **`multiStatements=true` is forced.** That is how several statements travel in one packet; `OpenDB` / `NewConnector` / `Connector` set it whatever the DSN says (the `*mysql.Config` you pass is copied, not modified; nothing in the driver is specific to one server: the capability probe decides, so another MySQL-protocol server that passes it, TiDB for instance, works the same way, without `RETURNING`; `loc`, `charset` / `collation`, `clientFoundRows`, `maxAllowedPacket` ... are kept). **Security note:** on this pool EVERY statement may carry several statements, so a SQL injection in a hand-written query that concatenates user input becomes much more damaging than on a plain go-sql-driver pool (a stacked `; DROP TABLE ...`). sqlgen's own statements never concatenate values (generated code binds arguments; the pipeline path inlines them by the rules below); keep hand-written queries on a plain `sql.Open("mysql", ...)` pool if you cannot rule injection out.
- **Connections are in autocommit mode**, and transactions are tracked through `database/sql` (`BeginTx`, `Commit`, `Rollback`); a hand-written `START TRANSACTION` / `COMMIT` sent with `Exec` is not seen.
- **Per connection**, at connect time, the driver reads `@@max_allowed_packet` and the connection character set in one extra turn (so the first pipeline on a connection already costs one turn). **Per connector**, once, it probes that the server (or a proxy in front of it) runs what the batches are made of (user variables, `ROW_COUNT()`, `FOUND_ROWS()`, a server-side `PREPARE` / `EXECUTE` / `DEALLOCATE`, several statements per text); a server that refuses it (some proxies and sharding layers) just keeps every pipeline on the sequential path.

**How a batch works** (full description in SPEC.md, "Wire flavors"). Per operation: `SET @sqlgen_p=<i>`, the statement (gated on `@sqlgen_f<0 AND` the `If` branch conditions `@sqlgen_c<ID>=1|0` where needed), `SET @sqlgen_n<i>=ROW_COUNT()` (`FOUND_ROWS()` after a SELECT); an assertion is `SET @sqlgen_f=IF(...)`, a condition `SET @sqlgen_c<ID>=COALESCE(...)`; the batch ends with `COMMIT` (or, when an assertion / `Abort` can fail, a `COMMIT` or `ROLLBACK` chosen server-side through `PREPARE`) and one summary row. Every variable is assigned in the same batch before it is read, so nothing depends on what a previous batch left in the session.

**Failure semantics.** Same observable contract as the PostgreSQL path (`runtime.ErrAssertionFailed` with `Statement` / `Want` / `Got`, your `Abort` error, the `*mysql.MySQLError`), with two mechanical differences. A failed assertion or `Abort` is not a server error: the rest of the batch is gated off, the transaction rolls back, the summary row says which operation failed and with how many rows, and no extra round-trip is needed (1 turn). A database error of one of the statements (a duplicate key, a deadlock, ...) is the real driver error: the connection then runs `ROLLBACK; SELECT @sqlgen_p` (one extra turn, error path only) to leave the transaction closed and to learn which operation it was; deadlock (1213) and lock-wait-timeout (1205) pass through unchanged, so `runtime.IsRetryableTxError` and `WithTxRetry` work as on the sequential path. An error of the `COMMIT` itself, and a transport error (an unknown outcome), are wrapped in `*runtime.CommitError` and are not retried after a dropped connection. Inside a transaction you began (`q.WithTx(tx).Pipeline(ctx)`) nothing is begun or ended by the batch; an operation with an assertion runs after a `SAVEPOINT`, a failed assertion costs one extra turn (`ROLLBACK TO SAVEPOINT`, which MySQL cannot run inside the batch's prepared statement) that undoes that operation and leaves the transaction usable, and a database error leaves the transaction open exactly like the sequential path (MySQL and MariaDB statement errors never abort the transaction). The savepoints are not released when the batch succeeds (they are replaced by the next batch; `COMMIT` / `ROLLBACK` clears them).

**Measured round-trips** (network turns per `PipelineTx`, established connection, identical on MySQL 8.4, 9.7 and MariaDB 10.11, 12.3; `integration/wire_my_roundtrip_test.go`):

| Workload | Sequential (plain go-sql-driver) | mysqlwire |
| :--- | :--- | :--- |
| 1 guarded update + read + `If`/`Else` + assertion | 12 first-seen / 10 warmed | **1** |
| 3 / 8 / 25 guarded updates (same shape) | 8 / 13 / 30 | **1 / 1 / 1** |
| standalone `Flush()` | 4 | **1** |
| `Flush()` in a user-managed tx | 2 | **1** (+1 for your `COMMIT`) |
| failed assertion or `Abort` (own transaction) | 4 | **1** (no server error, no recovery turn) |
| failed assertion in a user-managed tx | 2 | 2 (the batch + `ROLLBACK TO SAVEPOINT`) |
| database error (duplicate key) in `PipelineTx` / `Flush` / user tx | 6 / 4 / 2 | 2 (the batch + the recovery turn); the next pipeline on the connection is back to 1 |

The sequential figures are lower bounds that include statement-cache promotion turns. Wall clock (`make bench-engines ENGINES=mysql84,mariadb1011`, `BenchmarkMySQLPipelineTx`, loopback server in a VM, milliseconds per `PipelineTx`, sequential -> mysqlwire, turns in brackets; the server is fsync-bound so the 1 ms simulated latency per turn adds less than it would on a real network):

| Workload | MySQL 8.4 | MariaDB 10.11 |
| :--- | :--- | :--- |
| 1 statement | 5.0 (3) -> 1.3 (1) | 4.0 (3) -> 1.4 (1) |
| 5 guarded updates | 8.7 (7) -> 1.8 (1) | 9.8 (7) -> 1.7 (1) |
| 25 guarded updates | 34 (27) -> 4.1 (1) | 36 (27) -> 3.4 (1) |
| read-heavy (reads + `If` + assertions) | 27 (15) -> 2.6 (1) | 22 (15) -> 2.6 (1) |
| 25 guarded updates, +1 ms per turn | 37 (27) -> 4.2 (1) | 85 (27) -> 7.0 (1) |

Count turns the same way as above, with a custom dialer registered through `mysql.RegisterDialContext` (`integration/wire_my_roundtrip_test.go`).

**Falling back.** The connection answers `runtime.ErrWireNotApplicable` before sending anything, and the pipeline runs sequentially (same results; `SequentialPipelines` counts it), when: the rendered batch is larger than the connection's `max_allowed_packet` (the server would drop the connection if it were sent; the statements individually still fit, so each statement is sent on its own); the capability probe failed; an argument cannot be inlined (a Go type with no literal form, a `NaN` / `Inf` float, a year outside 0..9999, on TiDB a string that would need the `_<charset> 0x...` form); the `sql.TxOptions` isolation level has no MySQL equivalent (`LevelSnapshot`, `LevelLinearizable`; the four standard levels and `ReadOnly` map to `SET TRANSACTION ISOLATION LEVEL ...` and `START TRANSACTION READ ONLY` inside the batch); a statement has `RETURNING` on a server that has none (MySQL; MariaDB has it for `INSERT` and `DELETE`); or a `WhereCol` condition is evaluated over a `RETURNING` statement (it would have to run the write twice).

**Differences from the PostgreSQL path** (each is asserted by a test, not hidden):
- **`RETURNING`:** none on MySQL (no statement can have it); on MariaDB `INSERT ... RETURNING` and `DELETE ... RETURNING` work, including assertions and row-count conditions over them (an extra counter column is added and hidden); `WhereCol` over them falls back as above.
- **A `WhereCol` condition runs the SELECT twice** (once for its rows, once inside the aggregate that reads its `WhereCol` column: MySQL cannot aggregate over the rows a statement returned to the client), in the same transaction with the same arguments. Under `READ COMMITTED` the two executions can see different rows; under the default `REPEATABLE READ` they cannot.
- **`LastInsertId` is not available** for an operation run on the single-round-trip path: `ExecFuture.Result().LastInsertId()` returns `runtime.ErrLastInsertIDNotAvailable` (on every wire driver) instead of a silent 0; read the key back with a `SELECT`, or insert outside the pipeline.
- **A gated `SELECT` carries its gates in its own `HAVING` clause.** A `SELECT` inside an `If` branch, or after an assertion or a forwarded value's check, is sent as the statement itself with `HAVING <gates>` added (or `HAVING (<gates>) AND (<its own HAVING>)`), before its `WINDOW` / `ORDER BY` / `LIMIT` / `FOR UPDATE`: MySQL applies a `HAVING` without `GROUP BY` to every row (and to the single row of an aggregate), so a closed gate returns no row and an open one returns exactly the statement's rows, in the order of its own `ORDER BY` (a derived table would not keep it: MariaDB drops the `ORDER BY` of a derived table without `LIMIT`, and SQL leaves the order of its rows unspecified). A `SELECT` with a top-level `UNION` / `INTERSECT` / `EXCEPT`, or whose `HAVING`, `ORDER BY`, `LIMIT`, ... sits in an optional block, is wrapped in a derived table instead when it has no top-level `ORDER BY`, and runs on the sequential path when it has one.
- **A `WhereCol` condition, and a forwarded value's producer `SELECT`, use a derived table**, which MySQL requires to have unique column names: a select list such as `a.id, b.id` fails with error 1060 (give the columns aliases). The sequential path has no such limit.
- **A gated multi-row `INSERT`** becomes `SELECT ... FROM DUAL WHERE <gates> UNION ALL SELECT ...` (MySQL has no filterable `VALUES` table constructor), so the gate text is repeated per row.
- **Text protocol results:** the batch is a text query, so values come back in the text protocol (go-sql-driver then scans them exactly as it scans a prepared statement's, except that a single-precision `FLOAT` column is read as its shortest decimal, `0.1`, instead of the widened `float32`, `0.10000000149011612`). Use `DOUBLE` or `DECIMAL` where the exact value matters.
- **`FOUND_ROWS()`** (deprecated by MySQL 8.0.17 but present in every tested version, and the only way to count the rows a SELECT returned inside the same batch) is what the row count of a SELECT assertion uses.
- Prepared statements: the batch uses one server-side prepared statement name (`sqlgen_s`) per connection, only when a guard can fail, and deallocates it in the same batch; a failed `COMMIT` leaves it until the recovery turn removes it, so `Prepared_stmt_count` returns to its baseline after every path.

**Argument literals.** The protocol cannot bind parameters of a multi-statement text, so the arguments are inlined as literals by the renderer, which knows exactly where each placeholder is (it never searches text for `?`; quotes, comments and `?` inside your values are inert). The rules were verified against the prepared-statement path on all four engines and nine session variants (default, `NO_BACKSLASH_ESCAPES`, `ANSI_QUOTES`, non-strict `sql_mode`, `latin1`, `gbk`, `binary`, a non-UTC `loc` with a different `time_zone`, `utf8mb4_bin`):

| Go argument | Rendered as |
|---|---|
| integers | decimal; `bool` as `1` / `0`; `uint64` full range |
| `float32` / `float64` | shortest round-tripping exponent form (`float32` widened first, like the driver); `NaN` / `Inf` fall back |
| `string` | `'...'` with each `'` doubled; **as `_<charset> 0x<hex>` instead when it contains a backslash or the connection character set is `big5`, `cp932`, `gb2312`, `gbk`, `sjis` or `gb18030`** (escape rules there depend on `sql_mode` or on multi-byte characters), the empty string as `''`. On TiDB, which reads `_<charset> 0x<hex>` as a number in a numeric, temporal or JSON context, such a string is not inlined: the pipeline runs sequentially |
| `[]byte` | in a UTF-8 connection, valid UTF-8 bytes like a `string` (the prepared path sends them as text, so `[]byte("12")` stores 12 in a `BIGINT` and JSON text is accepted by a `JSON` column; a bare `0x3132` would be the number 12594); any other bytes `_binary 0x<hex>`; empty `''`, nil `NULL` |
| `time.Time` | `'YYYY-MM-DD HH:MM:SS[.fffffffff]'` in the connection's `loc` with every fractional digit (the server rounds or truncates to the column precision exactly as for a prepared statement); a time at exactly midnight as the bare date `'YYYY-MM-DD'` and the zero time as `'0000-00-00'` (what go-sql-driver sends, which matters for text and blob columns) |
| `nil`, nil pointers, `sql.Null*` | `NULL` or the value; `driver.Valuer` is resolved first, pointers are followed, named types follow their kind |
| anything else (structs, maps, slices other than `[]byte`) | not applicable: the pipeline runs sequentially |

Residual differences, each pinned by `TestRealEngineMySQLPipelineArgumentLiterals` (it compares stored bytes and error-ness against the prepared path on all four engines and nine session variants, and the hostile-string test `TestRealEngineMySQLPipelineArgumentsNeverTravelUnescaped` shows that quotes, backslashes, comment markers and stacked statements in values are stored verbatim): text written as `_<charset> 0x...` is refused (error 1300) when the character set cannot hold it (invalid UTF-8, or non-GBK text in a `gbk` session) where the prepared path stores the raw bytes in a BLOB; bytes in a `latin1` session written into a `utf8mb4` text column are refused (1366) where the prepared path converts them; a string such as `'-0.000001'` bound to a `BIGINT UNSIGNED` is rounded to 0 by the literal while the parameter is refused (1264); and server error numbers can differ between the two paths for the same rejected value. A pipeline fails in each of these cases, never stores something else.

Caveat of the hex form: `_utf8mb4 0x...` has the default collation of its character set, so comparing it with a column of another collation of the same set can fail with error 1267 (illegal mix of collations); it only arises for strings that contain a backslash (or the listed character sets), which the plain form cannot carry safely.

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

An `INSERT` queued inside a `p.If` branch is sent as `INSERT ... SELECT ... WHERE <gates>`, and PostgreSQL does not infer placeholder types from the target columns there. `sqlgen` therefore casts each placeholder from its Go type (`int`/`int64` to `bigint`, `string` to `text`, `bool` to `boolean`, `float64` to `double precision`, `[]byte` to `bytea`, `time.Time` to `timestamptz`, and so on). For any other Go type (for example a `uuid` or `jsonb` column fed by a `string`), add an explicit cast in the SQL, such as `:id:string::uuid`. The same applies where the generated cast is not the type of the column: a `time.Time` bound to a `timestamp` (without time zone) column inside a gated insert is first cast to `timestamptz`, so the instant is converted with the session time zone instead of keeping the Go wall clock; write `::timestamp` explicitly there (the "Argument types" rules above cover the ungated case).

```go
err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
    // 1. Queue a read or update and attach assertions:
    senderFut := p.GetAccount(orgID, senderID).AssertFound()

    // 2. Branch on the future's result and columns:
    p.If(
        senderFut.Found().And(
            senderFut.WhereCol("balance_cents >= ?", transferAmount), // bound, never spliced into the SQL
        ),
        func(thenP *Pipeline) {
            // Nested check: abort and roll back if account is frozen!
            thenP.If(
                senderFut.WhereCol("status = 'frozen'"),
                func(frozenP *Pipeline) {
                    frozenP.Abort(ErrAccountFrozen)
                },
                func(activeP *Pipeline) {
                    activeP.DebitAccount(orgID, senderID, transferAmount).AssertRows(1)
                    activeP.CreditAccount(orgID, receiverID, transferAmount).AssertRows(1)
                    activeP.InsertTransferReceipt(idempotencyKey, orgID, senderID, receiverID, transferAmount).AssertRows(1)
                },
            )
        },
        func(elseP *Pipeline) {
            // Insufficient funds: record rejection audit log and commit cleanly!
            elseP.InsertAuditEvent(orgID, senderID, "rejected_insufficient_funds", transferAmount).AssertRows(1)
        },
    )
    return nil
}, runtime.WithTxRetry(runtime.DefaultRetryPolicy()))
```

#### Pipeline Futures, Assertions & Branch Combinators

1. **Future Types**:
   - `*runtime.ExecFuture`: returned by `kind: exec` pipeline methods.
     - `.AssertRows(n int64) *ExecFuture`: fails the pipeline with an error matching `runtime.ErrAssertionFailed` if rows affected $\ne n$.
     - `.AssertFound() *ExecFuture`: fails the pipeline with an error matching `runtime.ErrAssertionFailed` if rows affected $\ne 1$.
     - `.RowsEq(n)`, `.RowsGt(n)`, `.RowsGe(n)`, `.RowsLt(n)`, `.RowsLe(n)`, `.Found()`, `.NotFound()`: returns a `PipelineCond` for `p.If(...)`.
     - The `Rows*` / `AssertRows` family is the same on every future (`ExecFuture`, `RowFuture`, `SliceFuture`): the count is rows affected for an exec statement and rows returned for a query.
     - `.Label(s string)` (every future, chainable): tags the operation; a failed assertion on it reports the label in `AssertionError.Label` and in the message (see *Assertion errors* below).
     - `.Result() (sql.Result, error)` and `.RowsAffected() (int64, error)` (valid after `Flush` / `PipelineTx` completes).
   - `*runtime.RowFuture[T]`: returned by `kind: one` pipeline methods.
     - `.AssertFound() *RowFuture[T]`: fails the pipeline with an error matching `runtime.ErrAssertionFailed` if the row count is not 1.
     - `.AssertRows(n int64) *RowFuture[T]`: fails the pipeline with an error matching `runtime.ErrAssertionFailed` if row count $\ne n$.
     - `.Found()`, `.NotFound()`, `.RowsEq(n)`, `.RowsGt(n)`, `.RowsGe(n)`, `.RowsLt(n)`, `.RowsLe(n)`: returns a `PipelineCond`.
     - `.WhereCol(sqlPred string, args ...any) PipelineCond`: true when the row was found and the predicate is `TRUE` for it (`NULL` counts as false). `sqlPred` is a boolean SQL expression that the server evaluates on every path, in the same round trip as the statement: it is added to the statement's own output column list as one more (hidden) column, `CASE WHEN (<sqlPred>) THEN 1 ELSE 0 END AS _sqlgen_wc<k>`, which never reaches the scanned row, the `SelectCols` projection or a forwarded value. So it is written in the scope of that list, as an extra output column would be: it can use the statement's tables and columns (and its aggregates, in a grouped or aggregate `SELECT`), but not the list's own aliases (write `SUM(qty) > ?`, not `total > ?`, for `SUM(qty) AS total`). The statement needs such a list: a `SELECT` that is neither `DISTINCT` nor a `UNION` / `INTERSECT` / `EXCEPT`, or a `RETURNING` clause; any other statement fails the pipeline with `runtime.ErrInvalidArgument`. Write one `?` per value in `args` (every `?` outside `'...'`, `"..."` and `` `...` `` counts, in every dialect; a count that does not match `args` fails the pipeline with `runtime.ErrInvalidArgument`). The values are bound, never spliced into the SQL text: placeholders on the sequential path and on `pgxwire` (where the server infers their type from the comparison), literals written by the same rules as the statement's own arguments on `mysqlwire` (an argument that has no literal form sends the pipeline down the sequential path): `f.WhereCol("balance_cents >= ?", amt)`. Never build `sqlPred` with `fmt.Sprintf` from values: bind them. (PostgreSQL's `jsonb` `?` operators need their function forms, for example `jsonb_exists(col, ?)`.) Several `WhereCol` on one future add one column each.
     - `.Get() (T, error)` (valid after flush).
   - `*runtime.SliceFuture[T]`: returned by `kind: many` pipeline methods.
     - `.AssertRows(n int64) *SliceFuture[T]`, `.AssertFound() *SliceFuture[T]`
     - `.Found()`, `.NotFound()`, `.RowsEq(n)`, `.RowsGt(n)`, `.RowsGe(n)`, `.RowsLt(n)`, `.RowsLe(n)`, `.WhereCol(sqlPred, args ...any)` (true when at least one row was returned and the predicate is `TRUE` for every row; `sqlPred` and `args` as for `RowFuture`)
     - `.Get() ([]T, error)` (valid after flush; no rows is an empty slice, not an error).
   - Reading any future's result (`Result`, `RowsAffected`, `Get`) before its pipeline has flushed (`Flush`, or the end of `PipelineTx`) returns `runtime.ErrFutureNotReady`, never a result that could be mistaken for a real one; after the flush a `*RowFuture` whose statement matched no row returns `sql.ErrNoRows`. **A failed pipeline has no results**: when `Flush` / `PipelineTx` returns an error (a failed assertion, `Abort`, a database error, a build error such as `ErrCollectionTooLarge`, a failed commit), the failing operation's future returns its own error and every other future returns the pipeline's error, also those of the operations that ran before the failure (nothing was committed, and the single-round-trip paths do not deliver the results of a failed batch); the same on every path. Read what you need for an error message after the failure, in a new query.
2. **Boolean Combinators & Control Flow**:
   - `condA.And(condB)`, `condA.Or(condB)`, `condA.Not()` combine `PipelineCond` values.
     The operands may come from different futures (`del.NotFound().And(get.Found())`): every leaf is evaluated over its own operation's rows, also on the single-round-trip path, where each earlier operation records its leaf server-side and the latest one combines the records, still in one network round-trip. A leaf over an operation that did not run (its branch was not taken) is false, so `Not` of it is true, identically on both paths.
   - `ifRes := p.If(cond, thenFn, elseFn)`: `ifRes.Taken()` and `ifRes.Skipped()` report whether the `then` branch was taken after flush.
   - **One mechanism for "this operation did not run"**: reading a future whose operation sits in an untaken `If`/`Else` branch returns `runtime.ErrBranchNotTaken` (test it with `errors.Is`); there is no separate `Skipped()` on futures. `ifRes.Skipped()` is about the block (it is inside an outer untaken branch), which has no error to carry the answer.
   - **Empty `VALUES` operations**: queuing an `INSERT ... VALUES` statement with an empty collection (section 2.3) queues an operation that sends no SQL: its future reports zero rows affected / no rows (a `Found()`/`RowsGt(0)` condition over it is false, `NotFound()` is true; `AssertRows(0)` holds), it is never handed to `pgxwire` / `mysqlwire`, and the other operations of the pipeline still run in one round trip. A pipeline in which an empty operation carries an assertion or a condition runs on the sequential path (counted in `CacheStats.SequentialPipelines`) so that the assertion and condition are evaluated in order; a pipeline made only of empty operations sends nothing at all. A failing assertion on an empty operation (say `AssertFound()`) fails the pipeline with a `*runtime.AssertionError` (got 0) before anything is sent, and the operation's own future reports that same error, like the future of any other failing operation.
   - `p.Abort(err)`: when reached inside an active branch, aborts the pipeline, returning `err` from `PipelineTx` / `Flush` (the transaction is rolled back only inside `PipelineTx` or a standalone `Pipeline`; see below).
3. **Assertion errors**: a failed assertion returns a `*runtime.AssertionError` (`errors.Is(err, runtime.ErrAssertionFailed)` is true; use `errors.As` to read the fields `Statement`, `Op`, `Label`, `Want`, `Got`, `GotKnown`). `Op` says *which* queued operation failed when one pipeline queues the same statement several times: it is the operation's 0-based position in the pipeline's queue (every `p.<Stmt>(...)` call and every `p.Abort` counts, in queue order, inside `If` branches too, taken or not). `Label` is an optional caller tag set with `.Label("...")` on any future (chainable: `p.Reserve(sku, qty).Label("cart line 3").AssertFound()`). The message names the statement, the operation, its label if any and the expected count, for example `sqlgen: pipeline assertion failed on statement "Reserve", operation 3 ("cart line 3"): expected 1 rows affected, got 0`. `Op` and `Label` are the same on the single-round-trip and the sequential path. On the single-roundtrip wire path the guard's error carries the actual row count too, so `Got` / `GotKnown` and the message are the same on both paths. Only the guard's own, marked error is translated: a genuine database error of one of your statements (an integer overflow, a unique violation, ...) that happens to carry an assertion is returned as that database error, identically on both paths.
4. **Who rolls back**:
   - `PipelineTx(ctx, opts, fn)` and a standalone `q.Pipeline(ctx)` (which runs in its own implicit transaction): a failed assertion, `Abort`, or any other error rolls the transaction back automatically; nothing the pipeline wrote is committed.
   - A pipeline on a **user-managed transaction** (`q.WithTx(tx).Pipeline(ctx).Flush()`): the pipeline never rolls the transaction back, and a failed assertion or `Abort` leaves it **usable on both paths**. `Flush` returns the error; the writes of the operations *before* the failing one (and everything else done on `tx`) stay in the open transaction; the operations after it never run; the caller then calls `tx.Rollback()` or deliberately `tx.Commit()`. One difference remains, by construction: sequentially the failing operation has already run when its assertion is checked in Go, so its own effect (say an update that touched 2 rows where 1 was asserted) also stays; on the single-round-trip path the guard fails that statement on the server, whose effect is therefore discarded. To keep the transaction alive pgxwire (and mysqlwire, which takes the savepoint only before operations with an assertion and does not release it on success) takes a `SAVEPOINT` before each guarded operation (inside the same batch, so no extra round-trip on success), releases it at the end of the batch, and on a guard failure rolls back to it, which costs one extra round-trip on that error path only. A *database* error raised by one of your own statements (a unique violation, a deadlock, ...) is not a guard failure: on PostgreSQL and CockroachDB it aborts the transaction on both paths (further commands fail with SQLSTATE 25P02 until you roll back), exactly as it does without sqlgen; MySQL, MariaDB and SQLite keep the transaction open.

#### Forwarding values between operations (`Arg[T]`)

A later operation of a pipeline can use, as an argument, a value an earlier operation produces on the server: the id an `INSERT ... RETURNING` generated, a `COUNT(*)`, a MySQL `AUTO_INCREMENT` id. The whole pipeline still travels in **one network round trip** on `pgxwire` and `mysqlwire` (the value never comes back to Go in between), and runs identically on the sequential path. A parameter opts in with the type `Arg[T]`:

```yaml
package: store
dialect: postgres
statements:
  - name: InsertOrder
    kind: one
    sql: INSERT INTO orders (customer_id, total) VALUES (:customer_id:int64, :total:int64) RETURNING id:int64

  - name: InsertOrderLine
    kind: exec
    sql: INSERT INTO order_lines (order_id, sku, qty) VALUES (:order_id:Arg[int64], :sku:string, :qty:int32)

  - name: InsertOrderPlain       # MySQL / MariaDB / TiDB / SQLite: an AUTO_INCREMENT id instead of RETURNING
    kind: exec
    sql: INSERT INTO orders (customer_id, total) VALUES (:customer_id:int64, :total:int64)
```

```go
err := q.PipelineTx(ctx, nil, func(p *store.Pipeline) error {
    order := p.InsertOrder(cust, total)                  // *runtime.RowFuture[int64]
    p.InsertOrderLine(order.Arg(), "sku-1", 2).AssertRows(1)
    p.InsertOrderLine(order.Arg(), "sku-2", 1).AssertRows(1)
    p.InsertOrderLine(runtime.Val(existingOrderID), "sku-3", 1) // a plain value
    // MySQL family: id := p.InsertOrderPlain(cust, total).LastInsertIDArg()
    return nil
})
```

**What is generated.** Only the statement's **Pipeline** method changes: `func (pl *Pipeline) InsertOrderLine(orderID runtime.Arg[int64], sku string, qty int32) *runtime.ExecFuture`. The `*Queries` method and `<Name>Query` keep the plain type (`q.InsertOrderLine(ctx, orderID int64, ...)`): there is nothing to forward outside a pipeline. With `args: struct` the Pipeline method takes `<Name>PipelineParams`, which is `<Name>Params` with `runtime.Arg[T]` fields for the `Arg` parameters. A `runtime.Arg[T]` is one of:
- `runtime.Val(v)`: a plain value (no allocation beyond the usual boxing);
- `fut.Arg()` on a `*runtime.RowFuture[T]` of a statement with exactly **one output column** (`RETURNING id:int64`, `SELECT COUNT(*) AS n:int64`): that column's value (calling it twice returns the same value; a statement with several columns fails the pipeline with `runtime.ErrInvalidArgument`);
- `fut.LastInsertIDArg()` on a `*runtime.ExecFuture` (MySQL, MariaDB, TiDB, SQLite): the `AUTO_INCREMENT` id the `INSERT` generated, as an `Arg[int64]`. The `$N` dialect has none (use `RETURNING` and `Arg()`); queuing it there fails the pipeline with `runtime.ErrInvalidArgument`.

`arg.Forwarded()` tells the two apart. The producer's type must be the consumer's `T`: the compiler checks it.

**Semantics** (the same on the sequential and on both single-round-trip paths, asserted by a differential test on every engine):

| Situation | Result |
| :--- | :--- |
| The producer returned exactly one row (`Arg`) / affected exactly one row and generated an id (`LastInsertIDArg`) | the consumer runs with that value; a SQL `NULL` forwards as `NULL` (representable when `T` is a pointer or `sql.Null*`) |
| The producer returned 0 rows or more than one; an `INSERT IGNORE` that inserted nothing; a multi-row `INSERT` (`LastInsertIDArg`) | the consumer fails, when (and only when) it would run, with a `*runtime.ArgError` (`errors.Is(err, runtime.ErrArgUnresolved)`): `Statement`, `Op`, `Label` name the consumer as in an `AssertionError`, `Producer`, `ProducerOp`, `ProducerLabel` the producer, `Got` its row count. The pipeline fails and rolls back like a failed assertion; with several unresolved values the first in argument order is reported |
| The consumer sits in an `If` branch that is not taken | silent, as for any skipped operation. To skip instead of failing, branch on the producer: `p.If(order.Found(), func(p *Pipeline) { p.InsertOrderLine(order.Arg(), ...) }, nil)` |
| The producer has an assertion (`AssertFound()`) | the assertion fails first, at the producer; with `AssertFound()` the value is always resolved when the consumer runs |
| The consumer is queued outside the `If` branch its producer is in (or in the sibling branch) | build error before anything is sent (`runtime.ErrInvalidArgument`): the consumer must be in the producer's branch or in one nested in it, so "the producer did not run" cannot happen |
| A value from another pipeline (an earlier `Pipeline`, `Flush`, or a `PipelineTx` attempt captured in a closure) | build error (`runtime.ErrInvalidArgument`); a retried `PipelineTx` closure creates new futures and values |
| A pipeline in a transaction you began (`q.WithTx(tx).Pipeline(ctx)`) | an unresolved value leaves the transaction usable, on every path (the consumer did not run) |
| An empty `VALUES` operation (section 3.4, "Empty `VALUES` operations") as the producer of `LastInsertIDArg` | unresolved (no id); such a pipeline runs on the sequential path |

**Checked when the code is generated.** `Arg[T]` takes a scalar `T` (any type a scalar parameter takes, `[]byte` included; not a slice, tuple, `sqlgen.*` directive or another `Arg`), in a statement that has a Pipeline method (not `kind: subquery`); a parameter is `Arg` at every occurrence or at none. On the `$N` dialect a forwarded value reaches the statement as text and is cast to the parameter's type (`int64` to `bigint`, `time.Time` to `timestamptz`, `[]byte` to `bytea`, `bool`, `float64`, ... as in "Typed placeholders" above). A `T` that is `string`, or has no such mapping (`sql.NullInt64`, a custom type), **must be followed by an explicit cast** in the SQL, at every occurrence: `:sku:Arg[string]::text` for a text column, `::uuid`, `::numeric`, `::jsonb`, `::interval`, ... (a `text` value is not assignable to those columns, and the server would only say so on the single-round-trip path).

**How it runs.**
- Sequential path: the producer runs first and its value is bound to the consumer's statement (the same SQL text and prepared statement as a call with a plain value). A `kind: one` producer whose value is forwarded reads every row it returns, so that "more than one row" is detected as on the wire paths.
- `pgxwire` (PostgreSQL, CockroachDB, YugabyteDB): the producer's statement records `'v' || value::text` (or a marker for "not exactly one row") with the transaction-local `set_config('sqlgen.v<ID>', ..., true)`, in the same CTE shape as the assertion guards; the consumer is preceded, in the same batch, by a one-row check statement that fails with a marked error when its branch gates are open and a value is unresolved (translated to the `*runtime.ArgError`), and its slot reads `current_setting('sqlgen.v<ID>')` cast as above instead of a `$N` placeholder. Inside your transaction the check runs after the operation's savepoint, so a failure leaves the transaction usable (one extra round trip, error path only).
- `mysqlwire` (MySQL, MariaDB, TiDB): a `SELECT` producer assigns `@sqlgen_v<ID>` while its rows are returned (`SELECT _sqlgen_op.*, (@sqlgen_v<ID>:=_sqlgen_op.col) FROM (<select>) AS _sqlgen_op`; the extra column is hidden), a MariaDB `INSERT ... RETURNING` producer adds the assignment to its `RETURNING` list, a `LastInsertIDArg` producer is preceded by `DO LAST_INSERT_ID(0)` and followed by `SET @sqlgen_v<ID>=LAST_INSERT_ID()`; the consumer is preceded by `SET @sqlgen_f=IF(... NOT <resolved> ...)` (a failure gate like an assertion's, nothing of the consumer runs) and reads the variable where the literal would be.

**Caveats** (each pinned by a test):
- MySQL deprecates assigning a user variable inside an expression (warning 1287, still accepted by 8.4 and 9.7; MariaDB does not warn). The connector's capability probe checks it, and `DO LAST_INSERT_ID(0)`, once per connector: a server that refuses either keeps the pipelines that forward values on the sequential path (`SequentialPipelines`); every other pipeline is unaffected.
- `LastInsertIDArg` forwards an id the `INSERT` **generated**. When the `INSERT` sets the `AUTO_INCREMENT` column itself, the sequential path forwards that explicit value (go-sql-driver reports it as the last insert id) while the single-round-trip path reports `ErrArgUnresolved` (the server's `LAST_INSERT_ID()` does not change): you know the value then, pass it with `runtime.Val`.
- MariaDB ignores the `ORDER BY` of a derived table without `LIMIT`: when a `SELECT` producer returns several rows (a failure for its consumers), which one its own `Get()` reports can differ between the paths. The forwarded value, which needs exactly one row, cannot.
- PostgreSQL family: the forwarded text is cast back in the same session, so `DateStyle`, `IntervalStyle` and `TimeZone` round-trip; with `extra_float_digits` lowered below its default (some poolers do) a `float8` loses precision on the single-round-trip path, as pgxwire's text-format results already do (section "Argument types"). CockroachDB: a `timestamp` / `timestamptz` of `infinity` / `-infinity` fails to forward under a non-UTC `TimeZone` (a bounds error, not a wrong value).
- MySQL family: the value travels in a session variable, which keeps the column's collation with implicit coercibility (a literal is coercible), so a comparison with a column of another collation of the same character set can fail with error 1267 where the literal would not; a `TIMESTAMP` in the hour a DST `time_zone` repeats loses which of the two instants it was (the sequential path loses it too when the driver's `loc` is that zone).
- Not in this version: forwarding one column of a multi-column row (a `ColArg` taking the column and a getter, like `WhereCol`) and an exec's affected-row count (`RowsAffectedArg`); select a single column, or use a condition (`If`) on the row count.

---

### 3.5. Cache Tuning & Observability (`StmtCache`)

`sqlgen` includes a 16-shard, Two-Tier W-TinyLFU + Segmented LRU (`StmtCache`) that caches rendered SQL strings in Tier 1 (bounded by both variant count and byte budget) and only promotes recurring heavy-hitter shapes to Tier-2 database prepared statements (`*sql.Stmt`).

| Option | Default | Description |
| :--- | :--- | :--- |
| `runtime.WithMaxPreparedStmts(n)` | `256` (`16`/shard) | Maximum Tier-2 `*sql.Stmt` handles across all 16 shards (`n <= 0` is ignored; to switch prepared statements off use `q.WithPrepared(false)`, the one way to do so). |
| `runtime.WithMaxSQLVariants(n)` | `4096` (`256`/shard) | Maximum Tier-1 rendered SQL strings cached across all 16 shards (`0` disables Tier 1+2). |
| `runtime.WithMaxBatchParams(params)` | `16384` | Maximum parameters per batch chunk. It is always clamped to the wire limit of the placeholder style: `65535` for `$N` (PostgreSQL, CockroachDB) and `32766` for `?` (SQLite, MySQL, MariaDB: the SQLite-safe value, below MySQL's own 65535, because the runtime only knows the placeholder style). With the default the limit never binds. |

The Tier-1 byte budget (16 MiB, 1 MiB per shard) and the promotion threshold (a Tier-1 variant, or a recurring batch size, is promoted to a Tier-2 `*sql.Stmt` once its frequency sketch reaches `2`) are fixed. `q.WithPrepared(false)` returns a view of `q` that never prepares (statements always go through `db.QueryContext` / `ExecContext`); `WithPrepared(true)` turns it back on.

Every `New` creates its own cache. The views of a `*Queries` (`q.WithTx(tx)`, `q.WithPrepared(...)`, the `*Queries` an `InTx` callback receives) share the cache of `q`, so make one `*Queries` per `*sql.DB` and derive the views from it.

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
- `stats.WirePipelines`: Pipeline flushes (one per `Pipeline.Flush`, one per `PipelineTx` attempt) that ran in a single network round-trip through a `wire.Pipeliner` / `wire.TxPipeliner` connection (see section 3.4).
- `stats.SequentialPipelines`: Pipeline flushes that executed one statement at a time (the portable fallback). A flush that fails while being built, or that queued nothing, counts as neither.

---

### 3.6. Reserved Identifiers

Every generated file declares the same fixed set of exported top-level names in your package, so no statement, `result_type` or tuple struct may use them. Its other top-level names are unexported (`sqlDialect`, `desc<Name>`, `params<Name>`, ...), which is why statement names, `result_type`s and tuple struct names must be exported identifiers (an unexported one is a generate-time error).

**Package-level names declared in every generated file** (`reservedTypeNames` in `generator.go`):

| Kind | Names |
| :--- | :--- |
| Types | `Queries`, `Pipeline` |
| Functions | `New` |

The generated file never re-declares runtime symbols: everything user-facing from the runtime (`runtime.Query`, `runtime.Order`, `runtime.Seek`, `runtime.WithTxRetry`, `runtime.DefaultRetryPolicy`, `runtime.PipelineCond`, `runtime.ExecFuture`, `runtime.IfResult`, ...) is spelled `runtime.X` in application code, which imports `github.com/CAFxX/sqlgen/runtime` itself. For example `q.InTx(ctx, nil, fn, runtime.WithTxRetry(runtime.DefaultRetryPolicy()))` and `After: runtime.Seek{}` (the first page).

**Method names generated on `*Queries` and `*Pipeline`** that a statement name must avoid (`reservedStatementNames`): `CacheStats`, `Close`, `If`, `InTx`, `Pipeline`, `PipelineTx`, `WithPrepared`, `WithTx`, plus `SetBuildError`, `QueueExec`, `QueueExecNoop`, `Abort` and `Flush`, which the generated `Pipeline` inherits from `runtime.Pipeline`. A statement becomes a method with its own name on both `*Queries` and `*Pipeline`, so it must not shadow any of them.

**What happens on a clash.** Generation fails with a clear error instead of emitting code that does not compile:

- a statement named like one of the methods above: `statement name "Close" collides with a method generated on *Queries / *Pipeline; rename it`;
- a `result_type` (or a tuple struct name such as `[]Queries(...)`) equal to a package-level name above: `statement Q: result_type "Queries" collides with a type or function declared in every generated file; rename it`.

Rename the statement or `result_type` (for example `Queries` to `QueryRecord`).
