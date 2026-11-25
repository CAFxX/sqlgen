# sqlgen

[![ci](https://github.com/CAFxX/sqlgen/actions/workflows/ci.yml/badge.svg)](https://github.com/CAFxX/sqlgen/actions/workflows/ci.yml)

`sqlgen` compiles a `sqlgen.yaml` file of annotated SQL into strongly typed, idiomatic Go code on top of
`database/sql`, backed by a small zero-reflection runtime.

```yaml
package: store
out: store_gen.go
dialect: postgres          # required: postgres | cockroachdb | mysql | mariadb | sqlite
statements:
  - name: GetItem
    kind: one
    result_type: Item
    sql: |
      SELECT id:int64, name:string, score:int64 FROM items WHERE id = :id:int64

  - name: SearchItems
    kind: many
    result_type: Item
    sql: |
      SELECT id:int64, name:string, score:int64 FROM items
      WHERE score >= :min_score:int64
        {?FilterOrg: AND org_id = :org_id:int64}
        {?FilterStatuses: AND status IN (:statuses:[]string)}
      ORDER BY id

  - name: CountItems            # one output column, no result_type: returns the int64 itself
    kind: one
    sql: SELECT COUNT(*) AS n:int64 FROM items

  - name: InsertItems           # multi-row insert: any size, chunked to fit the wire limit
    kind: exec
    sql: |
      INSERT INTO items (id, name, score)
      VALUES :items:[]NewItem(ID:int64 ASC, Name:string, Score:int64)
```

```go
//go:generate go run github.com/CAFxX/sqlgen/cmd/sqlgen -config sqlgen.yaml
q := store.New(db)
item, err := q.GetItem(ctx, 42)
rows, err := q.SearchItems(ctx, store.SearchItemsParams{MinScore: 10, FilterOrg: true, OrgID: 7})
for row, err := range q.SearchItemsSeq(ctx, params) { /* stream rows in O(1) memory */ }
n, err := q.CountItems(ctx)                 // (int64, error)
res, err := q.InsertItems(ctx, newItems)    // chunked automatically; an empty slice is a no-op
```

A `many` statement has two forms, `X` (a slice) and `XSeq` (an iterator); `runtime.Collect`, `runtime.AppendTo`
and `runtime.Each` consume any iterator as a new slice, appended to yours, or through a callback.

Typed output columns and parameters, slices (`IN (:ids:[]int64)`), multi-row tuple inserts with automatic
wire-limit-aware batching and streaming, optional `{?Flag: ...}` blocks, keyset pagination (`Seek`/`Order`),
composable sub-queries, deadlock-avoiding presorting, automatic transaction retries, and single-roundtrip
`Pipeline`/`PipelineTx` with server-side assertions and `If`/`ElseIf`/`Else` chains: with the `pgxwire` driver on PostgreSQL,
CockroachDB or YugabyteDB, or the `mysqlwire` driver on MySQL, MariaDB or TiDB, a whole transaction (`BEGIN`, every statement,
`COMMIT`) is one network round-trip, measured, with `CacheStats` counters to verify it, also when a later statement uses a
value an earlier one produced (`:order_id:Arg[int64]` fed by `order.Arg()`, the id an `INSERT ... RETURNING` generated). A two-tier adaptive statement cache
(rendered SQL, then prepared statements) keeps hot statements prepared without any caller bookkeeping.

**Documentation:** [DOCS.md](DOCS.md) is the user manual and DSL cookbook; [SPEC.md](SPEC.md) is the
engineering specification.

## Status

Unreleased (pre-1.0). The generated API and the YAML format may still change.

## Verified engines

The CI matrix runs the whole suite against real servers with real drivers (`pgx`, `go-sql-driver/mysql`):
PostgreSQL 15 and 18, CockroachDB, YugabyteDB 2025.2, MySQL 8.4 and 9.7, MariaDB 10.11 and 12.3, TiDB 8.5, plus SQLite
(`modernc.org/sqlite`). Every engine runs the whole suite per push; a nightly workflow adds a larger-scale fuzz soak. See "Engine support" in DOCS.md for what each engine supports. The single-roundtrip pipeline path has one
driver per wire protocol, each a nested module so the main module keeps its two dependencies:
`github.com/CAFxX/sqlgen/pgxwire` (PostgreSQL 15, 18, CockroachDB and YugabyteDB) and `github.com/CAFxX/sqlgen/mysqlwire`
(MySQL 8.4, 9.7, MariaDB 10.11, 12.3 and TiDB; it needs `multiStatements`, see DOCS.md 3.4). Both are verified to take
exactly one network turn per `PipelineTx`, first-seen statements included; every other engine and driver (SQLite,
a plain `*sql.DB`) runs the equivalent sequential path, as does any batch a wire driver declines (for example one
larger than MySQL's `max_allowed_packet`).

## Development

```sh
make check            # gofmt, go vet (every module), fixtures and generated code up to date
make gen              # regenerate the engine fixtures (integration/fixtures templates), then every generated package
make test             # hermetic tests (SQLite + mock driver), race detector
make engines-up       # start every real engine in containers (docker compose); ENGINES=postgres18,mysql84 to select
make test-engines     # integration suite + both wire drivers against all of them (same ENGINES selection)
make test-integration ENGINES=postgres18   # only the integration module (also what each CI engine job runs)
make test-integration ENGINES=yugabyte SMOKE=1   # a quick smoke subset (SMOKE_RUN in the Makefile; RUN=<regex> for any other)
make test-pgxwire ENGINES=postgres18       # the pgxwire driver's own tests (PostgreSQL / CockroachDB / YugabyteDB)
make test-mysqlwire ENGINES=mysql84        # the mysqlwire driver's own tests (MySQL / MariaDB / TiDB)
make bench-engines ENGINES=postgres18      # generated call vs hand-written database/sql, PipelineTx wire vs sequential
make engines-down
```

Engines are named once everywhere (compose service, `ENGINES`/`SQLGEN_ENGINES`, CI matrix, `SQLGEN_<NAME>_DSN` / `_PORT`
variables): `postgres15 postgres18 cockroach yugabyte mysql84 mysql97 mariadb1011 mariadb123 tidb`, plus the in-process
`sqlite`. YugabyteDB needs about 4 GB of memory (the compose file caps it there).
The per-engine fixtures (`integration/{pgq,myq,mdbq,fzpg,fzmy,fzsl}/sqlgen.yaml` and their Go adapters) are rendered from
one template each by `make gen`; never edit them by hand. `SQLGEN_FUZZ_SCALE=<n>` grows every fuzz suite and
`SQLGEN_FUZZ_SEED=<n>` replays one failing seed.

The real-engine suite lives in the nested module `integration/` and the pipeline drivers in `pgxwire/` (pgx) and
`mysqlwire/` (go-sql-driver/mysql), so the main module depends only on `yaml` and (for its tests) `sqlite`.

## License

[MIT](LICENSE).
