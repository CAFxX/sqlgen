# sqlgen

[![ci](https://github.com/CAFxX/sqlgen/actions/workflows/ci.yml/badge.svg)](https://github.com/CAFxX/sqlgen/actions/workflows/ci.yml)

`sqlgen` compiles a `sqlgen.yaml` file of annotated SQL into strongly typed, idiomatic Go code on top of
`database/sql`, backed by a small zero-reflection runtime.

```yaml
version: "1"
package: store
out: store_gen.go
dialect: postgres          # postgres | mysql | sqlite (aliases accepted, see DOCS.md)
statements:
  - name: GetItem
    kind: one
    result_type: Item
    sql: |
      SELECT id:int64, name:string, score:int64 FROM items WHERE id = :id:int64

  - name: SearchItems
    kind: many
    result_type: Item
    params_struct: true
    sql: |
      SELECT id:int64, name:string, score:int64 FROM items
      WHERE score >= :min_score:int64
        {?FilterOrg: AND org_id = :org_id:int64}
        {?FilterStatuses: AND status IN (:statuses:[]string)}
      ORDER BY id
```

```go
//go:generate go run github.com/CAFxX/sqlgen/cmd/sqlgen -config sqlgen.yaml
q := store.New(db)
item, err := q.GetItem(ctx, 42)
rows, err := q.SearchItems(ctx, store.SearchItemsParams{MinScore: 10, FilterOrg: true, OrgID: 7})
```

Typed output columns and parameters, slices (`IN (:ids:[]int64)`), multi-row tuple inserts with automatic
wire-limit-aware batching and streaming, optional `{?Flag: ...}` blocks, keyset pagination (`Seek`/`Order`),
composable sub-queries, deadlock-avoiding presorting, automatic transaction retries, and single-roundtrip
`Pipeline`/`PipelineTx` with server-side assertions and `If/Then/Else`. A two-tier adaptive statement cache
(rendered SQL, then prepared statements) keeps hot statements prepared without any caller bookkeeping.

**Documentation:** [DOCS.md](DOCS.md) is the user manual and DSL cookbook; [SPEC.md](SPEC.md) is the
engineering specification.

## Status

Unreleased (pre-1.0). The generated API and the YAML format may still change.

## Verified engines

The CI matrix runs the whole suite against real servers with real drivers (`pgx`, `go-sql-driver/mysql`):
PostgreSQL 15 and 18, CockroachDB, MySQL 8.4 and 9.7, MariaDB 10.11 and 12.3, plus SQLite (`modernc.org/sqlite`).
See "Engine support" in DOCS.md for what each engine supports (the single-roundtrip pipeline SQL is
PostgreSQL-protocol only; other engines use the equivalent sequential path).

## Development

```sh
make check            # gofmt, go vet, generated code up to date
make test             # hermetic tests (SQLite + mock driver), race detector
make engines-up       # start every real engine in containers (docker compose)
make test-engines     # integration suite against all of them (ENGINES=postgres18,mysql84 to select)
make bench-engines ENGINES=postgres18   # generated call vs hand-written database/sql
make engines-down
```

The real-engine suite lives in the nested module `integration/`, so the main module depends only on
`yaml` and (for its tests) `sqlite`.

## License

No license has been chosen yet; all rights reserved until one is added.
