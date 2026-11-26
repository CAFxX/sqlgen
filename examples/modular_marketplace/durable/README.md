# Durable checkout worker (DBOS + SQLGen)

This nested module is deliberately separate from the root SQLGen module so the DBOS SDK is not a dependency of the generator or runtime.

The worker registers a DBOS workflow and a tenant-partitioned queue before launch. `Submit` uses a stable workflow ID derived from marketplace and order IDs; duplicate submissions retrieve the existing workflow. The workflow then:

1. Runs the generated cross-package checkout as a DBOS step. The order-line writes and ledger transfer still share one SQLGen `PipelineTx`.
2. Waits with `dbos.Sleep`, which persists the deadline across process restarts.
3. Creates a carrier label in a separate step using a stable provider idempotency key.

## The transaction / checkpoint crash window

The SQLGen transaction and DBOS step checkpoint are separate commits in this example. A process can therefore die after the SQLGen transaction commits but before DBOS records the step result. On recovery, DBOS may execute the step again.

The transfer receipt is the business effect record: it is inserted in the same `PipelineTx` as the order lines and balance updates. `Service.Checkout` checks the receipt before running the pipeline and verifies its transfer payload. The unique receipt key remains the concurrency backstop. Thus a replay after the commit returns success without applying the transfer again.

The carrier call has its own unavoidable external crash window. The carrier adapter must pass the supplied idempotency key to the provider; a generic network request cannot be made exactly once by DBOS or SQLGen alone.

## Run

From this directory:

```sh
go test -race ./...
```

The test uses DBOS's SQLite system database and the same `*sql.DB` for the example's business tables. The SQLGen core module does not import DBOS.
