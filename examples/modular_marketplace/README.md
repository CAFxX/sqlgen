# Modular marketplace: one pipeline across domain packages

This runnable vertical slice composes two independently generated SQLGen modules:

- ecommerce_inventory owns the pipeline, database connection, transaction and statement cache; it writes order lines.
- fintech_ledger appends debit, credit and transfer-receipt statements to that same pipeline.

The checkout flow is intentionally small:

    err := inventory.PipelineTx(ctx, nil, func(p *ecommerce_inventory.Pipeline) error {
        p.InsertOrderItems(items).AssertRows(int64(len(items)))

        ledger := fintech_ledger.WrapPipeline(p.Pipeline)
        ledger.DebitAccount(total, marketplaceID, buyerID).AssertRows(1)
        ledger.CreditAccount(total, marketplaceID, sellerID).AssertRows(1)
        ledger.InsertTransferReceipt(key, marketplaceID, buyerID, sellerID, total).AssertRows(1)
        return nil
    })

WrapPipeline does not create another connection or transaction. The wrapped package retains typed generated methods and futures, while all operations use the original pipeline's database, transaction, cache and execution path. Its package-local prepared-statement fast slots are not used; cross-package operations use rendered-SQL cache keys, avoiding collisions between package-local statement IDs.

The test verifies that a duplicate transfer receipt rolls back the order lines and both account updates. Because this example uses SQLite, it exercises the sequential transactional fallback. The integration suite separately exercises shared pipelines on the PostgreSQL wire path and counts network turns.

This is intentionally a focused integration example, not a complete marketplace. It omits tenant authorization, stock reservation, tax, payment-provider calls and durable workflow orchestration; those belong in a larger application built on the same module boundaries.
