package fintech_ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
	"modernc.org/sqlite"
)

// sqliteWirePipelinerConnector wraps a SQLite database connection so that PipelineTx
// executes through the WirePipeliner code path against a live SQLite database.
type sqliteWirePipelinerConnector struct {
	dsn     string
	drv     driver.Driver
	configs sync.Map // map[string]string for connection-scoped set_config/current_setting
}

type sqliteWireConn struct {
	driver.Conn
	parent *sqliteWirePipelinerConnector
}

var (
	sqliteFuncOnce sync.Once
	sqliteConfigs  sync.Map
)

func ensureSQLiteFunctionsRegistered() {
	sqliteFuncOnce.Do(func() {
		_ = sqlite.RegisterScalarFunction("set_config", 3, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			key, _ := args[0].(string)
			val, _ := args[1].(string)
			sqliteConfigs.Store(key, val)
			return val, nil
		})
		_ = sqlite.RegisterScalarFunction("current_setting", 2, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			key, _ := args[0].(string)
			if v, ok := sqliteConfigs.Load(key); ok {
				return v.(string), nil
			}
			return "", nil
		})
	})
}

func newSQLiteWireDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	ensureSQLiteFunctionsRegistered()
	tmpDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	drv := tmpDB.Driver()
	_ = tmpDB.Close()
	connector := &sqliteWirePipelinerConnector{
		dsn: dsn,
		drv: drv,
	}
	db := sql.OpenDB(runtime.WrapConnector(connector))
	db.SetMaxOpenConns(1)
	return db
}

func (c *sqliteWirePipelinerConnector) Connect(ctx context.Context) (driver.Conn, error) {
	rawConn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &sqliteWireConn{Conn: rawConn, parent: c}, nil
}

func (c *sqliteWirePipelinerConnector) Driver() driver.Driver {
	return c.drv
}

func (w *sqliteWireConn) evalGatesInSQLite(ctx context.Context, gates []runtime.BranchGate) (bool, error) {
	for _, g := range gates {
		key := fmt.Sprintf("sqlgen.c%d", g.CondID)
		v, ok := sqliteConfigs.Load(key)
		valStr, _ := v.(string)
		isTrue := ok && valStr == "true"
		if g.WantTrue != isTrue {
			return false, nil
		}
	}
	return true, nil
}

func (w *sqliteWireConn) ExecPipeline(ctx context.Context, ops []runtime.PipelineOp) error {
	qConn := w.Conn.(driver.QueryerContext)
	eConn := w.Conn.(driver.ExecerContext)

	for i := range ops {
		op := &ops[i]
		gatesActive, err := w.evalGatesInSQLite(ctx, op.Input.BranchGates)
		if err != nil {
			return err
		}
		if !gatesActive {
			op.Skipped = true
			if op.OnSkipped != nil {
				op.OnSkipped()
			}
			continue
		}

		if op.Decorators.IsAbort {
			// Execute the rendered SELECT CASE WHEN ... THEN ABS(-9223372036854775808) SQL in SQLite
			rows, err := qConn.QueryContext(ctx, op.SQL, nil)
			if rows != nil {
				var dest [1]driver.Value
				if nextErr := rows.Next(dest[:]); nextErr != nil && err == nil {
					err = nextErr
				}
				_ = rows.Close()
			}
			if err != nil {
				return runtime.TranslateWireError(op, err)
			}
			return op.AbortErr
		}

		nvs := make([]driver.NamedValue, len(op.Args))
		for j, a := range op.Args {
			nvs[j] = driver.NamedValue{Ordinal: j + 1, Value: a}
		}

		if op.HasResultCols {
			coreSQL := runtime.BuildComposedSQL(op.Desc, &op.Input, runtime.DialectQuestion)
			dRows, err := qConn.QueryContext(ctx, coreSQL, nvs)
			if err != nil {
				op.Err = runtime.TranslateWireError(op, err)
				return op.Err
			}
			sqlRows, err := runtime.RowsFromDriverRows(ctx, dRows)
			if err != nil {
				_ = dRows.Close()
				op.Err = err
				return err
			}
			cnt, scanErr := op.ScanWireRows(sqlRows)
			_ = sqlRows.Close()
			if scanErr != nil {
				op.Err = runtime.TranslateWireError(op, scanErr)
				return op.Err
			}
			op.RowCount = cnt
		} else {
			coreSQL := runtime.BuildComposedSQL(op.Desc, &op.Input, runtime.DialectQuestion)
			res, err := eConn.ExecContext(ctx, coreSQL, nvs)
			if err != nil {
				op.Err = runtime.TranslateWireError(op, err)
				return op.Err
			}
			if res != nil {
				op.RowCount, _ = res.RowsAffected()
			}
		}

		if op.Decorators.AssertRows >= 0 && op.RowCount != op.Decorators.AssertRows {
			// Trigger SQLite integer overflow and map via TranslateWireError
			rows, err := qConn.QueryContext(ctx, "SELECT ABS(-9223372036854775808)", nil)
			if rows != nil {
				var dest [1]driver.Value
				if nextErr := rows.Next(dest[:]); nextErr != nil && err == nil {
					err = nextErr
				}
				_ = rows.Close()
			}
			op.Err = runtime.TranslateWireError(op, err)
			if op.OnDone != nil {
				op.OnDone(op.RowCount, op.Err)
			}
			return op.Err
		}

		if op.OnDone != nil {
			op.OnDone(op.RowCount, nil)
		}

		if op.Decorators.ProduceCond != nil {
			condVal := op.Decorators.ProduceCond.EvalGoFn()
			op.CondVal = condVal
			op.CondSet = true
			key := fmt.Sprintf("sqlgen.c%d", op.Decorators.CondID)
			if condVal {
				sqliteConfigs.Store(key, "true")
			} else {
				sqliteConfigs.Store(key, "false")
			}
		}
		for _, ec := range op.ExtraConds {
			condVal := ec.Cond.EvalGoFn()
			key := fmt.Sprintf("sqlgen.c%d", ec.CondID)
			if condVal {
				sqliteConfigs.Store(key, "true")
			} else {
				sqliteConfigs.Store(key, "false")
			}
		}
	}
	return nil
}

func openSeqSQLiteDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func runFintechLedgerSuite(t *testing.T, mode string, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, SchemaDDL); err != nil {
		t.Fatalf("[%s] schema DDL: %v", mode, err)
	}

	svc := NewLedgerService(db, runtime.WithPromoteThreshold(2))
	defer func() { _ = svc.Close() }()
	q := svc.Queries()

	const (
		orgID          int64 = 100
		numAccounts    int   = 20
		initialBalance int64 = 10_000_00 // $10,000.00 in cents
		expectedSupply int64 = int64(numAccounts) * initialBalance
	)

	// 1. Seed 20 accounts in reverse order to verify non-mutating presort in UpsertAccountBalances
	seedItems := make([]AccountSettlement, numAccounts)
	for i := 0; i < numAccounts; i++ {
		seedItems[i] = AccountSettlement{
			OrgID:      orgID,
			AccountID:  int64(numAccounts - i),
			Currency:   "USD",
			DeltaCents: initialBalance,
		}
	}
	affected, err := svc.BulkSettle(ctx, seedItems)
	if err != nil {
		t.Fatalf("[%s] initial BulkSettle: %v", mode, err)
	}
	if affected != int64(numAccounts) {
		t.Fatalf("[%s] expected %d rows affected, got %d", mode, numAccounts, affected)
	}
	// Verify caller slice was NOT mutated by presorting
	if seedItems[0].AccountID != int64(numAccounts) {
		t.Fatalf("[%s] caller slice was mutated by BulkSettle", mode)
	}

	// Verify initial total supply == $200,000.00
	supply, err := q.TotalSupplyByOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("[%s] TotalSupplyByOrg: %v", mode, err)
	}
	if supply != expectedSupply {
		t.Fatalf("[%s] initial supply = %d, want %d", mode, supply, expectedSupply)
	}

	// 2. Test validation errors, insufficient funds branch, and frozen account rollback
	if _, err := svc.Transfer(ctx, orgID, 1, 2, 0, "bad-amt"); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("[%s] expected ErrInvalidAmount, got %v", mode, err)
	}
	if _, err := svc.Transfer(ctx, orgID, 1, 1, 100, "same-acct"); !errors.Is(err, ErrSameAccount) {
		t.Fatalf("[%s] expected ErrSameAccount, got %v", mode, err)
	}

	// Insufficient funds attempt ($50,000 > $10,000): should commit audit log without moving funds
	out, err := svc.Transfer(ctx, orgID, 1, 2, 50_000_00, "overdraft-1")
	if err != nil {
		t.Fatalf("[%s] overdraft transfer returned error: %v", mode, err)
	}
	if out.Transferred || !out.InsufficientFunds {
		t.Fatalf("[%s] expected InsufficientFunds=true, got %+v", mode, out)
	}

	// Freeze account 20, attempt transfer -> must abort with ErrAccountFrozen and roll back
	if _, err := q.SetAccountStatus(ctx, "frozen", orgID, 20); err != nil {
		t.Fatalf("[%s] SetAccountStatus frozen: %v", mode, err)
	}
	if _, err := svc.Transfer(ctx, orgID, 20, 1, 500_00, "frozen-sender"); !errors.Is(err, ErrAccountFrozen) {
		t.Fatalf("[%s] expected ErrAccountFrozen for frozen sender, got %v", mode, err)
	}
	if _, err := svc.Transfer(ctx, orgID, 1, 20, 500_00, "frozen-receiver"); !errors.Is(err, ErrAccountFrozen) {
		t.Fatalf("[%s] expected ErrAccountFrozen for frozen receiver, got %v", mode, err)
	}
	// Unfreeze account 20 before concurrent stress test
	if _, err := q.SetAccountStatus(ctx, "active", orgID, 20); err != nil {
		t.Fatalf("[%s] SetAccountStatus active: %v", mode, err)
	}

	// 3. Spawn 16 goroutines concurrently executing bidirectional transfers (A -> B and B -> A)
	// and zero-sum bulk settlements
	const (
		numWorkers = 16
		opsPerG    = 25
	)
	var wg sync.WaitGroup
	var keySeq atomic.Uint64
	errCh := make(chan error, numWorkers*opsPerG)

	for g := 0; g < numWorkers; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(workerID+1), 0x9e3779b97f4a7c15))
			for iter := 0; iter < opsPerG; iter++ {
				a := int64(rng.IntN(numAccounts) + 1)
				b := int64(rng.IntN(numAccounts) + 1)
				if a == b {
					b = (a % int64(numAccounts)) + 1
				}
				if iter%7 == 0 {
					// Zero-sum bulk settlement across 4 accounts in unsorted order
					c := (b % int64(numAccounts)) + 1
					d := (c % int64(numAccounts)) + 1
					if a != b && b != c && c != d && a != c && a != d && b != d {
						batch := []AccountSettlement{
							{OrgID: orgID, AccountID: d, Currency: "USD", DeltaCents: 50},
							{OrgID: orgID, AccountID: a, Currency: "USD", DeltaCents: -50},
							{OrgID: orgID, AccountID: c, Currency: "USD", DeltaCents: -25},
							{OrgID: orgID, AccountID: b, Currency: "USD", DeltaCents: 25},
						}
						if _, err := svc.BulkSettle(ctx, batch); err != nil {
							errCh <- fmt.Errorf("worker %d BulkSettle: %w", workerID, err)
							return
						}
						continue
					}
				}

				amt := int64(rng.IntN(3_000_00) + 100)
				if iter%11 == 0 {
					// Occasional overdraft attempt
					amt = 500_000_00
				}
				idemKey := fmt.Sprintf("%s-w%d-i%d-%d", mode, workerID, iter, keySeq.Add(1))
				if _, err := svc.Transfer(ctx, orgID, a, b, amt, idemKey); err != nil {
					errCh <- fmt.Errorf("worker %d Transfer(%d->%d): %w", workerID, a, b, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("[%s] concurrent error: %v", mode, err)
	}

	// 4. Assert strict conservation of money: sum(balances) == $200,000.00
	finalSupply, err := q.TotalSupplyByOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("[%s] final TotalSupplyByOrg: %v", mode, err)
	}
	if finalSupply != expectedSupply {
		t.Fatalf("[%s] CONSERVATION VIOLATION: final supply = %d, want %d", mode, finalSupply, expectedSupply)
	}

	// Verify individual accounts sum to expectedSupply and exercise remaining generated methods
	var sumIndividual int64
	for id := int64(1); id <= int64(numAccounts); id++ {
		acct, err := q.GetAccount(ctx, orgID, id)
		if err != nil {
			t.Fatalf("[%s] GetAccount(%d): %v", mode, id, err)
		}
		sumIndividual += acct.BalanceCents
	}
	if sumIndividual != expectedSupply {
		t.Fatalf("[%s] individual account sum = %d, want %d", mode, sumIndividual, expectedSupply)
	}

	// Exercise Stream, StreamSeq, Query currying, WithPrepared, WithTx, InTxOpts, CacheStats
	// First, test empty slice error paths and not-found GetAccount error path
	if res, err := q.UpsertAccountBalances(ctx, nil); err != nil {
		t.Fatalf("[%s] UpsertAccountBalances(nil): %v", mode, err)
	} else if n, _ := res.RowsAffected(); n != 0 {
		t.Fatalf("[%s] UpsertAccountBalances(nil) affected %d rows", mode, n)
	}
	if _, err := q.GetAccount(ctx, orgID, 999999); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("[%s] expected sql.ErrNoRows from GetAccount(999999), got %v", mode, err)
	}
	// Test already-sorted and multi-org UpsertAccountBalances & multi-chunk UpsertAccountBalances
	if _, err := q.UpsertAccountBalances(ctx, []AccountSettlement{
		{OrgID: orgID + 1, AccountID: 2, Currency: "USD", DeltaCents: 0},
		{OrgID: orgID, AccountID: 2, Currency: "USD", DeltaCents: 10},
		{OrgID: orgID, AccountID: 1, Currency: "USD", DeltaCents: -10},
		{OrgID: orgID - 1, AccountID: 1, Currency: "USD", DeltaCents: 0},
	}); err != nil {
		t.Fatalf("[%s] unsorted UpsertAccountBalances: %v", mode, err)
	}
	if _, err := q.UpsertAccountBalances(ctx, []AccountSettlement{
		{OrgID: orgID, AccountID: 1, Currency: "USD", DeltaCents: 10},
		{OrgID: orgID, AccountID: 2, Currency: "USD", DeltaCents: -10},
	}); err != nil {
		t.Fatalf("[%s] sorted UpsertAccountBalances: %v", mode, err)
	}
	// Promote 2-row batch shape (threshold=2) then execute a 3-row sorted batch to exercise multi-chunk autoTx decomposition
	for range 2 {
		if _, err := q.UpsertAccountBalances(ctx, []AccountSettlement{
			{OrgID: orgID, AccountID: 1, Currency: "USD", DeltaCents: 0},
			{OrgID: orgID, AccountID: 2, Currency: "USD", DeltaCents: 0},
		}); err != nil {
			t.Fatalf("[%s] 2-row UpsertAccountBalances: %v", mode, err)
		}
	}
	// More rows than fit one statement (one row per statement): chunked in an implicit transaction.
	chunked := New(q.db, runtime.WithMaxBatchParams(1))
	defer func() { _ = chunked.Close() }()
	if _, err := chunked.UpsertAccountBalances(ctx, []AccountSettlement{
		{OrgID: orgID, AccountID: 1, Currency: "USD", DeltaCents: 0},
		{OrgID: orgID, AccountID: 2, Currency: "USD", DeltaCents: 0},
		{OrgID: orgID, AccountID: 3, Currency: "USD", DeltaCents: 0},
	}); err != nil {
		t.Fatalf("[%s] 3-row sorted multi-chunk UpsertAccountBalances: %v", mode, err)
	}
	if _, err := chunked.UpsertAccountBalances(ctx, []AccountSettlement{
		{OrgID: orgID, AccountID: 3, Currency: "USD", DeltaCents: 0},
		{OrgID: orgID, AccountID: 2, Currency: "USD", DeltaCents: 0},
		{OrgID: orgID, AccountID: 1, Currency: "USD", DeltaCents: 0},
	}); err != nil {
		t.Fatalf("[%s] 3-row unsorted multi-chunk UpsertAccountBalances: %v", mode, err)
	}

	zeroSumStream := runtime.FromSlice([]AccountSettlement{
		{OrgID: orgID, AccountID: 1, Currency: "USD", DeltaCents: 10},
		{OrgID: orgID, AccountID: 2, Currency: "USD", DeltaCents: -10},
	})
	if _, err := q.UpsertAccountBalancesStream(ctx, zeroSumStream); err != nil {
		t.Fatalf("[%s] UpsertAccountBalancesStream: %v", mode, err)
	}
	if _, err := q.UpsertAccountBalancesStream(ctx, runtime.FromSlice([]AccountSettlement(nil))); err != nil {
		t.Fatalf("[%s] empty UpsertAccountBalancesStream: %v", mode, err)
	}
	bigZeroSum := make([]AccountSettlement, 260)
	for i := range bigZeroSum {
		delta := int64(1)
		if i%2 == 1 {
			delta = -1
		}
		bigZeroSum[i] = AccountSettlement{OrgID: orgID, AccountID: int64((i % 2) + 1), Currency: "USD", DeltaCents: delta}
	}
	if _, err := q.UpsertAccountBalancesStream(ctx, runtime.FromSlice(bigZeroSum)); err != nil {
		t.Fatalf("[%s] 260-item UpsertAccountBalancesStream: %v", mode, err)
	}

	// Exercise direct exec & curried Query constructors
	if _, err := q.DebitAccount(ctx, 10, orgID, 1); err != nil {
		t.Fatalf("[%s] DebitAccount: %v", mode, err)
	}
	if _, err := q.CreditAccount(ctx, 10, orgID, 1); err != nil {
		t.Fatalf("[%s] CreditAccount: %v", mode, err)
	}
	if _, err := q.InsertTransferReceipt(ctx, "manual-rcpt-"+mode, orgID, 1, 2, 0); err != nil {
		t.Fatalf("[%s] InsertTransferReceipt: %v", mode, err)
	}
	if _, err := q.InsertAuditEvent(ctx, orgID, 1, "manual_check", 0); err != nil {
		t.Fatalf("[%s] InsertAuditEvent: %v", mode, err)
	}
	var dummyArgs []any
	q1 := q.GetAccountQuery(orgID, 1)
	_ = q1.Expand(&dummyArgs, runtime.DialectQuestion, &q1)
	q2 := q.TotalSupplyByOrgQuery(orgID)
	_ = q2.Expand(&dummyArgs, runtime.DialectQuestion, &q2)
	_ = NewWithCache(db, nil)
	_ = q.Cache()
	_ = q.CacheStats()
	unprepQ := q.WithPrepared(false)
	if _, err := unprepQ.TotalSupplyByOrg(ctx, orgID); err != nil {
		t.Fatalf("[%s] unprepQ.TotalSupplyByOrg: %v", mode, err)
	}
	if err := q.InTx(ctx, nil, func(txQ *Queries) error {
		p := txQ.Pipeline(ctx)
		_ = p.GetAccount(orgID, 1)
		_ = p.SetAccountStatus("active", orgID, 1)
		_ = p.TotalSupplyByOrg(orgID)
		_ = p.UpsertAccountBalances([]AccountSettlement{
			{OrgID: orgID, AccountID: 1, Currency: "USD", DeltaCents: 0},
		})
		return p.Flush()
	}); err != nil {
		t.Fatalf("[%s] InTxOpts pipeline: %v", mode, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	txView := q.WithTx(tx)
	_ = txView.Close()
	_ = tx.Rollback()
}

func TestFintechLedgerEndToEnd(t *testing.T) {
	t.Run("SequentialTx", func(t *testing.T) {
		db := openSeqSQLiteDB(t, ":memory:")
		defer func() { _ = db.Close() }()
		runFintechLedgerSuite(t, "seq", db)
	})

	t.Run("WirePipeliner", func(t *testing.T) {
		db := newSQLiteWireDB(t, ":memory:")
		defer func() { _ = db.Close() }()
		runFintechLedgerSuite(t, "wire", db)
	})
}
