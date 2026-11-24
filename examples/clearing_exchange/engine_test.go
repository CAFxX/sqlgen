package clearing_exchange

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
	"modernc.org/sqlite"
)

type exchangeWireConnector struct {
	dsn string
	drv driver.Driver
}

type exchangeWireConn struct {
	driver.Conn
}

var (
	exSQLiteOnce    sync.Once
	exSQLiteConfigs sync.Map
)

func ensureExchangeSQLiteFuncs() {
	exSQLiteOnce.Do(func() {
		_ = sqlite.RegisterScalarFunction("set_config", 3, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			k, _ := args[0].(string)
			v, _ := args[1].(string)
			exSQLiteConfigs.Store(k, v)
			return v, nil
		})
		_ = sqlite.RegisterScalarFunction("current_setting", 2, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			k, _ := args[0].(string)
			if v, ok := exSQLiteConfigs.Load(k); ok {
				return v.(string), nil
			}
			return "", nil
		})
	})
}

func openExchangeWireDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	ensureExchangeSQLiteFuncs()
	tmp, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	drv := tmp.Driver()
	_ = tmp.Close()
	db := sql.OpenDB(runtime.WrapConnector(&exchangeWireConnector{dsn: dsn, drv: drv}))
	db.SetMaxOpenConns(1)
	return db
}

func (c *exchangeWireConnector) Connect(_ context.Context) (driver.Conn, error) {
	raw, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &exchangeWireConn{Conn: raw}, nil
}

func (c *exchangeWireConnector) Driver() driver.Driver { return c.drv }

func (w *exchangeWireConn) evalGates(gates []runtime.BranchGate) bool {
	for _, g := range gates {
		key := fmt.Sprintf("sqlgen.c%d", g.CondID)
		v, ok := exSQLiteConfigs.Load(key)
		s, _ := v.(string)
		isTrue := ok && s == "true"
		if g.WantTrue != isTrue {
			return false
		}
	}
	return true
}

func (w *exchangeWireConn) ExecPipeline(ctx context.Context, ops []runtime.PipelineOp) error {
	qConn := w.Conn.(driver.QueryerContext)
	eConn := w.Conn.(driver.ExecerContext)

	for i := range ops {
		op := &ops[i]
		if !w.evalGates(op.Input.BranchGates) {
			op.Skipped = true
			if op.OnSkipped != nil {
				op.OnSkipped()
			}
			continue
		}
		if op.Decorators.IsAbort {
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
				exSQLiteConfigs.Store(key, "true")
			} else {
				exSQLiteConfigs.Store(key, "false")
			}
		}
		for _, ec := range op.ExtraConds {
			condVal := ec.Cond.EvalGoFn()
			key := fmt.Sprintf("sqlgen.c%d", ec.CondID)
			if condVal {
				exSQLiteConfigs.Store(key, "true")
			} else {
				exSQLiteConfigs.Store(key, "false")
			}
		}
	}
	return nil
}

func runClearingExchangeStressSuite(t *testing.T, mode string, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, SchemaDDL); err != nil {
		t.Fatalf("[%s] SchemaDDL: %v", mode, err)
	}

	eng := NewClearingEngine(db, runtime.WithPromoteThreshold(2))
	defer func() { _ = eng.Close() }()
	q := eng.Queries()

	// 1. Seed 4 clearing firms & 6 tradeable instruments
	firms := []struct {
		id   int64
		name string
		reg  string
		lev  int64
	}{
		{1, "Apex Quant Clearing", "CFTC-US", 2000},
		{2, "Boreal Prime Desk", "FCA-UK", 1500},
		{3, "Citadel Derivative Hub", "MAS-SG", 2500},
		{4, "Delta Retail Custody", "ESMA-EU", 500},
	}
	for _, f := range firms {
		if _, err := q.UpsertFirm(ctx, f.id, f.name, f.reg, f.lev); err != nil {
			t.Fatalf("[%s] UpsertFirm(%d): %v", mode, f.id, err)
		}
	}

	instruments := []struct {
		sym    string
		class  string
		margin int64
		hcut   int64
		tick   int64
	}{
		{"BTC-PERP", "crypto", 1, 500, 10},
		{"ETH-PERP", "crypto", 1, 600, 5},
		{"SOL-PERP", "crypto", 1, 800, 1},
		{"NVDA-EQ", "equity", 1, 1000, 1},
		{"UST-10Y", "rates", 1, 150, 1},
		{"MEME-SPOT", "crypto", 0, 5000, 1},
	}
	for _, inst := range instruments {
		if _, err := q.UpsertInstrument(ctx, inst.sym, inst.class, inst.margin, inst.hcut, inst.tick); err != nil {
			t.Fatalf("[%s] UpsertInstrument(%s): %v", mode, inst.sym, err)
		}
	}

	// 2. Seed 24 margin accounts (6 per firm) in reverse order to verify non-mutating multi-column presorting
	const (
		numAccounts       = 24
		initialCollateral = int64(100_000_00) // $100,000.00 per account
		expectedEquity    = int64(numAccounts) * initialCollateral
	)
	seedSettlements := make([]MarginSettlement, numAccounts)
	for i := 0; i < numAccounts; i++ {
		acctID := int64(numAccounts - i)
		firmID := ((acctID - 1) % 4) + 1
		kyc := int64(1)
		marginBps := int64(2000)
		if acctID%6 == 0 {
			kyc = 0
			marginBps = 1000
		}
		seedSettlements[i] = MarginSettlement{
			FirmID:           firmID,
			AccountID:        acctID,
			Status:           "active",
			CollateralCents:  initialCollateral,
			RealizedPnlCents: 0,
			MarginRatioBps:   marginBps,
			KycVerified:      kyc,
			UpdatedSeq:       1,
		}
	}
	snapshotSeed := slices.Clone(seedSettlements)
	affected, err := eng.BatchSettleMargins(ctx, seedSettlements)
	if err != nil || affected != numAccounts {
		t.Fatalf("[%s] BatchSettleMargins: affected=%d err=%v", mode, affected, err)
	}
	if !slices.Equal(seedSettlements, snapshotSeed) {
		t.Fatalf("[%s] caller slice was mutated by ApplyMarginSettlements!", mode)
	}

	eq0, err := q.TotalExchangeEquityCents(ctx)
	if err != nil || eq0 != expectedEquity {
		t.Fatalf("[%s] initial TotalExchangeEquityCents = %d, want %d (err=%v)", mode, eq0, expectedEquity, err)
	}

	// 3. Ingest orders via IngestOrders and IngestOrdersStream (testing mixed ASC/DESC presorting + EpochSeq + RETURNING)
	rawOrders := []OrderSubmission{
		{FirmID: 2, AccountID: 2, Symbol: "BTC-PERP", Side: "sell", LimitPriceCents: 49_500_00, Quantity: 5, Status: "open", ClientOrderID: "sell-btc-1", UpdatedSeq: 2},
		{FirmID: 1, AccountID: 1, Symbol: "BTC-PERP", Side: "buy", LimitPriceCents: 50_000_00, Quantity: 5, Status: "open", ClientOrderID: "buy-btc-1", UpdatedSeq: 2},
		{FirmID: 1, AccountID: 5, Symbol: "BTC-PERP", Side: "buy", LimitPriceCents: 51_000_00, Quantity: 5, Status: "open", ClientOrderID: "buy-btc-2", UpdatedSeq: 2},
		{FirmID: 4, AccountID: 6, Symbol: "ETH-PERP", Side: "buy", LimitPriceCents: 3_000_00, Quantity: 10, Status: "open", ClientOrderID: "buy-eth-retail", UpdatedSeq: 2},
		{FirmID: 3, AccountID: 3, Symbol: "ETH-PERP", Side: "sell", LimitPriceCents: 2_950_00, Quantity: 10, Status: "open", ClientOrderID: "sell-eth-1", UpdatedSeq: 2},
	}
	rawOrdersSnap := slices.Clone(rawOrders)
	acks, err := q.IngestOrders(ctx, rawOrders, 10)
	if err != nil || len(acks) != len(rawOrders) {
		t.Fatalf("[%s] IngestOrders: len=%d err=%v", mode, len(acks), err)
	}
	if !slices.Equal(rawOrders, rawOrdersSnap) {
		t.Fatalf("[%s] caller slice was mutated by IngestOrders!", mode)
	}
	// Verify DESC price presorting within same (FirmID=1, AccountID=1 vs 5) and upsert conflict resolution
	reAcks, err := q.IngestOrders(ctx, rawOrders, 11)
	if err != nil || len(reAcks) != len(rawOrders) || reAcks[0].UpdatedSeq != 11 {
		t.Fatalf("[%s] IngestOrders upsert conflict resolution failed: %+v err=%v", mode, reAcks, err)
	}

	// 4. Test ExecuteMatchAndClear across all 3 nested PipelineTx branches
	// 4a. Validation error
	if _, err := eng.ExecuteMatchAndClear(ctx, MatchRequest{}); !errors.Is(err, ErrInvalidTrade) {
		t.Fatalf("[%s] expected ErrInvalidTrade, got %v", mode, err)
	}

	// 4b. Halted account -> Level 1 Abort (rolls back entire transaction)
	if _, err := q.SetAccountStatus(ctx, "halted", 12, 1, 1); err != nil {
		t.Fatalf("[%s] SetAccountStatus halted: %v", mode, err)
	}
	_, err = eng.ExecuteMatchAndClear(ctx, MatchRequest{
		FillID:              "halted-attempt-" + mode,
		Symbol:              "BTC-PERP",
		BuyerFirmID:         1,
		BuyerAccountID:      1,
		BuyerPositionID:     101,
		BuyOrderID:          acks[0].OrderID,
		SellerFirmID:        2,
		SellerAccountID:     2,
		SellerPositionID:    102,
		SellOrderID:         acks[1].OrderID,
		PriceCents:          50_000_00,
		Quantity:            2,
		ClearingFeeCents:    25_00,
		RequiredMarginCents: 10_000_00,
		Seq:                 13,
	})
	if !errors.Is(err, ErrAccountNotActive) {
		t.Fatalf("[%s] expected ErrAccountNotActive for halted account, got %v", mode, err)
	}
	if _, err := q.SetAccountStatus(ctx, "active", 14, 1, 1); err != nil {
		t.Fatalf("[%s] SetAccountStatus active: %v", mode, err)
	}

	// 4c. Insufficient margin -> Level 2 Else branch (records rejection compliance notice without moving funds)
	rejectOut, err := eng.ExecuteMatchAndClear(ctx, MatchRequest{
		FillID:              "margin-reject-" + mode,
		Symbol:              "BTC-PERP",
		BuyerFirmID:         1,
		BuyerAccountID:      1,
		BuyerPositionID:     101,
		BuyOrderID:          acks[0].OrderID,
		SellerFirmID:        2,
		SellerAccountID:     2,
		SellerPositionID:    102,
		SellOrderID:         acks[1].OrderID,
		PriceCents:          50_000_00,
		Quantity:            2,
		ClearingFeeCents:    25_00,
		RequiredMarginCents: 999_999_99, // Exceeds $100,000 collateral
		Seq:                 15,
	})
	if err != nil {
		t.Fatalf("[%s] margin reject ExecuteMatchAndClear: %v", mode, err)
	}
	if rejectOut.Cleared || !rejectOut.Rejected || rejectOut.LargeTraderFiled {
		t.Fatalf("[%s] unexpected reject outcome: %+v", mode, rejectOut)
	}

	// 4d. Institutional KYC trade -> Level 1 Else -> Level 2 Then -> Level 3 Then ("institutional_cleared")
	instOut, err := eng.ExecuteMatchAndClear(ctx, MatchRequest{
		FillID:              "inst-fill-" + mode,
		Symbol:              "BTC-PERP",
		BuyerFirmID:         1,
		BuyerAccountID:      1, // kyc_verified=1, margin_ratio_bps=2000
		BuyerPositionID:     1*10 + 1,
		BuyOrderID:          acks[0].OrderID,
		SellerFirmID:        2,
		SellerAccountID:     2,
		SellerPositionID:    2*10 + 1,
		SellOrderID:         acks[4].OrderID,
		PriceCents:          50_000_00,
		Quantity:            2,
		ClearingFeeCents:    50_00,
		RequiredMarginCents: 10_000_00,
		Seq:                 16,
	})
	if err != nil {
		t.Fatalf("[%s] institutional ExecuteMatchAndClear: %v", mode, err)
	}
	if !instOut.Cleared || instOut.Rejected || !instOut.LargeTraderFiled {
		t.Fatalf("[%s] unexpected institutional outcome: %+v", mode, instOut)
	}

	// 4e. Retail trade (Account 6 has kyc_verified=0) -> Level 1 Else -> Level 2 Then -> Level 3 Else ("standard_cleared")
	retailOut, err := eng.ExecuteMatchAndClear(ctx, MatchRequest{
		FillID:              "retail-fill-" + mode,
		Symbol:              "ETH-PERP",
		BuyerFirmID:         2,
		BuyerAccountID:      6, // kyc_verified=0, margin_ratio_bps=1000
		BuyerPositionID:     6*10 + 2,
		BuyOrderID:          acks[3].OrderID,
		SellerFirmID:        3,
		SellerAccountID:     3,
		SellerPositionID:    3*10 + 2,
		SellOrderID:         acks[2].OrderID,
		PriceCents:          3_000_00,
		Quantity:            4,
		ClearingFeeCents:    15_00,
		RequiredMarginCents: 5_000_00,
		Seq:                 17,
	})
	if err != nil {
		t.Fatalf("[%s] retail ExecuteMatchAndClear: %v", mode, err)
	}
	if !retailOut.Cleared || retailOut.Rejected || retailOut.LargeTraderFiled {
		t.Fatalf("[%s] unexpected retail outcome: %+v", mode, retailOut)
	}

	// 5. Concurrent 16-goroutine clearing, settlement, order ingestion & risk query stress storm
	const (
		numWorkers = 16
		opsPerG    = 15
	)
	var wg sync.WaitGroup
	var seqCounter atomic.Int64
	seqCounter.Store(100)
	errCh := make(chan error, numWorkers*opsPerG)

	symbols := []string{"BTC-PERP", "ETH-PERP", "SOL-PERP", "NVDA-EQ"}

	for g := 0; g < numWorkers; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(workerID+101), 0xfeedfacecafebeef))
			for iter := 0; iter < opsPerG; iter++ {
				seq := seqCounter.Add(1)
				acctA := int64(rng.IntN(numAccounts) + 1)
				acctB := int64(rng.IntN(numAccounts) + 1)
				if acctA == acctB {
					acctB = (acctA % numAccounts) + 1
				}
				firmA := ((acctA - 1) % 4) + 1
				firmB := ((acctB - 1) % 4) + 1
				sym := symbols[iter%len(symbols)]

				if iter%4 == 0 {
					// Zero-sum multi-account margin settlement in unsorted order
					batch := []MarginSettlement{
						{FirmID: firmB, AccountID: acctB, Status: "active", CollateralCents: 100, RealizedPnlCents: -25, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: seq},
						{FirmID: firmA, AccountID: acctA, Status: "active", CollateralCents: -100, RealizedPnlCents: 25, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: seq},
					}
					if _, err := eng.BatchSettleMargins(ctx, batch); err != nil {
						errCh <- fmt.Errorf("worker %d BatchSettleMargins: %w", workerID, err)
						return
					}
					continue
				}

				// Ingest a matched buy + sell order pair and clear via ExecuteMatchAndClear
				price := int64(1_000_00 + rng.IntN(500_00))
				qty := int64(1 + rng.IntN(5))
				pairOrders := []OrderSubmission{
					{
						FirmID: firmB, AccountID: acctB, Symbol: sym, Side: "sell",
						LimitPriceCents: price - 10_00, Quantity: qty, Status: "open",
						ClientOrderID: fmt.Sprintf("%s-sell-w%d-i%d-%d", mode, workerID, iter, seq), UpdatedSeq: seq,
					},
					{
						FirmID: firmA, AccountID: acctA, Symbol: sym, Side: "buy",
						LimitPriceCents: price, Quantity: qty, Status: "open",
						ClientOrderID: fmt.Sprintf("%s-buy-w%d-i%d-%d", mode, workerID, iter, seq), UpdatedSeq: seq,
					},
				}
				pairAcks, err := q.IngestOrders(ctx, pairOrders, seq)
				if err != nil || len(pairAcks) != 2 {
					errCh <- fmt.Errorf("worker %d IngestOrders: %w", workerID, err)
					return
				}

				_, err = eng.ExecuteMatchAndClear(ctx, MatchRequest{
					FillID:              fmt.Sprintf("%s-fill-w%d-i%d-%d", mode, workerID, iter, seq),
					Symbol:              sym,
					BuyerFirmID:         firmA,
					BuyerAccountID:      acctA,
					BuyerPositionID:     acctA*10 + int64(iter%len(symbols)) + 1,
					BuyOrderID:          pairAcks[0].OrderID,
					SellerFirmID:        firmB,
					SellerAccountID:     acctB,
					SellerPositionID:    acctB*10 + int64(iter%len(symbols)) + 1,
					SellOrderID:         pairAcks[1].OrderID,
					PriceCents:          price,
					Quantity:            qty,
					ClearingFeeCents:    10,
					RequiredMarginCents: 1_000_00,
					Seq:                 seq,
				})
				if err != nil {
					errCh <- fmt.Errorf("worker %d ExecuteMatchAndClear: %w", workerID, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("[%s] concurrent worker error: %v", mode, err)
	}

	// 6. Verify strict mathematical double-entry conservation invariants across all accounts and symbols!
	finalEquity, err := q.TotalExchangeEquityCents(ctx)
	if err != nil {
		t.Fatalf("[%s] TotalExchangeEquityCents: %v", mode, err)
	}
	if finalEquity != expectedEquity {
		t.Fatalf("[%s] EQUITY CONSERVATION VIOLATION: got %d, want %d", mode, finalEquity, expectedEquity)
	}

	var sumAccountEquity int64
	for acctID := int64(1); acctID <= numAccounts; acctID++ {
		firmID := ((acctID - 1) % 4) + 1
		ma, err := q.GetMarginAccount(ctx, firmID, acctID)
		if err != nil {
			t.Fatalf("[%s] GetMarginAccount(%d, %d): %v", mode, firmID, acctID, err)
		}
		collatOnly, err := q.GetAccountCollateralCents(ctx, firmID, acctID)
		if err != nil || collatOnly != ma.CollateralCents {
			t.Fatalf("[%s] GetAccountCollateralCents mismatch: %d vs %d (err=%v)", mode, collatOnly, ma.CollateralCents, err)
		}
		sumAccountEquity += ma.CollateralCents + ma.RealizedPnlCents
	}
	if sumAccountEquity != expectedEquity {
		t.Fatalf("[%s] individual account equity sum = %d, want %d", mode, sumAccountEquity, expectedEquity)
	}

	for _, sym := range symbols {
		netQty, err := q.TotalNetPositionBySymbol(ctx, sym)
		if err != nil {
			t.Fatalf("[%s] TotalNetPositionBySymbol(%s): %v", mode, sym, err)
		}
		if netQty != 0 {
			t.Fatalf("[%s] NET POSITION CONSERVATION VIOLATION for %s: sum(net_quantity) = %d, want 0", mode, sym, netQty)
		}
	}

	// 7. Verify 3-level nested curried sqlgen.Query composition + coupled {?Flag} + SelectCols + 4-column Order/Seek pagination
	mixedOrder := OrderRiskPositionView(
		RiskPositionViewOrderByNotionalCents.Desc(),
		RiskPositionViewOrderByUnrealizedPnlCents.Asc(),
		RiskPositionViewOrderByUpdatedSeq.Desc(),
		RiskPositionViewOrderByPositionID.Asc(),
	)
	baseFilter := RiskQueryFilter{
		FirmIDs:          []int64{4, 2, 1, 3}, // unsorted input slice to exercise presorting inside EligibleAccountsSubquery
		RequireKYC:       true,
		MinMarginRatio:   true,
		MinMarginBps:     1500,
		AssetClasses:     []string{"equity", "crypto"}, // unsorted input slice to exercise presorting inside ActiveInstrumentsSubquery
		OnlyMarginable:   true,
		MaxHaircut:       true,
		MaxHaircutBps:    2000,
		MinAbsQuantity:   true,
		MinAbsQty:        1,
		IncludeGreeks:    true,
		IncludeFirmInfo:  true,
		MinNotional:      true,
		MinNotionalCents: 100,
		Order:            mixedOrder,
		PageSize:         500,
	}
	groundTruth, err := eng.CollectAllRiskPositions(ctx, baseFilter)
	if err != nil || len(groundTruth) == 0 {
		t.Fatalf("[%s] groundTruth CollectAllRiskPositions: len=%d err=%v", mode, len(groundTruth), err)
	}
	for _, row := range groundTruth {
		if row.FirmName == "" || row.RegulatorCode == "" || row.DeltaBps == 0 {
			t.Fatalf("[%s] expected coupled Greeks and FirmInfo columns populated, got %+v", mode, row)
		}
	}

	for _, pageSize := range []int64{1, 2, 5, 13} {
		f := baseFilter
		f.PageSize = pageSize
		paged, err := eng.CollectAllRiskPositions(ctx, f)
		if err != nil {
			t.Fatalf("[%s] CollectAllRiskPositions(pageSize=%d): %v", mode, pageSize, err)
		}
		if !slices.Equal(paged, groundTruth) {
			t.Fatalf("[%s] pageSize=%d keyset pagination mismatch: got %d rows, want %d", mode, pageSize, len(paged), len(groundTruth))
		}
	}

	// Test SelectCols bitmask subsetting + FilterAccounts + QueryRiskPositionsPageSeq iterator
	subsetMask := QueryRiskPositionsPageColPositionID | QueryRiskPositionsPageColSymbol | QueryRiskPositionsPageColNotionalCents
	acctSub := q.EligibleAccountsSubqueryQuery(EligibleAccountsSubqueryParams{
		FirmIDs:        []int64{1, 2, 3, 4},
		RequiredStatus: "active",
	})
	instSub := q.ActiveInstrumentsSubqueryQuery(ActiveInstrumentsSubqueryParams{
		AssetClasses: []string{"crypto", "equity"},
	})
	posSub := q.FilteredPositionsSubqueryQuery(FilteredPositionsSubqueryParams{
		EligibleAccounts:  acctSub,
		ActiveInstruments: instSub,
	})
	seqParams := QueryRiskPositionsPageParams{
		MatchingPositions: posSub,
		IncludeGreeks:     true,
		FilterAccounts:    true,
		AccountIDs:        []int64{3, 1, 2}, // unsorted to trigger ASC presort
		SelectCols:        subsetMask,
		Order:             OrderRiskPositionView(RiskPositionViewOrderByNotionalCents.Asc()),
		Limit:             10,
	}
	var seqCount int
	for row, err := range q.QueryRiskPositionsPageSeq(ctx, seqParams) {
		if err != nil {
			t.Fatalf("[%s] QueryRiskPositionsPageSeq: %v", mode, err)
		}
		if row.PositionID == 0 || row.Symbol == "" || row.FirmID != 0 || row.AccountID != 0 {
			t.Fatalf("[%s] SelectCols subsetting violated: %+v", mode, row)
		}
		seqCount++
		if seqCount == 2 {
			break // Exercise early break cleanup on Seq iterator
		}
	}

	// 8. Exercise remaining batch/stream/seq/pipeline/curried variants for >=95% coverage
	exerciseRemainingExchangeVariants(t, ctx, mode, db, eng, q)
}

func exerciseRemainingExchangeVariants(t *testing.T, ctx context.Context, mode string, db *sql.DB, eng *ClearingEngine, q *Queries) {
	t.Helper()

	// An empty VALUES collection is a successful no-op (nothing sent, nothing affected); an empty IN list is an error.
	if res, err := q.ApplyMarginSettlements(ctx, nil); err != nil {
		t.Fatalf("[%s] ApplyMarginSettlements(nil): %v", mode, err)
	} else if n, _ := res.RowsAffected(); n != 0 {
		t.Fatalf("[%s] ApplyMarginSettlements(nil) affected %d rows", mode, n)
	}
	if acks, err := q.IngestOrders(ctx, nil, 1); err != nil || len(acks) != 0 {
		t.Fatalf("[%s] IngestOrders(nil) = %v, %v; want no rows and no error", mode, acks, err)
	}
	if _, err := q.CancelOrdersByIDs(ctx, 1, 1, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("[%s] expected ErrEmptySliceParam, got %v", mode, err)
	}
	if _, err := q.GetMarginAccount(ctx, 9999, 9999); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("[%s] expected sql.ErrNoRows from GetMarginAccount, got %v", mode, err)
	}
	if _, err := q.GetAccountCollateralCents(ctx, 9999, 9999); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("[%s] expected sql.ErrNoRows from GetAccountCollateralCents, got %v", mode, err)
	}
	if n, err := eng.BatchSettleMargins(ctx, nil); err != nil || n != 0 {
		t.Fatalf("[%s] BatchSettleMargins(nil) = %d, %v; want 0 and no error", mode, n, err)
	}
	// Empty slice inside nested subqueries and parent QueryRiskPositionsPage
	if _, err := eng.CollectAllRiskPositions(ctx, RiskQueryFilter{FirmIDs: nil, AssetClasses: []string{"crypto"}}); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("[%s] expected ErrEmptySliceParam on empty FirmIDs in EligibleAccountsSubquery, got %v", mode, err)
	}
	if _, err := eng.CollectAllRiskPositions(ctx, RiskQueryFilter{FirmIDs: []int64{1}, AssetClasses: nil}); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("[%s] expected ErrEmptySliceParam on empty AssetClasses in ActiveInstrumentsSubquery, got %v", mode, err)
	}
	if _, err := eng.CollectAllRiskPositions(ctx, RiskQueryFilter{FirmIDs: []int64{1}, AssetClasses: []string{"crypto"}, FilterAccounts: true, AccountIDs: nil}); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("[%s] expected ErrEmptySliceParam on empty AccountIDs in QueryRiskPositionsPage, got %v", mode, err)
	}

	// Direct ApplyMarginSettlements (sorted and unsorted across firms & accounts)
	if _, err := q.ApplyMarginSettlements(ctx, []MarginSettlement{
		{FirmID: 1, AccountID: 1, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 498},
		{FirmID: 2, AccountID: 2, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 498},
	}); err != nil {
		t.Fatalf("[%s] sorted ApplyMarginSettlements: %v", mode, err)
	}
	if _, err := q.ApplyMarginSettlements(ctx, []MarginSettlement{
		{FirmID: 3, AccountID: 3, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 499},
		{FirmID: 1, AccountID: 5, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 499},
		{FirmID: 1, AccountID: 1, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 499},
	}); err != nil {
		t.Fatalf("[%s] unsorted ApplyMarginSettlements: %v", mode, err)
	}

	// Multi-chunk ApplyMarginSettlements (sorted & unsorted) and >256-row StreamSettleMargins
	for range 2 {
		if _, err := q.ApplyMarginSettlements(ctx, []MarginSettlement{
			{FirmID: 1, AccountID: 1, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 500},
			{FirmID: 1, AccountID: 5, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 500},
		}); err != nil {
			t.Fatalf("[%s] 2-row ApplyMarginSettlements: %v", mode, err)
		}
	}
	if _, err := q.ApplyMarginSettlements(ctx, []MarginSettlement{
		{FirmID: 1, AccountID: 1, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 501},
		{FirmID: 1, AccountID: 5, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 501},
		{FirmID: 2, AccountID: 2, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 501},
	}); err != nil {
		t.Fatalf("[%s] 3-row sorted ApplyMarginSettlements: %v", mode, err)
	}
	if _, err := q.ApplyMarginSettlements(ctx, []MarginSettlement{
		{FirmID: 2, AccountID: 2, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 502},
		{FirmID: 1, AccountID: 5, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 502},
		{FirmID: 1, AccountID: 1, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 502},
	}); err != nil {
		t.Fatalf("[%s] 3-row unsorted ApplyMarginSettlements: %v", mode, err)
	}

	streamZeroSettlements := make([]MarginSettlement, 260)
	for i := range streamZeroSettlements {
		streamZeroSettlements[i] = MarginSettlement{
			FirmID: 1, AccountID: 1, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: int64(600 + i),
		}
	}
	if _, err := eng.StreamSettleMargins(ctx, runtime.FromSlice(streamZeroSettlements)); err != nil {
		t.Fatalf("[%s] 260-row StreamSettleMargins: %v", mode, err)
	}
	if _, err := q.ApplyMarginSettlementsStream(ctx, runtime.FromSlice([]MarginSettlement(nil))); err != nil {
		t.Fatalf("[%s] empty ApplyMarginSettlementsStream: %v", mode, err)
	}

	// Exercise IngestOrdersSeq, IngestOrdersSeq, IngestOrdersStream, IngestOrdersStreamSeq (including >256 rows)
	smallOrders := []OrderSubmission{
		{FirmID: 1, AccountID: 1, Symbol: "ETH-PERP", Side: "buy", LimitPriceCents: 150_00, Quantity: 2, Status: "open", ClientOrderID: "eth-b1-" + mode, UpdatedSeq: 700},
		{FirmID: 1, AccountID: 1, Symbol: "SOL-PERP", Side: "buy", LimitPriceCents: 150_00, Quantity: 2, Status: "open", ClientOrderID: "sol-b1-" + mode, UpdatedSeq: 700},
		{FirmID: 1, AccountID: 1, Symbol: "SOL-PERP", Side: "buy", LimitPriceCents: 140_00, Quantity: 2, Status: "open", ClientOrderID: "sol-b2-" + mode, UpdatedSeq: 700},
		{FirmID: 1, AccountID: 1, Symbol: "SOL-PERP", Side: "buy", LimitPriceCents: 130_00, Quantity: 2, Status: "open", ClientOrderID: "sol-b3-" + mode, UpdatedSeq: 700},
	}
	var cancelIDs []int64
	for ack, err := range q.IngestOrdersSeq(ctx, smallOrders, 700) {
		if err != nil {
			t.Fatalf("[%s] IngestOrdersSeq: %v", mode, err)
		}
		cancelIDs = append(cancelIDs, ack.OrderID)
	}
	// Early break on IngestOrdersSeq and IngestOrdersEach
	for _, err := range q.IngestOrdersSeq(ctx, smallOrders, 700) {
		if err != nil {
			t.Fatalf("[%s] IngestOrdersSeq break: %v", mode, err)
		}
		break
	}
	if err := runtime.Each(q.IngestOrdersSeq(ctx, smallOrders[:2], 700), func(_ OrderAck) error { return nil }); err != nil {
		t.Fatalf("[%s] IngestOrdersEach: %v", mode, err)
	}
	for range 2 {
		if _, err := q.IngestOrders(ctx, smallOrders[:2], 701); err != nil {
			t.Fatalf("[%s] 2-row IngestOrders: %v", mode, err)
		}
	}
	// 3-row sorted & unsorted IngestOrders, IngestOrdersSeq, IngestOrdersEach
	if _, err := q.IngestOrders(ctx, smallOrders[:3], 702); err != nil {
		t.Fatalf("[%s] 3-row sorted IngestOrders: %v", mode, err)
	}
	unsortedOrders := []OrderSubmission{smallOrders[3], smallOrders[1], smallOrders[0], smallOrders[2]}
	for _, err := range q.IngestOrdersSeq(ctx, unsortedOrders[:3], 703) {
		if err != nil {
			t.Fatalf("[%s] 3-row unsorted IngestOrdersSeq: %v", mode, err)
		}
	}
	for _, err := range q.IngestOrdersSeq(ctx, smallOrders[:3], 703) {
		if err != nil {
			t.Fatalf("[%s] IngestOrdersSeq break: %v", mode, err)
		}
		break
	}
	if err := runtime.Each(q.IngestOrdersSeq(ctx, smallOrders[:3], 703), func(_ OrderAck) error { return nil }); err != nil {
		t.Fatalf("[%s] IngestOrdersEach: %v", mode, err)
	}

	streamOrders := make([]OrderSubmission, 260)
	for i := range streamOrders {
		streamOrders[i] = OrderSubmission{
			FirmID: 1, AccountID: 1, Symbol: "SOL-PERP", Side: "buy",
			LimitPriceCents: int64(100_00 + i), Quantity: 1, Status: "open",
			ClientOrderID: fmt.Sprintf("stream-ord-%s-%d", mode, i), UpdatedSeq: 800,
		}
	}
	streamAcks, err := q.IngestOrdersStream(ctx, runtime.FromSlice(streamOrders), 800)
	if err != nil || len(streamAcks) != 260 {
		t.Fatalf("[%s] 260-row IngestOrdersStream: len=%d err=%v", mode, len(streamAcks), err)
	}
	// Early break on IngestOrdersStreamSeq and empty stream
	for _, err := range q.IngestOrdersStreamSeq(ctx, runtime.FromSlice(streamOrders[:3]), 801) {
		if err != nil {
			t.Fatalf("[%s] IngestOrdersStreamSeq break: %v", mode, err)
		}
		break
	}
	for _, err := range q.IngestOrdersStreamSeq(ctx, runtime.FromSlice([]OrderSubmission(nil)), 801) {
		if err != nil {
			t.Fatalf("[%s] empty IngestOrdersStreamSeq: %v", mode, err)
		}
	}
	for _, a := range streamAcks[:10] {
		cancelIDs = append(cancelIDs, a.OrderID)
	}

	// CancelOrdersByIDs, CancelOrdersByIDsSeq, CancelOrdersByIDsEach, CancelOrdersByIDs, CancelOrdersByIDsSeq, CancelOrdersByIDsEach
	if _, err := q.CancelOrdersByIDs(ctx, 900, 1, []int64{cancelIDs[0], cancelIDs[1]}); err != nil {
		t.Fatalf("[%s] sorted CancelOrdersByIDs: %v", mode, err)
	}
	if _, err := q.CancelOrdersByIDs(ctx, 900, 1, []int64{cancelIDs[3], cancelIDs[2]}); err != nil {
		t.Fatalf("[%s] unsorted CancelOrdersByIDs: %v", mode, err)
	}
	for _, err := range q.CancelOrdersByIDsSeq(ctx, 901, 1, []int64{cancelIDs[4], cancelIDs[5]}) {
		if err != nil {
			t.Fatalf("[%s] CancelOrdersByIDsSeq: %v", mode, err)
		}
		break
	}
	if err := runtime.Each(q.CancelOrdersByIDsSeq(ctx, 901, 1, []int64{cancelIDs[5]}), func(_ OrderAck) error { return nil }); err != nil {
		t.Fatalf("[%s] CancelOrdersByIDsEach: %v", mode, err)
	}
	for range 2 {
		if _, err := q.CancelOrdersByIDs(ctx, 902, 1, []int64{cancelIDs[6], cancelIDs[7]}); err != nil {
			t.Fatalf("[%s] 2-row CancelOrdersByIDs: %v", mode, err)
		}
	}
	if _, err := q.CancelOrdersByIDs(ctx, 903, 1, []int64{cancelIDs[6], cancelIDs[7], cancelIDs[8]}); err != nil {
		t.Fatalf("[%s] 3-row sorted CancelOrdersByIDs: %v", mode, err)
	}
	for _, err := range q.CancelOrdersByIDsSeq(ctx, 904, 1, []int64{cancelIDs[10], cancelIDs[9], cancelIDs[8]}) {
		if err != nil {
			t.Fatalf("[%s] 3-row unsorted CancelOrdersByIDsSeq: %v", mode, err)
		}
	}
	for _, err := range q.CancelOrdersByIDsSeq(ctx, 904, 1, []int64{cancelIDs[8], cancelIDs[9], cancelIDs[10]}) {
		if err != nil {
			t.Fatalf("[%s] CancelOrdersByIDsSeq break: %v", mode, err)
		}
		break
	}
	if err := runtime.Each(q.CancelOrdersByIDsSeq(ctx, 905, 1, []int64{cancelIDs[11], cancelIDs[12], cancelIDs[13]}), func(_ OrderAck) error { return nil }); err != nil {
		t.Fatalf("[%s] CancelOrdersByIDsEach: %v", mode, err)
	}

	// Direct execution of remaining statements + curried Query + Pipeline wrappers
	if _, err := q.ListOpenOrdersForSymbol(ctx, "SOL-PERP", "buy", 5); err != nil {
		t.Fatalf("[%s] ListOpenOrdersForSymbol: %v", mode, err)
	}
	for _, err := range q.ListOpenOrdersForSymbolSeq(ctx, "SOL-PERP", "buy", 5) {
		if err != nil {
			t.Fatalf("[%s] ListOpenOrdersForSymbolSeq: %v", mode, err)
		}
		break
	}
	if err := runtime.Each(q.ListOpenOrdersForSymbolSeq(ctx, "SOL-PERP", "buy", 2), func(_ OpenOrderRow) error { return nil }); err != nil {
		t.Fatalf("[%s] ListOpenOrdersForSymbolEach: %v", mode, err)
	}
	if _, err := q.AdjustAccountMarginAndPnl(ctx, 0, 0, 910, 1, 1); err != nil {
		t.Fatalf("[%s] AdjustAccountMarginAndPnl: %v", mode, err)
	}
	if _, err := q.MarkOrderFilled(ctx, "cancelled", 0, 910, cancelIDs[0]); err != nil {
		t.Fatalf("[%s] MarkOrderFilled: %v", mode, err)
	}
	if _, err := q.UpsertPositionSnapshot(ctx, 99999, 1, 1, "UST-10Y", 0, 100_00, 0, 0, 0, 0, 910); err != nil {
		t.Fatalf("[%s] UpsertPositionSnapshot: %v", mode, err)
	}
	if _, err := q.InsertClearingFill(ctx, "direct-fill-"+mode, 1, 2, 1, 2, "UST-10Y", 100_00, 0, 0, 910); err != nil {
		t.Fatalf("[%s] InsertClearingFill: %v", mode, err)
	}
	if _, err := q.InsertComplianceNotice(ctx, "direct-fill-"+mode, 1, 1, "audit_ping", 0, 910); err != nil {
		t.Fatalf("[%s] InsertComplianceNotice: %v", mode, err)
	}
	if _, err := q.TouchExchangeHeartbeat(ctx, 910); err != nil {
		t.Fatalf("[%s] TouchExchangeHeartbeat: %v", mode, err)
	}
	if _, err := q.PurgeExpiredLocks(ctx); err != nil {
		t.Fatalf("[%s] PurgeExpiredLocks: %v", mode, err)
	}
	if cnt, err := q.CountClearingFills(ctx); err != nil || cnt == 0 {
		t.Fatalf("[%s] CountClearingFills: cnt=%d err=%v", mode, cnt, err)
	}
	if cnt, err := q.CountComplianceNotices(ctx, "institutional_cleared"); err != nil || cnt == 0 {
		t.Fatalf("[%s] CountComplianceNotices: cnt=%d err=%v", mode, cnt, err)
	}

	// Curried Query constructors & Pipeline methods
	acctSub := q.EligibleAccountsSubqueryQuery(EligibleAccountsSubqueryParams{FirmIDs: []int64{1}, RequiredStatus: "active"})
	instSub := q.ActiveInstrumentsSubqueryQuery(ActiveInstrumentsSubqueryParams{AssetClasses: []string{"crypto"}})
	posSub := q.FilteredPositionsSubqueryQuery(FilteredPositionsSubqueryParams{EligibleAccounts: acctSub, ActiveInstruments: instSub})
	riskParams := QueryRiskPositionsPageParams{
		MatchingPositions: posSub,
		Order:             OrderQueryRiskPositionsPage(QueryRiskPositionsPageOrderByNotionalCents.Asc(), QueryRiskPositionsPageOrderByPositionID.Desc()),
		Seek:              SeekQueryRiskPositionsPage(RiskPositionView{PositionID: 0}),
		Limit:             5,
	}
	if err := runtime.Each(q.QueryRiskPositionsPageSeq(ctx, riskParams), func(_ RiskPositionView) error { return nil }); err != nil {
		t.Fatalf("[%s] QueryRiskPositionsPageEach: %v", mode, err)
	}
	var dummyArgs []any
	for _, cq := range []Query{
		q.GetMarginAccountQuery(1, 1),
		q.GetAccountCollateralCentsQuery(1, 1),
		q.IngestOrdersQuery(smallOrders[:1], 920),
		q.CancelOrdersByIDsQuery(920, 1, []int64{1}),
		q.ListOpenOrdersForSymbolQuery("SOL-PERP", "buy", 5),
		q.QueryRiskPositionsPageQuery(riskParams),
		q.TotalExchangeEquityCentsQuery(),
		q.TotalNetPositionBySymbolQuery("BTC-PERP"),
		q.CountClearingFillsQuery(),
		q.CountComplianceNoticesQuery("audit_ping"),
	} {
		_ = cq.Expand(&dummyArgs, runtime.DialectQuestion, &cq)
	}

	if err := q.InTx(ctx, nil, func(txQ *Queries) error {
		p := txQ.Pipeline(ctx)
		_ = p.UpsertInstrument("EUR-FX", "fx", 1, 200, 1)
		_ = p.UpsertFirm(4, "Delta Retail Custody", "ESMA-EU", 500)
		_ = p.ApplyMarginSettlements([]MarginSettlement{
			{FirmID: 1, AccountID: 1, Status: "active", CollateralCents: 0, RealizedPnlCents: 0, MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 930},
		})
		_ = p.SetAccountStatus("active", 930, 1, 1)
		_ = p.IngestOrders(smallOrders[:1], 930)
		_ = p.CancelOrdersByIDs(930, 1, []int64{cancelIDs[0]})
		_ = p.QueryRiskPositionsPage(riskParams)
		_ = p.TotalExchangeEquityCents()
		_ = p.TotalNetPositionBySymbol("BTC-PERP")
		_ = p.CountClearingFills()
		_ = p.CountComplianceNotices("audit_ping")
		return p.Flush()
	}); err != nil {
		t.Fatalf("[%s] InTxOpts pipeline: %v", mode, err)
	}

	errStop := errors.New("stop-iter")
	_ = q.PipelineTx(ctx, nil, func(_ *Pipeline) error { return errStop })
	_, _ = eng.StreamSettleMargins(ctx, func(yield func(MarginSettlement, error) bool) { yield(MarginSettlement{}, errStop) })
	_, _ = q.IngestOrdersStream(ctx, func(yield func(OrderSubmission, error) bool) { yield(OrderSubmission{}, errStop) }, 940)
	_ = runtime.Each(q.IngestOrdersSeq(ctx, nil, 940), func(_ OrderAck) error { return nil })
	_ = runtime.Each(q.IngestOrdersSeq(ctx, smallOrders[:1], 940), func(_ OrderAck) error { return errStop })
	_ = runtime.Each(q.CancelOrdersByIDsSeq(ctx, 940, 1, nil), func(_ OrderAck) error { return nil })
	_ = runtime.Each(q.CancelOrdersByIDsSeq(ctx, 940, 1, []int64{cancelIDs[0]}), func(_ OrderAck) error { return errStop })
	_ = runtime.Each(q.ListOpenOrdersForSymbolSeq(ctx, "SOL-PERP", "buy", 2), func(_ OpenOrderRow) error { return errStop })
	_ = runtime.Each(q.QueryRiskPositionsPageSeq(ctx, riskParams), func(_ RiskPositionView) error { return errStop })
	_, _ = q.CancelOrdersByIDs(ctx, 941, 1, []int64{cancelIDs[1], cancelIDs[0], cancelIDs[0]})
	instDup := q.ActiveInstrumentsSubqueryQuery(ActiveInstrumentsSubqueryParams{AssetClasses: []string{"equity", "crypto", "crypto"}})
	_ = instDup.Expand(&dummyArgs, runtime.DialectQuestion, &instDup)
	_, _ = q.IngestOrders(ctx, []OrderSubmission{
		{FirmID: 1, AccountID: 1, Symbol: "SOL-PERP", Side: "buy", LimitPriceCents: 140_00, Quantity: 1, Status: "open", ClientOrderID: "dup-1-" + mode, UpdatedSeq: 942},
		{FirmID: 1, AccountID: 1, Symbol: "SOL-PERP", Side: "buy", LimitPriceCents: 150_00, Quantity: 1, Status: "open", ClientOrderID: "dup-2-" + mode, UpdatedSeq: 942},
		{FirmID: 1, AccountID: 1, Symbol: "SOL-PERP", Side: "buy", LimitPriceCents: 150_00, Quantity: 1, Status: "open", ClientOrderID: "dup-3-" + mode, UpdatedSeq: 942},
	}, 942)

	// Pipeline path: collections are presorted before they are queued, and an empty VALUES collection queues
	// nothing (its future is zero/empty) without disturbing the operations around it.
	pl := q.Pipeline(ctx)
	fSettle := pl.ApplyMarginSettlements([]MarginSettlement{
		{FirmID: 2, AccountID: 2, Status: "active", MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 950},
		{FirmID: 1, AccountID: 5, Status: "active", MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 950},
		{FirmID: 1, AccountID: 1, Status: "active", MarginRatioBps: 2000, KycVerified: 1, UpdatedSeq: 950},
	})
	fNoSettle := pl.ApplyMarginSettlements(nil)
	fIngest := pl.IngestOrders([]OrderSubmission{smallOrders[3], smallOrders[1], smallOrders[0]}, 950)
	fNoIngest := pl.IngestOrders(nil, 950)
	fCancel := pl.CancelOrdersByIDs(950, 1, []int64{cancelIDs[1], cancelIDs[0]})
	if err := pl.Flush(); err != nil {
		t.Fatalf("[%s] pipeline of collection statements: %v", mode, err)
	}
	if n, err := fSettle.RowsAffected(); err != nil || n != 3 {
		t.Fatalf("[%s] pipelined ApplyMarginSettlements = %d, %v", mode, n, err)
	}
	if n, err := fNoSettle.RowsAffected(); err != nil || n != 0 {
		t.Fatalf("[%s] pipelined empty ApplyMarginSettlements = %d, %v", mode, n, err)
	}
	if acks, err := fIngest.Get(); err != nil || len(acks) != 3 {
		t.Fatalf("[%s] pipelined IngestOrders = %v, %v", mode, acks, err)
	}
	if acks, err := fNoIngest.Get(); err != nil || len(acks) != 0 {
		t.Fatalf("[%s] pipelined empty IngestOrders = %v, %v", mode, acks, err)
	}
	if _, err := fCancel.Get(); err != nil {
		t.Fatalf("[%s] pipelined CancelOrdersByIDs: %v", mode, err)
	}

	_ = NewWithCache(db, nil)
	_ = q.Cache()
	_ = q.CacheStats()
	unprepQ := q.WithPrepared(false)
	if _, err := unprepQ.TotalExchangeEquityCents(ctx); err != nil {
		t.Fatalf("[%s] unprepQ.TotalExchangeEquityCents: %v", mode, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	txView := q.WithTx(tx)
	_ = txView.Close()
	_ = tx.Rollback()
}

func TestClearingExchangeEndToEnd(t *testing.T) {
	t.Run("SequentialTx", func(t *testing.T) {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		defer func() { _ = db.Close() }()
		runClearingExchangeStressSuite(t, "seq", db)
	})

	t.Run("WirePipeliner", func(t *testing.T) {
		db := openExchangeWireDB(t, ":memory:")
		defer func() { _ = db.Close() }()
		runClearingExchangeStressSuite(t, "wire", db)
	})
}
