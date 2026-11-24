//go:generate go run ../../cmd/sqlgen -config sqlgen.yaml

package clearing_exchange

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"

	"github.com/CAFxX/sqlgen/runtime"
)

var (
	ErrAccountNotActive = errors.New("clearing_exchange: margin account is not active")
	ErrInvalidTrade     = errors.New("clearing_exchange: trade quantity and price must be positive and accounts distinct")
)

const SchemaDDL = `
CREATE TABLE IF NOT EXISTS firms (
	firm_id          INTEGER PRIMARY KEY,
	firm_name        TEXT    NOT NULL,
	regulator_code   TEXT    NOT NULL,
	max_leverage_bps INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS instruments (
	symbol          TEXT    PRIMARY KEY,
	asset_class     TEXT    NOT NULL,
	is_marginable   INTEGER NOT NULL,
	haircut_bps     INTEGER NOT NULL,
	tick_size_cents INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS margin_accounts (
	firm_id            INTEGER NOT NULL,
	account_id         INTEGER NOT NULL,
	status             TEXT    NOT NULL DEFAULT 'active',
	collateral_cents   INTEGER NOT NULL,
	realized_pnl_cents INTEGER NOT NULL,
	margin_ratio_bps   INTEGER NOT NULL,
	kyc_verified       INTEGER NOT NULL,
	updated_seq        INTEGER NOT NULL,
	PRIMARY KEY (firm_id, account_id)
);

CREATE TABLE IF NOT EXISTS orders (
	order_id          INTEGER PRIMARY KEY AUTOINCREMENT,
	firm_id           INTEGER NOT NULL,
	account_id        INTEGER NOT NULL,
	symbol            TEXT    NOT NULL,
	side              TEXT    NOT NULL,
	limit_price_cents INTEGER NOT NULL,
	quantity          INTEGER NOT NULL,
	status            TEXT    NOT NULL,
	client_order_id   TEXT    NOT NULL,
	updated_seq       INTEGER NOT NULL,
	UNIQUE (firm_id, client_order_id)
);

CREATE TABLE IF NOT EXISTS positions (
	position_id          INTEGER PRIMARY KEY,
	firm_id              INTEGER NOT NULL,
	account_id           INTEGER NOT NULL,
	symbol               TEXT    NOT NULL,
	net_quantity         INTEGER NOT NULL,
	avg_entry_cents      INTEGER NOT NULL,
	notional_cents       INTEGER NOT NULL,
	unrealized_pnl_cents INTEGER NOT NULL,
	delta_bps            INTEGER NOT NULL,
	gamma_bps            INTEGER NOT NULL,
	updated_seq          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS clearing_fills (
	fill_id           TEXT    PRIMARY KEY,
	buy_order_id      INTEGER NOT NULL,
	sell_order_id     INTEGER NOT NULL,
	buyer_account_id  INTEGER NOT NULL,
	seller_account_id INTEGER NOT NULL,
	symbol            TEXT    NOT NULL,
	price_cents       INTEGER NOT NULL,
	quantity          INTEGER NOT NULL,
	notional_cents    INTEGER NOT NULL,
	seq               INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS compliance_notices (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	fill_id        TEXT    NOT NULL,
	firm_id        INTEGER NOT NULL,
	account_id     INTEGER NOT NULL,
	notice_kind    TEXT    NOT NULL,
	notional_cents INTEGER NOT NULL,
	seq            INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS exchange_heartbeat (
	singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
	last_seq     INTEGER NOT NULL,
	total_events INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS temp_locks (
	lock_id INTEGER PRIMARY KEY
);

INSERT OR IGNORE INTO exchange_heartbeat (singleton_id, last_seq, total_events) VALUES (1, 0, 0);
`

// ClearingEngine orchestrates multi-asset order ingestion, speculative pipeline matching,
// double-entry margin clearing, and 3-level nested subquery risk reporting.
type ClearingEngine struct {
	q *Queries
}

func NewClearingEngine(db *sql.DB, opts ...runtime.CacheOption) *ClearingEngine {
	return &ClearingEngine{
		q: New(db, opts...),
	}
}

func (e *ClearingEngine) Queries() *Queries {
	return e.q
}

func (e *ClearingEngine) Close() error {
	return e.q.Close()
}

// BatchSettleMargins applies a batch of margin settlements inside a retryable transaction.
// ApplyMarginSettlements automatically presorts by (FirmID ASC, AccountID ASC)
// without mutating caller memory.
func (e *ClearingEngine) BatchSettleMargins(ctx context.Context, settlements []MarginSettlement) (int64, error) {
	var affected int64
	err := e.q.InTx(ctx, nil, func(txQ *Queries) error {
		res, err := txQ.ApplyMarginSettlements(ctx, settlements)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		affected = n
		return nil
	}, WithTxRetry(DefaultRetryPolicy()))
	return affected, err
}

// StreamSettleMargins streams margin settlements in 256-row pooled chunks.
func (e *ClearingEngine) StreamSettleMargins(ctx context.Context, seq iter.Seq2[MarginSettlement, error]) (int64, error) {
	res, err := e.q.ApplyMarginSettlementsStream(ctx, seq)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// MatchRequest specifies the parameters for an atomic order-book match and clearing pipeline.
type MatchRequest struct {
	FillID              string
	Symbol              string
	BuyerFirmID         int64
	BuyerAccountID      int64
	BuyerPositionID     int64
	BuyOrderID          int64
	SellerFirmID        int64
	SellerAccountID     int64
	SellerPositionID    int64
	SellOrderID         int64
	PriceCents          int64
	Quantity            int64
	ClearingFeeCents    int64
	RequiredMarginCents int64
	Seq                 int64
}

// MatchOutcome reports the branch decisions taken by ExecuteMatchAndClear.
type MatchOutcome struct {
	Cleared          bool
	Rejected         bool
	LargeTraderFiled bool
}

// ExecuteMatchAndClear executes a speculative 3-level nested PipelineTx that verifies account statuses,
// free collateral, and open counter-orders on the book before clearing margin, positions, and compliance filings.
func (e *ClearingEngine) ExecuteMatchAndClear(ctx context.Context, req MatchRequest) (MatchOutcome, error) {
	if req.Quantity <= 0 || req.PriceCents <= 0 ||
		(req.BuyerFirmID == req.SellerFirmID && req.BuyerAccountID == req.SellerAccountID) {
		return MatchOutcome{}, ErrInvalidTrade
	}

	notionalCents := req.PriceCents * req.Quantity
	var clearBranch, complianceBranch *IfResult

	err := e.q.PipelineTx(ctx, nil, func(p *Pipeline) error {
		// Acquire account reads in canonical (firm_id, account_id) order
		firstFirm, firstAcct := req.BuyerFirmID, req.BuyerAccountID
		secondFirm, secondAcct := req.SellerFirmID, req.SellerAccountID
		swapped := firstFirm > secondFirm || (firstFirm == secondFirm && firstAcct > secondAcct)
		if swapped {
			firstFirm, secondFirm = secondFirm, firstFirm
			firstAcct, secondAcct = secondAcct, firstAcct
		}

		firstFut := p.GetMarginAccount(firstFirm, firstAcct).AssertFound()
		secondFut := p.GetMarginAccount(secondFirm, secondAcct).AssertFound()

		buyerFut, sellerFut := firstFut, secondFut
		if swapped {
			buyerFut, sellerFut = secondFut, firstFut
		}

		// Also read buyer's scalar collateral via scalar RowFuture[int64]
		buyerCollatFut := p.GetAccountCollateralCents(req.BuyerFirmID, req.BuyerAccountID).AssertFound()

		// Read top open sell orders on the book via SliceFuture[OpenOrderRow]
		openSellsFut := p.ListOpenOrdersForSymbol(req.Symbol, "sell", 5)

		// Condition 1: Either buyer or seller account is not active (e.g. frozen/halted)
		eitherHaltedCond := WhereCol(buyerFut, "status != 'active'", func(a MarginAccount) bool {
			return a.Status != "active"
		}).Or(WhereCol(sellerFut, "status != 'active'", func(a MarginAccount) bool {
			return a.Status != "active"
		}))

		// Condition 2: Buyer has sufficient collateral AND open sell orders exist with limit_price_cents <= req.PriceCents
		sufficientMarginCond := buyerFut.Found().And(sellerFut.Found()).And(
			WhereCol(buyerCollatFut, fmt.Sprintf("collateral_cents >= %d", req.RequiredMarginCents), func(c int64) bool {
				return c >= req.RequiredMarginCents
			}),
		)
		bookMatchCond := openSellsFut.Found().And(openSellsFut.RowsAffectedGe(1)).And(
			openSellsFut.WhereCol(fmt.Sprintf("limit_price_cents <= %d", req.PriceCents), func(o OpenOrderRow) bool {
				return o.LimitPriceCents <= req.PriceCents
			}),
		)
		canClearCond := sufficientMarginCond.And(bookMatchCond)

		// Level 1 If: Halt/Abort vs. Normal Clearing Evaluation
		p.If(
			eitherHaltedCond,
			func(haltP *Pipeline) {
				haltP.Abort(ErrAccountNotActive)
			},
			func(activeP *Pipeline) {
				// Level 2 If: Sufficient Margin & Matching Book vs. Margin/Price Rejection
				clearBranch = activeP.If(
					canClearCond,
					func(clearP *Pipeline) {
						// Double-entry zero-sum clearing fee / PnL transfer between buyer and seller
						clearP.AdjustAccountMarginAndPnl(-req.ClearingFeeCents, -req.ClearingFeeCents, req.Seq, req.BuyerFirmID, req.BuyerAccountID).AssertRowsAffected(1)
						clearP.AdjustAccountMarginAndPnl(req.ClearingFeeCents, req.ClearingFeeCents, req.Seq, req.SellerFirmID, req.SellerAccountID).AssertRowsAffected(1)

						clearP.MarkOrderFilled("filled", 0, req.Seq, req.BuyOrderID).AssertRowsAffected(1)
						clearP.MarkOrderFilled("filled", 0, req.Seq, req.SellOrderID).AssertRowsAffected(1)

						// Double-entry position updates (+Qty for buyer, -Qty for seller)
						clearP.UpsertPositionSnapshot(
							req.BuyerPositionID, req.BuyerFirmID, req.BuyerAccountID, req.Symbol,
							req.Quantity, req.PriceCents, notionalCents, 0, 10000, 250, req.Seq,
						).AssertRowsAffected(1)
						clearP.UpsertPositionSnapshot(
							req.SellerPositionID, req.SellerFirmID, req.SellerAccountID, req.Symbol,
							-req.Quantity, req.PriceCents, notionalCents, 0, -10000, 250, req.Seq,
						).AssertRowsAffected(1)

						fillFut := clearP.InsertClearingFill(
							req.FillID, req.BuyOrderID, req.SellOrderID, req.BuyerAccountID, req.SellerAccountID,
							req.Symbol, req.PriceCents, req.Quantity, notionalCents, req.Seq,
						).AssertRowsAffected(1)

						// Exercise UPDATE without WHERE and DELETE without WHERE inside Pipeline.If
						clearP.TouchExchangeHeartbeat(req.Seq).AssertRowsAffected(1)
						clearP.PurgeExpiredLocks()

						// Level 3 Nested If: Large-trader institutional KYC compliance filing vs. standard audit notice
						isInstitutionalCond := fillFut.RowsAffectedEq(1).And(
							WhereCol(buyerFut, "kyc_verified = 1 AND margin_ratio_bps >= 1500", func(a MarginAccount) bool {
								return a.KycVerified == 1 && a.MarginRatioBps >= 1500
							}),
						)
						complianceBranch = clearP.If(
							isInstitutionalCond,
							func(instP *Pipeline) {
								instP.InsertComplianceNotice(req.FillID, req.BuyerFirmID, req.BuyerAccountID, "institutional_cleared", notionalCents, req.Seq).AssertRowsAffected(1)
							},
							func(retailP *Pipeline) {
								retailP.InsertComplianceNotice(req.FillID, req.BuyerFirmID, req.BuyerAccountID, "standard_cleared", notionalCents, req.Seq).AssertRowsAffected(1)
							},
						)
					},
					func(rejectP *Pipeline) {
						rejectP.InsertComplianceNotice(req.FillID, req.BuyerFirmID, req.BuyerAccountID, "rejected_margin_or_price", notionalCents, req.Seq).AssertRowsAffected(1)
						rejectP.TouchExchangeHeartbeat(req.Seq).AssertRowsAffected(1)
					},
				)
			},
		)
		return nil
	}, WithTxRetry(DefaultRetryPolicy()))
	if err != nil {
		return MatchOutcome{}, err
	}

	cleared := clearBranch != nil && clearBranch.Taken()
	return MatchOutcome{
		Cleared:          cleared,
		Rejected:         clearBranch != nil && !clearBranch.Taken(),
		LargeTraderFiled: cleared && complianceBranch != nil && complianceBranch.Taken(),
	}, nil
}

// RiskQueryFilter configures the 3-level nested subquery + keyset pagination risk report.
type RiskQueryFilter struct {
	FirmIDs          []int64
	RequireKYC       bool
	MinMarginRatio   bool
	MinMarginBps     int64
	AssetClasses     []string
	OnlyMarginable   bool
	MaxHaircut       bool
	MaxHaircutBps    int64
	MinAbsQuantity   bool
	MinAbsQty        int64
	FilterAccounts   bool
	AccountIDs       []int64
	MinNotional      bool
	MinNotionalCents int64
	IncludeGreeks    bool
	IncludeFirmInfo  bool
	SelectCols       uint64
	Order            Order
	PageSize         int64
}

// CollectAllRiskPositions composes the 3-level subquery hierarchy (EligibleAccountsSubquery +
// ActiveInstrumentsSubquery -> FilteredPositionsSubquery -> QueryRiskPositionsPage) and
// walks all pages using composite keyset pagination.
func (e *ClearingEngine) CollectAllRiskPositions(ctx context.Context, cfg RiskQueryFilter) ([]RiskPositionView, error) {
	acctSub := e.q.EligibleAccountsSubqueryQuery(EligibleAccountsSubqueryParams{
		FirmIDs:           cfg.FirmIDs,
		RequiredStatus:    "active",
		RequireKyc:        cfg.RequireKYC,
		MinMarginRatio:    cfg.MinMarginRatio,
		MinMarginRatioBps: cfg.MinMarginBps,
	})
	instSub := e.q.ActiveInstrumentsSubqueryQuery(ActiveInstrumentsSubqueryParams{
		AssetClasses:   cfg.AssetClasses,
		OnlyMarginable: cfg.OnlyMarginable,
		MaxHaircut:     cfg.MaxHaircut,
		MaxHaircutBps:  cfg.MaxHaircutBps,
	})
	posSub := e.q.FilteredPositionsSubqueryQuery(FilteredPositionsSubqueryParams{
		EligibleAccounts:  acctSub,
		ActiveInstruments: instSub,
		MinAbsQuantity:    cfg.MinAbsQuantity,
		MinAbsQty:         cfg.MinAbsQty,
	})

	pageSize := cfg.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}

	var all []RiskPositionView
	var cursor Seek
	for {
		page, err := e.q.QueryRiskPositionsPage(ctx, QueryRiskPositionsPageParams{
			MatchingPositions: posSub,
			IncludeGreeks:     cfg.IncludeGreeks,
			IncludeFirmInfo:   cfg.IncludeFirmInfo,
			FilterAccounts:    cfg.FilterAccounts,
			AccountIDs:        cfg.AccountIDs,
			MinNotional:       cfg.MinNotional,
			MinNotionalCents:  cfg.MinNotionalCents,
			SelectCols:        cfg.SelectCols,
			Order:             cfg.Order,
			Seek:              cursor,
			Limit:             pageSize,
		})
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		if int64(len(page)) < pageSize {
			break
		}
		cursor = SeekRiskPositionView(page[len(page)-1])
	}
	return all, nil
}
