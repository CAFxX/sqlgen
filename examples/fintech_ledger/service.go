//go:generate go run ../../cmd/sqlgen -config sqlgen.yaml

package fintech_ledger

import (
	"context"
	"database/sql"
	"errors"

	"github.com/CAFxX/sqlgen/runtime"
)

var (
	ErrAccountFrozen = errors.New("fintech_ledger: account is frozen")
	ErrInvalidAmount = errors.New("fintech_ledger: transfer amount must be positive")
	ErrSameAccount   = errors.New("fintech_ledger: sender and receiver must be distinct")
)

const SchemaDDL = `
CREATE TABLE IF NOT EXISTS accounts (
	org_id        INTEGER NOT NULL,
	id            INTEGER NOT NULL,
	currency      TEXT    NOT NULL,
	balance_cents INTEGER NOT NULL,
	status        TEXT    NOT NULL DEFAULT 'active',
	PRIMARY KEY (org_id, id)
);

CREATE TABLE IF NOT EXISTS transfer_receipts (
	idempotency_key TEXT    PRIMARY KEY,
	org_id          INTEGER NOT NULL,
	sender_id       INTEGER NOT NULL,
	receiver_id     INTEGER NOT NULL,
	amount_cents    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	org_id       INTEGER NOT NULL,
	account_id   INTEGER NOT NULL,
	event_type   TEXT    NOT NULL,
	amount_cents INTEGER NOT NULL
);
`

type LedgerService struct {
	q *Queries
}

func NewLedgerService(db *sql.DB, opts ...runtime.CacheOption) *LedgerService {
	return &LedgerService{
		q: New(db, opts...),
	}
}

func (s *LedgerService) Queries() *Queries {
	return s.q
}

func (s *LedgerService) Close() error {
	return s.q.Close()
}

type TransferResult struct {
	Transferred       bool
	InsufficientFunds bool
}

// Transfer executes an atomic double-entry transfer using PipelineTx, speculative assertions,
// and nested If/Then/Else branching.
//
// - If either account is frozen, the transaction aborts with ErrAccountFrozen and rolls back completely.
// - If the sender has sufficient balance, sender is debited, receiver is credited, and a transfer receipt is recorded.
// - If the sender has insufficient balance, an audit event ("rejected_insufficient_funds") is recorded and committed without moving funds.
func (s *LedgerService) Transfer(
	ctx context.Context,
	orgID, senderID, receiverID, amountCents int64,
	idempotencyKey string,
) (TransferResult, error) {
	if amountCents <= 0 {
		return TransferResult{}, ErrInvalidAmount
	}
	if senderID == receiverID {
		return TransferResult{}, ErrSameAccount
	}

	var transferBranch *runtime.IfResult
	err := s.q.PipelineTx(ctx, nil, func(p *Pipeline) error {
		firstID, secondID := senderID, receiverID
		if firstID > secondID {
			firstID, secondID = secondID, firstID
		}
		firstFut := p.GetAccount(orgID, firstID).AssertFound()
		secondFut := p.GetAccount(orgID, secondID).AssertFound()

		senderFut := firstFut
		receiverFut := secondFut
		if senderID != firstID {
			senderFut = secondFut
			receiverFut = firstFut
		}

		eitherFrozenCond := senderFut.WhereCol("status = 'frozen'", func(a Account) bool {
			return a.Status == "frozen"
		}).Or(receiverFut.WhereCol("status = 'frozen'", func(a Account) bool {
			return a.Status == "frozen"
		}))

		sufficientCond := senderFut.Found().And(
			senderFut.WhereCol("balance_cents >= ?", func(a Account) bool {
				return a.BalanceCents >= amountCents
			}, amountCents),
		)

		p.If(
			eitherFrozenCond,
			func(frozenP *Pipeline) {
				frozenP.Abort(ErrAccountFrozen)
			},
			func(activeP *Pipeline) {
				transferBranch = activeP.If(
					sufficientCond,
					func(thenP *Pipeline) {
						thenP.DebitAccount(amountCents, orgID, senderID).AssertRows(1)
						thenP.CreditAccount(amountCents, orgID, receiverID).AssertRows(1)
						thenP.InsertTransferReceipt(idempotencyKey, orgID, senderID, receiverID, amountCents).AssertRows(1)
					},
					func(elseP *Pipeline) {
						elseP.InsertAuditEvent(orgID, senderID, "rejected_insufficient_funds", amountCents).AssertRows(1)
					},
				)
			},
		)
		return nil
	}, runtime.WithTxRetry(runtime.DefaultRetryPolicy()))
	if err != nil {
		return TransferResult{}, err
	}

	return TransferResult{
		Transferred:       transferBranch != nil && transferBranch.Taken(),
		InsufficientFunds: transferBranch != nil && !transferBranch.Taken(),
	}, nil
}

// BulkSettle applies a batch of account balance settlements inside a retryable transaction.
// UpsertAccountBalances automatically presorts items by (OrgID ASC, AccountID ASC)
// without mutating the caller's slice.
func (s *LedgerService) BulkSettle(ctx context.Context, items []AccountSettlement) (int64, error) {
	var affected int64
	err := s.q.InTx(ctx, nil, func(txQ *Queries) error {
		res, err := txQ.UpsertAccountBalances(ctx, items)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		affected = n
		return nil
	}, runtime.WithTxRetry(runtime.DefaultRetryPolicy()))
	return affected, err
}
