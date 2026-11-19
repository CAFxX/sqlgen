package integration

// This file is mirrored to adapter_myq_test.go with `pgq` -> `myq` (see `make sync-adapters`).
// It adapts the generated package to the engine-agnostic api used by the shared suite.

import (
	"context"
	"database/sql"
	"iter"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

type pgqAPI struct{ q *pgq.Queries }

func newPgqAPI(db *sql.DB, opts ...runtime.CacheOption) api { return &pgqAPI{q: pgq.New(db, opts...)} }

func toNewPgq(rows []item) []pgq.NewItem {
	out := make([]pgq.NewItem, len(rows))
	for i, r := range rows {
		out[i] = pgq.NewItem{ID: r.ID, OrgID: r.OrgID, Name: r.Name, Score: r.Score, Status: r.Status}
	}
	return out
}

func fromItemsPgq(rows []pgq.Item) []item {
	out := make([]item, len(rows))
	for i, r := range rows {
		out[i] = item{r.ID, r.OrgID, r.Name, r.Score, r.Status}
	}
	return out
}

func (a *pgqAPI) Insert(ctx context.Context, rows []item) error {
	_, err := a.q.InsertItems(ctx, toNewPgq(rows))
	return err
}
func (a *pgqAPI) InsertBatch(ctx context.Context, rows []item) error {
	_, err := a.q.InsertItemsBatch(ctx, toNewPgq(rows))
	return err
}
func (a *pgqAPI) InsertStream(ctx context.Context, rows iter.Seq[item]) error {
	_, err := a.q.InsertItemsStream(ctx, func(yield func(pgq.NewItem, error) bool) {
		for r := range rows {
			if !yield(pgq.NewItem{ID: r.ID, OrgID: r.OrgID, Name: r.Name, Score: r.Score, Status: r.Status}, nil) {
				return
			}
		}
	})
	return err
}
func (a *pgqAPI) Get(ctx context.Context, id int64) (item, error) {
	r, err := a.q.GetItem(ctx, id)
	return item{r.ID, r.OrgID, r.Name, r.Score, r.Status}, err
}
func (a *pgqAPI) ByIDs(ctx context.Context, ids []int64) ([]item, error) {
	r, err := a.q.ItemsByIDs(ctx, ids)
	return fromItemsPgq(r), err
}
func (a *pgqAPI) ByIDsBatch(ctx context.Context, ids []int64) ([]item, error) {
	r, err := a.q.ItemsByIDsBatch(ctx, ids)
	return fromItemsPgq(r), err
}
func (a *pgqAPI) Search(ctx context.Context, s searchArgs) ([]item, error) {
	p := pgq.SearchItemsParams{MinScore: s.MinScore}
	if s.OrgID != nil {
		p.FilterOrg, p.OrgID = true, *s.OrgID
	}
	if s.Statuses != nil {
		p.FilterStatuses, p.Statuses = true, s.Statuses
	}
	if s.NamePrefix != nil {
		p.FilterNamePrefix, p.NameLike = true, *s.NamePrefix+"%"
	}
	r, err := a.q.SearchItems(ctx, p)
	return fromItemsPgq(r), err
}
func (a *pgqAPI) Page(ctx context.Context, orgID int64, scoreDesc, idDesc bool, limit int64) ([]item, error) {
	sc, id := pgq.PageItemsOrderByScore.Asc(), pgq.PageItemsOrderByID.Asc()
	if scoreDesc {
		sc = pgq.PageItemsOrderByScore.Desc()
	}
	if idDesc {
		id = pgq.PageItemsOrderByID.Desc()
	}
	order := pgq.OrderPageItems(sc, id)
	var all []item
	var cursor runtime.Seek
	for {
		page, err := a.q.PageItems(ctx, pgq.PageItemsParams{OrgID: orgID, After: cursor, Sort: order, Limit: limit})
		if err != nil {
			return nil, err
		}
		for _, r := range page {
			all = append(all, item{r.ID, r.OrgID, r.Name, r.Score, r.Status})
		}
		if int64(len(page)) < limit {
			return all, nil
		}
		cursor = pgq.SeekPageItems(page[len(page)-1])
	}
}
func (a *pgqAPI) AddScore(ctx context.Context, orgID, delta int64, ids []int64) (int64, error) {
	res, err := a.q.AddScore(ctx, delta, ids, orgID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (a *pgqAPI) Delete(ctx context.Context, ids []int64) (int64, error) {
	res, err := a.q.DeleteItems(ctx, ids)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (a *pgqAPI) Transfer(ctx context.Context, first, second, amount int64, pol runtime.TxRetryPolicy) error {
	return a.q.InTx(ctx, nil, func(q *pgq.Queries) error {
		if _, err := q.AddScoreOne(ctx, -amount, first); err != nil {
			return err
		}
		_, err := q.AddScoreOne(ctx, amount, second)
		return err
	}, runtime.WithTxRetry(pol))
}
func (a *pgqAPI) PipelineTransfer(ctx context.Context, from, to, amount int64, pol runtime.TxRetryPolicy) error {
	return a.q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
		p.AddScoreOne(-amount, from).AssertRowsAffected(1)
		p.AddScoreOne(amount, to).AssertRowsAffected(1)
		return nil
	}, runtime.WithTxRetry(pol))
}
func (a *pgqAPI) OrgStats(ctx context.Context, minItems int64, status string) ([]orgStat, error) {
	rows, err := a.q.OrgStats(ctx, a.q.TopOrgsQuery(minItems), status)
	out := make([]orgStat, len(rows))
	for i, r := range rows {
		out[i] = orgStat{r.OrgID, r.N, r.Total}
	}
	return out, err
}
func (a *pgqAPI) EachByIDs(ctx context.Context, ids []int64, fn func(item) bool) error {
	for r, err := range a.q.ItemsByIDsSeq(ctx, ids) {
		if err != nil {
			return err
		}
		if !fn(item{r.ID, r.OrgID, r.Name, r.Score, r.Status}) {
			return nil
		}
	}
	return nil
}
func (a *pgqAPI) CacheStats() runtime.CacheStats { return a.q.CacheStats() }
func (a *pgqAPI) Close() error                   { return a.q.Close() }
