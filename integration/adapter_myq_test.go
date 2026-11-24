package integration

// This file is mirrored to adapter_myq_test.go with `myq` -> `myq` (see `make sync-adapters`).
// It adapts the generated package to the engine-agnostic api used by the shared suite.

import (
	"context"
	"database/sql"
	"iter"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/runtime"
)

type myqAPI struct{ q *myq.Queries }

func newMyqAPI(db *sql.DB, opts ...runtime.CacheOption) api { return &myqAPI{q: myq.New(db, opts...)} }

func toNewMyq(rows []item) []myq.NewItem {
	out := make([]myq.NewItem, len(rows))
	for i, r := range rows {
		out[i] = myq.NewItem{ID: r.ID, OrgID: r.OrgID, Name: r.Name, Score: r.Score, Status: r.Status}
	}
	return out
}

func fromItemsMyq(rows []myq.Item) []item {
	out := make([]item, len(rows))
	for i, r := range rows {
		out[i] = item{r.ID, r.OrgID, r.Name, r.Score, r.Status}
	}
	return out
}

func (a *myqAPI) Insert(ctx context.Context, rows []item) error {
	_, err := a.q.InsertItems(ctx, toNewMyq(rows))
	return err
}
func (a *myqAPI) InsertStream(ctx context.Context, rows iter.Seq[item]) error {
	_, err := a.q.InsertItemsStream(ctx, func(yield func(myq.NewItem, error) bool) {
		for r := range rows {
			if !yield(myq.NewItem{ID: r.ID, OrgID: r.OrgID, Name: r.Name, Score: r.Score, Status: r.Status}, nil) {
				return
			}
		}
	})
	return err
}
func (a *myqAPI) Get(ctx context.Context, id int64) (item, error) {
	r, err := a.q.GetItem(ctx, id)
	return item{r.ID, r.OrgID, r.Name, r.Score, r.Status}, err
}
func (a *myqAPI) ByIDs(ctx context.Context, ids []int64) ([]item, error) {
	r, err := a.q.ItemsByIDs(ctx, ids)
	return fromItemsMyq(r), err
}
func toSearchParamsMyq(s searchArgs) myq.SearchItemsParams {
	p := myq.SearchItemsParams{MinScore: s.MinScore}
	if s.OrgID != nil {
		p.FilterOrg, p.OrgID = true, *s.OrgID
	}
	if s.Statuses != nil {
		p.FilterStatuses, p.Statuses = true, s.Statuses
	}
	if s.NamePrefix != nil {
		p.FilterNamePrefix, p.NameLike = true, *s.NamePrefix+"%"
	}
	return p
}

func (a *myqAPI) Search(ctx context.Context, s searchArgs) ([]item, error) {
	r, err := a.q.SearchItems(ctx, toSearchParamsMyq(s))
	return fromItemsMyq(r), err
}

// ReadInTx runs a search and a bulk lookup inside one transaction (so both go through the
// transaction-scoped statement cache).
func (a *myqAPI) ReadInTx(ctx context.Context, s searchArgs, idl []int64) (found, byID []item, err error) {
	err = a.q.InTx(ctx, nil, func(q *myq.Queries) error {
		r1, err := q.SearchItems(ctx, toSearchParamsMyq(s))
		if err != nil {
			return err
		}
		r2, err := q.ItemsByIDs(ctx, idl)
		if err != nil {
			return err
		}
		found, byID = fromItemsMyq(r1), fromItemsMyq(r2)
		return nil
	})
	return
}
func (a *myqAPI) Page(ctx context.Context, orgID int64, scoreDesc, idDesc bool, limit int64) ([]item, error) {
	sc, id := myq.PageItemsOrderByScore.Asc(), myq.PageItemsOrderByID.Asc()
	if scoreDesc {
		sc = myq.PageItemsOrderByScore.Desc()
	}
	if idDesc {
		id = myq.PageItemsOrderByID.Desc()
	}
	order := myq.OrderPageItems(sc, id)
	var all []item
	var cursor runtime.Seek
	for {
		page, err := a.q.PageItems(ctx, myq.PageItemsParams{OrgID: orgID, After: cursor, Sort: order, Limit: limit})
		if err != nil {
			return nil, err
		}
		for _, r := range page {
			all = append(all, item{r.ID, r.OrgID, r.Name, r.Score, r.Status})
		}
		if int64(len(page)) < limit {
			return all, nil
		}
		cursor = myq.SeekPageItems(page[len(page)-1])
	}
}
func (a *myqAPI) AddScore(ctx context.Context, orgID, delta int64, ids []int64) (int64, error) {
	res, err := a.q.AddScore(ctx, delta, ids, orgID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (a *myqAPI) Delete(ctx context.Context, ids []int64) (int64, error) {
	res, err := a.q.DeleteItems(ctx, ids)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (a *myqAPI) Transfer(ctx context.Context, first, second, amount int64, pol runtime.TxRetryPolicy) error {
	return a.q.InTx(ctx, nil, func(q *myq.Queries) error {
		if _, err := q.AddScoreOne(ctx, -amount, first); err != nil {
			return err
		}
		_, err := q.AddScoreOne(ctx, amount, second)
		return err
	}, runtime.WithTxRetry(pol))
}
func (a *myqAPI) PipelineTransfer(ctx context.Context, from, to, amount int64, pol runtime.TxRetryPolicy) error {
	return a.q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
		p.AddScoreOne(-amount, from).AssertRowsAffected(1)
		p.AddScoreOne(amount, to).AssertRowsAffected(1)
		return nil
	}, runtime.WithTxRetry(pol))
}
func (a *myqAPI) OrgStats(ctx context.Context, minItems int64, status string) ([]orgStat, error) {
	rows, err := a.q.OrgStats(ctx, myq.TopOrgsQuery(minItems), status)
	out := make([]orgStat, len(rows))
	for i, r := range rows {
		out[i] = orgStat{r.OrgID, r.N, r.Total}
	}
	return out, err
}
func (a *myqAPI) EachByIDs(ctx context.Context, ids []int64, fn func(item) bool) error {
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
func (a *myqAPI) CacheStats() runtime.CacheStats { return a.q.CacheStats() }
func (a *myqAPI) Close() error                   { return a.q.Close() }
