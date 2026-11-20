package integration

// This file is mirrored to fuzz_adapter_fzmy_test.go with `fzsl` -> `fzmy` (see `make sync-adapters`).
// It adapts the generated fuzz package to the engine-agnostic fzAPI.

import (
	"context"
	"database/sql"
	"iter"

	"github.com/CAFxX/sqlgen/integration/fzsl"
	"github.com/CAFxX/sqlgen/runtime"
)

type fzslAPI struct{ q *fzsl.Queries }

func newFzslAPI(db *sql.DB, opts ...runtime.CacheOption) *fzslAPI {
	return &fzslAPI{q: fzsl.New(db, opts...)}
}

func (a *fzslAPI) Close() error { return a.q.Close() }

func (a *fzslAPI) Tx(tx *sql.Tx) fzAPI { return &fzslAPI{q: a.q.WithTx(tx)} }

func toNewFzsl(r fzRow) fzsl.FzNew {
	return fzsl.FzNew{ID: r.ID, Grp: r.Grp, K1: r.K[0], K2: r.K[1], K3: r.K[2], K4: r.K[3], K5: r.K[4],
		K6: r.K[5], K7: r.K[6], K8: r.K[7], Tag: r.Tag, Tag2: r.Tag2, Note: r.Note}
}

func toNewsFzsl(rows []fzRow) []fzsl.FzNew {
	out := make([]fzsl.FzNew, len(rows))
	for i, r := range rows {
		out[i] = toNewFzsl(r)
	}
	return out
}

func fromRowFzsl(r fzsl.FzRow) fzRow {
	return fzRow{ID: r.ID, Grp: r.Grp, K: [8]int64{r.K1, r.K2, r.K3, r.K4, r.K5, r.K6, r.K7, r.K8},
		Tag: r.Tag, Tag2: r.Tag2, Note: r.Note}
}

func fromRowsFzsl(rows []fzsl.FzRow, err error) ([]fzRow, error) {
	out := make([]fzRow, len(rows))
	for i, r := range rows {
		out[i] = fromRowFzsl(r)
	}
	return out, err
}

func toRowFzsl(r fzRow) fzsl.FzRow {
	return fzsl.FzRow{ID: r.ID, Grp: r.Grp, K1: r.K[0], K2: r.K[1], K3: r.K[2], K4: r.K[3], K5: r.K[4],
		K6: r.K[5], K7: r.K[6], K8: r.K[7], Tag: r.Tag, Tag2: r.Tag2, Note: r.Note}
}

func rowsAffectedFzsl(res sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func outsFzsl(rows []fzsl.FzOut, err error) ([]fzOut, error) {
	out := make([]fzOut, len(rows))
	for i, r := range rows {
		out[i] = fzOut{r.ID, r.K1}
	}
	return out, err
}

func varFzsl(
	into func(dst []fzsl.FzRow) ([]fzsl.FzRow, error),
	seq iter.Seq2[fzsl.FzRow, error],
	each func(func(fzsl.FzRow) error) error,
) fzVar {
	return fzVar{
		Into: func(prefix, spare int) ([]fzRow, error) {
			return fromRowsFzsl(into(make([]fzsl.FzRow, prefix, prefix+spare)))
		},
		Seq: func() iter.Seq2[fzRow, error] {
			return func(yield func(fzRow, error) bool) {
				for r, err := range seq {
					if !yield(fromRowFzsl(r), err) {
						return
					}
				}
			}
		},
		Each: func(fn func(fzRow) error) error {
			return each(func(r fzsl.FzRow) error { return fn(fromRowFzsl(r)) })
		},
	}
}

func searchParamsFzsl(s fzSearchArgs) fzsl.FzSearchParams {
	p := fzsl.FzSearchParams{Lo: s.Lo, Grp: s.Grp, Grps: s.Grps, Tags: s.Tags, NoteLike: s.NoteLike, Hi: s.Hi,
		ByGrp: s.ByGrp, ByGrps: s.ByGrps, ByTags: s.ByTags, FilterNote: s.FilterNote, NoteNull: s.NoteNull,
		ByPairs: s.ByPairs, Narrow: s.Narrow, HiK4: s.HiK4, SelectCols: s.SelectCols}
	for _, pr := range s.Pairs {
		p.Pairs = append(p.Pairs, fzsl.FzPair{Grp: pr[0], K2: pr[1]})
	}
	return p
}

func pageParamsFzsl(s fzPageArgs) fzsl.FzPageParams {
	var terms []fzsl.FzPageSortTerm
	for _, t := range s.Terms {
		if t.Desc {
			terms = append(terms, fzsl.FzPageSortCol(t.Col).Desc())
		} else {
			terms = append(terms, fzsl.FzPageSortCol(t.Col).Asc())
		}
	}
	return fzsl.FzPageParams{Lo: s.Lo, Grps: s.Grps, After: s.After, Sub: buildSubFzsl(s.Sub), ExTag: s.ExTag,
		Sort: fzsl.OrderFzPage(terms...), Limit: s.Limit, ByGrps: s.ByGrps, InSub: s.InSub, SkipTag: s.SkipTag,
		SelectCols: s.Cols}
}

func (a *fzslAPI) SearchV(ctx context.Context, s fzSearchArgs) fzVar {
	p := searchParamsFzsl(s)
	return varFzsl(func(dst []fzsl.FzRow) ([]fzsl.FzRow, error) { return a.q.FzSearchInto(ctx, dst, p) },
		a.q.FzSearchSeq(ctx, p), func(fn func(fzsl.FzRow) error) error { return a.q.FzSearchEach(ctx, p, fn) })
}

func (a *fzslAPI) PageV(ctx context.Context, s fzPageArgs) fzVar {
	p := pageParamsFzsl(s)
	return varFzsl(func(dst []fzsl.FzRow) ([]fzsl.FzRow, error) { return a.q.FzPageInto(ctx, dst, p) },
		a.q.FzPageSeq(ctx, p), func(fn func(fzsl.FzRow) error) error { return a.q.FzPageEach(ctx, p, fn) })
}

func (a *fzslAPI) ByIDsBatchV(ctx context.Context, ids []int64) fzVar {
	return varFzsl(func(dst []fzsl.FzRow) ([]fzsl.FzRow, error) { return a.q.FzByIDsBatchInto(ctx, dst, ids) },
		a.q.FzByIDsBatchSeq(ctx, ids), func(fn func(fzsl.FzRow) error) error { return a.q.FzByIDsBatchEach(ctx, ids, fn) })
}

func (a *fzslAPI) ByKeysBatchV(ctx context.Context, keys [][2]int64) fzVar {
	ks := make([]fzsl.FzKey, len(keys))
	for i, k := range keys {
		ks[i] = fzsl.FzKey{Grp: k[0], K1: k[1]}
	}
	return varFzsl(func(dst []fzsl.FzRow) ([]fzsl.FzRow, error) { return a.q.FzByKeysBatchInto(ctx, dst, ks) },
		a.q.FzByKeysBatchSeq(ctx, ks), func(fn func(fzsl.FzRow) error) error { return a.q.FzByKeysBatchEach(ctx, ks, fn) })
}

func fromTRowsFzsl(rows []fzsl.FzTypedRow, err error) ([]fzTRow, error) {
	out := make([]fzTRow, len(rows))
	for i, r := range rows {
		out[i] = fzTRow{r.ID, r.I32, r.F64, r.Flag, r.Bin, r.Ni}
	}
	return out, err
}

func (a *fzslAPI) TypedInsert(ctx context.Context, rows []fzTRow, batch bool) (int64, error) {
	in := make([]fzsl.FzTypedNew, len(rows))
	for i, r := range rows {
		in[i] = fzsl.FzTypedNew{ID: r.ID, I32: r.I32, F64: r.F64, Flag: r.Flag, Bin: r.Blob, Ni: r.NI}
	}
	if batch {
		return rowsAffectedFzsl(a.q.FzTypedInsertBatch(ctx, in))
	}
	return rowsAffectedFzsl(a.q.FzTypedInsert(ctx, in))
}

func (a *fzslAPI) TypedSearch(ctx context.Context, s fzTSearch) ([]fzTRow, error) {
	return fromTRowsFzsl(a.q.FzTypedSearch(ctx, fzsl.FzTypedSearchParams{Lo: s.Lo, I32s: s.I32s, F64s: s.F64s,
		Flag: s.Flag, Bin: s.Blob, NiMin: s.NiMin, ByI32s: s.ByI32s, ByF64s: s.ByF64s, ByFlag: s.ByFlag,
		ByBin: s.ByBlob, NiAtLeast: s.NiAtLeast}))
}

func (a *fzslAPI) TypedPage(ctx context.Context, terms []fzTerm, after runtime.Seek, limit int64) ([]fzTRow, error) {
	var ts []fzsl.FzTypedPageSortTerm
	for _, t := range terms {
		if t.Desc {
			ts = append(ts, fzsl.FzTypedPageSortCol(t.Col).Desc())
		} else {
			ts = append(ts, fzsl.FzTypedPageSortCol(t.Col).Asc())
		}
	}
	return fromTRowsFzsl(a.q.FzTypedPage(ctx, after, fzsl.OrderFzTypedPage(ts...), limit))
}

func (a *fzslAPI) TypedCursor(r fzTRow) runtime.Seek {
	return fzsl.SeekFzTypedPage(fzsl.FzTypedRow{ID: r.ID, I32: r.I32, F64: r.F64, Flag: r.Flag, Bin: r.Blob, Ni: r.NI})
}

func (a *fzslAPI) Insert(ctx context.Context, v fzIns, rows []fzRow) (int64, error) {
	if v.Order == 3 { // FzInsertLit: only id, grp, k1 and tag are parameters; the rest are literals in the template
		in := make([]fzsl.FzLit, len(rows))
		for i, r := range rows {
			in[i] = fzsl.FzLit{ID: r.ID, Grp: r.Grp, K1: r.K[0], Tag: r.Tag}
		}
		switch v.Path {
		case 0:
			return rowsAffectedFzsl(a.q.FzInsertLit(ctx, in))
		case 1:
			return rowsAffectedFzsl(a.q.FzInsertLitBatch(ctx, in))
		}
		return rowsAffectedFzsl(a.q.FzInsertLitStream(ctx, func(yield func(fzsl.FzLit, error) bool) {
			for _, r := range in {
				if !yield(r, nil) {
					return
				}
			}
		}))
	}
	in := toNewsFzsl(rows)
	seq := func() iter.Seq2[fzsl.FzNew, error] {
		return func(yield func(fzsl.FzNew, error) bool) {
			for _, r := range in {
				if !yield(r, nil) {
					return
				}
			}
		}
	}
	type fn func(context.Context, []fzsl.FzNew) (sql.Result, error)
	type sfn func(context.Context, iter.Seq2[fzsl.FzNew, error]) (sql.Result, error)
	var plain, batch fn
	var stream sfn
	switch v.Order {
	case 0:
		plain, batch, stream = a.q.FzInsertAsc, a.q.FzInsertAscBatch, a.q.FzInsertAscStream
	case 1:
		plain, batch, stream = a.q.FzInsertDesc, a.q.FzInsertDescBatch, a.q.FzInsertDescStream
	default:
		plain, batch, stream = a.q.FzInsertRaw, a.q.FzInsertRawBatch, a.q.FzInsertRawStream
	}
	switch v.Path {
	case 0:
		return rowsAffectedFzsl(plain(ctx, in))
	case 1:
		return rowsAffectedFzsl(batch(ctx, in))
	}
	return rowsAffectedFzsl(stream(ctx, seq()))
}

// InsertStreamErr streams rows then fails with err after the first n: the error must come back unchanged.
func (a *fzslAPI) InsertStreamErr(ctx context.Context, rows []fzRow, n int, err error) error {
	_, e := a.q.FzInsertRawStream(ctx, func(yield func(fzsl.FzNew, error) bool) {
		for i, r := range rows {
			if i == n {
				yield(fzsl.FzNew{}, err)
				return
			}
			if !yield(toNewFzsl(r), nil) {
				return
			}
		}
	})
	return e
}

func (a *fzslAPI) Search(ctx context.Context, s fzSearchArgs) ([]fzRow, error) {
	return fromRowsFzsl(a.q.FzSearch(ctx, searchParamsFzsl(s)))
}

func (a *fzslAPI) Narrow(ctx context.Context, minID int64, byGrp bool, grp int64, cols uint64) ([]fzRow, error) {
	rows, err := a.q.FzNarrow(ctx, fzsl.FzNarrowParams{MinID: minID, Grp: grp, ByGrp: byGrp, SelectCols: cols})
	out := make([]fzRow, len(rows))
	for i, r := range rows {
		out[i] = fzRow{ID: r.ID, Grp: r.Grp, Tag: r.Tag, Note: r.Note}
		out[i].K[0], out[i].K[1] = r.K1, r.K2
	}
	return out, err
}

func buildSubFzsl(s *fzSub) runtime.Query {
	if s == nil {
		return runtime.Query{}
	}
	if s.Note {
		return fzsl.FzSubNoteQuery(fzsl.FzSubNoteParams{Inner: buildSubFzsl(s.Inner), K3max: s.K3Max, MaxK3: s.MaxK3})
	}
	return fzsl.FzSubGrpsQuery(fzsl.FzSubGrpsParams{Grps: s.Grps, K2min: s.K2Min, MinK2: s.MinK2})
}

func (a *fzslAPI) Page(ctx context.Context, s fzPageArgs) ([]fzRow, error) {
	return fromRowsFzsl(a.q.FzPage(ctx, pageParamsFzsl(s)))
}

func (a *fzslAPI) Cursor(r fzRow) runtime.Seek { return fzsl.SeekFzPage(toRowFzsl(r)) }

func (a *fzslAPI) Agg(ctx context.Context, pick *fzSub, byTag bool, tag string) ([]fzAggRow, error) {
	rows, err := a.q.FzAgg(ctx, fzsl.FzAggParams{Pick: buildSubFzsl(pick), Tag: tag, ByTag: byTag})
	out := make([]fzAggRow, len(rows))
	for i, r := range rows {
		out[i] = fzAggRow{r.Grp, r.N, r.Total}
	}
	return out, err
}

func (a *fzslAPI) Subs(ctx context.Context, sa, sb *fzSub, lo int64, excludeB, againA bool) ([]fzRow, error) {
	return fromRowsFzsl(a.q.FzSubs(ctx, fzsl.FzSubsParams{A: buildSubFzsl(sa), Lo: lo, B: buildSubFzsl(sb),
		ExcludeB: excludeB, AgainA: againA}))
}

func (a *fzslAPI) Join(ctx context.Context, peerOff, minID int64, withPeer, byGrp bool, grp int64, cols uint64) ([]fzJoinRow, error) {
	rows, err := a.q.FzJoin(ctx, fzsl.FzJoinParams{PeerOff: peerOff, MinID: minID, Grp: grp, WithPeer: withPeer,
		ByGrp: byGrp, SelectCols: cols})
	out := make([]fzJoinRow, len(rows))
	for i, r := range rows {
		out[i] = fzJoinRow{r.ID, r.Grp, r.K1, r.PeerID, r.PeerTag}
	}
	return out, err
}

func (a *fzslAPI) ByKeys(ctx context.Context, keys [][2]int64, batch bool) ([]fzRow, error) {
	ks := make([]fzsl.FzKey, len(keys))
	for i, k := range keys {
		ks[i] = fzsl.FzKey{Grp: k[0], K1: k[1]}
	}
	if batch {
		return fromRowsFzsl(a.q.FzByKeysBatch(ctx, ks))
	}
	return fromRowsFzsl(a.q.FzByKeys(ctx, ks))
}

func (a *fzslAPI) ByIDs(ctx context.Context, ids []int64, batch bool) ([]fzRow, error) {
	if batch {
		return fromRowsFzsl(a.q.FzByIDsBatch(ctx, ids))
	}
	return fromRowsFzsl(a.q.FzByIDs(ctx, ids))
}

func (a *fzslAPI) Bump(ctx context.Context, delta int64, ids []int64, grp *int64, batch bool) (int64, error) {
	p := fzsl.FzBumpParams{Delta: delta, IDs: ids}
	if grp != nil {
		p.OnlyGrp, p.Grp = true, *grp
	}
	if batch {
		return rowsAffectedFzsl(a.q.FzBumpBatch(ctx, p))
	}
	return rowsAffectedFzsl(a.q.FzBump(ctx, p))
}

func (a *fzslAPI) Delete(ctx context.Context, ids []int64, tag *string, batch bool) (int64, error) {
	p := fzsl.FzDeleteParams{IDs: ids}
	if tag != nil {
		p.OnlyTag, p.Tag = true, *tag
	}
	if batch {
		return rowsAffectedFzsl(a.q.FzDeleteBatch(ctx, p))
	}
	return rowsAffectedFzsl(a.q.FzDelete(ctx, p))
}

func (a *fzslAPI) InsertRet(ctx context.Context, rows []fzRow, mode int) ([]fzOut, error) {
	switch mode {
	case 1:
		return outsFzsl(a.q.FzInsertRetBatch(ctx, toNewsFzsl(rows)))
	case 2:
		return outsFzsl(a.q.FzInsertRetStream(ctx, func(yield func(fzsl.FzNew, error) bool) {
			for _, r := range rows {
				if !yield(toNewFzsl(r), nil) {
					return
				}
			}
		}))
	}
	return outsFzsl(a.q.FzInsertRet(ctx, toNewsFzsl(rows)))
}

func (a *fzslAPI) DeleteRet(ctx context.Context, ids []int64, tag *string, batch bool) ([]fzOut, error) {
	p := fzsl.FzDeleteRetParams{IDs: ids}
	if tag != nil {
		p.OnlyTag, p.Tag = true, *tag
	}
	if batch {
		return outsFzsl(a.q.FzDeleteRetBatch(ctx, p))
	}
	return outsFzsl(a.q.FzDeleteRet(ctx, p))
}
