package integration

// This file is mirrored to fuzz_adapter_fzmy_test.go with `fzpg` -> `fzmy` (see `make sync-adapters`).
// It adapts the generated fuzz package to the engine-agnostic fzAPI.

import (
	"context"
	"database/sql"
	"iter"

	"github.com/CAFxX/sqlgen/integration/fzpg"
	"github.com/CAFxX/sqlgen/runtime"
)

type fzpgAPI struct{ q *fzpg.Queries }

func newFzpgAPI(db *sql.DB, opts ...runtime.CacheOption) *fzpgAPI {
	return &fzpgAPI{q: fzpg.New(db, opts...)}
}

func (a *fzpgAPI) Close() error { return a.q.Close() }

func (a *fzpgAPI) Tx(tx *sql.Tx) fzAPI { return &fzpgAPI{q: a.q.WithTx(tx)} }

func toNewFzpg(r fzRow) fzpg.FzNew {
	return fzpg.FzNew{ID: r.ID, Grp: r.Grp, K1: r.K[0], K2: r.K[1], K3: r.K[2], K4: r.K[3], K5: r.K[4],
		K6: r.K[5], K7: r.K[6], K8: r.K[7], Tag: r.Tag, Tag2: r.Tag2, Note: r.Note}
}

func toNewsFzpg(rows []fzRow) []fzpg.FzNew {
	out := make([]fzpg.FzNew, len(rows))
	for i, r := range rows {
		out[i] = toNewFzpg(r)
	}
	return out
}

func fromRowFzpg(r fzpg.FzRow) fzRow {
	return fzRow{ID: r.ID, Grp: r.Grp, K: [8]int64{r.K1, r.K2, r.K3, r.K4, r.K5, r.K6, r.K7, r.K8},
		Tag: r.Tag, Tag2: r.Tag2, Note: r.Note}
}

func fromRowsFzpg(rows []fzpg.FzRow, err error) ([]fzRow, error) {
	out := make([]fzRow, len(rows))
	for i, r := range rows {
		out[i] = fromRowFzpg(r)
	}
	return out, err
}

func toRowFzpg(r fzRow) fzpg.FzRow {
	return fzpg.FzRow{ID: r.ID, Grp: r.Grp, K1: r.K[0], K2: r.K[1], K3: r.K[2], K4: r.K[3], K5: r.K[4],
		K6: r.K[5], K7: r.K[6], K8: r.K[7], Tag: r.Tag, Tag2: r.Tag2, Note: r.Note}
}

func rowsAffectedFzpg(res sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func outsFzpg(rows []fzpg.FzOut, err error) ([]fzOut, error) {
	out := make([]fzOut, len(rows))
	for i, r := range rows {
		out[i] = fzOut{r.ID, r.K1}
	}
	return out, err
}

func varFzpg(
	into func(dst []fzpg.FzRow) ([]fzpg.FzRow, error),
	seq iter.Seq2[fzpg.FzRow, error],
	each func(func(fzpg.FzRow) error) error,
) fzVar {
	return fzVar{
		Into: func(prefix, spare int) ([]fzRow, error) {
			return fromRowsFzpg(into(make([]fzpg.FzRow, prefix, prefix+spare)))
		},
		Seq: func() iter.Seq2[fzRow, error] {
			return func(yield func(fzRow, error) bool) {
				for r, err := range seq {
					if !yield(fromRowFzpg(r), err) {
						return
					}
				}
			}
		},
		Each: func(fn func(fzRow) error) error {
			return each(func(r fzpg.FzRow) error { return fn(fromRowFzpg(r)) })
		},
	}
}

func searchParamsFzpg(s fzSearchArgs) fzpg.FzSearchParams {
	p := fzpg.FzSearchParams{Lo: s.Lo, Grp: s.Grp, Grps: s.Grps, Tags: s.Tags, NoteLike: s.NoteLike, Hi: s.Hi,
		ByGrp: s.ByGrp, ByGrps: s.ByGrps, ByTags: s.ByTags, FilterNote: s.FilterNote, NoteNull: s.NoteNull,
		ByPairs: s.ByPairs, Narrow: s.Narrow, HiK4: s.HiK4, SelectCols: s.SelectCols}
	for _, pr := range s.Pairs {
		p.Pairs = append(p.Pairs, fzpg.FzPair{Grp: pr[0], K2: pr[1]})
	}
	return p
}

func pageParamsFzpg(s fzPageArgs) fzpg.FzPageParams {
	var terms []fzpg.FzPageSortTerm
	for _, t := range s.Terms {
		if t.Desc {
			terms = append(terms, fzpg.FzPageSortCol(t.Col).Desc())
		} else {
			terms = append(terms, fzpg.FzPageSortCol(t.Col).Asc())
		}
	}
	return fzpg.FzPageParams{Lo: s.Lo, Grps: s.Grps, After: s.After, Sub: buildSubFzpg(s.Sub), ExTag: s.ExTag,
		Sort: fzpg.OrderFzPage(terms...), Limit: s.Limit, ByGrps: s.ByGrps, InSub: s.InSub, SkipTag: s.SkipTag,
		SelectCols: s.Cols}
}

func (a *fzpgAPI) SearchV(ctx context.Context, s fzSearchArgs) fzVar {
	p := searchParamsFzpg(s)
	return varFzpg(func(dst []fzpg.FzRow) ([]fzpg.FzRow, error) { return runtime.AppendTo(dst, a.q.FzSearchSeq(ctx, p)) },
		a.q.FzSearchSeq(ctx, p), func(fn func(fzpg.FzRow) error) error { return runtime.Each(a.q.FzSearchSeq(ctx, p), fn) })
}

func (a *fzpgAPI) PageV(ctx context.Context, s fzPageArgs) fzVar {
	p := pageParamsFzpg(s)
	return varFzpg(func(dst []fzpg.FzRow) ([]fzpg.FzRow, error) { return runtime.AppendTo(dst, a.q.FzPageSeq(ctx, p)) },
		a.q.FzPageSeq(ctx, p), func(fn func(fzpg.FzRow) error) error { return runtime.Each(a.q.FzPageSeq(ctx, p), fn) })
}

func (a *fzpgAPI) ByIDsV(ctx context.Context, ids []int64) fzVar {
	return varFzpg(func(dst []fzpg.FzRow) ([]fzpg.FzRow, error) { return runtime.AppendTo(dst, a.q.FzByIDsSeq(ctx, ids)) },
		a.q.FzByIDsSeq(ctx, ids), func(fn func(fzpg.FzRow) error) error { return runtime.Each(a.q.FzByIDsSeq(ctx, ids), fn) })
}

func (a *fzpgAPI) ByKeysV(ctx context.Context, keys [][2]int64) fzVar {
	ks := make([]fzpg.FzKey, len(keys))
	for i, k := range keys {
		ks[i] = fzpg.FzKey{Grp: k[0], K1: k[1]}
	}
	return varFzpg(func(dst []fzpg.FzRow) ([]fzpg.FzRow, error) { return runtime.AppendTo(dst, a.q.FzByKeysSeq(ctx, ks)) },
		a.q.FzByKeysSeq(ctx, ks), func(fn func(fzpg.FzRow) error) error { return runtime.Each(a.q.FzByKeysSeq(ctx, ks), fn) })
}

func fromTRowsFzpg(rows []fzpg.FzTypedRow, err error) ([]fzTRow, error) {
	out := make([]fzTRow, len(rows))
	for i, r := range rows {
		out[i] = fzTRow{r.ID, r.I32, r.F64, r.Flag, r.Bin, r.Ni}
	}
	return out, err
}

func (a *fzpgAPI) TypedInsert(ctx context.Context, rows []fzTRow) (int64, error) {
	in := make([]fzpg.FzTypedNew, len(rows))
	for i, r := range rows {
		in[i] = fzpg.FzTypedNew{ID: r.ID, I32: r.I32, F64: r.F64, Flag: r.Flag, Bin: r.Blob, Ni: r.NI}
	}
	return rowsAffectedFzpg(a.q.FzTypedInsert(ctx, in))
}

func (a *fzpgAPI) TypedSearch(ctx context.Context, s fzTSearch) ([]fzTRow, error) {
	return fromTRowsFzpg(a.q.FzTypedSearch(ctx, fzpg.FzTypedSearchParams{Lo: s.Lo, I32s: s.I32s, F64s: s.F64s,
		Flag: s.Flag, Bin: s.Blob, NiMin: s.NiMin, ByI32s: s.ByI32s, ByF64s: s.ByF64s, ByFlag: s.ByFlag,
		ByBin: s.ByBlob, NiAtLeast: s.NiAtLeast}))
}

func (a *fzpgAPI) TypedPage(ctx context.Context, terms []fzTerm, after runtime.Seek, limit int64) ([]fzTRow, error) {
	var ts []fzpg.FzTypedPageSortTerm
	for _, t := range terms {
		if t.Desc {
			ts = append(ts, fzpg.FzTypedPageSortCol(t.Col).Desc())
		} else {
			ts = append(ts, fzpg.FzTypedPageSortCol(t.Col).Asc())
		}
	}
	return fromTRowsFzpg(a.q.FzTypedPage(ctx, after, fzpg.OrderFzTypedPage(ts...), limit))
}

func (a *fzpgAPI) TypedCursor(r fzTRow) runtime.Seek {
	return fzpg.SeekFzTypedPage(fzpg.FzTypedRow{ID: r.ID, I32: r.I32, F64: r.F64, Flag: r.Flag, Bin: r.Blob, Ni: r.NI})
}

func (a *fzpgAPI) Insert(ctx context.Context, v fzIns, rows []fzRow) (int64, error) {
	if v.Order == 3 { // FzInsertLit: only id, grp, k1 and tag are parameters; the rest are literals in the template
		in := make([]fzpg.FzLit, len(rows))
		for i, r := range rows {
			in[i] = fzpg.FzLit{ID: r.ID, Grp: r.Grp, K1: r.K[0], Tag: r.Tag}
		}
		if v.Path == 0 {
			return rowsAffectedFzpg(a.q.FzInsertLit(ctx, in))
		}
		return rowsAffectedFzpg(a.q.FzInsertLitStream(ctx, func(yield func(fzpg.FzLit, error) bool) {
			for _, r := range in {
				if !yield(r, nil) {
					return
				}
			}
		}))
	}
	in := toNewsFzpg(rows)
	seq := func() iter.Seq2[fzpg.FzNew, error] {
		return func(yield func(fzpg.FzNew, error) bool) {
			for _, r := range in {
				if !yield(r, nil) {
					return
				}
			}
		}
	}
	type fn func(context.Context, []fzpg.FzNew) (sql.Result, error)
	type sfn func(context.Context, iter.Seq2[fzpg.FzNew, error]) (sql.Result, error)
	var plain fn
	var stream sfn
	switch v.Order {
	case 0:
		plain, stream = a.q.FzInsertAsc, a.q.FzInsertAscStream
	case 1:
		plain, stream = a.q.FzInsertDesc, a.q.FzInsertDescStream
	default:
		plain, stream = a.q.FzInsertRaw, a.q.FzInsertRawStream
	}
	if v.Path == 0 {
		return rowsAffectedFzpg(plain(ctx, in))
	}
	return rowsAffectedFzpg(stream(ctx, seq()))
}

// InsertStreamErr streams rows then fails with err after the first n: the error must come back unchanged.
func (a *fzpgAPI) InsertStreamErr(ctx context.Context, rows []fzRow, n int, err error) error {
	_, e := a.q.FzInsertRawStream(ctx, func(yield func(fzpg.FzNew, error) bool) {
		for i, r := range rows {
			if i == n {
				yield(fzpg.FzNew{}, err)
				return
			}
			if !yield(toNewFzpg(r), nil) {
				return
			}
		}
	})
	return e
}

func (a *fzpgAPI) Search(ctx context.Context, s fzSearchArgs) ([]fzRow, error) {
	return fromRowsFzpg(a.q.FzSearch(ctx, searchParamsFzpg(s)))
}

func (a *fzpgAPI) Narrow(ctx context.Context, minID int64, byGrp bool, grp int64, cols uint64) ([]fzRow, error) {
	rows, err := a.q.FzNarrow(ctx, fzpg.FzNarrowParams{MinID: minID, Grp: grp, ByGrp: byGrp, SelectCols: cols})
	out := make([]fzRow, len(rows))
	for i, r := range rows {
		out[i] = fzRow{ID: r.ID, Grp: r.Grp, Tag: r.Tag, Note: r.Note}
		out[i].K[0], out[i].K[1] = r.K1, r.K2
	}
	return out, err
}

func buildSubFzpg(s *fzSub) runtime.Query {
	if s == nil {
		return runtime.Query{}
	}
	if s.Note {
		return fzpg.FzSubNoteQuery(fzpg.FzSubNoteParams{Inner: buildSubFzpg(s.Inner), K3max: s.K3Max, MaxK3: s.MaxK3})
	}
	return fzpg.FzSubGrpsQuery(fzpg.FzSubGrpsParams{Grps: s.Grps, K2min: s.K2Min, MinK2: s.MinK2})
}

func (a *fzpgAPI) Page(ctx context.Context, s fzPageArgs) ([]fzRow, error) {
	return fromRowsFzpg(a.q.FzPage(ctx, pageParamsFzpg(s)))
}

func (a *fzpgAPI) Cursor(r fzRow) runtime.Seek { return fzpg.SeekFzPage(toRowFzpg(r)) }

func (a *fzpgAPI) Agg(ctx context.Context, pick *fzSub, byTag bool, tag string) ([]fzAggRow, error) {
	rows, err := a.q.FzAgg(ctx, fzpg.FzAggParams{Pick: buildSubFzpg(pick), Tag: tag, ByTag: byTag})
	out := make([]fzAggRow, len(rows))
	for i, r := range rows {
		out[i] = fzAggRow{r.Grp, r.N, r.Total}
	}
	return out, err
}

func (a *fzpgAPI) Subs(ctx context.Context, sa, sb *fzSub, lo int64, excludeB, againA bool) ([]fzRow, error) {
	return fromRowsFzpg(a.q.FzSubs(ctx, fzpg.FzSubsParams{A: buildSubFzpg(sa), Lo: lo, B: buildSubFzpg(sb),
		ExcludeB: excludeB, AgainA: againA}))
}

func (a *fzpgAPI) Join(ctx context.Context, peerOff, minID int64, withPeer, byGrp bool, grp int64, cols uint64) ([]fzJoinRow, error) {
	rows, err := a.q.FzJoin(ctx, fzpg.FzJoinParams{PeerOff: peerOff, MinID: minID, Grp: grp, WithPeer: withPeer,
		ByGrp: byGrp, SelectCols: cols})
	out := make([]fzJoinRow, len(rows))
	for i, r := range rows {
		out[i] = fzJoinRow{r.ID, r.Grp, r.K1, r.PeerID, r.PeerTag}
	}
	return out, err
}

func (a *fzpgAPI) ByKeys(ctx context.Context, keys [][2]int64) ([]fzRow, error) {
	ks := make([]fzpg.FzKey, len(keys))
	for i, k := range keys {
		ks[i] = fzpg.FzKey{Grp: k[0], K1: k[1]}
	}
	return fromRowsFzpg(a.q.FzByKeys(ctx, ks))
}

func (a *fzpgAPI) ByIDs(ctx context.Context, ids []int64) ([]fzRow, error) {
	return fromRowsFzpg(a.q.FzByIDs(ctx, ids))
}

func (a *fzpgAPI) Bump(ctx context.Context, delta int64, ids []int64, grp *int64) (int64, error) {
	p := fzpg.FzBumpParams{Delta: delta, IDs: ids}
	if grp != nil {
		p.OnlyGrp, p.Grp = true, *grp
	}
	return rowsAffectedFzpg(a.q.FzBump(ctx, p))
}

func (a *fzpgAPI) Delete(ctx context.Context, ids []int64, tag *string) (int64, error) {
	p := fzpg.FzDeleteParams{IDs: ids}
	if tag != nil {
		p.OnlyTag, p.Tag = true, *tag
	}
	return rowsAffectedFzpg(a.q.FzDelete(ctx, p))
}

func (a *fzpgAPI) InsertRet(ctx context.Context, rows []fzRow, mode int) ([]fzOut, error) {
	src := func(yield func(fzpg.FzNew, error) bool) {
		for _, r := range rows {
			if !yield(toNewFzpg(r), nil) {
				return
			}
		}
	}
	switch mode {
	case 1:
		return outsFzpg(a.q.FzInsertRetStream(ctx, src))
	case 2:
		return outsFzpg(runtime.Collect(a.q.FzInsertRetStreamSeq(ctx, src)))
	}
	return outsFzpg(a.q.FzInsertRet(ctx, toNewsFzpg(rows)))
}

func (a *fzpgAPI) DeleteRet(ctx context.Context, ids []int64, tag *string) ([]fzOut, error) {
	p := fzpg.FzDeleteRetParams{IDs: ids}
	if tag != nil {
		p.OnlyTag, p.Tag = true, *tag
	}
	return outsFzpg(a.q.FzDeleteRet(ctx, p))
}
