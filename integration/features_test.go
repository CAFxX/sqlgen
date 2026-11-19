package integration

import (
	"context"
	"database/sql"
	"iter"
	"slices"
	"testing"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/integration/pgq"
)

func pgOnly(t *testing.T, e engine) {
	t.Helper()
	if !e.isPG() {
		t.Skip("RETURNING on INSERT/UPDATE/DELETE and ON CONFLICT are exercised on the PostgreSQL-protocol engines")
	}
}

func newItems(rows []item) []pgq.NewItem {
	out := make([]pgq.NewItem, len(rows))
	for i, r := range rows {
		out[i] = pgq.NewItem{ID: r.ID, OrgID: r.OrgID, Name: r.Name, Score: r.Score, Status: r.Status}
	}
	return out
}

func writtenIDs(w []pgq.Written) []int64 {
	out := make([]int64, len(w))
	for i, r := range w {
		out[i] = r.ID
	}
	return out
}

func collectSeq(t *testing.T, seq iter.Seq2[pgq.Written, error]) []pgq.Written {
	t.Helper()
	var out []pgq.Written
	for w, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, w)
	}
	return out
}

// Every way of consuming a batched, presorted INSERT ... ON CONFLICT ... RETURNING must yield the same
// rows, in the presorted execution order, without mutating the caller's slice, across chunk boundaries
// and with explicit ::type casts on the tuple fields.
func TestRealEngineReturningBatchVariantsAgree(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		pgOnly(t, e)
		setupItems(t, db)
		ctx := ctxT(t)
		q := pgq.New(db)
		defer q.Close()

		const n = 9000 // > one chunk (16384 params / 5 columns)
		type variant struct {
			name string
			run  func(rows []pgq.NewItem) ([]pgq.Written, error)
		}
		variants := []variant{
			{"Batch", func(r []pgq.NewItem) ([]pgq.Written, error) { return q.UpsertItemsReturningBatch(ctx, r) }},
			{"BatchInto", func(r []pgq.NewItem) ([]pgq.Written, error) {
				return q.UpsertItemsReturningBatchInto(ctx, make([]pgq.Written, 0, 4), r)
			}},
			{"BatchSeq", func(r []pgq.NewItem) ([]pgq.Written, error) {
				return collectSeq(t, q.UpsertItemsReturningBatchSeq(ctx, r)), nil
			}},
			{"BatchEach", func(r []pgq.NewItem) (out []pgq.Written, err error) {
				err = q.UpsertItemsReturningBatchEach(ctx, r, func(w pgq.Written) error { out = append(out, w); return nil })
				return
			}},
			{"Stream", func(r []pgq.NewItem) ([]pgq.Written, error) {
				return q.UpsertItemsReturningStream(ctx, func(y func(pgq.NewItem, error) bool) {
					for _, it := range r {
						if !y(it, nil) {
							return
						}
					}
				})
			}},
			{"StreamSeq", func(r []pgq.NewItem) ([]pgq.Written, error) {
				return collectSeq(t, q.UpsertItemsReturningStreamSeq(ctx, func(y func(pgq.NewItem, error) bool) {
					for _, it := range r {
						if !y(it, nil) {
							return
						}
					}
				})), nil
			}},
		}
		for vi, v := range variants {
			first := int64(vi)*100000 + 1
			all := genItems(first, n)
			in := newItems(shuffled(all, uint64(vi+1)))
			before := slices.Clone(in)
			got, err := v.run(in)
			if err != nil {
				t.Fatalf("%s: %v", v.name, err)
			}
			if !slices.Equal(in, before) {
				t.Fatalf("%s mutated the caller's slice", v.name)
			}
			if len(got) != n {
				t.Fatalf("%s: returned %d rows, want %d", v.name, len(got), n)
			}
			// Streams are chunked as they arrive and so are only presorted within a chunk; the batch
			// variants presort globally before chunking.
			if v.name[:5] == "Batch" && !slices.IsSorted(writtenIDs(got)) {
				t.Fatalf("%s: rows not returned in global presorted (ascending id) order", v.name)
			}
			byID := map[int64]int64{}
			for _, w := range got {
				byID[w.ID] = w.Score
			}
			for _, it := range all {
				if s, ok := byID[it.ID]; !ok || s != it.Score {
					t.Fatalf("%s: id %d: got score %d (present=%v), want %d", v.name, it.ID, s, ok, it.Score)
				}
			}
		}

		// ON CONFLICT DO UPDATE path: re-upsert an overlapping window; scores accumulate.
		overlap := genItems(1, 500) // ids already written by the first variant
		for i := range overlap {
			overlap[i].Score = 1000
		}
		got, err := q.UpsertItemsReturningBatch(ctx, newItems(shuffled(overlap, 7)))
		if err != nil {
			t.Fatal(err)
		}
		base := genItems(1, 500)
		for i, w := range got {
			if w.ID != int64(i+1) || w.Score != base[i].Score+1000 {
				t.Fatalf("upsert conflict row %d: got %+v, want id=%d score=%d", i, w, i+1, base[i].Score+1000)
			}
		}
	})
}

func TestRealEngineReturningUpdateDelete(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		pgOnly(t, e)
		setupItems(t, db)
		ctx := ctxT(t)
		q := pgq.New(db)
		defer q.Close()
		all := genItems(1, 40000)
		if _, err := q.InsertItemsBatch(ctx, newItems(shuffled(all, 3))); err != nil {
			t.Fatal(err)
		}

		// 30000 ids (> one chunk), some of which do not exist: only existing rows come back.
		pick := make([]int64, 0, 30000)
		for i := int64(1); i <= 30000; i++ {
			pick = append(pick, (i*7)%45000+1)
		}
		pickCopy := slices.Clone(pick)
		got, err := q.BumpScoresReturningBatch(ctx, 5, pick)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(pick, pickCopy) {
			t.Fatal("BumpScoresReturningBatch mutated its ids")
		}
		want := map[int64]int64{}
		for _, id := range pick {
			if id <= 40000 {
				want[id] = all[id-1].Score + 5
			}
		}
		if len(got) != len(want) {
			t.Fatalf("returned %d rows, want %d", len(got), len(want))
		}
		for _, w := range got {
			if want[w.ID] != w.Score {
				t.Fatalf("id %d score %d, want %d", w.ID, w.Score, want[w.ID])
			}
		}

		// DELETE ... WHERE id IN (... DESC) RETURNING: the presort orders the bound parameters only; the
		// engine chooses the order rows are visited and returned, so compare as a set.
		del := []int64{5, 99, 1, 40000, 12345, 7}
		dgot, err := q.DeleteReturning(ctx, del)
		if err != nil {
			t.Fatal(err)
		}
		ids := writtenIDs(dgot)
		slices.Sort(ids)
		if !slices.Equal(ids, []int64{1, 5, 7, 99, 12345, 40000}) || countRows(t, db) != 40000-int64(len(del)) {
			t.Fatalf("deleted %v, rows left %d", ids, countRows(t, db))
		}
	})
}

// SelectCols must project exactly the requested columns; every other field stays zero.
func TestRealEngineSelectColsProjection(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		ctx := ctxT(t)
		a := newAPI(e, db)
		defer a.Close()
		all := genItems(1, 50)
		if err := a.Insert(ctx, all); err != nil {
			t.Fatal(err)
		}
		type proj struct {
			name  string
			mask  uint64
			check func(want, got item) bool
		}
		for _, p := range []proj{
			{"id", 1 << 0, func(w, g item) bool { return g == item{ID: w.ID} }},
			{"id+name", 1<<0 | 1<<2, func(w, g item) bool { return g == item{ID: w.ID, Name: w.Name} }},
			{"score+status", 1<<3 | 1<<4, func(w, g item) bool { return g == item{Score: w.Score, Status: w.Status} }},
			{"all", 1<<5 - 1, func(w, g item) bool { return g == w }},
		} {
			var got []item
			var err error
			if e.isPG() {
				var r []pgq.Item
				r, err = pgq.New(db).SearchItems(ctx, pgq.SearchItemsParams{SelectCols: p.mask})
				got = fromItemsPgq(r)
			} else {
				var r []myq.Item
				r, err = myq.New(db).SearchItems(ctx, myq.SearchItemsParams{SelectCols: p.mask})
				got = fromItemsMyq(r)
			}
			if err != nil {
				t.Fatalf("%s: %v", p.name, err)
			}
			if len(got) != len(all) {
				t.Fatalf("%s: %d rows, want %d", p.name, len(got), len(all))
			}
			for i := range got {
				if !p.check(all[i], got[i]) {
					t.Fatalf("%s: row %d = %+v for %+v", p.name, i, got[i], all[i])
				}
			}
		}
	})
}

var _ = context.Background
