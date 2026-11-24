package integration

// Differential tests of the generated statements against the oracle of fuzz_oracle_test.go: conditional blocks,
// SelectCols, chunked bulk statements, keyset pagination, composed queries, writes and RETURNING.

import (
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// ---- the tests ----

// scale multiplies the case counts: 1 on the server engines (keep each under ~60s), 2 on in-process SQLite (slow under -race),
// and everything by SQLGEN_FUZZ_SCALE when set (soak runs: `SQLGEN_FUZZ_SCALE=10 SQLGEN_FUZZ_SEED=...`).
func (f *fz) scale() int {
	n := 1
	if f.e.family == famSQLite {
		n = 2
	}
	if v, err := strconv.Atoi(os.Getenv("SQLGEN_FUZZ_SCALE")); err == nil && v > 0 {
		n *= v
	}
	return n
}

// a + e: every one of the 256 combinations of FzSearch's eight {?Flag:} blocks (two passes: the default
// cache, then one that prepares statements after the first use), with random parameters including empty and
// disabled slice parameters, NULL-able columns and random SelectCols projections.
func TestFuzzConditionalFlags(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		rows := f.genRows(150)
		f.reset(rows)
		apis := []fzAPI{}
		// Third pass: a tiny cache, so statements are constantly evicted, demoted and re-prepared.
		for _, opts := range [][]runtime.CacheOption{nil, {runtime.WithPromoteThreshold(1)},
			{runtime.WithPromoteThreshold(1), runtime.WithMaxSQLVariants(3), runtime.WithMaxPreparedStmts(2), runtime.WithMaxPartialBytes(512)}} {
			a, _ := newFzAPI(e, db, opts...)
			defer a.Close()
			apis = append(apis, a)
		}
		cases := 0
		for pass, a := range apis {
			for flags := 0; flags < 256; flags++ {
				cases++
				args := f.searchArgs(rows, flags)
				got, err := a.Search(f.ctx, args)
				if args.emptySlice() {
					if !errors.Is(err, runtime.ErrEmptySliceParam) {
						f.failf("pass %d flags %08b %+v: err = %v, want ErrEmptySliceParam", pass, flags, args, err)
					}
					continue
				}
				if err != nil {
					f.failf("pass %d flags %08b %+v: %v", pass, flags, args, err)
					continue
				}
				var want []fzRow
				for _, r := range rows {
					if args.match(r) {
						want = append(want, r)
					}
				}
				want = sortByID(want) // ORDER BY id happens on the unprojected rows
				for i := range want {
					want[i] = project(want[i], args.SelectCols)
				}
				if d := diffRows(got, want); d != "" {
					f.failf("pass %d flags %08b %+v: %s", pass, flags, args, d)
				}
			}
		}
		t.Logf("%d cases", cases)
	})
}

// e: FzNarrow with every one of the 63 non-empty column subsets (and the 0 mask) under both flag states.
func TestFuzzSelectCols(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		rows := f.genRows(60)
		f.reset(rows)
		a, _ := newFzAPI(e, db)
		defer a.Close()
		// FzNarrow selects id, grp, k1, k2, tag, note: bit i is the i-th of those.
		narrowBit := []int{fzColID, fzColGrp, fzColK1, fzColK1 + 1, fzColTag, fzColNote}
		for mask := uint64(0); mask < 64; mask++ {
			for _, byGrp := range []bool{false, true} {
				minID, grp := int64(f.intn(100)), int64(f.intn(4))
				got, err := a.Narrow(f.ctx, minID, byGrp, grp, mask)
				if err != nil {
					f.failf("mask %06b byGrp=%v: %v", mask, byGrp, err)
					continue
				}
				var full uint64 // translate into fzRow column bits for project()
				for i, c := range narrowBit {
					if mask&(1<<i) != 0 {
						full |= 1 << c
					}
				}
				var want []fzRow
				for _, r := range rows {
					if r.ID >= minID && (!byGrp || r.Grp == grp) {
						n := fzRow{ID: r.ID, Grp: r.Grp, Tag: r.Tag, Note: r.Note}
						n.K[0], n.K[1] = r.K[0], r.K[1]
						want = append(want, n)
					}
				}
				want = sortByID(want)
				for i := range want {
					want[i] = project(want[i], full)
				}
				if d := diffRows(got, want); d != "" {
					f.failf("mask %06b byGrp=%v minID=%d grp=%d: %s", mask, byGrp, minID, grp, d)
				}
			}
		}
	})
}

// b: multi-row inserts (presorted ASC / DESC / unsorted; auto-chunking base method and Stream) and IN / tuple-IN lookups at
// sizes around the chunk boundary, using small WithMaxBatchParams budgets so many chunks are exercised.
func TestFuzzBatchChunking(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		cases := 0
		for i := 0; i < 120*f.scale(); i++ {
			budget := f.pick(1, 5, 13, 14, 26, 27, 39, 40, 65, 100, 13*7, 200)
			a, _ := newFzAPI(e, db, runtime.WithMaxBatchParams(budget))
			rowsPerChunk := max(1, budget/fzNumCols)
			n := f.pick(1, rowsPerChunk-1, rowsPerChunk, rowsPerChunk+1, 2*rowsPerChunk, 2*rowsPerChunk+1,
				3*rowsPerChunk-1, f.rangeI(1, 40))
			n = max(1, min(n, 150))
			rows := f.genRows(n)
			in := slices.Clone(rows)
			v := fzIns{Order: f.intn(4), Path: f.intn(2)}
			if v.Order == 3 {
				rows = litRows(rows)
			}
			in = slices.Clone(rows)
			f.reset(nil)
			cases++
			aff, err := a.Insert(f.ctx, v, in)
			if err != nil {
				f.failf("budget %d n %d %+v: insert: %v", budget, n, v, err)
				a.Close()
				continue
			}
			if aff != int64(n) {
				f.failf("budget %d n %d %+v: RowsAffected = %d", budget, n, v, aff)
			}
			for j := range in {
				if !rowEq(in[j], rows[j]) {
					f.failf("budget %d n %d %+v: insert mutated the caller's slice at %d", budget, n, v, j)
					break
				}
			}
			if d := diffRows(f.dump(), sortByID(rows)); d != "" {
				f.failf("budget %d n %d %+v: table after insert: %s", budget, n, v, d)
			}

			// IN-list lookup (auto-chunking): ids = a random subset of the table padded with unknown ids
			// to a size around the 1D chunk size (= budget).
			sz := f.pick(1, budget-1, budget, budget+1, 2*budget+1, f.rangeI(1, 60))
			sz = max(1, min(sz, 600))
			idSet := f.distinctInts(sz, func() int64 {
				if f.chance(0.5) {
					return rows[f.intn(len(rows))].ID
				}
				return int64(f.rangeI(1, 700))
			})
			if len(idSet) > 0 {
				want := []fzRow{}
				for _, r := range rows {
					if slices.Contains(idSet, r.ID) {
						want = append(want, r)
					}
				}
				got, err := a.ByIDs(f.ctx, idSet)
				if err != nil {
					f.failf("budget %d ByIDs(%d ids): %v", budget, len(idSet), err)
				} else if d := diffRows(sortByID(got), sortByID(want)); d != "" {
					f.failf("budget %d ByIDs(%d ids): %s", budget, len(idSet), d)
				}
			}

			// Tuple IN lookup: (grp, k1) keys, distinct, existing and unknown, around budget/2 per chunk.
			per := max(1, budget/2)
			nk := max(1, min(f.pick(1, per-1, per, per+1, 2*per+1, f.rangeI(1, 30)), 300))
			seen := map[[2]int64]bool{}
			var keys [][2]int64
			for len(keys) < nk {
				k := [2]int64{int64(f.intn(5)), int64(f.intn(12))}
				if f.chance(0.5) {
					r := rows[f.intn(len(rows))]
					k = [2]int64{r.Grp, r.K[0]}
				}
				if !seen[k] {
					seen[k] = true
					keys = append(keys, k)
				}
				if len(seen) >= 5*12 {
					break
				}
			}
			var want []fzRow
			for _, r := range rows {
				if seen[[2]int64{r.Grp, r.K[0]}] {
					want = append(want, r)
				}
			}
			if got, err := a.ByKeys(f.ctx, keys); err != nil {
				f.failf("budget %d ByKeys(%d keys): %v", budget, len(keys), err)
			} else if d := diffRows(sortByID(got), sortByID(want)); d != "" {
				f.failf("budget %d ByKeys(%d keys): %s", budget, len(keys), d)
			}
			a.Close()
		}
		// Streams pull 256 items per chunk at the default budget: sizes around that boundary, one chunk and
		// several, through the presorting / literal-template / unsorted statements.
		for _, n := range []int{255, 256, 257, 511, 512, 513, 700} {
			cases++
			a, _ := newFzAPI(e, db)
			rows := f.genRows(n)
			v := fzIns{Order: f.intn(4), Path: 1}
			if v.Order == 3 {
				rows = litRows(rows)
			}
			f.reset(nil)
			if aff, err := a.Insert(f.ctx, v, rows); err != nil || aff != int64(n) {
				f.failf("stream %d rows %+v: affected %d, err %v", n, v, aff, err)
			} else if d := diffRows(f.dump(), sortByID(rows)); d != "" {
				f.failf("stream %d rows %+v: %s", n, v, d)
			}
			a.Close()
		}
		// An error yielded by the source ends the stream and comes back as it is.
		a, _ := newFzAPI(e, db, runtime.WithMaxBatchParams(40))
		for _, n := range []int{0, 1, 3, 4, 9} {
			f.reset(nil)
			if err := a.InsertStreamErr(f.ctx, f.genRows(10), n, errFzStop); !errors.Is(err, errFzStop) {
				f.failf("stream failing after %d items: err = %v, want the source's error", n, err)
			}
		}
		a.Close()
		t.Logf("%d cases", cases)
	})
}

// walk pages through the whole result of a (a fresh cursor per page, taken from the last row of the previous
// one) and requires every page to equal the oracle's slice, so there are no gaps, repeats or reordering.
func (f *fz) walk(a fzAPI, args fzPageArgs, want []fzRow, label string) {
	f.t.Helper()
	off := 0
	for pages := 0; ; pages++ {
		if pages > len(want)+2 {
			f.failf("%s: pagination does not terminate (%d pages for %d rows)", label, pages, len(want))
			return
		}
		got, err := a.Page(f.ctx, args)
		if err != nil {
			f.failf("%s: page %d (after row %d): %v", label, pages, off, err)
			return
		}
		exp := want[off:min(off+int(args.Limit), len(want))]
		if d := diffRows(got, exp); d != "" {
			f.failf("%s: page %d (rows %d..): %s", label, pages, off, d)
			return
		}
		off += len(got)
		if int64(len(got)) < args.Limit {
			break
		}
		args.After = a.Cursor(got[len(got)-1])
	}
	if off != len(want) {
		f.failf("%s: walked %d rows, want %d", label, off, len(want))
	}
	f.randomAccess(a, args, want, label)
}

// randomAccess requests single pages after the cursor of a few random rows of the oracle's sorted result: the
// page must be exactly the next rows (every cursor, not only the ones a walk happens to produce).
func (f *fz) randomAccess(a fzAPI, args fzPageArgs, want []fzRow, label string) {
	f.t.Helper()
	for i := 0; i < 3 && len(want) > 0; i++ {
		k := f.intn(len(want))
		args.After = a.Cursor(want[k])
		got, err := a.Page(f.ctx, args)
		if err != nil {
			f.failf("%s: page after row %d: %v", label, k, err)
			return
		}
		if d := diffRows(got, want[k+1:min(k+1+int(args.Limit), len(want))]); d != "" {
			f.failf("%s: page after row %d (%v): %s", label, k, want[k], d)
			return
		}
	}
}

func (f *fz) limit() int64 { return int64(f.pick(1, 1, 2, 3, 4, 5, 7, 8, 13, 16, 40, 200)) }

// c: keyset pagination over random Order specs of 0..12 columns (mixed directions, tie-breaker named,
// implied or in the middle) on duplicate-heavy data. The spec and its direction-flipped twin are walked page
// by page and must equal the oracle's total sort exactly.
func TestFuzzKeysetPagination(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		a, _ := newFzAPI(e, db)
		defer a.Close()
		cases := 0
		for ds := 0; ds < 10*f.scale(); ds++ {
			f.extreme = ds%4 == 3 // every fourth dataset sorts on int64 extremes
			rows := f.genRows(f.rangeI(30, 90))
			f.reset(rows)
			if got, err := a.Page(f.ctx, fzPageArgs{Limit: 0}); err != nil || len(got) != 0 {
				f.failf("ds %d: LIMIT 0 returned %d rows, err %v", ds, len(got), err)
			}
			// A repeated sort column is an error, not a different sort (and never reaches the database).
			bad := f.pageTermsRand()
			if len(bad) > 0 {
				bad = append(bad, bad[f.intn(len(bad))])
				if _, err := a.Page(f.ctx, fzPageArgs{Terms: bad, Limit: 5}); !errors.Is(err, runtime.ErrInvalidOrder) {
					f.failf("ds %d: repeated term in %s: err = %v, want ErrInvalidOrder", ds, fmtTerms(bad), err)
				}
			}
			if _, err := a.Page(f.ctx, fzPageArgs{Terms: []fzTerm{{Col: len(fzOrderCols) + f.intn(10)}}, Limit: 5}); !errors.Is(err, runtime.ErrInvalidOrder) {
				f.failf("ds %d: out-of-range term: err = %v, want ErrInvalidOrder", ds, err)
			}
			for i := 0; i < 12; i++ {
				terms := f.pageTermsRand()
				for _, ts := range [][]fzTerm{terms, flipTerms(terms)} {
					cases++
					args := fzPageArgs{Terms: ts, Limit: f.limit()}
					f.walk(a, args, pageWant(rows, args), fmt.Sprintf("ds %d order %s limit %d", ds, fmtTerms(ts), args.Limit))
				}
			}
		}
		t.Logf("%d walks", cases)
	})
}

// d: keyset pages over composed sub-queries (up to depth 3, with their own flags and slice parameters) combined
// with FzPage's own flags and a random Order; plus CTE aggregation over the same sub-query trees.
func TestFuzzComposedQueries(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		a, _ := newFzAPI(e, db)
		defer a.Close()
		cases := 0
		for ds := 0; ds < 6*f.scale(); ds++ {
			rows := f.genRows(f.rangeI(40, 100))
			f.reset(rows)
			for i := 0; i < 10; i++ {
				cases++
				args := fzPageArgs{Lo: int64(f.intn(3)), Terms: f.pageTermsRand(), Limit: f.limit(),
					InSub: f.chance(0.7), ByGrps: f.chance(0.5), SkipTag: f.chance(0.4), ExTag: f.str("abc", 1, 3)}
				args.Sub = f.newSub(f.rangeI(1, 3), false)
				if args.ByGrps && !f.chance(0.1) {
					args.Grps = f.distinctInts(f.rangeI(1, 3), func() int64 { return int64(f.intn(5)) })
				}
				label := fmt.Sprintf("ds %d order %s limit %d %+v", ds, fmtTerms(args.Terms), args.Limit, args)
				if (args.ByGrps && len(args.Grps) == 0) || (args.InSub && subEmpty(args.Sub)) {
					_, err := a.Page(f.ctx, args)
					if !errors.Is(err, runtime.ErrEmptySliceParam) {
						f.failf("%s: err = %v, want ErrEmptySliceParam", label, err)
					}
					continue
				}
				if !args.InSub && f.chance(0.5) {
					args.Sub = nil // a disabled block must not need its sub-query either
				}
				f.walk(a, args, pageWant(rows, args), label)
			}
			for i := 0; i < 10; i++ {
				cases++
				sub := f.newSub(f.rangeI(1, 3), true)
				byTag, tag := f.chance(0.5), f.str("abc", 1, 3)
				if f.chance(0.5) {
					tag = rows[f.intn(len(rows))].Tag
				}
				got, err := a.Agg(f.ctx, sub, byTag, tag)
				if err != nil {
					f.failf("ds %d Agg %+v: %v", ds, sub, err)
					continue
				}
				ids := subIDs(rows, sub)
				agg := map[int64]*fzAggRow{}
				for _, r := range rows {
					if ids[r.ID] && (!byTag || r.Tag == tag) {
						g := agg[r.Grp]
						if g == nil {
							g = &fzAggRow{Grp: r.Grp}
							agg[r.Grp] = g
						}
						g.N++
						g.Total += r.K[0]
					}
				}
				want := []fzAggRow{}
				for _, g := range agg {
					want = append(want, *g)
				}
				slices.SortFunc(want, func(x, y fzAggRow) int { return cmp.Compare(x.Grp, y.Grp) })
				if !slices.Equal(got, want) {
					f.failf("ds %d Agg(byTag=%v tag=%q) sub=%+v: got %v want %v", ds, byTag, tag, sub, got, want)
				}
			}
		}
		t.Logf("%d cases", cases)
	})
}

// f: UPDATE / DELETE ... WHERE id IN (presorted) with optional flags, through the auto-chunking base methods with tiny chunk budgets,
// and the RETURNING forms the engine supports; affected counts, returned rows and the whole table afterwards
// must match the oracle exactly.
func TestFuzzWritesAndReturning(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		retID, retUpd := fzRetSupport(e)
		cases := 0
		for i := 0; i < 240*f.scale(); i++ {
			budget := f.pick(1, 3, 4, 7, 16, 100000)
			a, ret := newFzAPI(e, db, runtime.WithMaxBatchParams(budget))
			rows := f.genRows(f.rangeI(10, 60))
			f.reset(rows)
			// ids: existing and unknown ones. The base methods chunk (the planner may split even a short list), so
			// a repeated id may be hit once per chunk: duplicates are only used for the idempotent DELETEs, where
			// the row is removed once and later chunks affect nothing.
			op := f.intn(6)
			dup := (op == 2 || op == 3) && f.chance(0.5)
			n := f.rangeI(1, 40)
			var ids []int64
			if !dup {
				ids = f.distinctInts(n, func() int64 { return f.pickID(rows) })
			} else {
				for len(ids) < n {
					ids = append(ids, f.pickID(rows))
				}
			}
			var grp *int64
			var tag *string
			if f.chance(0.5) {
				g := int64(f.intn(4))
				grp = &g
			}
			if f.chance(0.5) {
				s := rows[f.intn(len(rows))].Tag
				tag = &s
			}
			delta := int64(f.rangeI(1, 5))
			if f.chance(0.5) {
				delta = -delta
			}
			cases++
			label := fmt.Sprintf("case %d op %d budget %d dup=%v ids=%v grp=%v tag=%v", i, op, budget, dup, ids, ptrStr(grp), ptrStr(tag))
			inIDs := func(id int64) bool { return slices.Contains(ids, id) }
			var wantAff int64
			var wantOut []fzOut
			var wantTable []fzRow
			switch op {
			case 0, 1: // update (exec, RETURNING)
				if op == 1 && !retUpd {
					a.Close()
					continue
				}
				for _, r := range rows {
					if inIDs(r.ID) && (grp == nil || r.Grp == *grp) {
						wantAff++
						r.K[0] += delta
						r.K[1] -= delta // the statement is SET k1 = k1 + :delta, k2 = k2 - :delta
						wantOut = append(wantOut, fzOut{r.ID, r.K[0]})
					}
					wantTable = append(wantTable, r)
				}
				var got []fzOut
				var err error
				if op == 0 {
					var aff int64
					aff, err = a.Bump(f.ctx, delta, ids, grp)
					if aff != wantAff && err == nil {
						f.failf("%s: Bump RowsAffected = %d, want %d", label, aff, wantAff)
					}
				} else {
					got, err = ret.BumpRet(f.ctx, delta, ids, grp)
				}
				if err != nil {
					f.failf("%s: %v", label, err)
				} else if op == 1 {
					fzSameOuts(f, label+" BumpRet", got, wantOut)
				}
			case 2, 3: // delete (exec, RETURNING)
				if op == 3 && !retID {
					a.Close()
					continue
				}
				for _, r := range rows {
					if inIDs(r.ID) && (tag == nil || r.Tag == *tag) {
						wantAff++
						wantOut = append(wantOut, fzOut{r.ID, r.K[0]})
					} else {
						wantTable = append(wantTable, r)
					}
				}
				var got []fzOut
				var err error
				if op == 2 {
					var aff int64
					aff, err = a.Delete(f.ctx, ids, tag)
					if aff != wantAff && err == nil {
						f.failf("%s: Delete RowsAffected = %d, want %d", label, aff, wantAff)
					}
				} else {
					got, err = ret.DeleteRet(f.ctx, ids, tag)
				}
				if err != nil {
					f.failf("%s: %v", label, err)
				} else if op == 3 {
					fzSameOuts(f, label+" DeleteRet", got, wantOut)
				}
			default: // insert ... RETURNING, presorted ascending (also across chunks)
				if !retID {
					a.Close()
					continue
				}
				fresh := f.genRows(f.rangeI(1, 30))
				var add []fzRow
				for _, r := range fresh {
					r.ID += 1000 // disjoint from rows
					add = append(add, r)
				}
				wantTable = append(slices.Clone(rows), add...)
				mode := f.intn(3)
				got, err := ret.InsertRet(f.ctx, add, mode)
				if err != nil {
					f.failf("%s: InsertRet: %v", label, err)
					break
				}
				want := make([]fzOut, len(add))
				for j, r := range add {
					want[j] = fzOut{r.ID, r.K[0]}
				}
				if e.isPG() && mode == 0 {
					// DOCS 2.4: rows come back in presorted order on PostgreSQL / CockroachDB, also across the
					// chunks of the base method. (Stream cannot sort globally: each chunk is sorted on its own.)
					slices.SortFunc(want, func(x, y fzOut) int { return cmp.Compare(x.ID, y.ID) })
					if !slices.Equal(got, want) {
						f.failf("%s: InsertRet order: got %v want %v", label, got, want)
					}
				} else {
					fzSameOuts(f, label+" InsertRet", got, want)
				}
			}
			if d := diffRows(f.dump(), sortByID(wantTable)); d != "" {
				f.failf("%s: table afterwards: %s", label, d)
			}
			a.Close()
		}
		t.Logf("%d cases", cases)
	})
}

func (f *fz) pickID(rows []fzRow) int64 {
	if len(rows) > 0 && f.chance(0.7) {
		return rows[f.intn(len(rows))].ID
	}
	return int64(f.rangeI(1, 400))
}

func ptrStr[T any](p *T) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprint(*p)
}

// fzSameOuts compares RETURNING rows as sets (the order of UPDATE/DELETE ... RETURNING is unspecified).
func fzSameOuts(f *fz, label string, got, want []fzOut) {
	f.t.Helper()
	g, w := slices.Clone(got), slices.Clone(want)
	for _, s := range [][]fzOut{g, w} {
		slices.SortFunc(s, func(x, y fzOut) int { return cmp.Or(cmp.Compare(x.ID, y.ID), cmp.Compare(x.K1, y.K1)) })
	}
	if !slices.Equal(g, w) {
		f.failf("%s: returned %v, want %v", label, got, want)
	}
}

// d: two distinct sub-query slots plus the same slot referenced twice (its arguments must be bound at both
// places), and conditional SELECT columns / JOIN driven by a flag, with SelectCols projections.
func TestFuzzSubsAndJoin(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		a, _ := newFzAPI(e, db)
		defer a.Close()
		cases := 0
		for ds := 0; ds < 4*f.scale(); ds++ {
			rows := f.genRows(f.rangeI(30, 100))
			f.reset(rows)
			for i := 0; i < 25; i++ {
				cases++
				sa, sb := f.newSub(f.rangeI(1, 3), true), f.newSub(f.rangeI(1, 3), true)
				lo, excludeB, againA := int64(f.intn(3)), f.chance(0.6), f.chance(0.6)
				got, err := a.Subs(f.ctx, sa, sb, lo, excludeB, againA)
				if err != nil {
					f.failf("ds %d Subs a=%+v b=%+v lo=%d excludeB=%v againA=%v: %v", ds, sa, sb, lo, excludeB, againA, err)
					continue
				}
				idsA, idsB := subIDs(rows, sa), subIDs(rows, sb)
				grpsA := map[int64]bool{}
				for _, r := range rows {
					if idsA[r.ID] {
						grpsA[r.Grp] = true
					}
				}
				want := []fzRow{}
				for _, r := range rows {
					if idsA[r.ID] && r.K[1] >= lo && !(excludeB && idsB[r.ID]) && (!againA || grpsA[r.Grp]) {
						want = append(want, r)
					}
				}
				if d := diffRows(got, sortByID(want)); d != "" {
					f.failf("ds %d Subs a=%+v b=%+v lo=%d excludeB=%v againA=%v: %s", ds, sa, sb, lo, excludeB, againA, d)
				}
			}
			for i := 0; i < 25; i++ {
				cases++
				off := rows[f.intn(len(rows))].ID - rows[f.intn(len(rows))].K[1]
				minID, grp := int64(f.intn(50)), int64(f.intn(4))
				withPeer, byGrp := f.chance(0.6), f.chance(0.5)
				cols := uint64(0)
				if f.chance(0.6) {
					cols = uint64(f.rangeI(1, 7))
				}
				got, err := a.Join(f.ctx, off, minID, withPeer, byGrp, grp, cols)
				if err != nil {
					f.failf("ds %d Join off=%d min=%d withPeer=%v byGrp=%v cols=%b: %v", ds, off, minID, withPeer, byGrp, cols, err)
					continue
				}
				byID := map[int64]fzRow{}
				for _, r := range rows {
					byID[r.ID] = r
				}
				want := []fzJoinRow{}
				for _, r := range sortByID(rows) {
					if r.ID < minID || (byGrp && r.Grp != grp) {
						continue
					}
					j := fzJoinRow{ID: r.ID, Grp: r.Grp, K1: r.K[0]}
					if withPeer {
						p, ok := byID[r.K[1]+off]
						if !ok {
							continue // inner join
						}
						j.PeerID, j.PeerTag = p.ID, p.Tag
					}
					if cols != 0 { // SelectCols covers the unconditional columns id, grp, k1
						if cols&1 == 0 {
							j.ID = 0
						}
						if cols&2 == 0 {
							j.Grp = 0
						}
						if cols&4 == 0 {
							j.K1 = 0
						}
					}
					want = append(want, j)
				}
				if !slices.Equal(got, want) {
					f.failf("ds %d Join off=%d min=%d withPeer=%v byGrp=%v cols=%b: got %v want %v", ds, off, minID, withPeer, byGrp, cols, got, want)
				}
			}
		}
		t.Logf("%d cases", cases)
	})
}

// Parallel readers sharing one Queries (one statement cache, pooled buffers): every worker draws its own
// random searches, pages (from random cursors) and bulk lookups and checks them against the oracle.
func TestFuzzConcurrentReaders(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		rows := f.genRows(120)
		f.reset(rows)
		a, _ := newFzAPI(e, db, runtime.WithMaxBatchParams(40), runtime.WithPromoteThreshold(2))
		defer a.Close()
		db.SetMaxOpenConns(8)
		done := make(chan struct{})
		const workers = 8
		for w := 0; w < workers; w++ {
			go func(w int) {
				defer func() { done <- struct{}{} }()
				g := f.fork(w)
				for i := 0; i < 40*f.scale(); i++ {
					switch g.intn(3) {
					case 0:
						args := g.searchArgs(rows, g.intn(256))
						got, err := a.Search(g.ctx, args)
						if args.emptySlice() {
							if !errors.Is(err, runtime.ErrEmptySliceParam) {
								g.failf("search %+v: err = %v, want ErrEmptySliceParam", args, err)
							}
							continue
						}
						var want []fzRow
						for _, r := range sortByID(rows) {
							if args.match(r) {
								want = append(want, project(r, args.SelectCols))
							}
						}
						if err != nil {
							g.failf("search %+v: %v", args, err)
						} else if d := diffRows(got, want); d != "" {
							g.failf("search %+v: %s", args, d)
						}
					case 1:
						args := fzPageArgs{Terms: g.pageTermsRand(), Limit: g.limit(), Lo: int64(g.intn(3)), SkipTag: g.chance(0.5), ExTag: g.str("abc", 1, 2)}
						want := pageWant(rows, args)
						g.randomAccess(a, args, want, fmt.Sprintf("worker %d order %s limit %d", w, fmtTerms(args.Terms), args.Limit))
					default:
						ids := g.distinctInts(g.rangeI(1, 120), func() int64 { return g.pickID(rows) })
						got, err := a.ByIDs(g.ctx, ids)
						var want []fzRow
						for _, r := range rows {
							if slices.Contains(ids, r.ID) {
								want = append(want, r)
							}
						}
						if err != nil {
							g.failf("ByIDs %d ids: %v", len(ids), err)
						} else if d := diffRows(sortByID(got), sortByID(want)); d != "" {
							g.failf("ByIDs %d ids: %s", len(ids), d)
						}
					}
				}
			}(w)
		}
		for w := 0; w < workers; w++ {
			<-done
		}
	})
}

// Strings that look like SQL, placeholders, template syntax or are not ASCII must round-trip untouched through
// every insert path and match in IN lookups. (Values that a case-/accent-insensitive collation would equate
// with each other are not mixed in one table: this checks transport, not collation.)
func TestFuzzStringRoundTrip(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		weird := []string{"'", "\"", "\\", "''", "%", "_", ";--", "?", "$1", ":id:i64", "{?A: x}", "a'b", "\n", "\t.",
			"é", "日本語", "$$", "x;DROP", "/*", "*/", "\\'", "?,?", "$2,$3"}
		cases := 0
		for i := 0; i < 30*f.scale(); i++ {
			rows := f.genRows(f.rangeI(1, 25))
			for j := range rows {
				rows[j].Tag = weird[f.intn(len(weird))]
				if f.chance(0.7) {
					s := weird[f.intn(len(weird))]
					rows[j].Note = &s
				}
			}
			f.reset(nil)
			v := fzIns{Order: f.intn(4), Path: f.intn(2)}
			if v.Order == 3 {
				rows = litRows(rows)
			}
			a, _ := newFzAPI(e, db, runtime.WithMaxBatchParams(f.pick(13, 40, 200, 16384)))
			cases++
			if _, err := a.Insert(f.ctx, v, rows); err != nil {
				f.failf("insert %+v: %v", v, err)
				a.Close()
				continue
			}
			if d := diffRows(f.dump(), sortByID(rows)); d != "" {
				f.failf("round trip %+v: %s", v, d)
			}
			var wantTags []string
			for _, r := range rows[:min(3, len(rows))] {
				if !slices.Contains(wantTags, r.Tag) {
					wantTags = append(wantTags, r.Tag)
				}
			}
			got, err := a.Search(f.ctx, fzSearchArgs{ByTags: true, Tags: wantTags})
			var want []fzRow
			for _, r := range sortByID(rows) {
				if slices.Contains(wantTags, r.Tag) {
					want = append(want, r)
				}
			}
			if err != nil {
				f.failf("search tags %q: %v", wantTags, err)
			} else if d := diffRows(got, want); d != "" {
				f.failf("search tags %q: %s", wantTags, d)
			}
			a.Close()
		}
		t.Logf("%d cases", cases)
	})
}
