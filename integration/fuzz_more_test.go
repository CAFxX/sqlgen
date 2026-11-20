package integration

// More differential tests (see fuzz_oracle_test.go): user transactions, the Into / Seq / Each flavours and
// column types other than int64 / string.

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen/runtime"
)

// Statements bound to a user transaction (Queries.WithTx): writes are visible to reads inside it, equal to the
// oracle after every step, and vanish on rollback (or persist on commit).
func TestFuzzInTransaction(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		a, _ := newFzAPI(e, db, runtime.WithMaxBatchParams(40))
		defer a.Close()
		cases := 0
		for i := 0; i < 30*f.scale(); i++ {
			rows := f.genRows(f.rangeI(10, 60))
			f.reset(rows)
			tx, err := db.BeginTx(f.ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			ta := a.Tx(tx)
			cur := slices.Clone(rows)
			check := func(step string) {
				f.t.Helper()
				cases++
				got, err := ta.Search(f.ctx, fzSearchArgs{})
				if err != nil {
					f.failf("case %d after %s: %v", i, step, err)
				} else if d := diffRows(got, sortByID(cur)); d != "" {
					f.failf("case %d after %s: in-tx read: %s", i, step, d)
				}
				args := fzPageArgs{Terms: f.pageTermsRand(), Limit: f.limit()}
				f.walk(ta, args, pageWant(cur, args), fmt.Sprintf("case %d after %s", i, step))
			}
			for step := 0; step < 3; step++ {
				switch f.intn(3) {
				case 0:
					fresh := f.genRows(f.rangeI(1, 20))
					for j := range fresh {
						fresh[j].ID += int64(1000 * (step + 1))
					}
					v := fzIns{Order: f.intn(4), Path: f.intn(3)}
					if v.Order == 3 {
						fresh = litRows(fresh)
					}
					if _, err := ta.Insert(f.ctx, v, fresh); err != nil {
						f.failf("case %d insert: %v", i, err)
					}
					cur = append(cur, fresh...)
					check("insert")
				case 1:
					ids := f.distinctInts(f.rangeI(1, 20), func() int64 { return f.pickID(cur) })
					d := int64(f.rangeI(1, 4))
					aff, err := ta.Bump(f.ctx, d, ids, nil, f.chance(0.5))
					var want int64
					for j := range cur {
						if slices.Contains(ids, cur[j].ID) {
							cur[j].K[0] += d
							cur[j].K[1] -= d
							want++
						}
					}
					if err != nil || aff != want {
						f.failf("case %d bump: aff=%d want %d err=%v", i, aff, want, err)
					}
					check("bump")
				default:
					ids := f.distinctInts(f.rangeI(1, 20), func() int64 { return f.pickID(cur) })
					aff, err := ta.Delete(f.ctx, ids, nil, f.chance(0.5))
					var keep []fzRow
					for _, r := range cur {
						if !slices.Contains(ids, r.ID) {
							keep = append(keep, r)
						}
					}
					if err != nil || aff != int64(len(cur)-len(keep)) {
						f.failf("case %d delete: aff=%d want %d err=%v", i, aff, len(cur)-len(keep), err)
					}
					cur = keep
					check("delete")
				}
			}
			want := rows
			if f.chance(0.5) {
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				want = cur
			}
			if d := diffRows(f.dump(), sortByID(want)); d != "" {
				f.failf("case %d after tx end: %s", i, d)
			}
		}
		t.Logf("%d checks", cases)
	})
}

var errFzStop = errors.New("fuzz: stop iteration")

// connsIdle fails when a call left a connection checked out (an early exit from a Seq / Each must close the
// rows and give the connection back).
func (f *fz) connsIdle(label string) {
	f.t.Helper()
	for i := 0; i < 100; i++ {
		if f.db.Stats().InUse == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	f.failf("%s: %d connection(s) still in use after the call returned", label, f.db.Stats().InUse)
}

// checkVariants requires the Into / Seq / Each flavours of one call to return exactly ref (the slice flavour's
// result, already checked against the oracle), including after an early break or an error returned by the
// callback.
func (f *fz) checkVariants(label string, v fzVar, ref []fzRow) {
	f.t.Helper()
	prefix, spare := f.intn(4), f.intn(5)
	got, err := v.Into(prefix, spare)
	switch {
	case err != nil:
		f.failf("%s: Into: %v", label, err)
	case len(got) != prefix+len(ref):
		f.failf("%s: Into(prefix %d) returned %d rows, want %d", label, prefix, len(got), prefix+len(ref))
	default:
		for i := 0; i < prefix; i++ {
			if !rowEq(got[i], fzRow{}) {
				f.failf("%s: Into overwrote its prefix row %d: %v", label, i, got[i])
				break
			}
		}
		if d := diffRows(got[prefix:], ref); d != "" {
			f.failf("%s: Into: %s", label, d)
		}
	}
	collect := func(limit int) ([]fzRow, error) {
		var out []fzRow
		if limit == 0 {
			limit = -1
		}
		for r, err := range v.Seq() {
			if err != nil {
				return out, err
			}
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
		return out, nil
	}
	if got, err := collect(0); err != nil {
		f.failf("%s: Seq: %v", label, err)
	} else if d := diffRows(got, ref); d != "" {
		f.failf("%s: Seq: %s", label, d)
	}
	if k := f.rangeI(1, len(ref)+1); len(ref) > 0 {
		got, err := collect(k)
		if err != nil {
			f.failf("%s: Seq break after %d: %v", label, k, err)
		} else if d := diffRows(got, ref[:min(k, len(ref))]); d != "" {
			f.failf("%s: Seq break after %d: %s", label, k, d)
		}
		f.connsIdle(label + ": after Seq break")
	}
	var each []fzRow
	if err := v.Each(func(r fzRow) error { each = append(each, r); return nil }); err != nil {
		f.failf("%s: Each: %v", label, err)
	} else if d := diffRows(each, ref); d != "" {
		f.failf("%s: Each: %s", label, d)
	}
	if len(ref) > 0 {
		k := f.rangeI(1, len(ref))
		var part []fzRow
		err := v.Each(func(r fzRow) error {
			part = append(part, r)
			if len(part) == k {
				return errFzStop
			}
			return nil
		})
		if !errors.Is(err, errFzStop) {
			f.failf("%s: Each stopped after %d rows: err = %v, want the callback's error", label, k, err)
		} else if d := diffRows(part, ref[:k]); d != "" {
			f.failf("%s: Each stopped after %d rows: %s", label, k, d)
		}
		f.connsIdle(label + ": after Each error")
	}
}

// Every flavour (slice, Into, Seq, Each; and for the Batch lookups BatchInto / BatchSeq / BatchEach) of a call
// returns the same rows, also across chunks and when the consumer stops early.
func TestFuzzIterationVariants(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		cases := 0
		for ds := 0; ds < 6*f.scale(); ds++ {
			rows := f.genRows(f.rangeI(20, 90))
			f.reset(rows)
			a, _ := newFzAPI(e, db, runtime.WithMaxBatchParams(f.pick(3, 8, 20, 40, 16384)))
			for i := 0; i < 12; i++ {
				cases++
				switch f.intn(4) {
				case 0:
					args := f.searchArgs(rows, f.intn(256))
					if args.emptySlice() {
						continue
					}
					var want []fzRow
					for _, r := range sortByID(rows) {
						if args.match(r) {
							want = append(want, project(r, args.SelectCols))
						}
					}
					f.checkVariants(fmt.Sprintf("ds %d Search %+v", ds, args), a.SearchV(f.ctx, args), want)
				case 1:
					args := fzPageArgs{Terms: f.pageTermsRand(), Limit: f.limit(), Lo: int64(f.intn(3))}
					want := pageWant(rows, args)
					want = want[:min(len(want), int(args.Limit))]
					f.checkVariants(fmt.Sprintf("ds %d Page %s limit %d", ds, fmtTerms(args.Terms), args.Limit), a.PageV(f.ctx, args), want)
				case 2:
					ids := f.distinctInts(f.rangeI(1, 60), func() int64 { return f.pickID(rows) })
					var want []fzRow
					for _, r := range rows {
						if slices.Contains(ids, r.ID) {
							want = append(want, r)
						}
					}
					v := a.ByIDsBatchV(f.ctx, ids)
					// Batch concatenates chunks (each ordered by id), so compare against the slice flavour.
					ref, err := a.ByIDs(f.ctx, ids, true)
					if err != nil {
						f.failf("ds %d ByIDs batch: %v", ds, err)
						continue
					}
					if d := diffRows(sortByID(ref), sortByID(want)); d != "" {
						f.failf("ds %d ByIDs batch %d ids: %s", ds, len(ids), d)
					}
					f.checkVariants(fmt.Sprintf("ds %d ByIDsBatch %d ids", ds, len(ids)), v, ref)
				default:
					seen := map[[2]int64]bool{}
					var keys [][2]int64
					for n, try := f.rangeI(1, 12), 0; len(keys) < n && try < 100; try++ {
						r := rows[f.intn(len(rows))]
						if k := [2]int64{r.Grp, r.K[0]}; !seen[k] {
							seen[k] = true
							keys = append(keys, k)
						}
					}
					ref, err := a.ByKeys(f.ctx, keys, true)
					if err != nil {
						f.failf("ds %d ByKeys batch: %v", ds, err)
						continue
					}
					f.checkVariants(fmt.Sprintf("ds %d ByKeysBatch %d keys", ds, len(keys)), a.ByKeysBatchV(f.ctx, keys), ref)
				}
			}
			a.Close()
		}
		// A cancelled context fails every flavour with context.Canceled and leaves no connection checked out.
		a, _ := newFzAPI(e, db, runtime.WithMaxBatchParams(8))
		defer a.Close()
		cctx, cancel := context.WithCancel(f.ctx)
		cancel()
		for name, v := range map[string]fzVar{"Search": a.SearchV(cctx, fzSearchArgs{}),
			"Page": a.PageV(cctx, fzPageArgs{Limit: 5}), "ByIDsBatch": a.ByIDsBatchV(cctx, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}),
			"ByKeysBatch": a.ByKeysBatchV(cctx, [][2]int64{{0, 0}, {1, 1}, {2, 2}, {0, 3}, {1, 4}})} {
			if _, err := v.Into(0, 0); !errors.Is(err, context.Canceled) {
				f.failf("%s Into on a cancelled context: err = %v", name, err)
			}
			for _, err := range v.Seq() {
				if !errors.Is(err, context.Canceled) {
					f.failf("%s Seq on a cancelled context: err = %v", name, err)
				}
				break
			}
			if err := v.Each(func(fzRow) error { return nil }); !errors.Is(err, context.Canceled) {
				f.failf("%s Each on a cancelled context: err = %v", name, err)
			}
		}
		f.connsIdle("after cancelled calls")
		t.Logf("%d cases", cases)
	})
}

// ---- column types other than int64 / string ----

func setupTyped(t *testing.T, e engine, db *sql.DB) {
	t.Helper()
	f64, blob := "DOUBLE PRECISION", "BYTEA"
	switch {
	case e.isMySQL():
		f64, blob = "DOUBLE", "VARBINARY(32)"
	case e.family == famSQLite:
		f64, blob = "REAL", "BLOB"
	}
	mustExec(t, db, "DROP TABLE IF EXISTS fz_typed")
	mustExec(t, db, "CREATE TABLE fz_typed (id BIGINT PRIMARY KEY, i32 INT NOT NULL, f64 "+f64+" NOT NULL, flag BOOLEAN NOT NULL, bin "+blob+" NOT NULL, ni INT NULL)")
	shortGC(t, db, "fz_typed")
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS fz_typed") })
}

func (f *fz) genTyped(n int) []fzTRow {
	i32s := []int32{math.MinInt32, -1, 0, 1, math.MaxInt32}[:f.pick(1, 2, 3, 5)]
	f64s := []float64{-1e300, -0.25, 0, 0.25, 1e300}[:f.pick(1, 2, 3, 5)]
	ids := map[int64]bool{}
	var rows []fzTRow
	for len(rows) < n {
		id := int64(f.rangeI(1, n*4+8))
		if ids[id] {
			continue
		}
		ids[id] = true
		b := make([]byte, f.rangeI(0, 4))
		for i := range b {
			b[i] = []byte{0, 1, 0xff, 'a'}[f.intn(4)]
		}
		r := fzTRow{ID: id, I32: i32s[f.intn(len(i32s))], F64: f64s[f.intn(len(f64s))], Flag: f.chance(0.5), Blob: b}
		if !f.chance(0.3) {
			v := i32s[f.intn(len(i32s))]
			r.NI = &v
		}
		rows = append(rows, r)
	}
	return rows
}

func (r fzTRow) String() string {
	ni := "NULL"
	if r.NI != nil {
		ni = strconv.Itoa(int(*r.NI))
	}
	return fmt.Sprintf("{id=%d i32=%d f64=%g flag=%v blob=%x ni=%s}", r.ID, r.I32, r.F64, r.Flag, r.Blob, ni)
}

func tRowEq(a, b fzTRow) bool {
	return a.ID == b.ID && a.I32 == b.I32 && a.F64 == b.F64 && a.Flag == b.Flag && bytes.Equal(a.Blob, b.Blob) &&
		(a.NI == nil) == (b.NI == nil) && (a.NI == nil || *a.NI == *b.NI)
}

func diffTRows(got, want []fzTRow) string {
	for i := 0; i < min(len(got), len(want)); i++ {
		if !tRowEq(got[i], want[i]) {
			return fmt.Sprintf("row %d: got %v want %v (got %d rows, want %d)", i, got[i], want[i], len(got), len(want))
		}
	}
	if len(got) != len(want) {
		return fmt.Sprintf("got %d rows, want %d", len(got), len(want))
	}
	return ""
}

func sortTByID(rows []fzTRow) []fzTRow {
	out := slices.Clone(rows)
	slices.SortFunc(out, func(a, b fzTRow) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

func (f *fz) resetTyped(rows []fzTRow) {
	f.t.Helper()
	mustExec(f.t, f.db, "DELETE FROM fz_typed")
	for _, r := range rows { // hand-written SQL, one row at a time
		var ni any
		if r.NI != nil {
			ni = *r.NI
		}
		mustExec(f.t, f.db, "INSERT INTO fz_typed (id, i32, f64, flag, bin, ni) VALUES ("+
			strings.Join([]string{f.ph(1), f.ph(2), f.ph(3), f.ph(4), f.ph(5), f.ph(6)}, ", ")+")",
			r.ID, r.I32, r.F64, r.Flag, r.Blob, ni)
	}
}

func (f *fz) dumpTyped() []fzTRow {
	f.t.Helper()
	rs, err := f.db.Query("SELECT id, i32, f64, flag, bin, ni FROM fz_typed ORDER BY id")
	if err != nil {
		f.t.Fatal(err)
	}
	defer rs.Close()
	var out []fzTRow
	for rs.Next() {
		var r fzTRow
		var ni sql.NullInt32
		if err := rs.Scan(&r.ID, &r.I32, &r.F64, &r.Flag, &r.Blob, &ni); err != nil {
			f.t.Fatal(err)
		}
		if ni.Valid {
			v := ni.Int32
			r.NI = &v
		}
		out = append(out, r)
	}
	return out
}

func tMatch(s fzTSearch, r fzTRow) bool {
	return r.F64 >= s.Lo && (!s.ByI32s || slices.Contains(s.I32s, r.I32)) && (!s.ByF64s || slices.Contains(s.F64s, r.F64)) &&
		(!s.ByFlag || r.Flag == s.Flag) && (!s.ByBlob || bytes.Equal(r.Blob, s.Blob)) &&
		(!s.NiAtLeast || (r.NI != nil && s.NiMin != nil && *r.NI >= *s.NiMin))
}

func tCmp(a, b fzTRow, col int) int {
	switch col {
	case 0:
		return cmp.Compare(a.F64, b.F64)
	case 1:
		return cmp.Compare(a.I32, b.I32)
	case 2:
		return cmp.Compare(btoi(a.Flag), btoi(b.Flag))
	}
	return cmp.Compare(a.ID, b.ID)
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// d (types): int32 / float64 / bool / []byte / nullable *int32 columns and parameters (scalars, slices, a NULL
// scalar argument) through flags, bulk inserts and keyset pagination over float, int32 and bool sort keys.
func TestFuzzColumnTypes(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupTyped(t, e, db)
		f := newFz(t, e, db, seed)
		cases := 0
		for ds := 0; ds < 6*f.scale(); ds++ {
			rows := f.genTyped(f.rangeI(20, 70))
			f.resetTyped(rows)
			a, _ := newFzAPI(e, db, runtime.WithMaxBatchParams(f.pick(6, 13, 100, 16384)))
			for flags := 0; flags < 32; flags++ {
				cases++
				s := fzTSearch{Lo: []float64{-2e300, -0.25, 0, 0.25}[f.intn(4)], ByI32s: flags&1 != 0, ByF64s: flags&2 != 0,
					ByFlag: flags&4 != 0, Flag: f.chance(0.5), ByBlob: flags&8 != 0, NiAtLeast: flags&16 != 0}
				for n := f.intn(4); n > 0; n-- {
					s.I32s = append(s.I32s, rows[f.intn(len(rows))].I32)
					s.F64s = append(s.F64s, rows[f.intn(len(rows))].F64)
				}
				s.Blob = rows[f.intn(len(rows))].Blob
				if f.chance(0.2) {
					s.Blob = []byte{}
				}
				if !f.chance(0.2) { // otherwise a NULL scalar argument: ni >= NULL matches nothing
					v := int32(f.intn(3)) - 1
					s.NiMin = &v
				}
				got, err := a.TypedSearch(f.ctx, s)
				if (s.ByI32s && len(s.I32s) == 0) || (s.ByF64s && len(s.F64s) == 0) {
					if !errors.Is(err, runtime.ErrEmptySliceParam) {
						f.failf("ds %d flags %05b %+v: err = %v, want ErrEmptySliceParam", ds, flags, s, err)
					}
					continue
				}
				var want []fzTRow
				for _, r := range sortTByID(rows) {
					if tMatch(s, r) {
						want = append(want, r)
					}
				}
				if err != nil {
					f.failf("ds %d flags %05b %+v: %v", ds, flags, s, err)
				} else if d := diffTRows(got, want); d != "" {
					f.failf("ds %d flags %05b %+v: %s", ds, flags, s, d)
				}
			}
			// Keyset pages over (f64, i32, flag, id) in random order and direction.
			for i := 0; i < 8; i++ {
				cases++
				cols := f.r.Perm(3)[:f.rangeI(0, 3)]
				if f.chance(0.7) {
					cols = append(cols, 3)
				}
				mode := f.intn(4)
				var terms []fzTerm
				for _, c := range cols {
					terms = append(terms, fzTerm{c, mode == 1 || (mode > 1 && f.chance(0.5))})
				}
				eff := slices.Clone(terms)
				if !slices.ContainsFunc(eff, func(t fzTerm) bool { return t.Col == 3 }) {
					eff = append(eff, fzTerm{3, len(eff) > 0 && eff[len(eff)-1].Desc})
				}
				want := slices.Clone(rows)
				slices.SortStableFunc(want, func(x, y fzTRow) int {
					for _, t := range eff {
						c := tCmp(x, y, t.Col)
						if t.Desc {
							c = -c
						}
						if c != 0 {
							return c
						}
					}
					return 0
				})
				limit := int64(f.pick(1, 2, 3, 7, 50))
				var got []fzTRow
				var cur runtime.Seek
				for pages := 0; pages <= len(want)+2; pages++ {
					page, err := a.TypedPage(f.ctx, terms, cur, limit)
					if err != nil {
						f.failf("ds %d order %s limit %d: %v", ds, fmtTerms(terms), limit, err)
						break
					}
					got = append(got, page...)
					if int64(len(page)) < limit {
						break
					}
					cur = a.TypedCursor(page[len(page)-1])
				}
				if d := diffTRows(got, want); d != "" {
					f.failf("ds %d order %s limit %d: %s", ds, fmtTerms(terms), limit, d)
				}
			}
			// Bulk insert round trip (plain and Batch), including NULL *int32, empty and binary []byte.
			f.resetTyped(nil)
			batch := f.chance(0.5)
			if aff, err := a.TypedInsert(f.ctx, rows, batch); err != nil || aff != int64(len(rows)) {
				f.failf("ds %d typed insert batch=%v: affected %d, err %v", ds, batch, aff, err)
			} else if d := diffTRows(f.dumpTyped(), sortTByID(rows)); d != "" {
				f.failf("ds %d typed insert batch=%v: %s", ds, batch, d)
			}
			a.Close()
		}
		t.Logf("%d cases", cases)
	})
}
