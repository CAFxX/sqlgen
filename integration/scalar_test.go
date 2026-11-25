package integration

// Scalar results (one output column, no result_type) on real engines: the plain value comes back instead
// of a row struct, through the chunking base method, Seq / Collect / AppendTo / Each, the RETURNING form
// and pipeline futures typed on the value.

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"os"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/CAFxX/sqlgen/integration/mdbq"
	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

type scalarAPI struct {
	count     func(ctx context.Context, org int64) (int64, error)
	names     func(ctx context.Context, ids []int64) ([]string, error)
	namesSeq  func(ctx context.Context, ids []int64) iter.Seq2[string, error]
	insertIDs func(ctx context.Context, ids []int64) ([]int64, error) // INSERT ... RETURNING id (PostgreSQL / MariaDB)
	pipeline  func(ctx context.Context, org int64, ids []int64) (n int64, names []string, err error)
	stats     func() runtime.CacheStats
	close     func() error
}

func newItemsFor(ids []int64) (pg []pgq.NewItem, my []mdbq.NewItem) {
	for _, id := range ids {
		pg = append(pg, pgq.NewItem{ID: id, OrgID: 2, Name: "ins", Score: 1, Status: "new"})
		my = append(my, mdbq.NewItem{ID: id, OrgID: 2, Name: "ins", Score: 1, Status: "new"})
	}
	return
}

func TestRealEngineScalarResults(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		seedItems(t, db) // ids 1..4, org 1, names n1..n4
		ctx := ctxT(t)
		// Wire path where there is one: the pipeline futures below must be typed on the value and cost one turn.
		var wdb *sql.DB
		var turns *atomic.Int64
		if e.isPG() {
			wdb, turns, _ = countingDB(t, os.Getenv(e.env), true)
		} else {
			wdb, turns = myOpenCounted(t, os.Getenv(e.env), true, nil)
		}
		var api scalarAPI
		budget := runtime.WithMaxBatchParams(3) // 3 ids per statement: 5 ids are 2 chunks, with an implicit transaction
		switch {
		case e.isPG():
			q := pgq.New(wdb, budget)
			api = scalarAPI{q.CountOrgItems, q.NamesByIDs, q.NamesByIDsSeq,
				func(ctx context.Context, ids []int64) ([]int64, error) {
					pg, _ := newItemsFor(ids)
					return q.InsertedIDs(ctx, pg)
				},
				func(ctx context.Context, org int64, ids []int64) (n int64, names []string, err error) {
					var cf *runtime.RowFuture[int64]
					var nf *runtime.SliceFuture[string]
					err = q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error { cf, nf = p.CountOrgItems(org), p.NamesByIDs(ids); return nil })
					if err != nil {
						return
					}
					if n, err = cf.Get(); err != nil {
						return
					}
					names, err = nf.Get()
					return
				}, q.CacheStats, q.Close}
		case e.family == famMariaDB:
			q := mdbq.New(wdb, budget)
			api = scalarAPI{q.CountOrgItems, q.NamesByIDs, q.NamesByIDsSeq,
				func(ctx context.Context, ids []int64) ([]int64, error) {
					_, my := newItemsFor(ids)
					return q.InsertedIDs(ctx, my)
				},
				func(ctx context.Context, org int64, ids []int64) (n int64, names []string, err error) {
					var cf *runtime.RowFuture[int64]
					var nf *runtime.SliceFuture[string]
					err = q.PipelineTx(ctx, nil, func(p *mdbq.Pipeline) error { cf, nf = p.CountOrgItems(org), p.NamesByIDs(ids); return nil })
					if err != nil {
						return
					}
					if n, err = cf.Get(); err != nil {
						return
					}
					names, err = nf.Get()
					return
				}, q.CacheStats, q.Close}
		default:
			q := myq.New(wdb, budget)
			api = scalarAPI{q.CountOrgItems, q.NamesByIDs, q.NamesByIDsSeq, nil,
				func(ctx context.Context, org int64, ids []int64) (n int64, names []string, err error) {
					var cf *runtime.RowFuture[int64]
					var nf *runtime.SliceFuture[string]
					err = q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { cf, nf = p.CountOrgItems(org), p.NamesByIDs(ids); return nil })
					if err != nil {
						return
					}
					if n, err = cf.Get(); err != nil {
						return
					}
					names, err = nf.Get()
					return
				}, q.CacheStats, q.Close}
		}
		defer api.close()

		// one: the plain value; COUNT over no rows is 0, not sql.ErrNoRows.
		if n, err := api.count(ctx, 1); n != 4 || err != nil {
			t.Errorf("count org 1 = %d, %v, want 4", n, err)
		}
		if n, err := api.count(ctx, 99); n != 0 || err != nil {
			t.Errorf("count org 99 = %d, %v, want 0", n, err)
		}

		// many: []string in ORDER BY order, unknown ids ignored. NamesByIDs has an ORDER BY, so it is not
		// decomposable (DOCS section 3.2): it never chunks, and a collection over the budget fails before
		// anything is sent.
		ids := []int64{4, 99, 2, 1, 3}
		if got, err := api.names(ctx, ids); got != nil || !errors.Is(err, runtime.ErrCollectionTooLarge) {
			t.Errorf("names(5 ids over a 3-id budget) = %v, %v, want ErrCollectionTooLarge", got, err)
		}
		if _, err := runtime.Collect(api.namesSeq(ctx, ids)); !errors.Is(err, runtime.ErrCollectionTooLarge) {
			t.Errorf("Collect(Seq(5 ids over a 3-id budget)) = %v, want ErrCollectionTooLarge", err)
		}
		few := []int64{4, 2, 1}
		want := []string{"n1", "n2", "n4"}
		if got, err := api.names(ctx, few); !slices.Equal(got, want) || err != nil {
			t.Errorf("names = %v, %v, want %v", got, err, want)
		}
		if got, err := runtime.Collect(api.namesSeq(ctx, few)); !slices.Equal(got, want) || err != nil {
			t.Errorf("Collect(Seq) = %v, %v", got, err)
		}
		dst := []string{"keep"}
		if got, err := runtime.AppendTo(dst, api.namesSeq(ctx, few)); !slices.Equal(got, append([]string{"keep"}, want...)) || err != nil {
			t.Errorf("AppendTo = %v, %v", got, err)
		}
		var each []string
		if err := runtime.Each(api.namesSeq(ctx, few), func(s string) error {
			each = append(each, s)
			if len(each) == 2 {
				return runtime.ErrStop
			}
			return nil
		}); err != nil || !slices.Equal(each, want[:2]) {
			t.Errorf("Each with ErrStop = %v, %v", each, err)
		}
		// Empty IN list: error, nil slice; AppendTo gives dst back untouched.
		if got, err := api.names(ctx, nil); got != nil || !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Errorf("names(empty) = %v, %v", got, err)
		}
		if got, err := runtime.AppendTo(dst, api.namesSeq(ctx, nil)); !errors.Is(err, runtime.ErrEmptySliceParam) || !slices.Equal(got, dst) {
			t.Errorf("AppendTo(empty ids) = %v, %v", got, err)
		}

		// Pipeline futures typed on the value, one turn on the wire path.
		run := func() {
			n, names, err := api.pipeline(ctx, 1, ids) // a pipeline operation never chunks
			if n != 4 || !slices.Equal(names, []string{"n1", "n2", "n3", "n4"}) || err != nil {
				t.Errorf("pipeline = %d, %v, %v", n, names, err)
			}
		}
		run()
		if c := measure(t, wdb, turns, run); c != 1 {
			t.Errorf("scalar pipeline cost %d turns, want 1", c)
		}

		// INSERT ... RETURNING a scalar (PostgreSQL / CockroachDB / MariaDB): ids of 5 rows over 2 chunks, then empty.
		if api.insertIDs != nil {
			ins := []int64{107, 103, 105, 101, 102}
			got, err := api.insertIDs(ctx, ins)
			if err != nil {
				t.Fatalf("InsertedIDs: %v", err)
			}
			if e.isPG() && !slices.IsSorted(got) { // presorted ASC, also across chunks
				t.Errorf("InsertedIDs not in presorted order: %v", got)
			}
			slices.Sort(got)
			if !slices.Equal(got, []int64{101, 102, 103, 105, 107}) {
				t.Errorf("InsertedIDs = %v", got)
			}
			if got, err := api.insertIDs(ctx, nil); err != nil || len(got) != 0 {
				t.Errorf("InsertedIDs(empty) = %v, %v, want a successful no-op", got, err)
			}
		}
	})
}
