package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen/runtime"
)

// ---- engine-agnostic view of the two generated packages (pgq: $N, myq: ?) ----

type item struct {
	ID, OrgID int64
	Name      string
	Score     int64
	Status    string
}

type orgStat struct{ OrgID, N, Total int64 }

type searchArgs struct {
	MinScore   int64
	OrgID      *int64
	Statuses   []string
	NamePrefix *string
}

type api interface {
	Insert(ctx context.Context, rows []item) error
	InsertBatch(ctx context.Context, rows []item) error
	InsertStream(ctx context.Context, rows iter.Seq[item]) error
	Get(ctx context.Context, id int64) (item, error)
	ByIDs(ctx context.Context, ids []int64) ([]item, error)
	ByIDsBatch(ctx context.Context, ids []int64) ([]item, error)
	EachByIDs(ctx context.Context, ids []int64, fn func(item) bool) error
	Search(ctx context.Context, s searchArgs) ([]item, error)
	Page(ctx context.Context, orgID int64, scoreDesc, idDesc bool, limit int64) ([]item, error)
	AddScore(ctx context.Context, orgID, delta int64, ids []int64) (int64, error)
	Delete(ctx context.Context, ids []int64) (int64, error)
	Transfer(ctx context.Context, first, second, amount int64, pol runtime.TxRetryPolicy) error
	PipelineTransfer(ctx context.Context, from, to, amount int64, pol runtime.TxRetryPolicy) error
	OrgStats(ctx context.Context, minItems int64, status string) ([]orgStat, error)
	CacheStats() runtime.CacheStats
	Close() error
}

func newAPI(e engine, db *sql.DB, opts ...runtime.CacheOption) api {
	if e.isPG() {
		return newPgqAPI(db, opts...)
	}
	return newMyqAPI(db, opts...)
}

// ---- fixtures ----

const itemsDDL = `CREATE TABLE sg_items (
	id     BIGINT PRIMARY KEY,
	org_id BIGINT NOT NULL,
	name   VARCHAR(64) NOT NULL,
	score  BIGINT NOT NULL,
	status VARCHAR(16) NOT NULL
)`

func setupItems(t *testing.T, db *sql.DB) {
	t.Helper()
	mustExec(t, db, "DROP TABLE IF EXISTS sg_items")
	mustExec(t, db, itemsDDL)
	mustExec(t, db, "CREATE INDEX sg_items_org_score ON sg_items (org_id, score, id)")
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS sg_items") })
}

var statuses = []string{"new", "active", "closed"}

// genItems builds n deterministic items with ids first..first+n-1, many score ties and 7 orgs.
func genItems(first, n int64) []item {
	out := make([]item, n)
	for i := range out {
		id := first + int64(i)
		out[i] = item{ID: id, OrgID: id%7 + 1, Name: fmt.Sprintf("item-%06d", id), Score: (id * 37) % 101, Status: statuses[id%3]}
	}
	return out
}

func shuffled(rows []item, seed uint64) []item {
	out := slices.Clone(rows)
	r := rand.New(rand.NewPCG(seed, seed+1))
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func ids(rows []item) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func countRows(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow("SELECT COUNT(*) FROM sg_items").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func sameItems(t *testing.T, what string, got, want []item) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d rows, want %d", what, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: row %d: got %+v, want %+v", what, i, got[i], want[i])
		}
	}
}

// ---- tests ----

// Wire-limit-aware chunking, presorted (deadlock-free) multi-row inserts and bulk lookups must work
// against real servers, whose parameter limits and parsers are the thing the chunk maths protects.
func TestRealEngineBatchInsertAndLookup(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		ctx := ctxT(t)
		a := newAPI(e, db)
		defer a.Close()

		all := genItems(1, 30000)
		in := shuffled(all, 1)
		before := slices.Clone(in)
		if err := a.InsertBatch(ctx, in); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}
		if !slices.Equal(in, before) {
			t.Fatal("InsertBatch mutated the caller's slice")
		}
		if n := countRows(t, db); n != 30000 {
			t.Fatalf("rows = %d, want 30000", n)
		}

		// A bulk lookup larger than one statement may carry (> chunk size) must still return everything.
		got, err := a.ByIDsBatch(ctx, ids(shuffled(all, 2)))
		if err != nil {
			t.Fatalf("ByIDsBatch: %v", err)
		}
		slices.SortFunc(got, func(x, y item) int { return int(x.ID - y.ID) })
		sameItems(t, "ByIDsBatch", got, all)

		// A single statement with ~20k placeholders (the wire-limit case) in one round trip.
		got, err = a.ByIDs(ctx, ids(all[:20000]))
		if err != nil {
			t.Fatalf("ByIDs(20000): %v", err)
		}
		sameItems(t, "ByIDs(20000)", got, all[:20000])

		// Streaming ingestion in O(1) memory.
		more := genItems(30001, 5000)
		if err := a.InsertStream(ctx, slices.Values(shuffled(more, 3))); err != nil {
			t.Fatalf("InsertStream: %v", err)
		}
		if n := countRows(t, db); n != 35000 {
			t.Fatalf("rows after stream = %d, want 35000", n)
		}
		one, err := a.Get(ctx, 34999)
		if err != nil || one != more[4998] {
			t.Fatalf("Get(34999) = %+v, %v", one, err)
		}
		if _, err := a.Get(ctx, 999999); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("Get(missing) err = %v, want sql.ErrNoRows", err)
		}
	})
}

func TestRealEngineSearchFlagCombinations(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		ctx := ctxT(t)
		a := newAPI(e, db)
		defer a.Close()
		all := genItems(1, 600)
		if err := a.Insert(ctx, shuffled(all, 4)); err != nil {
			t.Fatal(err)
		}
		org := int64(3)
		prefix := "item-0001"
		for mask := 0; mask < 8; mask++ {
			for _, minScore := range []int64{0, 50, 101} {
				s := searchArgs{MinScore: minScore}
				if mask&1 != 0 {
					s.OrgID = &org
				}
				if mask&2 != 0 {
					s.Statuses = []string{"new", "closed"}
				}
				if mask&4 != 0 {
					s.NamePrefix = &prefix
				}
				var want []item
				for _, r := range all {
					if r.Score < minScore ||
						(s.OrgID != nil && r.OrgID != *s.OrgID) ||
						(s.Statuses != nil && !slices.Contains(s.Statuses, r.Status)) ||
						(s.NamePrefix != nil && !strings.HasPrefix(r.Name, *s.NamePrefix)) {
						continue
					}
					want = append(want, r)
				}
				got, err := a.Search(ctx, s)
				if err != nil {
					t.Fatalf("mask=%03b min=%d: %v", mask, minScore, err)
				}
				sameItems(t, fmt.Sprintf("mask=%03b min=%d", mask, minScore), got, want)
			}
		}
	})
}

// Keyset pagination must reproduce the full ORDER BY exactly, on duplicate-heavy data, for every
// sort-direction combination and page size, on every engine's own comparison semantics.
func TestRealEngineKeysetPagination(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		ctx := ctxT(t)
		a := newAPI(e, db)
		defer a.Close()
		all := genItems(1, 1400)
		if err := a.Insert(ctx, shuffled(all, 5)); err != nil {
			t.Fatal(err)
		}
		const org = 4
		for _, scoreDesc := range []bool{false, true} {
			for _, idDesc := range []bool{false, true} {
				var want []item
				for _, r := range all {
					if r.OrgID == org {
						want = append(want, r)
					}
				}
				slices.SortFunc(want, func(x, y item) int {
					if x.Score != y.Score {
						if scoreDesc {
							return int(y.Score - x.Score)
						}
						return int(x.Score - y.Score)
					}
					if idDesc {
						return int(y.ID - x.ID)
					}
					return int(x.ID - y.ID)
				})
				for _, limit := range []int64{1, 7, 50, 1000} {
					got, err := a.Page(ctx, org, scoreDesc, idDesc, limit)
					if err != nil {
						t.Fatalf("scoreDesc=%v idDesc=%v limit=%d: %v", scoreDesc, idDesc, limit, err)
					}
					sameItems(t, fmt.Sprintf("scoreDesc=%v idDesc=%v limit=%d", scoreDesc, idDesc, limit), got, want)
				}
			}
		}
	})
}

func TestRealEngineComposedSubqueryAndAggregates(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		ctx := ctxT(t)
		a := newAPI(e, db)
		defer a.Close()
		all := genItems(1, 500)
		// Make org 7 small so the TopOrgs filter is meaningful.
		for i := range all {
			if all[i].OrgID == 7 && all[i].ID > 100 {
				all[i].OrgID = 1
			}
		}
		if err := a.Insert(ctx, all); err != nil {
			t.Fatal(err)
		}
		const minItems = 30
		counts := map[int64]int64{}
		for _, r := range all {
			counts[r.OrgID]++
		}
		want := map[int64]orgStat{}
		for _, r := range all {
			if counts[r.OrgID] >= minItems && r.Status == "active" {
				s := want[r.OrgID]
				s.OrgID, s.N, s.Total = r.OrgID, s.N+1, s.Total+r.Score
				want[r.OrgID] = s
			}
		}
		got, err := a.OrgStats(ctx, minItems, "active")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) || len(got) == 0 {
			t.Fatalf("got %d orgs, want %d (%v)", len(got), len(want), got)
		}
		for _, g := range got {
			if g != want[g.OrgID] {
				t.Fatalf("org %d: got %+v, want %+v", g.OrgID, g, want[g.OrgID])
			}
		}
	})
}

func TestRealEngineUpdateAndDelete(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		ctx := ctxT(t)
		a := newAPI(e, db)
		defer a.Close()
		all := genItems(1, 200)
		if err := a.Insert(ctx, all); err != nil {
			t.Fatal(err)
		}
		pick := []int64{150, 3, 77, 8, 120, 15} // unsorted on purpose; org filter excludes some
		var wantTouched int64
		for _, id := range pick {
			if all[id-1].OrgID == 2 {
				wantTouched++
			}
		}
		n, err := a.AddScore(ctx, 2, 1000, pick)
		if err != nil || n != wantTouched {
			t.Fatalf("AddScore affected %d (%v), want %d", n, err, wantTouched)
		}
		for _, id := range pick {
			r, _ := a.Get(ctx, id)
			wantScore := all[id-1].Score
			if all[id-1].OrgID == 2 {
				wantScore += 1000
			}
			if r.Score != wantScore {
				t.Fatalf("id %d score = %d, want %d", id, r.Score, wantScore)
			}
		}
		n, err = a.Delete(ctx, pick)
		if err != nil || n != int64(len(pick)) {
			t.Fatalf("Delete affected %d (%v), want %d", n, err, len(pick))
		}
		if c := countRows(t, db); c != 200-int64(len(pick)) {
			t.Fatalf("rows = %d", c)
		}
	})
}

// Concurrent transfers that lock rows in opposite orders provoke genuine deadlocks (InnoDB, PostgreSQL)
// or serialization restarts (CockroachDB). With the retry policy money must be conserved and no
// transfer may fail.
func TestRealEngineContendedTransfersConserveMoney(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		ctx := ctxT(t)
		a := newAPI(e, db)
		defer a.Close()
		const accounts, start = 6, 1000
		rows := make([]item, accounts)
		for i := range rows {
			rows[i] = item{ID: int64(i + 1), OrgID: 1, Name: fmt.Sprintf("acct-%d", i), Score: start, Status: "active"}
		}
		if err := a.Insert(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if e.family == famPostgres {
			// PostgreSQL only runs deadlock detection after deadlock_timeout (1s by default), which turns
			// deliberate opposing-order contention into minutes of waiting. Tighten it for this database.
			var dbName string
			if err := db.QueryRow("SELECT current_database()").Scan(&dbName); err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, fmt.Sprintf("ALTER DATABASE %q SET deadlock_timeout = '20ms'", dbName))
			t.Cleanup(func() { _, _ = db.Exec(fmt.Sprintf("ALTER DATABASE %q RESET deadlock_timeout", dbName)) })
			db.SetConnMaxLifetime(time.Nanosecond) // force fresh sessions that pick up the new setting
			time.Sleep(50 * time.Millisecond)
			db.SetConnMaxLifetime(0)
		}
		pol := runtime.TxRetryPolicy{MaxRetries: 200, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond}
		workers, perWorker := 12, 40
		if e.family == famCockroach {
			workers, perWorker = 4, 15 // optimistic restarts under this much contention are slow; keep CI fast
		}
		var failures, pipeFailures atomic.Int64
		var wg sync.WaitGroup
		for g := 0; g < workers; g++ {
			wg.Add(1)
			go func(seed uint64) {
				defer wg.Done()
				r := rand.New(rand.NewPCG(seed, seed*7+1))
				for i := 0; i < perWorker; i++ {
					from, to := int64(r.IntN(accounts)+1), int64(r.IntN(accounts)+1)
					if from == to {
						continue
					}
					amt := int64(r.IntN(5) + 1)
					if i%4 == 3 {
						if err := a.PipelineTransfer(ctx, from, to, amt, pol); err != nil {
							pipeFailures.Add(1)
							t.Errorf("PipelineTransfer %d->%d: %v", from, to, err)
						}
						continue
					}
					// Debit first, credit second: opposing lock order across goroutines.
					if err := a.Transfer(ctx, from, to, amt, pol); err != nil {
						failures.Add(1)
						t.Errorf("Transfer %d->%d: %v", from, to, err)
					}
				}
			}(uint64(g + 1))
		}
		wg.Wait()
		var sum int64
		if err := db.QueryRow("SELECT SUM(score) FROM sg_items").Scan(&sum); err != nil {
			t.Fatal(err)
		}
		if sum != accounts*start {
			t.Fatalf("money not conserved: sum = %d, want %d (failed transfers: %d, pipeline: %d)", sum, accounts*start, failures.Load(), pipeFailures.Load())
		}
	})
}

// Streaming iterators abandoned early, transactions and cache promotion must never strand the only
// connection of a pool limited to one (a leaked rows/stmt/tx handle shows up as a hang).
func TestRealEngineSingleConnectionPoolNoLeaks(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		db.SetMaxOpenConns(1)
		ctx := ctxT(t)
		a := newAPI(e, db, runtime.WithPromoteThreshold(1), runtime.WithMaxPreparedStmts(64))
		defer a.Close()
		all := genItems(1, 300)
		if err := a.Insert(ctx, all); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			n := 0
			if err := a.EachByIDs(ctx, ids(all), func(item) bool { n++; return n < 3 }); err != nil {
				t.Fatalf("EachByIDs: %v", err)
			}
			if _, err := a.Get(ctx, 1); err != nil {
				t.Fatalf("Get after early break #%d: %v", i, err)
			}
		}
		if err := a.Transfer(ctx, 1, 2, 5, runtime.DefaultRetryPolicy()); err != nil {
			t.Fatalf("Transfer: %v", err)
		}
		if err := a.PipelineTransfer(ctx, 2, 1, 5, runtime.DefaultRetryPolicy()); err != nil {
			t.Fatalf("PipelineTransfer: %v", err)
		}
		if _, err := a.Get(ctx, 2); err != nil {
			t.Fatalf("Get after tx: %v", err)
		}
	})
}

// A hot statement must be promoted to a server-side prepared statement and keep returning correct
// results through it; Close must release every prepared statement.
func TestRealEnginePreparedStatementPromotion(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupItems(t, db)
		ctx := ctxT(t)
		a := newAPI(e, db, runtime.WithPromoteThreshold(2), runtime.WithMaxPreparedStmts(64))
		all := genItems(1, 100)
		if err := a.Insert(ctx, all); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 200; i++ {
			id := int64(i%100 + 1)
			r, err := a.Get(ctx, id)
			if err != nil || r != all[id-1] {
				t.Fatalf("Get(%d) = %+v, %v", id, r, err)
			}
		}
		st := a.CacheStats()
		if st.Promotions == 0 || st.PreparedHits == 0 {
			t.Fatalf("hot statement never reached Tier 2: %+v", st)
		}
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}
