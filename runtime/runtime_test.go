package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	stdruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

func openTestDB(t testing.TB) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	t.Cleanup(func() {
		_ = db.Close()
	})
	if _, err := db.Exec(`
		CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			category TEXT NOT NULL,
			score INTEGER NOT NULL,
			label TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= 32; i++ {
		if _, err := db.Exec(`INSERT INTO items (id, category, score, label) VALUES (?, ?, ?, ?)`,
			i, fmt.Sprintf("cat_%d", i%4), i*10, fmt.Sprintf("label_%d", i)); err != nil {
			t.Fatalf("seed items: %v", err)
		}
	}
	return db
}

func TestVariantKeyAndSubqueryShape(t *testing.T) {
	kExact := runtime.MakeVariantKey(12, 0x0F, 0x03, 0x100, 5, 10)
	if !kExact.IsExact() {
		t.Fatalf("expected exact VariantKey, got %v", kExact)
	}

	kExact4 := runtime.MakeVariantKey(12, 0x0F, 0x03, 0, 1, 2, 3, 4)
	if !kExact4.IsExact() {
		t.Fatalf("expected exact 4-dim VariantKey")
	}

	// Overflow tests -> InexactKeyBit must be set
	inexactCases := []runtime.VariantKey{
		runtime.MakeVariantKey(0, 1, 1, 0),
		runtime.MakeVariantKey(0x8000, 1, 1, 0),
		runtime.MakeVariantKey(1, 0x1000000, 1, 0),
		runtime.MakeVariantKey(1, 1, 0x1000000, 0),
		runtime.MakeVariantKey(1, 1, 1, 0, 1, 2, 3, 4, 5),
		runtime.MakeVariantKey(1, 1, 1, 0, 1, 2, 1<<33),
		runtime.MakeVariantKey(1, 1, 1, 0, runtime.InexactKeyBit|42),
	}
	for i, k := range inexactCases {
		if k.IsExact() {
			t.Fatalf("case %d: expected inexact VariantKey, got %v", i, k)
		}
	}

	// ComputeSubqueryShape exact vs inexact
	sExact := runtime.ComputeSubqueryShape(3, 0x07, 0x01, 0, false, 4, 8)
	if sExact&runtime.InexactKeyBit != 0 {
		t.Fatalf("expected exact subquery shape tag, got %#x", sExact)
	}
	wantExact := (uint64(3) << 48) | (uint64(0x07) << 40) | (uint64(0x01) << 32) | (uint64(4) << 16) | uint64(8)
	if sExact != wantExact {
		t.Fatalf("subquery shape tag mismatch: got %#x, want %#x", sExact, wantExact)
	}

	sInexactCases := []uint64{
		runtime.ComputeSubqueryShape(3, 0x07, 0x01, 0, true, 4),
		runtime.ComputeSubqueryShape(3, 0x07, 0x01, 1, false, 4),
		runtime.ComputeSubqueryShape(0, 0x07, 0x01, 0, false),
		runtime.ComputeSubqueryShape(0x8000, 0x07, 0x01, 0, false),
		runtime.ComputeSubqueryShape(3, 0x100, 0x01, 0, false),
		runtime.ComputeSubqueryShape(3, 0x07, 0x100, 0, false),
		runtime.ComputeSubqueryShape(3, 0x07, 0x01, 0, false, 1, 2, 3),
		runtime.ComputeSubqueryShape(3, 0x07, 0x01, 0, false, 70000),
	}
	for i, s := range sInexactCases {
		if s&runtime.InexactKeyBit == 0 {
			t.Fatalf("inexact subquery case %d: expected InexactKeyBit set, got %#x", i, s)
		}
	}
}

func TestExpandOrderAndSeek(t *testing.T) {
	// Default spec == 0 -> tie-breaker (last column) ASC
	terms, count, err := runtime.ExpandOrder(0, 3)
	if err != nil || count != 1 || terms[0].ColIdx != 2 || terms[0].Desc {
		t.Fatalf("unexpected default ExpandOrder: count=%d terms=%v err=%v", count, terms[:count], err)
	}

	// Single DESC column -> tie-breaker automatically appended inheriting DESC
	spec := runtime.PackOrder(runtime.Desc(0))
	terms, count, err = runtime.ExpandOrder(spec, 3)
	if err != nil || count != 2 {
		t.Fatalf("expected 2 terms, got %d (err=%v)", count, err)
	}
	if terms[0] != (runtime.OrderTerm{ColIdx: 0, Desc: true}) || terms[1] != (runtime.OrderTerm{ColIdx: 2, Desc: true}) {
		t.Fatalf("unexpected expanded terms: %v", terms[:count])
	}

	// Out-of-range column index -> ErrInvalidOrder
	badSpec := runtime.PackOrder(runtime.Asc(5))
	if _, _, err := runtime.ExpandOrder(badSpec, 3); !errors.Is(err, runtime.ErrInvalidOrder) {
		t.Fatalf("expected ErrInvalidOrder, got %v", err)
	}

	// Seek validation
	dst := runtime.AcquireArgs(8)
	defer runtime.ReleaseArgs(dst)
	badSeek := runtime.NewSeek(1)
	if err := runtime.AppendSeekArgs(dst, badSeek, spec, 3); !errors.Is(err, runtime.ErrInvalidSeek) {
		t.Fatalf("expected ErrInvalidSeek, got %v", err)
	}

	// Seek with full 3 declared column values selects indices 0 and 2 (active terms) -> 1 + 2 = 3 args
	goodSeek := runtime.NewSeek("cat_a", 999, 42)
	if err := runtime.AppendSeekArgs(dst, goodSeek, spec, 3); err != nil {
		t.Fatalf("unexpected AppendSeekArgs error: %v", err)
	}
	wantArgs := []any{"cat_a", "cat_a", 42}
	if len(*dst) != len(wantArgs) {
		t.Fatalf("got %d seek args, want %d", len(*dst), len(wantArgs))
	}
	for i := range wantArgs {
		if (*dst)[i] != wantArgs[i] {
			t.Fatalf("seek arg %d = %v, want %v", i, (*dst)[i], wantArgs[i])
		}
	}
}

func TestArgPoolClearsReferences(t *testing.T) {
	buf := runtime.AcquireArgs(4)
	*buf = append(*buf, "secret_1", "secret_2")
	underlying := (*buf)[:2]
	runtime.ReleaseArgs(buf)
	if underlying[0] != nil || underlying[1] != nil {
		t.Fatalf("expected ReleaseArgs to clear underlying slice elements, got %v", underlying)
	}
}

func TestWTinyLFUScanFloodResistance(t *testing.T) {
	db := openTestDB(t)
	const (
		maxPartial      = 128
		maxPartialBytes = 32 * 1024
		maxPrepared     = 160
	)
	cache := runtime.NewStmtCache(
		runtime.WithMaxSQLVariants(maxPartial),
		runtime.WithMaxPartialBytes(maxPartialBytes),
		runtime.WithMaxPreparedStmts(maxPrepared),
		runtime.WithPromoteThreshold(2),
	)
	defer func() { _ = cache.Close() }()

	ctx := context.Background()

	// Warm 50 hot variations so they promote to Tier 2 and enter preparedProt
	hotKeys := make([]runtime.VariantKey, 50)
	hotSQLs := make([]string, 50)
	for i := 0; i < 50; i++ {
		hotKeys[i] = runtime.MakeVariantKey(1, 0, 0, 0, uint64(i+1))
		hotSQLs[i] = fmt.Sprintf("SELECT id, score FROM items WHERE id = ? AND %d = %d", i+1, i+1)
		for hit := 0; hit < 6; hit++ {
			row, ms, err := cache.QueryRow(ctx, db, nil, true, hotKeys[i], 1, 0, 0, nil, func() string {
				return hotSQLs[i]
			}, (i%32)+1)
			if err != nil {
				t.Fatalf("warm hot query %d: %v", i, err)
			}
			var id, score int
			if err := row.Scan(&id, &score); err != nil {
				t.Fatalf("scan hot query %d: %v", i, err)
			}
			if ms != nil {
				_ = ms.Release()
			}
		}
		if st := cache.LookupState(hotKeys[i], hotSQLs[i]); st != runtime.StatePrepared {
			t.Fatalf("expected hot key %d in StatePrepared before flood, got %v", i, st)
		}
	}

	// Flood cache with 20,000 one-off scan variations while interleaving hot variations
	for i := 0; i < 20_000; i++ {
		oneOffID := uint64(10_000 + i)
		vk := runtime.MakeVariantKey(2, 0, 0, 0, oneOffID)
		sqlStr := fmt.Sprintf("SELECT id FROM items WHERE id = ? AND %d = %d", oneOffID, oneOffID)
		row, ms, err := cache.QueryRow(ctx, db, nil, true, vk, 2, 0, 0, nil, func() string {
			return sqlStr
		}, 1)
		if err != nil {
			t.Fatalf("flood query %d: %v", i, err)
		}
		var id int
		if err := row.Scan(&id); err != nil {
			t.Fatalf("flood scan %d: %v", i, err)
		}
		if ms != nil {
			_ = ms.Release()
		}

		if i%20 == 0 {
			hIdx := (i / 20) % len(hotKeys)
			row, ms, err := cache.QueryRow(ctx, db, nil, true, hotKeys[hIdx], 1, 0, 0, nil, func() string {
				return hotSQLs[hIdx]
			}, (hIdx%32)+1)
			if err != nil {
				t.Fatalf("interleaved hot query %d: %v", hIdx, err)
			}
			var id, score int
			if err := row.Scan(&id, &score); err != nil {
				t.Fatalf("interleaved hot scan %d: %v", hIdx, err)
			}
			if ms != nil {
				_ = ms.Release()
			}
		}
	}

	stats := cache.Stats()
	if stats.ActivePartial > maxPartial {
		t.Fatalf("ActivePartial (%d) exceeded cap (%d)", stats.ActivePartial, maxPartial)
	}
	if stats.PartialBytes > maxPartialBytes {
		t.Fatalf("PartialBytes (%d) exceeded cap (%d)", stats.PartialBytes, maxPartialBytes)
	}
	if stats.ActivePrepared > maxPrepared {
		t.Fatalf("ActivePrepared (%d) exceeded cap (%d)", stats.ActivePrepared, maxPrepared)
	}

	// Verify all 50 hot variations remain in Tier 2 (StatePrepared)
	for i := range hotKeys {
		if st := cache.LookupState(hotKeys[i], hotSQLs[i]); st != runtime.StatePrepared {
			t.Fatalf("hot variation %d was evicted from Tier 2 during scan flood (state=%v)", i, st)
		}
	}
}

func TestConcurrentStreamingAndDemotionSafety(t *testing.T) {
	db := openTestDB(t)
	// Tiny Tier-2 capacity (maxPrepared = 4) with PromoteThreshold = 1 to force continuous demotions
	cache := runtime.NewStmtCache(
		runtime.WithMaxPreparedStmts(4),
		runtime.WithMaxSQLVariants(64),
		runtime.WithPromoteThreshold(1),
	)
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	const numGoroutines = 32
	const iterations = 40

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for it := 0; it < iterations; it++ {
				variant := uint64((gid*iterations + it) % 24)
				vk := runtime.MakeVariantKey(1, 0, 0, 0, variant)
				sqlStr := fmt.Sprintf("SELECT id, score FROM items WHERE id >= ? AND %d = %d ORDER BY id ASC", variant, variant)

				rows, ms, err := cache.Query(ctx, db, nil, true, vk, 1, 0, 0, nil, func() string {
					return sqlStr
				}, 1)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d iter %d Query: %w", gid, it, err)
					return
				}

				count := 0
				for rows.Next() {
					var id, score int
					if err := rows.Scan(&id, &score); err != nil {
						_ = rows.Close()
						if ms != nil {
							_ = ms.Release()
						}
						errCh <- fmt.Errorf("goroutine %d iter %d Scan: %w", gid, it, err)
						return
					}
					count++
					if count == 5 && it%3 == 0 {
						// Early break while other goroutines are promoting/demoting
						break
					}
				}
				if err := rows.Err(); err != nil {
					_ = rows.Close()
					if ms != nil {
						_ = ms.Release()
					}
					errCh <- fmt.Errorf("goroutine %d iter %d rows.Err: %w", gid, it, err)
					return
				}
				_ = rows.Close()
				if ms != nil {
					if err := ms.Release(); err != nil {
						errCh <- fmt.Errorf("goroutine %d iter %d ms.Release: %w", gid, it, err)
						return
					}
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	stats := cache.Stats()
	if stats.Demotions == 0 {
		t.Fatalf("expected continuous Tier-2 demotions under maxPrepared=4, got 0 demotions")
	}
}

func TestTxStmtCacheGCCleanupReleasesManagedStmt(t *testing.T) {
	db := openTestDB(t)
	cache := runtime.NewStmtCache(
		runtime.WithMaxPreparedStmts(1),
		runtime.WithPromoteThreshold(1),
	)
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	vk1 := runtime.MakeVariantKey(1, 0, 0, 0, 1)
	sql1 := "SELECT id FROM items WHERE id = ? AND 1 = 1"

	// Prepare vk1 into Tier 2 and capture its ManagedStmt
	rows, ms1, err := cache.Query(ctx, db, nil, true, vk1, 1, 0, 0, nil, func() string { return sql1 }, 1)
	if err != nil {
		t.Fatalf("Query vk1: %v", err)
	}
	_ = rows.Close()
	if ms1 == nil {
		t.Fatalf("expected non-nil ManagedStmt in Tier 2")
	}
	_ = ms1.Release()

	// Bind ms1 inside a manual TxStmtCache without calling tc.Close()
	func() {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		defer func() { _ = tx.Rollback() }()

		tc := runtime.NewTxStmtCache(tx)
		r, ms, err := cache.Query(ctx, db, tc, true, vk1, 1, 0, 0, nil, func() string { return sql1 }, 1)
		if err != nil {
			t.Fatalf("Query in tx: %v", err)
		}
		_ = r.Close()
		if ms != nil {
			_ = ms.Release()
		}
		// While tc is reachable, ms1 has 1 ref from Tier 2 + 1 ref held by tc = 2
		if refs := ms1.Refs(); refs < 2 {
			t.Fatalf("expected ms1.Refs() >= 2 while TxStmtCache is open, got %d", refs)
		}
	}()

	// Now demote vk1 from Tier 2 by promoting another statement in the same shard
	for cand := uint64(2); cand < 200; cand++ {
		vk2 := runtime.MakeVariantKey(1, 0, 0, 0, cand)
		if (vk2.Hash() & 15) == (vk1.Hash() & 15) {
			sql2 := fmt.Sprintf("SELECT id FROM items WHERE id = ? AND %d = %d", cand, cand)
			r, ms2, err := cache.Query(ctx, db, nil, true, vk2, 1, 0, 0, nil, func() string { return sql2 }, 1)
			if err != nil {
				t.Fatalf("Query vk2: %v", err)
			}
			_ = r.Close()
			if ms2 != nil {
				_ = ms2.Release()
			}
			break
		}
	}

	// Trigger GC so runtime.AddCleanup on the unreachable TxStmtCache releases its held reference to ms1
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stdruntime.GC()
		if ms1.Refs() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if refs := ms1.Refs(); refs != 0 {
		t.Fatalf("expected ms1.Refs() == 0 after TxStmtCache GC cleanup, got %d", refs)
	}
}

func BenchmarkStaticQueryHit(b *testing.B) {
	db := openTestDB(b)
	cache := runtime.NewStmtCache(runtime.WithPromoteThreshold(1))
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	vk := runtime.MakeVariantKey(1, 0, 0, 0)
	var fastSlot atomic.Pointer[runtime.ManagedStmt]
	var buildCount atomic.Int64
	buildSQL := func() string {
		buildCount.Add(1)
		return "SELECT id, score FROM items WHERE id = ?"
	}

	// Warm Tier 2
	row, ms, err := cache.QueryRow(ctx, db, nil, true, vk, 1, 0, 0, &fastSlot, buildSQL, 1)
	if err != nil {
		b.Fatal(err)
	}
	var id, score int
	if err := row.Scan(&id, &score); err != nil {
		b.Fatal(err)
	}
	if ms != nil {
		_ = ms.Release()
	}

	buildsBefore := buildCount.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		argBuf := runtime.AcquireArgs(1)
		*argBuf = append(*argBuf, 1)
		r, m, err := cache.QueryRow(ctx, db, nil, true, vk, 1, 0, 0, &fastSlot, buildSQL, (*argBuf)...)
		runtime.ReleaseArgs(argBuf)
		if err != nil {
			b.Fatal(err)
		}
		var outID, outScore int
		if err := r.Scan(&outID, &outScore); err != nil {
			b.Fatal(err)
		}
		if m != nil {
			_ = m.Release()
		}
	}
	b.StopTimer()
	if buildCount.Load() != buildsBefore {
		b.Fatalf("expected 0 SQL string builds on Tier-2 hit, got %d", buildCount.Load()-buildsBefore)
	}
}

func BenchmarkDynamicSliceHit(b *testing.B) {
	db := openTestDB(b)
	cache := runtime.NewStmtCache(runtime.WithPromoteThreshold(1))
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	ids := []int64{1, 2, 3, 4}
	vk := runtime.MakeVariantKey(2, 0, 0, 0, uint64(len(ids)))
	desc := &runtime.QueryDescriptor{
		Segments:  []string{"SELECT id, score FROM items WHERE id IN (", ")"},
		Slots:     []runtime.Slot{{Kind: runtime.SlotSlice}},
		NumSlices: 1,
	}
	var buildCount atomic.Int64
	buildSQL := func() string {
		buildCount.Add(1)
		in := runtime.RenderInput{SliceLens: []int{len(ids)}}
		return runtime.BuildComposedSQL(desc, &in, runtime.DialectQuestion)
	}

	// Warm Tier 2
	argBuf := runtime.AcquireArgs(len(ids))
	for _, v := range ids {
		*argBuf = append(*argBuf, v)
	}
	rows, ms, err := cache.Query(ctx, db, nil, true, vk, 2, 0, 0, nil, buildSQL, (*argBuf)...)
	runtime.ReleaseArgs(argBuf)
	if err != nil {
		b.Fatal(err)
	}
	_ = rows.Close()
	if ms != nil {
		_ = ms.Release()
	}

	buildsBefore := buildCount.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ab := runtime.AcquireArgs(len(ids))
		for _, v := range ids {
			*ab = append(*ab, v)
		}
		r, m, err := cache.Query(ctx, db, nil, true, vk, 2, 0, 0, nil, buildSQL, (*ab)...)
		runtime.ReleaseArgs(ab)
		if err != nil {
			b.Fatal(err)
		}
		var itemID, itemScore int64
		scanBuf := [2]any{&itemID, &itemScore}
		for r.Next() {
			if err := r.Scan(scanBuf[:]...); err != nil {
				b.Fatal(err)
			}
		}
		_ = r.Close()
		if m != nil {
			_ = m.Release()
		}
	}
	b.StopTimer()
	if buildCount.Load() != buildsBefore {
		b.Fatalf("expected 0 SQL string allocations/builds on Tier-2 slice hit, got %d", buildCount.Load()-buildsBefore)
	}
}

type benchItemRow struct {
	ID       int64
	Category string
	Score    int64
	Label    string
}

func BenchmarkSelectColsScan(b *testing.B) {
	db := openTestDB(b)
	cache := runtime.NewStmtCache(runtime.WithPromoteThreshold(1))
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	const colMask uint64 = (1 << 0) | (1 << 2) // ID and Score only
	vk := runtime.MakeVariantKey(3, colMask, 0, 0)
	desc := &runtime.QueryDescriptor{
		Segments:       []string{"SELECT ", " FROM items WHERE id <= ", ""},
		Slots:          []runtime.Slot{{Kind: runtime.SlotSelectCols}, {Kind: runtime.SlotScalar}},
		SelectColExprs: []string{"id", "category", "score", "label"},
	}
	buildSQL := func() string {
		in := runtime.RenderInput{ColMask: colMask}
		return runtime.BuildComposedSQL(desc, &in, runtime.DialectQuestion)
	}

	seqFn := func(limitID int64) iter.Seq2[benchItemRow, error] {
		return func(yield func(benchItemRow, error) bool) {
			ab := runtime.AcquireArgs(1)
			*ab = append(*ab, limitID)
			rows, ms, err := cache.Query(ctx, db, nil, true, vk, 3, colMask<<32, 0, nil, buildSQL, (*ab)...)
			runtime.ReleaseArgs(ab)
			if err != nil {
				yield(benchItemRow{}, err)
				return
			}
			var item benchItemRow
			var scanBuf [4]any
			scanCount := 0
			if (colMask & (1 << 0)) != 0 {
				scanBuf[scanCount] = &item.ID
				scanCount++
			}
			if (colMask & (1 << 1)) != 0 {
				scanBuf[scanCount] = &item.Category
				scanCount++
			}
			if (colMask & (1 << 2)) != 0 {
				scanBuf[scanCount] = &item.Score
				scanCount++
			}
			if (colMask & (1 << 3)) != 0 {
				scanBuf[scanCount] = &item.Label
				scanCount++
			}
			scanSlice := scanBuf[:scanCount]
			for rows.Next() {
				item = benchItemRow{}
				if err := rows.Scan(scanSlice...); err != nil {
					_ = rows.Close()
					if ms != nil {
						_ = ms.Release()
					}
					yield(benchItemRow{}, err)
					return
				}
				if !yield(item, nil) {
					_ = rows.Close()
					if ms != nil {
						_ = ms.Release()
					}
					return
				}
			}
			_ = rows.Close()
			if ms != nil {
				_ = ms.Release()
			}
		}
	}

	// Warm Tier 2
	for _, err := range seqFn(16) {
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row, err := range seqFn(16) {
			if err != nil {
				b.Fatal(err)
			}
			if row.ID == 0 || row.Category != "" {
				b.Fatalf("unexpected row: %+v", row)
			}
		}
	}
}
