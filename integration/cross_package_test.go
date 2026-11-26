package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/CAFxX/sqlgen/integration/fzpg"
	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

// Two generated packages can construct child queries independently, and the receiving package's
// prepared/SQL cache must distinguish them when their query trees are composed into one parent.
func TestCrossPackageQueryCompositionKeepsCacheEntriesDistinct(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL dialect query contracts")
		}
		setupItems(t, db)
		mustExec(t, db, "DROP TABLE IF EXISTS fz_rows")
		t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS fz_rows") })
		mustExec(t, db, "CREATE TABLE fz_rows (id BIGINT PRIMARY KEY, grp BIGINT NOT NULL, k1 BIGINT NOT NULL)")
		mustExec(t, db, "INSERT INTO sg_items (id, org_id, name, score, status) VALUES (302, 1, 'picked elsewhere', 10, 'new')")
		mustExec(t, db, "INSERT INTO fz_rows (id, grp, k1) VALUES (301, 7, 11)")

		ctx := ctxT(t)
		parent := fzpg.New(db)
		defer parent.Close()

		// These child queries come from different generated modules.
		fromItems := pgq.ItemsByIDsQuery([]int64{302})
		fromGroups := fzpg.FzSubGrpsQuery(fzpg.FzSubGrpsParams{Grps: []int64{7}})

		// Simulate an older generated module with the same local statement ordinal and shape.
		// Its legacy shape helper must mark the tag inexact, making the receiving parent cache
		// by rendered SQL rather than treating the package-local ordinal as globally unique.
		fromGroups.StmtID = fromItems.StmtID
		originalDescID := fromGroups.Desc.StmtID
		fromGroups.Desc.StmtID = fromItems.StmtID
		defer func() { fromGroups.Desc.StmtID = originalDescID }()
		fromGroups.ShapeTag = runtime.ComputeSubqueryShape(
			fromGroups.StmtID,
			fromGroups.Input.ColMask,
			fromGroups.Input.FlagMask,
			runtime.EncodeOrderKey(fromGroups.Input.OrderSpec, fromGroups.Input.HasSeek),
			fromGroups.Desc.NumSubs > 0,
			fromGroups.Input.SliceLens...,
		)
		if fromItems.StmtID != fromGroups.StmtID {
			t.Fatal("test setup did not create equal package-local statement ordinals")
		}
		if fromItems.ShapeTag == fromGroups.ShapeTag || fromGroups.ShapeTag&(uint64(1)<<63) == 0 {
			t.Fatalf("legacy child shape was not made inexact: new=%#x legacy=%#x", fromItems.ShapeTag, fromGroups.ShapeTag)
		}

		run := func(child runtime.Query) []fzpg.FzAggRow {
			got, err := parent.FzAgg(ctx, fzpg.FzAggParams{Pick: child})
			if err != nil {
				t.Fatalf("FzAgg: %v", err)
			}
			return got
		}

		// Warm both parent variants, then alternate them. If two child shapes are accidentally
		// treated as the same cache key, the second/warmed parent can render the first child SQL.
		for i := 0; i < 3; i++ {
			items := run(fromItems)
			groups := run(fromGroups)
			if len(items) != 0 {
				t.Fatalf("item query should select an ID absent from fz_rows, got %+v", items)
			}
			if len(groups) != 1 || groups[0].Grp != 7 || groups[0].N != 1 || groups[0].Total != 11 {
				t.Fatalf("group query composition mismatch: %+v", groups)
			}
		}
	})
}

// A PipelineTx created by one generated package accepts operations queued by another generated
// package. On pgxwire, all of them must still be in the owner's single transaction and one wire turn.
func TestCrossPackagePipelineTxUsesOneWireRoundTrip(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		setupItems(t, db)
		mustExec(t, db, "DROP TABLE IF EXISTS fz_rows")
		t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS fz_rows") })
		mustExec(t, db, "CREATE TABLE fz_rows (id BIGINT PRIMARY KEY, grp BIGINT NOT NULL, k1 BIGINT NOT NULL, k2 BIGINT NOT NULL)")
		mustExec(t, db, "INSERT INTO sg_items (id, org_id, name, score, status) VALUES (1, 1, 'owner-module', 10, 'new')")
		mustExec(t, db, "INSERT INTO fz_rows (id, grp, k1, k2) VALUES (9001, 1, 5, 20)")

		wdb, turns, _ := countingDB(t, os.Getenv(e.env), true)
		owner := pgq.New(wdb)
		defer owner.Close()
		ctx := ctxT(t)
		var opErr error
		roundTrips := measure(t, wdb, turns, func() {
			opErr = owner.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
				p.AddScoreOne(3, 1).AssertRows(1)

				inventory := fzpg.WrapPipeline(p.Pipeline)
				inventory.FzBump(fzpg.FzBumpParams{
					Delta: 2,
					IDs:   []int64{9001},
				}).AssertRows(1)
				return nil
			})
		})
		if opErr != nil {
			t.Fatalf("cross-package PipelineTx: %v", opErr)
		}
		if roundTrips != 1 {
			t.Fatalf("cross-package PipelineTx used %d wire turns, want exactly 1", roundTrips)
		}
		stats := owner.CacheStats()
		if stats.WirePipelines != 1 || stats.SequentialPipelines != 0 {
			t.Fatalf("expected one wire pipeline and no sequential fallback, got %+v", stats)
		}

		var score, k1, k2 int64
		if err := wdb.QueryRow("SELECT score FROM sg_items WHERE id = 1").Scan(&score); err != nil {
			t.Fatal(err)
		}
		if err := wdb.QueryRow("SELECT k1, k2 FROM fz_rows WHERE id = 9001").Scan(&k1, &k2); err != nil {
			t.Fatal(err)
		}
		if score != 13 || k1 != 7 || k2 != 18 {
			t.Fatalf("cross-package transaction results: score=%d k1=%d k2=%d", score, k1, k2)
		}
	})
}

