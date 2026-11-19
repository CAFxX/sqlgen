package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// Real-engine micro-benchmarks: the overhead of a generated call over hand-written database/sql, on a
// real server. Run with:  make bench-engines ENGINES=postgres18,mysql84
//
// Variants per engine:
//   raw-unprepared   db.QueryRowContext(sql, id)          (what a plain database/sql user writes)
//   raw-prepared     a long-lived *sql.Stmt                (the best a hand-written caller can do)
//   sqlgen           generated Get, default cache          (promotes hot statements to Tier 2)
//   sqlgen-nocache   generated Get, prepared statements off (Tier-1 SQL cache only)

func forEachBenchEngine(b *testing.B, fn func(b *testing.B, e engine, db *sql.DB)) {
	selected := selectedEngines()
	for _, e := range engines {
		if selected == nil || !selected[e.name] {
			continue // benchmarks are opt-in: they only run for explicitly selected engines
		}
		b.Run(e.name, func(b *testing.B) {
			dsn := os.Getenv(e.env)
			if dsn == "" {
				b.Skipf("%s not set", e.env)
			}
			db, err := sql.Open(e.driver, dsn)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(8)
			db.SetMaxIdleConns(8)
			fn(b, e, db)
		})
	}
}

func benchSetup(b *testing.B, db *sql.DB) {
	b.Helper()
	for _, q := range []string{"DROP TABLE IF EXISTS sg_items", itemsDDL} {
		if _, err := db.Exec(q); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS sg_items") })
}

func BenchmarkGetByID(b *testing.B) {
	forEachBenchEngine(b, func(b *testing.B, e engine, db *sql.DB) {
		benchSetup(b, db)
		ctx := context.Background()
		seed := newAPI(e, db)
		if err := seed.Insert(ctx, genItems(1, 1000)); err != nil {
			b.Fatal(err)
		}
		_ = seed.Close()

		ph := "?"
		if e.isPG() {
			ph = "$1"
		}
		query := fmt.Sprintf("SELECT id, org_id, name, score, status FROM sg_items WHERE id = %s", ph)
		scan := func(r *sql.Row) (item, error) {
			var it item
			err := r.Scan(&it.ID, &it.OrgID, &it.Name, &it.Score, &it.Status)
			return it, err
		}

		b.Run("raw-unprepared", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := scan(db.QueryRowContext(ctx, query, int64(i%1000+1))); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("raw-prepared", func(b *testing.B) {
			st, err := db.PrepareContext(ctx, query)
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := scan(st.QueryRowContext(ctx, int64(i%1000+1))); err != nil {
					b.Fatal(err)
				}
			}
		})
		for _, v := range []struct {
			name string
			opts []runtime.CacheOption
		}{
			{"sqlgen", nil},
			{"sqlgen-promote-1", []runtime.CacheOption{runtime.WithPromoteThreshold(1)}},
		} {
			b.Run(v.name, func(b *testing.B) {
				a := newAPI(e, db, v.opts...)
				defer a.Close()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := a.Get(ctx, int64(i%1000+1)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
}
