package integration

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

// A VALUES list joined into an UPDATE (`WITH v AS (VALUES ...) UPDATE ... FROM v WHERE t.id = v.id`) is
// row-wise, so it is chunked: a collection over the single-statement limit runs as several statements in
// one implicit transaction and applies every delta exactly once; the Stream form feeds it from an iterator.
func TestRealEngineValuesJoinUpdateIsChunked(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("ApplyDeltas is a PostgreSQL-flavor fixture statement")
		}
		setupItems(t, db)
		for i := int64(1); i <= 5; i++ {
			mustExec(t, db, fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', %d, 'new')", i, i, i*10))
		}
		q := pgq.New(db, runtime.WithMaxBatchParams(4)) // two (id, delta) rows per statement
		defer q.Close()
		deltas := []pgq.ScoreDelta{{ID: 5, Delta: 1}, {ID: 1, Delta: 2}, {ID: 3, Delta: 3}, {ID: 2, Delta: 4}, {ID: 4, Delta: 5}}
		res, err := q.ApplyDeltas(ctxT(t), deltas)
		if err != nil {
			t.Fatalf("ApplyDeltas over 3 chunks: %v", err)
		}
		if n, err := res.RowsAffected(); err != nil || n != 5 {
			t.Fatalf("rows affected = %d, %v; want 5", n, err)
		}
		res, err = q.ApplyDeltasStream(ctxT(t), runtime.FromSlice(deltas[:3]))
		if err != nil {
			t.Fatalf("ApplyDeltasStream: %v", err)
		}
		if n, err := res.RowsAffected(); err != nil || n != 3 {
			t.Fatalf("stream rows affected = %d, %v; want 3", n, err)
		}
		got := map[int64]int64{}
		rows, err := db.QueryContext(ctxT(t), "SELECT id, score FROM sg_items")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, score int64
			if err := rows.Scan(&id, &score); err != nil {
				t.Fatal(err)
			}
			got[id] = score
		}
		want := map[int64]int64{1: 14, 2: 24, 3: 36, 4: 45, 5: 52}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("scores = %v, want %v", got, want)
		}
	})
}
