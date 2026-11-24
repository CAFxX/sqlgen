package integration

import (
	"database/sql"
	"fmt"
	"slices"
	"testing"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

// TestRealEngineWidestOrder pages a sqlgen.Order over the widest column list the generator accepts
// (runtime.MaxOrderCols), naming every column with scrambled directions, and checks the page sequence
// against a reference sort computed in Go. A truncated Order would reorder ties or repeat/skip rows.
func TestRealEngineWidestOrder(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		const n = runtime.MaxOrderCols
		ddl := "CREATE TABLE sg_wide (id BIGINT PRIMARY KEY"
		for i := 1; i < n; i++ {
			ddl += fmt.Sprintf(", c%d BIGINT NOT NULL", i)
		}
		mustExec(t, db, "DROP TABLE IF EXISTS sg_wide")
		mustExec(t, db, ddl+")")
		t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS sg_wide") })

		// Rows differ only in the later columns, so a sort that drops a term mis-orders them.
		type row [n]int64 // c1..c11, id
		var rows []row
		for id := int64(0); id < 300; id++ {
			var r row
			for c := 0; c < n-1; c++ {
				// c1..c7 constant; the last columns carry the entropy.
				if c >= 7 {
					r[c] = (id * int64(c+3) / 7) % 4
				}
			}
			r[n-1] = id
			rows = append(rows, r)
			args := make([]any, 0, n)
			ph := ""
			for c := 0; c < n; c++ {
				args = append(args, r[c])
				if c > 0 {
					ph += ", "
				}
				if e.isPG() {
					ph += fmt.Sprintf("$%d", c+1)
				} else {
					ph += "?"
				}
			}
			colsList := "c1"
			for c := 2; c < n; c++ {
				colsList += fmt.Sprintf(", c%d", c)
			}
			mustExec(t, db, "INSERT INTO sg_wide ("+colsList+", id) VALUES ("+ph+")", args...)
		}

		// Sort by every column in declared order, mixing directions.
		desc := func(i int) bool { return i%3 == 1 }
		want := slices.Clone(rows)
		slices.SortFunc(want, func(a, b row) int {
			for i := 0; i < n; i++ {
				if a[i] == b[i] {
					continue
				}
				c := 1
				if a[i] < b[i] {
					c = -1
				}
				if desc(i) {
					c = -c
				}
				return c
			}
			return 0
		})

		var pgTerms []pgq.WideRowsSortTerm
		var myTerms []myq.WideRowsSortTerm
		for i := 0; i < n; i++ {
			if desc(i) {
				pgTerms = append(pgTerms, pgq.WideRowsSortCol(i).Desc())
				myTerms = append(myTerms, myq.WideRowsSortCol(i).Desc())
			} else {
				pgTerms = append(pgTerms, pgq.WideRowsSortCol(i).Asc())
				myTerms = append(myTerms, myq.WideRowsSortCol(i).Asc())
			}
		}

		var got []row
		page := func(after runtime.Seek) (rs []row, next runtime.Seek, err error) {
			if e.isPG() {
				r, err := pgq.New(db).WideRows(ctxT(t), pgq.WideRowsParams{After: after, Sort: pgq.OrderWideRows(pgTerms...), Limit: 7})
				for _, w := range r {
					rs = append(rs, row{w.C1, w.C2, w.C3, w.C4, w.C5, w.C6, w.C7, w.C8, w.C9, w.C10, w.C11, w.ID})
				}
				if len(r) > 0 {
					next = pgq.SeekWideRows(r[len(r)-1])
				}
				return rs, next, err
			}
			r, err := myq.New(db).WideRows(ctxT(t), myq.WideRowsParams{After: after, Sort: myq.OrderWideRows(myTerms...), Limit: 7})
			for _, w := range r {
				rs = append(rs, row{w.C1, w.C2, w.C3, w.C4, w.C5, w.C6, w.C7, w.C8, w.C9, w.C10, w.C11, w.ID})
			}
			if len(r) > 0 {
				next = myq.SeekWideRows(r[len(r)-1])
			}
			return rs, next, err
		}
		var cursor runtime.Seek
		for {
			rs, next, err := page(cursor)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, rs...)
			if len(rs) < 7 {
				break
			}
			cursor = next
		}
		if len(got) != len(want) {
			t.Fatalf("paged %d rows, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("row %d: got %v want %v", i, got[i], want[i])
			}
		}
	})
}
