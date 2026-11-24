package integration

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Pins the engine facts DOCS.md "Engine support" states: which engines accept RETURNING (and on which
// verbs), and that go-sql-driver/mysql needs parseTime=true to scan a DATETIME into a time.Time.
func TestRealEngineReturningSupport(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		tbl := makeTable(t, db, "sg_ret", "CREATE TABLE %s (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
		ph := func(n int) string {
			if e.isPG() {
				return fmt.Sprintf("$%d", n)
			}
			return "?"
		}
		returns := func(q string, args ...any) error {
			rows, err := db.Query(q, args...)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			n := 0
			for rows.Next() {
				n++
			}
			if err := rows.Err(); err != nil {
				return err
			}
			if n != 1 {
				return fmt.Errorf("RETURNING produced %d rows, want 1", n)
			}
			return nil
		}
		ins := returns("INSERT INTO "+tbl+" (id, v) VALUES ("+ph(1)+", "+ph(2)+") RETURNING id", 1, 10)
		upd := returns("UPDATE "+tbl+" SET v = v + 1 WHERE id = "+ph(1)+" RETURNING v", 1)
		del := returns("DELETE FROM "+tbl+" WHERE id = "+ph(1)+" RETURNING id", 1)
		want := map[family][3]bool{ // insert, update, delete
			famPostgres:  {true, true, true},
			famCockroach: {true, true, true},
			famMySQL:     {false, false, false},
			famMariaDB:   {true, false, true},
			famTiDB:      {false, false, false},
			famYugabyte:  {true, true, true},
		}[e.family]
		for i, c := range []struct {
			verb string
			err  error
		}{{"INSERT", ins}, {"UPDATE", upd}, {"DELETE", del}} {
			if ok := c.err == nil; ok != want[i] {
				t.Errorf("%s ... RETURNING on %s: err = %v, want success=%v", c.verb, e.name, c.err, want[i])
			}
		}
	})
}

func TestRealEngineMySQLTimeNeedsParseTime(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("go-sql-driver/mysql only")
		}
		tbl := makeTable(t, db, "sg_time", "CREATE TABLE %s (id BIGINT PRIMARY KEY, at DATETIME NOT NULL)")
		mustExec(t, db, "INSERT INTO "+tbl+" VALUES (1, '2024-05-06 07:08:09')")

		var got time.Time
		err := db.QueryRow("SELECT at FROM " + tbl).Scan(&got)
		if err == nil || !strings.Contains(err.Error(), "time.Time") {
			t.Fatalf("without parseTime=true scanning into time.Time: err = %v, want a time.Time scan error", err)
		}

		dsn := os.Getenv(e.env)
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		pdb, err := sql.Open("mysql", dsn+sep+"parseTime=true")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = pdb.Close() }()
		if err := pdb.QueryRow("SELECT at FROM " + tbl).Scan(&got); err != nil {
			t.Fatalf("with parseTime=true: %v", err)
		}
		if want := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}
