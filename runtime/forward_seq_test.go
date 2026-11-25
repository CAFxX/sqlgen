package runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"testing"
)

// The sequential path of value forwarding against SQLite, at the runtime level (the generated-code view
// is in the root module's forward_test.go).

func fwdSQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "fwd.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, customer TEXT NOT NULL);
		CREATE TABLE lines (order_id INTEGER, sku TEXT, qty INTEGER);`); err != nil {
		t.Fatal(err)
	}
	return db
}

func scanInt(r RowScanner, d *int64) error { return r.Scan(d) }

func qRow(p *Pipeline, desc *QueryDescriptor, args ...any) *RowFuture[int64] {
	return QueueRow[int64](p, desc.StmtID, desc, RenderInput{}, MakeVariantKey(desc.StmtID, 0, 0, 0), nil, &args, nil, scanInt)
}

func TestForwardSequentialRuntime(t *testing.T) {
	ctx := context.Background()
	db := fwdSQLite(t)
	cache := NewStmtCache()
	run := func(fn func(p *Pipeline) error) error {
		return RunPipelineTx(ctx, db, cache, nil, false, DialectQuestion, nil, nil, fn)
	}
	lines := func() int {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM lines").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	var order *RowFuture[int64]
	if err := run(func(p *Pipeline) error {
		order = qRow(p, fwdInsertOrder, "alice")
		lid := fwdPlain(p, fwdInsertPlain, "bob")
		fwdExec(p, fwdInsertLine, order.Arg().Bind(), Val("a").Bind(), lid.LastInsertIDArg().Bind()).AssertRows(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var orderID, qty int64
	if err := db.QueryRow("SELECT order_id, qty FROM lines").Scan(&orderID, &qty); err != nil || orderID != 1 || qty != 2 {
		t.Fatalf("line (%d, %d) %v", orderID, qty, err)
	}

	// two rows, the consumer fails and everything rolls back
	if _, err := db.Exec("INSERT INTO orders (customer) VALUES ('twice'), ('twice')"); err != nil {
		t.Fatal(err)
	}
	err := run(func(p *Pipeline) error {
		fwdExec(p, fwdInsertLine, int64(0), "first", int64(1))
		o := qRow(p, fwdGetOrder, "twice")
		fwdExec(p, fwdInsertLine, o.Arg().Bind(), "x", int64(1))
		return nil
	})
	var ae *ArgError
	if !errors.As(err, &ae) || ae.Got != 2 || ae.Op != 2 || ae.ProducerOp != 1 || lines() != 1 {
		t.Fatalf("err = %v, lines = %d", err, lines())
	}

	// branch scope: nested is fine; outside or in the sibling branch is a build error
	if err := run(func(p *Pipeline) error {
		o := qRow(p, fwdGetOrder, "alice")
		p.If(o.Found(), func(p *Pipeline) {
			inner := qRow(p, fwdGetOrder, "alice")
			p.If(inner.Found(), func(p *Pipeline) { fwdExec(p, fwdInsertLine, inner.Arg().Bind(), "nested", int64(1)) })
		})
		return nil
	}); err != nil || lines() != 2 {
		t.Fatalf("nested: %v", err)
	}
	for name, fn := range map[string]func(p *Pipeline){
		"outside": func(p *Pipeline) {
			var a Arg[int64]
			o := qRow(p, fwdGetOrder, "alice")
			p.If(o.Found(), func(p *Pipeline) { a = qRow(p, fwdGetOrder, "alice").Arg() })
			fwdExec(p, fwdInsertLine, a.Bind(), "x", int64(1))
		},
		"sibling": func(p *Pipeline) {
			var a Arg[int64]
			o := qRow(p, fwdGetOrder, "alice")
			p.If(o.Found(), func(p *Pipeline) { a = qRow(p, fwdGetOrder, "alice").Arg() }).Else(func(p *Pipeline) { fwdExec(p, fwdInsertLine, a.Bind(), "x", int64(1)) })
		},
	} {
		if err := run(func(p *Pipeline) error { fn(p); return nil }); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if lines() != 2 {
		t.Fatal("a build error sent something")
	}
}

// A wire connection of the PostgreSQL flavor cannot carry forwarded values of the question dialect: the
// pipeline runs sequentially (nothing is sent to the pipeliner).
func TestForwardWireFallsBackWithoutDollar(t *testing.T) {
	for _, tx := range []bool{false, true} {
		cfg := &mockDriverConfig{enableWire: !tx, enableWireTx: tx, rowsCols: []string{"id"}, rowsData: [][]driver.Value{{int64(7)}}}
		db := newMockDB(t, cfg)
		cache := NewStmtCache()
		var err error
		if tx {
			err = RunPipelineTx(context.Background(), db, cache, nil, false, DialectQuestion, nil, nil, func(p *Pipeline) error {
				fwdExec(p, fwdInsertLine, qRow(p, fwdGetOrder, "a").Arg().Bind(), "s", int64(1))
				return nil
			})
		} else {
			p := NewPipeline(context.Background(), db, cache, nil, false, DialectQuestion)
			fwdExec(p, fwdInsertLine, qRow(p, fwdGetOrder, "a").Arg().Bind(), "s", int64(1))
			err = p.Flush()
		}
		if err != nil {
			t.Fatal(err)
		}
		if st := cache.Stats(); st.SequentialPipelines != 1 || st.WirePipelines != 0 || cfg.wireCalls.Load() != 0 {
			t.Fatalf("tx=%v: %+v, wire calls %d", tx, st, cfg.wireCalls.Load())
		}
	}
}
