package sqlgen_test

import (
	"context"
	"errors"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// Value forwarding on the sequential path (SQLite): the semantics the wire paths are compared against
// (integration/wire_*_test.go).

func TestForwardSequential(t *testing.T) {
	ctx := context.Background()
	db, q := setupE2EDB(t)
	count := func(table string) int {
		t.Helper()
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	t.Run("one row binds the value, every method shape", func(t *testing.T) {
		var order *runtime.RowFuture[int64]
		var n *runtime.RowFuture[int64]
		var skus *runtime.SliceFuture[string]
		err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			order = p.FwdInsertOrder("alice", 10)
			id := order.Arg()
			if !id.Forwarded() || runtime.Val(int64(1)).Forwarded() {
				t.Fatal("Forwarded")
			}
			p.FwdInsertLine(id, "a", 2).AssertRows(1)
			p.FwdInsertLine(order.Arg(), "b", 1).AssertRows(1)
			p.FwdInsertLineStruct(FwdInsertLineStructPipelineParams{OrderID: id, SKU: runtime.Val("c"), Qty: 3}).AssertRows(1)
			n = p.FwdCountLines(FwdCountLinesPipelineParams{OrderID: id})
			skus = p.FwdLineSkus(id, runtime.Val(int64(2)))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		id, err := order.Get()
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := n.Get(); got != 3 {
			t.Fatalf("count = %d", got)
		}
		if got, _ := skus.Get(); len(got) != 2 || got[0] != "a" || got[1] != "c" {
			t.Fatalf("skus = %v", got)
		}
		if got := fwdLinesOf(t, q, id); len(got) != 3 {
			t.Fatalf("lines = %v", got)
		}
	})

	t.Run("plain values", func(t *testing.T) {
		if err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			p.FwdInsertLine(runtime.Val(int64(999)), "plain", 1).AssertRows(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if got := fwdLinesOf(t, q, 999); len(got) != 1 || got[0] != "plain" {
			t.Fatalf("lines = %v", got)
		}
	})

	t.Run("zero and two rows fail the consumer and roll back", func(t *testing.T) {
		if _, err := q.FwdInsertOrder(ctx, "twice", 1); err != nil {
			t.Fatal(err)
		}
		if _, err := q.FwdInsertOrder(ctx, "twice", 2); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			customer string
			got      int64
		}{{"nobody", 0}, {"twice", 2}} {
			before := count("fwd_lines")
			var line *runtime.ExecFuture
			err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
				p.FwdInsertLine(runtime.Val(int64(1)), "before", 1) // runs, then rolled back
				o := p.FwdOrderByCustomer(tc.customer).Label("lookup")
				line = p.FwdInsertLine(o.Arg(), "x", 1).Label("line")
				return nil
			})
			var ae *runtime.ArgError
			if !errors.As(err, &ae) || !errors.Is(err, runtime.ErrArgUnresolved) {
				t.Fatalf("%s: err = %v", tc.customer, err)
			}
			want := runtime.ArgError{Statement: "FwdInsertLine", Op: 2, Label: "line", Producer: "FwdOrderByCustomer", ProducerOp: 1, ProducerLabel: "lookup", Got: tc.got, GotKnown: true}
			if *ae != want {
				t.Fatalf("%s: %+v, want %+v", tc.customer, *ae, want)
			}
			if _, ferr := line.RowsAffected(); ferr != err {
				t.Fatalf("the consumer's future reports %v", ferr)
			}
			if count("fwd_lines") != before {
				t.Fatal("not rolled back")
			}
			if ae.Error() == "" {
				t.Fatal("message")
			}
		}
	})

	t.Run("a consumer in an untaken branch is silent", func(t *testing.T) {
		var ifr *runtime.IfResult
		err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			o := p.FwdOrderByCustomer("nobody")
			ifr = p.If(o.Found(), func(t *Pipeline) {
				t.FwdInsertLine(o.Arg(), "never", 1)
			}, nil)
			return nil
		})
		if err != nil || ifr.Taken() {
			t.Fatalf("err = %v taken = %v", err, ifr.Taken())
		}
	})

	t.Run("an assertion on the producer fails first", func(t *testing.T) {
		err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			o := p.FwdOrderByCustomer("nobody").AssertFound()
			p.FwdInsertLine(o.Arg(), "never", 1)
			return nil
		})
		if !errors.Is(err, runtime.ErrAssertionFailed) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("LastInsertIDArg", func(t *testing.T) {
		var lineID int64
		err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			id := p.FwdInsertOrderPlain(nil, "bob", 5).AssertRows(1).LastInsertIDArg()
			p.FwdInsertLine(id, "lid", 1).AssertRows(1)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow("SELECT order_id FROM fwd_lines WHERE sku = 'lid'").Scan(&lineID); err != nil {
			t.Fatal(err)
		}
		var orderID int64
		if err := db.QueryRow("SELECT id FROM fwd_orders WHERE customer = 'bob'").Scan(&orderID); err != nil || orderID != lineID {
			t.Fatalf("order %d line %d (%v)", orderID, lineID, err)
		}
		// an INSERT OR IGNORE that inserted nothing: no id
		err = q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			id := p.FwdInsertOrderPlain(&orderID, "bob", 5).LastInsertIDArg()
			p.FwdInsertLine(id, "never", 1)
			return nil
		})
		var ae *runtime.ArgError
		if !errors.As(err, &ae) || !ae.LastInsertID || ae.Got != 0 || !ae.GotKnown {
			t.Fatalf("err = %v", err)
		}
		// an empty VALUES list (a NoOp operation): no id, on the sequential path
		err = q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			id := p.FwdInsertOrders(nil).LastInsertIDArg()
			p.FwdInsertLine(id, "never", 1)
			return nil
		})
		if !errors.As(err, &ae) || ae.Got != 0 {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("a user-managed transaction stays usable", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		p := q.WithTx(tx).Pipeline(ctx)
		p.FwdInsertLine(runtime.Val(int64(77)), "kept", 1)
		o := p.FwdOrderByCustomer("nobody")
		p.FwdInsertLine(o.Arg(), "never", 1)
		if err := p.Flush(); !errors.Is(err, runtime.ErrArgUnresolved) {
			t.Fatalf("err = %v", err)
		}
		var n int
		if err := tx.QueryRow("SELECT COUNT(*) FROM fwd_lines WHERE order_id = 77").Scan(&n); err != nil || n != 1 {
			t.Fatalf("n = %d, %v", n, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("build errors: branch scope, another pipeline, several columns", func(t *testing.T) {
		before := count("fwd_lines")
		err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			o := p.FwdOrderByCustomer("alice")
			var inner runtime.Arg[int64]
			p.If(o.Found(), func(t *Pipeline) {
				inner = t.FwdInsertOrder("carol", 1).Arg()
			}, nil)
			p.FwdInsertLine(inner, "outside", 1)
			return nil
		})
		if !errors.Is(err, runtime.ErrInvalidArgument) || count("fwd_lines") != before {
			t.Fatalf("outside the branch: %v", err)
		}
		err = q.PipelineTx(ctx, nil, func(p *Pipeline) error {
			o := p.FwdOrderByCustomer("alice")
			var then runtime.Arg[int64]
			p.If(o.Found(), func(t *Pipeline) {
				then = t.FwdInsertOrder("carol", 1).Arg()
			}, func(e *Pipeline) {
				e.FwdInsertLine(then, "sibling", 1)
			})
			return nil
		})
		if !errors.Is(err, runtime.ErrInvalidArgument) {
			t.Fatalf("sibling branch: %v", err)
		}
		p1 := q.Pipeline(ctx)
		stale := p1.FwdInsertOrder("dave", 1).Arg()
		p2 := q.Pipeline(ctx)
		p2.FwdInsertLine(stale, "stale", 1)
		if err := p2.Flush(); !errors.Is(err, runtime.ErrInvalidArgument) {
			t.Fatalf("another pipeline: %v", err)
		}
		p3 := q.Pipeline(ctx)
		_ = p3.GetUserByID(1).Arg()
		if err := p3.Flush(); !errors.Is(err, runtime.ErrInvalidArgument) {
			t.Fatalf("several columns: %v", err)
		}
		if count("fwd_lines") != before {
			t.Fatal("a build error sent something")
		}
	})
}

func fwdLinesOf(t *testing.T, q *Queries, orderID int64) []string {
	t.Helper()
	skus, err := q.FwdLineSkus(context.Background(), orderID, 0)
	if err != nil {
		t.Fatal(err)
	}
	return skus
}
