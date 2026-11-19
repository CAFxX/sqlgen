package integration

// A reference WirePipeliner for PostgreSQL-protocol engines. The runtime renders each queued operation
// into server-side-gated SQL (data-modifying CTEs, assertion guards, set_config-recorded conditions);
// a real wire driver would flush those statements in one round trip. Here they are executed one by one
// on the real connection inside the pipeline's transaction, which is enough to prove that the rendered
// SQL is valid for, and behaves identically on, a real server: the wire path must be observationally
// equivalent to the sequential fallback.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/runtime"
)

type wireConnector struct{ base driver.Connector }

func (w *wireConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := w.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &wireConn{Conn: c.(*stdlib.Conn)}, nil
}
func (w *wireConnector) Driver() driver.Driver { return w.base.Driver() }

type wireConn struct{ *stdlib.Conn }

func named(args []any) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

func (c *wireConn) ExecPipeline(ctx context.Context, ops []runtime.PipelineOp) error {
	for i := range ops {
		op := &ops[i]
		if op.Decorators.IsAbort {
			// Aborts are evaluated after the batch; the server-side gate decides whether they fire.
			continue
		}
		var err error
		switch {
		case op.HasResultCols:
			var dRows driver.Rows
			if dRows, err = c.QueryContext(ctx, op.SQL, named(op.Args)); err == nil {
				var rows *sql.Rows
				if rows, err = runtime.RowsFromDriverRows(ctx, dRows); err == nil {
					op.RowCount, err = op.ScanWireRows(rows)
					_ = rows.Close()
				}
			}
		case strings.HasPrefix(op.SQL, "WITH _sqlgen_op AS"):
			// Guarded exec: the batch yields (affected rows, assertion value).
			var dRows driver.Rows
			if dRows, err = c.QueryContext(ctx, op.SQL, named(op.Args)); err == nil {
				dest := make([]driver.Value, 2)
				if err = dRows.Next(dest); err == nil {
					op.RowCount, _ = dest[0].(int64)
				} else if errors.Is(err, io.EOF) {
					err = nil
				}
				_ = dRows.Close()
			}
		default:
			var res driver.Result
			if res, err = c.ExecContext(ctx, op.SQL, named(op.Args)); err == nil {
				op.RowCount, _ = res.RowsAffected()
			}
		}
		if err != nil {
			return runtime.TranslateWireError(op, err)
		}
		if op.Decorators.ProduceCond != nil {
			// Conditions are recorded server-side by set_config('sqlgen.c<ID>', ..., true).
			dRows, qerr := c.QueryContext(ctx, "SELECT current_setting($1, true)", named([]any{fmt.Sprintf("sqlgen.c%d", op.Decorators.CondID)}))
			if qerr != nil {
				return qerr
			}
			dest := make([]driver.Value, 1)
			if err := dRows.Next(dest); err == nil {
				if s, ok := dest[0].(string); ok {
					op.CondVal, op.CondSet = s == "true", true
				}
			}
			_ = dRows.Close()
		}
	}
	return nil
}

func openWire(t *testing.T, dsn string, wire bool) *sql.DB {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	var c driver.Connector = stdlib.GetConnector(*cfg)
	if wire {
		c = runtime.WrapConnector(&wireConnector{base: c})
	}
	db := sql.OpenDB(c)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type outcome struct {
	Err   string
	State string // every row, id-ordered
	Taken []bool // IfResult.Taken() for each If queued by the scenario
}

func snapshot(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query("SELECT id, org_id, name, score, status FROM sg_items ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var id, org, sc int64
		var name, status string
		if err := rows.Scan(&id, &org, &name, &sc, &status); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sb, "%d/%d/%s/%d/%s;", id, org, name, sc, status)
	}
	return sb.String()
}

// Pipelines under test. Each queues work and reports every IfResult so the test can read Taken()
// after the pipeline has run.
type scenario struct {
	name string
	run  func(ctx context.Context, q *pgq.Queries) (ifs []*runtime.IfResult, err error)
}

var errStop = errors.New("scenario abort")

func tx(ctx context.Context, q *pgq.Queries, fn func(p *pgq.Pipeline) []*runtime.IfResult) (out []*runtime.IfResult, err error) {
	err = q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
		out = fn(p)
		return nil
	})
	return
}

var wireScenarios = []scenario{
	{"guarded transfer succeeds", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			p.AddScoreOne(-5, 1).AssertRowsAffected(1)
			p.AddScoreOne(5, 2).AssertRowsAffected(1)
			return nil
		})
	}},
	{"failed assertion rolls the whole batch back", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			p.AddScoreOne(-5, 1).AssertRowsAffected(1)
			p.AddScoreOne(5, 999).AssertRowsAffected(1) // no such row
			return nil
		})
	}},
	{"AssertFound on a row-returning op that finds the row", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			p.GetItem(1).AssertFound()
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			return nil
		})
	}},
	{"AssertFound on a row-returning op that finds nothing", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			p.GetItem(999).AssertFound() // must fire even though the op returns zero rows
			return nil
		})
	}},
	{"if: row found -> then branch", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			get := p.GetItem(1)
			return []*runtime.IfResult{p.If(get.Found(),
				func(p *pgq.Pipeline) { p.AddScoreOne(100, 2).AssertRowsAffected(1) },
				func(p *pgq.Pipeline) { p.AddScoreOne(7, 3).AssertRowsAffected(1) })}
		})
	}},
	{"if: row missing -> else branch", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			get := p.GetItem(12345)
			return []*runtime.IfResult{p.If(get.Found(),
				func(p *pgq.Pipeline) { p.AddScoreOne(100, 2).AssertRowsAffected(1) },
				func(p *pgq.Pipeline) { p.AddScoreOne(7, 3).AssertRowsAffected(1) })}
		})
	}},
	{"if: unguarded else delete", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			get := p.GetItem(12345)
			return []*runtime.IfResult{p.If(get.Found(),
				func(p *pgq.Pipeline) { p.DeleteItems([]int64{1}) },
				func(p *pgq.Pipeline) { p.DeleteItems([]int64{4}) })}
		})
	}},
	{"WhereCol predicate true -> then", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			get := p.GetItem(2) // score 20
			return []*runtime.IfResult{p.If(pgq.WhereCol(get, "score >= 20", func(it pgq.Item) bool { return it.Score >= 20 }),
				func(p *pgq.Pipeline) { p.AddScoreOne(1, 2).AssertRowsAffected(1) },
				func(p *pgq.Pipeline) { p.AddScoreOne(2, 3).AssertRowsAffected(1) })}
		})
	}},
	{"WhereCol predicate false -> else", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			get := p.GetItem(2)
			return []*runtime.IfResult{p.If(pgq.WhereCol(get, "score >= 21", func(it pgq.Item) bool { return it.Score >= 21 }),
				func(p *pgq.Pipeline) { p.AddScoreOne(1, 2).AssertRowsAffected(1) },
				func(p *pgq.Pipeline) { p.AddScoreOne(2, 3).AssertRowsAffected(1) })}
		})
	}},
	{"gated single-row insert (then) / multi-row insert (else)", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			get := p.GetItem(1)
			return []*runtime.IfResult{p.If(get.Found(),
				func(p *pgq.Pipeline) { p.InsertItem(50, 9, "then-row", 5, "new").AssertRowsAffected(1) },
				func(p *pgq.Pipeline) {
					p.InsertItems([]pgq.NewItem{{ID: 60, OrgID: 9, Name: "else-a"}, {ID: 61, OrgID: 9, Name: "else-b"}})
				})}
		})
	}},
	{"gated multi-row insert runs when the else branch is taken", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			get := p.GetItem(777)
			return []*runtime.IfResult{p.If(get.Found(),
				func(p *pgq.Pipeline) { p.InsertItem(50, 9, "then-row", 5, "new").AssertRowsAffected(1) },
				func(p *pgq.Pipeline) {
					p.InsertItems([]pgq.NewItem{{ID: 61, OrgID: 9, Name: "else-b", Status: "x"}, {ID: 60, OrgID: 9, Name: "else-a", Status: "x"}})
				})}
		})
	}},
	{"gated UPDATE without WHERE", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			get := p.GetItem(1)
			return []*runtime.IfResult{p.If(get.Found(),
				func(p *pgq.Pipeline) { p.SetAllStatus("then") },
				func(p *pgq.Pipeline) { p.SetAllStatus("else") })}
		})
	}},
	{"abort in the taken branch rolls everything back", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			get := p.GetItem(1)
			return []*runtime.IfResult{p.If(get.Found(),
				func(p *pgq.Pipeline) { p.Abort(errStop) }, nil)}
		})
	}},
	{"abort in the branch NOT taken is inert", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			get := p.GetItem(1)
			return []*runtime.IfResult{p.If(get.Found(), nil,
				func(p *pgq.Pipeline) { p.Abort(errStop) })}
		})
	}},
	{"nested if", func(ctx context.Context, q *pgq.Queries) ([]*runtime.IfResult, error) {
		return tx(ctx, q, func(p *pgq.Pipeline) []*runtime.IfResult {
			outer := p.GetItem(1)
			var inner *runtime.IfResult
			o := p.If(outer.Found(), func(p *pgq.Pipeline) {
				g2 := p.GetItem(12345)
				inner = p.If(g2.Found(),
					func(p *pgq.Pipeline) { p.AddScoreOne(1000, 1) },
					func(p *pgq.Pipeline) { p.AddScoreOne(5, 4).AssertRowsAffected(1) })
			}, func(p *pgq.Pipeline) { p.AddScoreOne(2000, 1) })
			return []*runtime.IfResult{o, inner}
		})
	}},
}

func TestRealEnginePipelineWireMatchesSequential(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("the single-round-trip pipeline SQL is only generated for the $N (PostgreSQL-protocol) dialect")
		}
		dsn := os.Getenv(e.env)
		for _, sc := range wireScenarios {
			t.Run(strings.ReplaceAll(sc.name, " ", "_"), func(t *testing.T) {
				run := func(wire bool) outcome {
					setupItems(t, db)
					for i := int64(1); i <= 4; i++ {
						mustExec(t, db, fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', %d, 'new')", i, i, i*10))
					}
					wdb := openWire(t, dsn, wire)
					q := pgq.New(wdb)
					defer q.Close()
					ifs, err := sc.run(ctxT(t), q)
					o := outcome{State: snapshot(t, db)}
					for _, r := range ifs {
						if r != nil {
							o.Taken = append(o.Taken, r.Taken())
						}
					}
					if err != nil {
						o.Err = err.Error()
						switch {
						case errors.Is(err, runtime.ErrAssertionFailed):
							o.Err = "ErrAssertionFailed"
						case errors.Is(err, errStop):
							o.Err = "errStop"
						}
					}
					return o
				}
				seq, wire := run(false), run(true)
				t.Logf("sequential: %+v", seq)
				t.Logf("wire:       %+v", wire)
				if seq.Err != wire.Err || seq.State != wire.State || !slices.Equal(seq.Taken, wire.Taken) {
					t.Fatalf("wire pipeline diverges from sequential fallback:\n  sequential %+v\n  wire       %+v", seq, wire)
				}
			})
		}
	})
}
