package integration

// Empty collections. An empty multi-row VALUES collection that is the row source of an INSERT is a
// successful no-op (exec: RowsAffected 0; many: no rows; nothing is sent, no transaction is opened).
// Every other empty collection (IN lists, tuple IN lists, a VALUES list that is not the INSERT row
// source) stays runtime.ErrEmptySliceParam. The no-op is observed at the database/sql driver boundary
// (statements, prepares and transactions are counted) so "nothing is sent" is asserted, not inferred.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// driverOps counts what reaches the driver: executes, queries, prepares and transaction begins.
type driverOps struct{ n atomic.Int64 }

type countingConnector struct {
	drv driver.Driver
	dsn string
	ops *driverOps
}

func (c countingConnector) Connect(context.Context) (driver.Conn, error) {
	cn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: cn, ops: c.ops}, nil
}
func (c countingConnector) Driver() driver.Driver { return c.drv }

type countingConn struct {
	driver.Conn
	ops *driverOps
}

func (c *countingConn) Prepare(q string) (driver.Stmt, error) {
	c.ops.n.Add(1)
	return c.Conn.Prepare(q)
}
func (c *countingConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	c.ops.n.Add(1)
	if p, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return p.PrepareContext(ctx, q)
	}
	return c.Conn.Prepare(q)
}
func (c *countingConn) Begin() (driver.Tx, error) { c.ops.n.Add(1); return c.Conn.Begin() } //nolint:staticcheck
func (c *countingConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	c.ops.n.Add(1)
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, o)
	}
	return c.Conn.Begin() //nolint:staticcheck
}
func (c *countingConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	e, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	c.ops.n.Add(1)
	return e.ExecContext(ctx, q, args)
}
func (c *countingConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	e, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	c.ops.n.Add(1)
	return e.QueryContext(ctx, q, args)
}
func (c *countingConn) CheckNamedValue(nv *driver.NamedValue) error {
	if k, ok := c.Conn.(driver.NamedValueChecker); ok {
		return k.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}
func (c *countingConn) ResetSession(ctx context.Context) error {
	if r, ok := c.Conn.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}
func (c *countingConn) IsValid() bool {
	if v, ok := c.Conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

// forEachCountingEngine runs fn against every configured engine plus SQLite, with a second *sql.DB on the
// same database whose driver operations are counted.
func forEachCountingEngine(t *testing.T, fn func(t *testing.T, e engine, db, counted *sql.DB, ops *driverOps)) {
	t.Helper()
	open := func(t *testing.T, e engine, dsn string) (*sql.DB, *sql.DB, *driverOps) {
		db, err := sql.Open(e.driver, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		ops := new(driverOps)
		counted := sql.OpenDB(countingConnector{drv: db.Driver(), dsn: dsn, ops: ops})
		t.Cleanup(func() { _ = counted.Close() })
		return db, counted, ops
	}
	forEachEngine(t, func(t *testing.T, e engine, _ *sql.DB) {
		db, counted, ops := open(t, e, os.Getenv(e.env))
		fn(t, e, db, counted, ops)
	})
	if sel := selectedEngines(); sel != nil && !sel["sqlite"] {
		return
	}
	t.Run("sqlite", func(t *testing.T) {
		db, counted, ops := open(t, sqliteEngine, "file:"+filepath.Join(t.TempDir(), "empty.db")+"?_pragma=busy_timeout(20000)")
		fn(t, sqliteEngine, db, counted, ops)
	})
}

func TestRealEngineEmptyCollections(t *testing.T) {
	forEachCountingEngine(t, func(t *testing.T, e engine, db, counted *sql.DB, ops *driverOps) {
		setupFz(t, db)
		f := newFz(t, e, db, 7)
		ctx := f.ctx
		f.reset(nil)
		a, ret := newFzAPI(e, counted)
		defer a.Close()
		retID, _ := fzRetSupport(e)

		// A control: a real insert is visible to the counter (so a zero below means something).
		seed := f.genRows(5)
		if _, err := a.Insert(ctx, fzIns{Order: 0, Path: 0}, seed); err != nil {
			t.Fatal(err)
		}
		if ops.n.Load() == 0 {
			t.Fatal("the counting driver saw nothing of a real insert")
		}
		ops.n.Store(0)

		// Empty INSERT ... VALUES: exec through the base method (presorted ASC / DESC / raw / literal template)
		// and through Stream: RowsAffected 0, no error, nothing sent.
		for order := 0; order < 4; order++ {
			for path := 0; path < 2; path++ {
				for _, in := range [][]fzRow{nil, {}} {
					aff, err := a.Insert(ctx, fzIns{Order: order, Path: path}, in)
					if err != nil || aff != 0 {
						t.Errorf("empty insert order %d path %d (nil=%v): affected %d, err %v", order, path, in == nil, aff, err)
					}
				}
			}
		}
		if err := a.InsertStreamErr(ctx, nil, 0, nil); err != nil {
			t.Errorf("empty stream: %v", err)
		}
		if aff, err := a.TypedInsert(ctx, nil); err != nil || aff != 0 {
			t.Errorf("empty typed insert: affected %d, err %v", aff, err)
		}

		// Empty INSERT ... VALUES ... RETURNING (many): base, Stream, StreamSeq give no rows, no error.
		if retID {
			for mode := 0; mode < 3; mode++ {
				out, err := ret.InsertRet(ctx, nil, mode)
				if err != nil || len(out) != 0 {
					t.Errorf("empty InsertRet mode %d: %v, err %v", mode, out, err)
				}
			}
		}

		// Every other empty collection is still an error: IN list, tuple IN list, UPDATE / DELETE ... IN.
		if _, err := a.ByIDs(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Errorf("empty IN list (SELECT): err = %v, want ErrEmptySliceParam", err)
		}
		if _, err := a.ByKeys(ctx, [][2]int64{}); !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Errorf("empty tuple IN list: err = %v, want ErrEmptySliceParam", err)
		}
		if _, err := a.Bump(ctx, 1, nil, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Errorf("empty IN list (UPDATE): err = %v, want ErrEmptySliceParam", err)
		}
		if _, err := a.Delete(ctx, []int64{}, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
			t.Errorf("empty IN list (DELETE): err = %v, want ErrEmptySliceParam", err)
		}
		if retID {
			if _, err := ret.DeleteRet(ctx, nil, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
				t.Errorf("empty IN list (DELETE RETURNING): err = %v, want ErrEmptySliceParam", err)
			}
		}
		for name, v := range map[string]fzVar{"ByIDsV": a.ByIDsV(ctx, nil), "ByKeysV": a.ByKeysV(ctx, nil)} {
			if _, err := v.Into(0, 0); !errors.Is(err, runtime.ErrEmptySliceParam) {
				t.Errorf("%s AppendTo: err = %v, want ErrEmptySliceParam", name, err)
			}
			for _, err := range v.Seq() {
				if !errors.Is(err, runtime.ErrEmptySliceParam) {
					t.Errorf("%s Seq: err = %v, want ErrEmptySliceParam", name, err)
				}
			}
			if err := v.Each(func(fzRow) error { return nil }); !errors.Is(err, runtime.ErrEmptySliceParam) {
				t.Errorf("%s Each: err = %v, want ErrEmptySliceParam", name, err)
			}
		}

		if n := ops.n.Load(); n != 0 {
			t.Errorf("%d driver operations reached the database for empty collections, want 0", n)
		}
		if d := diffRows(f.dump(), sortByID(seed)); d != "" {
			t.Errorf("table changed by empty operations: %s", d)
		}
	})
}

// A chunked base call is atomic: when a later chunk fails (here a duplicate primary key in the last chunk) the
// implicit transaction rolls back and nothing of the earlier chunks stays. Also through RETURNING.
func TestRealEngineChunkedInsertRollsBackOnMidChunkFailure(t *testing.T) {
	seed := fuzzSeed(t)
	forEachFuzzEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		setupFz(t, db)
		f := newFz(t, e, db, seed)
		a, ret := newFzAPI(e, db, runtime.WithMaxBatchParams(2*fzNumCols)) // 2 rows per chunk
		defer a.Close()
		retID, _ := fzRetSupport(e)
		for _, order := range []int{0, 1, 2} {
			for path := 0; path < 2; path++ {
				f.reset(nil)
				rows := f.genRows(9)              // 5 chunks
				rows[len(rows)-1].ID = rows[0].ID // the last chunk collides with the first (any sort order)
				if aff, err := a.Insert(f.ctx, fzIns{Order: order, Path: path}, rows); err == nil {
					t.Errorf("order %d path %d: duplicate key went through (affected %d)", order, path, aff)
				}
				// The base method is one transaction (nothing stays); a Stream is not: the chunks before the
				// failing one are committed (DOCS "Atomicity"), the failing chunk's statement leaves nothing.
				if n := len(f.dump()); (path == 0 && n != 0) || (path == 1 && n != len(rows)-1) {
					t.Errorf("order %d path %d: %d rows left after a failed chunked insert", order, path, n)
				}
			}
		}
		if retID {
			for mode := 0; mode < 3; mode++ {
				f.reset(nil)
				rows := f.genRows(9)
				rows[len(rows)-1].ID = rows[0].ID
				if _, err := ret.InsertRet(f.ctx, rows, mode); err == nil {
					t.Errorf("InsertRet mode %d: duplicate key went through", mode)
				}
				if n := len(f.dump()); (mode == 0 && n != 0) || (mode != 0 && n != len(rows)-1) {
					t.Errorf("InsertRet mode %d: %d rows left after a failed chunked insert", mode, n)
				}
			}
		}
	})
}
