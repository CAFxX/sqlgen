package integration

// Measures real network turns. A "turn" is a Read from the socket that follows a Write: the client sent
// something and then had to wait for the server. Pipelined messages written back to back are one turn,
// however many statements they carry. The counter wraps the pgx connection's dial function, so it sees
// every byte the driver exchanges with the server and nothing else.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/CAFxX/sqlgen/integration/pgq"
	"github.com/CAFxX/sqlgen/pgxwire"
)

type turnConn struct {
	net.Conn
	delay  time.Duration // simulated network round-trip time, paid once per turn
	turns  *atomic.Int64
	wrote  atomic.Bool
	writes *atomic.Int64
}

// snoopNeedle, when set, makes every Write that carries it bump snoopHits: tests use it to assert that a
// statement's text never went over the wire. Only tests that set it (and do not run in parallel) see hits.
var (
	snoopNeedle atomic.Pointer[[]byte]
	snoopHits   atomic.Int64
)

func (c *turnConn) Write(p []byte) (int, error) {
	if n := snoopNeedle.Load(); n != nil && bytes.Contains(p, *n) {
		snoopHits.Add(1)
	}
	c.wrote.Store(true)
	c.writes.Add(1)
	return c.Conn.Write(p)
}

func (c *turnConn) Read(p []byte) (int, error) {
	if c.wrote.Swap(false) {
		c.turns.Add(1)
		if c.delay > 0 {
			time.Sleep(c.delay)
		}
	}
	return c.Conn.Read(p)
}

// countingDB opens dsn with a dialer that counts turns. wire selects pgxwire (single round-trip
// pipelines) or the plain pgx stdlib driver (the sequential baseline).
func countingDB(t testing.TB, dsn string, wire bool) (db *sql.DB, turns, writes *atomic.Int64) {
	t.Helper()
	return countingDBWithRTT(t, dsn, wire, 0)
}

// countingDBWithRTT is countingDB with a simulated network round-trip time added to every turn.
func countingDBWithRTT(t testing.TB, dsn string, wire bool, rtt time.Duration) (db *sql.DB, turns, writes *atomic.Int64) {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	turns, writes = new(atomic.Int64), new(atomic.Int64)
	cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &turnConn{Conn: c, delay: rtt, turns: turns, writes: writes}, nil
	}
	// pgx's stdlib driver pings a pooled connection that was idle for more than a second before handing it
	// out again. That health check is a turn that has nothing to do with sqlgen; keep it out of the numbers.
	noPing := stdlib.OptionShouldPing(func(context.Context, stdlib.ShouldPingParams) bool { return false })
	if wire {
		db = sql.OpenDB(pgxwire.Connector(cfg, noPing))
	} else {
		db = sql.OpenDB(stdlib.GetConnector(*cfg, noPing))
	}
	// Two connections so that the single-round-trip path is eligible (it needs db.Conn), one kept idle
	// and warm so that connection establishment (startup, authentication) is never measured.
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	t.Cleanup(func() { _ = db.Close() })
	return db, turns, writes
}

// measure returns the number of turns fn costs on an already established connection.
func measure(t testing.TB, db *sql.DB, turns *atomic.Int64, fn func()) int64 {
	t.Helper()
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil { // connection up, authenticated, warm
		t.Fatal(err)
	}
	before := turns.Load()
	fn()
	return turns.Load() - before
}

// A pipeline workload: n guarded updates, optionally with a read, an If/Else and an assertion, so that
// every kind of operation the wire path knows is represented.
type workload struct {
	name   string
	seqMin int64 // fewest turns the sequential path can take: BEGIN + one per executed statement + COMMIT
	fn     func(ctx context.Context, q *pgq.Queries) error
}

func transferN(n int) workload {
	return workload{fmt.Sprintf("%d guarded updates", n), int64(n) + 2, func(ctx context.Context, q *pgq.Queries) error {
		return q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
			for i := 0; i < n; i++ {
				p.AddScoreOne(1, int64(i%4)+1).AssertRowsAffected(1)
			}
			return nil
		})
	}}
}

var mixedWorkload = workload{"read + If/Else + assertions", 6, func(ctx context.Context, q *pgq.Queries) error {
	return q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
		get := p.GetItem(1).AssertFound()
		p.If(get.Found(),
			func(p *pgq.Pipeline) {
				p.AddScoreOne(-1, 1).AssertRowsAffected(1)
				p.AddScoreOne(1, 2).AssertRowsAffected(1)
			},
			func(p *pgq.Pipeline) { p.AddScoreOne(7, 3).AssertRowsAffected(1) })
		p.ItemsByIDs([]int64{1, 2, 3})
		return nil
	})
}}

func seedItems(t *testing.T, db *sql.DB) {
	t.Helper()
	setupItems(t, db)
	for i := 1; i <= 4; i++ {
		mustExec(t, db, fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', %d, 'new')", i, i, i*10))
	}
}

// The headline claim: a PipelineTx is ONE network round-trip on the single-round-trip path, however many
// operations it carries, versus BEGIN + one turn per statement + COMMIT on the sequential path.
func TestRealEngineWirePipelineTxIsOneRoundTrip(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		dsn := os.Getenv(e.env)
		seedItems(t, db)
		ctx := ctxT(t)

		loads := []workload{transferN(1), transferN(3), transferN(10), mixedWorkload}
		type result struct{ firstSeen, warmed int64 }
		results := map[string][2]result{} // [sequential, wire]

		for wi, wire := range []bool{false, true} {
			// A fresh database handle per path: the first run of each workload is "first-seen" (no statement
			// text has been seen by this connection), the second is "warmed".
			wdb, turns, _ := countingDB(t, dsn, wire)
			q := pgq.New(wdb)
			for _, w := range loads {
				var r result
				r.firstSeen = measure(t, wdb, turns, func() {
					if err := w.fn(ctx, q); err != nil {
						t.Fatalf("%s: %v", w.name, err)
					}
				})
				r.warmed = measure(t, wdb, turns, func() {
					if err := w.fn(ctx, q); err != nil {
						t.Fatalf("%s: %v", w.name, err)
					}
				})
				pair := results[w.name]
				pair[wi] = r
				results[w.name] = pair
			}
			st := q.CacheStats()
			if wire && (st.SequentialPipelines != 0 || st.WirePipelines != uint64(2*len(loads))) {
				t.Fatalf("wire stats: %+v", st)
			}
			if !wire && (st.WirePipelines != 0 || st.SequentialPipelines != uint64(2*len(loads))) {
				t.Fatalf("sequential stats: %+v", st)
			}
			q.Close()
		}

		for _, w := range loads {
			r := results[w.name]
			t.Logf("%-12s %-32s turns: sequential first-seen=%d warmed=%d | wire first-seen=%d warmed=%d",
				e.name, w.name, r[0].firstSeen, r[0].warmed, r[1].firstSeen, r[1].warmed)
		}
		for _, w := range loads {
			r := results[w.name]
			if r[1].firstSeen != 1 || r[1].warmed != 1 {
				t.Errorf("%s: a PipelineTx must cost exactly one network turn (first-seen and warmed), got %d / %d", w.name, r[1].firstSeen, r[1].warmed)
			}
			if r[0].warmed < w.seqMin || r[0].firstSeen < w.seqMin {
				t.Errorf("%s: the sequential baseline should cost at least %d turns, got first-seen=%d warmed=%d", w.name, w.seqMin, r[0].firstSeen, r[0].warmed)
			}
		}
	})
}

// A standalone Flush and a Flush inside a user-managed transaction are one turn too (the latter for the
// operations only: BEGIN and COMMIT belong to the caller).
func TestRealEngineWireFlushVariantsRoundTrips(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		seedItems(t, db)
		ctx := ctxT(t)
		wdb, turns, _ := countingDB(t, os.Getenv(e.env), true)
		q := pgq.New(wdb)
		defer q.Close()
		build := func(p *pgq.Pipeline) {
			get := p.GetItem(1)
			p.If(get.Found(), func(p *pgq.Pipeline) { p.AddScoreOne(1, 2).AssertRowsAffected(1) }, nil)
		}

		standalone := measure(t, wdb, turns, func() {
			p := q.Pipeline(ctx)
			build(p)
			if err := p.Flush(); err != nil {
				t.Fatal(err)
			}
		})
		var userTx int64
		tx, err := wdb.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		userTx = measure(t, wdb, turns, func() { // BEGIN was sent before the measurement window
			p := q.WithTx(tx).Pipeline(ctx)
			build(p)
			if err := p.Flush(); err != nil {
				t.Fatal(err)
			}
		})
		commit := measure(t, wdb, turns, func() {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("%s: standalone Flush = %d turn(s); Flush in a user-managed tx = %d turn(s) (+%d for the caller's COMMIT)", e.name, standalone, userTx, commit)
		if standalone != 1 || userTx != 1 {
			t.Fatalf("standalone=%d userTx=%d, want 1 and 1", standalone, userTx)
		}
		st := q.CacheStats()
		if st.WirePipelines != 2 || st.SequentialPipelines != 0 {
			t.Fatalf("stats = %+v", st)
		}
	})
}

// With a single-connection pool the runtime never borrows a second connection (that would deadlock), so a
// PipelineTx or a standalone Flush begins an ordinary transaction on the only connection and sends the
// operations through the wire pipeliner of that very connection: BEGIN, one turn for the operations,
// COMMIT. It completes (no deadlock) and is counted as a wire pipeline.
func TestRealEngineWireSingleConnectionPool(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isPG() {
			t.Skip("PostgreSQL protocol only")
		}
		seedItems(t, db)
		ctx, cancel := context.WithTimeout(ctxT(t), 20*time.Second)
		defer cancel()
		wdb, turns, _ := countingDB(t, os.Getenv(e.env), true)
		wdb.SetMaxOpenConns(1)
		q := pgq.New(wdb)
		defer q.Close()

		ptx := measure(t, wdb, turns, func() {
			if err := transferN(3).fn(ctx, q); err != nil {
				t.Fatal(err)
			}
		})
		flush := measure(t, wdb, turns, func() {
			p := q.Pipeline(ctx)
			p.AddScoreOne(1, 1).AssertRowsAffected(1)
			p.AddScoreOne(1, 2).AssertRowsAffected(1)
			if err := p.Flush(); err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("%s: SetMaxOpenConns(1): PipelineTx = %d turn(s), standalone Flush = %d turn(s)", e.name, ptx, flush)
		if ptx != 3 || flush != 3 {
			t.Errorf("single-connection pool: PipelineTx=%d Flush=%d turns, want 3 (BEGIN, operations, COMMIT)", ptx, flush)
		}
		if st := q.CacheStats(); st.WirePipelines != 2 || st.SequentialPipelines != 0 {
			t.Errorf("stats = %+v, want 2 wire / 0 sequential pipelines", st)
		}
		var score int64
		if err := wdb.QueryRow("SELECT score FROM sg_items WHERE id = 1").Scan(&score); err != nil || score != 10+1+1 {
			t.Errorf("score of item 1 = %d (%v), want 12", score, err)
		}
	})
}
