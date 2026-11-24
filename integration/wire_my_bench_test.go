package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/mysqlwire"
)

// PipelineTx on a real MySQL / MariaDB server: the single-round-trip path (mysqlwire) against the
// sequential fallback (plain go-sql-driver), at the loopback round-trip time of the test server and with
// 1 ms of simulated network latency added to every turn. Opt-in, like the other benchmarks:
//
//	make bench-engines ENGINES=mysql84,mariadb1011
//
// turns/op is the measured number of network turns per transaction.

// myBenchOpen opens dsn with a dialer that counts turns and adds rtt to each of them. wire selects
// mysqlwire or the plain driver. Two connections, one kept warm, so that the single-round-trip path is
// eligible (it needs db.Conn) and connection establishment is never measured.
func myBenchOpen(b *testing.B, dsn string, wire bool, rtt time.Duration) (db *sql.DB, turns *atomic.Int64) {
	b.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		b.Fatal(err)
	}
	turns = new(atomic.Int64)
	writes := new(atomic.Int64)
	network := cfg.Net
	if network == "" {
		network = "tcp"
	}
	name := fmt.Sprintf("sqlgenbench%d_%d", os.Getpid(), myNetSeq.Add(1))
	mysql.RegisterDialContext(name, func(ctx context.Context, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &turnConn{Conn: c, delay: rtt, turns: turns, writes: writes}, nil
	})
	b.Cleanup(func() { mysql.DeregisterDialContext(name) })
	cfg.Net = name
	if wire {
		c, err := mysqlwire.Connector(cfg)
		if err != nil {
			b.Fatal(err)
		}
		db = sql.OpenDB(c)
	} else {
		c, err := mysql.NewConnector(cfg)
		if err != nil {
			b.Fatal(err)
		}
		db = sql.OpenDB(c)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	b.Cleanup(func() { _ = db.Close() })
	return db, turns
}

type myBenchWorkload struct {
	name string
	run  func(ctx context.Context, q *myq.Queries, i int) error
}

func myGuardedN(n int) myBenchWorkload {
	return myBenchWorkload{fmt.Sprintf("%d-guarded", n), func(ctx context.Context, q *myq.Queries, i int) error {
		return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			for k := 0; k < n; k++ {
				p.AddScoreOne(1, int64((i+k)%8)+1).AssertRows(1)
			}
			return nil
		})
	}}
}

var myBenchWorkloads = []myBenchWorkload{
	{"1-statement", func(ctx context.Context, q *myq.Queries, i int) error {
		return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			p.AddScoreOne(1, int64(i%8)+1)
			return nil
		})
	}},
	myGuardedN(5),
	myGuardedN(25),
	// read-heavy: a write, an If on a read, and a dozen reads of both shapes
	{"read-heavy", func(ctx context.Context, q *myq.Queries, i int) error {
		return q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error {
			p.AddScoreOne(1, int64(i%8)+1).AssertRows(1)
			get := p.GetItem(1).AssertFound()
			p.If(get.Found(), func(p *myq.Pipeline) { p.GetItem(2); p.GetItem(3) }, func(p *myq.Pipeline) { p.GetItem(4) })
			for k := 0; k < 5; k++ {
				p.GetItem(int64((i+k)%8) + 1)
			}
			for k := 0; k < 4; k++ {
				p.ItemsByIDs([]int64{1, 2, 3, 4, int64(k) + 5})
			}
			return nil
		})
	}},
}

func BenchmarkMySQLPipelineTx(b *testing.B) {
	selected := selectedEngines()
	for _, e := range engines {
		if !e.isMySQL() || selected == nil || !selected[e.name] {
			continue
		}
		b.Run(e.name, func(b *testing.B) {
			dsn := os.Getenv(e.env)
			if dsn == "" {
				b.Skipf("%s not set", e.env)
			}
			admin, err := sql.Open(e.driver, dsn)
			if err != nil {
				b.Fatal(err)
			}
			defer admin.Close()
			benchSetup(b, admin)
			for i := 1; i <= 8; i++ {
				if _, err := admin.Exec(fmt.Sprintf("INSERT INTO sg_items VALUES (%d, 1, 'n%d', 0, 'new')", i, i)); err != nil {
					b.Fatal(err)
				}
			}
			for _, rtt := range []time.Duration{0, time.Millisecond} {
				for _, w := range myBenchWorkloads {
					for _, wire := range []bool{false, true} {
						name := fmt.Sprintf("rtt=%v/%s/%s", rtt, w.name, map[bool]string{false: "sequential", true: "wire"}[wire])
						b.Run(name, func(b *testing.B) {
							db, turns := myBenchOpen(b, dsn, wire, rtt)
							q := myq.New(db)
							defer q.Close()
							ctx := context.Background()
							for i := 0; i < 20; i++ { // warm: connections, statement caches
								if err := w.run(ctx, q, i); err != nil {
									b.Fatal(err)
								}
							}
							b.ResetTimer()
							before := turns.Load()
							for i := 0; i < b.N; i++ {
								if err := w.run(ctx, q, i); err != nil {
									b.Fatal(err)
								}
							}
							b.ReportMetric(float64(turns.Load()-before)/float64(b.N), "turns/op")
							if st := q.CacheStats(); wire && st.SequentialPipelines != 0 {
								b.Fatalf("a wire benchmark ran sequentially: %+v", st)
							}
						})
					}
				}
			}
		})
	}
}
