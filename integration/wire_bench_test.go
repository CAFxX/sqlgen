package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen/integration/pgq"
)

// PipelineTx on a real server: the single-round-trip path (pgxwire) against the sequential fallback
// (plain pgx), at the loopback round-trip time of the test server and with 1 ms and 5 ms of simulated
// network latency added to every turn. Opt-in, like the other benchmarks:
//
//	make bench-engines ENGINES=postgres18    # or: go test -run '^$' -bench PipelineTx ./... with SQLGEN_ENGINES set
//
// turns/op is the measured number of network turns per transaction.
func BenchmarkPipelineTx(b *testing.B) {
	selected := selectedEngines()
	for _, e := range engines {
		if !e.isPG() || selected == nil || !selected[e.name] {
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
			for _, rtt := range []time.Duration{0, time.Millisecond, 5 * time.Millisecond} {
				for _, ops := range []int{3, 10} {
					for _, wire := range []bool{false, true} {
						name := fmt.Sprintf("rtt=%v/ops=%d/%s", rtt, ops, map[bool]string{false: "sequential", true: "wire"}[wire])
						b.Run(name, func(b *testing.B) {
							db, turns, _ := countingDBWithRTT(b, dsn, wire, rtt)
							q := pgq.New(db)
							defer q.Close()
							ctx := context.Background()
							run := func(i int) {
								err := q.PipelineTx(ctx, nil, func(p *pgq.Pipeline) error {
									for k := 0; k < ops; k++ {
										p.AddScoreOne(1, int64((i+k)%8)+1).AssertRowsAffected(1)
									}
									return nil
								})
								if err != nil {
									b.Fatal(err)
								}
							}
							for i := 0; i < 20; i++ { // warm: connection, statement caches
								run(i)
							}
							b.ResetTimer()
							before := turns.Load()
							for i := 0; i < b.N; i++ {
								run(i)
							}
							b.ReportMetric(float64(turns.Load()-before)/float64(b.N), "turns/op")
						})
					}
				}
			}
		})
	}
}
