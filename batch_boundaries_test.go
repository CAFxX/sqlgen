package sqlgen_test

// Row counts and results at chunk boundaries (1, 255..257, 4096..4097, 8192.., 20000) through Batch, Stream,
// BatchSeq and RETURNING variants with shuffled input (global presort).

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

func TestBatchAndStreamBoundarySizesAllModes(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{1, 2, 3, 255, 256, 257, 511, 513, 1000, 4095, 4096, 4097, 8192, 8193, 20000} {
		for _, mode := range []string{"batch", "stream", "ret", "retstream", "retbatchseq"} {
			db, q := setupE2EDB(t)
			rows := make([]QuadInput, n)
			for i := range rows {
				rows[i] = QuadInput{ID: int64(i + 1), GroupKey: "g", Priority: int64(i), CreatedTs: 5}
			}
			rand.Shuffle(n, func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
			var cnt int
			switch mode {
			case "batch":
				res, err := q.InsertQuadBatchBatch(ctx, rows)
				if err != nil {
					t.Fatalf("n=%d %s: %v", n, mode, err)
				}
				ra, _ := res.RowsAffected()
				if int(ra) != n {
					t.Fatalf("n=%d %s: rows affected %d", n, mode, ra)
				}
			case "stream":
				res, err := q.InsertQuadBatchStream(ctx, runtime.FromSlice(rows))
				if err != nil {
					t.Fatalf("n=%d %s: %v", n, mode, err)
				}
				ra, _ := res.RowsAffected()
				if int(ra) != n {
					t.Fatalf("n=%d %s: rows affected %d", n, mode, ra)
				}
			default:
				bu := make([]BatchUserInput, n)
				for i := range bu {
					bu[i] = BatchUserInput{ID: int64(i + 1), Name: "x", Points: 1}
				}
				rand.Shuffle(n, func(i, j int) { bu[i], bu[j] = bu[j], bu[i] })
				var got []InsertUsersBatchReturningRow
				var err error
				switch mode {
				case "ret":
					got, err = q.InsertUsersBatchReturningBatch(ctx, bu)
				case "retstream":
					got, err = q.InsertUsersBatchReturningStream(ctx, runtime.FromSlice(bu))
				case "retbatchseq":
					for r, e := range q.InsertUsersBatchReturningBatchSeq(ctx, bu) {
						if e != nil {
							err = e
							break
						}
						got = append(got, r)
					}
				}
				if err != nil {
					t.Fatalf("n=%d %s: %v", n, mode, err)
				}
				if len(got) != n {
					t.Fatalf("n=%d %s: returned %d rows", n, mode, len(got))
				}
				seen := map[int64]bool{}
				for _, r := range got {
					if seen[r.ID] {
						t.Fatalf("n=%d %s: dup id %d", n, mode, r.ID)
					}
					seen[r.ID] = true
				}
				_ = db.QueryRow("SELECT COUNT(*) FROM batch_users").Scan(&cnt)
				if cnt != n {
					t.Fatalf("n=%d %s: table has %d", n, mode, cnt)
				}
				continue
			}
			_ = db.QueryRow("SELECT COUNT(*) FROM quad_items").Scan(&cnt)
			if cnt != n {
				t.Fatalf("n=%d %s: table has %d", n, mode, cnt)
			}
		}
	}
}
