package integration

// Engine-agnostic view of the generated fuzz fixtures (fzpg: $N placeholders, fzmy: ?). The adapters in
// fuzz_adapter_fzpg_test.go / fuzz_adapter_fzmy_test.go implement fzAPI; the differential tests in
// fuzz_test.go are written once against it.

import (
	"context"
	"database/sql"
	"iter"

	"github.com/CAFxX/sqlgen/runtime"
)

// fzRow mirrors one fz_rows row. K holds k1..k8.
type fzRow struct {
	ID, Grp   int64
	K         [8]int64
	Tag, Tag2 string
	Note      *string // NULL-able column
}

// fzOut is the RETURNING row of the write statements.
type fzOut struct{ ID, K1 int64 }

type fzAggRow struct{ Grp, N, Total int64 }

// fzJoinRow is a row of FzJoin: three unconditional columns plus the conditional peer columns.
type fzJoinRow struct {
	ID, Grp, K1, PeerID int64
	PeerTag             string
}

// Column indexes of fzRow for SelectCols masks (bit i selects column i, in declaration order) and for
// sqlgen.Order terms (index into fzOrderCols).
const (
	fzColID   = 0
	fzColGrp  = 1
	fzColK1   = 2 // k1..k8 are columns 2..9
	fzColK8   = 9
	fzColTag  = 10
	fzColTag2 = 11
	fzColNote = 12
	fzNumCols = 13
)

// fzOrderCols is the sqlgen.Order(...) column list of FzPage, in declaration order; the last one (id) is
// the unique tie-breaker.
var fzOrderCols = []int{fzColGrp, fzColK1, fzColK1 + 1, fzColK1 + 2, fzColK1 + 3, fzColK1 + 4, fzColK1 + 5,
	fzColK1 + 6, fzColK1 + 7, fzColTag, fzColTag2, fzColID}

// fzTerm is one sqlgen.Order term: index into fzOrderCols plus a direction.
type fzTerm struct {
	Col  int
	Desc bool
}

type fzSearchArgs struct {
	Lo, Hi     int64
	ByGrp      bool
	Grp        int64
	ByGrps     bool
	Grps       []int64
	ByTags     bool
	Tags       []string
	FilterNote bool
	NoteLike   string
	NoteNull   bool
	ByPairs    bool
	Pairs      [][2]int64 // (grp, k2)
	Narrow     bool
	HiK4       bool
	SelectCols uint64
}

// fzSub is a composed sub-query tree: FzSubGrps (leaf) or FzSubNote (wraps Inner).
type fzSub struct {
	Note  bool // FzSubNote (needs Inner) rather than FzSubGrps
	Grps  []int64
	MinK2 bool
	K2Min int64
	MaxK3 bool
	K3Max int64
	Inner *fzSub
}

type fzPageArgs struct {
	Lo      int64
	ByGrps  bool
	Grps    []int64
	InSub   bool
	Sub     *fzSub
	SkipTag bool
	ExTag   string
	Terms   []fzTerm // nil: zero runtime.Order
	After   runtime.Seek
	Limit   int64
	Cols    uint64
}

// Insert variants: Order selects the generated statement (presort ASC / DESC / none), Path how it runs.
type fzIns struct {
	Order int // 0 asc, 1 desc, 2 raw, 3 FzInsertLit (literals in the tuple template; see litRow)
	Path  int // 0 base method (auto-chunking), 1 stream
}

// fzVar exposes the Into / Seq / Each flavours of one prepared call (the plain slice flavour is the API
// method itself). Into(prefix, spare) passes a dst with prefix zero rows and spare extra capacity.
type fzVar struct {
	Into func(prefix, spare int) ([]fzRow, error)
	Seq  func() iter.Seq2[fzRow, error]
	Each func(fn func(fzRow) error) error
}

// fzTRow is a row of fz_typed (column types other than int64 / string).
type fzTRow struct {
	ID   int64
	I32  int32
	F64  float64
	Flag bool
	Blob []byte
	NI   *int32
}

type fzTSearch struct {
	Lo        float64
	ByI32s    bool
	I32s      []int32
	ByF64s    bool
	F64s      []float64
	ByFlag    bool
	Flag      bool
	ByBlob    bool
	Blob      []byte
	NiAtLeast bool
	NiMin     *int32
}

type fzAPI interface {
	TypedInsert(ctx context.Context, rows []fzTRow) (int64, error)
	TypedSearch(ctx context.Context, a fzTSearch) ([]fzTRow, error)
	// TypedPage: terms index [f64, i32, flag, id].
	TypedPage(ctx context.Context, terms []fzTerm, after runtime.Seek, limit int64) ([]fzTRow, error)
	TypedCursor(r fzTRow) runtime.Seek
	SearchV(ctx context.Context, a fzSearchArgs) fzVar
	PageV(ctx context.Context, a fzPageArgs) fzVar
	ByIDsV(ctx context.Context, ids []int64) fzVar
	ByKeysV(ctx context.Context, keys [][2]int64) fzVar
	Insert(ctx context.Context, v fzIns, rows []fzRow) (int64, error)
	InsertStreamErr(ctx context.Context, rows []fzRow, n int, err error) error
	Search(ctx context.Context, a fzSearchArgs) ([]fzRow, error)
	Narrow(ctx context.Context, minID int64, byGrp bool, grp int64, cols uint64) ([]fzRow, error)
	Page(ctx context.Context, a fzPageArgs) ([]fzRow, error)
	Cursor(r fzRow) runtime.Seek
	Agg(ctx context.Context, pick *fzSub, byTag bool, tag string) ([]fzAggRow, error)
	Subs(ctx context.Context, a, b *fzSub, lo int64, excludeB, againA bool) ([]fzRow, error)
	Join(ctx context.Context, peerOff, minID int64, withPeer, byGrp bool, grp int64, cols uint64) ([]fzJoinRow, error)
	ByKeys(ctx context.Context, keys [][2]int64) ([]fzRow, error)
	ByIDs(ctx context.Context, ids []int64) ([]fzRow, error)
	Bump(ctx context.Context, delta int64, ids []int64, grp *int64) (int64, error)
	Delete(ctx context.Context, ids []int64, tag *string) (int64, error)
	// Tx returns a view of the same statements bound to tx (Queries.WithTx); it shares the cache and must not be closed.
	Tx(tx *sql.Tx) fzAPI
	Close() error
}

// fzRetAPI covers the RETURNING statements an engine supports (see fzRetSupport).
type fzRetAPI interface {
	// InsertRet mode: 0 base method (auto-chunking), 1 Stream, 2 StreamSeq.
	InsertRet(ctx context.Context, rows []fzRow, mode int) ([]fzOut, error)
	DeleteRet(ctx context.Context, ids []int64, tag *string) ([]fzOut, error)
	// BumpRet is UPDATE ... RETURNING: PostgreSQL and CockroachDB only (MariaDB has no UPDATE RETURNING).
	BumpRet(ctx context.Context, delta int64, ids []int64, grp *int64) ([]fzOut, error)
}

func newFzAPI(e engine, db *sql.DB, opts ...runtime.CacheOption) (fzAPI, fzRetAPI) {
	switch {
	case e.isPG():
		a := newFzpgAPI(db, opts...)
		return a, a
	case e.family == famSQLite:
		a := newFzslAPI(db, opts...)
		return a, a
	}
	a := newFzmyAPI(db, opts...)
	return a, a
}

// fzRetSupport reports which RETURNING forms an engine supports (DOCS "Engine support"): MySQL none,
// MariaDB INSERT and DELETE, PostgreSQL/CockroachDB/SQLite all three.
func fzRetSupport(e engine) (insertDelete, update bool) {
	switch e.family {
	case famMySQL:
		return false, false
	case famMariaDB:
		return true, false
	}
	return true, true
}
