package runtime

import (
	"database/sql"
	"errors"
	"iter"
	"strconv"
	"sync"
)

// Dialect specifies the SQL parameter placeholder format.
type Dialect uint8

const (
	// DialectQuestion emits question-mark placeholders (?, ?, ...).
	DialectQuestion Dialect = iota
	// DialectDollar emits positional placeholders ($1, $2, ...).
	DialectDollar
)

// StmtVerb identifies the top-level SQL DML/DQL verb of a statement.
type StmtVerb uint8

const (
	VerbOther StmtVerb = iota
	VerbSelect
	VerbInsert
	VerbUpdate
	VerbDelete
)

// SlotKind identifies a dynamic placeholder or structural directive in a QueryDescriptor.
type SlotKind uint8

const (
	SlotScalar                 SlotKind = iota // ? or $N
	SlotSlice                                  // ?, ? or $N, $N+1
	SlotTupleSlice                             // (?, ?), (?, ?)
	SlotQuery                                  // Composed runtime.Query
	SlotSelectCols                             // Dynamic SELECT column projection (ColMask)
	SlotOptBlock                               // Conditional block {?Flag: ...} (FlagMask)
	SlotSeek                                   // Keyset pagination WHERE predicate
	SlotOrder                                  // Custom multi-column ORDER BY
	SlotBranchGuardOpen                        // "(" + gates + ") AND (" after WHERE in UPDATE/DELETE
	SlotBranchGuardClose                       // ")" at end of WHERE in UPDATE/DELETE
	SlotBranchGuardWhere                       // " WHERE " + gates in UPDATE/DELETE without WHERE
	SlotInsertValuesOpen                       // "VALUES (" or "SELECT "
	SlotInsertValuesClose                      // ")" or " FROM (SELECT 1) AS _sqlgen_dual WHERE " + gates
	SlotInsertTupleValuesOpen                  // "VALUES " or "WITH _sqlgen_v AS (VALUES "
	SlotInsertTupleValuesClose                 // "" or ") SELECT * FROM _sqlgen_v WHERE " + gates
)

const (
	defaultMaxPreparedStmts = 256
	defaultMaxSQLVariants   = 4096
	defaultMaxPartialBytes  = 16 << 20 // 16 MB
	defaultPromoteThreshold = 2
	defaultMaxBatchParams   = 16384
	maxRowsPerChunk         = 65535 // the largest wire limit of any placeholder style ($N): one row per parameter
	inexactKeyBit           = uint64(1) << 63
)

var (
	// ErrEmptySliceParam is returned when an active 1D slice or tuple-slice parameter has length 0.
	ErrEmptySliceParam = errors.New("sqlgen: slice parameter cannot be empty")
	// ErrUninitializedQuery is returned when an active composed Query parameter is uninitialized.
	ErrUninitializedQuery = errors.New("sqlgen: curried Query argument is uninitialized")
	// ErrAssertionFailed is matched (errors.Is) by every pipeline row-count assertion failure. The returned
	// error is an *AssertionError naming the statement and the expected count.
	ErrAssertionFailed = errors.New("sqlgen: pipeline row-count assertion failed")
	// ErrNilArgument is returned by the entry points that take an iterator or a callback (the generated
	// <Name>Stream / <Name>StreamSeq sequence, InTx and PipelineTx, and runtime.Each / AppendTo) when it is nil.
	ErrNilArgument = errors.New("sqlgen: nil iterator or callback argument")
	// ErrFutureNotReady is returned when a future (ExecFuture, RowFuture, SliceFuture) is read before the
	// pipeline that queued it has been flushed (Flush, or the end of PipelineTx). It is distinct from
	// sql.ErrNoRows, which a flushed RowFuture returns for a statement that matched no row.
	ErrFutureNotReady = errors.New("sqlgen: pipeline future read before the pipeline was flushed")
	// ErrBranchNotTaken is returned when reading a future from an untaken If/Else branch.
	ErrBranchNotTaken = errors.New("sqlgen: pipeline if/else branch was not taken")
	// ErrInvalidSeek is returned when a Seek cursor has an invalid column count.
	ErrInvalidSeek = errors.New("sqlgen: invalid seek cursor")
	// ErrInvalidOrder is returned when an Order specification is invalid.
	ErrInvalidOrder = errors.New("sqlgen: invalid order specification")
	// ErrInvalidArgument is matched (errors.Is) by the errors of generated methods called with arguments
	// that contradict each other, such as a <Name>Stream whose params struct also carries the collection.
	ErrInvalidArgument = errors.New("sqlgen: invalid argument")
	// ErrCollectionTooLarge is matched (errors.Is) by the error a statement whose result cannot be split
	// into chunks (an aggregate, GROUP BY, DISTINCT, ORDER BY, LIMIT, ... over its collection, see DOCS
	// section 3.2) returns when its collection does not fit one statement. Nothing is sent. The returned
	// error is a *CollectionTooLargeError naming the statement and the limit.
	ErrCollectionTooLarge = errors.New("sqlgen: collection too large for one statement")
)

var argPool = sync.Pool{
	New: func() any {
		s := make([]any, 0, 16)
		return &s
	},
}

// acquireArgs obtains a pooled *[]any buffer reset to length 0.
func acquireArgs() *[]any {
	buf := argPool.Get().(*[]any)
	*buf = (*buf)[:0]
	return buf
}

// AcquireArgsCap obtains a pooled *[]any buffer with at least minCap capacity.
func AcquireArgsCap(minCap int) *[]any {
	buf := argPool.Get().(*[]any)
	s := (*buf)[:0]
	if cap(s) < minCap {
		newCap := minCap
		if newCap < 16 {
			newCap = 16
		}
		s = make([]any, 0, newCap)
	}
	*buf = s
	return buf
}

// ReleaseArgs clears all elements in buf and returns it to the pool.
func ReleaseArgs(buf *[]any) {
	if buf == nil {
		return
	}
	s := *buf
	clear(s)
	if cap(s) > 4096 {
		return
	}
	*buf = s[:0]
	argPool.Put(buf)
}

var indexPool = sync.Pool{
	New: func() any {
		s := make([]int, 0, 64)
		return &s
	},
}

// AcquireIndices obtains a pooled *[]int slice of length n initialized to the identity permutation 0..n-1.
func AcquireIndices(n int) *[]int {
	buf := indexPool.Get().(*[]int)
	s := *buf
	if cap(s) < n {
		newCap := n
		if newCap < 64 {
			newCap = 64
		}
		s = make([]int, n, newCap)
	} else {
		s = s[:n]
	}
	for i := 0; i < n; i++ {
		s[i] = i
	}
	*buf = s
	return buf
}

// ReleaseIndices returns a pooled *[]int index slice to the pool.
func ReleaseIndices(buf *[]int) {
	if buf == nil {
		return
	}
	s := *buf
	if cap(s) > 65536 {
		return
	}
	*buf = s[:0]
	indexPool.Put(buf)
}

// VariantKey is a 256-bit comparable value type that uniquely identifies a query variation
// across StmtID, ColMask, FlagMask, OrderKey, and up to 4 slice/tuple/subquery dimensions.
type VariantKey [4]uint64

// MakeVariantKey packs statement shape coordinates into a 256-bit VariantKey.
func MakeVariantKey(stmtID uint32, colMask, flagMask, orderKey uint64, dims ...uint64) VariantKey {
	inexact := stmtID == 0 || stmtID > 0x7FFF || colMask > 0xFFFFFF || flagMask > 0xFFFFFF || len(dims) > 4
	for _, d := range dims {
		if (d & inexactKeyBit) != 0 {
			inexact = true
			break
		}
	}
	if !inexact {
		canPackDims := true
		var w2, w3 uint64
		switch len(dims) {
		case 0:
		case 1:
			w2 = dims[0]
		case 2:
			w2 = dims[0]
			w3 = dims[1]
		case 3:
			if dims[0] <= 0xFFFFFFFF && dims[1] <= 0xFFFFFFFF && dims[2] <= 0xFFFFFFFF {
				w2 = (dims[0] << 32) | dims[1]
				w3 = dims[2]
			} else {
				canPackDims = false
			}
		case 4:
			if dims[0] <= 0xFFFFFFFF && dims[1] <= 0xFFFFFFFF && dims[2] <= 0xFFFFFFFF && dims[3] <= 0xFFFFFFFF {
				w2 = (dims[0] << 32) | dims[1]
				w3 = (dims[2] << 32) | dims[3]
			} else {
				canPackDims = false
			}
		}
		if canPackDims {
			// Encode len(dims) in bits 47..45 so (dims=[0]) and (dims=[]) or (dims=[1,0]) are bijective:
			// Word 0: [Bit 63: 0][Bits 62..48: stmtID (15b)][Bits 47..45: len(dims) (3b)][Bits 44..22: colMask (23b)][Bits 21..0: flagMask (22b)]
			if colMask <= 0x7FFFFF && flagMask <= 0x3FFFFF {
				w0 := (uint64(stmtID) << 48) | (uint64(len(dims)) << 45) | ((colMask & 0x7FFFFF) << 22) | (flagMask & 0x3FFFFF)
				return VariantKey{w0, orderKey, w2, w3}
			}
		}
	}
	return VariantKey{inexactKeyBit | uint64(stmtID), colMask ^ flagMask, orderKey, uint64(len(dims))}
}

// IsExact reports whether k bijectively encodes the query shape without collisions.
func (k VariantKey) IsExact() bool { return (k[0] & inexactKeyBit) == 0 }

func (k VariantKey) hash() uint64 {
	h := k[0]*0x9e3779b97f4a7c15 + k[1]*0xbf58476d1ce4e5b9 + k[2]*0x94d049bb133111eb + k[3]
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	return h ^ (h >> 31)
}

// Hash returns a 64-bit hash of the VariantKey.
func (k VariantKey) Hash() uint64 {
	return k.hash()
}

func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// hashString computes a fast, allocation-free 64-bit hash of s.
func hashString(s string) uint64 {
	h := uint64(0xcbf29ce484222325)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 0x100000001b3
	}
	return mix64(h)
}

// ComputeSubqueryShape packs a subquery's coordinates bijectively into 63 bits (inexactKeyBit == 0)
// when all fields fit, or sets inexactKeyBit so the outer query falls back to exact SQL string keying.
func ComputeSubqueryShape(stmtID uint32, colMask, flagMask, orderKey uint64, hasNestedSubs bool, sliceOrTupleLens ...int) uint64 {
	if !hasNestedSubs && orderKey == 0 && stmtID > 0 && stmtID <= 0x3FFF &&
		colMask <= 0x7F && flagMask <= 0xFF && len(sliceOrTupleLens) <= 2 {
		var s0, s1 uint64
		fits := true
		if len(sliceOrTupleLens) >= 1 {
			if sliceOrTupleLens[0] < 0 || sliceOrTupleLens[0] > 0xFFFF {
				fits = false
			}
			s0 = uint64(sliceOrTupleLens[0])
		}
		if len(sliceOrTupleLens) == 2 {
			if sliceOrTupleLens[1] < 0 || sliceOrTupleLens[1] > 0xFFFF {
				fits = false
			}
			s1 = uint64(sliceOrTupleLens[1])
		}
		if fits {
			return (uint64(stmtID) << 49) | (uint64(len(sliceOrTupleLens)) << 47) |
				((colMask & 0x7F) << 40) | ((flagMask & 0xFF) << 32) | (s0 << 16) | s1
		}
	}
	vk := MakeVariantKey(stmtID, colMask, flagMask, orderKey)
	return inexactKeyBit | (vk.hash() & ^inexactKeyBit)
}

// MaxOrderCols is the largest number of columns a sqlgen.Order(...) directive may list, and so the
// largest number of terms a packed Order can carry: every term is 5 bits wide (a 1-based column index
// of 1..12 in the upper four bits, the DESC flag in the lowest), so twelve terms fill bits 0..59 and
// leave bit 63 for the Seek flag of EncodeOrderKey.
const MaxOrderCols = 12

const (
	orderTermBits = 5
	orderTermMask = 1<<orderTermBits - 1
)

// Order encodes up to MaxOrderCols packed sort terms in a 64-bit integer.
type Order = uint64

// Asc encodes a 0-based sortable column index as an ascending sort term.
func Asc(colIdx uint8) uint8 {
	return ((colIdx + 1) << 1) | 0
}

// Desc encodes a 0-based sortable column index as a descending sort term.
func Desc(colIdx uint8) uint8 {
	return ((colIdx + 1) << 1) | 1
}

// orderInvalidBit marks a packed Order built from an invalid term list (see PackOrder). It sits above the
// 60 term bits and below the Seek flag, and is ignored by expandOrder.
const orderInvalidBit = uint64(1) << 62

// PackOrder packs up to MaxOrderCols sort column terms (each encoded as ((colIdx1Based) << 1) | descBit)
// into a single collision-free uint64 with zero heap allocations. It accepts any type whose underlying
// type is uint8, so the generated per-statement term types (which make mixing the tokens of two
// statements a compile error) pack without conversion. A term that is zero, names a column beyond
// MaxOrderCols, or repeats a column already named marks the result invalid: OrderValid reports false
// and the generated statement methods return ErrInvalidOrder instead of running a sort the caller did
// not ask for.
func PackOrder[T ~uint8](terms ...T) uint64 {
	var (
		packed uint64
		seen   uint64
		shift  uint
	)
	for _, term := range terms {
		t := uint8(term)
		col1 := t >> 1
		if col1 == 0 || col1 > MaxOrderCols {
			packed |= orderInvalidBit
			continue
		}
		bit := uint64(1) << col1
		if (seen & bit) != 0 {
			packed |= orderInvalidBit
			continue
		}
		seen |= bit
		packed |= uint64(t) << shift
		shift += orderTermBits
	}
	return packed
}

// OrderValid reports whether orderSpec was packed from a valid term list (see PackOrder). The zero Order
// (no terms) is valid.
func OrderValid(orderSpec Order) bool { return orderSpec&orderInvalidBit == 0 }

// EncodeOrderKey combines a packed Order specification (60 term bits plus the invalid flag) and a boolean hasSeek flag into a 64-bit key.
func EncodeOrderKey(orderSpec Order, hasSeek bool) uint64 {
	if hasSeek {
		return orderSpec | (1 << 63)
	}
	return orderSpec &^ (1 << 63)
}

// expandOrder unpacks a packed uint64 order specification into column indices (0..numCols-1)
// and direction flags (true == DESC), automatically appending the tie-breaker column (numCols-1)
// if not already present (inheriting the preceding sort column's direction so single-direction
// DESC sorts use uniform tuple comparison). numCols must not exceed MaxOrderCols (the generator
// rejects larger Order directives); count is at most numCols.
func expandOrder(orderSpec uint64, numCols int) (cols [MaxOrderCols]int, desc [MaxOrderCols]bool, count int, allSameDir bool) {
	if numCols <= 0 {
		return cols, desc, 0, true
	}
	if numCols > MaxOrderCols {
		numCols = MaxOrderCols
	}
	var seen uint64
	for shift := uint(0); shift < MaxOrderCols*orderTermBits; shift += orderTermBits {
		t := uint8((orderSpec >> shift) & orderTermMask)
		if t == 0 {
			break
		}
		colIdx := int(t>>1) - 1
		if colIdx < 0 || colIdx >= numCols {
			continue
		}
		bit := uint64(1) << uint(colIdx)
		if (seen & bit) != 0 {
			continue
		}
		seen |= bit
		cols[count] = colIdx
		desc[count] = (t & 1) != 0
		count++
	}
	tieIdx := numCols - 1
	tieBit := uint64(1) << uint(tieIdx)
	if (seen & tieBit) == 0 {
		cols[count] = tieIdx
		if count > 0 {
			desc[count] = desc[count-1]
		} else {
			desc[count] = false
		}
		count++
	}
	allSameDir = true
	for i := 1; i < count; i++ {
		if desc[i] != desc[0] {
			allSameDir = false
			break
		}
	}
	return cols, desc, count, allSameDir
}

// Seek holds keyset pagination cursor values.
type Seek struct {
	Values []any
}

// OrCols combines the column constants a positional statement receives in its variadic selectCols argument
// into the projection mask (no argument: 0, which renders every column).
func OrCols(cols []uint64) uint64 {
	var m uint64
	for _, c := range cols {
		m |= c
	}
	return m
}

// NewSeek constructs an active Seek cursor with the given column values.
func NewSeek(vals ...any) Seek {
	if len(vals) == 0 {
		return Seek{}
	}
	return Seek{Values: vals}
}

// IsActive reports whether the Seek cursor is populated.
func (s Seek) IsActive() bool {
	return len(s.Values) > 0
}

// Count returns the number of values stored in the Seek cursor.
func (s Seek) Count() int {
	return len(s.Values)
}

// Val returns the i-th stored value in the Seek cursor.
func (s Seek) Val(i int) any {
	return s.Values[i]
}

// seekArgCount returns the number of positional parameters consumed by SlotSeek.
func seekArgCount(dialect Dialect, orderSpec uint64, numCols int, hasSeek bool) int {
	if !hasSeek || numCols <= 0 {
		return 0
	}
	_, _, count, allSameDir := expandOrder(orderSpec, numCols)
	if dialect == DialectDollar || count == 1 || allSameDir {
		return count
	}
	// DialectQuestion (?) with mixed ASC/DESC directions expands to:
	// 1 bounding-box param + count*(count+1)/2 lexicographical branch params.
	return 1 + (count*(count+1))/2
}

// AppendSeekArgs appends cursor column values into dst in the exact order expected by formatSeekSQL.
func AppendSeekArgs(dst *[]any, dialect Dialect, orderSpec uint64, numCols int, colValues []any) {
	if len(colValues) == 0 || numCols <= 0 {
		return
	}
	cols, _, count, allSameDir := expandOrder(orderSpec, numCols)
	valAt := func(i int) any {
		cIdx := cols[i]
		if len(colValues) == numCols && cIdx >= 0 && cIdx < len(colValues) {
			return colValues[cIdx]
		}
		if i < len(colValues) {
			return colValues[i]
		}
		return colValues[len(colValues)-1]
	}
	if dialect == DialectDollar || count == 1 || allSameDir {
		for i := 0; i < count; i++ {
			*dst = append(*dst, valAt(i))
		}
		return
	}
	*dst = append(*dst, valAt(0))
	for i := 0; i < count; i++ {
		for j := 0; j <= i; j++ {
			*dst = append(*dst, valAt(j))
		}
	}
}

// TupleDescriptor describes the SQL template of a single tuple in a SlotTupleSlice.
type TupleDescriptor struct {
	Segments  []string // len(Segments) == NumFields + 1
	NumFields int
	// Casts optionally holds, per field, a PostgreSQL type name (e.g. "bigint") applied to the field's
	// placeholder when the tuple is rendered inside a branch-gated INSERT (see Slot.Cast). nil or
	// len(Casts) == NumFields.
	Casts []string
}

// Slot represents a single dynamic expansion site in a QueryDescriptor.
type Slot struct {
	Kind    SlotKind
	FlagBit uint8
	Tuple   *TupleDescriptor
	Inner   *QueryDescriptor
	// Cast, for a SlotScalar, is a PostgreSQL type name (e.g. "bigint") appended to the placeholder
	// ("$1::bigint") only when it is rendered inside a branch-gated INSERT under DialectDollar. A
	// gated INSERT is rewritten from VALUES (...) to SELECT ... WHERE <gates>, and in a SELECT list an
	// untyped placeholder is not inferred from the target column (it resolves to text, or fails with
	// "could not determine data type"), unlike in VALUES.
	Cast string
}

// QueryDescriptor holds the pre-parsed static SQL segments and dynamic slots for a statement.
type QueryDescriptor struct {
	Name           string // statement name, used in diagnostics
	StmtID         uint32
	StmtVerb       StmtVerb
	StaticSQL      string
	Segments       []string // len(Segments) == len(Slots) + 1
	Slots          []Slot
	SelectColExprs []string
	OrderCols      []string
	HasReturning   bool
	NumSlices      int
	NumTuples      int
	NumSubs        int
}

// AssertionError is the error returned when AssertRows/AssertFound fails. errors.Is(err,
// ErrAssertionFailed) is true for it. Op tells which queued operation failed when one pipeline queues
// the same statement several times. GotKnown is false only when the actual row count could not be
// determined.
type AssertionError struct {
	Statement string // statement name; empty when unknown
	// Op is the failing operation's 0-based position among the operations queued on the pipeline, in
	// queue order: every p.<Stmt>(...) call and every p.Abort counts, inside If branches too (taken or
	// not). It is the same on the single-round-trip and the sequential path.
	Op        int
	Label     string // the operation's Label; empty when none was set
	Want, Got int64
	GotKnown  bool
}

func (e *AssertionError) Error() string {
	msg := "sqlgen: pipeline assertion failed on "
	if e.Statement != "" {
		msg += "statement " + strconv.Quote(e.Statement) + ", "
	}
	msg += "operation " + strconv.Itoa(e.Op)
	if e.Label != "" {
		msg += " (" + strconv.Quote(e.Label) + ")"
	}
	msg += ": expected " + strconv.FormatInt(e.Want, 10) + " rows affected"
	if e.GotKnown {
		msg += ", got " + strconv.FormatInt(e.Got, 10)
	}
	return msg
}

// Unwrap makes errors.Is(err, ErrAssertionFailed) true.
func (e *AssertionError) Unwrap() error { return ErrAssertionFailed }

// InvalidArgument returns an error matching ErrInvalidArgument with the given explanation.
func InvalidArgument(msg string) error {
	return &invalidArgumentError{msg}
}

type invalidArgumentError struct{ msg string }

func (e *invalidArgumentError) Error() string { return ErrInvalidArgument.Error() + ": " + e.msg }
func (e *invalidArgumentError) Unwrap() error { return ErrInvalidArgument }

// CollectionTooLargeError is the error a non-decomposable statement (one whose result is not the
// concatenation of the results of its chunks) returns when its collection holds more items than one
// statement can carry (Limit, see WithMaxBatchParams). errors.Is(err, ErrCollectionTooLarge) is true for it.
type CollectionTooLargeError struct {
	Statement    string
	Items, Limit int
}

func (e *CollectionTooLargeError) Error() string {
	return "sqlgen: statement " + strconv.Quote(e.Statement) + ": collection of " + strconv.Itoa(e.Items) +
		" items exceeds the single-statement limit of " + strconv.Itoa(e.Limit) +
		" items, and the statement cannot be chunked (its result is not the concatenation of per-chunk results)"
}

// Unwrap makes errors.Is(err, ErrCollectionTooLarge) true.
func (e *CollectionTooLargeError) Unwrap() error { return ErrCollectionTooLarge }

// BranchGate represents an enclosing Pipeline.If condition gate on a PipelineOp.
type BranchGate struct {
	CondID   int
	WantTrue bool
	Cond     *PipelineCond
}

// RenderInput carries the runtime shape coordinates needed to render a QueryDescriptor.
type RenderInput struct {
	ColMask     uint64
	FlagMask    uint64
	OrderSpec   uint64
	HasSeek     bool
	SliceLens   []int
	TupleLens   []int
	Subs        []Query
	BranchGates []BranchGate
}

// OpDecorators describes pipeline assertions, condition producers, or aborts attached to an operation.
type OpDecorators struct {
	AssertRows  int64 // -1 when inactive
	IsAbort     bool
	ProduceCond *PipelineCond
	CondID      int
	// ExtraConds are further conditions over the same operation (a second If on the same future).
	// Rendered like ProduceCond, each recorded server-side under its own CondID. Pipeline.If keeps it in
	// step with PipelineOp.ExtraConds.
	ExtraConds []ProducedCond
	// LeafRecs record, server-side, the value of a leaf of a compound condition that is rooted in a LATER
	// operation (see PipelineCond.Recorded): this operation's statement evaluates the leaf over its own
	// rows and stores it under sqlgen.l<CondID>; the later statement combines the recorded values.
	// Val and Set of the entries are unused (nobody reads them back).
	LeafRecs []ProducedCond
}

// guarded reports whether the statement of an operation needs the WITH _sqlgen_op guard shape: an
// assertion, a condition that is recorded server-side, or the record of a leaf of someone else's
// compound condition. (ExtraConds only ever accompany a ProduceCond.)
func (d *OpDecorators) guarded() bool {
	return d.AssertRows >= 0 || d.ProduceCond != nil || len(d.LeafRecs) > 0
}

// argsExpander appends a Query's bound arguments into dst.
type argsExpander func(dst *[]any, dialect Dialect, q *Query) error

// Query represents a curried, composable SQL query value with bound typed parameters.
type Query struct {
	StmtID   uint32
	Desc     *QueryDescriptor
	Input    RenderInput
	Params   any
	Expand   argsExpander
	ShapeTag uint64
}

// IsValid reports whether q was initialized by a statement Query constructor.
func (q Query) IsValid() bool {
	return q.Desc != nil && q.Expand != nil
}

// AppendArgs validates and appends all bind arguments of q to dst for the given dialect.
func (q *Query) AppendArgs(dst *[]any, dialect Dialect) error {
	if !q.IsValid() {
		return ErrUninitializedQuery
	}
	return q.Expand(dst, dialect, q)
}

// BuildSQL renders the SQL string for q in the given dialect.
func (q Query) BuildSQL(dialect Dialect) string {
	if q.Desc == nil {
		return ""
	}
	return BuildComposedSQL(q.Desc, &q.Input, dialect)
}

// FromSeq adapts an error-free iter.Seq[T] into an iter.Seq2[T, error].
func FromSeq[T any](seq iter.Seq[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for v := range seq {
			if !yield(v, nil) {
				return
			}
		}
	}
}

// FromSlice adapts a slice []T into an iter.Seq2[T, error].
func FromSlice[T any](items []T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for i := range items {
			if !yield(items[i], nil) {
				return
			}
		}
	}
}

// MapSeq2 transforms each element of seq using fn, propagating errors immediately.
func MapSeq2[In, Out any](seq iter.Seq2[In, error], fn func(In) (Out, error)) iter.Seq2[Out, error] {
	return func(yield func(Out, error) bool) {
		var zero Out
		for v, err := range seq {
			if err != nil {
				yield(zero, err)
				return
			}
			out, err := fn(v)
			if err != nil {
				yield(zero, err)
				return
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

// MapSeq transforms each element of seq using an infallible fn, propagating upstream errors.
func MapSeq[In, Out any](seq iter.Seq2[In, error], fn func(In) Out) iter.Seq2[Out, error] {
	return func(yield func(Out, error) bool) {
		var zero Out
		for v, err := range seq {
			if err != nil {
				yield(zero, err)
				return
			}
			if !yield(fn(v), nil) {
				return
			}
		}
	}
}

// ErrStop is returned by the callback of Each (as is or wrapped) to end the iteration early without an
// error: Each then returns nil. Only the callback can stop this way: a sequence error wrapping ErrStop is
// returned as is. It is never returned by Each itself.
var ErrStop = errors.New("sqlgen: stop iteration")

// Collect gathers every row of seq into a new slice. It returns (nil, err) at the first error, so a
// failed iteration never hands out a partial result; an iteration that yields no rows returns a nil slice
// (test len(rows) == 0, not rows == nil).
func Collect[T any](seq iter.Seq2[T, error]) ([]T, error) {
	return AppendTo[T](nil, seq)
}

// AppendTo appends every row of seq to dst and returns the extended slice: the usual append contract, so
// reuse a slice to avoid growing a new one per call (dst = dst[:0] keeps its capacity). It allocates
// nothing beyond what append itself grows. At the first error it returns (dst, err) with dst exactly as
// the caller passed it: the rows already read are not appended and the caller's data is never dropped
// (elements of dst past len(dst) may have been overwritten, as with any append that fits the capacity).
// A nil seq is ErrNilArgument.
func AppendTo[T any](dst []T, seq iter.Seq2[T, error]) ([]T, error) {
	if seq == nil {
		return dst, ErrNilArgument
	}
	out := dst
	var failure error
	seq(func(v T, err error) bool {
		if err != nil {
			failure = err
			return false
		}
		out = append(out, v)
		return true
	})
	if failure != nil {
		return dst, failure
	}
	return out, nil
}

// Each calls fn for every row of seq in order and stops at the first error: the one seq reported, or the
// one fn returned (returned unchanged). A nil seq or fn is ErrNilArgument. fn may return ErrStop (or an
// error wrapping it) to end the iteration early; Each then returns nil. A panic in fn propagates after the rows of the iteration have been released. Each is the
// callback form of ranging over the sequence; it allocates nothing of its own.
func Each[T any](seq iter.Seq2[T, error], fn func(T) error) error {
	if seq == nil || fn == nil {
		return ErrNilArgument
	}
	var failure error
	seq(func(v T, err error) bool {
		if err == nil {
			if err = fn(v); errors.Is(err, ErrStop) {
				return false
			}
		}
		if err != nil {
			failure = err
			return false
		}
		return true
	})
	return failure
}

// BatchExecResult aggregates sql.Result across multiple batch chunks.
type BatchExecResult struct {
	RowsAffectedVal int64
	// RowsAffectedErr is the first error a chunk's RowsAffected reported; the sum is then incomplete.
	RowsAffectedErr error
	LastInsertIDVal int64
	LastInsertIDErr error
}

// LastInsertId returns the last insert ID from the final batch chunk.
func (r BatchExecResult) LastInsertId() (int64, error) {
	return r.LastInsertIDVal, r.LastInsertIDErr
}

// RowsAffected returns the sum of rows affected across all executed batch chunks, or the first error a
// chunk reported (the sum would otherwise silently undercount).
func (r BatchExecResult) RowsAffected() (int64, error) {
	return r.RowsAffectedVal, r.RowsAffectedErr
}

// Add accumulates a chunk sql.Result into r.
func (r *BatchExecResult) Add(res sql.Result) {
	if res == nil {
		return
	}
	if n, err := res.RowsAffected(); err == nil {
		r.RowsAffectedVal += n
	} else if r.RowsAffectedErr == nil {
		r.RowsAffectedErr = err
	}
	id, err := res.LastInsertId()
	r.LastInsertIDVal = id
	r.LastInsertIDErr = err
}
