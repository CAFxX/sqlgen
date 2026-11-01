package runtime

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrEmptySliceParam is returned when an active 1D slice or tuple-slice parameter has length 0.
	ErrEmptySliceParam = errors.New("sqlgen: slice parameter cannot be empty")
	// ErrNilQuery is returned when an active composed Query parameter is uninitialized.
	ErrNilQuery = errors.New("sqlgen: composed query is uninitialized")
	// ErrInvalidSeek is returned when a Seek cursor has an invalid column count.
	ErrInvalidSeek = errors.New("sqlgen: invalid seek cursor")
	// ErrInvalidOrder is returned when an Order specification contains an out-of-range column index.
	ErrInvalidOrder = errors.New("sqlgen: invalid order specification")
)

// Dialect specifies the SQL parameter placeholder format.
type Dialect uint8

const (
	// DialectPostgres emits positional placeholders ($1, $2, ...).
	DialectPostgres Dialect = iota
	// DialectQuestion emits question-mark placeholders (?, ?, ...).
	DialectQuestion
)

// AppendPlaceholder appends a dialect-specific bind parameter placeholder to dst.
func AppendPlaceholder(dst []byte, dialect Dialect, paramIdx int) []byte {
	if dialect == DialectPostgres {
		dst = append(dst, '$')
		return strconv.AppendInt(dst, int64(paramIdx), 10)
	}
	return append(dst, '?')
}

var argPool = sync.Pool{
	New: func() any {
		s := make([]any, 0, 16)
		return &s
	},
}

// AcquireArgs obtains a pooled *[]any buffer with at least minCap capacity.
func AcquireArgs(minCap int) *[]any {
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

// InexactKeyBit is bit 63 of VariantKey[0] and subquery shape tags.
// When set, the key is not bijective and StmtCache keys by rendered SQL string.
const InexactKeyBit uint64 = 1 << 63

// VariantKey is a 256-bit value-type cache key for zero-allocation map lookups.
type VariantKey [4]uint64

// IsExact reports whether k bijectively encodes the query shape without collisions.
func (k VariantKey) IsExact() bool {
	return k[0]&InexactKeyBit == 0
}

// Hash computes a fast 64-bit hash of the 256-bit VariantKey.
func (k VariantKey) Hash() uint64 {
	h := mix64(k[0] ^ 0x9e3779b97f4a7c15)
	h = mix64(h ^ k[1])
	h = mix64(h ^ k[2])
	h = mix64(h ^ k[3])
	return h
}

func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// HashString computes a fast, allocation-free 64-bit hash of s.
func HashString(s string) uint64 {
	h := uint64(0xcbf29ce484222325)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 0x100000001b3
	}
	return mix64(h)
}

// MakeVariantKey packs statement shape coordinates into a 256-bit VariantKey.
//
// Word 0: bit 63 = InexactKeyBit, bits 48..62 = stmtID (15 bits),
//
//	bits 24..47 = colMask (24 bits), bits 0..23 = flagMask (24 bits).
//
// Word 1: orderKey (64 bits).
// Word 2 & 3: up to two 64-bit dims (or four 32-bit dims).
func MakeVariantKey(stmtID uint32, colMask, flagMask, orderKey uint64, dims ...uint64) VariantKey {
	inexact := stmtID == 0 || stmtID > 0x7FFF || colMask > 0xFFFFFF || flagMask > 0xFFFFFF || len(dims) > 4
	for _, d := range dims {
		if d&InexactKeyBit != 0 {
			inexact = true
			break
		}
	}

	w0 := (uint64(stmtID)&0x7FFF)<<48 | (colMask&0xFFFFFF)<<24 | (flagMask & 0xFFFFFF)
	w1 := orderKey
	var w2, w3 uint64

	switch len(dims) {
	case 0:
	case 1:
		w2 = dims[0]
	case 2:
		w2 = dims[0]
		w3 = dims[1]
	case 3:
		if dims[0] > 0xFFFFFFFF || dims[1] > 0xFFFFFFFF || dims[2] > 0xFFFFFFFF {
			inexact = true
		}
		w2 = (dims[0] << 32) | (dims[1] & 0xFFFFFFFF)
		w3 = dims[2] << 32
	case 4:
		if dims[0] > 0xFFFFFFFF || dims[1] > 0xFFFFFFFF || dims[2] > 0xFFFFFFFF || dims[3] > 0xFFFFFFFF {
			inexact = true
		}
		w2 = (dims[0] << 32) | (dims[1] & 0xFFFFFFFF)
		w3 = (dims[2] << 32) | (dims[3] & 0xFFFFFFFF)
	default:
		inexact = true
	}

	if inexact {
		w0 |= InexactKeyBit
		for i, d := range dims {
			m := mix64(d ^ uint64(i+1)*0x9e3779b97f4a7c15)
			if i&1 == 0 {
				w2 ^= m
			} else {
				w3 ^= m
			}
		}
	}

	return VariantKey{w0, w1, w2, w3}
}

// ComputeSubqueryShape computes a 64-bit shape tag for a composed subquery.
func ComputeSubqueryShape(stmtID uint32, colMask, flagMask, orderKey uint64, hasNestedSubs bool, sliceOrTupleLens ...int) uint64 {
	inexact := hasNestedSubs || orderKey != 0 || stmtID == 0 || stmtID > 0x7FFF || colMask > 0xFF || flagMask > 0xFF || len(sliceOrTupleLens) > 2
	var s0, s1 uint64
	if len(sliceOrTupleLens) >= 1 {
		if sliceOrTupleLens[0] < 0 || sliceOrTupleLens[0] > 0xFFFF {
			inexact = true
		} else {
			s0 = uint64(sliceOrTupleLens[0])
		}
	}
	if len(sliceOrTupleLens) >= 2 {
		if sliceOrTupleLens[1] < 0 || sliceOrTupleLens[1] > 0xFFFF {
			inexact = true
		} else {
			s1 = uint64(sliceOrTupleLens[1])
		}
	}
	if !inexact {
		return (uint64(stmtID) << 48) | ((colMask & 0xFF) << 40) | ((flagMask & 0xFF) << 32) | (s0 << 16) | s1
	}
	h := mix64(uint64(stmtID)<<32 ^ colMask<<16 ^ flagMask ^ orderKey)
	for i, l := range sliceOrTupleLens {
		h = mix64(h ^ (uint64(l) + uint64(i+1)*0x517cc1b727220a95))
	}
	return InexactKeyBit | (h & ^InexactKeyBit)
}

// Order encodes up to 7 packed sort terms in a 64-bit integer.
type Order uint64

// OrderTerm represents a single expanded column index (0-based) and sort direction.
type OrderTerm struct {
	ColIdx uint8
	Desc   bool
}

// Asc encodes a 0-based sortable column index as an ascending sort term.
func Asc(colIdx uint8) uint8 {
	return ((colIdx + 1) << 1) | 0
}

// Desc encodes a 0-based sortable column index as a descending sort term.
func Desc(colIdx uint8) uint8 {
	return ((colIdx + 1) << 1) | 1
}

// PackOrder packs up to 7 sort term tokens into an Order value.
func PackOrder(terms ...uint8) Order {
	limit := len(terms)
	if limit > 7 {
		limit = 7
	}
	var out uint64
	outIdx := 0
	var seen uint16
	for i := 0; i < limit; i++ {
		t := terms[i]
		col1 := t >> 1
		if col1 == 0 {
			continue
		}
		if col1 <= 15 {
			bit := uint16(1) << col1
			if seen&bit != 0 {
				continue
			}
			seen |= bit
		}
		out |= uint64(t) << uint(outIdx*8)
		outIdx++
	}
	return Order(out)
}

// ExpandOrder unpacks spec against numCols declared sortable columns and appends
// the final tie-breaker column (numCols-1) if omitted by the caller.
func ExpandOrder(spec Order, numCols int) ([8]OrderTerm, int, error) {
	var terms [8]OrderTerm
	if numCols <= 0 || numCols > 15 {
		return terms, 0, ErrInvalidOrder
	}
	tieBreaker := uint8(numCols - 1)
	if spec == 0 {
		terms[0] = OrderTerm{ColIdx: tieBreaker, Desc: false}
		return terms, 1, nil
	}

	count := 0
	hasTieBreaker := false
	lastDesc := false
	var seen uint16

	for i := 0; i < 7; i++ {
		b := uint8(( uint64(spec) >> uint(i*8) ) & 0xFF)
		if b == 0 {
			break
		}
		col1 := b >> 1
		if col1 == 0 || int(col1) > numCols {
			return terms, 0, ErrInvalidOrder
		}
		colIdx := col1 - 1
		bit := uint16(1) << colIdx
		if seen&bit != 0 {
			continue
		}
		seen |= bit
		desc := (b & 1) != 0
		terms[count] = OrderTerm{ColIdx: colIdx, Desc: desc}
		count++
		lastDesc = desc
		if colIdx == tieBreaker {
			hasTieBreaker = true
		}
	}

	if count == 0 {
		terms[0] = OrderTerm{ColIdx: tieBreaker, Desc: false}
		return terms, 1, nil
	}

	if !hasTieBreaker {
		terms[count] = OrderTerm{ColIdx: tieBreaker, Desc: lastDesc}
		count++
	}
	return terms, count, nil
}

// EncodeOrderKey combines Order and HasSeek into a 64-bit VariantKey word.
func EncodeOrderKey(spec Order, hasSeek bool) uint64 {
	k := uint64(spec) << 8
	if hasSeek {
		k |= 1
	}
	return k
}

// Seek holds keyset pagination cursor values inline without heap slice allocation.
type Seek struct {
	vals   [15]any
	count  uint8
	active bool
}

// NewSeek constructs an active Seek cursor with the given column values.
func NewSeek(vals ...any) Seek {
	if len(vals) == 0 {
		return Seek{}
	}
	var s Seek
	s.active = true
	if len(vals) > 15 {
		s.count = 15
	} else {
		s.count = uint8(len(vals))
	}
	for i := uint8(0); i < s.count; i++ {
		s.vals[i] = vals[i]
	}
	return s
}

// IsActive reports whether the Seek cursor is populated.
func (s Seek) IsActive() bool {
	return s.active && s.count > 0
}

// Count returns the number of values stored in the Seek cursor.
func (s Seek) Count() int {
	return int(s.count)
}

// Val returns the i-th stored value in the Seek cursor.
func (s Seek) Val(i int) any {
	return s.vals[i]
}

// AppendSeekArgs validates the Seek cursor against the expanded Order and appends
// the lexicographical keyset comparison bind arguments to dst.
func AppendSeekArgs(dst *[]any, seek Seek, spec Order, numOrderCols int) error {
	terms, termCount, err := ExpandOrder(spec, numOrderCols)
	if err != nil {
		return err
	}
	if !seek.IsActive() {
		return nil
	}
	byDeclaredCol := int(seek.count) == numOrderCols
	byExpandedPos := int(seek.count) == termCount
	if !byDeclaredCol && !byExpandedPos {
		return ErrInvalidSeek
	}

	var activeVals [8]any
	for i := 0; i < termCount; i++ {
		if byDeclaredCol {
			activeVals[i] = seek.vals[terms[i].ColIdx]
		} else {
			activeVals[i] = seek.vals[i]
		}
	}

	args := *dst
	for i := 0; i < termCount; i++ {
		for j := 0; j <= i; j++ {
			args = append(args, activeVals[j])
		}
	}
	*dst = args
	return nil
}

// SlotKind identifies a dynamic placeholder or structural directive in a QueryDescriptor.
type SlotKind uint8

const (
	// SlotScalar is a single scalar bind parameter ($1 or ?).
	SlotScalar SlotKind = iota
	// SlotSlice is a 1D slice bind parameter ($1, $2, ... or ?, ?, ...).
	SlotSlice
	// SlotTupleSlice is a multi-row tuple slice parameter ((...), (...)).
	SlotTupleSlice
	// SlotOptBlock is a conditional SQL block ({?Flag: ...}).
	SlotOptBlock
	// SlotQuery is a runtime-composed subquery (Sub:sqlgen.Query).
	SlotQuery
	// SlotSelectCols is a dynamic unconditional column projection list.
	SlotSelectCols
	// SlotOrder is a dynamic multi-column ORDER BY clause.
	SlotOrder
	// SlotSeek is a dynamic keyset pagination predicate.
	SlotSeek
)

// TupleDescriptor describes the SQL template of a single tuple in a SlotTupleSlice.
type TupleDescriptor struct {
	Segments  []string // len(Segments) == NumFields + 1
	NumFields int
}

// Slot represents a single dynamic expansion site in a QueryDescriptor.
type Slot struct {
	Kind    SlotKind
	FlagBit uint8
	Tuple   *TupleDescriptor
	Inner   *QueryDescriptor
}

// QueryDescriptor holds the pre-parsed static SQL segments and dynamic slots for a statement.
type QueryDescriptor struct {
	Segments       []string // len(Segments) == len(Slots) + 1
	Slots          []Slot
	SelectColExprs []string
	OrderCols      []string
	NumSlices      int
	NumTuples      int
	NumSubs        int
}

// RenderInput carries the runtime shape coordinates needed to render a QueryDescriptor.
type RenderInput struct {
	ColMask   uint64
	FlagMask  uint64
	OrderSpec Order
	HasSeek   bool
	SliceLens []int
	TupleLens []int
	Subs      []Query
}

// ArgsExpander appends a Query's bound arguments into dst.
type ArgsExpander func(dst *[]any, q *Query) error

// Query represents a curried, composable SQL query value with bound typed parameters.
type Query struct {
	StmtID   uint32
	Desc     *QueryDescriptor
	Input    RenderInput
	Params   any
	Expand   ArgsExpander
	ShapeTag uint64
}

// IsValid reports whether q was initialized by a statement Query constructor.
func (q Query) IsValid() bool {
	return q.Desc != nil && q.Expand != nil
}

// AppendArgs validates and appends all bind arguments of q to dst.
func (q *Query) AppendArgs(dst *[]any) error {
	if !q.IsValid() {
		return ErrNilQuery
	}
	return q.Expand(dst, q)
}

// BuildSQL renders the SQL string for q in the given dialect.
func (q Query) BuildSQL(dialect Dialect) string {
	if q.Desc == nil {
		return ""
	}
	return BuildComposedSQL(q.Desc, &q.Input, dialect)
}

type renderCursor struct {
	dialect  Dialect
	paramIdx int
	sliceIdx int
	tupleIdx int
	subIdx   int
}

// BuildComposedSQL renders desc with in for dialect into a single SQL string.
func BuildComposedSQL(desc *QueryDescriptor, in *RenderInput, dialect Dialect) string {
	var buf []byte
	baseLen := 0
	for _, seg := range desc.Segments {
		baseLen += len(seg)
	}
	buf = make([]byte, 0, baseLen+64)
	cur := renderCursor{
		dialect:  dialect,
		paramIdx: 1,
	}
	buf = writeDescriptorSQL(buf, desc, desc, in, &cur)
	return strings.TrimSpace(string(buf))
}

func writeDescriptorSQL(buf []byte, rootDesc, desc *QueryDescriptor, in *RenderInput, cur *renderCursor) []byte {
	for i, slot := range desc.Slots {
		buf = append(buf, desc.Segments[i]...)
		switch slot.Kind {
		case SlotSelectCols:
			numCols := len(rootDesc.SelectColExprs)
			if numCols > 0 {
				validMask := (uint64(1) << uint(numCols)) - 1
				effectiveMask := in.ColMask & validMask
				if effectiveMask == 0 {
					effectiveMask = validMask
				}
				first := true
				for bit := 0; bit < numCols; bit++ {
					if (effectiveMask & (uint64(1) << uint(bit))) != 0 {
						if !first {
							buf = append(buf, ", "...)
						}
						first = false
						buf = append(buf, rootDesc.SelectColExprs[bit]...)
					}
				}
			}

		case SlotScalar:
			buf = AppendPlaceholder(buf, cur.dialect, cur.paramIdx)
			cur.paramIdx++

		case SlotSlice:
			n := 0
			if cur.sliceIdx < len(in.SliceLens) {
				n = in.SliceLens[cur.sliceIdx]
			}
			cur.sliceIdx++
			for j := 0; j < n; j++ {
				if j > 0 {
					buf = append(buf, ", "...)
				}
				buf = AppendPlaceholder(buf, cur.dialect, cur.paramIdx)
				cur.paramIdx++
			}

		case SlotTupleSlice:
			numRows := 0
			if cur.tupleIdx < len(in.TupleLens) {
				numRows = in.TupleLens[cur.tupleIdx]
			}
			cur.tupleIdx++
			td := slot.Tuple
			for r := 0; r < numRows; r++ {
				if r > 0 {
					buf = append(buf, ", "...)
				}
				buf = append(buf, '(')
				for f := 0; f < td.NumFields; f++ {
					buf = append(buf, td.Segments[f]...)
					buf = AppendPlaceholder(buf, cur.dialect, cur.paramIdx)
					cur.paramIdx++
				}
				buf = append(buf, td.Segments[td.NumFields]...)
				buf = append(buf, ')')
			}

		case SlotQuery:
			var sub Query
			if cur.subIdx < len(in.Subs) {
				sub = in.Subs[cur.subIdx]
			}
			cur.subIdx++
			if sub.Desc != nil {
				subCur := renderCursor{
					dialect:  cur.dialect,
					paramIdx: cur.paramIdx,
				}
				buf = writeDescriptorSQL(buf, sub.Desc, sub.Desc, &sub.Input, &subCur)
				cur.paramIdx = subCur.paramIdx
			}

		case SlotOptBlock:
			active := (in.FlagMask & (uint64(1) << slot.FlagBit)) != 0
			if active && slot.Inner != nil {
				buf = writeDescriptorSQL(buf, rootDesc, slot.Inner, in, cur)
			} else if slot.Inner != nil {
				cur.sliceIdx += slot.Inner.NumSlices
				cur.tupleIdx += slot.Inner.NumTuples
				cur.subIdx += slot.Inner.NumSubs
			}

		case SlotOrder:
			terms, termCount, err := ExpandOrder(in.OrderSpec, len(rootDesc.OrderCols))
			if err == nil {
				for t := 0; t < termCount; t++ {
					if t > 0 {
						buf = append(buf, ", "...)
					}
					buf = append(buf, rootDesc.OrderCols[terms[t].ColIdx]...)
					if terms[t].Desc {
						buf = append(buf, " DESC"...)
					} else {
						buf = append(buf, " ASC"...)
					}
				}
			}

		case SlotSeek:
			if !in.HasSeek || len(rootDesc.OrderCols) == 0 {
				buf = append(buf, "1=1"...)
			} else {
				terms, termCount, err := ExpandOrder(in.OrderSpec, len(rootDesc.OrderCols))
				if err != nil || termCount == 0 {
					buf = append(buf, "1=1"...)
				} else {
					buf = append(buf, '(')
					for t := 0; t < termCount; t++ {
						if t > 0 {
							buf = append(buf, " OR "...)
						}
						if termCount > 1 {
							buf = append(buf, '(')
						}
						for j := 0; j < t; j++ {
							buf = append(buf, rootDesc.OrderCols[terms[j].ColIdx]...)
							buf = append(buf, " = "...)
							buf = AppendPlaceholder(buf, cur.dialect, cur.paramIdx)
							cur.paramIdx++
							buf = append(buf, " AND "...)
						}
						buf = append(buf, rootDesc.OrderCols[terms[t].ColIdx]...)
						if terms[t].Desc {
							buf = append(buf, " < "...)
						} else {
							buf = append(buf, " > "...)
						}
						buf = AppendPlaceholder(buf, cur.dialect, cur.paramIdx)
						cur.paramIdx++
						if termCount > 1 {
							buf = append(buf, ')')
						}
					}
					buf = append(buf, ')')
				}
			}
		}
	}
	if len(desc.Segments) > len(desc.Slots) {
		buf = append(buf, desc.Segments[len(desc.Slots)]...)
	}
	return buf
}

// BatchExecResult aggregates sql.Result across multiple batch chunks.
type BatchExecResult struct {
	RowsAffectedVal int64
	LastInsertIDVal int64
	LastInsertIDErr error
}

// LastInsertId returns the last insert ID from the final batch chunk.
func (r BatchExecResult) LastInsertId() (int64, error) {
	return r.LastInsertIDVal, r.LastInsertIDErr
}

// RowsAffected returns the sum of rows affected across all executed batch chunks.
func (r BatchExecResult) RowsAffected() (int64, error) {
	return r.RowsAffectedVal, nil
}

// Add accumulates a chunk sql.Result into r.
func (r *BatchExecResult) Add(res sql.Result) {
	if res == nil {
		return
	}
	if n, err := res.RowsAffected(); err == nil {
		r.RowsAffectedVal += n
	}
	id, err := res.LastInsertId()
	r.LastInsertIDVal = id
	r.LastInsertIDErr = err
}
