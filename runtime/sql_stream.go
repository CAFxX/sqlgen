package runtime

import (
	"fmt"
	"strconv"
	"strings"
)

type streamCursor struct {
	dialect  Dialect
	castVals bool // inside a branch-gated INSERT's value list under DialectDollar: write Slot.Cast
	paramIdx int
	numBuf   [20]byte
	sliceIdx int
	tupleIdx int
	subIdx   int
	// colArgs collects the arguments of the WhereCol columns written at SlotColPreds (nil: they are written
	// as literals on the MySQL flavor, or not at all). Under DialectDollar they are numbered after the
	// operation's own colBase arguments (the driver appends them); under DialectQuestion they must be
	// inserted after the first colAt of the operation's own arguments (the placeholders written before).
	colArgs *[]any
	colBase int
	colAt   int
	// my is non-nil when rendering for WireFlavorMySQL: placeholders become inlined literals and the
	// branch gates read session variables (see mysql_wire.go).
	my *mysqlCursor
	// refs is set (PostgreSQL flavor) when the operation's arguments hold forwarded values: refs.args are
	// the arguments, and refs.before[k] the number of forwarded values among args[:k]. A forwarded value's
	// slot reads the producer's record instead of a placeholder, and the later placeholders are numbered
	// without it (the driver receives the arguments without the forwarded values).
	refs *refSlots
}

type refSlots struct {
	args   []any
	before []int32
}

// newRefSlots returns the refSlots of args, or nil when they hold no forwarded value.
func newRefSlots(args []any) *refSlots {
	var rs *refSlots
	for k, a := range args {
		if _, ok := a.(*argRef); ok && rs == nil {
			rs = &refSlots{args: args, before: make([]int32, len(args)+1)}
			for j := k; j < len(args); j++ {
				rs.before[j+1] = rs.before[j]
				if _, ok := args[j].(*argRef); ok {
					rs.before[j+1]++
				}
			}
		}
	}
	return rs
}

// ref returns the forwarded value of the idx-th (1-based) argument, or nil.
func (rs *refSlots) ref(idx int) *argRef {
	if rs == nil || idx < 1 || idx > len(rs.args) {
		return nil
	}
	r, _ := rs.args[idx-1].(*argRef)
	return r
}

// num returns the $N number of the idx-th (1-based) argument once the forwarded values are removed.
func (rs *refSlots) num(idx int) int {
	if rs == nil || idx < 1 {
		return idx
	}
	k := idx - 1
	if k > len(rs.args) {
		k = len(rs.args)
	}
	return idx - int(rs.before[k])
}

func (cur *streamCursor) writeDollar(b *strings.Builder, idx int) {
	b.WriteByte('$')
	b.Write(strconv.AppendInt(cur.numBuf[:0], int64(cur.refs.num(idx)), 10))
}

func (cur *streamCursor) writePlaceholder(b *strings.Builder) {
	if cur.my != nil {
		cur.my.writeArg(b, cur.paramIdx)
	} else if cur.dialect == DialectQuestion {
		b.WriteByte('?')
	} else {
		cur.writeDollar(b, cur.paramIdx)
	}
	cur.paramIdx++
}

// writeScalar writes the placeholder of a scalar slot: on the PostgreSQL flavor a forwarded value reads the
// producer's record, cast to the parameter's type (cast; an explicit cast in the SQL follows it).
func (cur *streamCursor) writeScalar(b *strings.Builder, cast string) {
	r := cur.refs.ref(cur.paramIdx)
	if r == nil || cur.my != nil {
		cur.writePlaceholder(b)
		if cur.castVals && cast != "" {
			b.WriteString("::")
			b.WriteString(cast)
		}
		return
	}
	cur.paramIdx++
	b.WriteString("(CASE WHEN ")
	writeValSetting(b, cur.numBuf[:0], r.id)
	b.WriteString(" LIKE 'v%' THEN substr(")
	writeValSetting(b, cur.numBuf[:0], r.id)
	b.WriteString(", 2) END)")
	if cast != "" {
		b.WriteString("::")
		b.WriteString(cast)
	}
}

// writeValSetting writes current_setting('sqlgen.v<id>', true): the record a producer made of a forwarded
// value, 'v<text>' (resolved), 'n' (resolved, NULL), 'x<rows>' (unresolved), NULL or ” (never recorded).
func writeValSetting(b *strings.Builder, numBuf []byte, id int) {
	b.WriteString("current_setting('sqlgen.v")
	b.Write(strconv.AppendInt(numBuf, int64(id), 10))
	b.WriteString("', true)")
}

// refSlotBytes bounds what writeScalar writes for a forwarded value, cast excluded.
const refSlotBytes = 200 // the cast included (pgCastForGoType names at most 16 bytes)

func formatOrderSQL(b *strings.Builder, orderCols []string, orderSpec uint64) {
	cols, desc, count, _ := expandOrder(orderSpec, len(orderCols))
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(orderCols[cols[i]])
		if desc[i] {
			b.WriteString(" DESC")
		} else {
			b.WriteString(" ASC")
		}
	}
}

func (cur *streamCursor) formatSeekSQL(b *strings.Builder, orderCols []string, orderSpec uint64, hasSeek bool) {
	if !hasSeek || len(orderCols) == 0 {
		b.WriteString("TRUE")
		return
	}
	cols, desc, count, allSameDir := expandOrder(orderSpec, len(orderCols))
	writeParam := func(idx int) {
		if cur.my != nil {
			cur.my.writeArg(b, idx)
		} else if cur.dialect == DialectQuestion {
			b.WriteByte('?')
		} else {
			cur.writeDollar(b, idx)
		}
	}
	if count == 1 {
		b.WriteString(orderCols[cols[0]])
		if desc[0] {
			b.WriteString(" < ")
		} else {
			b.WriteString(" > ")
		}
		writeParam(cur.paramIdx)
		cur.paramIdx++
		return
	}
	if allSameDir {
		b.WriteByte('(')
		for i := 0; i < count; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(orderCols[cols[i]])
		}
		if desc[0] {
			b.WriteString(") < (")
		} else {
			b.WriteString(") > (")
		}
		for i := 0; i < count; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			writeParam(cur.paramIdx)
			cur.paramIdx++
		}
		b.WriteByte(')')
		return
	}
	// Mixed ASC/DESC directions: emit index-friendly bounding-box + lexicographical OR branches
	baseParam := cur.paramIdx
	if cur.dialect == DialectDollar {
		cur.paramIdx += count
	}
	nextColParam := func(colPos int) int {
		if cur.dialect == DialectDollar {
			return baseParam + colPos
		}
		p := cur.paramIdx
		cur.paramIdx++
		return p
	}
	b.WriteByte('(')
	b.WriteString(orderCols[cols[0]])
	if desc[0] {
		b.WriteString(" <= ")
	} else {
		b.WriteString(" >= ")
	}
	writeParam(nextColParam(0))
	b.WriteString(" AND (")
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteString(" OR (")
		}
		for j := 0; j < i; j++ {
			b.WriteString(orderCols[cols[j]])
			b.WriteString(" = ")
			writeParam(nextColParam(j))
			b.WriteString(" AND ")
		}
		b.WriteString(orderCols[cols[i]])
		if desc[i] {
			b.WriteString(" < ")
		} else {
			b.WriteString(" > ")
		}
		writeParam(nextColParam(i))
		if i > 0 {
			b.WriteByte(')')
		}
	}
	b.WriteString("))")
}

// hasGates reports whether the statements rendered for in must be gated: it has enclosing If branches
// or (MySQL flavor) an earlier operation of the batch may have failed an assertion or aborted.
func (cur *streamCursor) hasGates(in *RenderInput) bool {
	return len(in.BranchGates) > 0 || (cur.my != nil && cur.my.failGate)
}

func (cur *streamCursor) writeBranchGates(b *strings.Builder, gates []BranchGate) {
	if cur.my != nil {
		cur.my.writeGates(b, gates, cur.numBuf[:0])
		return
	}
	for i, g := range gates {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("current_setting('sqlgen.c")
		b.Write(strconv.AppendInt(cur.numBuf[:0], int64(g.CondID), 10))
		if g.WantTrue {
			b.WriteString("', true) = 'true'")
		} else {
			b.WriteString("', true) = 'false'")
		}
	}
}

// Markers of the errors a guard raises on purpose. A guard must fail the statement with an error that
// cannot be mistaken for anything else the statement may raise: an operation of the caller can itself
// fail with a genuine integer overflow, which has to surface unchanged. The guard therefore casts a
// text that carries the marker to an integer; PostgreSQL and CockroachDB answer with
// `invalid input syntax for type integer: "sqlgen:assert:3"` (22P02), which names the marker and, for an
// assertion, the number of rows the operation actually produced. TranslateWireError matches the marker.
const (
	wireAssertMarker = "sqlgen:assert:"
	wireAbortMarker  = "sqlgen:abort:"
	wireRefMarker    = "sqlgen:ref:"
)

// writeGuardRaise writes an expression that raises the guard error for marker when, and only when, it is
// evaluated. rows is the SQL of the row count the marker carries.
//
// DialectDollar must not use a constant expression: PostgreSQL constant-folds it at plan time, even
// inside the unreached branch of a CASE, which once made every guarded statement fail unconditionally.
// The aggregate in rows keeps the planner from folding it, so it is only evaluated when the CASE branch
// is actually taken.
//
// DialectQuestion (SQLite) keeps a constant overflow: that dialect has no wire path, and SQLite
// evaluates lazily.
func writeGuardRaise(b *strings.Builder, dialect Dialect, marker, rows string) {
	if dialect == DialectDollar {
		b.WriteString("('")
		b.WriteString(marker)
		b.WriteString("' || ")
		b.WriteString(rows)
		b.WriteString("::text)::int")
	} else {
		b.WriteString("ABS(-9223372036854775808)")
	}
}

// gateExtraBytes bounds what wrapping a leaf in "CASE WHEN (<gates>) THEN (...) ELSE FALSE END" adds.
func gateExtraBytes(numGates int) int {
	if numGates == 0 {
		return 0
	}
	return 48 + 56*numGates
}

// estimateBytes bounds the bytes writeSQL streams for c on an operation with numGates branch gates.
func (c *PipelineCond) estimateBytes(numGates int) int {
	if c == nil {
		return 8
	}
	switch c.Kind {
	case CondRowsEq, CondRowsGt, CondRowsGe, CondRowsLt, CondRowsLe:
		return 80 + gateExtraBytes(numGates)
	case CondColPred:
		return 80 + gateExtraBytes(numGates)
	case CondAnd:
		return 9 + c.Left.estimateBytes(numGates) + c.Right.estimateBytes(numGates)
	case CondOr:
		return 8 + c.Left.estimateBytes(numGates) + c.Right.estimateBytes(numGates)
	case CondNot:
		return 6 + c.Left.estimateBytes(numGates)
	default:
		return 16
	}
}

// writeSQL streams c as a boolean expression for the statement of an operation that has the given
// branch gates. The expression is evaluated inside an aggregate SELECT over that operation's rows
// (_sqlgen_op); a leaf over a different operation reads the value its own operation recorded.
func (c *PipelineCond) writeSQL(b *strings.Builder, cur *streamCursor, gates []BranchGate) {
	if c == nil {
		b.WriteString("TRUE")
		return
	}
	switch c.Kind {
	case CondRowsEq, CondRowsGt, CondRowsGe, CondRowsLt, CondRowsLe, CondColPred:
		c.writeLeafSQL(b, cur, gates)
	case CondAnd:
		b.WriteByte('(')
		c.Left.writeSQL(b, cur, gates)
		b.WriteString(" AND ")
		c.Right.writeSQL(b, cur, gates)
		b.WriteByte(')')
	case CondOr:
		b.WriteByte('(')
		c.Left.writeSQL(b, cur, gates)
		b.WriteString(" OR ")
		c.Right.writeSQL(b, cur, gates)
		b.WriteByte(')')
	case CondNot:
		b.WriteString("NOT (")
		c.Left.writeSQL(b, cur, gates)
		b.WriteByte(')')
	default:
		b.WriteString("TRUE")
	}
}

// writeLeafSQL streams a leaf. Its value is FALSE when its operation did not run (the operation's own
// branch gates are closed), exactly as PipelineCond.EvalGoFn treats a skipped operation, so Not(leaf)
// is TRUE then; NULL gates (never recorded) count as closed.
func (c *PipelineCond) writeLeafSQL(b *strings.Builder, cur *streamCursor, gates []BranchGate) {
	if c.Recorded != 0 {
		b.WriteString("COALESCE(current_setting('sqlgen.l")
		b.Write(strconv.AppendInt(cur.numBuf[:0], int64(c.Recorded), 10))
		b.WriteString("', true) = 'true', false)")
		return
	}
	if c.Op == nil {
		b.WriteString("FALSE") // no operation: EvalGoFn is false
		return
	}
	if len(gates) > 0 {
		b.WriteString("CASE WHEN (")
		cur.writeBranchGates(b, gates)
		b.WriteString(") THEN (")
	}
	switch c.Kind {
	case CondRowsEq:
		b.WriteString("COUNT(*) = ")
		b.Write(strconv.AppendInt(cur.numBuf[:0], c.Threshold, 10))
	case CondRowsGt:
		b.WriteString("COUNT(*) > ")
		b.Write(strconv.AppendInt(cur.numBuf[:0], c.Threshold, 10))
	case CondRowsGe:
		b.WriteString("COUNT(*) >= ")
		b.Write(strconv.AppendInt(cur.numBuf[:0], c.Threshold, 10))
	case CondRowsLt:
		b.WriteString("COUNT(*) < ")
		b.Write(strconv.AppendInt(cur.numBuf[:0], c.Threshold, 10))
	case CondRowsLe:
		b.WriteString("COUNT(*) <= ")
		b.Write(strconv.AppendInt(cur.numBuf[:0], c.Threshold, 10))
	case CondColPred:
		if c.Col == 0 {
			b.WriteString("FALSE")
			break
		}
		b.WriteString("COUNT(*) > 0 AND MIN(_sqlgen_wc")
		b.Write(strconv.AppendInt(cur.numBuf[:0], int64(c.Col), 10))
		b.WriteString(") = 1")
	}
	if len(gates) > 0 {
		b.WriteString(") ELSE FALSE END")
	}
}

type estCursor struct {
	sliceIdx int
	tupleIdx int
	subIdx   int
}

func estimateDescriptorBytes(rootDesc, desc *QueryDescriptor, in *RenderInput, cur *estCursor) int {
	if desc == nil {
		return 0
	}
	total := 0
	for _, seg := range desc.Segments {
		total += len(seg)
	}
	numGates := len(in.BranchGates)
	for _, slot := range desc.Slots {
		switch slot.Kind {
		case SlotScalar:
			total += 12
			if len(in.BranchGates) > 0 {
				total += len(slot.Cast) + 2
			}
		case SlotSlice:
			n := 0
			if cur.sliceIdx < len(in.SliceLens) {
				n = in.SliceLens[cur.sliceIdx]
			}
			cur.sliceIdx++
			if n > 0 {
				total += 12 * n
			}
		case SlotTupleSlice:
			numRows := 0
			if cur.tupleIdx < len(in.TupleLens) {
				numRows = in.TupleLens[cur.tupleIdx]
			}
			cur.tupleIdx++
			if numRows > 0 && slot.Tuple != nil {
				rowBytes := 4
				for _, seg := range slot.Tuple.Segments {
					rowBytes += len(seg) + 12
				}
				if len(in.BranchGates) > 0 {
					for _, c := range slot.Tuple.Casts {
						rowBytes += len(c) + 2
					}
				}
				total += numRows * rowBytes
			}
		case SlotQuery:
			var sub Query
			if cur.subIdx < len(in.Subs) {
				sub = in.Subs[cur.subIdx]
			}
			cur.subIdx++
			if sub.Desc != nil {
				total += estimateOpBytes(sub.Desc, &sub.Input, nil)
			}
		case SlotSelectCols:
			for _, expr := range rootDesc.SelectColExprs {
				total += len(expr) + 2
			}
		case SlotOptBlock:
			active := (in.FlagMask & (uint64(1) << slot.FlagBit)) != 0
			if active && slot.Inner != nil {
				total += estimateDescriptorBytes(rootDesc, slot.Inner, in, cur)
			} else if slot.Inner != nil {
				cur.sliceIdx += slot.Inner.NumSlices
				cur.tupleIdx += slot.Inner.NumTuples
				cur.subIdx += slot.Inner.NumSubs
			}
		case SlotOrder:
			for _, col := range rootDesc.OrderCols {
				total += len(col) + 7
			}
		case SlotSeek:
			if !in.HasSeek || len(rootDesc.OrderCols) == 0 {
				total += 4
			} else {
				k := len(rootDesc.OrderCols)
				maxColLen := 0
				for _, col := range rootDesc.OrderCols {
					if len(col) > maxColLen {
						maxColLen = len(col)
					}
				}
				total += (1+(k*(k+1))/2)*(maxColLen+24) + 32
			}
		case SlotInsertValuesOpen, SlotInsertValuesClose, SlotInsertTupleValuesOpen, SlotInsertTupleValuesClose:
			if numGates > 0 {
				total += 96 + 56*numGates
			} else {
				total += 16
			}
		case SlotBranchGuardOpen, SlotBranchGuardClose, SlotBranchGuardWhere:
			if numGates > 0 {
				total += 16 + 56*numGates
			}
		case SlotColPreds:
			total += colPredBytes(in.ColPreds)
		case SlotSelectGuardOpen, SlotSelectGuardClose, SlotSelectGuardHaving:
			total += 16 + 56*numGates
		}
	}
	return total
}

// valRecBytes bounds the recorder of a forwarded value (the column name excluded); checkRefBytes bounds
// what writeCheckSQL writes per forwarded value, checkBaseBytes the rest (gates excluded).
const (
	valRecBytes    = 520 // the recorder, its read in the final SELECT, and the WITH / LATERAL scaffolding
	checkRefBytes  = 200
	checkBaseBytes = 64
)

// estimateWireOpBytes bounds what renderPipelineBatch writes for op: its SQL and, for an operation that
// uses forwarded values, the slots that read them and its CheckSQL.
func estimateWireOpBytes(op *PipelineOp) int {
	n := estimateOpBytes(op.Desc, &op.Input, &op.Decorators)
	if op.nRefs > 0 {
		n += op.nRefs*(refSlotBytes+checkRefBytes) + checkBaseBytes + 56*len(op.Input.BranchGates)
	}
	return n
}

// estimateOpBytes computes a provable mathematical upper bound on the byte length
// of the SQL string rendered for (desc, in, dec).
func estimateOpBytes(desc *QueryDescriptor, in *RenderInput, dec *OpDecorators) int {
	var emptyIn RenderInput
	if in == nil {
		in = &emptyIn
	}
	numGates := len(in.BranchGates)
	if dec != nil && dec.IsAbort {
		return 144 + 56*numGates
	}
	cur := estCursor{}
	total := estimateDescriptorBytes(desc, desc, in, &cur)
	if desc != nil && desc.StmtVerb == VerbSelect && numGates > 0 {
		total += 48 + 56*numGates
	}
	if dec != nil {
		if dec.AssertRows >= 0 {
			total += 400 + 56*numGates
		}
		if dec.ProduceCond != nil {
			total += 480 + dec.ProduceCond.estimateBytes(numGates)
			for i := range dec.ExtraConds {
				total += 480 + dec.ExtraConds[i].Cond.estimateBytes(numGates)
			}
		}
		for i := range dec.LeafRecs {
			total += 480 + dec.LeafRecs[i].Cond.estimateBytes(numGates)
		}
		for i := range dec.ValRecs {
			total += valRecBytes + len(dec.ValRecs[i].Col)
		}
	}
	if total < 8 {
		total = 8
	}
	return total
}

func streamDescriptorSQL(b *strings.Builder, rootDesc, desc *QueryDescriptor, in *RenderInput, cur *streamCursor) {
	if desc == nil {
		return
	}
	for i, slot := range desc.Slots {
		b.WriteString(desc.Segments[i])
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
							b.WriteString(", ")
						}
						first = false
						b.WriteString(rootDesc.SelectColExprs[bit])
					}
				}
			}

		case SlotScalar:
			cur.writeScalar(b, slot.Cast)

		case SlotSlice:
			n := 0
			if cur.sliceIdx < len(in.SliceLens) {
				n = in.SliceLens[cur.sliceIdx]
			}
			cur.sliceIdx++
			for j := 0; j < n; j++ {
				if j > 0 {
					b.WriteString(", ")
				}
				cur.writePlaceholder(b)
			}

		case SlotTupleSlice:
			numRows := 0
			if cur.tupleIdx < len(in.TupleLens) {
				numRows = in.TupleLens[cur.tupleIdx]
			}
			cur.tupleIdx++
			td := slot.Tuple
			if td != nil && cur.my != nil && cur.hasGates(in) {
				// Gated multi-row INSERT on MySQL/MariaDB: there is no VALUES table constructor that can be
				// filtered (and ROW() is not portable), so every row is a SELECT with the gates, glued by
				// UNION ALL. No derived table: it would need unique column names.
				for r := 0; r < numRows; r++ {
					if r > 0 {
						b.WriteString(" UNION ALL ")
					}
					b.WriteString("SELECT ")
					for f := 0; f < td.NumFields; f++ {
						b.WriteString(td.Segments[f])
						cur.writePlaceholder(b)
					}
					b.WriteString(td.Segments[td.NumFields])
					b.WriteString(" FROM DUAL WHERE ")
					cur.writeBranchGates(b, in.BranchGates)
				}
			} else if td != nil {
				for r := 0; r < numRows; r++ {
					if r > 0 {
						b.WriteString(", ")
					}
					b.WriteByte('(')
					for f := 0; f < td.NumFields; f++ {
						b.WriteString(td.Segments[f])
						cur.writePlaceholder(b)
						if cur.castVals && f < len(td.Casts) && td.Casts[f] != "" {
							b.WriteString("::")
							b.WriteString(td.Casts[f])
						}
					}
					b.WriteString(td.Segments[td.NumFields])
					b.WriteByte(')')
				}
			}

		case SlotQuery:
			var sub Query
			if cur.subIdx < len(in.Subs) {
				sub = in.Subs[cur.subIdx]
			}
			cur.subIdx++
			if sub.Desc != nil {
				subCur := streamCursor{
					dialect:  cur.dialect,
					paramIdx: cur.paramIdx,
					my:       cur.my.forSub(),
					refs:     cur.refs,
				}
				streamDescriptorSQL(b, sub.Desc, sub.Desc, &sub.Input, &subCur)
				cur.paramIdx = subCur.paramIdx
			}

		case SlotOptBlock:
			active := (in.FlagMask & (uint64(1) << slot.FlagBit)) != 0
			if active && slot.Inner != nil {
				streamDescriptorSQL(b, rootDesc, slot.Inner, in, cur)
			} else if slot.Inner != nil {
				cur.sliceIdx += slot.Inner.NumSlices
				cur.tupleIdx += slot.Inner.NumTuples
				cur.subIdx += slot.Inner.NumSubs
			}

		case SlotOrder:
			formatOrderSQL(b, rootDesc.OrderCols, in.OrderSpec)

		case SlotSeek:
			cur.formatSeekSQL(b, rootDesc.OrderCols, in.OrderSpec, in.HasSeek)

		case SlotInsertValuesOpen:
			if cur.hasGates(in) {
				cur.castVals = cur.dialect == DialectDollar
				b.WriteString("SELECT ")
			} else {
				b.WriteString("VALUES (")
			}

		case SlotInsertValuesClose:
			cur.castVals = false
			if cur.hasGates(in) {
				b.WriteString(" FROM (SELECT 1) AS _sqlgen_dual WHERE ")
				cur.writeBranchGates(b, in.BranchGates)
			} else {
				b.WriteByte(')')
			}

		case SlotInsertTupleValuesOpen:
			if cur.my != nil && cur.hasGates(in) {
				// the rows are SELECTs (see SlotTupleSlice): no prefix
			} else if cur.hasGates(in) {
				cur.castVals = cur.dialect == DialectDollar
				b.WriteString("WITH _sqlgen_v AS (VALUES ")
			} else {
				b.WriteString("VALUES ")
			}

		case SlotInsertTupleValuesClose:
			cur.castVals = false
			if cur.my == nil && cur.hasGates(in) {
				b.WriteString(") SELECT * FROM _sqlgen_v WHERE ")
				cur.writeBranchGates(b, in.BranchGates)
			}

		case SlotBranchGuardOpen:
			if cur.hasGates(in) {
				b.WriteByte('(')
				cur.writeBranchGates(b, in.BranchGates)
				b.WriteString(") AND (")
			}

		case SlotBranchGuardClose:
			if cur.hasGates(in) {
				b.WriteByte(')')
			}

		case SlotBranchGuardWhere:
			if cur.hasGates(in) {
				b.WriteString(" WHERE ")
				cur.writeBranchGates(b, in.BranchGates)
			}

		case SlotColPreds:
			cur.writeColPreds(b, in.ColPreds)

		case SlotSelectGuardOpen:
			if cur.my != nil && cur.hasGates(in) {
				b.WriteByte('(')
				cur.writeBranchGates(b, in.BranchGates)
				b.WriteString(") AND (")
			}

		case SlotSelectGuardClose:
			if cur.my != nil && cur.hasGates(in) {
				b.WriteString(") ")
			}

		case SlotSelectGuardHaving:
			if cur.my != nil && cur.hasGates(in) {
				if w := b.String(); len(w) > 0 && w[len(w)-1] != ' ' && w[len(w)-1] != '\n' {
					b.WriteByte(' ')
				}
				b.WriteString("HAVING ")
				cur.writeBranchGates(b, in.BranchGates)
				b.WriteByte(' ')
			}
		}
	}
	if len(desc.Segments) > len(desc.Slots) {
		b.WriteString(desc.Segments[len(desc.Slots)])
	}
}

// colPredBytes bounds what writeColPreds writes for preds, their arguments' placeholders included (the
// literals of the MySQL flavor excluded: estimateMySQLOpBytes adds them).
func colPredBytes(preds []ColPred) int {
	n := 0
	for i := range preds {
		n += 64 + len(preds[i].SQL) + 12*len(preds[i].Args)
	}
	return n
}

// writeColPreds writes the WhereCol columns: ", CASE WHEN (<pred>) THEN 1 ELSE 0 END AS _sqlgen_wc<k>" per
// predicate, then a space (the slot may sit right before FROM). The CASE makes the column 1 exactly when
// the predicate is TRUE, on every engine (a bare boolean is a bool on PostgreSQL, an integer elsewhere).
func (cur *streamCursor) writeColPreds(b *strings.Builder, preds []ColPred) {
	if len(preds) == 0 {
		return
	}
	if cur.colArgs != nil && cur.dialect == DialectQuestion {
		cur.colAt = cur.paramIdx - 1
	}
	for k := range preds {
		pr := &preds[k]
		var lits []any
		if cur.my != nil && len(pr.Args) > 0 {
			var err error
			if lits, err = cur.my.st.fl.normalize(pr.Args); err != nil && cur.my.st.err == nil {
				cur.my.st.err = fmt.Errorf("WhereCol: %w", err)
			}
		}
		b.WriteString(", CASE WHEN (")
		writePred(b, pr.SQL, func(j int) {
			switch {
			case cur.my != nil:
				if j < len(lits) {
					cur.my.st.fl.WriteLiteral(b, lits[j])
				} else {
					b.WriteString("NULL")
				}
			case cur.dialect == DialectQuestion:
				b.WriteByte('?')
			default:
				n := cur.colBase + 1
				if cur.colArgs != nil {
					n += len(*cur.colArgs)
				}
				b.WriteByte('$')
				b.Write(strconv.AppendInt(cur.numBuf[:0], int64(n), 10))
			}
			if cur.my == nil && cur.colArgs != nil && j < len(pr.Args) {
				*cur.colArgs = append(*cur.colArgs, pr.Args[j])
			}
		})
		b.WriteString(") THEN 1 ELSE 0 END AS _sqlgen_wc")
		b.Write(strconv.AppendInt(cur.numBuf[:0], int64(k+1), 10))
	}
	b.WriteByte(' ')
}

// spliceColArgs returns the arguments of a statement rendered with WhereCol columns: own (the operation's
// arguments as sent) with the columns' arguments, appended under DialectDollar, inserted at their
// placeholders' position under DialectQuestion. It never writes into own.
func spliceColArgs(dialect Dialect, own, col []any, at int) []any {
	out := make([]any, 0, len(own)+len(col))
	if dialect == DialectQuestion {
		at = min(max(at, 0), len(own))
		out = append(out, own[:at]...)
		out = append(out, col...)
		return append(out, own[at:]...)
	}
	out = append(out, own...)
	return append(out, col...)
}

// renderColPredSQL renders op (a statement with WhereCol columns) for the sequential path, with its
// arguments.
func renderColPredSQL(dialect Dialect, op *PipelineOp) (string, []any) {
	var b strings.Builder
	b.Grow(estimateOpBytes(op.Desc, &op.Input, nil))
	var col []any
	cur := streamCursor{dialect: dialect, paramIdx: 1, colArgs: &col, colBase: len(op.Args)}
	streamCoreStatement(&b, &cur, op.Desc, &op.Input)
	return b.String(), spliceColArgs(dialect, op.Args, col, cur.colAt)
}

// hasSelectGuard reports whether the statement carries its gates in its own HAVING (MySQL flavor).
func (d *QueryDescriptor) hasSelectGuard() bool {
	for i := range d.Slots {
		if k := d.Slots[i].Kind; k == SlotSelectGuardClose || k == SlotSelectGuardHaving {
			return true
		}
	}
	return false
}

func streamCoreStatement(b *strings.Builder, cur *streamCursor, desc *QueryDescriptor, in *RenderInput) {
	if desc == nil {
		return
	}
	hasGates := cur.hasGates(in)
	if desc.StmtVerb == VerbSelect && hasGates && cur.my != nil {
		// The gates go into the statement's own HAVING: a derived table would lose its ORDER BY on
		// MariaDB and TiDB (SQL leaves the order of a derived table's rows unspecified).
		if desc.hasSelectGuard() {
			streamDescriptorSQL(b, desc, desc, in, cur)
			return
		}
		if desc.TopOrderBy && cur.my.st.err == nil {
			cur.my.st.err = fmt.Errorf("a gated SELECT whose ORDER BY a derived table would not keep (a UNION, or a clause in an optional block): %w", ErrWireNotApplicable)
		}
	}
	if desc.StmtVerb == VerbSelect && hasGates {
		b.WriteString("SELECT * FROM (")
	}
	streamDescriptorSQL(b, desc, desc, in, cur)
	if desc.StmtVerb == VerbSelect && hasGates {
		b.WriteString(") AS _sqlgen_if WHERE ")
		cur.writeBranchGates(b, in.BranchGates)
	}
}

// streamOpSQL writes the SQL of one operation. The arguments of its WhereCol columns are appended to
// *colArgs (see streamCursor.colArgs; nil when in has none), numbered after the colBase arguments the
// operation sends itself; it returns the colAt of the cursor.
func streamOpSQL(b *strings.Builder, dialect Dialect, desc *QueryDescriptor, in *RenderInput, dec *OpDecorators, colArgs *[]any, colBase int, refs *refSlots) (colAt int) {
	var emptyIn RenderInput
	if in == nil {
		in = &emptyIn
	}
	cur := streamCursor{
		dialect:  dialect,
		paramIdx: 1,
		colArgs:  colArgs,
		colBase:  colBase,
		refs:     refs,
	}
	defer func() { colAt = cur.colAt }()
	hasGates := len(in.BranchGates) > 0
	if dec != nil && dec.IsAbort {
		if hasGates {
			b.WriteString("SELECT CASE WHEN (")
			cur.writeBranchGates(b, in.BranchGates)
			b.WriteString(") THEN ")
			writeGuardRaise(b, dialect, wireAbortMarker, "(SELECT COUNT(*))")
			b.WriteString(" ELSE 1 END")
		} else {
			b.WriteString("SELECT ")
			writeGuardRaise(b, dialect, wireAbortMarker, "(SELECT COUNT(*))")
		}
		return 0
	}

	hasAssert := dec != nil && dec.AssertRows >= 0
	hasCond := dec != nil && dec.ProduceCond != nil
	var leafRecs []ProducedCond
	var valRecs []ValRec
	if dec != nil {
		leafRecs = dec.LeafRecs
		valRecs = dec.ValRecs
	}
	if !hasAssert && !hasCond && len(leafRecs) == 0 && len(valRecs) == 0 {
		streamCoreStatement(b, &cur, desc, in)
		return 0
	}

	b.WriteString("WITH _sqlgen_op AS (")
	streamCoreStatement(b, &cur, desc, in)
	if desc != nil && desc.StmtVerb != VerbSelect && !desc.HasReturning {
		b.WriteString(" RETURNING 1")
	}
	b.WriteByte(')')

	if hasAssert {
		b.WriteString(", _sqlgen_chk AS (SELECT CASE WHEN ")
		if hasGates {
			b.WriteString("NOT (")
			cur.writeBranchGates(b, in.BranchGates)
			b.WriteString(") OR ")
		}
		b.WriteString("COUNT(*) = ")
		b.Write(strconv.AppendInt(cur.numBuf[:0], dec.AssertRows, 10))
		b.WriteString(" THEN 1 ELSE ")
		writeGuardRaise(b, dialect, wireAssertMarker, "COUNT(*)")
		b.WriteString(" END AS _ok FROM _sqlgen_op)")
	}

	// Each recorded condition is a CTE that stores its value with the transaction-local set_config under
	// sqlgen.<kind><ID>. COUNT(*) makes the SELECT aggregate, so it yields exactly one row whatever
	// _sqlgen_op returned, and the CTE can be read by a scalar subquery.
	// writeName writes base followed by id (no suffix when id is 0).
	writeName := func(base string, id int) {
		b.WriteString(base)
		if id != 0 {
			b.Write(strconv.AppendInt(cur.numBuf[:0], int64(id), 10))
		}
	}
	writeRecorder := func(cteBase string, kind byte, id int, cond *PipelineCond, cteID int) {
		b.WriteString(", ")
		writeName(cteBase, cteID)
		b.WriteString(" AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.")
		b.WriteByte(kind)
		b.Write(strconv.AppendInt(cur.numBuf[:0], int64(id), 10))
		b.WriteString("', CASE WHEN (")
		cond.writeSQL(b, &cur, in.BranchGates)
		b.WriteString(") THEN 'true' ELSE 'false' END, true) = 'true' AS _ok FROM _sqlgen_op)")
	}
	if hasCond {
		writeRecorder("_sqlgen_cond", 'c', dec.CondID, dec.ProduceCond, 0)
		for i := range dec.ExtraConds {
			ec := &dec.ExtraConds[i]
			writeRecorder("_sqlgen_x", 'c', ec.CondID, ec.Cond, ec.CondID)
		}
	}
	for i := range leafRecs {
		writeRecorder("_sqlgen_l", 'l', leafRecs[i].CondID, leafRecs[i].Cond, leafRecs[i].CondID)
	}
	// A forwarded value is recorded as 'v' || its text when the operation yielded exactly one row ('n' for
	// NULL), otherwise as 'x' || the row count; the consumer casts the text back (see writeScalar). MIN is
	// the row itself when there is one, and works for every type through text (bool, json, uuid have no MIN).
	for i := range valRecs {
		b.WriteString(", ")
		writeName("_sqlgen_v", valRecs[i].ID)
		b.WriteString(" AS (SELECT set_config('sqlgen.v")
		b.Write(strconv.AppendInt(cur.numBuf[:0], int64(valRecs[i].ID), 10))
		b.WriteString("', CASE COUNT(*) WHEN 1 THEN COALESCE('v' || MIN((")
		b.WriteString(valRecs[i].Col)
		b.WriteString(")::text), 'n') ELSE 'x' || COUNT(*)::text END, true) AS _ok FROM _sqlgen_op)")
	}

	// The guards, in the order of the CTEs: the alias (the column name in the final SELECT) and the CTE
	// name are each a base plus the condition ID (0: none).
	guards := func(yield func(aliasBase, cteBase string, id int)) {
		if hasAssert {
			yield("_a", "_sqlgen_chk", 0)
		}
		if hasCond {
			yield("_c", "_sqlgen_cond", 0)
			for i := range dec.ExtraConds {
				yield("_x", "_sqlgen_x", dec.ExtraConds[i].CondID)
			}
		}
		for i := range leafRecs {
			yield("_l", "_sqlgen_l", leafRecs[i].CondID)
		}
		for i := range valRecs {
			yield("_v", "_sqlgen_v", valRecs[i].ID)
		}
	}
	writeGuardRead := func(aliasBase, cteBase string, id int) {
		b.WriteString(", (SELECT _ok FROM ")
		writeName(cteBase, id)
		b.WriteString(") AS ")
		writeName(aliasBase, id)
	}

	hasResultCols := desc != nil && (desc.StmtVerb == VerbSelect || desc.HasReturning)
	if hasResultCols {
		// _f carries the assertion / condition CTEs; they have side effects (an error, set_config) that
		// MUST run exactly when the batch runs, even if _sqlgen_op yields no rows. A plain join against
		// _f let PostgreSQL prune its unused columns and never evaluate the CTEs. Making _f the outer
		// side of a LATERAL join whose inner side references every _f column forces it to be evaluated
		// first, whatever the planner decides about join order or column pruning.
		b.WriteString(" SELECT _r.* FROM (SELECT 1 AS _o")
		guards(writeGuardRead)
		b.WriteString(") AS _f CROSS JOIN LATERAL (SELECT * FROM _sqlgen_op WHERE _f._o IS NOT NULL")
		guards(func(aliasBase, _ string, id int) {
			b.WriteString(" AND _f.")
			writeName(aliasBase, id)
			b.WriteString(" IS NOT NULL")
		})
		b.WriteString(") AS _r")
		return 0
	}
	// Every guard is read by a scalar subquery in the target list, which a statement can never drop:
	// an unreferenced CTE (here an assertion whose only trace would be an overflow error, or a
	// set_config) is pruned by PostgreSQL. The first column is the affected-row count.
	b.WriteString(" SELECT (SELECT COUNT(*) FROM _sqlgen_op) AS _cnt")
	guards(writeGuardRead)
	return 0
}

// BuildComposedSQL renders desc with in for dialect using a single pre-sized strings.Builder.
func BuildComposedSQL(desc *QueryDescriptor, in *RenderInput, dialect Dialect) string {
	if desc == nil {
		return ""
	}
	var emptyIn RenderInput
	if in == nil {
		in = &emptyIn
	}
	if desc.StaticSQL != "" && len(in.BranchGates) == 0 && in.ColMask == 0 && in.FlagMask == 0 && len(in.ColPreds) == 0 {
		if len(desc.Slots) == 0 || (dialect == DialectDollar) == strings.Contains(desc.StaticSQL, "$1") {
			return desc.StaticSQL
		}
	}
	est := estimateOpBytes(desc, in, nil)
	var b strings.Builder
	b.Grow(est)
	streamOpSQL(&b, dialect, desc, in, nil, nil, 0, nil)
	return b.String()
}

// dropCondArgs restores the operations' own Args where renderPipelineBatch rewrote them (WhereCol
// arguments appended, forwarded values removed), so that a batch a pipeliner declined
// (ErrWireNotApplicable) runs sequentially with the statements' own arguments.
func dropCondArgs(ops []PipelineOp) {
	for i := range ops {
		if ops[i].argsRewritten {
			ops[i].Args = ops[i].origArgs
			ops[i].origArgs = nil
			ops[i].argsRewritten = false
		}
		ops[i].CheckSQL = ""
	}
}

// writeCheckSQL writes the statement that fails (22P02 with the marker sqlgen:ref:<ID>:<record>) when op's
// branch gates are open and one of its forwarded values is unresolved: the first in argument order, so
// that the error names the same producer as on the sequential path.
func writeCheckSQL(b *strings.Builder, op *PipelineOp) {
	var numBuf [20]byte
	b.WriteString("SELECT CASE")
	if gates := op.Input.BranchGates; len(gates) > 0 {
		cur := streamCursor{dialect: DialectDollar}
		b.WriteString(" WHEN (")
		cur.writeBranchGates(b, gates)
		b.WriteString(") IS NOT TRUE THEN 1")
	}
	op.forEachRef(func(r *argRef) {
		b.WriteString(" WHEN COALESCE(left(")
		writeValSetting(b, numBuf[:0], r.id)
		b.WriteString(", 1), '') NOT IN ('v', 'n') THEN ('")
		b.WriteString(wireRefMarker)
		b.Write(strconv.AppendInt(numBuf[:0], int64(r.id), 10))
		b.WriteString(":' || COALESCE(")
		writeValSetting(b, numBuf[:0], r.id)
		b.WriteString(", '?'))::int")
	})
	b.WriteString(" ELSE 1 END")
}

// renderPipelineBatch renders all PipelineOp SQL strings for a batch using at most a single strings.Builder.
// An error (wrapping ErrWireNotApplicable) means the batch cannot be rendered for the PostgreSQL flavor:
// forwarded values need the $N dialect (set_config / current_setting).
func renderPipelineBatch(dialect Dialect, cache *StmtCache, ops []PipelineOp) error {
	if len(ops) == 0 {
		return nil
	}
	type opSpan struct {
		start  int
		end    int
		render bool
	}
	var stackSpans [32]opSpan
	var spans []opSpan
	if len(ops) <= len(stackSpans) {
		spans = stackSpans[:len(ops)]
	} else {
		spans = make([]opSpan, len(ops))
	}

	totalEst := 0
	for i := range ops {
		op := &ops[i]
		dropCondArgs(ops[i : i+1]) // a batch rendered again (retried) binds them once
		if (op.nRefs > 0 || len(op.Decorators.ValRecs) > 0) && dialect != DialectDollar {
			return fmt.Errorf("%s: forwarded values need the $N dialect on this flavor: %w", opLabel(op), ErrWireNotApplicable)
		}
		plain := !op.Decorators.IsAbort &&
			!op.Decorators.guarded() &&
			len(op.Input.BranchGates) == 0 &&
			len(op.Input.ColPreds) == 0 &&
			op.nRefs == 0
		if plain && op.Desc != nil && op.Desc.StaticSQL != "" && op.Input.ColMask == 0 && op.Input.FlagMask == 0 {
			op.SQL = op.Desc.StaticSQL
			continue
		}
		if plain && cache != nil && op.Key.IsExact() {
			if cachedSQL, ok := cache.LookupSQL(op.Key); ok {
				op.SQL = cachedSQL
				continue
			}
		}
		spans[i].render = true
		totalEst += estimateWireOpBytes(op)
	}

	if totalEst == 0 {
		return nil
	}

	var b strings.Builder
	b.Grow(totalEst)

	var extra []any
	var checks []int // per op with forwarded values: start, end of its CheckSQL in the master string
	for i := range ops {
		if !spans[i].render {
			continue
		}
		op := &ops[i]
		var refs *refSlots
		if op.nRefs > 0 {
			refs = newRefSlots(op.Args)
			checks = append(checks, i, b.Len())
			writeCheckSQL(&b, op)
			checks = append(checks, b.Len())
		}
		spans[i].start = b.Len()
		extra = extra[:0]
		own := len(op.Args) - op.nRefs
		at := streamOpSQL(&b, dialect, op.Desc, &op.Input, &op.Decorators, &extra, own, refs)
		if len(extra) > 0 || refs != nil {
			// never into the pooled argument buffer
			args := make([]any, 0, own)
			for _, a := range op.Args {
				if _, isRef := a.(*argRef); !isRef {
					args = append(args, a)
				}
			}
			op.origArgs, op.argsRewritten = op.Args, true
			op.Args = spliceColArgs(dialect, args, extra, at)
		}
		op.overflowGuard = dialect != DialectDollar && (op.Decorators.IsAbort || op.Decorators.AssertRows >= 0)
		spans[i].end = b.Len()
	}

	master := b.String()
	for i := range ops {
		if spans[i].render {
			ops[i].SQL = master[spans[i].start:spans[i].end]
		}
	}
	for k := 0; k < len(checks); k += 3 {
		ops[checks[k]].CheckSQL = master[checks[k+1]:checks[k+2]]
	}
	return nil
}
