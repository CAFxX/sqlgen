package runtime

import (
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
}

func (cur *streamCursor) writePlaceholder(b *strings.Builder) {
	if cur.dialect == DialectQuestion {
		b.WriteByte('?')
	} else {
		b.WriteByte('$')
		b.Write(strconv.AppendInt(cur.numBuf[:0], int64(cur.paramIdx), 10))
	}
	cur.paramIdx++
}

func formatOrderSQL(b *strings.Builder, orderCols []string, orderSpec uint64) {
	cols, desc, count, _ := ExpandOrder(orderSpec, len(orderCols))
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
	cols, desc, count, allSameDir := ExpandOrder(orderSpec, len(orderCols))
	writeParam := func(idx int) {
		if cur.dialect == DialectQuestion {
			b.WriteByte('?')
		} else {
			b.WriteByte('$')
			b.Write(strconv.AppendInt(cur.numBuf[:0], int64(idx), 10))
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

func (cur *streamCursor) writeBranchGates(b *strings.Builder, gates []BranchGate) {
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

// writeOverflowExpr writes an expression that raises an integer-overflow error (SQLSTATE 22003 /
// "bigint out of range") when, and only when, it is evaluated.
//
// DialectDollar must not use a constant expression: PostgreSQL constant-folds it at plan time, even
// inside the unreached branch of a CASE, which made every guarded statement fail unconditionally
// (and "-9223372036854775808::bigint" parses as -(9223372036854775808::bigint), whose inner cast
// already overflows). The aggregate in the scalar subquery keeps the planner from folding it, so it
// is only evaluated when the CASE branch is actually taken.
func writeOverflowExpr(b *strings.Builder, dialect Dialect) {
	if dialect == DialectDollar {
		b.WriteString("(9223372036854775807::bigint + (SELECT COUNT(*) + 1))")
	} else {
		b.WriteString("ABS(-9223372036854775808)")
	}
}

func (c *PipelineCond) estimateBytes() int {
	if c == nil {
		return 8
	}
	switch c.Kind {
	case CondRowsEq, CondRowsGt, CondRowsGe, CondRowsLt, CondRowsLe:
		return 36
	case CondColPred:
		return 64 + len(c.SQLPred)
	case CondAnd:
		return 9 + c.Left.estimateBytes() + c.Right.estimateBytes()
	case CondOr:
		return 8 + c.Left.estimateBytes() + c.Right.estimateBytes()
	case CondNot:
		return 6 + c.Left.estimateBytes()
	default:
		return 16
	}
}

func (c *PipelineCond) writeSQL(b *strings.Builder, cur *streamCursor) {
	if c == nil {
		b.WriteString("TRUE")
		return
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
		b.WriteString("COUNT(*) > 0 AND MIN(CASE WHEN (")
		b.WriteString(c.SQLPred)
		b.WriteString(") THEN 1 ELSE 0 END) = 1")
	case CondAnd:
		b.WriteByte('(')
		c.Left.writeSQL(b, cur)
		b.WriteString(" AND ")
		c.Right.writeSQL(b, cur)
		b.WriteByte(')')
	case CondOr:
		b.WriteByte('(')
		c.Left.writeSQL(b, cur)
		b.WriteString(" OR ")
		c.Right.writeSQL(b, cur)
		b.WriteByte(')')
	case CondNot:
		b.WriteString("NOT (")
		c.Left.writeSQL(b, cur)
		b.WriteByte(')')
	default:
		b.WriteString("TRUE")
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
				total += EstimateOpBytes(sub.Desc, &sub.Input, nil)
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
		}
	}
	return total
}

// EstimateOpBytes computes a provable mathematical upper bound on the byte length
// of the SQL string rendered for (desc, in, dec).
func EstimateOpBytes(desc *QueryDescriptor, in *RenderInput, dec *OpDecorators) int {
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
			total += 480 + dec.ProduceCond.estimateBytes()
			for i := range dec.ExtraConds {
				total += 480 + dec.ExtraConds[i].Cond.estimateBytes()
			}
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
			cur.writePlaceholder(b)
			if cur.castVals && slot.Cast != "" {
				b.WriteString("::")
				b.WriteString(slot.Cast)
			}

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
			if td != nil {
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
			if len(in.BranchGates) > 0 {
				cur.castVals = cur.dialect == DialectDollar
				b.WriteString("SELECT ")
			} else {
				b.WriteString("VALUES (")
			}

		case SlotInsertValuesClose:
			cur.castVals = false
			if len(in.BranchGates) > 0 {
				b.WriteString(" FROM (SELECT 1) AS _sqlgen_dual WHERE ")
				cur.writeBranchGates(b, in.BranchGates)
			} else {
				b.WriteByte(')')
			}

		case SlotInsertTupleValuesOpen:
			if len(in.BranchGates) > 0 {
				cur.castVals = cur.dialect == DialectDollar
				b.WriteString("WITH _sqlgen_v AS (VALUES ")
			} else {
				b.WriteString("VALUES ")
			}

		case SlotInsertTupleValuesClose:
			cur.castVals = false
			if len(in.BranchGates) > 0 {
				b.WriteString(") SELECT * FROM _sqlgen_v WHERE ")
				cur.writeBranchGates(b, in.BranchGates)
			}

		case SlotBranchGuardOpen:
			if len(in.BranchGates) > 0 {
				b.WriteByte('(')
				cur.writeBranchGates(b, in.BranchGates)
				b.WriteString(") AND (")
			}

		case SlotBranchGuardClose:
			if len(in.BranchGates) > 0 {
				b.WriteByte(')')
			}

		case SlotBranchGuardWhere:
			if len(in.BranchGates) > 0 {
				b.WriteString(" WHERE ")
				cur.writeBranchGates(b, in.BranchGates)
			}
		}
	}
	if len(desc.Segments) > len(desc.Slots) {
		b.WriteString(desc.Segments[len(desc.Slots)])
	}
}

func streamCoreStatement(b *strings.Builder, cur *streamCursor, desc *QueryDescriptor, in *RenderInput) {
	if desc == nil {
		return
	}
	hasGates := len(in.BranchGates) > 0
	if desc.StmtVerb == VerbSelect && hasGates {
		b.WriteString("SELECT * FROM (")
	}
	streamDescriptorSQL(b, desc, desc, in, cur)
	if desc.StmtVerb == VerbSelect && hasGates {
		b.WriteString(") AS _sqlgen_if WHERE ")
		cur.writeBranchGates(b, in.BranchGates)
	}
}

func streamOpSQL(b *strings.Builder, dialect Dialect, desc *QueryDescriptor, in *RenderInput, dec *OpDecorators) {
	var emptyIn RenderInput
	if in == nil {
		in = &emptyIn
	}
	cur := streamCursor{
		dialect:  dialect,
		paramIdx: 1,
	}
	hasGates := len(in.BranchGates) > 0
	if dec != nil && dec.IsAbort {
		if hasGates {
			b.WriteString("SELECT CASE WHEN (")
			cur.writeBranchGates(b, in.BranchGates)
			b.WriteString(") THEN ")
			writeOverflowExpr(b, dialect)
			b.WriteString(" ELSE 1 END")
		} else {
			b.WriteString("SELECT ")
			writeOverflowExpr(b, dialect)
		}
		return
	}

	hasAssert := dec != nil && dec.AssertRows >= 0
	hasCond := dec != nil && dec.ProduceCond != nil
	if !hasAssert && !hasCond {
		streamCoreStatement(b, &cur, desc, in)
		return
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
		writeOverflowExpr(b, dialect)
		b.WriteString(" END AS _ok FROM _sqlgen_op)")
	}

	if hasCond {
		b.WriteString(", _sqlgen_cond AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.c")
		b.Write(strconv.AppendInt(cur.numBuf[:0], int64(dec.CondID), 10))
		b.WriteString("', CASE WHEN (")
		dec.ProduceCond.writeSQL(b, &cur)
		b.WriteString(") THEN 'true' ELSE 'false' END, true) = 'true' AS _ok FROM _sqlgen_op)")
		for i := range dec.ExtraConds {
			ec := &dec.ExtraConds[i]
			b.WriteString(", _sqlgen_x")
			b.Write(strconv.AppendInt(cur.numBuf[:0], int64(ec.CondID), 10))
			b.WriteString(" AS (SELECT COUNT(*) AS _cnt, set_config('sqlgen.c")
			b.Write(strconv.AppendInt(cur.numBuf[:0], int64(ec.CondID), 10))
			b.WriteString("', CASE WHEN (")
			ec.Cond.writeSQL(b, &cur)
			b.WriteString(") THEN 'true' ELSE 'false' END, true) = 'true' AS _ok FROM _sqlgen_op)")
		}
	}

	hasResultCols := desc != nil && (desc.StmtVerb == VerbSelect || desc.HasReturning)
	if hasResultCols {
		// _f carries the assertion / condition CTEs; they have side effects (an error, set_config) that
		// MUST run exactly when the batch runs, even if _sqlgen_op yields no rows. A plain join against
		// _f let PostgreSQL prune its unused columns and never evaluate the CTEs. Making _f the outer
		// side of a LATERAL join whose inner side references every _f column forces it to be evaluated
		// first, whatever the planner decides about join order or column pruning.
		b.WriteString(" SELECT _r.* FROM (SELECT 1 AS _o")
		if hasAssert {
			b.WriteString(", (SELECT _ok FROM _sqlgen_chk) AS _a")
		}
		if hasCond {
			b.WriteString(", (SELECT _ok FROM _sqlgen_cond) AS _c")
			for i := range dec.ExtraConds {
				id := strconv.AppendInt(cur.numBuf[:0], int64(dec.ExtraConds[i].CondID), 10)
				b.WriteString(", (SELECT _ok FROM _sqlgen_x")
				b.Write(id)
				b.WriteString(") AS _x")
				b.Write(id)
			}
		}
		b.WriteString(") AS _f CROSS JOIN LATERAL (SELECT * FROM _sqlgen_op WHERE _f._o IS NOT NULL")
		if hasAssert {
			b.WriteString(" AND _f._a IS NOT NULL")
		}
		if hasCond {
			b.WriteString(" AND _f._c IS NOT NULL")
			for i := range dec.ExtraConds {
				b.WriteString(" AND _f._x")
				b.Write(strconv.AppendInt(cur.numBuf[:0], int64(dec.ExtraConds[i].CondID), 10))
				b.WriteString(" IS NOT NULL")
			}
		}
		b.WriteString(") AS _r")
	} else {
		// Every guard is read by a scalar subquery in the target list, which a statement can never drop:
		// an unreferenced CTE (here an assertion whose only trace would be an overflow error, or a
		// set_config) is pruned by PostgreSQL. The first column is the affected-row count.
		b.WriteString(" SELECT (SELECT COUNT(*) FROM _sqlgen_op) AS _cnt")
		if hasAssert {
			b.WriteString(", (SELECT _ok FROM _sqlgen_chk) AS _a")
		}
		if hasCond {
			b.WriteString(", (SELECT _ok FROM _sqlgen_cond) AS _c")
			for i := range dec.ExtraConds {
				id := strconv.AppendInt(cur.numBuf[:0], int64(dec.ExtraConds[i].CondID), 10)
				b.WriteString(", (SELECT _ok FROM _sqlgen_x")
				b.Write(id)
				b.WriteString(") AS _x")
				b.Write(id)
			}
		}
	}
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
	if desc.StaticSQL != "" && len(in.BranchGates) == 0 && in.ColMask == 0 && in.FlagMask == 0 {
		if len(desc.Slots) == 0 || (dialect == DialectDollar) == strings.Contains(desc.StaticSQL, "$1") {
			return desc.StaticSQL
		}
	}
	est := EstimateOpBytes(desc, in, nil)
	var b strings.Builder
	b.Grow(est)
	streamOpSQL(&b, dialect, desc, in, nil)
	return b.String()
}

// RenderPipelineBatch renders all PipelineOp SQL strings for a batch using at most a single strings.Builder.
func RenderPipelineBatch(dialect Dialect, cache *StmtCache, ops []PipelineOp) {
	if len(ops) == 0 {
		return
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
		plain := !op.Decorators.IsAbort &&
			op.Decorators.AssertRows < 0 &&
			op.Decorators.ProduceCond == nil &&
			len(op.Input.BranchGates) == 0
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
		totalEst += EstimateOpBytes(op.Desc, &op.Input, &op.Decorators)
	}

	if totalEst == 0 {
		return
	}

	var b strings.Builder
	b.Grow(totalEst)

	for i := range ops {
		if !spans[i].render {
			continue
		}
		op := &ops[i]
		spans[i].start = b.Len()
		streamOpSQL(&b, dialect, op.Desc, &op.Input, &op.Decorators)
		spans[i].end = b.Len()
	}

	master := b.String()
	for i := range ops {
		if spans[i].render {
			ops[i].SQL = master[spans[i].start:spans[i].end]
		}
	}
}
