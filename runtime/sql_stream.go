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

// Markers of the errors a guard raises on purpose. A guard must fail the statement with an error that
// cannot be mistaken for anything else the statement may raise: an operation of the caller can itself
// fail with a genuine integer overflow, which has to surface unchanged. The guard therefore casts a
// text that carries the marker to an integer; PostgreSQL and CockroachDB answer with
// `invalid input syntax for type integer: "sqlgen:assert:3"` (22P02), which names the marker and, for an
// assertion, the number of rows the operation actually produced. TranslateWireError matches the marker.
const (
	wireAssertMarker = "sqlgen:assert:"
	wireAbortMarker  = "sqlgen:abort:"
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
		return 80 + len(c.SQLPred) + gateExtraBytes(numGates)
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
		b.WriteString("COUNT(*) > 0 AND MIN(CASE WHEN (")
		b.WriteString(c.SQLPred)
		b.WriteString(") THEN 1 ELSE 0 END) = 1")
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
			total += 480 + dec.ProduceCond.estimateBytes(numGates)
			for i := range dec.ExtraConds {
				total += 480 + dec.ExtraConds[i].Cond.estimateBytes(numGates)
			}
		}
		for i := range dec.LeafRecs {
			total += 480 + dec.LeafRecs[i].Cond.estimateBytes(numGates)
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
			writeGuardRaise(b, dialect, wireAbortMarker, "(SELECT COUNT(*))")
			b.WriteString(" ELSE 1 END")
		} else {
			b.WriteString("SELECT ")
			writeGuardRaise(b, dialect, wireAbortMarker, "(SELECT COUNT(*))")
		}
		return
	}

	hasAssert := dec != nil && dec.AssertRows >= 0
	hasCond := dec != nil && dec.ProduceCond != nil
	var leafRecs []ProducedCond
	if dec != nil {
		leafRecs = dec.LeafRecs
	}
	if !hasAssert && !hasCond && len(leafRecs) == 0 {
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
		return
	}
	// Every guard is read by a scalar subquery in the target list, which a statement can never drop:
	// an unreferenced CTE (here an assertion whose only trace would be an overflow error, or a
	// set_config) is pruned by PostgreSQL. The first column is the affected-row count.
	b.WriteString(" SELECT (SELECT COUNT(*) FROM _sqlgen_op) AS _cnt")
	guards(writeGuardRead)
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
			!op.Decorators.guarded() &&
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
		op.overflowGuard = dialect != DialectDollar && (op.Decorators.IsAbort || op.Decorators.AssertRows >= 0)
		spans[i].end = b.Len()
	}

	master := b.String()
	for i := range ops {
		if spans[i].render {
			ops[i].SQL = master[spans[i].start:spans[i].end]
		}
	}
}
