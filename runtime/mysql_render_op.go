package runtime

import (
	"errors"
	"fmt"
	"strconv"
)

func opLabel(op *PipelineOp) string {
	if n := op.stmtName(); n != "" {
		return n
	}
	return "pipeline statement"
}

// cursor returns the (reused) placeholder cursor, reset for one statement of operation i. At most one
// statement is being written at a time, so one cursor serves the whole batch.
func (r *mysqlBatchRender) cursor(i int, failGate bool) *streamCursor {
	r.st = mysqlArgs{fl: r.o.Flavor, args: r.args[i]}
	r.my = mysqlCursor{st: &r.st, failGate: failGate}
	r.cur = streamCursor{dialect: DialectQuestion, paramIdx: 1, my: &r.my}
	return &r.cur
}

// writeOwnGates writes the gates of an operation's enclosing If branches alone (1 when it has none).
func (r *mysqlBatchRender) writeOwnGates(gates []BranchGate) {
	if len(gates) == 0 {
		r.b.WriteByte('1')
		return
	}
	m := mysqlCursor{}
	m.writeGates(&r.b, gates, r.numBuf[:0])
}

// writeFailSet writes the statement that records operation i as the first failed guard when it is not
// already failed, its own gates are open and, for an assertion, the recorded count differs from want.
func (r *mysqlBatchRender) writeFailSet(i int, gates []BranchGate, want *int64) {
	r.b.WriteString("SET @sqlgen_f=IF(@sqlgen_f<0 AND (")
	r.writeOwnGates(gates)
	r.b.WriteByte(')')
	if want != nil {
		r.b.WriteString(" AND NOT (@sqlgen_n")
		r.num(i)
		r.b.WriteString("<=>")
		r.b.Write(strconv.AppendInt(r.numBuf[:0], *want, 10))
		r.b.WriteByte(')')
	}
	r.b.WriteByte(',')
	r.num(i)
	r.b.WriteString(",@sqlgen_f);")
}

func (r *mysqlBatchRender) renderOp(i int, op *PipelineOp, failGate bool) error {
	dec := &op.Decorators
	r.b.WriteString("SET @sqlgen_p=")
	r.num(i)
	r.b.WriteByte(';')
	if dec.IsAbort {
		r.writeFailSet(i, op.Input.BranchGates, nil)
		return nil
	}
	desc := op.Desc
	if desc == nil {
		return errors.New("operation without a statement descriptor")
	}
	hasRes := op.HasResultCols
	isSelect := desc.StmtVerb == VerbSelect
	if hasRes && !isSelect && !r.o.Flavor.Returning {
		return fmt.Errorf("a statement with RETURNING has no equivalent on this server: %w", ErrWireNotApplicable)
	}
	guarded := dec.guarded()
	counter := hasRes && !isSelect && guarded
	if counter {
		for _, pc := range r.condsOf(op) {
			if pc.Cond.countColPred() > 0 {
				return fmt.Errorf("a column condition over a RETURNING statement would run it twice: %w", ErrWireNotApplicable)
			}
		}
	}
	if r.o.InTx && dec.AssertRows >= 0 {
		r.b.WriteString("SAVEPOINT ")
		r.b.WriteString(mysqlSavepointName(i))
		r.b.WriteByte(';')
		r.batch.flags[i] |= opSavepoint
	}
	if op.nRefs > 0 {
		if err := r.writeRefChecks(i, op); err != nil {
			return err
		}
	}
	if counter {
		r.b.WriteString("SET @sqlgen_k=0;")
		r.batch.flags[i] |= opTrim
	}
	if hasRes {
		r.batch.sets = append(r.batch.sets, i)
	}
	// Forwarded values this operation produces: a row value is assigned while the row is returned (an
	// extra, trimmed column), the generated id is read after the statement. Every variable is reset first.
	var rowRec *ValRec
	for k := range dec.ValRecs {
		vr := &dec.ValRecs[k]
		if vr.LastInsertID {
			r.b.WriteString("DO LAST_INSERT_ID(0);")
			continue
		}
		if !hasRes || rowRec != nil || vr.Col == "" {
			return fmt.Errorf("a forwarded value needs a statement with one result column: %w", ErrWireNotApplicable)
		}
		rowRec = vr
		r.b.WriteString("SET @sqlgen_v")
		r.num(vr.ID)
		r.b.WriteString("=NULL;")
		r.batch.flags[i] |= opTrimVal
	}

	cur := r.cursor(i, failGate)
	cur.my.keepResultSet = hasRes && desc.StmtVerb == VerbDelete
	if rowRec != nil && isSelect {
		// The assignment runs over the rows actually returned: a derived table fixes the evaluation order
		// relative to ORDER BY / LIMIT (unspecified for a user variable in the select list otherwise).
		r.b.WriteString("SELECT _sqlgen_op.*,(@sqlgen_v")
		r.num(rowRec.ID)
		r.b.WriteString(":=_sqlgen_op.")
		r.b.WriteString(rowRec.Col)
		r.b.WriteString(") FROM (")
	}
	streamCoreStatement(&r.b, cur, desc, &op.Input)
	if counter {
		r.b.WriteString(", (@sqlgen_k:=@sqlgen_k+1)")
	}
	if rowRec != nil {
		if isSelect {
			r.b.WriteString(") AS _sqlgen_op")
		} else {
			// MariaDB INSERT / DELETE ... RETURNING: one more item of the RETURNING list.
			r.b.WriteString(", (@sqlgen_v")
			r.num(rowRec.ID)
			r.b.WriteString(":=")
			r.b.WriteString(rowRec.Expr)
			r.b.WriteByte(')')
		}
	}
	if cur.my.st.err != nil {
		return cur.my.st.err
	}
	r.b.WriteByte(';')

	if op.mysqlNeedsCount() {
		r.b.WriteString("SET @sqlgen_n")
		r.num(i)
		switch {
		case counter:
			r.b.WriteString("=@sqlgen_k;")
		case hasRes:
			r.b.WriteString("=FOUND_ROWS();")
		default:
			r.b.WriteString("=ROW_COUNT();")
		}
		if !hasRes || dec.AssertRows >= 0 || len(dec.ValRecs) > 0 {
			r.batch.sum = append(r.batch.sum, sumCol{kind: sumCount, op: i})
		}
	}
	for k := range dec.ValRecs {
		if vr := &dec.ValRecs[k]; vr.LastInsertID {
			r.b.WriteString("SET @sqlgen_v")
			r.num(vr.ID)
			r.b.WriteString("=LAST_INSERT_ID();")
		}
	}
	if dec.AssertRows >= 0 {
		r.writeFailSet(i, op.Input.BranchGates, &dec.AssertRows)
	}
	if dec.ProduceCond != nil {
		if err := r.writeRecord('c', dec.CondID, dec.ProduceCond, i, op); err != nil {
			return err
		}
		r.batch.sum = append(r.batch.sum, sumCol{kind: sumCond, op: i})
		for j := range dec.ExtraConds {
			ec := &dec.ExtraConds[j]
			if err := r.writeRecord('c', ec.CondID, ec.Cond, i, op); err != nil {
				return err
			}
			r.batch.sum = append(r.batch.sum, sumCol{kind: sumExtra, op: i, extra: j})
		}
	}
	for j := range dec.LeafRecs {
		lr := &dec.LeafRecs[j]
		if err := r.writeRecord('l', lr.CondID, lr.Cond, i, op); err != nil {
			return err
		}
	}
	return nil
}

// writeRefChecks writes, for consumer i, one check per forwarded value it uses (in argument order): when
// its gates are open, no earlier guard failed and the value is unresolved, it records the value's ID in
// @sqlgen_r and len(ops)+i in @sqlgen_f, which gates the consumer and everything after it off (nothing of
// the consumer ran, so no savepoint is needed); MySQLBatch.Apply builds the *ArgError.
func (r *mysqlBatchRender) writeRefChecks(i int, op *PipelineOp) error {
	var err error
	op.forEachRef(func(ref *argRef) {
		j := opIndex(r.ops, ref.op)
		if j < 0 || j >= i {
			if err == nil {
				err = fmt.Errorf("a forwarded value's producer is not an earlier operation of the batch: %w", ErrWireNotApplicable)
			}
			return
		}
		for _, v := range [2]byte{'r', 'f'} {
			r.b.WriteString("SET @sqlgen_")
			r.b.WriteByte(v)
			r.b.WriteString("=IF(@sqlgen_f<0 AND (")
			r.writeOwnGates(op.Input.BranchGates)
			r.b.WriteString(") AND NOT COALESCE(@sqlgen_n")
			r.num(j)
			r.b.WriteString("=1")
			if ref.lastID {
				r.b.WriteString(" AND @sqlgen_v")
				r.num(ref.id)
				r.b.WriteString("<>0")
			}
			r.b.WriteString(",0),")
			if v == 'r' {
				r.num(ref.id)
			} else {
				r.num(len(r.ops) + i)
			}
			r.b.WriteString(",@sqlgen_")
			r.b.WriteByte(v)
			r.b.WriteString(");")
		}
	})
	return err
}

// condsOf lists every condition an operation records.
func (r *mysqlBatchRender) condsOf(op *PipelineOp) []ProducedCond {
	var out []ProducedCond
	if op.Decorators.ProduceCond != nil {
		out = append(out, ProducedCond{CondID: op.Decorators.CondID, Cond: op.Decorators.ProduceCond})
	}
	out = append(out, op.Decorators.ExtraConds...)
	out = append(out, op.Decorators.LeafRecs...)
	return out
}

func (c *PipelineCond) countColPred() int {
	if c == nil {
		return 0
	}
	switch c.Kind {
	case CondColPred:
		if c.Recorded == 0 && c.Op != nil {
			return 1
		}
		return 0
	case CondAnd, CondOr:
		return c.Left.countColPred() + c.Right.countColPred()
	case CondNot:
		return c.Left.countColPred()
	}
	return 0
}

// writeRecord writes `SET @sqlgen_<kind><id>=COALESCE(<cond>,0);`.
func (r *mysqlBatchRender) writeRecord(kind byte, id int, c *PipelineCond, i int, op *PipelineOp) error {
	r.b.WriteString("SET @sqlgen_")
	r.b.WriteByte(kind)
	r.num(id)
	r.b.WriteString("=COALESCE(")
	if err := r.writeCond(c, i, op); err != nil {
		return err
	}
	r.b.WriteString(",0);")
	return nil
}

// writeCond writes c as an expression over the session variables of the statements already run. A leaf
// over another operation reads the value that operation recorded; a leaf over this operation is 0 when
// the operation's own gates are closed (it did not run), as PipelineCond.EvalGoFn treats a skipped one.
func (r *mysqlBatchRender) writeCond(c *PipelineCond, i int, op *PipelineOp) error {
	if c == nil {
		r.b.WriteByte('1')
		return nil
	}
	switch c.Kind {
	case CondAnd, CondOr:
		r.b.WriteByte('(')
		if err := r.writeCond(c.Left, i, op); err != nil {
			return err
		}
		if c.Kind == CondAnd {
			r.b.WriteString(" AND ")
		} else {
			r.b.WriteString(" OR ")
		}
		if err := r.writeCond(c.Right, i, op); err != nil {
			return err
		}
		r.b.WriteByte(')')
	case CondNot:
		r.b.WriteString("NOT (")
		if err := r.writeCond(c.Left, i, op); err != nil {
			return err
		}
		r.b.WriteByte(')')
	default:
		return r.writeLeaf(c, i, op)
	}
	return nil
}

func (r *mysqlBatchRender) writeLeaf(c *PipelineCond, i int, op *PipelineOp) error {
	if c.Recorded != 0 {
		r.b.WriteString("COALESCE(@sqlgen_l")
		r.num(c.Recorded)
		r.b.WriteString(",0)")
		return nil
	}
	if c.Op == nil {
		r.b.WriteByte('0')
		return nil
	}
	gated := len(op.Input.BranchGates) > 0
	if gated {
		r.b.WriteString("IF(")
		r.writeOwnGates(op.Input.BranchGates)
		r.b.WriteByte(',')
	}
	switch c.Kind {
	case CondRowsEq, CondRowsGt, CondRowsGe, CondRowsLt, CondRowsLe:
		r.b.WriteString("@sqlgen_n")
		r.num(i)
		switch c.Kind {
		case CondRowsEq:
			r.b.WriteByte('=')
		case CondRowsGt:
			r.b.WriteByte('>')
		case CondRowsGe:
			r.b.WriteString(">=")
		case CondRowsLt:
			r.b.WriteByte('<')
		case CondRowsLe:
			r.b.WriteString("<=")
		}
		r.b.Write(strconv.AppendInt(r.numBuf[:0], c.Threshold, 10))
	case CondColPred:
		if op.Desc == nil || op.Desc.StmtVerb != VerbSelect {
			return fmt.Errorf("a column condition over a statement that is not a SELECT cannot run it twice: %w", ErrWireNotApplicable)
		}
		if c.Col == 0 {
			r.b.WriteByte('0')
			break
		}
		// MySQL cannot aggregate over the rows a statement just returned to the client, so the select runs
		// a second time, inside the aggregate (same transaction, same arguments), with the same WhereCol
		// columns (their arguments inlined by the rules of the operation's own, MySQLFlavor.WriteLiteral).
		r.b.WriteString("(SELECT COALESCE(COUNT(*)>0 AND MIN(_sqlgen_wc")
		r.num(c.Col)
		r.b.WriteString(")=1,0) FROM (")
		in := op.Input
		in.BranchGates = nil
		cur := r.cursor(i, false)
		streamCoreStatement(&r.b, cur, op.Desc, &in)
		if cur.my.st.err != nil {
			return cur.my.st.err
		}
		r.b.WriteString(") AS _sqlgen_op)")
	}
	if gated {
		r.b.WriteString(",0)")
	}
	return nil
}

// dummyGates pads a RenderInput so that the shared descriptor estimator counts one more gate.
var dummyGates [65]BranchGate

// estimateMySQLOpBytes bounds the bytes RenderMySQLBatch writes for op (args are its normalized
// arguments), summary included.
func estimateMySQLOpBytes(fl *MySQLFlavor, op *PipelineOp, args []any) int {
	in := op.Input
	numGates := len(in.BranchGates)
	// The failure gate counts as one more gate.
	if numGates+1 < len(dummyGates) {
		in.BranchGates = dummyGates[:numGates+1]
	} else {
		in.BranchGates = make([]BranchGate, numGates+1)
	}
	g := 56 * (numGates + 1)
	dec := &op.Decorators
	total := 160 + 20 // SET @sqlgen_p, SAVEPOINT, counter, summary column
	if dec.IsAbort {
		return total + 120 + g
	}
	lits := 0
	for _, a := range args {
		lits += fl.literalBound(a)
	}
	var core int
	if op.Desc != nil {
		cur := estCursor{}
		core = estimateDescriptorBytes(op.Desc, op.Desc, &in, &cur) + lits
		if op.Desc.StmtVerb == VerbSelect {
			core += 48 + g
		}
		// gated multi-row INSERT: every row is a SELECT ... FROM DUAL WHERE <gates>
		rows := 0
		for _, n := range in.TupleLens {
			rows += n
		}
		core += rows * (40 + g)
		core += 64 // RETURNING counter
	}
	total += core + 160 + g
	add := func(c *PipelineCond) {
		total += 480 + c.estimateBytes(numGates+1)
		if k := c.countColPred(); k > 0 {
			total += k * (core + 160)
		}
	}
	if dec.ProduceCond != nil {
		add(dec.ProduceCond)
		for i := range dec.ExtraConds {
			add(dec.ExtraConds[i].Cond)
		}
	}
	for i := range dec.LeafRecs {
		add(dec.LeafRecs[i].Cond)
	}
	// the WhereCol columns' literals, in the statement and in every copy of it a condition runs
	if n := colPredLitBound(fl, in.ColPreds); n > 0 {
		total += n * (1 + nColPredCopies(dec))
	}
	for i := range dec.ValRecs {
		total += 160 + len(dec.ValRecs[i].Col) + len(dec.ValRecs[i].Expr)
	}
	// per forwarded value of a consumer: the two checks (writeRefChecks)
	total += op.nRefs * (2*(120+g) + 64)
	return total
}

// colPredLitBound bounds the bytes of the literals of every WhereCol argument of preds.
func colPredLitBound(fl *MySQLFlavor, preds []ColPred) int {
	n := 0
	for i := range preds {
		if lits, err := fl.normalize(preds[i].Args); err == nil {
			for _, a := range lits {
				n += fl.literalBound(a)
			}
		}
	}
	return n
}

// nColPredCopies counts the copies of the statement the conditions recorded by an operation run.
func nColPredCopies(dec *OpDecorators) int {
	n := 0
	if dec.ProduceCond != nil {
		n += dec.ProduceCond.countColPred()
		for i := range dec.ExtraConds {
			n += dec.ExtraConds[i].Cond.countColPred()
		}
	}
	for i := range dec.LeafRecs {
		n += dec.LeafRecs[i].Cond.countColPred()
	}
	return n
}
