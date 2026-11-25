package runtime

import (
	"errors"
	"strconv"
	"strings"
)

// Value forwarding: a pipeline operation can use, as one of its arguments, a value an EARLIER operation of
// the same pipeline produces on the server (the id an INSERT ... RETURNING generated, a COUNT(*), a
// MySQL AUTO_INCREMENT id). The parameter opts in with the DSL type Arg[T]; only the statement's Pipeline
// method then takes a runtime.Arg[T], which is either a plain value (Val) or a forwarded one
// (RowFuture.Arg, ExecFuture.LastInsertIDArg).
//
// The consumer binds the producer's value if and only if the producer yielded exactly one row (or
// affected exactly one row and generated an id); otherwise the consumer fails with an *ArgError when it
// runs, and the pipeline fails like a failed assertion. The rule and every error are the same on the
// sequential and on the single-round-trip paths.

// Arg is a pipeline argument: a plain value (Val) or a value forwarded from an earlier operation of the
// same pipeline (RowFuture.Arg, ExecFuture.LastInsertIDArg). The zero Arg is the plain zero value of T.
type Arg[T any] struct {
	v   T
	ref *argRef
}

// Val returns a plain-value Arg.
func Val[T any](v T) Arg[T] { return Arg[T]{v: v} }

// Forwarded reports whether a carries a value forwarded from another operation.
func (a Arg[T]) Forwarded() bool { return a.ref != nil }

// Bind is used by generated code: it returns the plain value, or the marker of the forwarded value
// that the pipeline replaces with the producer's value when the operation runs.
func (a Arg[T]) Bind() any {
	if a.ref != nil {
		return a.ref
	}
	return a.v
}

// ErrArgUnresolved is matched (errors.Is) by every *ArgError.
var ErrArgUnresolved = errors.New("sqlgen: forwarded pipeline argument unresolved")

// ArgError is the error of an operation whose forwarded argument has no value: its producer did not
// yield exactly one row (RowFuture.Arg) or did not affect exactly one row with a generated id
// (ExecFuture.LastInsertIDArg). errors.Is(err, ErrArgUnresolved) is true for it. The fields name the
// consuming operation (as AssertionError does) and the producing one.
type ArgError struct {
	Statement string // the consuming statement; empty when unknown
	Op        int    // the consuming operation's queue position (see AssertionError.Op)
	Label     string // the consuming operation's Label
	// Producer, ProducerOp and ProducerLabel identify the operation whose value was forwarded.
	Producer      string
	ProducerOp    int
	ProducerLabel string
	// Got is the producer's row count (rows returned, or rows affected for LastInsertIDArg); GotKnown is
	// false when it could not be determined.
	Got      int64
	GotKnown bool
	// LastInsertID reports that the value was forwarded with LastInsertIDArg.
	LastInsertID bool
}

func (e *ArgError) Error() string {
	var b strings.Builder
	b.WriteString("sqlgen: forwarded argument unresolved on ")
	if e.Statement != "" {
		b.WriteString("statement " + strconv.Quote(e.Statement) + ", ")
	}
	b.WriteString("operation " + strconv.Itoa(e.Op))
	if e.Label != "" {
		b.WriteString(" (" + strconv.Quote(e.Label) + ")")
	}
	b.WriteString(": its producer, ")
	if e.Producer != "" {
		b.WriteString("statement " + strconv.Quote(e.Producer) + ", ")
	}
	b.WriteString("operation " + strconv.Itoa(e.ProducerOp))
	if e.ProducerLabel != "" {
		b.WriteString(" (" + strconv.Quote(e.ProducerLabel) + ")")
	}
	switch {
	case e.LastInsertID && e.GotKnown && e.Got == 1:
		b.WriteString(", affected 1 row but generated no id")
	case e.LastInsertID && e.GotKnown:
		b.WriteString(", affected " + strconv.FormatInt(e.Got, 10) + " rows (exactly 1 with a generated id is required)")
	case e.LastInsertID:
		b.WriteString(", did not affect exactly 1 row with a generated id")
	case e.GotKnown:
		b.WriteString(", returned " + strconv.FormatInt(e.Got, 10) + " rows (exactly 1 is required)")
	default:
		b.WriteString(", did not return exactly 1 row")
	}
	return b.String()
}

// Unwrap makes errors.Is(err, ErrArgUnresolved) true.
func (e *ArgError) Unwrap() error { return ErrArgUnresolved }

// ValRec asks an operation to record, server-side, the value it produces for forwarding (see
// OpDecorators.ValRecs). A wire driver of the PostgreSQL flavor never needs to look at it: the runtime
// renders it into the operation's SQL.
type ValRec struct {
	// ID names the record: sqlgen.v<ID> (PostgreSQL flavor), @sqlgen_v<ID> (MySQL flavor).
	ID int
	// Col is the producer's result column whose value is forwarded (RowFuture.Arg): its name in the
	// statement's result, and Expr the column's expression (what a MariaDB RETURNING list can reference).
	Col, Expr string
	// LastInsertID: the value is the AUTO_INCREMENT id the statement generated (ExecFuture.LastInsertIDArg).
	LastInsertID bool
}

type argRef struct {
	pl     *Pipeline
	op     *PipelineOp // the producer (the future's own operation)
	id     int         // ValRec.ID
	lastID bool
	// value returns the producer's value for the sequential path, and whether it has one (beyond the row
	// count rule, which resolve applies).
	value func() (any, bool)
}

// newRef registers a forwarded value of op.
func (p *Pipeline) newRef(op *PipelineOp, lastID bool, value func() (any, bool)) *argRef {
	p.valSeq++
	r := &argRef{pl: p, op: op, id: p.valSeq, lastID: lastID, value: value}
	rec := ValRec{ID: r.id, LastInsertID: lastID}
	if !lastID && op.Desc != nil && len(op.Desc.ResultCols) == 1 {
		rec.Col, rec.Expr = op.Desc.ResultCols[0], op.Desc.ResultExpr
	}
	op.Decorators.ValRecs = append(op.Decorators.ValRecs, rec)
	return r
}

// Arg returns the value of the operation's single result column, forwarded to a later operation of the
// same pipeline (a parameter declared :name:Arg[T]). The consumer receives it when this operation
// returned exactly one row; otherwise the consumer fails with an *ArgError (ErrArgUnresolved) when it
// runs. The statement must have exactly one output column (RETURNING id:int64, SELECT COUNT(*) AS n:int64).
func (f *RowFuture[T]) Arg() Arg[T] {
	if f == nil || f.pl == nil || f.op == nil {
		return Arg[T]{}
	}
	if f.ref == nil {
		op := f.op
		if op.Desc == nil || len(op.Desc.ResultCols) != 1 {
			n := 0
			if op.Desc != nil {
				n = len(op.Desc.ResultCols)
			}
			f.pl.SetBuildError(InvalidArgument("Arg: statement " + strconv.Quote(op.stmtName()) + " returns " +
				strconv.Itoa(n) + " columns; only the value of a statement with exactly one output column can be forwarded"))
		}
		op.countRows = true
		f.ref = f.pl.newRef(op, false, func() (any, bool) { return f.val, f.found })
	}
	return Arg[T]{ref: f.ref}
}

// LastInsertIDArg returns the AUTO_INCREMENT id the operation's INSERT generates (MySQL, MariaDB, TiDB,
// SQLite), forwarded to a later operation of the same pipeline (a parameter declared
// :name:Arg[int64]). The consumer receives it when the statement affected exactly one row and generated
// an id; otherwise the consumer fails with an *ArgError (ErrArgUnresolved) when it runs. The $N dialect
// (PostgreSQL, CockroachDB, YugabyteDB) has no generated-id result: use INSERT ... RETURNING and
// RowFuture.Arg there (queuing fails with ErrInvalidArgument).
func (f *ExecFuture) LastInsertIDArg() Arg[int64] {
	if f == nil || f.pl == nil || f.op == nil {
		return Arg[int64]{}
	}
	if f.ref == nil {
		if f.pl.dialect == DialectDollar {
			f.pl.SetBuildError(InvalidArgument("LastInsertIDArg: statement " + strconv.Quote(f.op.stmtName()) +
				": the $N dialect has no generated-id result; declare the id with INSERT ... RETURNING id:int64 (kind: one) and forward it with Arg()"))
		}
		f.ref = f.pl.newRef(f.op, true, func() (any, bool) {
			if f.res == nil {
				return nil, false
			}
			id, err := f.res.LastInsertId()
			if err != nil || id == 0 {
				return nil, false
			}
			return id, true
		})
	}
	return Arg[int64]{ref: f.ref}
}

// BindForwarded is used by generated code right after it queued an operation with Arg parameters: it
// checks the forwarded values the operation uses (same pipeline; the producer's If branch encloses the
// consumer, so a consumer can only run after its producer ran) and marks the operation.
func (p *Pipeline) BindForwarded() {
	if len(p.opPtrs) == 0 {
		return
	}
	op := p.opPtrs[len(p.opPtrs)-1]
	n := 0
	for _, a := range op.Args {
		r, ok := a.(*argRef)
		if !ok {
			continue
		}
		n++
		switch {
		case r.pl != p:
			p.SetBuildError(InvalidArgument("statement " + strconv.Quote(op.stmtName()) + " (operation " + strconv.Itoa(op.opIdx) +
				") uses a value forwarded from another pipeline (a future of an earlier Pipeline, Flush or PipelineTx attempt); forward values within one pipeline only"))
		case !gatePrefix(r.op.Input.BranchGates, op.Input.BranchGates):
			p.SetBuildError(InvalidArgument("statement " + strconv.Quote(op.stmtName()) + " (operation " + strconv.Itoa(op.opIdx) +
				") uses a value forwarded from statement " + strconv.Quote(r.op.stmtName()) + " (operation " + strconv.Itoa(r.op.opIdx) +
				") outside the If branch that operation was queued in; queue the consumer in the same branch as its producer or in one nested in it"))
		}
	}
	op.nRefs = n
}

// gatePrefix reports whether the producer's branch gates are a prefix of the consumer's: the consumer then
// runs only when the producer ran.
func gatePrefix(producer, consumer []BranchGate) bool {
	if len(producer) > len(consumer) {
		return false
	}
	for i := range producer {
		if producer[i].CondID != consumer[i].CondID || producer[i].WantTrue != consumer[i].WantTrue {
			return false
		}
	}
	return true
}

// argError builds the *ArgError of consumer op for the forwarded value r.
func (r *argRef) argError(op *PipelineOp, got int64, gotKnown bool) *ArgError {
	return &ArgError{
		Statement: op.stmtName(), Op: op.opIdx, Label: op.label,
		Producer: r.op.stmtName(), ProducerOp: r.op.opIdx, ProducerLabel: r.op.label,
		Got: got, GotKnown: gotKnown, LastInsertID: r.lastID,
	}
}

// resolveRefs replaces, for the sequential path, every forwarded value in op.Args with the producer's
// value, or returns the *ArgError of the first (in argument order) that has none.
func resolveRefs(op *PipelineOp) error {
	for i, a := range op.Args {
		r, ok := a.(*argRef)
		if !ok {
			continue
		}
		v, has := r.value()
		if r.op.Skipped || r.op.RowCount != 1 || !has {
			return r.argError(op, r.op.RowCount, !r.op.Skipped)
		}
		op.Args[i] = v
	}
	return nil
}

// refByID returns the forwarded value with ID id among op's arguments (the original ones, before a wire
// renderer removed them), or nil.
func (op *PipelineOp) refByID(id int) *argRef {
	args := op.Args
	if op.argsRewritten {
		args = op.origArgs
	}
	for _, a := range args {
		if r, ok := a.(*argRef); ok && r.id == id {
			return r
		}
	}
	return nil
}

// forEachRef calls fn once per distinct forwarded value of op, in argument order.
func (op *PipelineOp) forEachRef(fn func(r *argRef)) {
	args := op.Args
	if op.argsRewritten {
		args = op.origArgs
	}
	var seen []int
next:
	for _, a := range args {
		r, ok := a.(*argRef)
		if !ok {
			continue
		}
		for _, id := range seen {
			if id == r.id {
				continue next
			}
		}
		seen = append(seen, r.id)
		fn(r)
	}
}

// Forwards reports whether the operation uses a value forwarded from an earlier operation.
func (op *PipelineOp) Forwards() bool { return op.nRefs > 0 }
