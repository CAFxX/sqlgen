package runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// CondKind identifies the type of boolean predicate in a PipelineCond AST.
type CondKind uint8

const (
	CondRowsEq CondKind = iota
	CondRowsGt
	CondRowsGe
	CondRowsLt
	CondRowsLe
	CondColPred
	CondAnd
	CondOr
	CondNot
)

// PipelineCond represents a composable boolean condition over a pipelined operation's result.
// Constructing PipelineCond values never performs string formatting or heap string builder allocations.
type PipelineCond struct {
	Kind      CondKind
	Op        *PipelineOp
	Threshold int64
	// Col is the WhereCol predicate of a CondColPred leaf: the 1-based index into Op.Input.ColPreds,
	// whose value the operation's statement returns as its column _sqlgen_wc<Col> (0: none, false).
	Col   int
	Left  *PipelineCond
	Right *PipelineCond
	// Recorded is set only on the leaves of a condition bound by Pipeline.If whose operation is NOT the
	// operation that evaluates the condition (the latest one of a compound): it is the ID under which
	// that leaf's own operation recorded the leaf's value server-side (OpDecorators.LeafRecs). The
	// evaluating statement reads the recorded value instead of looking at its own rows.
	Recorded int
}

// And combines c and other with a logical AND.
func (c PipelineCond) And(other PipelineCond) PipelineCond {
	l := c
	r := other
	return PipelineCond{
		Kind:  CondAnd,
		Op:    latestOp(l.Op, r.Op),
		Left:  &l,
		Right: &r,
	}
}

// Or combines c and other with a logical OR.
func (c PipelineCond) Or(other PipelineCond) PipelineCond {
	l := c
	r := other
	return PipelineCond{
		Kind:  CondOr,
		Op:    latestOp(l.Op, r.Op),
		Left:  &l,
		Right: &r,
	}
}

// Not negates c.
func (c PipelineCond) Not() PipelineCond {
	l := c
	return PipelineCond{
		Kind: CondNot,
		Op:   l.Op,
		Left: &l,
	}
}

func latestOp(a, b *PipelineOp) *PipelineOp {
	if a == nil {
		return b
	}
	if b == nil || a.opIdx >= b.opIdx {
		return a
	}
	return b
}

// EvalGoFn evaluates the condition in Go for sequential fallback mode and post-scan wire mode.
func (c *PipelineCond) EvalGoFn() bool {
	if c == nil {
		return false
	}
	switch c.Kind {
	case CondRowsEq:
		return c.Op != nil && !c.Op.Skipped && c.Op.RowCount == c.Threshold
	case CondRowsGt:
		return c.Op != nil && !c.Op.Skipped && c.Op.RowCount > c.Threshold
	case CondRowsGe:
		return c.Op != nil && !c.Op.Skipped && c.Op.RowCount >= c.Threshold
	case CondRowsLt:
		return c.Op != nil && !c.Op.Skipped && c.Op.RowCount < c.Threshold
	case CondRowsLe:
		return c.Op != nil && !c.Op.Skipped && c.Op.RowCount <= c.Threshold
	case CondColPred:
		return c.Op != nil && !c.Op.Skipped && c.Op.colPredHolds(c.Col)
	case CondAnd:
		return c.Left.EvalGoFn() && c.Right.EvalGoFn()
	case CondOr:
		return c.Left.EvalGoFn() || c.Right.EvalGoFn()
	case CondNot:
		return !c.Left.EvalGoFn()
	default:
		return false
	}
}

// ProducedCond represents an additional boolean condition evaluated after a PipelineOp completes.
type ProducedCond struct {
	CondID int
	Cond   *PipelineCond
	// Val and Set are filled in by a wire driver that read the condition back from the server (as
	// PipelineOp.CondVal / CondSet are for the first condition). When Set is false the condition is
	// evaluated in Go after the results were scanned.
	Val, Set bool
}

// PipelineOp represents a single queued operation inside a Pipeline or PipelineTx.
type PipelineOp struct {
	opIdx int    // position in the pipeline's queue (AssertionError.Op)
	label string // set by the future's Label (AssertionError.Label)
	// argsRewritten is set when renderPipelineBatch replaced Args for the wire (WhereCol arguments appended,
	// forwarded values removed); origArgs are the operation's own arguments, restored by dropCondArgs.
	argsRewritten bool
	origArgs      []any
	// nRefs is the number of forwarded values among Args (BindForwarded); countRows is set on a producer
	// whose value is forwarded (RowFuture.Arg): the sequential path then counts every row it returns, so
	// that "more than one row" is detected as on the single-round-trip paths.
	nRefs      int
	countRows  bool
	StmtID     uint32
	Desc       *QueryDescriptor
	Input      RenderInput
	Decorators OpDecorators
	ExtraConds []ProducedCond
	Key        VariantKey
	FastSlot   *atomic.Pointer[ManagedStmt]
	SQL        string
	Args       []any
	// CheckSQL, set by renderPipelineBatch (PostgreSQL flavor) on an operation that uses forwarded values,
	// is a statement the driver must send right before SQL, inside the same batch: it fails with a marked
	// error (TranslateWireError makes an *ArgError of it) when the operation would run and a forwarded value
	// is unresolved. Inside a transaction the caller began, take the operation's savepoint before it.
	CheckSQL string
	// colFail[k] is set when a row the statement returned had a value other than 1 in its WhereCol column
	// _sqlgen_wc<k+1> (Input.ColPreds[k]); reset whenever the statement's rows are scanned again.
	colFail       []bool
	argBuf        *[]any
	HasResultCols bool
	IsSingleRow   bool
	// NoOp marks an operation that has nothing to send: a statement whose multi-row VALUES collection was
	// empty (see QueueExecNoop). It never reaches a wire driver; its result is zero rows, no error.
	NoOp         bool
	ExecSeq      func(ctx context.Context, p *Pipeline, op *PipelineOp) error
	ScanWireRows func(rows *sql.Rows) (int64, error)
	OnSkipped    func()
	OnDone       func(rowCount int64, err error)
	AbortErr     error
	RowCount     int64
	Skipped      bool
	CondVal      bool
	CondSet      bool
	Err          error
	// overflowGuard is set by renderPipelineBatch for an operation whose guard was rendered for the
	// question dialect (SQLite, which the example suites use to simulate a wire driver): that dialect
	// cannot carry a marker in an error message, its guard is a constant integer overflow, and
	// TranslateWireError recognises that overflow instead of a marker.
	overflowGuard bool
	// queueErr is set on an operation queued after its pipeline ran (ErrPipelineRan): it never runs and
	// its future reports this error.
	queueErr error
}

// ExecFuture represents the pending result of an exec statement queued in a Pipeline.
type ExecFuture struct {
	pl      *Pipeline
	op      *PipelineOp
	res     sql.Result
	skipped bool
	err     error
	ref     *argRef // LastInsertIDArg
}

// AssertRows requires the operation to affect exactly n rows (rows affected for an exec, rows returned for a query), rolling back with ErrAssertionFailed otherwise.
func (f *ExecFuture) AssertRows(n int64) *ExecFuture {
	f.op.Decorators.AssertRows = n
	return f
}

// Label names the operation for error reports: a failed assertion on it returns an *AssertionError whose
// Label field (and message) carry label, next to the operation's queue position (AssertionError.Op).
func (f *ExecFuture) Label(label string) *ExecFuture {
	f.op.label = label
	return f
}

// AssertFound requires the operation to affect exactly 1 row.
func (f *ExecFuture) AssertFound() *ExecFuture {
	f.op.Decorators.AssertRows = 1
	return f
}

func (f *ExecFuture) RowsEq(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: n}
}

func (f *ExecFuture) RowsGt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: n}
}

func (f *ExecFuture) RowsGe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGe, Op: f.op, Threshold: n}
}

func (f *ExecFuture) RowsLt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLt, Op: f.op, Threshold: n}
}

func (f *ExecFuture) RowsLe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLe, Op: f.op, Threshold: n}
}

func (f *ExecFuture) Found() PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: 0}
}

func (f *ExecFuture) NotFound() PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: 0}
}

// futureErr is what a future reports instead of a result: ErrFutureNotReady before its pipeline has
// flushed and, when the flush failed, the operation's own error or else the pipeline's. A failed pipeline
// committed nothing, and the single-round-trip paths do not deliver the results of a failed batch, so no
// future of it reports a result (the same on every path).
//
// An operation queued after its pipeline ran reports its own *PipelineRanError (ErrPipelineRan) instead.
func futureErr(pl *Pipeline, op *PipelineOp, own error) error {
	switch {
	case pl == nil:
		return nil
	case op != nil && op.queueErr != nil:
		return op.queueErr
	case !pl.flushed:
		return ErrFutureNotReady
	case pl.failErr != nil && own != nil:
		return own
	}
	return pl.failErr
}

// Result returns the sql.Result of the operation after the pipeline has flushed (ErrFutureNotReady before).
func (f *ExecFuture) Result() (sql.Result, error) {
	if err := futureErr(f.pl, f.op, f.err); err != nil {
		return nil, err
	}
	if f.skipped {
		return nil, ErrBranchNotTaken
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.res != nil {
		return f.res, nil
	}
	return BatchExecResult{RowsAffectedVal: f.op.RowCount, LastInsertIDErr: ErrLastInsertIDNotAvailable}, nil
}

// ErrLastInsertIDNotAvailable is what LastInsertId of the sql.Result of an ExecFuture reports when the
// operation ran on the single-round-trip path: the wire drivers read the affected-row count of every
// operation, not the generated key. Use RETURNING (PostgreSQL, MariaDB), or read the key back with a
// SELECT, or run the statement outside a pipeline.
var ErrLastInsertIDNotAvailable = errors.New("sqlgen: LastInsertId is not available for a pipeline operation that ran on the single-round-trip path")

// RowsAffected returns the number of rows affected by the operation after the pipeline has flushed
// (ErrFutureNotReady before).
func (f *ExecFuture) RowsAffected() (int64, error) {
	if err := futureErr(f.pl, f.op, f.err); err != nil {
		return 0, err
	}
	if f.skipped {
		return 0, ErrBranchNotTaken
	}
	if f.err != nil {
		return 0, f.err
	}
	return f.op.RowCount, nil
}

// RowFuture represents the pending result of a single-row ("one") statement queued in a Pipeline.
type RowFuture[T any] struct {
	pl      *Pipeline
	op      *PipelineOp
	val     T
	found   bool
	skipped bool
	err     error
	ref     *argRef // Arg
}

// AssertFound requires the query to return 1 row, rolling back with ErrAssertionFailed otherwise.
func (f *RowFuture[T]) AssertFound() *RowFuture[T] {
	f.op.Decorators.AssertRows = 1
	return f
}

// AssertRows requires the query to return n rows (0 or 1).
func (f *RowFuture[T]) AssertRows(n int64) *RowFuture[T] {
	f.op.Decorators.AssertRows = n
	return f
}

// Label names the operation for error reports: a failed assertion on it returns an *AssertionError whose
// Label field (and message) carry label, next to the operation's queue position (AssertionError.Op).
func (f *RowFuture[T]) Label(label string) *RowFuture[T] {
	f.op.label = label
	return f
}

func (f *RowFuture[T]) RowsEq(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) RowsGt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) RowsGe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGe, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) RowsLt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLt, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) RowsLe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLe, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) Found() PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: 0}
}

func (f *RowFuture[T]) NotFound() PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: 0}
}

// WhereCol constructs a column-level PipelineCond: true when the operation returned its row and the
// predicate is TRUE for it (NULL counts as false). sqlPred is a boolean SQL expression evaluated by the
// server in the scope of the statement's output column list (as if it were one more output column: it can
// use the tables, columns and aggregates the list can, but not the list's own aliases), with one '?'
// placeholder (outside quotes) per value in args, which are bound, never written into the SQL text. The
// statement returns it as one more (hidden) column on every path, so the condition is decided by the server
// in the same round trip as the statement itself. The statement needs such a column list: a SELECT
// (neither DISTINCT nor a UNION / INTERSECT / EXCEPT) or a RETURNING clause; otherwise the pipeline fails
// with ErrInvalidArgument.
func (f *RowFuture[T]) WhereCol(sqlPred string, args ...any) PipelineCond {
	if f == nil {
		return PipelineCond{Kind: CondColPred}
	}
	return colPredCond(f.pl, f.op, sqlPred, args)
}

// colPredCond adds sqlPred as a WhereCol column of op and returns the CondColPred leaf that reads it. A
// placeholder count that does not match args, or a statement without a place for the column, fails the
// pipeline's build (ErrInvalidArgument from Flush / PipelineTx).
func colPredCond(pl *Pipeline, op *PipelineOp, sqlPred string, args []any) PipelineCond {
	n := 0
	forPredMarkers(sqlPred, func(int) { n++ })
	if n != len(args) && pl != nil {
		pl.SetBuildError(InvalidArgument("WhereCol: the predicate " + strconv.Quote(sqlPred) + " has " +
			strconv.Itoa(n) + " '?' placeholders but " + strconv.Itoa(len(args)) + " arguments were given"))
	}
	if op == nil || op.NoOp {
		// A NoOp sends no statement and returns no row: the condition is false, as for any empty result.
		return PipelineCond{Kind: CondColPred, Op: op}
	}
	if !op.Desc.hasColPredSlot() {
		if pl != nil {
			pl.SetBuildError(InvalidArgument("WhereCol: statement " + strconv.Quote(op.stmtName()) +
				" has no output column list a predicate column can be added to (a SELECT that is neither DISTINCT nor a UNION / INTERSECT / EXCEPT, or a RETURNING clause)"))
		}
		return PipelineCond{Kind: CondColPred, Op: op}
	}
	op.Input.ColPreds = append(op.Input.ColPreds, ColPred{SQL: sqlPred, Args: args})
	op.colFail = append(op.colFail, false)
	return PipelineCond{Kind: CondColPred, Op: op, Col: len(op.Input.ColPreds)}
}

// colPredHolds reports whether the WhereCol column col (1-based) of op was 1 in every row of a non-empty
// result.
func (op *PipelineOp) colPredHolds(col int) bool {
	return col > 0 && col <= len(op.colFail) && op.RowCount > 0 && !op.colFail[col-1]
}

// hasColPredSlot reports whether the statement has the place WhereCol columns are rendered at.
func (d *QueryDescriptor) hasColPredSlot() bool {
	if d == nil {
		return false
	}
	for i := range d.Slots {
		if d.Slots[i].Kind == SlotColPreds {
			return true
		}
	}
	return false
}

// forPredMarkers calls fn with the byte offset of every '?' placeholder of a WhereCol predicate: every '?'
// outside '...' string literals and "..." / `...` quoted identifiers (a doubled quote inside one closes
// and reopens it, which keeps the scan right).
func forPredMarkers(s string, fn func(i int)) {
	var q byte
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case q != 0:
			if c == q {
				q = 0
			}
		case c == '\'' || c == '"' || c == '`':
			q = c
		case c == '?':
			fn(i)
		}
	}
}

// writePred writes the predicate s, calling ph for its k-th placeholder instead of writing the '?'.
func writePred(b *strings.Builder, s string, ph func(k int)) {
	last, k := 0, 0
	forPredMarkers(s, func(i int) {
		b.WriteString(s[last:i])
		ph(k)
		k++
		last = i + 1
	})
	b.WriteString(s[last:])
}

// Get returns the scanned row after the pipeline has flushed (ErrFutureNotReady before, sql.ErrNoRows
// when the flushed statement matched no row).
func (f *RowFuture[T]) Get() (T, error) {
	var zero T
	if err := futureErr(f.pl, f.op, f.err); err != nil {
		return zero, err
	}
	if f.skipped {
		return zero, ErrBranchNotTaken
	}
	if f.err != nil {
		return zero, f.err
	}
	if !f.found {
		return zero, sql.ErrNoRows
	}
	return f.val, nil
}

// SliceFuture represents the pending result of a multi-row ("many") statement queued in a Pipeline.
type SliceFuture[T any] struct {
	pl      *Pipeline
	op      *PipelineOp
	vals    []T
	skipped bool
	err     error
}

// AssertFound requires the query to return 1 row.
func (f *SliceFuture[T]) AssertFound() *SliceFuture[T] {
	f.op.Decorators.AssertRows = 1
	return f
}

// AssertRows requires the query to return exactly n rows.
func (f *SliceFuture[T]) AssertRows(n int64) *SliceFuture[T] {
	f.op.Decorators.AssertRows = n
	return f
}

// Label names the operation for error reports: a failed assertion on it returns an *AssertionError whose
// Label field (and message) carry label, next to the operation's queue position (AssertionError.Op).
func (f *SliceFuture[T]) Label(label string) *SliceFuture[T] {
	f.op.label = label
	return f
}

func (f *SliceFuture[T]) RowsEq(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) RowsGt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) RowsGe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGe, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) RowsLt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLt, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) RowsLe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLe, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) Found() PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: 0}
}

func (f *SliceFuture[T]) NotFound() PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: 0}
}

// WhereCol constructs a column-level PipelineCond: true when the operation returned at least one row and
// the predicate is TRUE for every row (NULL counts as false); sqlPred and args are as for
// RowFuture.WhereCol.
func (f *SliceFuture[T]) WhereCol(sqlPred string, args ...any) PipelineCond {
	if f == nil {
		return PipelineCond{Kind: CondColPred}
	}
	return colPredCond(f.pl, f.op, sqlPred, args)
}

// Get returns the scanned slice of rows after the pipeline has flushed (ErrFutureNotReady before).
func (f *SliceFuture[T]) Get() ([]T, error) {
	if err := futureErr(f.pl, f.op, f.err); err != nil {
		return nil, err
	}
	if f.skipped {
		return nil, ErrBranchNotTaken
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.vals, nil
}

// ifResult records which arm of an If chain ran; executePipelineOps resolves it after a successful run.
type ifResult struct {
	base     []BranchGate   // the gates of the scope the chain was queued in
	arms     [][]BranchGate // each arm's gates: base, the earlier arms' conditions false, its own true
	taken    int            // the arm that ran (-1: none); valid once resolved
	skipped  bool
	resolved bool
}

// IfChain is an If block of a pipeline: If starts it with its first arm, ElseIf adds an arm, Else adds
// the final arm and ends the chain. At most one arm runs: the first whose condition holds, else the Else
// arm. Each arm's closure receives a handle scoped to that arm (see Pipeline). Once the pipeline ran,
// Taken reports which arm ran.
type IfChain[P any] struct {
	res    *ifResult
	p      *Pipeline // the handle If was called on
	handle func(*Pipeline) P
	negs   []BranchGate // the earlier arms' conditions, false
	end    int          // the queue length when the last arm's closure returned
	done   bool         // Else was added
}

// Taken reports whether arm i ran: 0 is the If arm, 1 the first ElseIf, and so on; the Else arm (if any)
// is the index after the last ElseIf. It is false for every arm before the pipeline ran successfully, and
// when the chain sits inside an enclosing arm that did not run (Skipped). An i that names no arm of the
// chain panics.
func (c *IfChain[P]) Taken(i int) bool {
	r := c.res
	if i < 0 || i >= len(r.arms) {
		panic("sqlgen: IfChain.Taken(" + strconv.Itoa(i) + "): the chain has " + strconv.Itoa(len(r.arms)) + " arms")
	}
	return r.resolved && r.taken == i
}

// Skipped reports whether the chain sat inside an enclosing If arm that did not run, so that none of its
// conditions was considered (false before the pipeline ran successfully).
func (c *IfChain[P]) Skipped() bool {
	return c.res.resolved && c.res.skipped
}

// If is used by generated code: it queues the first arm of an If chain on p, gated on cond, and runs fn
// with a handle scoped to that arm (handle wraps the runtime handle in the generated Pipeline type).
func If[P any](p *Pipeline, cond PipelineCond, fn func(P), handle func(*Pipeline) P) *IfChain[P] {
	c := &IfChain[P]{res: &ifResult{base: p.cloneGates(), taken: -1}, p: p, handle: handle}
	p.checkHandle("If")
	p.ifResults = append(p.ifResults, c.res)
	c.arm("If", &cond, fn)
	return c
}

// ElseIf adds an arm that runs when no earlier arm of the chain ran and cond holds. It must directly follow
// the chain's previous arm, on the same pipeline handle.
func (c *IfChain[P]) ElseIf(cond PipelineCond, fn func(P)) *IfChain[P] {
	if c.follows("ElseIf") {
		c.arm("ElseIf", &cond, fn)
	}
	return c
}

// Else adds the final arm, which runs when no other arm of the chain ran, and ends the chain: adding
// another arm afterwards fails the pipeline with ErrInvalidArgument.
func (c *IfChain[P]) Else(fn func(P)) *IfChain[P] {
	if c.follows("Else") {
		c.arm("Else", nil, fn)
		c.done = true
	}
	return c
}

// follows checks that an ElseIf / Else is added right after the chain's previous arm.
func (c *IfChain[P]) follows(what string) bool {
	p := c.p
	p.checkHandle(what)
	switch {
	case c.done:
		p.SetBuildError(InvalidArgument(what + " after the chain's Else"))
		return false
	case p.cur != p.scope || len(p.opPtrs) != c.end:
		p.SetBuildError(InvalidArgument(what + " must directly follow the previous arm of its If chain: nothing may be queued in between"))
		return false
	}
	return true
}

// arm queues one arm: its gates are the chain's scope, every earlier arm's condition false and (except
// for Else) its own condition true; fn runs with a handle scoped to the arm.
func (c *IfChain[P]) arm(what string, cond *PipelineCond, fn func(P)) {
	p := c.p
	n := len(p.branchStack)
	p.branchStack = append(p.branchStack, c.negs...)
	if cond != nil {
		cc := *cond
		id := p.produceCond(what, &cc)
		p.branchStack = append(p.branchStack, BranchGate{CondID: id, WantTrue: true, Cond: &cc})
		c.negs = append(c.negs, BranchGate{CondID: id, WantTrue: false, Cond: &cc})
	}
	c.res.arms = append(c.res.arms, p.cloneGates())
	if fn != nil {
		s := &branchScope{}
		prev := p.cur
		p.cur = s
		fn(c.handle(&Pipeline{pipelineState: p.pipelineState, scope: s}))
		s.closed = true
		p.cur = prev
	}
	p.branchStack = p.branchStack[:n]
	c.end = len(p.opPtrs)
}

// wirePipeliner and wireTxPipeliner are the method sets of the wire.Pipeliner and wire.TxPipeliner
// contracts (package runtime/wire documents them); the runtime finds a driver connection's
// implementation by structural type assertion, so it does not import that package.
type wirePipeliner interface {
	ExecPipeline(ctx context.Context, ops []PipelineOp) error
}

type wireTxPipeliner interface {
	ExecTxPipeline(ctx context.Context, txOpts *sql.TxOptions, ops []PipelineOp) error
}

// CommitError wraps an error returned by the COMMIT of an ExecTxPipeline. It unwraps to the driver error,
// so errors.As / errors.Is keep working; the runtime uses it to apply IsRetryableTxError's duringCommit
// rule and removes the wrapper before returning an error to the caller.
type CommitError struct{ Err error }

func (e *CommitError) Error() string { return e.Err.Error() }
func (e *CommitError) Unwrap() error { return e.Err }

// ErrFlushInPipelineTx is returned by Pipeline.Flush when called from inside a PipelineTx
// callback: the transaction flushes the pipeline itself once the callback returns.
var ErrFlushInPipelineTx = errors.New("sqlgen: Flush must not be called inside PipelineTx; the transaction flushes the pipeline itself")

// ErrWireNotApplicable is returned by a wire.Pipeliner or wire.TxPipeliner BEFORE it sent anything to the
// server, to say that it cannot run this particular batch in one round-trip (a batch larger than the
// server's packet limit, an argument it cannot represent, a transaction option it has no equivalent for,
// a server that failed the capability probe, ...). The runtime then runs the pipeline on the sequential
// path, counts it in CacheStats.SequentialPipelines, and the results are identical. Nothing may have been
// executed when it is returned; any other error means the batch was (at least partly) sent.
var ErrWireNotApplicable = errors.New("sqlgen: pipeline not applicable to the single-round-trip path")

// WireFlavor selects how the statements of a pipeline are rendered for a wire pipeliner.
type WireFlavor uint8

const (
	// WireFlavorPostgres (the default for a pipeliner that does not implement wire.Flavored): one statement
	// per operation, built on WITH _sqlgen_op CTEs and transaction-local set_config conditions, with
	// numbered placeholders. The runtime renders the SQL (renderPipelineBatch) into PipelineOp.SQL before
	// it calls the pipeliner.
	WireFlavorPostgres WireFlavor = iota
	// WireFlavorMySQL: a statement SEQUENCE per operation built on session user variables (@sqlgen_*),
	// ROW_COUNT() / FOUND_ROWS() and conditional COMMIT / ROLLBACK, with the arguments inlined as
	// literals. Rendering needs per-connection knowledge (the time zone and character set of the
	// literals, whether a transaction is open), so the runtime does NOT pre-render: the pipeliner calls
	// RenderMySQLBatch itself.
	WireFlavorMySQL
)

// wireFlavored is the method set of wire.Flavored. A pipeliner without it is WireFlavorPostgres.
type wireFlavored interface {
	WireFlavor() WireFlavor
}

// prerenders reports whether the runtime must render op.SQL (renderPipelineBatch) before handing ops to
// the pipeliner p.
func prerenders(p any) bool {
	if f, ok := p.(wireFlavored); ok {
		return f.WireFlavor() == WireFlavorPostgres
	}
	return true
}

type pipelineCtxKey struct{}

type pipelineCtxPayload struct {
	dialect Dialect
	cache   *StmtCache
	ops     []PipelineOp
	handled bool
}

// withPipelineOps attaches ops to ctx for driver-level wire.Pipeliner interception.
func withPipelineOps(ctx context.Context, ops []PipelineOp) context.Context {
	return context.WithValue(ctx, pipelineCtxKey{}, &pipelineCtxPayload{ops: ops})
}

func withPipelinePayload(ctx context.Context, payload *pipelineCtxPayload) context.Context {
	return context.WithValue(ctx, pipelineCtxKey{}, payload)
}

// pipelineOpsFromContext extracts a queued []PipelineOp slice from ctx.
func pipelineOpsFromContext(ctx context.Context) ([]PipelineOp, bool) {
	if p, ok := ctx.Value(pipelineCtxKey{}).(*pipelineCtxPayload); ok && p != nil {
		return p.ops, true
	}
	return nil, false
}

type wrappedDriver struct {
	base driver.Driver
}

func (d *wrappedDriver) Open(name string) (driver.Conn, error) {
	c, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &pipelineConnWrapper{Conn: c}, nil
}

type wrappedConnector struct {
	base driver.Connector
	drv  driver.Driver
}

func (c *wrappedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &pipelineConnWrapper{Conn: conn}, nil
}

func (c *wrappedConnector) Driver() driver.Driver {
	return c.drv
}

// WrapConnector wraps a database/sql/driver.Connector so that transactions (*sql.Tx)
// can flush PipelineTx batches in a single network round-trip when the underlying
// driver.Conn implements wire.Pipeliner. Wrapping a connector it returned again returns it unchanged.
func WrapConnector(base driver.Connector) driver.Connector {
	if w, ok := base.(*wrappedConnector); ok {
		return w
	}
	return &wrappedConnector{
		base: base,
		drv:  &wrappedDriver{base: base.Driver()},
	}
}

type pipelineConnWrapper struct {
	driver.Conn
}

// wirePipelinerOf returns the connection's wire.Pipeliner, or nil. A connection produced by
// WrapConnector always has an ExecPipeline method (it must intercept the transactional sentinel), so
// merely type-asserting it would claim wire support even when the wrapped driver has none.
func wirePipelinerOf(driverConn any) wirePipeliner {
	if w, ok := driverConn.(*pipelineConnWrapper); ok {
		wp, _ := w.Conn.(wirePipeliner)
		return wp
	}
	wp, _ := driverConn.(wirePipeliner)
	return wp
}

// wireTxPipelinerOf is wirePipelinerOf for wire.TxPipeliner.
func wireTxPipelinerOf(driverConn any) wireTxPipeliner {
	if w, ok := driverConn.(*pipelineConnWrapper); ok {
		tp, _ := w.Conn.(wireTxPipeliner)
		return tp
	}
	tp, _ := driverConn.(wireTxPipeliner)
	return tp
}

func (w *pipelineConnWrapper) ExecTxPipeline(ctx context.Context, txOpts *sql.TxOptions, ops []PipelineOp) error {
	if tp, ok := w.Conn.(wireTxPipeliner); ok {
		return tp.ExecTxPipeline(ctx, txOpts, ops)
	}
	return driver.ErrSkip
}

func (w *pipelineConnWrapper) ExecPipeline(ctx context.Context, ops []PipelineOp) error {
	if wp, ok := w.Conn.(wirePipeliner); ok {
		return wp.ExecPipeline(ctx, ops)
	}
	return driver.ErrSkip
}

func (w *pipelineConnWrapper) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if query == "--sqlgen:pipeline" {
		if payload, ok := ctx.Value(pipelineCtxKey{}).(*pipelineCtxPayload); ok && payload != nil {
			if wp, ok := w.Conn.(wirePipeliner); ok {
				payload.handled = true
				var err error
				if prerenders(wp) {
					err = renderPipelineBatch(payload.dialect, payload.cache, payload.ops)
				}
				if err == nil {
					err = wp.ExecPipeline(ctx, payload.ops)
				}
				if err != nil {
					if errors.Is(err, ErrWireNotApplicable) {
						// Nothing was sent: the caller runs the operations sequentially.
						dropCondArgs(payload.ops)
						payload.handled = false
						return driver.RowsAffected(0), nil
					}
					return nil, err
				}
				return driver.RowsAffected(1), nil
			}
			// Underlying connection does not support wire.Pipeliner; leave payload.handled = false
			// and return without sending sentinel SQL to the database server.
			return driver.RowsAffected(0), nil
		}
	}
	if execer, ok := w.Conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (w *pipelineConnWrapper) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := w.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (w *pipelineConnWrapper) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if prep, ok := w.Conn.(driver.ConnPrepareContext); ok {
		return prep.PrepareContext(ctx, query)
	}
	return w.Conn.Prepare(query)
}

func (w *pipelineConnWrapper) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bt, ok := w.Conn.(driver.ConnBeginTx); ok {
		return bt.BeginTx(ctx, opts)
	}
	return w.Conn.Begin()
}

func (w *pipelineConnWrapper) Ping(ctx context.Context) error {
	if p, ok := w.Conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (w *pipelineConnWrapper) ResetSession(ctx context.Context) error {
	if rs, ok := w.Conn.(driver.SessionResetter); ok {
		return rs.ResetSession(ctx)
	}
	return nil
}

func (w *pipelineConnWrapper) IsValid() bool {
	if v, ok := w.Conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (w *pipelineConnWrapper) CheckNamedValue(nv *driver.NamedValue) error {
	if cnv, ok := w.Conn.(driver.NamedValueChecker); ok {
		return cnv.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

// WireResultKind tells a wire driver what the server answers for an operation whose SQL was rendered
// by renderPipelineBatch, and therefore how to read it.
type WireResultKind uint8

const (
	// WireResultCommandTag: a plain statement without result columns; the command tag's row count is the
	// number of affected rows (op.RowCount).
	WireResultCommandTag WireResultKind = iota
	// WireResultRows: the statement returns the operation's result columns; feed them to op.ScanWireRows,
	// which returns the number of rows (op.RowCount).
	WireResultRows
	// WireResultGuarded: an exec carrying an assertion and/or a recorded condition. The statement returns
	// one row whose first column is the number of affected rows (op.RowCount); the second column is the
	// guard's value and carries no further information.
	WireResultGuarded
	// WireResultDiscard: an Abort bail-out. It returns one row that is ignored; its only effect is the
	// marked error raised when the branch gates are open, which TranslateWireError maps to op.AbortErr.
	WireResultDiscard
)

// WireResult reports how a wire driver must read the rows (if any) the server returns for op.
func (op *PipelineOp) WireResult() WireResultKind {
	switch {
	case op.Decorators.IsAbort:
		return WireResultDiscard
	case op.HasResultCols:
		return WireResultRows
	case op.Decorators.guarded():
		return WireResultGuarded
	default:
		return WireResultCommandTag
	}
}

func (op *PipelineOp) stmtName() string {
	if op.Desc == nil {
		return ""
	}
	return op.Desc.Name
}

// TranslateWireError maps the error a guard raised on the server back to what the caller expects:
// op.AbortErr for an Abort operation, an *AssertionError (with the actual row count) for an operation
// with an assertion. A guard fails its statement with invalid_text_representation (22P02) and an
// unmistakable marker in the message (see writeGuardRaise); nothing else is translated, in particular a
// genuine integer overflow (22003) raised by the caller's own statement passes through unchanged, as on
// the sequential path.
func TranslateWireError(op *PipelineOp, err error) error {
	if err == nil || op == nil {
		return err
	}
	if op.overflowGuard {
		return translateOverflowGuard(op, err)
	}
	var st interface{ SQLState() string }
	if errors.As(err, &st) && st.SQLState() != "22P02" {
		op.Err = err
		return err
	}
	msg := err.Error()
	if op.nRefs > 0 {
		if aerr := translateRefError(op, msg); aerr != nil {
			op.Err = aerr
			return aerr
		}
	}
	if op.Decorators.IsAbort && op.AbortErr != nil && strings.Contains(msg, wireAbortMarker) {
		op.Err = op.AbortErr
		return op.AbortErr
	}
	if op.Decorators.AssertRows >= 0 {
		if i := strings.Index(msg, wireAssertMarker); i >= 0 {
			aerr := &AssertionError{Statement: op.stmtName(), Op: op.opIdx, Label: op.label, Want: op.Decorators.AssertRows}
			// The marker carries the row count the statement produced.
			rest := msg[i+len(wireAssertMarker):]
			n := 0
			for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
				n++
			}
			if got, perr := strconv.ParseInt(rest[:n], 10, 64); perr == nil {
				aerr.Got, aerr.GotKnown = got, true
			}
			op.Err = aerr
			return aerr
		}
	}
	op.Err = err
	return err
}

// translateRefError builds the *ArgError of the marker sqlgen:ref:<ID>:<record> that the check of op
// (CheckSQL) raised, or returns nil when msg carries no such marker. The record is 'x<rows>' for a
// producer that did not yield exactly one row.
func translateRefError(op *PipelineOp, msg string) *ArgError {
	i := strings.Index(msg, wireRefMarker)
	if i < 0 {
		return nil
	}
	rest := msg[i+len(wireRefMarker):]
	n := 0
	for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
		n++
	}
	id, err := strconv.Atoi(rest[:n])
	if err != nil {
		return nil
	}
	r := op.refByID(id)
	if r == nil {
		return nil
	}
	rest = rest[n:]
	var got int64
	known := false
	if strings.HasPrefix(rest, ":x") {
		rest = rest[2:]
		n = 0
		for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
			n++
		}
		got, err = strconv.ParseInt(rest[:n], 10, 64)
		known = err == nil
	}
	return r.argError(op, got, known)
}

// translateOverflowGuard is TranslateWireError for guards rendered for the question dialect, whose only
// possible failure is the constant integer overflow ABS(-9223372036854775808): there is no marker, so an
// overflow of that class on a guarded operation is taken to be the guard.
func translateOverflowGuard(op *PipelineOp, err error) error {
	isOverflow := false
	var st interface{ SQLState() string }
	if errors.As(err, &st) && st.SQLState() == "22003" {
		isOverflow = true
	} else {
		msg := strings.ToLower(err.Error())
		isOverflow = strings.Contains(msg, "sqlstate 22003") ||
			strings.Contains(msg, "integer overflow") ||
			strings.Contains(msg, "bigint out of range")
	}
	if isOverflow {
		if op.Decorators.IsAbort && op.AbortErr != nil {
			op.Err = op.AbortErr
			return op.AbortErr
		}
		if op.Decorators.AssertRows >= 0 {
			op.Err = &AssertionError{Statement: op.stmtName(), Op: op.opIdx, Label: op.label, Want: op.Decorators.AssertRows}
			return op.Err
		}
	}
	op.Err = err
	return err
}

// Pipeline queues SQL operations for single-roundtrip wire execution or sequential fallback.
//
// A *Pipeline is a handle on the queue. The handle an If arm's closure receives is scoped to that arm:
// it can queue only while the closure runs, and while it runs no other handle can queue (doing so fails
// the pipeline with ErrInvalidArgument), so every operation lands in the branch its code is written in.
type Pipeline struct {
	*pipelineState
	scope *branchScope // the If arm this handle belongs to; nil for the pipeline itself
}

// branchScope is one If arm; closed once its closure returned.
type branchScope struct{ closed bool }

// pipelineState is the queue the handles of one pipeline share.
type pipelineState struct {
	cur         *branchScope // the arm whose closure is running (nil: none)
	ctx         context.Context
	db          *sql.DB
	cache       *StmtCache
	txCache     *TxStmtCache
	usePrepared bool
	dialect     Dialect
	ops         []PipelineOp
	opPtrs      []*PipelineOp
	branchStack []BranchGate
	ifResults   []*ifResult // every If chain, resolved after the run
	condSeq     int
	valSeq      int // ValRec IDs
	buildErr    error
	noOps       int // number of NoOp operations queued
	flushed     bool
	failErr     error // the error of a failed flush (futureErr)
	lateErr     error // the first operation queued after the pipeline ran (noteLate)
	ownsTx      bool  // PipelineTx: the runtime begins, commits and (on error) rolls back the transaction
	txOpts      *sql.TxOptions
	// noWire is set once a wire pipeliner answered ErrWireNotApplicable: the pipeline then runs entirely on
	// the sequential path instead of being offered to another wire attempt inside the fallback transaction.
	noWire bool
}

// NewPipeline creates a new Pipeline.
func NewPipeline(ctx context.Context, db *sql.DB, cache *StmtCache, txCache *TxStmtCache, usePrepared bool, dialect Dialect) *Pipeline {
	return &Pipeline{pipelineState: &pipelineState{
		ctx:         ctx,
		db:          db,
		cache:       cache,
		txCache:     txCache,
		usePrepared: usePrepared,
		dialect:     dialect,
		ops:         make([]PipelineOp, 0, 8),
		opPtrs:      make([]*PipelineOp, 0, 8),
	}}
}

// checkHandle fails the pipeline when handle p cannot queue now (what names the call): the pipeline
// already ran, p belongs to an If arm whose closure returned, or another arm's closure is running.
func (p *Pipeline) checkHandle(what string) {
	switch {
	case p.flushed:
		p.noteLate(&PipelineRanError{Statement: what, Op: len(p.opPtrs)})
	case p.scope != nil && p.scope.closed:
		p.SetBuildError(InvalidArgument(what + " was queued on the handle of an If arm whose closure has returned; " +
			"queue it on the handle in scope (the enclosing arm's parameter, or the pipeline)"))
	case p.scope != p.cur:
		p.SetBuildError(InvalidArgument(what + " was queued on an outer pipeline handle while an If arm's closure is running; " +
			"queue it on the handle the closure received"))
	}
}

// noteLate records the first misuse of a pipeline that already ran (an operation or If queued after Flush
// or after the PipelineTx callback returned): every later Flush returns it.
func (p *Pipeline) noteLate(err error) {
	if p.lateErr == nil {
		p.lateErr = err
	}
}

// finish marks a pipeline whose PipelineTx callback failed as done with err: its futures report err and
// anything queued on it afterwards fails with ErrPipelineRan.
func (p *Pipeline) finish(err error) {
	p.flushed = true
	p.failErr = err
	p.releaseArgBufs()
}

// SetBuildError records an argument validation error discovered while queuing an operation.
func (p *Pipeline) SetBuildError(err error) {
	if p.buildErr == nil && err != nil {
		p.buildErr = err
	}
}

func (p *Pipeline) cloneGates() []BranchGate {
	if len(p.branchStack) == 0 {
		return nil
	}
	cp := make([]BranchGate, len(p.branchStack))
	copy(cp, p.branchStack)
	return cp
}

func (p *Pipeline) appendOp(op PipelineOp) *PipelineOp {
	op.opIdx = len(p.opPtrs)
	if p.flushed {
		op.queueErr = &PipelineRanError{Statement: op.stmtName(), Op: op.opIdx}
		p.noteLate(op.queueErr)
	} else if p.scope != p.cur {
		what := "Abort"
		if !op.Decorators.IsAbort {
			what = "statement " + strconv.Quote(op.stmtName())
		}
		p.checkHandle(what + " (operation " + strconv.Itoa(op.opIdx) + ")")
	}
	opPtr := new(PipelineOp)
	*opPtr = op
	p.opPtrs = append(p.opPtrs, opPtr)
	p.ops = append(p.ops, *opPtr)
	return opPtr
}

func (p *Pipeline) syncOpsFromPtrs() {
	for i, ptr := range p.opPtrs {
		p.ops[i] = *ptr
	}
}

// QueueExec queues an exec operation on p and returns its *ExecFuture.
func (p *Pipeline) QueueExec(
	stmtID uint32,
	desc *QueryDescriptor,
	in RenderInput,
	key VariantKey,
	fastSlot *atomic.Pointer[ManagedStmt],
	argBuf *[]any,
	expandErr error,
) *ExecFuture {
	if expandErr != nil {
		p.SetBuildError(expandErr)
	}
	in.BranchGates = p.cloneGates()
	fut := &ExecFuture{pl: p}
	var args []any
	if argBuf != nil {
		args = *argBuf
	}
	opPtr := p.appendOp(PipelineOp{
		StmtID:        stmtID,
		Desc:          desc,
		Input:         in,
		Decorators:    OpDecorators{AssertRows: -1},
		Key:           key,
		FastSlot:      fastSlot,
		Args:          args,
		argBuf:        argBuf,
		HasResultCols: false,
		OnSkipped: func() {
			fut.skipped = true
			if fut.op != nil {
				fut.op.Skipped = true
			}
		},
		OnDone: func(rowCount int64, err error) {
			fut.err = err
			if fut.op != nil {
				fut.op.RowCount = rowCount
			}
		},
		ExecSeq: func(ctx context.Context, pl *Pipeline, op *PipelineOp) error {
			shapeTag := ComputeBatchShapeTag(op.Input.ColMask, op.Input.FlagMask)
			res, err := pl.cache.Exec(ctx, pl.db, pl.txCache, pl.usePrepared, op.Key, op.StmtID, shapeTag, 0, op.FastSlot, pl.dialect, op.Desc, &op.Input, op.Args...)
			if err != nil {
				fut.err = err
				op.Err = err
				return err
			}
			fut.res = res
			if res != nil {
				if n, err := res.RowsAffected(); err == nil {
					op.RowCount = n
				}
			}
			return nil
		},
	})
	fut.op = opPtr
	return fut
}

// QueueExecNoop queues an exec operation that has nothing to execute (the statement's multi-row VALUES
// collection is empty) and returns its *ExecFuture. No SQL is sent for it, on any path: the future
// resolves to zero rows affected and no error, a condition over it sees zero rows, and inside an untaken
// If branch it is skipped like any other operation.
func (p *Pipeline) QueueExecNoop() *ExecFuture {
	fut := &ExecFuture{pl: p}
	fut.op = p.queueNoop(func() { fut.skipped = true }, func(err error) { fut.err = err })
	return fut
}

// QueueSliceNoop is QueueExecNoop for a multi-row statement: the SliceFuture resolves to no rows.
func QueueSliceNoop[T any](p *Pipeline) *SliceFuture[T] {
	fut := &SliceFuture[T]{pl: p}
	fut.op = p.queueNoop(func() { fut.skipped = true }, func(err error) { fut.err = err })
	return fut
}

// queueNoop queues a NoOp. onDone receives the operation's error (a failing assertion on it), so its
// future reports that error like the future of any other failing operation.
func (p *Pipeline) queueNoop(onSkipped func(), onDone func(err error)) *PipelineOp {
	var in RenderInput
	in.BranchGates = p.cloneGates()
	p.noOps++
	var opPtr *PipelineOp
	opPtr = p.appendOp(PipelineOp{
		Input:      in,
		Decorators: OpDecorators{AssertRows: -1},
		NoOp:       true,
		OnSkipped: func() {
			onSkipped()
			opPtr.Skipped = true
		},
		OnDone:  func(_ int64, err error) { onDone(err) },
		ExecSeq: func(context.Context, *Pipeline, *PipelineOp) error { return nil },
	})
	return opPtr
}

// plainNoOp reports whether op is a NoOp nothing else depends on: no assertion, and no condition recorded
// over it (those need the operation to run, which only the sequential path can do without SQL).
func (op *PipelineOp) plainNoOp() bool {
	d := &op.Decorators
	return op.NoOp && d.AssertRows < 0 && d.ProduceCond == nil && len(d.ExtraConds) == 0 && len(d.LeafRecs) == 0 &&
		len(d.ValRecs) == 0
}

// wireOps returns the operations a wire driver is handed: p.ops without the NoOp operations (there is no
// statement for them). restore copies what the driver wrote (row counts, conditions, SQL) back into p.ops
// and must be called once the driver returned having run the batch.
func (p *Pipeline) wireOps() (ops []PipelineOp, restore func()) {
	if p.noOps == 0 {
		return p.ops, func() {}
	}
	idx := make([]int, 0, len(p.ops)-p.noOps)
	ops = make([]PipelineOp, 0, len(p.ops)-p.noOps)
	for i := range p.ops {
		if !p.ops[i].NoOp {
			idx = append(idx, i)
			ops = append(ops, p.ops[i])
		}
	}
	return ops, func() {
		for k, i := range idx {
			p.ops[i] = ops[k]
		}
	}
}

// prepareNoOps decides how a pipeline with NoOp operations runs. done is true when nothing is left to
// send, so the operations are resolved without touching the database (no connection, no transaction).
//
// A NoOp's row count is known client-side (0), so an assertion on a NoOp outside any If branch is decided
// here: a passing one is dropped (the NoOp becomes plain and the pipeline keeps its single round trip), a
// failing one fails the pipeline before anything is sent (err, the same *AssertionError the sequential path
// returns; nothing is committed either way). A NoOp that something else depends on (an assertion inside an
// If branch, which applies only if the branch is taken, or a condition) makes the pipeline run on the
// sequential path instead, which resolves it exactly like any other operation.
func (p *Pipeline) prepareNoOps() (done bool, err error) {
	if p.noOps == 0 {
		return false, nil
	}
	allPlain := true
	for i := range p.ops {
		op := &p.ops[i]
		if op.NoOp && op.Decorators.AssertRows >= 0 && len(op.Input.BranchGates) == 0 {
			if op.Decorators.AssertRows != 0 {
				aerr := &AssertionError{Statement: op.stmtName(), Op: op.opIdx, Label: op.label, Want: op.Decorators.AssertRows, Got: 0, GotKnown: true}
				op.Err = aerr
				*p.opPtrs[i] = *op
				if op.OnDone != nil {
					op.OnDone(0, aerr)
				}
				return false, aerr
			}
			op.Decorators.AssertRows = -1
		}
		if op.NoOp && !op.plainNoOp() {
			p.noWire = true
		}
		if !op.plainNoOp() {
			allPlain = false
		}
	}
	return allPlain, nil
}

// RowScanner is what a generated scan function reads one row from: *sql.Row, *sql.Rows, or the reader of a
// row that also carries WhereCol columns.
type RowScanner interface{ Scan(dest ...any) error }

// colPredScanner reads the rows of a statement with WhereCol columns: the caller's destinations, then one
// integer per column (Input.ColPreds order), recorded in op.colFail.
type colPredScanner struct {
	rows *sql.Rows
	op   *PipelineOp
	dest []any
	vals []sql.NullInt64
}

// newRowScanner returns the scanner of rows for op: rows itself unless op has WhereCol columns. It resets
// what the columns recorded (the statement may run again: a retried transaction, a fallback).
func newRowScanner(rows *sql.Rows, op *PipelineOp) RowScanner {
	if op == nil || len(op.Input.ColPreds) == 0 {
		return rows
	}
	clear(op.colFail)
	return &colPredScanner{rows: rows, op: op, vals: make([]sql.NullInt64, len(op.Input.ColPreds))}
}

func (s *colPredScanner) Scan(dest ...any) error {
	s.dest = append(s.dest[:0], dest...)
	for k := range s.vals {
		s.dest = append(s.dest, &s.vals[k])
	}
	if err := s.rows.Scan(s.dest...); err != nil {
		return err
	}
	for k, v := range s.vals {
		if (!v.Valid || v.Int64 != 1) && k < len(s.op.colFail) {
			s.op.colFail[k] = true
		}
	}
	return nil
}

// querySeq runs a query operation on the sequential path. A statement with WhereCol columns is rendered
// on its own (the predicates and their arguments are not part of the cached variants).
func (p *Pipeline) querySeq(ctx context.Context, op *PipelineOp) (*sql.Rows, *ManagedStmt, error) {
	if len(op.Input.ColPreds) == 0 {
		shapeTag := ComputeBatchShapeTag(op.Input.ColMask, op.Input.FlagMask)
		return p.cache.Query(ctx, p.db, p.txCache, p.usePrepared, op.Key, op.StmtID, shapeTag, 0, op.FastSlot, p.dialect, op.Desc, &op.Input, op.Args...)
	}
	sqlStr, args := renderColPredSQL(p.dialect, op)
	var rows *sql.Rows
	var err error
	if p.txCache != nil {
		rows, err = p.txCache.tx.QueryContext(ctx, sqlStr, args...)
	} else {
		rows, err = p.db.QueryContext(ctx, sqlStr, args...)
	}
	return rows, nil, err
}

// QueueRow queues a single-row ("one") operation on p and returns its *RowFuture[T].
func QueueRow[T any](
	p *Pipeline,
	stmtID uint32,
	desc *QueryDescriptor,
	in RenderInput,
	key VariantKey,
	fastSlot *atomic.Pointer[ManagedStmt],
	argBuf *[]any,
	expandErr error,
	scan func(RowScanner, *T) error,
) *RowFuture[T] {
	if expandErr != nil {
		p.SetBuildError(expandErr)
	}
	in.BranchGates = p.cloneGates()
	fut := &RowFuture[T]{pl: p}
	var args []any
	if argBuf != nil {
		args = *argBuf
	}
	opPtr := p.appendOp(PipelineOp{
		StmtID:        stmtID,
		Desc:          desc,
		Input:         in,
		Decorators:    OpDecorators{AssertRows: -1},
		Key:           key,
		FastSlot:      fastSlot,
		Args:          args,
		argBuf:        argBuf,
		HasResultCols: true,
		IsSingleRow:   true,
		OnSkipped: func() {
			fut.skipped = true
			if fut.op != nil {
				fut.op.Skipped = true
			}
		},
		OnDone: func(rowCount int64, err error) {
			if err != nil {
				fut.err = err
			}
			if fut.op != nil {
				fut.op.RowCount = rowCount
			}
		},
		ScanWireRows: func(rows *sql.Rows) (int64, error) {
			var count int64
			sc := newRowScanner(rows, fut.op)
			for rows.Next() {
				var item T
				if err := scan(sc, &item); err != nil {
					fut.err = err
					return count, err
				}
				if count == 0 {
					fut.val = item
					fut.found = true
				}
				count++
			}
			err := rows.Err()
			fut.err = err
			return count, err
		},
		ExecSeq: func(ctx context.Context, pl *Pipeline, op *PipelineOp) error {
			if op.countRows || len(op.Input.ColPreds) > 0 {
				// A producer of a forwarded value, or a statement with WhereCol columns: count (and read)
				// every row, as the wire paths do.
				rows, ms, err := pl.querySeq(ctx, op)
				if err != nil {
					fut.err = err
					op.Err = err
					return err
				}
				var count int64
				sc := newRowScanner(rows, op)
				for rows.Next() {
					var item T
					if err := scan(sc, &item); err != nil {
						closeRows(rows, ms)
						fut.err = err
						op.Err = err
						return err
					}
					if count == 0 {
						fut.val, fut.found = item, true
					}
					count++
				}
				err = rows.Err()
				closeRows(rows, ms)
				if err != nil {
					fut.err = err
					op.Err = err
					return err
				}
				op.RowCount = count
				return nil
			}
			shapeTag := ComputeBatchShapeTag(op.Input.ColMask, op.Input.FlagMask)
			row, ms, err := pl.cache.QueryRow(ctx, pl.db, pl.txCache, pl.usePrepared, op.Key, op.StmtID, shapeTag, 0, op.FastSlot, pl.dialect, op.Desc, &op.Input, op.Args...)
			if err != nil {
				fut.err = err
				op.Err = err
				return err
			}
			var item T
			scanErr := scan(row, &item)
			_ = ms.Release()
			if scanErr != nil {
				if errors.Is(scanErr, sql.ErrNoRows) {
					op.RowCount = 0
					fut.found = false
					return nil
				}
				fut.err = scanErr
				op.Err = scanErr
				return scanErr
			}
			fut.val = item
			fut.found = true
			op.RowCount = 1
			return nil
		},
	})
	fut.op = opPtr
	return fut
}

// QueueSlice queues a multi-row ("many") operation on p and returns its *SliceFuture[T].
func QueueSlice[T any](
	p *Pipeline,
	stmtID uint32,
	desc *QueryDescriptor,
	in RenderInput,
	key VariantKey,
	fastSlot *atomic.Pointer[ManagedStmt],
	argBuf *[]any,
	expandErr error,
	scan func(RowScanner, *T) error,
) *SliceFuture[T] {
	if expandErr != nil {
		p.SetBuildError(expandErr)
	}
	in.BranchGates = p.cloneGates()
	fut := &SliceFuture[T]{pl: p}
	var args []any
	if argBuf != nil {
		args = *argBuf
	}
	opPtr := p.appendOp(PipelineOp{
		StmtID:        stmtID,
		Desc:          desc,
		Input:         in,
		Decorators:    OpDecorators{AssertRows: -1},
		Key:           key,
		FastSlot:      fastSlot,
		Args:          args,
		argBuf:        argBuf,
		HasResultCols: true,
		IsSingleRow:   false,
		OnSkipped: func() {
			fut.skipped = true
			if fut.op != nil {
				fut.op.Skipped = true
			}
		},
		OnDone: func(rowCount int64, err error) {
			if err != nil {
				fut.err = err
			}
			if fut.op != nil {
				fut.op.RowCount = rowCount
			}
		},
		ScanWireRows: func(rows *sql.Rows) (int64, error) {
			var count int64
			sc := newRowScanner(rows, fut.op)
			for rows.Next() {
				var item T
				if err := scan(sc, &item); err != nil {
					fut.err = err
					return count, err
				}
				fut.vals = append(fut.vals, item)
				count++
			}
			err := rows.Err()
			fut.err = err
			return count, err
		},
		ExecSeq: func(ctx context.Context, pl *Pipeline, op *PipelineOp) error {
			rows, ms, err := pl.querySeq(ctx, op)
			if err != nil {
				fut.err = err
				op.Err = err
				return err
			}
			var count int64
			sc := newRowScanner(rows, op)
			for rows.Next() {
				var item T
				if err := scan(sc, &item); err != nil {
					closeRows(rows, ms)
					fut.err = err
					op.Err = err
					return err
				}
				fut.vals = append(fut.vals, item)
				count++
			}
			err = rows.Err()
			closeRows(rows, ms)
			if err != nil {
				fut.err = err
				op.Err = err
				return err
			}
			op.RowCount = count
			return nil
		},
	})
	fut.op = opPtr
	return fut
}

// Abort queues a conditional or unconditional server-side bail-out that aborts the transaction with err.
func (p *Pipeline) Abort(err error) {
	if err == nil {
		err = ErrAssertionFailed
	}
	var in RenderInput
	in.BranchGates = p.cloneGates()
	p.appendOp(PipelineOp{
		Input: in,
		Decorators: OpDecorators{
			AssertRows: -1,
			IsAbort:    true,
		},
		AbortErr: err,
	})
}

// produceCond makes cond's latest operation produce its value under a new condition ID, which it
// returns. Every leaf of a compound condition is evaluated over ITS OWN operation's rows: only the root
// (latest) operation's statement can see its own rows, so the earlier operations record their leaves
// server-side and the bound condition reads those records. A zero PipelineCond, or one over an operation
// of another pipeline, fails the pipeline with ErrInvalidArgument.
func (p *Pipeline) produceCond(what string, cond *PipelineCond) int {
	p.condSeq++
	condID := p.condSeq
	if err := p.checkCond(cond); err != "" {
		p.SetBuildError(InvalidArgument(what + ": " + err))
		return condID
	}
	rootOp := cond.Op
	bound := p.bindCond(cond, rootOp)
	if rootOp.Decorators.ProduceCond == nil {
		rootOp.Decorators.ProduceCond = bound
		rootOp.Decorators.CondID = condID
	} else {
		pc := ProducedCond{CondID: condID, Cond: bound}
		rootOp.ExtraConds = append(rootOp.ExtraConds, pc)
		rootOp.Decorators.ExtraConds = append(rootOp.Decorators.ExtraConds, pc)
	}
	return condID
}

// checkCond returns why c cannot be a condition of p ("" when it can): every leaf must be over an
// operation queued on p (a zero PipelineCond has none).
func (p *Pipeline) checkCond(c *PipelineCond) string {
	switch {
	case c == nil:
		return "a missing operand"
	case c.Kind == CondAnd || c.Kind == CondOr:
		if e := p.checkCond(c.Left); e != "" {
			return e
		}
		return p.checkCond(c.Right)
	case c.Kind == CondNot:
		return p.checkCond(c.Left)
	case c.Op == nil:
		return "the condition is a zero PipelineCond; build conditions from a future (f.Found(), f.RowsEq(n), f.WhereCol(...))"
	case c.Op.opIdx >= len(p.opPtrs) || p.opPtrs[c.Op.opIdx] != c.Op:
		return "the condition is over an operation of another pipeline; use the futures of this pipeline"
	}
	return ""
}

// bindCond returns a copy of c in which every leaf over an operation other than root refers to the
// server-side record its own operation makes of it. c itself is never modified (it may be reused by the
// caller in other conditions).
func (p *Pipeline) bindCond(c *PipelineCond, root *PipelineOp) *PipelineCond {
	if c == nil {
		return nil
	}
	out := *c
	switch c.Kind {
	case CondAnd, CondOr, CondNot:
		out.Left = p.bindCond(c.Left, root)
		out.Right = p.bindCond(c.Right, root)
	default:
		if c.Op != nil && c.Op != root {
			out.Recorded = p.recordLeaf(c)
		}
	}
	return &out
}

// recordLeaf makes leaf's own operation record the leaf's value and returns the record's ID. Equal
// leaves of one operation share a record.
func (p *Pipeline) recordLeaf(leaf *PipelineCond) int {
	op := leaf.Op
	for _, r := range op.Decorators.LeafRecs {
		if r.Cond.Kind == leaf.Kind && r.Cond.Threshold == leaf.Threshold && r.Cond.Col == leaf.Col {
			return r.CondID
		}
	}
	p.condSeq++
	own := *leaf
	own.Recorded = 0
	op.Decorators.LeafRecs = append(op.Decorators.LeafRecs, ProducedCond{CondID: p.condSeq, Cond: &own})
	return p.condSeq
}

func (p *Pipeline) releaseArgBufs() {
	for i := range p.ops {
		if p.ops[i].argBuf != nil {
			ReleaseArgs(p.ops[i].argBuf)
			p.ops[i].argBuf = nil
		}
	}
}

func evalGatesGo(gates []BranchGate, condVals map[int]bool) bool {
	for _, g := range gates {
		val, ok := condVals[g.CondID]
		if !ok && g.Cond != nil {
			val = g.Cond.EvalGoFn()
		}
		if val != g.WantTrue {
			return false
		}
	}
	return true
}

func (p *Pipeline) executePipelineOps(ctx context.Context, wireMode bool) error {
	condVals := make(map[int]bool, len(p.ifResults))
	for i := range p.ops {
		op := &p.ops[i]
		if !evalGatesGo(op.Input.BranchGates, condVals) {
			op.Skipped = true
			*p.opPtrs[i] = *op
			if op.OnSkipped != nil {
				op.OnSkipped()
			}
			continue
		}
		if op.Decorators.IsAbort {
			return op.AbortErr
		}
		if !wireMode && op.nRefs > 0 {
			if err := resolveRefs(op); err != nil {
				op.Err = err
				*p.opPtrs[i] = *op
				if op.OnDone != nil {
					op.OnDone(0, err)
				}
				return err
			}
		}
		if !wireMode && op.ExecSeq != nil {
			op.Input.BranchGates = nil
			if err := op.ExecSeq(ctx, p, op); err != nil {
				*p.opPtrs[i] = *op
				return err
			}
		}
		*p.opPtrs[i] = *op
		if op.Decorators.AssertRows >= 0 && op.RowCount != op.Decorators.AssertRows {
			aerr := &AssertionError{Statement: op.stmtName(), Op: op.opIdx, Label: op.label, Want: op.Decorators.AssertRows, Got: op.RowCount, GotKnown: true}
			op.Err = aerr
			*p.opPtrs[i] = *op
			if op.OnDone != nil {
				op.OnDone(op.RowCount, aerr)
			}
			return aerr
		}
		if op.Decorators.ProduceCond != nil {
			if wireMode && op.CondSet {
				condVals[op.Decorators.CondID] = op.CondVal
			} else {
				val := op.Decorators.ProduceCond.EvalGoFn()
				op.CondVal = val
				op.CondSet = true
				condVals[op.Decorators.CondID] = val
			}
		}
		for _, ec := range op.ExtraConds {
			if wireMode && ec.Set {
				condVals[ec.CondID] = ec.Val
			} else {
				condVals[ec.CondID] = ec.Cond.EvalGoFn()
			}
		}
		*p.opPtrs[i] = *op
		if op.OnDone != nil {
			op.OnDone(op.RowCount, op.Err)
		}
	}
	for _, r := range p.ifResults {
		r.resolved = true
		if !evalGatesGo(r.base, condVals) {
			r.skipped = true
			continue
		}
		for i, g := range r.arms {
			if evalGatesGo(g, condVals) {
				r.taken = i
				break
			}
		}
	}
	return nil
}

// Flush executes all queued operations in the pipeline.
func (p *Pipeline) Flush() error {
	if p.ownsTx {
		return ErrFlushInPipelineTx
	}
	if p.scope != nil || p.cur != nil {
		err := InvalidArgument("Flush was called on an If arm's handle or while an If arm's closure is running; flush the pipeline itself, after its If chains")
		p.SetBuildError(err)
		return err
	}
	return p.run()
}

func (p *Pipeline) run() (err error) {
	if p.flushed {
		// Flush is idempotent: it reports the outcome of the run, unless something was queued after it.
		if p.lateErr != nil {
			return p.lateErr
		}
		return p.failErr
	}
	defer func() { p.failErr = err }()
	p.flushed = true
	p.syncOpsFromPtrs()
	defer p.releaseArgBufs()

	if p.buildErr != nil {
		return p.buildErr
	}
	if len(p.ops) == 0 {
		return nil
	}
	if done, err := p.prepareNoOps(); err != nil {
		return err
	} else if done {
		return p.executePipelineOps(p.ctx, true)
	}
	switch {
	case p.txCache != nil:
		return p.flushInTx()
	case p.ownsTx:
		return p.flushOwnTx()
	default:
		return p.flushStandalone()
	}
}

// flushInTx runs the pipeline inside a transaction somebody else began (a user-managed *sql.Tx, or the
// portable PipelineTx path): ops only, in one round-trip when the connection is a wire.Pipeliner behind
// WrapConnector, otherwise one statement at a time.
func (p *Pipeline) flushInTx() error {
	if p.db != nil {
		if _, isWrapped := p.db.Driver().(*wrappedDriver); isWrapped && !p.noWire {
			ops, restore := p.wireOps()
			payload := &pipelineCtxPayload{
				dialect: p.dialect,
				cache:   p.cache,
				ops:     ops,
			}
			_, err := p.txCache.tx.ExecContext(withPipelinePayload(p.ctx, payload), "--sqlgen:pipeline")
			if payload.handled {
				restore()
				p.cache.notePipeline(true)
				if err != nil {
					return err
				}
				return p.executePipelineOps(p.ctx, true)
			}
		}
	}
	p.cache.notePipeline(false)
	return p.executePipelineOps(p.ctx, false)
}

// flushWireConn tries the single-round-trip paths on a pooled connection obtained through db.Conn and
// conn.Raw. handled reports whether a wire pipeliner executed the batch (err is then its outcome, a
// *CommitError when the COMMIT failed); when it is false nothing was sent and the caller falls back.
// A wire.TxPipeliner (BEGIN + ops + COMMIT) is always preferred; a plain wire.Pipeliner is used only when
// allowPlain is set, because it cannot begin or commit a transaction by itself.
func (p *Pipeline) flushWireConn(txOpts *sql.TxOptions, allowPlain bool) (handled bool, err error) {
	if p.db == nil || p.noWire || p.db.Stats().MaxOpenConnections == 1 {
		return false, nil
	}
	conn, cerr := p.db.Conn(p.ctx)
	if cerr != nil {
		return false, nil
	}
	var wireErr error
	rawErr := conn.Raw(func(driverConn any) error {
		if tp := wireTxPipelinerOf(driverConn); tp != nil {
			handled = true
			ops, restore := p.wireOps()
			if prerenders(tp) {
				wireErr = renderPipelineBatch(p.dialect, p.cache, ops)
			}
			if wireErr == nil {
				wireErr = tp.ExecTxPipeline(p.ctx, txOpts, ops)
			}
			if !errors.Is(wireErr, ErrWireNotApplicable) {
				restore()
			} else {
				dropCondArgs(ops)
			}
			return nil
		}
		if wp := wirePipelinerOf(driverConn); allowPlain && wp != nil {
			handled = true
			ops, restore := p.wireOps()
			if prerenders(wp) {
				wireErr = renderPipelineBatch(p.dialect, p.cache, ops)
			}
			if wireErr == nil {
				wireErr = wp.ExecPipeline(p.ctx, ops)
			}
			if !errors.Is(wireErr, ErrWireNotApplicable) {
				restore()
			} else {
				dropCondArgs(ops)
			}
		}
		return nil
	})
	_ = conn.Close()
	if rawErr != nil || !handled {
		return false, nil
	}
	if errors.Is(wireErr, ErrWireNotApplicable) {
		p.noWire = true
		return false, nil
	}
	p.cache.notePipeline(true)
	if wireErr != nil {
		return true, wireErr
	}
	return true, p.executePipelineOps(p.ctx, true)
}

// unwrapCommitError removes the *CommitError marker so callers outside the retry loop see the driver error.
func unwrapCommitError(err error) error {
	if ce, ok := err.(*CommitError); ok {
		return ce.Err
	}
	return err
}

// flushStandalone runs a Pipeline that is not bound to any transaction: atomically (all or nothing),
// in one round-trip when the connection supports it.
func (p *Pipeline) flushStandalone() error {
	if handled, err := p.flushWireConn(nil, true); handled {
		return unwrapCommitError(err)
	}

	// Sequential fallback inside an implicit transaction.
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}
	tc := NewTxStmtCache(tx)
	defer func() { _ = tc.Close() }()
	p.txCache = tc
	if err := p.flushInTx(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// flushOwnTx runs a PipelineTx attempt: the runtime owns the transaction. With a wire.TxPipeliner
// connection BEGIN, every operation and COMMIT travel in one round-trip; otherwise it falls back to
// BeginTx + flushInTx + Commit. Errors raised by COMMIT are wrapped in a *CommitError.
func (p *Pipeline) flushOwnTx() error {
	if handled, err := p.flushWireConn(p.txOpts, false); handled {
		return err
	}
	tx, err := p.db.BeginTx(p.ctx, p.txOpts)
	if err != nil {
		return err
	}
	tc := NewTxStmtCache(tx)
	defer func() {
		_ = tc.Close()
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()
	p.txCache = tc
	if err := p.flushInTx(); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return &CommitError{Err: err}
	}
	return nil
}

// TxRetryPolicy configures automatic exponential-backoff retries on transient transaction conflicts.
type TxRetryPolicy struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// DefaultRetryPolicy returns the standard production retry policy for serialization/deadlock errors.
func DefaultRetryPolicy() TxRetryPolicy {
	return TxRetryPolicy{
		MaxRetries: 5,
		BaseDelay:  2 * time.Millisecond,
		MaxDelay:   100 * time.Millisecond,
	}
}

// txCfg holds transaction execution options.
type txCfg struct {
	RetryPolicy TxRetryPolicy
}

// TxOption configures InTx and PipelineTx execution.
type TxOption func(*txCfg)

// WithTxRetry enables automatic retries with exponential backoff and full jitter according to policy.
func WithTxRetry(policy TxRetryPolicy) TxOption {
	return func(cfg *txCfg) {
		cfg.RetryPolicy = policy
	}
}

// IsRetryableTxError reports whether err is a transient deadlock, lock timeout, or serialization
// failure safe to retry. Transport drops during Commit are never retried.
func IsRetryableTxError(err error, duringCommit bool) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrAssertionFailed) {
		return false
	}
	if duringCommit {
		if errors.Is(err, driver.ErrBadConn) ||
			errors.Is(err, io.EOF) ||
			errors.Is(err, io.ErrUnexpectedEOF) {
			return false
		}
		var netErr net.Error
		if errors.As(err, &netErr) {
			return false
		}
	} else {
		if errors.Is(err, driver.ErrBadConn) ||
			errors.Is(err, io.EOF) ||
			errors.Is(err, io.ErrUnexpectedEOF) {
			return true
		}
		var netErr net.Error
		if errors.As(err, &netErr) {
			return true
		}
	}

	// Structured codes first (errors.As walks wrapped errors, e.g. fmt.Errorf("...: %w", err)).
	var st interface{ SQLState() string }
	if errors.As(err, &st) {
		switch st.SQLState() {
		case "40001", "40P01", "55P03":
			return true
		}
	}
	var cd interface{ Code() int }
	if errors.As(err, &cd) {
		code := cd.Code()
		if retryableMySQLNumber(code) {
			return true
		}
		if lowByte := code & 0xFF; lowByte == 5 || lowByte == 6 { // SQLITE_BUSY / SQLITE_LOCKED (+ extended codes)
			return true
		}
	}
	var num interface{ Number() uint16 }
	if errors.As(err, &num) {
		if retryableMySQLNumber(int(num.Number())) {
			return true
		}
	}

	// Drivers that expose codes as struct fields rather than methods (go-sql-driver/mysql's
	// MySQLError.Number / .SQLState) are recognised by their stable message text.
	// Only unambiguous driver phrasing: a bare "40001" or "deadlock" would also match data values in
	// permanent errors (e.g. "Duplicate entry '40001' for key ...") and make RunTxWithRetry re-run a
	// transaction that can never succeed.
	msg := strings.ToLower(err.Error())
	for _, marker := range transientErrorMarkers {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// retryableMySQLNumber: MySQL/MariaDB error numbers that are transient and safe to retry: 1213 deadlock,
// 1205 lock wait timeout, and TiDB's 9007 (write conflict, "try again later") and 8022 ("KV error safe to
// retry", raised at commit).
func retryableMySQLNumber(n int) bool {
	switch n {
	case 1213, 1205, 9007, 8022:
		return true
	}
	return false
}

var transientErrorMarkers = []string{
	"sqlstate 40001", "sqlstate 40p01", "sqlstate 55p03", // PostgreSQL / CockroachDB
	"deadlock detected", "could not serialize access", "restart transaction", // PostgreSQL / CockroachDB
	"error 1213 (", "error 1205 (", // MySQL / MariaDB: deadlock, lock wait timeout
	"error 9007 (", "error 8022 (", "[try again later]", // TiDB: write conflict, KV error safe to retry
	"deadlock found when trying to get lock", "lock wait timeout exceeded",
	"record has changed since last read",                                             // MariaDB 11.6+ (innodb_snapshot_isolation): error 1020, "try restarting transaction"
	"sqlite_busy", "sqlite_locked", "database is locked", "database table is locked", // SQLite
}

// RunTxWithRetry executes attemptFn inside a transaction managed by db, applying opts (such as WithTxRetry).
func RunTxWithRetry(
	ctx context.Context,
	db *sql.DB,
	existingTxCache *TxStmtCache,
	txOpts *sql.TxOptions,
	opts []TxOption,
	attemptFn func(txCache *TxStmtCache) error,
) error {
	if existingTxCache != nil {
		return attemptFn(existingTxCache)
	}
	return retryTx(ctx, txConfig(opts).RetryPolicy, func() (bool, error) {
		var duringCommit bool
		err := func() (retErr error) {
			tx, err := db.BeginTx(ctx, txOpts)
			if err != nil {
				return err
			}
			tc := NewTxStmtCache(tx)
			defer func() {
				_ = tc.Close()
				if r := recover(); r != nil {
					_ = tx.Rollback()
					panic(r)
				}
			}()
			if err := attemptFn(tc); err != nil {
				_ = tx.Rollback()
				return err
			}
			duringCommit = true
			return tx.Commit()
		}()
		return duringCommit, err
	})
}

// RunPipelineTx implements PipelineTx. build queues the operations on the pipeline it is given; the
// runtime then executes them in one transaction, retrying the whole attempt (build included) under
// WithTxRetry. When the pooled connection is a wire.TxPipeliner the attempt costs a single network
// round-trip, BEGIN and COMMIT included; otherwise it is BeginTx, the portable execution of the queued
// operations, Commit. When existingTxCache is non-nil the transaction belongs to the caller and the
// pipeline just runs inside it (no retry, no commit).
func RunPipelineTx(
	ctx context.Context,
	db *sql.DB,
	cache *StmtCache,
	existingTxCache *TxStmtCache,
	usePrepared bool,
	dialect Dialect,
	txOpts *sql.TxOptions,
	opts []TxOption,
	build func(p *Pipeline) error,
) error {
	if existingTxCache != nil {
		p := NewPipeline(ctx, db, cache, existingTxCache, usePrepared, dialect)
		if err := build(p); err != nil {
			p.finish(err)
			return err
		}
		return p.Flush()
	}
	return retryTx(ctx, txConfig(opts).RetryPolicy, func() (bool, error) {
		p := NewPipeline(ctx, db, cache, nil, usePrepared, dialect)
		p.ownsTx = true
		p.txOpts = txOpts
		if err := build(p); err != nil {
			p.finish(err)
			return false, err
		}
		err := p.run()
		var ce *CommitError
		if errors.As(err, &ce) {
			return true, unwrapCommitError(err)
		}
		return false, err
	})
}

func txConfig(opts []TxOption) txCfg {
	var cfg txCfg
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// retryTx runs attempt until it succeeds, fails permanently, or policy.MaxRetries is exhausted, sleeping
// an exponentially growing, fully jittered delay between attempts. attempt reports whether its error
// happened while committing (IsRetryableTxError treats a dropped connection there as unsafe to retry).
func retryTx(ctx context.Context, policy TxRetryPolicy, attempt func() (duringCommit bool, err error)) error {
	for n := 0; ; n++ {
		duringCommit, err := attempt()
		if err == nil {
			return nil
		}
		if n >= policy.MaxRetries || !IsRetryableTxError(err, duringCommit) {
			return err
		}
		base := policy.BaseDelay
		if base <= 0 {
			base = 2 * time.Millisecond
		}
		maxD := policy.MaxDelay
		if maxD <= 0 {
			maxD = 100 * time.Millisecond
		}
		shift := n
		if shift > 20 {
			shift = 20
		}
		ceil := base << shift
		if ceil > maxD || ceil <= 0 {
			ceil = maxD
		}
		sleepDur := time.Duration(rand.Int64N(int64(ceil) + 1))
		timer := time.NewTimer(sleepDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
