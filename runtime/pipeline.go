package runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"math/rand/v2"
	"net"
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
	Kind       CondKind
	Op         *PipelineOp
	Threshold  int64
	SQLPred    string
	EvalGoPred func() bool
	Left       *PipelineCond
	Right      *PipelineCond
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
		return c.Op != nil && !c.Op.Skipped && c.EvalGoPred != nil && c.EvalGoPred()
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

// WhereCol constructs a column-level PipelineCond over a RowFuture.
func WhereCol[T any](f *RowFuture[T], sqlPred string, goPred func(T) bool) PipelineCond {
	if f == nil {
		return PipelineCond{Kind: CondColPred, SQLPred: sqlPred}
	}
	return f.WhereCol(sqlPred, goPred)
}

// ProducedCond represents an additional boolean condition evaluated after a PipelineOp completes.
type ProducedCond struct {
	CondID int
	Cond   *PipelineCond
}

// PipelineOp represents a single queued operation inside a Pipeline or PipelineTx.
type PipelineOp struct {
	opIdx         int
	StmtID        uint32
	Desc          *QueryDescriptor
	Input         RenderInput
	Decorators    OpDecorators
	ExtraConds    []ProducedCond
	Key           VariantKey
	FastSlot      *atomic.Pointer[ManagedStmt]
	SQL           string
	Args          []any
	argBuf        *[]any
	HasResultCols bool
	IsSingleRow   bool
	ExecSeq       func(ctx context.Context, p *Pipeline, op *PipelineOp) error
	ScanWireRows  func(rows *sql.Rows) (int64, error)
	OnSkipped     func()
	OnDone        func(rowCount int64, err error)
	AbortErr      error
	RowCount      int64
	Skipped       bool
	CondVal       bool
	CondSet       bool
	Err           error
}

// ExecFuture represents the pending result of an exec statement queued in a Pipeline.
type ExecFuture struct {
	op      *PipelineOp
	res     sql.Result
	skipped bool
	err     error
}

// AssertRowsAffected requires the operation to affect exactly n rows, rolling back with ErrAssertionFailed otherwise.
func (f *ExecFuture) AssertRowsAffected(n int64) *ExecFuture {
	f.op.Decorators.AssertRows = n
	return f
}

// AssertFound requires the operation to affect exactly 1 row.
func (f *ExecFuture) AssertFound() *ExecFuture {
	f.op.Decorators.AssertRows = 1
	return f
}

func (f *ExecFuture) RowsAffectedEq(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: n}
}

func (f *ExecFuture) RowsAffectedGt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: n}
}

func (f *ExecFuture) RowsAffectedGe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGe, Op: f.op, Threshold: n}
}

func (f *ExecFuture) RowsAffectedLt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLt, Op: f.op, Threshold: n}
}

func (f *ExecFuture) RowsAffectedLe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLe, Op: f.op, Threshold: n}
}

func (f *ExecFuture) Found() PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: 0}
}

func (f *ExecFuture) NotFound() PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: 0}
}

// Result returns the sql.Result of the operation after the pipeline has flushed.
func (f *ExecFuture) Result() (sql.Result, error) {
	if f.skipped {
		return nil, ErrBranchNotTaken
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.res != nil {
		return f.res, nil
	}
	return BatchExecResult{RowsAffectedVal: f.op.RowCount}, nil
}

// RowsAffected returns the number of rows affected by the operation after the pipeline has flushed.
func (f *ExecFuture) RowsAffected() (int64, error) {
	if f.skipped {
		return 0, ErrBranchNotTaken
	}
	if f.err != nil {
		return 0, f.err
	}
	return f.op.RowCount, nil
}

// Skipped reports whether this operation was skipped due to an untaken If/Else branch.
func (f *ExecFuture) Skipped() bool {
	return f.skipped
}

// RowFuture represents the pending result of a single-row ("one") statement queued in a Pipeline.
type RowFuture[T any] struct {
	op      *PipelineOp
	val     T
	found   bool
	skipped bool
	err     error
}

// AssertFound requires the query to return 1 row, rolling back with ErrAssertionFailed otherwise.
func (f *RowFuture[T]) AssertFound() *RowFuture[T] {
	f.op.Decorators.AssertRows = 1
	return f
}

// AssertRowsAffected requires the query to return n rows (0 or 1).
func (f *RowFuture[T]) AssertRowsAffected(n int64) *RowFuture[T] {
	f.op.Decorators.AssertRows = n
	return f
}

func (f *RowFuture[T]) RowsAffectedEq(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) RowsAffectedGt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) RowsAffectedGe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGe, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) RowsAffectedLt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLt, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) RowsAffectedLe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLe, Op: f.op, Threshold: n}
}

func (f *RowFuture[T]) Found() PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: 0}
}

func (f *RowFuture[T]) NotFound() PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: 0}
}

// WhereCol constructs a column-level PipelineCond evaluated both in SQL and Go.
func (f *RowFuture[T]) WhereCol(sqlPred string, goPred func(T) bool) PipelineCond {
	return PipelineCond{
		Kind:    CondColPred,
		Op:      f.op,
		SQLPred: sqlPred,
		EvalGoPred: func() bool {
			return f.found && goPred != nil && goPred(f.val)
		},
	}
}

// Get returns the scanned row after the pipeline has flushed.
func (f *RowFuture[T]) Get() (T, error) {
	var zero T
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

// Value is an alias for Get.
func (f *RowFuture[T]) Value() (T, error) {
	return f.Get()
}

// Skipped reports whether this operation was skipped due to an untaken If/Else branch.
func (f *RowFuture[T]) Skipped() bool {
	return f.skipped
}

// SliceFuture represents the pending result of a multi-row ("many") statement queued in a Pipeline.
type SliceFuture[T any] struct {
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

// AssertRowsAffected requires the query to return exactly n rows.
func (f *SliceFuture[T]) AssertRowsAffected(n int64) *SliceFuture[T] {
	f.op.Decorators.AssertRows = n
	return f
}

func (f *SliceFuture[T]) RowsAffectedEq(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) RowsAffectedGt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) RowsAffectedGe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsGe, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) RowsAffectedLt(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLt, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) RowsAffectedLe(n int64) PipelineCond {
	return PipelineCond{Kind: CondRowsLe, Op: f.op, Threshold: n}
}

func (f *SliceFuture[T]) Found() PipelineCond {
	return PipelineCond{Kind: CondRowsGt, Op: f.op, Threshold: 0}
}

func (f *SliceFuture[T]) NotFound() PipelineCond {
	return PipelineCond{Kind: CondRowsEq, Op: f.op, Threshold: 0}
}

// WhereCol constructs a column-level PipelineCond requiring at least 1 row and all rows to satisfy pred.
func (f *SliceFuture[T]) WhereCol(sqlPred string, goPred func(T) bool) PipelineCond {
	return PipelineCond{
		Kind:    CondColPred,
		Op:      f.op,
		SQLPred: sqlPred,
		EvalGoPred: func() bool {
			if len(f.vals) == 0 || goPred == nil {
				return false
			}
			for i := range f.vals {
				if !goPred(f.vals[i]) {
					return false
				}
			}
			return true
		},
	}
}

// Get returns the scanned slice of rows after the pipeline has flushed.
func (f *SliceFuture[T]) Get() ([]T, error) {
	if f.skipped {
		return nil, ErrBranchNotTaken
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.vals, nil
}

// Values is an alias for Get.
func (f *SliceFuture[T]) Values() ([]T, error) {
	return f.Get()
}

// Skipped reports whether this operation was skipped due to an untaken If/Else branch.
func (f *SliceFuture[T]) Skipped() bool {
	return f.skipped
}

// IfResult reports whether the "then" branch of a Pipeline.If block was taken.
type IfResult struct {
	condID  int
	cond    PipelineCond
	gates   []BranchGate
	taken   bool
	skipped bool
}

// Taken reports whether the condition evaluated to true (and any enclosing If branch was also active).
func (r *IfResult) Taken() bool {
	if r == nil || r.skipped {
		return false
	}
	return r.taken
}

// Skipped reports whether this If block was inside an outer untaken branch.
func (r *IfResult) Skipped() bool {
	if r == nil {
		return true
	}
	return r.skipped
}

// WirePipeliner is implemented by driver connections capable of executing a batch of
// PipelineOp statements in a single network round-trip.
type WirePipeliner interface {
	ExecPipeline(ctx context.Context, ops []PipelineOp) error
}

type pipelineCtxKey struct{}

type pipelineCtxPayload struct {
	dialect Dialect
	cache   *StmtCache
	ops     []PipelineOp
	handled bool
}

// WithPipelineOps attaches ops to ctx for driver-level WirePipeliner interception.
func WithPipelineOps(ctx context.Context, ops []PipelineOp) context.Context {
	return context.WithValue(ctx, pipelineCtxKey{}, &pipelineCtxPayload{ops: ops})
}

func withPipelinePayload(ctx context.Context, payload *pipelineCtxPayload) context.Context {
	return context.WithValue(ctx, pipelineCtxKey{}, payload)
}

// PipelineOpsFromContext extracts a queued []PipelineOp slice from ctx.
func PipelineOpsFromContext(ctx context.Context) ([]PipelineOp, bool) {
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
// driver.Conn implements WirePipeliner.
func WrapConnector(base driver.Connector) driver.Connector {
	return &wrappedConnector{
		base: base,
		drv:  &wrappedDriver{base: base.Driver()},
	}
}

type pipelineConnWrapper struct {
	driver.Conn
}

func (w *pipelineConnWrapper) ExecPipeline(ctx context.Context, ops []PipelineOp) error {
	if wp, ok := w.Conn.(WirePipeliner); ok {
		return wp.ExecPipeline(ctx, ops)
	}
	return driver.ErrSkip
}

func (w *pipelineConnWrapper) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if query == "--sqlgen:pipeline" {
		if payload, ok := ctx.Value(pipelineCtxKey{}).(*pipelineCtxPayload); ok && payload != nil {
			if wp, ok := w.Conn.(WirePipeliner); ok {
				payload.handled = true
				RenderPipelineBatch(payload.dialect, payload.cache, payload.ops)
				if err := wp.ExecPipeline(ctx, payload.ops); err != nil {
					return nil, err
				}
				return driver.RowsAffected(1), nil
			}
			// Underlying connection does not support WirePipeliner; leave payload.handled = false
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

// TranslateWireError maps server-side integer-overflow sentinel errors (SQLSTATE 22003)
// back to op.AbortErr (for IsAbort operations) or ErrAssertionFailed (for AssertRows operations).
func TranslateWireError(op *PipelineOp, err error) error {
	if err == nil || op == nil {
		return err
	}
	msg := strings.ToLower(err.Error())
	isOverflow := strings.Contains(msg, "22003") ||
		strings.Contains(msg, "integer overflow") ||
		strings.Contains(msg, "bigint out of range") ||
		strings.Contains(msg, "out of range")
	if st, ok := err.(interface{ SQLState() string }); ok && st.SQLState() == "22003" {
		isOverflow = true
	}
	if isOverflow {
		if op.Decorators.IsAbort && op.AbortErr != nil {
			op.Err = op.AbortErr
			return op.AbortErr
		}
		if op.Decorators.AssertRows >= 0 {
			op.Err = ErrAssertionFailed
			return ErrAssertionFailed
		}
	}
	op.Err = err
	return err
}

// Pipeline queues SQL operations for single-roundtrip wire execution or sequential fallback.
type Pipeline struct {
	ctx         context.Context
	db          *sql.DB
	cache       *StmtCache
	txCache     *TxStmtCache
	usePrepared bool
	dialect     Dialect
	ops         []PipelineOp
	opPtrs      []*PipelineOp
	branchStack []BranchGate
	ifResults   []*IfResult
	condSeq     int
	buildErr    error
	flushed     bool
}

// NewPipeline creates a new Pipeline.
func NewPipeline(ctx context.Context, db *sql.DB, cache *StmtCache, txCache *TxStmtCache, usePrepared bool, dialect Dialect) *Pipeline {
	return &Pipeline{
		ctx:         ctx,
		db:          db,
		cache:       cache,
		txCache:     txCache,
		usePrepared: usePrepared,
		dialect:     dialect,
		ops:         make([]PipelineOp, 0, 8),
		opPtrs:      make([]*PipelineOp, 0, 8),
	}
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
	fut := &ExecFuture{}
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
	scanRowFn func(*sql.Row, *T) error,
	scanRowsFn func(*sql.Rows, *T) error,
) *RowFuture[T] {
	if expandErr != nil {
		p.SetBuildError(expandErr)
	}
	in.BranchGates = p.cloneGates()
	fut := &RowFuture[T]{}
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
			for rows.Next() {
				var item T
				if err := scanRowsFn(rows, &item); err != nil {
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
			shapeTag := ComputeBatchShapeTag(op.Input.ColMask, op.Input.FlagMask)
			row, ms, err := pl.cache.QueryRow(ctx, pl.db, pl.txCache, pl.usePrepared, op.Key, op.StmtID, shapeTag, 0, op.FastSlot, pl.dialect, op.Desc, &op.Input, op.Args...)
			if err != nil {
				fut.err = err
				op.Err = err
				return err
			}
			var item T
			scanErr := scanRowFn(row, &item)
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
	scanRowsFn func(*sql.Rows, *T) error,
) *SliceFuture[T] {
	if expandErr != nil {
		p.SetBuildError(expandErr)
	}
	in.BranchGates = p.cloneGates()
	fut := &SliceFuture[T]{}
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
			for rows.Next() {
				var item T
				if err := scanRowsFn(rows, &item); err != nil {
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
			shapeTag := ComputeBatchShapeTag(op.Input.ColMask, op.Input.FlagMask)
			rows, ms, err := pl.cache.Query(ctx, pl.db, pl.txCache, pl.usePrepared, op.Key, op.StmtID, shapeTag, 0, op.FastSlot, pl.dialect, op.Desc, &op.Input, op.Args...)
			if err != nil {
				fut.err = err
				op.Err = err
				return err
			}
			var count int64
			for rows.Next() {
				var item T
				if err := scanRowsFn(rows, &item); err != nil {
					CloseRows(rows, ms)
					fut.err = err
					op.Err = err
					return err
				}
				fut.vals = append(fut.vals, item)
				count++
			}
			err = rows.Err()
			CloseRows(rows, ms)
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

// If queues conditional thenFn and optional elseFn branches gated on cond.
func (p *Pipeline) If(cond PipelineCond, thenFn func(), elseFn func()) *IfResult {
	p.condSeq++
	condID := p.condSeq
	if rootOp := cond.Op; rootOp != nil {
		cCopy := cond
		if rootOp.Decorators.ProduceCond == nil {
			rootOp.Decorators.ProduceCond = &cCopy
			rootOp.Decorators.CondID = condID
		} else {
			rootOp.ExtraConds = append(rootOp.ExtraConds, ProducedCond{
				CondID: condID,
				Cond:   &cCopy,
			})
		}
	}
	res := &IfResult{
		condID: condID,
		cond:   cond,
		gates:  p.cloneGates(),
	}
	p.ifResults = append(p.ifResults, res)

	cCopy := cond
	if thenFn != nil {
		p.branchStack = append(p.branchStack, BranchGate{
			CondID:   condID,
			WantTrue: true,
			Cond:     &cCopy,
		})
		thenFn()
		p.branchStack = p.branchStack[:len(p.branchStack)-1]
	}
	if elseFn != nil {
		p.branchStack = append(p.branchStack, BranchGate{
			CondID:   condID,
			WantTrue: false,
			Cond:     &cCopy,
		})
		elseFn()
		p.branchStack = p.branchStack[:len(p.branchStack)-1]
	}
	return res
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
		if !wireMode && op.ExecSeq != nil {
			op.Input.BranchGates = nil
			if err := op.ExecSeq(ctx, p, op); err != nil {
				*p.opPtrs[i] = *op
				return err
			}
		}
		*p.opPtrs[i] = *op
		if op.Decorators.AssertRows >= 0 && op.RowCount != op.Decorators.AssertRows {
			op.Err = ErrAssertionFailed
			*p.opPtrs[i] = *op
			if op.OnDone != nil {
				op.OnDone(op.RowCount, ErrAssertionFailed)
			}
			return ErrAssertionFailed
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
			condVals[ec.CondID] = ec.Cond.EvalGoFn()
		}
		*p.opPtrs[i] = *op
		if op.OnDone != nil {
			op.OnDone(op.RowCount, op.Err)
		}
	}
	for _, r := range p.ifResults {
		if !evalGatesGo(r.gates, condVals) {
			r.skipped = true
			r.taken = false
			continue
		}
		if v, ok := condVals[r.condID]; ok {
			r.taken = v
		} else {
			r.taken = r.cond.EvalGoFn()
		}
	}
	return nil
}

// Flush executes all queued operations in the pipeline.
func (p *Pipeline) Flush() error {
	if p.flushed {
		return nil
	}
	p.flushed = true
	p.syncOpsFromPtrs()
	defer p.releaseArgBufs()

	if p.buildErr != nil {
		return p.buildErr
	}
	if len(p.ops) == 0 {
		return nil
	}

	// Case 1: Already bound to an explicit transaction (*TxStmtCache).
	if p.txCache != nil {
		if p.db != nil {
			if _, isWrapped := p.db.Driver().(*wrappedDriver); isWrapped {
				payload := &pipelineCtxPayload{
					dialect: p.dialect,
					cache:   p.cache,
					ops:     p.ops,
				}
				_, err := p.txCache.tx.ExecContext(withPipelinePayload(p.ctx, payload), "--sqlgen:pipeline")
				if payload.handled {
					if err != nil {
						return err
					}
					return p.executePipelineOps(p.ctx, true)
				}
			}
		}
		return p.executePipelineOps(p.ctx, false)
	}

	// Case 2: Standalone Pipeline on *sql.DB.
	// Try single-roundtrip WirePipeliner via conn.Raw first when MaxOpenConnections != 1.
	if p.db != nil && p.db.Stats().MaxOpenConnections != 1 {
		conn, err := p.db.Conn(p.ctx)
		if err == nil {
			var wireSupported bool
			var wireErr error
			rawErr := conn.Raw(func(driverConn any) error {
				wp, ok := driverConn.(WirePipeliner)
				if !ok {
					return nil
				}
				wireSupported = true
				RenderPipelineBatch(p.dialect, p.cache, p.ops)
				wireErr = wp.ExecPipeline(p.ctx, p.ops)
				return nil
			})
			_ = conn.Close()
			if rawErr == nil && wireSupported {
				if wireErr != nil {
					return wireErr
				}
				return p.executePipelineOps(p.ctx, true)
			}
		}
	}

	// Sequential fallback inside an implicit transaction.
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		return err
	}
	tc := NewTxStmtCache(tx)
	defer func() { _ = tc.Close() }()
	p.txCache = tc
	if err := p.executePipelineOps(p.ctx, false); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Exec is an alias for Flush.
func (p *Pipeline) Exec() error {
	return p.Flush()
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

// TxConfig holds transaction execution options.
type TxConfig struct {
	RetryPolicy TxRetryPolicy
}

// TxOption configures InTx and PipelineTx execution.
type TxOption func(*TxConfig)

// WithTxRetry enables automatic retries with exponential backoff and full jitter according to policy.
func WithTxRetry(policy TxRetryPolicy) TxOption {
	return func(cfg *TxConfig) {
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
		if code == 1213 || code == 1205 {
			return true
		}
		if lowByte := code & 0xFF; lowByte == 5 || lowByte == 6 { // SQLITE_BUSY / SQLITE_LOCKED (+ extended codes)
			return true
		}
	}
	var num interface{ Number() uint16 }
	if errors.As(err, &num) {
		if n := num.Number(); n == 1213 || n == 1205 {
			return true
		}
	}

	// Drivers that expose codes as struct fields rather than methods (go-sql-driver/mysql's
	// MySQLError.Number / .SQLState) are recognised by their stable message text.
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "40001") ||
		strings.Contains(msg, "40p01") ||
		strings.Contains(msg, "55p03") ||
		strings.Contains(msg, "deadlock") ||
		strings.Contains(msg, "serialization failure") ||
		strings.Contains(msg, "could not serialize") ||
		strings.Contains(msg, "lock wait timeout") ||
		strings.Contains(msg, "error 1213") ||
		strings.Contains(msg, "error 1205") ||
		strings.Contains(msg, "sqlite_busy") ||
		strings.Contains(msg, "sqlite_locked") ||
		strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") {
		return true
	}
	return false
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
	var cfg TxConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	policy := cfg.RetryPolicy

	for attempt := 0; ; attempt++ {
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
		if err == nil {
			return nil
		}
		if attempt >= policy.MaxRetries || !IsRetryableTxError(err, duringCommit) {
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
		shift := attempt
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

type wireRowsCtxKey struct{}

type wireRowsConnector struct{}

func (wireRowsConnector) Connect(context.Context) (driver.Conn, error) {
	return wireRowsConn{}, nil
}

func (wireRowsConnector) Driver() driver.Driver { return nil }

type wireRowsConn struct{}

func (wireRowsConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (wireRowsConn) Close() error                        { return nil }
func (wireRowsConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (wireRowsConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	r, ok := ctx.Value(wireRowsCtxKey{}).(driver.Rows)
	if !ok || r == nil {
		return nil, driver.ErrSkip
	}
	return r, nil
}

var wireRowsDB = sql.OpenDB(wireRowsConnector{})

// RowsFromDriverRows wraps a low-level driver.Rows in a standard *sql.Rows for WirePipeliner implementations.
func RowsFromDriverRows(ctx context.Context, dRows driver.Rows) (*sql.Rows, error) {
	return wireRowsDB.QueryContext(context.WithValue(ctx, wireRowsCtxKey{}, dRows), "wire")
}
