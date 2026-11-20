package runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"iter"
	"math/rand/v2"
	"net"
	"path/filepath"
	"regexp"
	stdruntime "runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// ============================================================================
// 1. AST & Static Invariant Gate (Section 12.1.1)
// ============================================================================

func TestSingleStringsBuilderASTAudit(t *testing.T) {
	filesToAudit := []string{
		"runtime.go",
		"sql_stream.go",
		"cache.go",
		"pipeline.go",
		"../generator.go",
		"../examples/fintech_ledger/ledger_gen.go",
		"../examples/saas_multitenant/portal_gen.go",
		"../examples/ecommerce_inventory/inventory_gen.go",
	}

	fset := token.NewFileSet()
	builderCountByFunc := make(map[string]int)

	for _, relPath := range filesToAudit {
		p := filepath.Clean(relPath)
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}

		// Law 2: reflect must NEVER be imported in runtime, generator, or generated files
		for _, imp := range f.Imports {
			if strings.Trim(imp.Path.Value, `"`) == "reflect" {
				t.Fatalf("file %s imports forbidden package \"reflect\"", p)
			}
		}

		isSQLStream := filepath.Base(p) == "sql_stream.go"

		for _, decl := range f.Decls {
			fnDecl, ok := decl.(*ast.FuncDecl)
			if !ok || fnDecl.Body == nil {
				continue
			}
			fnName := fnDecl.Name.Name

			ast.Inspect(fnDecl.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.SelectorExpr:
					if ident, ok := node.X.(*ast.Ident); ok {
						if isSQLStream {
							if ident.Name == "fmt" && node.Sel.Name == "Sprintf" {
								t.Fatalf("sql_stream.go (%s) calls forbidden fmt.Sprintf", fnName)
							}
							if ident.Name == "strings" && node.Sel.Name == "Join" {
								t.Fatalf("sql_stream.go (%s) calls forbidden strings.Join", fnName)
							}
							if ident.Name == "bytes" && node.Sel.Name == "Buffer" {
								t.Fatalf("sql_stream.go (%s) references forbidden bytes.Buffer", fnName)
							}
						}
					}
				case *ast.ValueSpec:
					if sel, ok := node.Type.(*ast.SelectorExpr); ok {
						if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "strings" && sel.Sel.Name == "Builder" {
							builderCountByFunc[fnName] += len(node.Names)
						}
					}
				case *ast.CompositeLit:
					if sel, ok := node.Type.(*ast.SelectorExpr); ok {
						if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "strings" && sel.Sel.Name == "Builder" {
							builderCountByFunc[fnName]++
						}
					}
				}
				return true
			})
		}
	}

	if builderCountByFunc["BuildComposedSQL"] != 1 {
		t.Fatalf("expected exactly 1 strings.Builder in BuildComposedSQL, got %d", builderCountByFunc["BuildComposedSQL"])
	}
	if builderCountByFunc["RenderPipelineBatch"] != 1 {
		t.Fatalf("expected exactly 1 strings.Builder in RenderPipelineBatch, got %d", builderCountByFunc["RenderPipelineBatch"])
	}
	for fn, cnt := range builderCountByFunc {
		if fn != "BuildComposedSQL" && fn != "RenderPipelineBatch" && cnt > 0 {
			t.Fatalf("unexpected strings.Builder (%d) in function %s", cnt, fn)
		}
	}
}

// ============================================================================
// 2. Deterministic Fault-Injecting & Handle-Counting Driver (mockdriver)
// ============================================================================

type mockError struct {
	msg      string
	sqlState string
	errCode  int
}

func (e *mockError) Error() string    { return e.msg }
func (e *mockError) SQLState() string { return e.sqlState }
func (e *mockError) Code() int        { return e.errCode }

type mockNetError struct {
	msg string
}

func (e *mockNetError) Error() string   { return e.msg }
func (e *mockNetError) Timeout() bool   { return true }
func (e *mockNetError) Temporary() bool { return true }

var _ net.Error = (*mockNetError)(nil)

type mockDriverConfig struct {
	mu             sync.Mutex
	openStmts      atomic.Int64
	prepareCalls   atomic.Int64
	beginCalls     atomic.Int64
	commitCalls    atomic.Int64
	rollbackCalls  atomic.Int64
	execCalls      atomic.Int64
	queryCalls     atomic.Int64
	wireCalls      atomic.Int64
	prepareDelay   time.Duration
	prepareHook    func(query string)                           // optional; runs inside PrepareContext before fault injection
	stmtExecHook   func(query string, args []driver.NamedValue) // optional; runs on every statement-level ExecContext
	stmtExecSQL    []string                                     // SQL of every statement-level ExecContext, in order (guarded by mu)
	failPrepare    error
	failBegin      error
	failExec       error
	failQuery      error
	failRowsNextAt int // 1-based row index to fail on
	failRowsNext   error
	failRowsClose  error
	failStmtClose  error
	failCommit     error
	failCommitOnce bool
	enableWire     bool
	wireHandler    func(ctx context.Context, ops []PipelineOp) error
	enableWireTx   bool // connections also implement WireTxPipeliner
	wireTxCalls    atomic.Int64
	wireTxHandler  func(ctx context.Context, txOpts *sql.TxOptions, ops []PipelineOp) error
	rowsData       [][]driver.Value
	rowsCols       []string
	lastSQL        string
}

type mockConnector struct {
	cfg *mockDriverConfig
}

func (c *mockConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.cfg.enableWireTx {
		return &mockWireTxConn{mockWireConn: mockWireConn{mockConn: mockConn{cfg: c.cfg}}}, nil
	}
	if c.cfg.enableWire {
		return &mockWireConn{mockConn: mockConn{cfg: c.cfg}}, nil
	}
	return &mockConn{cfg: c.cfg}, nil
}

func (c *mockConnector) Driver() driver.Driver {
	return &mockDriver{cfg: c.cfg}
}

type mockDriver struct {
	cfg *mockDriverConfig
}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	return (&mockConnector{cfg: d.cfg}).Connect(context.Background())
}

type mockConn struct {
	cfg *mockDriverConfig
}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *mockConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.cfg.prepareDelay > 0 {
		time.Sleep(c.cfg.prepareDelay)
	}
	c.cfg.prepareCalls.Add(1)
	if hook := c.cfg.prepareHook; hook != nil {
		hook(query)
	}
	c.cfg.mu.Lock()
	err := c.cfg.failPrepare
	c.cfg.lastSQL = query
	c.cfg.mu.Unlock()
	if err != nil {
		return nil, err
	}
	c.cfg.openStmts.Add(1)
	return &mockStmt{cfg: c.cfg, query: query}, nil
}

func (c *mockConn) Close() error { return nil }

func (c *mockConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *mockConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.cfg.beginCalls.Add(1)
	c.cfg.mu.Lock()
	err := c.cfg.failBegin
	c.cfg.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &mockTx{cfg: c.cfg}, nil
}

func (c *mockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.cfg.execCalls.Add(1)
	c.cfg.mu.Lock()
	err := c.cfg.failExec
	c.cfg.lastSQL = query
	c.cfg.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

func (c *mockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.cfg.queryCalls.Add(1)
	c.cfg.mu.Lock()
	err := c.cfg.failQuery
	c.cfg.lastSQL = query
	cols := c.cfg.rowsCols
	data := c.cfg.rowsData
	c.cfg.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		cols = []string{"id"}
	}
	return &mockRows{cfg: c.cfg, cols: cols, data: data}, nil
}

type mockWireConn struct {
	mockConn
}

func (w *mockWireConn) ExecPipeline(ctx context.Context, ops []PipelineOp) error {
	w.cfg.wireCalls.Add(1)
	if w.cfg.wireHandler != nil {
		return w.cfg.wireHandler(ctx, ops)
	}
	for i := range ops {
		op := &ops[i]
		op.RowCount = 1
		if op.Decorators.ProduceCond != nil {
			op.CondVal = true
			op.CondSet = true
		}
		if op.OnDone != nil {
			op.OnDone(op.RowCount, nil)
		}
	}
	return nil
}

type mockStmt struct {
	cfg    *mockDriverConfig
	query  string
	closed atomic.Bool
}

func (s *mockStmt) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		s.cfg.openStmts.Add(-1)
	}
	s.cfg.mu.Lock()
	err := s.cfg.failStmtClose
	s.cfg.mu.Unlock()
	return err
}

func (s *mockStmt) NumInput() int { return -1 }

func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (s *mockStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.cfg.execCalls.Add(1)
	s.cfg.mu.Lock()
	err := s.cfg.failExec
	s.cfg.stmtExecSQL = append(s.cfg.stmtExecSQL, s.query)
	hook := s.cfg.stmtExecHook
	s.cfg.mu.Unlock()
	if hook != nil {
		hook(s.query, args)
	}
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), nil)
}

func (s *mockStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.cfg.queryCalls.Add(1)
	s.cfg.mu.Lock()
	err := s.cfg.failQuery
	cols := s.cfg.rowsCols
	data := s.cfg.rowsData
	s.cfg.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		cols = []string{"id"}
	}
	return &mockRows{cfg: s.cfg, cols: cols, data: data}, nil
}

type mockTx struct {
	cfg *mockDriverConfig
}

func (tx *mockTx) Commit() error {
	tx.cfg.commitCalls.Add(1)
	tx.cfg.mu.Lock()
	err := tx.cfg.failCommit
	if tx.cfg.failCommitOnce {
		tx.cfg.failCommit = nil
		tx.cfg.failCommitOnce = false
	}
	tx.cfg.mu.Unlock()
	return err
}

func (tx *mockTx) Rollback() error {
	tx.cfg.rollbackCalls.Add(1)
	return nil
}

type mockRows struct {
	cfg  *mockDriverConfig
	cols []string
	data [][]driver.Value
	idx  int
}

func (r *mockRows) Columns() []string { return r.cols }

func (r *mockRows) Close() error {
	r.cfg.mu.Lock()
	err := r.cfg.failRowsClose
	r.cfg.mu.Unlock()
	return err
}

func (r *mockRows) Next(dest []driver.Value) error {
	r.idx++
	r.cfg.mu.Lock()
	failAt := r.cfg.failRowsNextAt
	failErr := r.cfg.failRowsNext
	r.cfg.mu.Unlock()
	if failAt > 0 && r.idx == failAt {
		return failErr
	}
	if r.idx > len(r.data) {
		return io.EOF
	}
	row := r.data[r.idx-1]
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
		}
	}
	return nil
}

func newMockDB(t testing.TB, cfg *mockDriverConfig) *sql.DB {
	t.Helper()
	var c driver.Connector = &mockConnector{cfg: cfg}
	if cfg.enableWire || cfg.enableWireTx {
		c = WrapConnector(c)
	}
	db := sql.OpenDB(c)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

//go:noinline
func orphanTxStmtCache(t *testing.T, ctx context.Context, db *sql.DB, cache *StmtCache, vk VariantKey, desc *QueryDescriptor, in *RenderInput) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("orphanTxStmtCache BeginTx: %v", err)
	}
	tc := NewTxStmtCache(tx)
	r, m, err := cache.Query(ctx, db, tc, true, vk, 1, 0, 0, nil, DialectQuestion, desc, in, 1)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("orphanTxStmtCache Query: %v", err)
	}
	CloseRows(r, m)
	_ = tx.Rollback()
	tc = nil
}

func TestMockDriverFaultInjectionAndHandleAccounting(t *testing.T) {
	cfg := &mockDriverConfig{
		rowsCols: []string{"id"},
		rowsData: [][]driver.Value{{int64(10)}, {int64(20)}, {int64(30)}},
	}
	db := newMockDB(t, cfg)
	cache := NewStmtCache(
		WithMaxPreparedStmts(1),
		WithMaxSQLVariants(16),
		WithPromoteThreshold(1),
	)

	ctx := context.Background()
	desc := &QueryDescriptor{
		StmtID:    1,
		StmtVerb:  VerbSelect,
		StaticSQL: "SELECT id FROM items WHERE id = ?",
		Segments:  []string{"SELECT id FROM items WHERE id = ", ""},
		Slots:     []Slot{{Kind: SlotScalar}},
	}
	in := RenderInput{}

	// 1. Promote vk1 into Tier 2 -> OpenStmts == 1
	vk1 := MakeVariantKey(1, 0, 0, 0, 1)
	rows, ms1, err := cache.Query(ctx, db, nil, true, vk1, 1, 0, 0, nil, DialectQuestion, desc, &in, 1)
	if err != nil {
		t.Fatalf("Query vk1: %v", err)
	}
	CloseRows(rows, ms1)
	if got := cfg.openStmts.Load(); got != 1 {
		t.Fatalf("expected 1 open stmt, got %d", got)
	}

	// 2. Mid-stream Rows.Next fault injection
	cfg.mu.Lock()
	cfg.failRowsNextAt = 2
	cfg.failRowsNext = errors.New("disk read fault mid-stream")
	cfg.mu.Unlock()

	rows2, ms2, err := cache.Query(ctx, db, nil, true, vk1, 1, 0, 0, nil, DialectQuestion, desc, &in, 1)
	if err != nil {
		t.Fatalf("Query vk1 (2): %v", err)
	}
	var scanned []int64
	for rows2.Next() {
		var v int64
		if err := rows2.Scan(&v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		scanned = append(scanned, v)
	}
	if rows2.Err() == nil || !strings.Contains(rows2.Err().Error(), "disk read fault mid-stream") {
		t.Fatalf("expected mid-stream error, got %v", rows2.Err())
	}
	CloseRows(rows2, ms2)

	cfg.mu.Lock()
	cfg.failRowsNextAt = 0
	cfg.failRowsNext = nil
	cfg.mu.Unlock()

	// 3. PrepareContext fault injection on promotion returns the Prepare error cleanly and leaves OpenStmts intact
	cfg.mu.Lock()
	cfg.failPrepare = errors.New("server out of prepared statements")
	cfg.mu.Unlock()

	for cand := uint64(10); cand < 200; cand++ {
		vkFailPrep := MakeVariantKey(2, 0, 0, 0, cand)
		if (vkFailPrep.Hash() & 15) != (vk1.Hash() & 15) {
			desc2 := &QueryDescriptor{
				StmtID:   2,
				StmtVerb: VerbUpdate,
				Segments: []string{"UPDATE items SET score = ", " WHERE id = ", ""},
				Slots:    []Slot{{Kind: SlotScalar}, {Kind: SlotScalar}},
			}
			if _, err := cache.Exec(ctx, db, nil, true, vkFailPrep, 2, 0, 0, nil, DialectQuestion, desc2, &in, 100, 1); err == nil || !strings.Contains(err.Error(), "server out of prepared statements") {
				t.Fatalf("expected PrepareContext failure error, got %v", err)
			}
			break
		}
	}
	cfg.mu.Lock()
	cfg.failPrepare = nil
	cfg.mu.Unlock()

	// 4. Orphaned TxStmtCache GC cleanup + Tier-2 demotion -> OpenStmts must return to 1 (for the new stmt) and then 0 on Close()
	orphanTxStmtCache(t, ctx, db, cache, vk1, desc, &in)

	// Promote another statement in the same shard as vk1 with higher frequency to demote vk1
	for cand := uint64(2); cand < 200; cand++ {
		vk2 := MakeVariantKey(1, 0, 0, 0, cand)
		if (vk2.Hash() & 15) == (vk1.Hash() & 15) {
			descVk2 := &QueryDescriptor{
				StmtID:   1,
				StmtVerb: VerbSelect,
				Segments: []string{"SELECT id FROM items WHERE id = ", " /* vk2 */"},
				Slots:    []Slot{{Kind: SlotScalar}},
			}
			for range 6 {
				r, m, err := cache.Query(ctx, db, nil, true, vk2, 1, 0, 0, nil, DialectQuestion, descVk2, &in, 1)
				if err != nil {
					t.Fatalf("Query vk2: %v", err)
				}
				CloseRows(r, m)
			}
			break
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("cache.Close: %v", err)
	}
	// Trigger GC so the orphaned TxStmtCache releases vk1's ManagedStmt
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stdruntime.GC()
		if cfg.openStmts.Load() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if open := cfg.openStmts.Load(); open != 0 {
		for sIdx := range cache.shards {
			sh := &cache.shards[sIdx]
			sh.mu.Lock()
			t.Logf("shard %d: prepProb=%d prepProt=%d byKey=%d", sIdx, sh.preparedProb.len, sh.preparedProt.len, len(sh.byKey))
			sh.mu.Unlock()
		}
		t.Fatalf("expected OpenStmts == 0 after cache.Close(), got %d", open)
	}
}

func TestSingleflightThunderingHerdPromotion(t *testing.T) {
	cfg := &mockDriverConfig{
		prepareDelay: 25 * time.Millisecond,
		rowsCols:     []string{"id"},
		rowsData:     [][]driver.Value{{int64(1)}},
	}
	db := newMockDB(t, cfg)
	cache := NewStmtCache(WithPromoteThreshold(2))
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	vk := MakeVariantKey(7, 0, 0, 0, 1)
	desc := &QueryDescriptor{
		StmtID:    7,
		StmtVerb:  VerbSelect,
		StaticSQL: "SELECT id FROM items WHERE id = ?",
		Segments:  []string{"SELECT id FROM items WHERE id = ", ""},
		Slots:     []Slot{{Kind: SlotScalar}},
	}
	in := RenderInput{}

	// Warm once into Tier 1 (freq=1 < threshold=2) so all 64 concurrent callers hit Tier 1 simultaneously and trigger promotion.
	if _, _, err := cache.Resolve(ctx, db, nil, true, vk, 7, 0, 0, nil, DialectQuestion, desc, &in); err != nil {
		t.Fatalf("warm Tier 1: %v", err)
	}
	if got := cfg.prepareCalls.Load(); got != 0 {
		t.Fatalf("expected 0 PrepareContext calls on Tier 1 admission, got %d", got)
	}

	const goroutines = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ms, _, err := cache.Resolve(ctx, db, nil, true, vk, 7, 0, 0, nil, DialectQuestion, desc, &in)
			if err != nil {
				errCh <- err
				return
			}
			_ = ms.Release()
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent Resolve error: %v", err)
	}

	if got := cfg.prepareCalls.Load(); got != 1 {
		t.Fatalf("expected singleflight to call PrepareContext exactly 1 time across %d goroutines, got %d", goroutines, got)
	}
}

func TestTxRetryPolicyAndPanicRollback(t *testing.T) {
	ctx := context.Background()

	// 1. Test IsRetryableTxError matrix directly
	retryableErrs := []error{
		&mockError{msg: "serialization failure", sqlState: "40001"},
		&mockError{msg: "deadlock detected", sqlState: "40P01"},
		&mockError{msg: "lock not available", sqlState: "55P03"},
		&mockError{msg: "mysql deadlock", errCode: 1213},
		&mockError{msg: "mysql lock wait timeout", errCode: 1205},
		&mockError{msg: "sqlite busy", errCode: 5},
		&mockError{msg: "sqlite locked", errCode: (1 << 8) | 6},
		errors.New("ERROR: could not serialize access due to concurrent update"),
		errors.New("database table is locked"),
	}
	for _, err := range retryableErrs {
		if !IsRetryableTxError(err, false) {
			t.Fatalf("expected %v to be retryable during body", err)
		}
		if !IsRetryableTxError(err, true) {
			t.Fatalf("expected %v to be retryable during commit", err)
		}
	}

	// Non-retryable errors
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, ErrAssertionFailed, errors.New("syntax error")} {
		if IsRetryableTxError(err, false) || IsRetryableTxError(err, true) {
			t.Fatalf("expected %v to be non-retryable", err)
		}
	}

	// Transport/connection drops: retryable during body (duringCommit == false), NEVER retryable during Commit (duringCommit == true)!
	connDropErrs := []error{
		driver.ErrBadConn,
		io.EOF,
		io.ErrUnexpectedEOF,
		&mockNetError{msg: "connection reset by peer"},
	}
	for _, err := range connDropErrs {
		if !IsRetryableTxError(err, false) {
			t.Fatalf("expected transport error %v to be retryable before commit", err)
		}
		if IsRetryableTxError(err, true) {
			t.Fatalf("SAFETY VIOLATION: transport error %v MUST NOT be retried during Commit", err)
		}
	}

	// 2. Test RunTxWithRetry with SQLSTATE 40001 during Commit (must retry and succeed)
	cfg := &mockDriverConfig{
		failCommit:     &mockError{msg: "serialization failure on commit", sqlState: "40001"},
		failCommitOnce: true,
	}
	db := newMockDB(t, cfg)
	policy := TxRetryPolicy{MaxRetries: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}
	attempts := 0
	if err := RunTxWithRetry(ctx, db, nil, nil, []TxOption{WithTxRetry(policy)}, func(tc *TxStmtCache) error {
		attempts++
		return nil
	}); err != nil {
		t.Fatalf("expected retry on 40001 commit failure to succeed, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts for 40001 commit retry, got %d", attempts)
	}

	// 3. Test RunTxWithRetry with driver.ErrBadConn / io.EOF during Commit (must NOT retry)
	cfg.mu.Lock()
	cfg.failCommit = io.EOF
	cfg.failCommitOnce = true
	cfg.mu.Unlock()
	attempts = 0
	if err := RunTxWithRetry(ctx, db, nil, nil, []TxOption{WithTxRetry(policy)}, func(tc *TxStmtCache) error {
		attempts++
		return nil
	}); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF from Commit, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected Commit io.EOF to abort after 1 attempt without retrying, got %d attempts", attempts)
	}

	// 4. Test user closure panic inside RunTxWithRetry rolls back transaction and re-panics
	rollbacksBefore := cfg.rollbackCalls.Load()
	func() {
		defer func() {
			if r := recover(); r != "boom-in-tx" {
				t.Fatalf("expected panic 'boom-in-tx', got %v", r)
			}
		}()
		_ = RunTxWithRetry(ctx, db, nil, nil, nil, func(tc *TxStmtCache) error {
			panic("boom-in-tx")
		})
	}()
	if cfg.rollbackCalls.Load() <= rollbacksBefore {
		t.Fatalf("expected panic inside RunTxWithRetry to call tx.Rollback()")
	}
}

// ============================================================================
// 4. Property-Based Testing Suite (PBT 1 & PBT 4) (Section 12.1.3)
// ============================================================================

func TestPropertyEstimateOpBytesStrictUpperBound(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x123456789abcdef0, 0xfedcba9876543210))
	dialects := []Dialect{DialectQuestion, DialectDollar}

	for iter := 0; iter < 2000; iter++ {
		dialect := dialects[rng.IntN(len(dialects))]
		verb := StmtVerb(rng.IntN(5))

		numCols := rng.IntN(6) + 1
		colExprs := make([]string, numCols)
		for i := range colExprs {
			colExprs[i] = fmt.Sprintf("t.col_%d", i)
		}

		numOrder := rng.IntN(4) + 1
		orderCols := make([]string, numOrder)
		for i := range orderCols {
			orderCols[i] = fmt.Sprintf("t.ord_%d", i)
		}

		tupleDesc := &TupleDescriptor{
			Segments:  []string{"", ", ", ", NOW()"},
			NumFields: 2,
		}

		innerDesc := &QueryDescriptor{
			Segments:  []string{" AND t.flag_val IN (", ") AND t.x = ", ""},
			Slots:     []Slot{{Kind: SlotSlice}, {Kind: SlotScalar}},
			NumSlices: 1,
		}

		subDesc := &QueryDescriptor{
			StmtID:    99,
			StmtVerb:  VerbSelect,
			Segments:  []string{"SELECT id FROM sub WHERE a = ", " AND b IN (", ")"},
			Slots:     []Slot{{Kind: SlotScalar}, {Kind: SlotSlice}},
			NumSlices: 1,
		}

		desc := &QueryDescriptor{
			StmtID:   uint32(iter%100 + 1),
			StmtVerb: verb,
			Segments: []string{"SELECT ", " FROM tbl t WHERE ", "t.id = ", " AND t.s IN (", ") AND t.tup IN (", ")", " AND t.sub IN (", ") AND ", " ORDER BY ", ""},
			Slots: []Slot{
				{Kind: SlotSelectCols},
				{Kind: SlotBranchGuardOpen},
				{Kind: SlotScalar},
				{Kind: SlotSlice},
				{Kind: SlotTupleSlice, Tuple: tupleDesc},
				{Kind: SlotOptBlock, FlagBit: 0, Inner: innerDesc},
				{Kind: SlotQuery},
				{Kind: SlotSeek},
				{Kind: SlotOrder},
				{Kind: SlotBranchGuardClose},
			},
			SelectColExprs: colExprs,
			OrderCols:      orderCols,
			HasReturning:   rng.IntN(2) == 1,
			NumSlices:      2,
			NumTuples:      1,
			NumSubs:        1,
		}

		s1 := rng.IntN(8) + 1
		s2 := rng.IntN(6) + 1
		t1 := rng.IntN(5) + 1
		subSliceLen := rng.IntN(4) + 1

		var orderTerms []uint8
		for i := 0; i < numOrder; i++ {
			if rng.IntN(2) == 0 {
				orderTerms = append(orderTerms, Asc(uint8(i)))
			} else {
				orderTerms = append(orderTerms, Desc(uint8(i)))
			}
		}

		subQ := Query{
			StmtID: 99,
			Desc:   subDesc,
			Input:  RenderInput{SliceLens: []int{subSliceLen}},
		}

		var gates []BranchGate
		if rng.IntN(2) == 1 {
			gates = append(gates, BranchGate{CondID: rng.IntN(10), WantTrue: rng.IntN(2) == 1})
		}
		if rng.IntN(3) == 1 {
			gates = append(gates, BranchGate{CondID: rng.IntN(10) + 10, WantTrue: rng.IntN(2) == 1})
		}

		in := RenderInput{
			ColMask:     uint64(rng.IntN((1<<numCols)-1) + 1),
			FlagMask:    uint64(rng.IntN(2)),
			OrderSpec:   PackOrder(orderTerms...),
			HasSeek:     rng.IntN(2) == 1,
			SliceLens:   []int{s1, s2},
			TupleLens:   []int{t1},
			Subs:        []Query{subQ},
			BranchGates: gates,
		}

		var dec OpDecorators
		dec.AssertRows = -1
		if rng.IntN(3) == 0 {
			dec.AssertRows = int64(rng.IntN(5))
		}
		if rng.IntN(4) == 0 {
			dec.IsAbort = true
		}
		var pCond PipelineCond
		if rng.IntN(2) == 0 {
			pCond = PipelineCond{
				Kind:      CondColPred,
				Threshold: 1,
				SQLPred:   "col_0 > 42",
			}
			dec.ProduceCond = &pCond
			dec.CondID = rng.IntN(8)
		}

		estPlain := EstimateOpBytes(desc, &in, nil)
		renderedPlain := BuildComposedSQL(desc, &in, dialect)
		if len(renderedPlain) > estPlain {
			t.Fatalf("iter %d: EstimateOpBytes (%d) < len(BuildComposedSQL) (%d) for dialect=%v", iter, estPlain, len(renderedPlain), dialect)
		}

		// Also verify pipeline decorated rendering via RenderPipelineBatch
		ops := []PipelineOp{{
			StmtID:        desc.StmtID,
			Desc:          desc,
			Input:         in,
			Decorators:    dec,
			HasResultCols: true,
		}}
		estDec := EstimateOpBytes(desc, &in, &dec)
		RenderPipelineBatch(dialect, nil, ops)
		if len(ops[0].SQL) > estDec {
			t.Fatalf("iter %d: EstimateOpBytes (%d) < len(op.SQL) (%d) for decorated op: %s", iter, estDec, len(ops[0].SQL), ops[0].SQL)
		}
	}

	// Verify Law 1: BuildComposedSQL allocates at most 1 heap object (the returned string)
	sampleDesc := &QueryDescriptor{
		StmtID:    1,
		StmtVerb:  VerbSelect,
		Segments:  []string{"SELECT id, name FROM users WHERE id IN (", ") AND org_id = ", ""},
		Slots:     []Slot{{Kind: SlotSlice}, {Kind: SlotScalar}},
		NumSlices: 1,
	}
	sampleIn := RenderInput{SliceLens: []int{5}}
	allocs := testing.AllocsPerRun(100, func() {
		s := BuildComposedSQL(sampleDesc, &sampleIn, DialectDollar)
		if len(s) == 0 {
			t.Fatal("empty SQL")
		}
	})
	if allocs > 1.0 {
		t.Fatalf("Law 1 violation: BuildComposedSQL allocated %.2f objects/op (want <= 1.0)", allocs)
	}
}

func TestPropertyBatchChunkDecompositionConservation(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xabcdef0123456789, 0x9876543210fedcba))

	for trial := 0; trial < 200; trial++ {
		cache := NewStmtCache(
			WithPromoteThreshold(2),
			WithMaxBatchParams(16384),
		)
		stmtID := uint32(trial + 1)
		colMask := uint64(0x0F)
		flagMask := uint64(0x01)
		scalarParams := rng.IntN(10)
		fieldsPerRow := rng.IntN(6) + 1
		maxRows := cache.MaxRowsPerBatch(DialectQuestion, scalarParams, fieldsPerRow)

		// Randomly promote 1..5 heavy-hitter chunk sizes into preparedSizes
		shapeTag := ComputeBatchShapeTag(colMask, flagMask)
		numPromoted := rng.IntN(5) + 1
		for range numPromoted {
			candSize := rng.IntN(min(maxRows, 1000)) + 1
			cache.recordPreparedBatchSize(stmtID, shapeTag, candSize)
		}

		// Test batch sizes across 1..25,000
		batchSizes := []int{1, 2, 3, 7, 15, 16, 31, 64, 127, 255, 500, 1024, 4096, 10000, 25000}
		for range 10 {
			batchSizes = append(batchSizes, rng.IntN(25000)+1)
		}

		for _, totalRows := range batchSizes {
			sumChunks := 0
			steps := 0
			for offset := 0; offset < totalRows; {
				chunk := cache.PlanBatchChunk(stmtID, colMask, flagMask, totalRows-offset, maxRows, offset == 0)
				if chunk <= 0 || chunk > totalRows-offset {
					t.Fatalf("trial %d totalRows=%d: invalid chunk=%d (remaining=%d)", trial, totalRows, chunk, totalRows-offset)
				}
				if chunk*fieldsPerRow+scalarParams > 16384 && maxRows*fieldsPerRow+scalarParams <= 16384 {
					t.Fatalf("trial %d: chunk=%d exceeded MaxBatchParams (fields=%d, scalar=%d)", trial, chunk, fieldsPerRow, scalarParams)
				}
				sumChunks += chunk
				offset += chunk
				steps++
				if steps > totalRows {
					t.Fatalf("infinite loop in PlanBatchChunk")
				}
			}
			if sumChunks != totalRows {
				t.Fatalf("conservation violation: sum(chunks)=%d != totalRows=%d", sumChunks, totalRows)
			}
		}
		_ = cache.Close()
	}
}

// ============================================================================
// 5. Native Go Fuzzing Suite (Section 12.1.4)
// ============================================================================

func FuzzEstimateOpBytesUpperBound(f *testing.F) {
	f.Add(uint8(0), uint8(1), uint64(0x07), uint64(0x01), uint8(3), uint8(2), true, int64(1), true)
	f.Add(uint8(1), uint8(2), uint64(0x01), uint64(0x00), uint8(1), uint8(4), false, int64(-1), false)

	f.Fuzz(func(t *testing.T, dialectRaw, verbRaw uint8, colMask, flagMask uint64, sliceLenRaw, tupleLenRaw uint8, hasSeek bool, assertRows int64, isAbort bool) {
		dialect := DialectQuestion
		if dialectRaw%2 == 1 {
			dialect = DialectDollar
		}
		verb := StmtVerb(verbRaw % 5)
		sLen := int(sliceLenRaw%16) + 1
		tLen := int(tupleLenRaw%16) + 1

		desc := &QueryDescriptor{
			StmtID:   1,
			StmtVerb: verb,
			Segments: []string{"SELECT ", " FROM tbl WHERE ", "id IN (", ") AND tup IN (", ") AND ", " ORDER BY ", ""},
			Slots: []Slot{
				{Kind: SlotSelectCols},
				{Kind: SlotBranchGuardOpen},
				{Kind: SlotSlice},
				{Kind: SlotTupleSlice, Tuple: &TupleDescriptor{Segments: []string{"", ", ", ""}, NumFields: 2}},
				{Kind: SlotSeek},
				{Kind: SlotOrder},
				{Kind: SlotBranchGuardClose},
			},
			SelectColExprs: []string{"c1", "c2", "c3"},
			OrderCols:      []string{"c1", "c2"},
			NumSlices:      1,
			NumTuples:      1,
		}
		in := RenderInput{
			ColMask:     colMask,
			FlagMask:    flagMask,
			OrderSpec:   PackOrder(Asc(0), Desc(1)),
			HasSeek:     hasSeek,
			SliceLens:   []int{sLen},
			TupleLens:   []int{tLen},
			BranchGates: []BranchGate{{CondID: 0, WantTrue: true}},
		}
		if assertRows < -1 {
			assertRows = -1
		}
		dec := OpDecorators{
			AssertRows: assertRows % 10,
			IsAbort:    isAbort,
		}

		est := EstimateOpBytes(desc, &in, &dec)
		ops := []PipelineOp{{
			StmtID:     1,
			Desc:       desc,
			Input:      in,
			Decorators: dec,
		}}
		RenderPipelineBatch(dialect, nil, ops)
		if len(ops[0].SQL) > est {
			t.Fatalf("EstimateOpBytes (%d) < len(op.SQL) (%d): %q", est, len(ops[0].SQL), ops[0].SQL)
		}
	})
}

func FuzzVariantKeyAndSubqueryShapeBijectivity(f *testing.F) {
	f.Add(uint32(1), uint64(3), uint64(1), uint64(0), uint64(5), uint64(10), uint32(2), uint64(7), uint64(2), uint64(0), uint64(5), uint64(11))
	f.Add(uint32(10), uint64(1), uint64(0), uint64(42), uint64(1), uint64(2), uint32(10), uint64(1), uint64(0), uint64(42), uint64(1), uint64(2))

	f.Fuzz(func(
		t *testing.T,
		id1 uint32, col1, flag1, ord1, d1a, d1b uint64,
		id2 uint32, col2, flag2, ord2, d2a, d2b uint64,
	) {
		k1 := MakeVariantKey(id1, col1, flag1, ord1, d1a, d1b)
		k2 := MakeVariantKey(id2, col2, flag2, ord2, d2a, d2b)
		sameCoords := id1 == id2 && col1 == col2 && flag1 == flag2 && ord1 == ord2 && d1a == d2a && d1b == d2b
		if k1.IsExact() && k2.IsExact() {
			if (k1 == k2) != sameCoords {
				t.Fatalf("VariantKey bijectivity violation: k1=%v k2=%v sameCoords=%v", k1, k2, sameCoords)
			}
		}

		s1 := ComputeSubqueryShape(id1, col1, flag1, ord1, false, int(d1a&0xFFFF), int(d1b&0xFFFF))
		s2 := ComputeSubqueryShape(id2, col2, flag2, ord2, false, int(d2a&0xFFFF), int(d2b&0xFFFF))
		sameSubCoords := id1 == id2 && col1 == col2 && flag1 == flag2 && ord1 == ord2 && (d1a&0xFFFF) == (d2a&0xFFFF) && (d1b&0xFFFF) == (d2b&0xFFFF)
		if (s1&InexactKeyBit) == 0 && (s2&InexactKeyBit) == 0 {
			if (s1 == s2) != sameSubCoords {
				t.Fatalf("ComputeSubqueryShape bijectivity violation: s1=%#x s2=%#x sameSubCoords=%v", s1, s2, sameSubCoords)
			}
		}
	})
}

func verifyCacheInvariants(t testing.TB, c *StmtCache) {
	t.Helper()
	for sIdx := range c.shards {
		sh := &c.shards[sIdx]
		sh.mu.Lock()

		// Walk all 4 doubly-linked lists and verify length & tier tags
		checkList := func(name string, l *entryList, wantTier entryTier) int {
			cnt := 0
			var prev *cacheEntry
			for cur := l.head; cur != nil; cur = cur.next {
				if cur.prev != prev {
					sh.mu.Unlock()
					t.Fatalf("shard %d %s: broken prev pointer", sIdx, name)
				}
				if cur.tier != wantTier {
					sh.mu.Unlock()
					t.Fatalf("shard %d %s: entry tier=%v, want %v", sIdx, name, cur.tier, wantTier)
				}
				prev = cur
				cnt++
			}
			if prev != l.tail {
				sh.mu.Unlock()
				t.Fatalf("shard %d %s: tail mismatch", sIdx, name)
			}
			if cnt != l.len {
				sh.mu.Unlock()
				t.Fatalf("shard %d %s: counted %d != l.len %d", sIdx, name, cnt, l.len)
			}
			return cnt
		}

		pProb := checkList("partialProb", &sh.partialProb, tierPartialProb)
		pProt := checkList("partialProt", &sh.partialProt, tierPartialProt)
		prepProb := checkList("preparedProb", &sh.preparedProb, tierPreparedProb)
		prepProt := checkList("preparedProt", &sh.preparedProt, tierPreparedProt)

		if prepProb+prepProt > sh.maxPrepared {
			sh.mu.Unlock()
			t.Fatalf("shard %d: prepared count %d > maxPrepared %d", sIdx, prepProb+prepProt, sh.maxPrepared)
		}
		if pProb+pProt > sh.maxPartial {
			sh.mu.Unlock()
			t.Fatalf("shard %d: partial count %d > maxPartial %d", sIdx, pProb+pProt, sh.maxPartial)
		}

		var mapPartialCount int
		var mapPreparedCount int
		var sumPartialBytes int
		checkEntry := func(e *cacheEntry) {
			switch e.tier {
			case tierPartialProb, tierPartialProt:
				mapPartialCount++
				sumPartialBytes += len(e.sqlStr)
			case tierPreparedProb, tierPreparedProt:
				mapPreparedCount++
			default:
				sh.mu.Unlock()
				t.Fatalf("shard %d: entry in map has invalid tier %v", sIdx, e.tier)
			}
		}
		for _, e := range sh.byKey {
			checkEntry(e)
		}
		for _, e := range sh.bySQL {
			checkEntry(e)
		}

		if pProb+pProt != mapPartialCount {
			sh.mu.Unlock()
			t.Fatalf("shard %d: partial list len (%d) != map partial count (%d)", sIdx, pProb+pProt, mapPartialCount)
		}
		if prepProb+prepProt != mapPreparedCount {
			sh.mu.Unlock()
			t.Fatalf("shard %d: prepared list len (%d) != map prepared count (%d)", sIdx, prepProb+prepProt, mapPreparedCount)
		}
		if sh.partialBytes != sumPartialBytes {
			sh.mu.Unlock()
			t.Fatalf("shard %d: partialBytes (%d) != sum(len(sqlStr)) (%d)", sIdx, sh.partialBytes, sumPartialBytes)
		}
		if sh.partialBytes > sh.maxPartialBytes {
			sh.mu.Unlock()
			t.Fatalf("shard %d: partialBytes (%d) > maxPartialBytes (%d)", sIdx, sh.partialBytes, sh.maxPartialBytes)
		}

		sh.mu.Unlock()
	}
}

func FuzzStmtCacheAdversarialStateTransitions(f *testing.F) {
	f.Add([]byte{1, 2, 1, 3, 2, 4, 5, 1, 2, 6, 7, 8, 9, 1, 2, 3, 4})
	f.Add([]byte{10, 20, 30, 40, 50, 10, 10, 10, 20, 20, 255, 128, 64})

	f.Fuzz(func(t *testing.T, ops []byte) {
		cfg := &mockDriverConfig{
			rowsCols: []string{"id"},
			rowsData: [][]driver.Value{{int64(1)}},
		}
		db := newMockDB(t, cfg)
		cache := NewStmtCache(
			WithMaxPreparedStmts(32),
			WithMaxSQLVariants(64),
			WithMaxPartialBytes(16*1024),
			WithPromoteThreshold(2),
		)
		defer func() { _ = cache.Close() }()

		ctx := context.Background()
		for i, b := range ops {
			stmtID := uint32((b % 8) + 1)
			dim := uint64(b % 16)
			inexact := (b & 0x80) != 0
			usePrep := (b & 0x40) == 0

			var vk VariantKey
			if inexact {
				vk = MakeVariantKey(0, 0, 0, 0, dim)
			} else {
				vk = MakeVariantKey(stmtID, 0, 0, 0, dim)
			}
			padding := strings.Repeat(" ", int(b%32))
			sqlText := fmt.Sprintf("SELECT id FROM items WHERE id = ? /* %d_%d_%s */", stmtID, dim, padding)
			desc := &QueryDescriptor{
				StmtID:    stmtID,
				StmtVerb:  VerbSelect,
				StaticSQL: sqlText,
				Segments:  []string{sqlText},
			}
			in := RenderInput{}

			if i%3 == 0 {
				_, _ = cache.Exec(ctx, db, nil, usePrep, vk, stmtID, 0, 0, nil, DialectQuestion, desc, &in, 1)
			} else {
				r, ms, err := cache.Query(ctx, db, nil, usePrep, vk, stmtID, 0, 0, nil, DialectQuestion, desc, &in, 1)
				if err == nil {
					CloseRows(r, ms)
				}
			}
		}

		verifyCacheInvariants(t, cache)
	})
}

// ============================================================================
// 6. Unit Tests, Zero-Allocation Gates & Pipeline Coverage
// ============================================================================

func openTestDB(t testing.TB) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
		CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			category TEXT NOT NULL,
			score INTEGER NOT NULL,
			label TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= 32; i++ {
		if _, err := db.Exec(`INSERT INTO items (id, category, score, label) VALUES (?, ?, ?, ?)`,
			i, fmt.Sprintf("cat_%d", i%4), i*10, fmt.Sprintf("label_%d", i)); err != nil {
			t.Fatalf("seed items: %v", err)
		}
	}
	return db
}

func TestVariantKeyAndSubqueryShape(t *testing.T) {
	kExact := MakeVariantKey(12, 0x0F, 0x03, 0x100, 5, 10)
	if !kExact.IsExact() {
		t.Fatalf("expected exact VariantKey, got %v", kExact)
	}
	kExact3 := MakeVariantKey(12, 0x0F, 0x03, 0, 1, 2, 3)
	if !kExact3.IsExact() {
		t.Fatalf("expected exact 3-dim VariantKey")
	}
	kExact4 := MakeVariantKey(12, 0x0F, 0x03, 0, 1, 2, 3, 4)
	if !kExact4.IsExact() {
		t.Fatalf("expected exact 4-dim VariantKey")
	}

	inexactCases := []VariantKey{
		MakeVariantKey(0, 1, 1, 0),
		MakeVariantKey(0x8000, 1, 1, 0),
		MakeVariantKey(1, 0x1000000, 1, 0),
		MakeVariantKey(1, 1, 0x1000000, 0),
		MakeVariantKey(1, 1, 1, 0, 1, 2, 3, 4, 5),
		MakeVariantKey(1, 1, 1, 0, 1, 2, 1<<33),
		MakeVariantKey(1, 1, 1, 0, 1, 2, 3, 1<<33),
		MakeVariantKey(1, 1, 1, 0, InexactKeyBit|42),
	}
	for i, k := range inexactCases {
		if k.IsExact() {
			t.Fatalf("case %d: expected inexact VariantKey, got %v", i, k)
		}
	}

	sExact := ComputeSubqueryShape(3, 0x07, 0x01, 0, false, 4, 8)
	if sExact&InexactKeyBit != 0 {
		t.Fatalf("expected exact subquery shape tag, got %#x", sExact)
	}
	sInexactCases := []uint64{
		ComputeSubqueryShape(3, 0x07, 0x01, 0, true, 4),
		ComputeSubqueryShape(3, 0x07, 0x01, 1, false, 4),
		ComputeSubqueryShape(0, 0x07, 0x01, 0, false),
		ComputeSubqueryShape(0x8000, 0x07, 0x01, 0, false),
		ComputeSubqueryShape(3, 0x100, 0x01, 0, false),
		ComputeSubqueryShape(3, 0x07, 0x100, 0, false),
		ComputeSubqueryShape(3, 0x07, 0x01, 0, false, 1, 2, 3),
		ComputeSubqueryShape(3, 0x07, 0x01, 0, false, 70000),
		ComputeSubqueryShape(3, 0x07, 0x01, 0, false, 1, 70000),
	}
	for i, s := range sInexactCases {
		if s&InexactKeyBit == 0 {
			t.Fatalf("inexact subquery case %d: expected InexactKeyBit set, got %#x", i, s)
		}
	}
}

func TestExpandOrderAndSeekAndHelpers(t *testing.T) {
	cols, desc, count, allSame := ExpandOrder(0, 3)
	if count != 1 || cols[0] != 2 || desc[0] || !allSame {
		t.Fatalf("unexpected default ExpandOrder: count=%d cols=%v desc=%v", count, cols[:count], desc[:count])
	}

	spec := PackOrder(Desc(0), Desc(0), Asc(200))
	cols, desc, count, allSame = ExpandOrder(spec, 3)
	if count != 2 || cols[0] != 0 || !desc[0] || cols[1] != 2 || !desc[1] || !allSame {
		t.Fatalf("unexpected single-DESC ExpandOrder: count=%d cols=%v desc=%v", count, cols[:count], desc[:count])
	}

	mixedSpec := PackOrder(Asc(0), Desc(1))
	_, _, count, allSame = ExpandOrder(mixedSpec, 3)
	if count != 3 || allSame {
		t.Fatalf("expected 3 mixed terms, got count=%d allSame=%v", count, allSame)
	}
	if n := SeekArgCount(DialectQuestion, mixedSpec, 3, true); n != 7 {
		t.Fatalf("expected 7 seek args for 3-col mixed Question, got %d", n)
	}
	if n := SeekArgCount(DialectDollar, mixedSpec, 3, true); n != 3 {
		t.Fatalf("expected 3 seek args for Dollar, got %d", n)
	}
	if n := SeekArgCount(DialectQuestion, mixedSpec, 3, false); n != 0 {
		t.Fatalf("expected 0 seek args when hasSeek=false, got %d", n)
	}

	sEmpty := NewSeek()
	if sEmpty.IsActive() || sEmpty.Count() != 0 {
		t.Fatalf("expected inactive empty Seek")
	}
	sGood := NewSeek("cat_a", 999, 42)
	if !sGood.IsActive() || sGood.Count() != 3 || sGood.Val(0) != "cat_a" {
		t.Fatalf("unexpected Seek accessors")
	}

	dst := AcquireArgsCap(8)
	defer ReleaseArgs(dst)
	AppendSeekArgs(dst, DialectQuestion, spec, 3, sGood.Values)
	wantArgs := []any{"cat_a", 42}
	if !slices.Equal(*dst, wantArgs) {
		t.Fatalf("got %v, want %v", *dst, wantArgs)
	}

	*dst = (*dst)[:0]
	AppendSeekArgs(dst, DialectQuestion, mixedSpec, 3, sGood.Values)
	if len(*dst) != 7 {
		t.Fatalf("expected 7 expanded seek args for mixed Question, got %d", len(*dst))
	}

	// Test Seq helpers (FromSeq, FromSlice, MapSeq2, MapSeq) and BatchExecResult
	rawSeq := func(yield func(int) bool) {
		for _, v := range []int{1, 2, 3} {
			if !yield(v) {
				return
			}
		}
	}
	mapped := MapSeq(FromSeq(rawSeq), func(v int) int { return v * 10 })
	var gotMapped []int
	for v, err := range mapped {
		if err != nil {
			t.Fatal(err)
		}
		gotMapped = append(gotMapped, v)
	}
	if !slices.Equal(gotMapped, []int{10, 20, 30}) {
		t.Fatalf("got %v", gotMapped)
	}
	for range FromSeq(rawSeq) {
		break
	}
	for range MapSeq(FromSlice([]int{1, 2}), func(v int) int { return v }) {
		break
	}
	errSeq := func(yield func(int, error) bool) {
		yield(0, errors.New("upstream error"))
	}
	for _, err := range MapSeq(errSeq, func(v int) int { return v }) {
		if err == nil {
			t.Fatal("expected upstream error")
		}
	}
	for _, err := range MapSeq2(errSeq, func(v int) (int, error) { return v, nil }) {
		if err == nil {
			t.Fatal("expected upstream error")
		}
	}
	for _, err := range MapSeq2(FromSlice([]int{1}), func(v int) (int, error) { return 0, errors.New("map err") }) {
		if err == nil {
			t.Fatal("expected map error")
		}
	}

	var br BatchExecResult
	br.Add(nil)
	br.Add(driver.RowsAffected(5))
	if n, err := br.RowsAffected(); err != nil || n != 5 {
		t.Fatalf("RowsAffected=%d, err=%v", n, err)
	}
	_, _ = br.LastInsertId()

	if EncodeOrderKey(1, true) == EncodeOrderKey(1, false) {
		t.Fatal("EncodeOrderKey collision")
	}

	// Uninitialized Query checks
	var zeroQ Query
	if zeroQ.IsValid() || zeroQ.BuildSQL(DialectQuestion) != "" {
		t.Fatalf("expected zero Query to be invalid")
	}
	if err := zeroQ.AppendArgs(dst, DialectQuestion); !errors.Is(err, ErrUninitializedQuery) {
		t.Fatalf("expected ErrUninitializedQuery, got %v", err)
	}
}

func TestArgAndIndexPoolsAndZeroAllocHotPaths(t *testing.T) {
	buf := AcquireArgs()
	*buf = append(*buf, "secret_1", "secret_2")
	underlying := (*buf)[:2]
	ReleaseArgs(buf)
	if underlying[0] != nil || underlying[1] != nil {
		t.Fatalf("expected ReleaseArgs to clear underlying slice elements, got %v", underlying)
	}
	ReleaseArgs(nil)

	bigBuf := AcquireArgsCap(5000)
	ReleaseArgs(bigBuf)

	idxBuf := AcquireIndices(100)
	if len(*idxBuf) != 100 || (*idxBuf)[99] != 99 {
		t.Fatalf("unexpected AcquireIndices(100)")
	}
	ReleaseIndices(idxBuf)
	ReleaseIndices(nil)
	bigIdx := AcquireIndices(70000)
	ReleaseIndices(bigIdx)

	// Zero-allocation hot-path checks (Section 12.1.6)
	if allocs := testing.AllocsPerRun(100, func() {
		vk := MakeVariantKey(1, 0x07, 0x03, 0, 10, 20)
		_ = vk.Hash()
	}); allocs != 0 {
		t.Fatalf("MakeVariantKey allocated %.1f objects", allocs)
	}

	if allocs := testing.AllocsPerRun(100, func() {
		ord := PackOrder(Asc(0), Desc(1), Asc(2))
		_, _, _, _ = ExpandOrder(ord, 4)
	}); allocs != 0 {
		t.Fatalf("PackOrder/ExpandOrder allocated %.1f objects", allocs)
	}

	if allocs := testing.AllocsPerRun(100, func() {
		ab := AcquireArgsCap(8)
		*ab = append(*ab, 1, 2, 3)
		ReleaseArgs(ab)
	}); allocs != 0 {
		t.Fatalf("AcquireArgsCap/ReleaseArgs allocated %.1f objects", allocs)
	}

	if allocs := testing.AllocsPerRun(100, func() {
		ib := AcquireIndices(16)
		ReleaseIndices(ib)
	}); allocs != 0 {
		t.Fatalf("AcquireIndices/ReleaseIndices allocated %.1f objects", allocs)
	}
}

func TestWTinyLFUScanFloodResistance(t *testing.T) {
	db := openTestDB(t)
	const (
		maxPartial      = 128
		maxPartialBytes = 32 * 1024
		maxPrepared     = 160
	)
	cache := NewStmtCache(
		WithMaxSQLVariants(maxPartial),
		WithMaxPartialBytes(maxPartialBytes),
		WithMaxPreparedStmts(maxPrepared),
		WithPromoteThreshold(2),
	)
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	in := RenderInput{}

	hotKeys := make([]VariantKey, 50)
	hotDescs := make([]*QueryDescriptor, 50)
	for i := 0; i < 50; i++ {
		hotKeys[i] = MakeVariantKey(1, 0, 0, 0, uint64(i+1))
		sqlText := fmt.Sprintf("SELECT id, score FROM items WHERE id = ? AND %d = %d", i+1, i+1)
		hotDescs[i] = &QueryDescriptor{
			StmtID:    1,
			StmtVerb:  VerbSelect,
			StaticSQL: sqlText,
			Segments:  []string{sqlText},
		}
		for hit := 0; hit < 6; hit++ {
			row, ms, err := cache.QueryRow(ctx, db, nil, true, hotKeys[i], 1, 0, 0, nil, DialectQuestion, hotDescs[i], &in, (i%32)+1)
			if err != nil {
				t.Fatalf("warm hot query %d: %v", i, err)
			}
			var id, score int
			if err := row.Scan(&id, &score); err != nil {
				t.Fatalf("scan hot query %d: %v", i, err)
			}
			_ = ms.Release()
		}
		if st := cache.LookupState(hotKeys[i], hotDescs[i].StaticSQL); st != StatePrepared {
			t.Fatalf("expected hot key %d in StatePrepared before flood, got %v", i, st)
		}
	}

	for i := 0; i < 10_000; i++ {
		oneOffID := uint64(10_000 + i)
		vk := MakeVariantKey(2, 0, 0, 0, oneOffID)
		sqlStr := fmt.Sprintf("SELECT id FROM items WHERE id = ? AND %d = %d", oneOffID, oneOffID)
		oneOffDesc := &QueryDescriptor{
			StmtID:    2,
			StmtVerb:  VerbSelect,
			StaticSQL: sqlStr,
			Segments:  []string{sqlStr},
		}
		row, ms, err := cache.QueryRow(ctx, db, nil, true, vk, 2, 0, 0, nil, DialectQuestion, oneOffDesc, &in, 1)
		if err != nil {
			t.Fatalf("flood query %d: %v", i, err)
		}
		var id int
		if err := row.Scan(&id); err != nil {
			t.Fatalf("flood scan %d: %v", i, err)
		}
		_ = ms.Release()

		if i%20 == 0 {
			hIdx := (i / 20) % len(hotKeys)
			row, ms, err := cache.QueryRow(ctx, db, nil, true, hotKeys[hIdx], 1, 0, 0, nil, DialectQuestion, hotDescs[hIdx], &in, (hIdx%32)+1)
			if err != nil {
				t.Fatalf("interleaved hot query %d: %v", hIdx, err)
			}
			var id, score int
			if err := row.Scan(&id, &score); err != nil {
				t.Fatalf("interleaved hot scan %d: %v", hIdx, err)
			}
			_ = ms.Release()
		}
	}

	verifyCacheInvariants(t, cache)
	stats := cache.Stats()
	if stats.ActivePartial > maxPartial || stats.PartialBytes > maxPartialBytes || stats.ActivePrepared > maxPrepared {
		t.Fatalf("cache stats exceeded caps: %+v", stats)
	}
	for i := range hotKeys {
		if st := cache.LookupState(hotKeys[i], hotDescs[i].StaticSQL); st != StatePrepared {
			t.Fatalf("hot variation %d was evicted from Tier 2 during scan flood (state=%v)", i, st)
		}
	}
}

func TestPipelineWirePipelinerAndSequentialCoverage(t *testing.T) {
	ctx := context.Background()

	// 1. Test PostgreSQL CTE WirePipeliner rendering and execution via mockdriver
	cfg := &mockDriverConfig{
		enableWire: true,
	}
	cfg.wireHandler = func(ctx context.Context, ops []PipelineOp) error {
		for i := range ops {
			op := &ops[i]
			if op.HasResultCols && op.ScanWireRows != nil {
				dRows := &mockRows{
					cfg:  cfg,
					cols: []string{"id"},
					data: [][]driver.Value{{int64(42)}},
				}
				sqlRows, err := RowsFromDriverRows(ctx, dRows)
				if err != nil {
					return err
				}
				n, err := op.ScanWireRows(sqlRows)
				_ = sqlRows.Close()
				if err != nil {
					return err
				}
				op.RowCount = n
			} else {
				op.RowCount = 1
			}
			if op.Decorators.ProduceCond != nil {
				op.CondVal = true
				op.CondSet = true
			}
			for range op.ExtraConds {
				// extra conditions evaluated
			}
			if op.OnDone != nil {
				op.OnDone(op.RowCount, nil)
			}
		}
		return nil
	}

	db := newMockDB(t, cfg)
	cache := NewStmtCache(WithPromoteThreshold(1))
	defer func() { _ = cache.Close() }()

	selDesc := &QueryDescriptor{
		StmtID:    1,
		StmtVerb:  VerbSelect,
		StaticSQL: "SELECT id FROM items WHERE id = $1",
		Segments:  []string{"SELECT id FROM items WHERE id = ", ""},
		Slots:     []Slot{{Kind: SlotScalar}},
	}
	updNoWhereDesc := &QueryDescriptor{
		StmtID:   2,
		StmtVerb: VerbUpdate,
		Segments: []string{"UPDATE items SET score = ", "", ""},
		Slots:    []Slot{{Kind: SlotScalar}, {Kind: SlotBranchGuardWhere}},
	}
	insSingleDesc := &QueryDescriptor{
		StmtID:   3,
		StmtVerb: VerbInsert,
		Segments: []string{"INSERT INTO items (id, score) ", "", ", ", "", ""},
		Slots: []Slot{
			{Kind: SlotInsertValuesOpen},
			{Kind: SlotScalar},
			{Kind: SlotScalar},
			{Kind: SlotInsertValuesClose},
		},
	}
	insTupleDesc := &QueryDescriptor{
		StmtID:   4,
		StmtVerb: VerbInsert,
		Segments: []string{"INSERT INTO items (id, score) ", "", "", ""},
		Slots: []Slot{
			{Kind: SlotInsertTupleValuesOpen},
			{Kind: SlotTupleSlice, Tuple: &TupleDescriptor{Segments: []string{"", ", ", ""}, NumFields: 2}},
			{Kind: SlotInsertTupleValuesClose},
		},
		NumTuples: 1,
	}

	p := NewPipeline(ctx, db, cache, nil, true, DialectDollar)
	vk := MakeVariantKey(1, 0, 0, 0)

	ab1 := AcquireArgsCap(1)
	*ab1 = append(*ab1, int64(1))
	rowFut := QueueRow[int64](p, 1, selDesc, RenderInput{}, vk, nil, ab1, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	).AssertFound().AssertRowsAffected(1)

	ab2 := AcquireArgsCap(1)
	*ab2 = append(*ab2, int64(1))
	sliceFut := QueueSlice[int64](p, 1, selDesc, RenderInput{}, vk, nil, ab2, nil,
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	).AssertFound().AssertRowsAffected(1)

	// Exercise all condition constructors and combinators (.And, .Or, .Not, WhereCol)
	c1 := rowFut.Found().And(rowFut.RowsAffectedEq(1)).And(rowFut.RowsAffectedGe(1)).And(rowFut.RowsAffectedLe(1))
	c2 := rowFut.NotFound().Or(rowFut.RowsAffectedGt(5)).Or(rowFut.RowsAffectedLt(0)).Not()
	c3 := WhereCol(rowFut, "id = 42", func(v int64) bool { return v == 42 }).And(c1).And(c2)

	sc1 := sliceFut.Found().And(sliceFut.RowsAffectedEq(1)).And(sliceFut.RowsAffectedGe(1)).And(sliceFut.RowsAffectedLe(1))
	sc2 := sliceFut.NotFound().Or(sliceFut.RowsAffectedGt(5)).Or(sliceFut.RowsAffectedLt(0)).Not()
	sc3 := sliceFut.WhereCol("id = 42", func(v int64) bool { return v == 42 }).And(sc1).And(sc2)

	var execFutThen, execFutElse *ExecFuture
	ifRes := p.If(c3.And(sc3), func() {
		ab3 := AcquireArgsCap(1)
		*ab3 = append(*ab3, int64(99))
		execFutThen = p.QueueExec(2, updNoWhereDesc, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, ab3, nil).AssertFound().AssertRowsAffected(1)

		ab4 := AcquireArgsCap(2)
		*ab4 = append(*ab4, int64(10), int64(20))
		_ = p.QueueExec(3, insSingleDesc, RenderInput{}, MakeVariantKey(3, 0, 0, 0), nil, ab4, nil)

		ab5 := AcquireArgsCap(2)
		*ab5 = append(*ab5, int64(11), int64(21))
		_ = p.QueueExec(4, insTupleDesc, RenderInput{TupleLens: []int{1}}, MakeVariantKey(4, 0, 0, 0, 1), nil, ab5, nil)

		ec1 := execFutThen.Found().And(execFutThen.RowsAffectedEq(1)).And(execFutThen.RowsAffectedGe(1)).And(execFutThen.RowsAffectedLe(1))
		ec2 := execFutThen.NotFound().Or(execFutThen.RowsAffectedGt(5)).Or(execFutThen.RowsAffectedLt(0)).Not()
		p.If(ec1.And(ec2), func() {}, nil)
	}, func() {
		abElse := AcquireArgsCap(1)
		*abElse = append(*abElse, int64(0))
		execFutElse = p.QueueExec(2, updNoWhereDesc, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, abElse, nil)
	})

	if err := p.Flush(); err != nil {
		t.Fatalf("WirePipeliner Flush: %v", err)
	}
	if !ifRes.Taken() {
		t.Fatalf("expected ifRes.Taken() == true")
	}
	if val, err := rowFut.Get(); err != nil || val != 42 {
		t.Fatalf("rowFut.Get() = %d, err=%v", val, err)
	}
	if val, err := rowFut.Value(); err != nil || val != 42 || rowFut.Skipped() {
		t.Fatalf("rowFut.Value() = %d, err=%v", val, err)
	}
	if vals, err := sliceFut.Get(); err != nil || len(vals) != 1 || vals[0] != 42 {
		t.Fatalf("sliceFut.Get() = %v, err=%v", vals, err)
	}
	if vals, err := sliceFut.Values(); err != nil || len(vals) != 1 || sliceFut.Skipped() {
		t.Fatalf("sliceFut.Values() = %v, err=%v", vals, err)
	}
	if n, err := execFutThen.RowsAffected(); err != nil || n != 1 || execFutThen.Skipped() {
		t.Fatalf("execFutThen.RowsAffected() = %d, err=%v", n, err)
	}
	if _, err := execFutThen.Result(); err != nil {
		t.Fatalf("execFutThen.Result(): %v", err)
	}
	if !execFutElse.Skipped() {
		t.Fatalf("expected execFutElse.Skipped() == true")
	}
	if _, err := execFutElse.Result(); !errors.Is(err, ErrBranchNotTaken) {
		t.Fatalf("expected ErrBranchNotTaken from skipped ExecFuture.Result, got %v", err)
	}
	if _, err := execFutElse.RowsAffected(); !errors.Is(err, ErrBranchNotTaken) {
		t.Fatalf("expected ErrBranchNotTaken from skipped ExecFuture.RowsAffected, got %v", err)
	}

	// 2. Test TranslateWireError for the guard markers on IsAbort and AssertRows
	abortCustomErr := errors.New("custom abort")
	abortOp := &PipelineOp{
		Decorators: OpDecorators{IsAbort: true, AssertRows: -1},
		AbortErr:   abortCustomErr,
	}
	if got := TranslateWireError(abortOp, &mockError{msg: `invalid input syntax for type integer: "sqlgen:abort:1"`, sqlState: "22P02"}); !errors.Is(got, abortCustomErr) {
		t.Fatalf("expected abortCustomErr from TranslateWireError(abort marker), got %v", got)
	}
	assertOp := &PipelineOp{
		Decorators: OpDecorators{AssertRows: 1},
	}
	if got := TranslateWireError(assertOp, errors.New(`invalid input syntax for type integer: "sqlgen:assert:0"`)); !errors.Is(got, ErrAssertionFailed) {
		t.Fatalf("expected ErrAssertionFailed from TranslateWireError, got %v", got)
	}
	if got := TranslateWireError(nil, nil); got != nil {
		t.Fatalf("expected nil from TranslateWireError(nil, nil)")
	}
	otherErr := errors.New("generic wire error")
	if got := TranslateWireError(assertOp, otherErr); !errors.Is(got, otherErr) {
		t.Fatalf("expected generic wire error passthrough")
	}

	// 3. Test WirePipeliner inside an explicit transaction (*TxStmtCache) via WrapConnector
	if err := RunTxWithRetry(ctx, db, nil, nil, []TxOption{WithTxRetry(DefaultRetryPolicy())}, func(tc *TxStmtCache) error {
		_ = tc.Tx()
		pTx := NewPipeline(ctx, db, cache, tc, true, DialectDollar)
		ab := AcquireArgsCap(1)
		*ab = append(*ab, int64(1))
		rf := QueueRow[int64](pTx, 1, selDesc, RenderInput{}, vk, nil, ab, nil,
			func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
			func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
		)
		pTx.If(rf.Found(), func() {
			abIn := AcquireArgsCap(1)
			*abIn = append(*abIn, int64(5))
			_ = pTx.QueueExec(2, updNoWhereDesc, RenderInput{}, MakeVariantKey(2, 0, 0, 0), nil, abIn, nil)
		}, nil)
		return pTx.Exec()
	}); err != nil {
		t.Fatalf("PipelineTx wire execution failed: %v", err)
	}

	// 4. Test Sequential Pipeline execution against live SQLite (covering executeSequential, EvalGoFn, WhereCol, Abort, AssertRows, TxStmtCache)
	sqliteDB := openTestDB(t)
	sqliteCache := NewStmtCache(
		WithPromoteThreshold(0),
		WithPromoteThreshold(20),
		WithPromoteThreshold(1),
		WithMaxBatchParams(1024),
	)
	defer func() { _ = sqliteCache.Close() }()

	if sqliteCache.MaxBatchParams() != 1024 || (*StmtCache)(nil).MaxBatchParams() != DefaultMaxBatchParams {
		t.Fatalf("unexpected MaxBatchParams")
	}
	_ = sqliteCache.MaxRowsPerBatch(DialectDollar, 0, 0)
	_ = sqliteCache.MaxRowsPerBatch(DialectQuestion, 40000, 1)
	_ = (*StmtCache)(nil).PlanBatchChunk(1, 0, 0, 0, 100, true)
	_ = (*StmtCache)(nil).PlanBatchChunk(1, 0, 0, 50, 100, true)
	_ = (*StmtCache)(nil).LookupState(vk, "")
	_ = (*StmtCache)(nil).Stats()

	qSelDesc := &QueryDescriptor{
		StmtID:    10,
		StmtVerb:  VerbSelect,
		StaticSQL: "SELECT score FROM items WHERE id = ?",
		Segments:  []string{"SELECT score FROM items WHERE id = ", ""},
		Slots:     []Slot{{Kind: SlotScalar}},
	}
	qUpdDesc := &QueryDescriptor{
		StmtID:   11,
		StmtVerb: VerbUpdate,
		Segments: []string{"UPDATE items SET score = ", " WHERE id = ", ""},
		Slots:    []Slot{{Kind: SlotScalar}, {Kind: SlotScalar}},
	}

	pSeq := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	abSeq1 := AcquireArgsCap(1)
	*abSeq1 = append(*abSeq1, int64(1))
	rfSeq := QueueRow[int64](pSeq, 10, qSelDesc, RenderInput{}, MakeVariantKey(10, 0, 0, 0), nil, abSeq1, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	)

	abSeq2 := AcquireArgsCap(1)
	*abSeq2 = append(*abSeq2, int64(999999)) // non-existent row -> sql.ErrNoRows in ExecSeq
	rfMissing := QueueRow[int64](pSeq, 10, qSelDesc, RenderInput{}, MakeVariantKey(10, 0, 0, 0), nil, abSeq2, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	)

	abSeq3 := AcquireArgsCap(1)
	*abSeq3 = append(*abSeq3, int64(2))
	sfSeq := QueueSlice[int64](pSeq, 10, qSelDesc, RenderInput{}, MakeVariantKey(10, 0, 0, 0), nil, abSeq3, nil,
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	)

	seqCond := rfSeq.WhereCol("score = 10", func(v int64) bool { return v == 10 }).
		And(rfSeq.RowsAffectedEq(1)).
		And(rfSeq.RowsAffectedGe(1)).
		And(rfSeq.RowsAffectedLe(1)).
		And(rfSeq.RowsAffectedGt(0)).
		And(rfSeq.RowsAffectedLt(2)).
		And(rfMissing.NotFound()).
		And(sfSeq.WhereCol("score = 20", func(v int64) bool { return v == 20 }))

	var efSeqThen *ExecFuture
	var nestedSkippedIf *IfResult
	var rfSkipped *RowFuture[int64]
	var sfSkipped *SliceFuture[int64]
	pSeq.If(seqCond, func() {
		abUpd := AcquireArgsCap(2)
		*abUpd = append(*abUpd, int64(111), int64(1))
		efSeqThen = pSeq.QueueExec(11, qUpdDesc, RenderInput{}, MakeVariantKey(11, 0, 0, 0), nil, abUpd, nil).AssertFound()
	}, func() {
		rfSkipped = QueueRow[int64](pSeq, 10, qSelDesc, RenderInput{}, MakeVariantKey(10, 0, 0, 0), nil, nil, nil,
			func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
			func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
		)
		sfSkipped = QueueSlice[int64](pSeq, 10, qSelDesc, RenderInput{}, MakeVariantKey(10, 0, 0, 0), nil, nil, nil,
			func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
		)
		nestedSkippedIf = pSeq.If(rfSeq.Found(), func() {}, nil)
		pSeq.Abort(errors.New("should not abort in untaken else"))
	})

	if err := pSeq.Flush(); err != nil {
		t.Fatalf("sequential Flush failed: %v", err)
	}
	// Second Flush is a no-op
	if err := pSeq.Flush(); err != nil {
		t.Fatalf("second Flush failed: %v", err)
	}
	if _, err := rfMissing.Get(); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for rfMissing, got %v", err)
	}
	if _, err := rfSkipped.Get(); !errors.Is(err, ErrBranchNotTaken) {
		t.Fatalf("expected ErrBranchNotTaken for rfSkipped, got %v", err)
	}
	if _, err := sfSkipped.Get(); !errors.Is(err, ErrBranchNotTaken) {
		t.Fatalf("expected ErrBranchNotTaken for sfSkipped, got %v", err)
	}
	if !nestedSkippedIf.Skipped() || nestedSkippedIf.Taken() || (*IfResult)(nil).Taken() || !(*IfResult)(nil).Skipped() {
		t.Fatalf("unexpected nestedSkippedIf state")
	}
	if _, err := efSeqThen.Result(); err != nil {
		t.Fatalf("efSeqThen.Result(): %v", err)
	}

	// Test sequential pipeline Abort on taken branch, buildErr, empty pipeline, and AssertRows failure
	pEmpty := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	if err := pEmpty.Flush(); err != nil {
		t.Fatalf("empty Flush: %v", err)
	}

	pBuildErr := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	_ = pBuildErr.QueueExec(11, qUpdDesc, RenderInput{}, MakeVariantKey(11, 0, 0, 0), nil, nil, ErrEmptySliceParam)
	if err := pBuildErr.Flush(); !errors.Is(err, ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam from buildErr, got %v", err)
	}

	pAbort := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	pAbort.Abort(nil) // nil defaults to ErrAssertionFailed
	if err := pAbort.Flush(); !errors.Is(err, ErrAssertionFailed) {
		t.Fatalf("expected ErrAssertionFailed from Abort(nil), got %v", err)
	}

	pAssertFail := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	abFail := AcquireArgsCap(1)
	*abFail = append(*abFail, int64(999999))
	rfAssertFail := QueueRow[int64](pAssertFail, 10, qSelDesc, RenderInput{}, MakeVariantKey(10, 0, 0, 0), nil, abFail, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	).AssertFound()
	if err := pAssertFail.Flush(); !errors.Is(err, ErrAssertionFailed) {
		t.Fatalf("expected ErrAssertionFailed, got %v", err)
	}
	if _, err := rfAssertFail.Get(); !errors.Is(err, ErrAssertionFailed) {
		t.Fatalf("expected ErrAssertionFailed from rfAssertFail.Get(), got %v", err)
	}

	// Test WithPipelineOps / PipelineOpsFromContext and LookupSQL
	ctxWithOps := WithPipelineOps(ctx, []PipelineOp{{StmtID: 1}})
	if opsOut, ok := PipelineOpsFromContext(ctxWithOps); !ok || len(opsOut) != 1 {
		t.Fatalf("PipelineOpsFromContext failed")
	}
	if _, ok := PipelineOpsFromContext(ctx); ok {
		t.Fatalf("expected false from PipelineOpsFromContext on plain context")
	}
	if s, ok := sqliteCache.LookupSQL(MakeVariantKey(10, 0, 0, 0)); !ok || s == "" {
		t.Fatalf("expected LookupSQL hit on warm key")
	}

	// Test globalMaxPrep < 16 cross-shard eviction (evictGlobalPreparedVictim)
	smallGlobalCache := NewStmtCache(WithMaxPreparedStmts(2), WithPromoteThreshold(1))
	defer func() { _ = smallGlobalCache.Close() }()
	for i := uint32(1); i <= 8; i++ {
		k := MakeVariantKey(i, 0, 0, 0, uint64(i))
		d := &QueryDescriptor{
			StmtID:    i,
			StmtVerb:  VerbSelect,
			StaticSQL: fmt.Sprintf("SELECT score FROM items WHERE id = ? AND %d = %d", i, i),
			Segments:  []string{fmt.Sprintf("SELECT score FROM items WHERE id = ? AND %d = %d", i, i)},
		}
		r, ms, err := smallGlobalCache.Query(ctx, sqliteDB, nil, true, k, i, 0, int(i), nil, DialectQuestion, d, &RenderInput{}, 1)
		if err != nil {
			t.Fatalf("smallGlobalCache.Query: %v", err)
		}
		CloseRows(r, ms)
	}

	// 5. Test pipelineConnWrapper methods, isPromotedBatchSize, RenderPipelineBatch >32 ops & cache hit, and QueryRow/Exec in Tx
	wc := WrapConnector(&mockConnector{cfg: cfg})
	if _, err := wc.Driver().Open("mock"); err != nil {
		t.Fatalf("wrappedDriver.Open: %v", err)
	}
	wConn, err := wc.Connect(ctx)
	if err != nil {
		t.Fatalf("wc.Connect: %v", err)
	}
	pw := wConn.(*pipelineConnWrapper)
	_, _ = pw.QueryContext(ctx, "SELECT 1", nil)
	st, err := pw.PrepareContext(ctx, "SELECT 1")
	if err == nil && st != nil {
		_ = st.Close()
	}
	_ = pw.Ping(ctx)
	_ = pw.ResetSession(ctx)
	_ = pw.IsValid()
	_ = pw.CheckNamedValue(&driver.NamedValue{Name: "a", Ordinal: 1, Value: int64(1)})
	_, _ = pw.ExecContext(ctx, "UPDATE items SET score = 1", nil)

	// Test non-wire wrapped connector fallback inside transaction
	nonWireDB := sql.OpenDB(WrapConnector(&mockConnector{cfg: &mockDriverConfig{}}))
	defer func() { _ = nonWireDB.Close() }()
	_ = RunTxWithRetry(ctx, nonWireDB, nil, nil, nil, func(tc *TxStmtCache) error {
		// Calling RunTxWithRetry with non-nil existingTxCache delegates directly
		return RunTxWithRetry(ctx, nonWireDB, tc, nil, nil, func(innerTC *TxStmtCache) error {
			pFallback := NewPipeline(ctx, nonWireDB, sqliteCache, innerTC, false, DialectQuestion)
			ab := AcquireArgsCap(2)
			*ab = append(*ab, int64(1), int64(1))
			_ = pFallback.QueueExec(11, qUpdDesc, RenderInput{}, MakeVariantKey(11, 0, 0, 0), nil, ab, nil)
			return pFallback.Flush()
		})
	})

	// Test Tier-2 prepared QueryRow & Exec inside transaction + usePrepared=false on Tier-2 hit
	var fastSlot atomic.Pointer[ManagedStmt]
	hotVK := MakeVariantKey(10, 0, 0, 0)
	_, msWarm, err := sqliteCache.QueryRow(ctx, sqliteDB, nil, true, hotVK, 10, 0, 0, &fastSlot, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = msWarm.Release()
	// Fast-slot hit
	_, msFast, err := sqliteCache.QueryRow(ctx, sqliteDB, nil, true, hotVK, 10, 0, 0, &fastSlot, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = msFast.Release()
	// usePrepared=false on Tier-2 hit
	_, _, err = sqliteCache.QueryRow(ctx, sqliteDB, nil, false, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sqliteCache.Exec(ctx, sqliteDB, nil, true, MakeVariantKey(11, 0, 0, 0), 11, 0, 0, nil, DialectQuestion, qUpdDesc, &RenderInput{}, 10, 1)
	if err != nil {
		t.Fatal(err)
	}

	_ = RunTxWithRetry(ctx, sqliteDB, nil, nil, nil, func(tc *TxStmtCache) error {
		r1, m1, err := sqliteCache.QueryRow(ctx, sqliteDB, tc, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
		if err != nil {
			return err
		}
		var sc int64
		_ = r1.Scan(&sc)
		_ = m1.Release()

		r2, m2, err := sqliteCache.QueryRow(ctx, sqliteDB, tc, false, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
		if err != nil {
			return err
		}
		_ = r2.Scan(&sc)
		_ = m2.Release()

		if _, err := sqliteCache.Exec(ctx, sqliteDB, tc, true, MakeVariantKey(11, 0, 0, 0), 11, 0, 0, nil, DialectQuestion, qUpdDesc, &RenderInput{}, 10, 1); err != nil {
			return err
		}
		if _, err := sqliteCache.Exec(ctx, sqliteDB, tc, false, MakeVariantKey(11, 0, 0, 0), 11, 0, 0, nil, DialectQuestion, qUpdDesc, &RenderInput{}, 10, 1); err != nil {
			return err
		}
		return nil
	})

	// Test isPromotedBatchSize path (PromoteThreshold=3, promote batchSize=7 via PlanBatchChunk, then Resolve with batchSize=7)
	batchCache := NewStmtCache(WithPromoteThreshold(3))
	defer func() { _ = batchCache.Close() }()
	shapeTag7 := ComputeBatchShapeTag(0, 0)
	for range 3 {
		_ = batchCache.PlanBatchChunk(55, 0, 0, 7, 100, true)
	}
	if !batchCache.isPromotedBatchSize(55, shapeTag7, 7) {
		t.Fatalf("expected batchSize 7 to be promoted")
	}
	// First Resolve enters Tier 1 (freq=1 < 3, so it stays in Tier 1 on miss because effThresh==1 only on Tier 1 hit or if we check Tier 1 hit)
	// Actually on miss with isPromotedBatchSize(55, shapeTag7, 7) == true, effThresh becomes 1 on miss!
	msB, _, err := batchCache.Resolve(ctx, sqliteDB, nil, true, MakeVariantKey(55, 0, 0, 0, 7), 55, shapeTag7, 7, nil, DialectQuestion, qSelDesc, &RenderInput{})
	if err != nil {
		t.Fatal(err)
	}
	_ = msB.Release()

	// Test RenderPipelineBatch with >32 ops and cached SQL lookup
	RenderPipelineBatch(DialectDollar, sqliteCache, nil)
	manyOps := make([]PipelineOp, 36)
	for i := range manyOps {
		if i%2 == 0 {
			manyOps[i] = PipelineOp{
				StmtID:     10,
				Desc:       qSelDesc,
				Key:        hotVK,
				Decorators: OpDecorators{AssertRows: -1},
			}
		} else {
			manyOps[i] = PipelineOp{
				StmtID:     11,
				Desc:       qUpdDesc,
				Key:        MakeVariantKey(11, 0, 0, 0),
				Decorators: OpDecorators{AssertRows: 1},
			}
		}
	}
	RenderPipelineBatch(DialectDollar, sqliteCache, manyOps)
	for i, op := range manyOps {
		if op.SQL == "" {
			t.Fatalf("op %d SQL empty", i)
		}
	}

	// Test Query.AppendArgs and Query.BuildSQL on valid Query with nested subqueries
	validSub := Query{
		StmtID: 10,
		Desc:   qSelDesc,
		Input:  RenderInput{},
		Expand: func(dst *[]any, d Dialect, q *Query) error {
			*dst = append(*dst, int64(42))
			return nil
		},
	}
	if !validSub.IsValid() || validSub.BuildSQL(DialectQuestion) == "" {
		t.Fatalf("expected validSub to be valid")
	}
	dstArgs := AcquireArgsCap(4)
	defer ReleaseArgs(dstArgs)
	if err := validSub.AppendArgs(dstArgs, DialectQuestion); err != nil || len(*dstArgs) != 1 {
		t.Fatalf("validSub.AppendArgs: %v", err)
	}

	// 6. Exercise error branches in Query, QueryRow, Exec, TxStmtCache, and Pipeline
	errMockCfg := &mockDriverConfig{
		failExec:  errors.New("exec fail"),
		failQuery: errors.New("query fail"),
	}
	errDB := newMockDB(t, errMockCfg)
	errCache := NewStmtCache(WithPromoteThreshold(1))
	defer func() { _ = errCache.Close() }()

	// Tier-2 prepared Query error & Exec error
	_, _, _ = errCache.Query(ctx, errDB, nil, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	_, _ = errCache.Exec(ctx, errDB, nil, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	// Tier-1 Query error & Exec error
	_, _, _ = errCache.Query(ctx, errDB, nil, false, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	_, _ = errCache.Exec(ctx, errDB, nil, false, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)

	// Closed TxStmtCache & released ManagedStmt errors in StmtFromManaged
	_ = RunTxWithRetry(ctx, sqliteDB, nil, nil, nil, func(tc *TxStmtCache) error {
		_ = tc.Close()
		_, _ = tc.StmtFromManaged(ctx, msWarm)
		_, _, _ = sqliteCache.Query(ctx, sqliteDB, tc, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
		_, _, _ = sqliteCache.QueryRow(ctx, sqliteDB, tc, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
		_, _ = sqliteCache.Exec(ctx, sqliteDB, tc, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
		return nil
	})
	_ = (*TxStmtCache)(nil).Close()

	// Closed StmtCache Resolve error
	closedCache := NewStmtCache()
	_ = closedCache.Close()
	_ = closedCache.Close()
	_, _, _ = closedCache.Query(ctx, sqliteDB, nil, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	_, _, _ = closedCache.QueryRow(ctx, sqliteDB, nil, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	_, _ = closedCache.Exec(ctx, sqliteDB, nil, true, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{}, 1)
	_, _ = closedCache.LookupSQL(hotVK)

	// Nil StmtCache Resolve
	_, sNil, err := (*StmtCache)(nil).Resolve(ctx, sqliteDB, nil, false, hotVK, 10, 0, 0, nil, DialectQuestion, qSelDesc, &RenderInput{})
	if err != nil || sNil == "" {
		t.Fatalf("expected nil cache Resolve to render SQL")
	}

	// Sequential Pipeline with ExecSeq scan errors and extra conditions
	pScanErr := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	abScan1 := AcquireArgsCap(1)
	*abScan1 = append(*abScan1, int64(1))
	rfScanErr := QueueRow[int64](pScanErr, 10, qSelDesc, RenderInput{}, hotVK, nil, abScan1, nil,
		func(r *sql.Row, dst *int64) error { return errors.New("custom row scan err") },
		func(rs *sql.Rows, dst *int64) error { return errors.New("custom rows scan err") },
	)
	if err := pScanErr.Flush(); err == nil {
		t.Fatalf("expected row scan error")
	}
	_, _ = rfScanErr.Get()

	pSliceScanErr := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	abScan2 := AcquireArgsCap(1)
	*abScan2 = append(*abScan2, int64(1))
	sfScanErr := QueueSlice[int64](pSliceScanErr, 10, qSelDesc, RenderInput{}, hotVK, nil, abScan2, nil,
		func(rs *sql.Rows, dst *int64) error { return errors.New("custom slice scan err") },
	)
	if err := pSliceScanErr.Flush(); err == nil {
		t.Fatalf("expected slice scan error")
	}
	_, _ = sfScanErr.Get()

	// Multiple If blocks sharing the same rootOp (ExtraConds)
	pMultiIf := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	abMulti := AcquireArgsCap(1)
	*abMulti = append(*abMulti, int64(1))
	rfMulti := QueueRow[int64](pMultiIf, 10, qSelDesc, RenderInput{}, hotVK, nil, abMulti, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	)
	pMultiIf.If(rfMulti.Found(), func() {}, nil)
	pMultiIf.If(rfMulti.RowsAffectedEq(1), func() {}, nil)
	if err := pMultiIf.Flush(); err != nil {
		t.Fatalf("pMultiIf.Flush: %v", err)
	}

	// 7. Additional edge-case coverage for wireRowsConn, PipelineCond, non-exact VariantKey (bySQL),
	// segmented LRU protected-segment step-down, and pool oversized capacities.
	wrc := wireRowsConnector{}
	_ = wrc.Driver()
	wrConn, _ := wrc.Connect(ctx)
	_, _ = wrConn.Prepare("SELECT 1")
	_, _ = wrConn.Begin()
	_ = wrConn.Close()
	if qrc, ok := wrConn.(driver.QueryerContext); ok {
		_, _ = qrc.QueryContext(ctx, "SELECT 1", nil)
	}

	_ = WhereCol(rfMulti, "score > 0", func(v int64) bool { return v > 0 })
	_ = WhereCol[int64](nil, "score > 0", nil)
	var nilCond *PipelineCond
	_ = nilCond.EvalGoFn()
	_ = (&PipelineCond{Kind: 99}).EvalGoFn()
	_ = nilCond.estimateBytes(0)
	_ = (&PipelineCond{Kind: 99}).estimateBytes(0)
	var sb strings.Builder
	sc := streamCursor{dialect: DialectDollar, paramIdx: 1}
	nilCond.writeSQL(&sb, &sc, nil)
	(&PipelineCond{Kind: 99}).writeSQL(&sb, &sc, nil)
	streamCoreStatement(&sb, &sc, nil, &RenderInput{})
	_ = estimateDescriptorBytes(nil, nil, &RenderInput{}, &estCursor{})

	if BuildComposedSQL(nil, nil, DialectQuestion) != "" {
		t.Fatalf("expected empty SQL for nil descriptor")
	}
	_ = BuildComposedSQL(qUpdDesc, nil, DialectQuestion)
	_ = EstimateOpBytes(qUpdDesc, nil, nil)

	// Oversized pool allocations (>64 args, >256 indices)
	bigArgs := AcquireArgsCap(128)
	ReleaseArgs(bigArgs)
	bigIdx := AcquireIndices(512)
	ReleaseIndices(bigIdx)

	// MapSeq2 early break when yield returns false
	seq2 := MapSeq2(FromSlice([]int{1, 2, 3}), func(v int) (int, error) { return v * 10, nil })
	for range seq2 {
		break
	}
	errSeq := func(yield func(int, error) bool) {
		yield(0, errors.New("seq err"))
	}
	for _, err := range MapSeq2(errSeq, func(v int) (int, error) { return v, nil }) {
		if err == nil {
			t.Fatalf("expected error from MapSeq2")
		}
	}

	// Non-exact VariantKey (>4 dims sets the overflow bit so !vk.IsExact(), exercising sh.bySQL)
	inexactCache := NewStmtCache(
		WithMaxSQLVariants(64),
		WithMaxPreparedStmts(64),
		WithPromoteThreshold(3),
	)
	for i := uint32(1); i <= 24; i++ {
		inexactVK := MakeVariantKey(i, 0, 0, 0, 1, 2, 3, 4, 5)
		if inexactVK.IsExact() {
			t.Fatalf("expected inexact VariantKey for >4 dims")
		}
		dInexact := &QueryDescriptor{
			StmtID:   i,
			StmtVerb: VerbSelect,
			Segments: []string{fmt.Sprintf("SELECT score FROM items WHERE id = ? AND %d = %d", i, i)},
		}
		// Hit 1 (miss -> partialProb in bySQL)
		_, _, _ = inexactCache.Resolve(ctx, sqliteDB, nil, true, inexactVK, i, 0, 0, nil, DialectQuestion, dInexact, &RenderInput{})
		_ = inexactCache.LookupState(inexactVK, dInexact.Segments[0])
		// Hit 2 (partialProb -> partialProt)
		_, _, _ = inexactCache.Resolve(ctx, sqliteDB, nil, true, inexactVK, i, 0, 0, nil, DialectQuestion, dInexact, &RenderInput{})
		// Hit 3 (promote to preparedProb in bySQL)
		msInexact, _, err := inexactCache.Resolve(ctx, sqliteDB, nil, true, inexactVK, i, 0, 0, nil, DialectQuestion, dInexact, &RenderInput{})
		if err == nil && msInexact != nil {
			_ = msInexact.Release()
		}
		// Hit 4 (preparedProb -> preparedProt)
		msInexact2, _, err := inexactCache.Resolve(ctx, sqliteDB, nil, true, inexactVK, i, 0, 0, nil, DialectQuestion, dInexact, &RenderInput{})
		if err == nil && msInexact2 != nil {
			_ = msInexact2.Release()
		}
		// Hit 5 (touch in preparedProt)
		msInexact3, _, err := inexactCache.Resolve(ctx, sqliteDB, nil, true, inexactVK, i, 0, 0, nil, DialectQuestion, dInexact, &RenderInput{})
		if err == nil && msInexact3 != nil {
			_ = msInexact3.Release()
		}
	}
	_ = inexactCache.Close()

	// PackOrder, ExpandOrder, and AppendSeekArgs edge cases
	_ = PackOrder(0, Asc(0), Desc(0), Asc(1), Asc(2), Asc(3), Asc(4), Asc(5), Asc(6), Asc(7), Asc(8))
	dupOrderSpec := uint64(Asc(0)) | (uint64(Desc(0)) << 8) | (uint64(Asc(99)) << 16)
	_, _, _, _ = ExpandOrder(dupOrderSpec, 3)
	sevenOrder := PackOrder(Asc(0), Asc(1), Asc(2), Asc(3), Asc(4), Asc(5), Asc(6))
	_, _, _, _ = ExpandOrder(sevenOrder, 8)
	var seekArgs []any
	AppendSeekArgs(&seekArgs, DialectQuestion, PackOrder(Asc(0), Desc(1)), 3, nil)
	AppendSeekArgs(&seekArgs, DialectQuestion, PackOrder(Asc(0), Desc(1)), 3, []any{10})

	// Exercise partialProt touch non-head and step-down, plus NewStmtCache small variants/bytes
	protCache := NewStmtCache(
		WithMaxSQLVariants(4),
		WithMaxPartialBytes(8),
		WithMaxPreparedStmts(32),
		WithPromoteThreshold(10),
	)
	_ = protCache.Close()
	protCache2 := NewStmtCache(
		WithMaxSQLVariants(64),
		WithMaxPartialBytes(65536),
		WithMaxPreparedStmts(32),
		WithPromoteThreshold(10),
	)
	for round := 0; round < 4; round++ {
		for i := uint32(1); i <= 32; i++ {
			k := MakeVariantKey(i, 0, 0, 0, uint64(i))
			d := &QueryDescriptor{
				StmtID:   i,
				StmtVerb: VerbSelect,
				Segments: []string{fmt.Sprintf("SELECT %d", i)},
			}
			_, _, _ = protCache2.Resolve(ctx, sqliteDB, nil, false, k, i, 0, 0, nil, DialectQuestion, d, &RenderInput{})
		}
	}
	_ = protCache2.Close()

	// Evict inexact VariantKeys from Tier 1 (exercising delete(s.bySQL, e.sqlStr))
	// and demote with MaxSQLVariants(0), fastSlot != nil, and batchSize > 0
	inexactEvictCache := NewStmtCache(WithMaxSQLVariants(16), WithPromoteThreshold(10))
	for i := uint32(1); i <= 64; i++ {
		inexactVK := MakeVariantKey(i, 0, 0, 0, 1, 2, 3, 4, 5)
		d := &QueryDescriptor{
			StmtID:   i,
			StmtVerb: VerbSelect,
			Segments: []string{fmt.Sprintf("SELECT %d", i)},
		}
		_, _, _ = inexactEvictCache.Resolve(ctx, sqliteDB, nil, false, inexactVK, i, 0, 0, nil, DialectQuestion, d, &RenderInput{})
	}
	_ = inexactEvictCache.Close()

	noPartialCache := NewStmtCache(WithMaxSQLVariants(0), WithMaxPreparedStmts(1), WithPromoteThreshold(1))
	for i := uint32(1); i <= 32; i++ {
		var fs atomic.Pointer[ManagedStmt]
		k := MakeVariantKey(i, 0, 0, 0, uint64(i))
		d := &QueryDescriptor{
			StmtID:   i,
			StmtVerb: VerbSelect,
			Segments: []string{fmt.Sprintf("SELECT %d", i)},
		}
		for range 3 {
			msNP, _, err := noPartialCache.Resolve(ctx, sqliteDB, nil, true, k, i, 0, int(i), &fs, DialectQuestion, d, &RenderInput{})
			if err == nil && msNP != nil {
				_ = msNP.Release()
			}
		}
	}
	_ = noPartialCache.Close()

	// QueueRow / QueueSlice expandErr, ScanWireRows error, ExecSeq errors, and RunTxWithRetry context cancellation
	pRowExpandErr := NewPipeline(ctx, sqliteDB, sqliteCache, nil, true, DialectQuestion)
	_ = QueueRow[int64](pRowExpandErr, 10, qSelDesc, RenderInput{}, hotVK, nil, nil, ErrEmptySliceParam,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	)
	_ = QueueSlice[int64](pRowExpandErr, 10, qSelDesc, RenderInput{}, hotVK, nil, nil, ErrEmptySliceParam,
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	)
	_ = pRowExpandErr.Flush()

	// Wire ScanWireRows errors on RowFuture and SliceFuture
	pWireScanErr := NewPipeline(ctx, db, cache, nil, true, DialectDollar)
	_ = QueueRow[int64](pWireScanErr, 1, selDesc, RenderInput{}, vk, nil, nil, nil,
		func(r *sql.Row, dst *int64) error { return errors.New("row wire scan err") },
		func(rs *sql.Rows, dst *int64) error { return errors.New("rows wire scan err") },
	)
	_ = pWireScanErr.Flush()

	pWireSliceScanErr := NewPipeline(ctx, db, cache, nil, true, DialectDollar)
	_ = QueueSlice[int64](pWireSliceScanErr, 1, selDesc, RenderInput{}, vk, nil, nil, nil,
		func(rs *sql.Rows, dst *int64) error { return errors.New("slice wire scan err") },
	)
	_ = pWireSliceScanErr.Flush()

	// Sequential QueueExec / QueueRow / QueueSlice ExecSeq error paths (using closedCache / errDB)
	pSeqExecErr := NewPipeline(ctx, errDB, errCache, nil, false, DialectQuestion)
	efErr := pSeqExecErr.QueueExec(11, qUpdDesc, RenderInput{}, hotVK, nil, nil, nil)
	_ = pSeqExecErr.Flush()
	_, _ = efErr.Result()
	_, _ = efErr.RowsAffected()

	pSeqRowErr := NewPipeline(ctx, sqliteDB, closedCache, nil, true, DialectQuestion)
	_ = QueueRow[int64](pSeqRowErr, 10, qSelDesc, RenderInput{}, hotVK, nil, nil, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	)
	_ = pSeqRowErr.Flush()

	pSeqSliceErr := NewPipeline(ctx, errDB, errCache, nil, false, DialectQuestion)
	_ = QueueSlice[int64](pSeqSliceErr, 10, qSelDesc, RenderInput{}, hotVK, nil, nil, nil,
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	)
	_ = pSeqSliceErr.Flush()

	// RunTxWithRetry with zero BaseDelay/MaxDelay and context cancellation during backoff
	cancelCtx, cancel := context.WithCancel(ctx)
	_ = RunTxWithRetry(cancelCtx, sqliteDB, nil, nil, []TxOption{WithTxRetry(TxRetryPolicy{MaxRetries: 5})}, func(tc *TxStmtCache) error {
		cancel()
		return errors.New("SQLITE_BUSY")
	})

	// PlanBatchChunk greedy reuse of prepared Tier-2 batch size <= capRows (and exact prepared size hit)
	greedyCache := NewStmtCache(WithPromoteThreshold(2))
	shapeGreedy := ComputeBatchShapeTag(1, 0)
	for range 2 {
		msG, _, err := greedyCache.Resolve(ctx, sqliteDB, nil, true, MakeVariantKey(77, 1, 0, 0, 16), 77, shapeGreedy, 16, nil, DialectQuestion, qSelDesc, &RenderInput{ColMask: 1})
		if err == nil && msG != nil {
			_ = msG.Release()
		}
	}
	// Exact prepared size hit (when not in promotedSizes via PlanBatchChunk) and greedy <= capRows hit
	_ = greedyCache.PlanBatchChunk(77, 1, 0, 16, 100, false)
	if got := greedyCache.PlanBatchChunk(77, 1, 0, 20, 100, false); got != 16 {
		t.Fatalf("expected greedy prepared batch size 16 for remaining=20, got %d", got)
	}
	_ = greedyCache.Close()
}

// ============================================================================
// 7. Benchmarks (Section 13 Gate 5)
// ============================================================================

func BenchmarkStaticQueryHit(b *testing.B) {
	db := openTestDB(b)
	cache := NewStmtCache(WithPromoteThreshold(1))
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	vk := MakeVariantKey(1, 0, 0, 0)
	var fastSlot atomic.Pointer[ManagedStmt]
	desc := &QueryDescriptor{
		StmtID:    1,
		StmtVerb:  VerbSelect,
		StaticSQL: "SELECT id, score FROM items WHERE id = ?",
		Segments:  []string{"SELECT id, score FROM items WHERE id = ", ""},
		Slots:     []Slot{{Kind: SlotScalar}},
	}
	in := RenderInput{}

	row, ms, err := cache.QueryRow(ctx, db, nil, true, vk, 1, 0, 0, &fastSlot, DialectQuestion, desc, &in, 1)
	if err != nil {
		b.Fatal(err)
	}
	var id, score int
	if err := row.Scan(&id, &score); err != nil {
		b.Fatal(err)
	}
	_ = ms.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		argBuf := AcquireArgsCap(1)
		*argBuf = append(*argBuf, 1)
		r, m, err := cache.QueryRow(ctx, db, nil, true, vk, 1, 0, 0, &fastSlot, DialectQuestion, desc, &in, (*argBuf)...)
		ReleaseArgs(argBuf)
		if err != nil {
			b.Fatal(err)
		}
		var outID, outScore int
		if err := r.Scan(&outID, &outScore); err != nil {
			b.Fatal(err)
		}
		_ = m.Release()
	}
}

func BenchmarkDynamicSliceHit(b *testing.B) {
	db := openTestDB(b)
	cache := NewStmtCache(WithPromoteThreshold(1))
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	ids := []int64{1, 2, 3, 4}
	vk := MakeVariantKey(2, 0, 0, 0, uint64(len(ids)))
	desc := &QueryDescriptor{
		StmtID:    2,
		StmtVerb:  VerbSelect,
		Segments:  []string{"SELECT id, score FROM items WHERE id IN (", ")"},
		Slots:     []Slot{{Kind: SlotSlice}},
		NumSlices: 1,
	}
	in := RenderInput{SliceLens: []int{len(ids)}}

	argBuf := AcquireArgsCap(len(ids))
	for _, v := range ids {
		*argBuf = append(*argBuf, v)
	}
	rows, ms, err := cache.Query(ctx, db, nil, true, vk, 2, 0, 0, nil, DialectQuestion, desc, &in, (*argBuf)...)
	ReleaseArgs(argBuf)
	if err != nil {
		b.Fatal(err)
	}
	CloseRows(rows, ms)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ab := AcquireArgsCap(len(ids))
		for _, v := range ids {
			*ab = append(*ab, v)
		}
		r, m, err := cache.Query(ctx, db, nil, true, vk, 2, 0, 0, nil, DialectQuestion, desc, &in, (*ab)...)
		ReleaseArgs(ab)
		if err != nil {
			b.Fatal(err)
		}
		var itemID, itemScore int64
		scanBuf := [2]any{&itemID, &itemScore}
		for r.Next() {
			if err := r.Scan(scanBuf[:]...); err != nil {
				b.Fatal(err)
			}
		}
		CloseRows(r, m)
	}
}

type benchItemRow struct {
	ID       int64
	Category string
	Score    int64
	Label    string
}

func BenchmarkSelectColsScan(b *testing.B) {
	db := openTestDB(b)
	cache := NewStmtCache(WithPromoteThreshold(1))
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	const colMask uint64 = (1 << 0) | (1 << 2)
	vk := MakeVariantKey(3, colMask, 0, 0)
	desc := &QueryDescriptor{
		StmtID:         3,
		StmtVerb:       VerbSelect,
		Segments:       []string{"SELECT ", " FROM items WHERE id <= ", ""},
		Slots:          []Slot{{Kind: SlotSelectCols}, {Kind: SlotScalar}},
		SelectColExprs: []string{"id", "category", "score", "label"},
	}
	in := RenderInput{ColMask: colMask}

	seqFn := func(limitID int64) iter.Seq2[benchItemRow, error] {
		return func(yield func(benchItemRow, error) bool) {
			ab := AcquireArgsCap(1)
			*ab = append(*ab, limitID)
			rows, ms, err := cache.Query(ctx, db, nil, true, vk, 3, ComputeBatchShapeTag(colMask, 0), 0, nil, DialectQuestion, desc, &in, (*ab)...)
			ReleaseArgs(ab)
			if err != nil {
				yield(benchItemRow{}, err)
				return
			}
			var item benchItemRow
			var scanBuf [4]any
			scanBuf[0] = &item.ID
			scanBuf[1] = &item.Score
			scanSlice := scanBuf[:2]
			for rows.Next() {
				item = benchItemRow{}
				if err := rows.Scan(scanSlice...); err != nil {
					CloseRows(rows, ms)
					yield(benchItemRow{}, err)
					return
				}
				if !yield(item, nil) {
					CloseRows(rows, ms)
					return
				}
			}
			CloseRows(rows, ms)
		}
	}

	for _, err := range seqFn(16) {
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row, err := range seqFn(16) {
			if err != nil {
				b.Fatal(err)
			}
			if row.ID == 0 || row.Category != "" {
				b.Fatalf("unexpected row: %+v", row)
			}
		}
	}
}

func BenchmarkZeroAllocHotPath(b *testing.B) {
	staticDesc := &QueryDescriptor{
		StmtID:    1,
		StmtVerb:  VerbSelect,
		StaticSQL: "SELECT id, score FROM items WHERE id = ?",
		Segments:  []string{"SELECT id, score FROM items WHERE id = ", ""},
		Slots:     []Slot{{Kind: SlotScalar}},
	}
	in := RenderInput{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ord := PackOrder(1, 0x01, 0x82)
		vk := MakeVariantKey(1, 0b11, 0b1, EncodeOrderKey(ord, true), 4)
		_ = vk
		sqlStr := BuildComposedSQL(staticDesc, &in, DialectQuestion)
		if sqlStr == "" {
			b.Fatal("empty SQL")
		}
		ab := AcquireArgsCap(4)
		*ab = append(*ab, int64(42))
		ReleaseArgs(ab)
		ib := AcquireIndices(4)
		*ib = append(*ib, 0, 1)
		ReleaseIndices(ib)
	}
}

// Deterministic races around Tier-2 promotion. tryPromoteLocked releases the shard lock while
// PrepareContext runs, so the entry it is promoting can be evicted/recycled, or the whole cache
// closed, before it re-acquires the lock.

// sameShardKeys returns two exact VariantKeys that land on the same cache shard.
func sameShardKeys(t *testing.T) (a, b VariantKey) {
	t.Helper()
	a = MakeVariantKey(1, 0, 0, 0, 1)
	for cand := uint64(2); cand < 10_000; cand++ {
		b = MakeVariantKey(2, 0, 0, 0, cand)
		if b.hash()&(numCacheShards-1) == a.hash()&(numCacheShards-1) {
			return a, b
		}
	}
	t.Fatal("no same-shard key found")
	return
}

func staticExecDesc(id uint32, table string) *QueryDescriptor {
	return &QueryDescriptor{
		StmtID:   id,
		StmtVerb: VerbUpdate,
		Segments: []string{"UPDATE " + table + " SET x = ", ""},
		Slots:    []Slot{{Kind: SlotScalar}},
	}
}

// blockingPrepareHook blocks PrepareContext for any query containing match until release is closed,
// signalling entered (once) when the first such Prepare is in flight.
func blockingPrepareHook(match string) (hook func(string), entered <-chan struct{}, release func()) {
	in := make(chan struct{})
	rel := make(chan struct{})
	var once sync.Once
	var relOnce sync.Once
	hook = func(q string) {
		if strings.Contains(q, match) {
			once.Do(func() { close(in) })
			<-rel
		}
	}
	return hook, in, func() { relOnce.Do(func() { close(rel) }) }
}

func TestPromotionEntryRecycledDuringPrepare(t *testing.T) {
	cfg := &mockDriverConfig{}
	hook, entered, release := blockingPrepareHook("UPDATE a ")
	cfg.prepareHook = hook
	defer release()
	db := newMockDB(t, cfg)
	cache := NewStmtCache(WithMaxPreparedStmts(64), WithMaxSQLVariants(16), WithPromoteThreshold(1))
	defer cache.Close()
	ctx := context.Background()
	vkA, vkB := sameShardKeys(t)
	descA, descB := staticExecDesc(1, "a"), staticExecDesc(2, "b")
	in := RenderInput{}

	done := make(chan error, 1)
	go func() {
		_, err := cache.Exec(ctx, db, nil, true, vkA, 1, 0, 0, nil, DialectQuestion, descA, &in, 1)
		done <- err
	}()
	<-entered // A is mid-Prepare, its entry still sits in the shard's single Tier-1 slot

	// B lands on the same shard: with one Tier-1 slot per shard it evicts/recycles A's entry.
	if _, err := cache.Exec(ctx, db, nil, true, vkB, 2, 0, 0, nil, DialectQuestion, descB, &in, 1); err != nil {
		t.Fatalf("Exec B: %v", err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("Exec A: %v", err)
	}
	verifyCacheInvariants(t, cache)

	// Every cached statement must still execute the SQL of the variant it is keyed under: whenever a
	// call is served by a prepared statement (recorded at the stmt level), it must be its own SQL.
	stmtExecs := func() int {
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		return len(cfg.stmtExecSQL)
	}
	lastStmtSQL := func() string {
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		return cfg.stmtExecSQL[len(cfg.stmtExecSQL)-1]
	}
	for i := 0; i < 3; i++ {
		for _, c := range []struct {
			vk   VariantKey
			desc *QueryDescriptor
			want string
		}{{vkB, descB, "UPDATE b SET x = ?"}, {vkA, descA, "UPDATE a SET x = ?"}} {
			before := stmtExecs()
			if _, err := cache.Exec(ctx, db, nil, true, c.vk, c.desc.StmtID, 0, 0, nil, DialectQuestion, c.desc, &in, 1); err != nil {
				t.Fatalf("Exec %q: %v", c.want, err)
			}
			if stmtExecs() > before {
				if got := lastStmtSQL(); got != c.want {
					t.Fatalf("variant keyed for %q executed prepared SQL %q (statement cross-talk)", c.want, got)
				}
			}
		}
	}
	verifyCacheInvariants(t, cache)

	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if open := cfg.openStmts.Load(); open != 0 {
		t.Fatalf("leaked %d prepared statements after Close", open)
	}
}

func TestPromotionCacheClosedDuringPrepare(t *testing.T) {
	cfg := &mockDriverConfig{}
	hook, entered, release := blockingPrepareHook("UPDATE a ")
	cfg.prepareHook = hook
	defer release()
	db := newMockDB(t, cfg)
	cache := NewStmtCache(WithMaxPreparedStmts(64), WithMaxSQLVariants(16), WithPromoteThreshold(1))
	ctx := context.Background()
	vkA, _ := sameShardKeys(t)
	in := RenderInput{}

	done := make(chan error, 1)
	go func() {
		_, err := cache.Exec(ctx, db, nil, true, vkA, 1, 0, 0, nil, DialectQuestion, staticExecDesc(1, "a"), &in, 1)
		done <- err
	}()
	<-entered
	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	release()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected closed-cache error, got %v", err)
	}
	if open := cfg.openStmts.Load(); open != 0 {
		t.Fatalf("leaked %d prepared statements", open)
	}
}

func TestPromotionPrepareFailureDoesNotWedgeEntry(t *testing.T) {
	cfg := &mockDriverConfig{}
	db := newMockDB(t, cfg)
	cache := NewStmtCache(WithMaxPreparedStmts(64), WithMaxSQLVariants(16), WithPromoteThreshold(1))
	defer cache.Close()
	ctx := context.Background()
	vkA, _ := sameShardKeys(t)
	descA := staticExecDesc(1, "a")
	in := RenderInput{}

	cfg.mu.Lock()
	cfg.failPrepare = errors.New("boom")
	cfg.mu.Unlock()
	for i := 0; i < 3; i++ {
		if _, err := cache.Exec(ctx, db, nil, true, vkA, 1, 0, 0, nil, DialectQuestion, descA, &in, 1); err == nil {
			t.Fatal("expected prepare failure")
		}
	}
	verifyCacheInvariants(t, cache)

	// Once the server recovers, the same variant must promote normally (no stuck `preparing` flag).
	cfg.mu.Lock()
	cfg.failPrepare = nil
	cfg.mu.Unlock()
	if _, err := cache.Exec(ctx, db, nil, true, vkA, 1, 0, 0, nil, DialectQuestion, descA, &in, 1); err != nil {
		t.Fatalf("Exec after recovery: %v", err)
	}
	if st := cache.LookupState(vkA, ""); st != StatePrepared {
		t.Fatalf("variant not promoted after recovery, state=%v", st)
	}
	verifyCacheInvariants(t, cache)
}

// TestStmtCacheConcurrentAdversarialStress hammers a deliberately tiny cache from many goroutines
// (constant eviction, promotion and demotion, slow Prepare to widen race windows) and checks that
// a prepared statement never runs another variant's SQL, no handle leaks, and the shard lists stay
// consistent. Run with -race.
func TestStmtCacheConcurrentAdversarialStress(t *testing.T) {
	const variants = 96
	for _, tc := range []struct {
		name      string
		threshold uint8
		prepared  int
		variants  int
	}{
		{"threshold1/tiny", 1, 16, 16},
		{"threshold2/tiny", 2, 16, 16},
		{"threshold1/roomy", 1, 64, 256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &mockDriverConfig{prepareDelay: 200 * time.Microsecond}
			var crossTalk atomic.Int64
			cfg.stmtExecHook = func(query string, args []driver.NamedValue) {
				if len(args) != 1 || query != fmt.Sprintf("UPDATE t%d SET x = ?", args[0].Value) {
					crossTalk.Add(1)
				}
			}
			db := newMockDB(t, cfg)
			db.SetMaxOpenConns(32)
			cache := NewStmtCache(WithMaxPreparedStmts(tc.prepared), WithMaxSQLVariants(tc.variants), WithPromoteThreshold(tc.threshold))

			keys := make([]VariantKey, variants)
			descs := make([]*QueryDescriptor, variants)
			for i := range keys {
				keys[i] = MakeVariantKey(uint32(i+1), 0, 0, 0, uint64(i))
				descs[i] = staticExecDesc(uint32(i+1), fmt.Sprintf("t%d", i))
			}

			ctx := context.Background()
			var wg sync.WaitGroup
			for g := 0; g < 24; g++ {
				wg.Add(1)
				go func(seed uint64) {
					defer wg.Done()
					r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
					in := RenderInput{}
					for n := 0; n < 600; n++ {
						i := int(r.Uint64N(variants))
						if r.Uint64N(3) == 0 { // skew towards a hot set so promotions happen
							i = int(r.Uint64N(8))
						}
						if _, err := cache.Exec(ctx, db, nil, true, keys[i], uint32(i+1), 0, 0, nil, DialectQuestion, descs[i], &in, i); err != nil {
							t.Errorf("Exec variant %d: %v", i, err)
							return
						}
					}
				}(uint64(g + 1))
			}
			wg.Wait()

			if n := crossTalk.Load(); n != 0 {
				t.Fatalf("%d statement executions ran the wrong variant's SQL", n)
			}
			verifyCacheInvariants(t, cache)
			if err := cache.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if open := cfg.openStmts.Load(); open != 0 {
				t.Fatalf("leaked %d prepared statements after Close", open)
			}
		})
	}
}

// Regression tests for defects found by executing the rendered pipeline SQL on real PostgreSQL and
// CockroachDB (integration/wire_pg_test.go). They pin the SQL shape so a regression fails without a
// database.

func renderPipelineOp(t *testing.T, dialect Dialect, desc *QueryDescriptor, in RenderInput, dec OpDecorators) string {
	t.Helper()
	ops := []PipelineOp{{Desc: desc, Input: in, Decorators: dec}}
	RenderPipelineBatch(dialect, nil, ops)
	if est := EstimateOpBytes(desc, &in, &dec); len(ops[0].SQL) > est {
		t.Fatalf("EstimateOpBytes %d < rendered length %d:\n%s", est, len(ops[0].SQL), ops[0].SQL)
	}
	return ops[0].SQL
}

// the marker cast to an integer, whose operand contains an aggregate
var runtimeGuardRe = regexp.MustCompile(`\('sqlgen:(assert|abort):' \|\| (COUNT\(\*\)|\(SELECT COUNT\(\*\)\))::text\)::int`)

func TestPipelineSQLOverflowGuardIsNotConstantFoldable(t *testing.T) {
	upd := &QueryDescriptor{StmtVerb: VerbUpdate, Segments: []string{"UPDATE t SET v = ", " WHERE id = ", ""}, Slots: []Slot{{Kind: SlotScalar}, {Kind: SlotScalar}}}
	gates := []BranchGate{{CondID: 1, WantTrue: true}}
	for name, got := range map[string]string{
		"assert":      renderPipelineOp(t, DialectDollar, upd, RenderInput{}, OpDecorators{AssertRows: 1}),
		"assert+gate": renderPipelineOp(t, DialectDollar, upd, RenderInput{BranchGates: gates}, OpDecorators{AssertRows: 1}),
		"abort":       renderPipelineOp(t, DialectDollar, nil, RenderInput{}, OpDecorators{AssertRows: -1, IsAbort: true}),
		"abort+gate":  renderPipelineOp(t, DialectDollar, nil, RenderInput{BranchGates: gates}, OpDecorators{AssertRows: -1, IsAbort: true}),
	} {
		// PostgreSQL folds constant expressions at plan time even in an unreached CASE branch: the
		// guard must depend on a runtime value (an aggregate), never be a constant cast of a literal.
		if !runtimeGuardRe.MatchString(got) {
			t.Errorf("%s: guard error may be constant-folded by PostgreSQL:\n%s", name, got)
		}
	}
	// SQLite keeps its constant form (it evaluates lazily and has no plan-time folding of errors).
	if got := renderPipelineOp(t, DialectQuestion, upd, RenderInput{}, OpDecorators{AssertRows: 1}); !strings.Contains(got, "ABS(-9223372036854775808)") {
		t.Errorf("question dialect overflow guard changed:\n%s", got)
	}
}

func TestPipelineSQLSideEffectCTEsCannotBePruned(t *testing.T) {
	sel := &QueryDescriptor{StmtVerb: VerbSelect, Segments: []string{"SELECT id FROM t WHERE id = ", ""}, Slots: []Slot{{Kind: SlotScalar}}}
	cond := &PipelineCond{Kind: CondRowsGt, Threshold: 0}
	got := renderPipelineOp(t, DialectDollar, sel, RenderInput{}, OpDecorators{AssertRows: 1, ProduceCond: cond, CondID: 3})
	// The assertion/condition CTEs have side effects (an error, set_config) and must run even when the
	// op returns no rows. PostgreSQL pruned them when they were unused columns of a plain join; the
	// outer side of a LATERAL join whose inner side references every column cannot be pruned.
	for _, want := range []string{"CROSS JOIN LATERAL", "_f._o IS NOT NULL", "_f._a IS NOT NULL", "_f._c IS NOT NULL", "AS _r"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "JOIN _sqlgen_op ON TRUE") {
		t.Errorf("prunable join shape is back:\n%s", got)
	}
}

func TestGatedInsertPlaceholdersAreTypedOnlyForDollar(t *testing.T) {
	single := &QueryDescriptor{
		StmtVerb: VerbInsert,
		Segments: []string{"INSERT INTO t (id, name) ", "", ", ", "", ""},
		Slots:    []Slot{{Kind: SlotInsertValuesOpen}, {Kind: SlotScalar, Cast: "bigint"}, {Kind: SlotScalar, Cast: "text"}, {Kind: SlotInsertValuesClose}},
	}
	tuple := &QueryDescriptor{
		StmtVerb:  VerbInsert,
		Segments:  []string{"INSERT INTO t (id, name) ", "", "", ""},
		Slots:     []Slot{{Kind: SlotInsertTupleValuesOpen}, {Kind: SlotTupleSlice, Tuple: &TupleDescriptor{Segments: []string{"", ", ", ""}, NumFields: 2, Casts: []string{"bigint", "text"}}}, {Kind: SlotInsertTupleValuesClose}},
		NumTuples: 1,
	}
	gates := []BranchGate{{CondID: 1, WantTrue: true}}
	in := RenderInput{BranchGates: gates, TupleLens: []int{2}}

	if got := renderPipelineOp(t, DialectDollar, single, RenderInput{BranchGates: gates}, OpDecorators{AssertRows: -1}); !strings.Contains(got, "SELECT $1::bigint, $2::text FROM (SELECT 1)") {
		t.Errorf("gated single-row insert placeholders untyped:\n%s", got)
	}
	if got := renderPipelineOp(t, DialectDollar, tuple, in, OpDecorators{AssertRows: -1}); !strings.Contains(got, "VALUES ($1::bigint, $2::text), ($3::bigint, $4::text)") {
		t.Errorf("gated tuple insert placeholders untyped:\n%s", got)
	}
	// Ungated inserts keep plain VALUES, where PostgreSQL infers types from the target columns.
	if got := renderPipelineOp(t, DialectDollar, single, RenderInput{}, OpDecorators{AssertRows: -1}); strings.Contains(got, "::") {
		t.Errorf("ungated insert must not carry casts:\n%s", got)
	}
	if got := renderPipelineOp(t, DialectQuestion, single, RenderInput{BranchGates: gates}, OpDecorators{AssertRows: -1}); strings.Contains(got, "::") {
		t.Errorf("question dialect must not carry PostgreSQL casts:\n%s", got)
	}
}

// EstimateOpBytes must be a strict upper bound for every decorated pipeline-op shape, or the renderer
// would reallocate its single strings.Builder mid-render (a spec violation).
func TestEstimateOpBytesCoversEveryPipelineOpShape(t *testing.T) {
	sel := &QueryDescriptor{StmtVerb: VerbSelect, Segments: []string{"SELECT id FROM t WHERE id = ", ""}, Slots: []Slot{{Kind: SlotScalar}}}
	upd := &QueryDescriptor{StmtVerb: VerbUpdate, Segments: []string{"UPDATE t SET v = ", ""}, Slots: []Slot{{Kind: SlotScalar}}}
	updRet := &QueryDescriptor{StmtVerb: VerbUpdate, HasReturning: true, Segments: []string{"UPDATE t SET v = ", " RETURNING id"}, Slots: []Slot{{Kind: SlotScalar}}}
	op := &PipelineOp{}
	conds := map[string]*PipelineCond{
		"none":     nil,
		"rows":     {Kind: CondRowsGt, Op: op, Threshold: -9223372036854775807},
		"col":      {Kind: CondColPred, Op: op, SQLPred: "score >= 100 AND status = 'active'"},
		"recorded": {Kind: CondOr, Left: &PipelineCond{Kind: CondRowsGe, Op: op, Recorded: 2147483647}, Right: &PipelineCond{Kind: CondRowsLt, Op: op, Threshold: -9223372036854775807}},
		"and":      {Kind: CondAnd, Left: &PipelineCond{Kind: CondRowsLe, Op: op, Threshold: 99}, Right: &PipelineCond{Kind: CondNot, Left: &PipelineCond{Kind: CondRowsEq, Op: op, Threshold: 1}}},
	}
	for dname, dialect := range map[string]Dialect{"dollar": DialectDollar, "question": DialectQuestion} {
		for dn, desc := range map[string]*QueryDescriptor{"select": sel, "update": upd, "update-returning": updRet} {
			for cn, cond := range conds {
				for _, assert := range []int64{-1, 0, 1, 9223372036854775807} {
					for _, nGates := range []int{0, 1, 4} {
						var gates []BranchGate
						for g := 0; g < nGates; g++ {
							gates = append(gates, BranchGate{CondID: 1000000 + g, WantTrue: g%2 == 0})
						}
						dec := OpDecorators{AssertRows: assert, ProduceCond: cond, CondID: 9223372036854775807 & 0x7fffffff}
						if cond != nil {
							dec.ExtraConds = []ProducedCond{{CondID: 2147483647, Cond: cond}}
							dec.LeafRecs = []ProducedCond{{CondID: 2147483647, Cond: conds["col"]}, {CondID: 2147483646, Cond: cond}}
						}
						in := RenderInput{BranchGates: gates}
						sql := renderPipelineOp(t, dialect, desc, in, dec) // fails on estimate < len
						_ = sql
						_ = dname + dn + cn
					}
				}
			}
		}
	}
}

type codedErr struct {
	state string
	code  int
	msg   string
}

func (e codedErr) Error() string    { return e.msg }
func (e codedErr) SQLState() string { return e.state }
func (e codedErr) Code() int        { return e.code }

// Wrapped driver errors must be classified by their structured code, not only by message text.
func TestIsRetryableTxErrorSeesThroughWrapping(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"pg deadlock":            {codedErr{state: "40P01", msg: "opaque"}, true},
		"pg serialization":       {codedErr{state: "40001", msg: "opaque"}, true},
		"pg lock not available":  {codedErr{state: "55P03", msg: "opaque"}, true},
		"sqlite busy (extended)": {codedErr{code: 5 | 2<<8, msg: "opaque"}, true},
		"mysql deadlock code":    {codedErr{code: 1213, msg: "opaque"}, true},
		"unique violation":       {codedErr{state: "23505", msg: "opaque"}, false},
		"syntax error":           {codedErr{state: "42601", code: 1064, msg: "opaque"}, false},
	} {
		for _, wrap := range []string{"bare", "wrapped", "double"} {
			err := tc.err
			switch wrap {
			case "wrapped":
				err = fmt.Errorf("exec failed: %w", err)
			case "double":
				err = fmt.Errorf("tx: %w", fmt.Errorf("exec failed: %w", err))
			}
			if got := IsRetryableTxError(err, false); got != tc.want {
				t.Errorf("%s/%s: IsRetryableTxError = %v, want %v", name, wrap, got, tc.want)
			}
		}
	}
}

// A database opened through WrapConnector over a driver that is NOT a WirePipeliner must fall back to
// the sequential path for a standalone Pipeline.Flush instead of surfacing driver.ErrSkip.
func TestStandalonePipelineFlushOnWrappedNonWireDriverFallsBack(t *testing.T) {
	cfg := &mockDriverConfig{}
	db := sql.OpenDB(WrapConnector(&mockConnector{cfg: cfg}))
	t.Cleanup(func() { _ = db.Close() })
	cache := NewStmtCache()
	defer func() { _ = cache.Close() }()

	desc := &QueryDescriptor{
		StmtID:   1,
		StmtVerb: VerbUpdate,
		Segments: []string{"UPDATE items SET score = ", ""},
		Slots:    []Slot{{Kind: SlotScalar}},
	}
	p := NewPipeline(context.Background(), db, cache, nil, true, DialectQuestion)
	ab := AcquireArgsCap(1)
	*ab = append(*ab, int64(1))
	fut := p.QueueExec(1, desc, RenderInput{}, MakeVariantKey(1, 0, 0, 0), nil, ab, nil)
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush on a wrapped, non-wire driver must use the sequential fallback, got: %v", err)
	}
	if _, err := fut.Result(); err != nil {
		t.Fatalf("future result: %v", err)
	}
	if cfg.execCalls.Load() == 0 {
		t.Fatal("the operation never reached the database")
	}
}

// The message-text fallback must not mistake data values or unrelated numbers for transient-error codes.
func TestIsRetryableTxErrorTextFallbackIgnoresDataValues(t *testing.T) {
	for _, msg := range []string{
		"Error 1062 (23000): Duplicate entry '40001' for key 'PRIMARY'",
		"ERROR: duplicate key value violates unique constraint \"t_pkey\" (SQLSTATE 23505): Key (id)=(140001) already exists",
		"Error 1062 (23000): Duplicate entry 'order-55p03-x' for key 'uk_ref'",
		"ERROR: value too long for type character varying(40) (SQLSTATE 22001): 40P01-ref",
	} {
		if IsRetryableTxError(errors.New(msg), false) {
			t.Errorf("permanent error classified as retryable: %s", msg)
		}
	}
	// ...while the real messages of the supported engines still are.
	for _, msg := range []string{
		"ERROR: deadlock detected (SQLSTATE 40P01)",
		"ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)",
		"ERROR: restart transaction: TransactionRetryWithProtoRefreshError: WriteTooOldError (SQLSTATE 40001)",
		"ERROR: could not obtain lock on row in relation \"t\" (SQLSTATE 55P03)",
		"Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction",
		"Error 1205 (HY000): Lock wait timeout exceeded; try restarting transaction",
		"database is locked (5) (SQLITE_BUSY)",
		"database table is locked (6) (SQLITE_LOCKED)",
	} {
		if !IsRetryableTxError(errors.New(msg), false) {
			t.Errorf("transient error not classified as retryable: %s", msg)
		}
	}
}

type sqlStateErr struct{ state, msg string }

func (e sqlStateErr) Error() string    { return e.msg }
func (e sqlStateErr) SQLState() string { return e.state }

// Only the guard's own marked error may be translated into the pipeline's assertion/abort errors:
// every other failure of the statement, a genuine integer overflow of the caller's own operation
// included (it must surface as the real database error, as on the sequential path), passes unchanged.
func TestTranslateWireErrorOnlyTranslatesTheGuardMarkers(t *testing.T) {
	assertOp := func() *PipelineOp { return &PipelineOp{Decorators: OpDecorators{AssertRows: 1}} }
	abortErr := errors.New("custom abort")
	abortOp := func() *PipelineOp {
		return &PipelineOp{Decorators: OpDecorators{AssertRows: -1, IsAbort: true}, AbortErr: abortErr}
	}
	for _, e := range []error{
		sqlStateErr{"22008", "ERROR: date/time field value out of range: \"2026-13-45\" (SQLSTATE 22008)"},
		errors.New("ERROR: timestamp out of range (SQLSTATE 22008)"),
		// a genuine overflow of the user's statement, in every wording the engines use
		sqlStateErr{"22003", "ERROR: bigint out of range"},
		fmt.Errorf("exec: %w", sqlStateErr{"22003", "opaque"}),
		errors.New("ERROR: bigint out of range (SQLSTATE 22003)"),
		errors.New("integer overflow"),
		errors.New("ERROR: integer out of range"),
		// an invalid-text error that is not the guard's
		sqlStateErr{"22P02", `ERROR: invalid input syntax for type integer: "abc"`},
		// the marker text, but with another SQLSTATE: not the guard
		sqlStateErr{"23505", `duplicate key value "sqlgen:assert:3"`},
		// the other guard's marker
		sqlStateErr{"22P02", `ERROR: invalid input syntax for type integer: "sqlgen:zzz:1"`},
	} {
		if got := TranslateWireError(assertOp(), e); got != e {
			t.Errorf("unrelated error %q translated to %v", e, got)
		}
		if got := TranslateWireError(abortOp(), e); got != e {
			t.Errorf("unrelated error %q translated to %v on abort op", e, got)
		}
	}
	// each operation only answers to its own marker
	if e := (sqlStateErr{"22P02", `ERROR: invalid input syntax for type integer: "sqlgen:abort:1"`}); TranslateWireError(assertOp(), e) != error(e) {
		t.Error("an assertion operation translated the abort marker")
	}
	if e := (sqlStateErr{"22P02", `ERROR: invalid input syntax for type integer: "sqlgen:assert:2"`}); TranslateWireError(abortOp(), e) != error(e) {
		t.Error("an abort operation translated the assertion marker")
	}
	// nil error / nil op
	if TranslateWireError(assertOp(), nil) != nil || TranslateWireError(nil, errors.New("x")) == nil {
		t.Error("nil handling")
	}
}
