package integration

// Every combination of statement kind x assertion x recorded conditions x gating x run mode must behave
// on the MySQL / MariaDB single-round-trip path (mysqlwire) exactly as on the sequential fallback (plain
// go-sql-driver): same final state, same error (kind, assertion statement / wanted / actual row count,
// MySQL error number), same IfResult.Taken() and same future values. Counterpart of wire_guard_test.go.
//
// The guards of the single-round-trip path are SET statements over session user variables, gated by
// @sqlgen_f<0; a shape that lets a later statement run after a failed guard, or that loses a recorded
// condition, shows up here as a divergence.
//
// The same scenarios run against two generated packages through a thin adapter: myq (dialect mysql) on
// MySQL and mdbq (dialect mariadb, which adds INSERT / DELETE ... RETURNING) on MariaDB.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen/integration/mdbq"
	"github.com/CAFxX/sqlgen/integration/myq"
	"github.com/CAFxX/sqlgen/runtime"
)

// --- the adapter over myq and mdbq ------------------------------------------------------------------

// myFut is a future seen through what the tests need, whatever its Go type.
type myFut struct {
	src         fuzzCondSrc
	assertRows  func(n int64)
	assertFound func()
	colPred     func(minScore int64) runtime.PipelineCond // nil: no column predicate (exec and RETURNING futures)
	desc        func() string                             // what a caller observes after a successful flush
}

type myNew struct {
	ID, Org int64
	Name    string
	Score   int64
	Status  string
}

type myPipe interface {
	AddScoreOne(delta, id int64) myFut
	AddScore(delta int64, ids []int64, org int64) myFut
	InsertItem(id, org int64, name string, score int64, status string) myFut
	InsertItems(rows []myNew) myFut
	DeleteItems(ids []int64) myFut
	SetAllStatus(status string) myFut
	GetItem(id int64) myFut
	ItemsByIDs(ids []int64) myFut
	// RETURNING (MariaDB only: Returning reports whether they exist)
	InsertItemReturning(id, org int64, name string, score int64, status string) myFut
	InsertItemsReturning(rows []myNew) myFut
	DeleteReturning(ids []int64) myFut
	DeleteOneReturning(id int64) myFut
	Returning() bool
	If(cond runtime.PipelineCond, thenFn, elseFn func(myPipe)) *runtime.IfResult
	Abort(err error)
	// value forwarding: producers return the forwarded value too
	FwdScoreOf(id int64) (myFut, runtime.Arg[int64])
	FwdIDsByStatus(status string) (myFut, runtime.Arg[int64])
	CountOrgItems(org int64) (myFut, runtime.Arg[int64])
	FwdInsertReturningID(id int64) (myFut, runtime.Arg[int64])
	FwdSetScore(score, id runtime.Arg[int64]) myFut
	FwdItemsAbove(min runtime.Arg[int64]) myFut
	FwdInsertItem(id int64, score runtime.Arg[int64]) myFut
}

type myQueries interface {
	PipelineTx(ctx context.Context, build func(myPipe) error) error
	Pipeline(ctx context.Context) (p myPipe, flush func() error)
	WithTx(tx *sql.Tx) myQueries
	CacheStats() runtime.CacheStats
	Close() error
}

func myDescribe(v any, err error) string {
	return fmt.Sprintf("%v|%s|sk=%v", v, myErrKind(err), errors.Is(err, runtime.ErrBranchNotTaken))
}

func myExecFut(f *runtime.ExecFuture) myFut {
	return myFut{
		src:         f,
		assertRows:  func(n int64) { f.AssertRows(n) },
		assertFound: func() { f.AssertFound() },
		desc: func() string {
			n, err := f.RowsAffected()
			return myDescribe(n, err)
		},
	}
}

func myRowFut[T any](f *runtime.RowFuture[T], score func(T) int64) myFut {
	m := myFut{
		src:         f,
		assertRows:  func(n int64) { f.AssertRows(n) },
		assertFound: func() { f.AssertFound() },
		desc: func() string {
			v, err := f.Get()
			return myDescribe(v, err)
		},
	}
	if score != nil {
		m.colPred = func(min int64) runtime.PipelineCond {
			return f.WhereCol(fmt.Sprintf("score >= %d", min), func(v T) bool { return score(v) >= min })
		}
	}
	return m
}

func mySliceFut[T any](f *runtime.SliceFuture[T], score func(T) int64) myFut {
	m := myFut{
		src:         f,
		assertRows:  func(n int64) { f.AssertRows(n) },
		assertFound: func() { f.AssertFound() },
		desc: func() string {
			v, err := f.Get()
			return myDescribe(v, err)
		},
	}
	if score != nil {
		m.colPred = func(min int64) runtime.PipelineCond {
			return f.WhereCol(fmt.Sprintf("score >= %d", min), func(v T) bool { return score(v) >= min })
		}
	}
	return m
}

// fwdManyRowsFut is myRowFut for a kind-one statement that may return several rows: which row Get reports
// then depends on the order of the rows, and MariaDB ignores the ORDER BY of the derived table the
// single-round-trip path wraps a gated SELECT (or a producer of a forwarded value) in, so only the outcome
// is compared. (With exactly one row, the only case in which the value is forwarded, there is no order.)
func fwdManyRowsFut(f *runtime.RowFuture[int64]) myFut {
	m := myRowFut(f, nil)
	m.desc = func() string {
		_, err := f.Get()
		return myDescribe("-", err)
	}
	return m
}

// fwdSortedFut is mySliceFut comparing the rows as a set, for the same reason.
func fwdSortedFut(f *runtime.SliceFuture[int64]) myFut {
	m := mySliceFut(f, nil)
	m.desc = func() string {
		v, err := f.Get()
		v = slices.Clone(v)
		slices.Sort(v)
		return myDescribe(v, err)
	}
	return m
}

// myqPipe adapts a myq.Pipeline.
type myqPipe struct{ p *myq.Pipeline }

func myqRows(rows []myNew) []myq.NewItem {
	out := make([]myq.NewItem, len(rows))
	for i, r := range rows {
		out[i] = myq.NewItem{ID: r.ID, OrgID: r.Org, Name: r.Name, Score: r.Score, Status: r.Status}
	}
	return out
}

func (a myqPipe) AddScoreOne(delta, id int64) myFut { return myExecFut(a.p.AddScoreOne(delta, id)) }
func (a myqPipe) AddScore(delta int64, ids []int64, org int64) myFut {
	return myExecFut(a.p.AddScore(delta, ids, org))
}
func (a myqPipe) InsertItem(id, org int64, name string, score int64, status string) myFut {
	return myExecFut(a.p.InsertItem(id, org, name, score, status))
}
func (a myqPipe) InsertItems(rows []myNew) myFut   { return myExecFut(a.p.InsertItems(myqRows(rows))) }
func (a myqPipe) DeleteItems(ids []int64) myFut    { return myExecFut(a.p.DeleteItems(ids)) }
func (a myqPipe) SetAllStatus(status string) myFut { return myExecFut(a.p.SetAllStatus(status)) }
func (a myqPipe) GetItem(id int64) myFut {
	return myRowFut(a.p.GetItem(id), func(it myq.Item) int64 { return it.Score })
}
func (a myqPipe) ItemsByIDs(ids []int64) myFut {
	return mySliceFut(a.p.ItemsByIDs(ids), func(it myq.Item) int64 { return it.Score })
}
func (a myqPipe) InsertItemReturning(id, org int64, name string, score int64, status string) myFut {
	panic("RETURNING does not exist on MySQL")
}
func (a myqPipe) InsertItemsReturning([]myNew) myFut { panic("RETURNING does not exist on MySQL") }
func (a myqPipe) DeleteReturning([]int64) myFut      { panic("RETURNING does not exist on MySQL") }
func (a myqPipe) DeleteOneReturning(int64) myFut     { panic("RETURNING does not exist on MySQL") }
func (a myqPipe) Returning() bool                    { return false }
func (a myqPipe) Abort(err error)                    { a.p.Abort(err) }
func (a myqPipe) FwdScoreOf(id int64) (myFut, runtime.Arg[int64]) {
	f := a.p.FwdScoreOf(id)
	return myRowFut(f, nil), f.Arg()
}
func (a myqPipe) FwdIDsByStatus(status string) (myFut, runtime.Arg[int64]) {
	f := a.p.FwdIDsByStatus(status)
	return fwdManyRowsFut(f), f.Arg()
}
func (a myqPipe) CountOrgItems(org int64) (myFut, runtime.Arg[int64]) {
	f := a.p.CountOrgItems(org)
	return myRowFut(f, nil), f.Arg()
}
func (a myqPipe) FwdSetScore(score, id runtime.Arg[int64]) myFut {
	return myExecFut(a.p.FwdSetScore(score, id))
}
func (a myqPipe) FwdItemsAbove(min runtime.Arg[int64]) myFut {
	return fwdSortedFut(a.p.FwdItemsAbove(min))
}
func (a myqPipe) FwdInsertItem(id int64, score runtime.Arg[int64]) myFut {
	return myExecFut(a.p.FwdInsertItem(runtime.Val(id), "f", score))
}
func (a myqPipe) FwdInsertReturningID(int64) (myFut, runtime.Arg[int64]) {
	panic("RETURNING does not exist on MySQL")
}
func (a myqPipe) If(cond runtime.PipelineCond, thenFn, elseFn func(myPipe)) *runtime.IfResult {
	var t, e func(*myq.Pipeline)
	if thenFn != nil {
		t = func(p *myq.Pipeline) { thenFn(myqPipe{p}) }
	}
	if elseFn != nil {
		e = func(p *myq.Pipeline) { elseFn(myqPipe{p}) }
	}
	return a.p.If(cond, t, e)
}

type myqQueries struct{ q *myq.Queries }

func (a myqQueries) PipelineTx(ctx context.Context, build func(myPipe) error) error {
	return a.q.PipelineTx(ctx, nil, func(p *myq.Pipeline) error { return build(myqPipe{p}) })
}
func (a myqQueries) Pipeline(ctx context.Context) (myPipe, func() error) {
	p := a.q.Pipeline(ctx)
	return myqPipe{p}, p.Flush
}
func (a myqQueries) WithTx(tx *sql.Tx) myQueries    { return myqQueries{a.q.WithTx(tx)} }
func (a myqQueries) CacheStats() runtime.CacheStats { return a.q.CacheStats() }
func (a myqQueries) Close() error                   { return a.q.Close() }

// mdbqPipe adapts an mdbq.Pipeline.
type mdbqPipe struct{ p *mdbq.Pipeline }

func mdbqRows(rows []myNew) []mdbq.NewItem {
	out := make([]mdbq.NewItem, len(rows))
	for i, r := range rows {
		out[i] = mdbq.NewItem{ID: r.ID, OrgID: r.Org, Name: r.Name, Score: r.Score, Status: r.Status}
	}
	return out
}

func (a mdbqPipe) AddScoreOne(delta, id int64) myFut { return myExecFut(a.p.AddScoreOne(delta, id)) }
func (a mdbqPipe) AddScore(delta int64, ids []int64, org int64) myFut {
	return myExecFut(a.p.AddScore(delta, ids, org))
}
func (a mdbqPipe) InsertItem(id, org int64, name string, score int64, status string) myFut {
	return myExecFut(a.p.InsertItem(id, org, name, score, status))
}
func (a mdbqPipe) InsertItems(rows []myNew) myFut   { return myExecFut(a.p.InsertItems(mdbqRows(rows))) }
func (a mdbqPipe) DeleteItems(ids []int64) myFut    { return myExecFut(a.p.DeleteItems(ids)) }
func (a mdbqPipe) SetAllStatus(status string) myFut { return myExecFut(a.p.SetAllStatus(status)) }
func (a mdbqPipe) GetItem(id int64) myFut {
	return myRowFut(a.p.GetItem(id), func(it mdbq.Item) int64 { return it.Score })
}
func (a mdbqPipe) ItemsByIDs(ids []int64) myFut {
	return mySliceFut(a.p.ItemsByIDs(ids), func(it mdbq.Item) int64 { return it.Score })
}
func (a mdbqPipe) InsertItemReturning(id, org int64, name string, score int64, status string) myFut {
	return myRowFut(a.p.InsertItemReturning(id, org, name, score, status), func(w mdbq.Written) int64 { return w.Score })
}
func (a mdbqPipe) InsertItemsReturning(rows []myNew) myFut {
	return mySliceFut(a.p.InsertItemsReturning(mdbqRows(rows)), func(w mdbq.Written) int64 { return w.Score })
}
func (a mdbqPipe) DeleteReturning(ids []int64) myFut {
	return mySliceFut(a.p.DeleteReturning(ids), func(w mdbq.Written) int64 { return w.Score })
}
func (a mdbqPipe) DeleteOneReturning(id int64) myFut {
	return myRowFut(a.p.DeleteOneReturning(id), func(w mdbq.Written) int64 { return w.Score })
}
func (a mdbqPipe) Returning() bool { return true }
func (a mdbqPipe) Abort(err error) { a.p.Abort(err) }
func (a mdbqPipe) FwdScoreOf(id int64) (myFut, runtime.Arg[int64]) {
	f := a.p.FwdScoreOf(id)
	return myRowFut(f, nil), f.Arg()
}
func (a mdbqPipe) FwdIDsByStatus(status string) (myFut, runtime.Arg[int64]) {
	f := a.p.FwdIDsByStatus(status)
	return fwdManyRowsFut(f), f.Arg()
}
func (a mdbqPipe) CountOrgItems(org int64) (myFut, runtime.Arg[int64]) {
	f := a.p.CountOrgItems(org)
	return myRowFut(f, nil), f.Arg()
}
func (a mdbqPipe) FwdSetScore(score, id runtime.Arg[int64]) myFut {
	return myExecFut(a.p.FwdSetScore(score, id))
}
func (a mdbqPipe) FwdItemsAbove(min runtime.Arg[int64]) myFut {
	return fwdSortedFut(a.p.FwdItemsAbove(min))
}
func (a mdbqPipe) FwdInsertItem(id int64, score runtime.Arg[int64]) myFut {
	return myExecFut(a.p.FwdInsertItem(runtime.Val(id), "f", score))
}
func (a mdbqPipe) FwdInsertReturningID(id int64) (myFut, runtime.Arg[int64]) {
	f := a.p.FwdInsertReturningID(id, 1, "r")
	return myRowFut(f, nil), f.Arg()
}
func (a mdbqPipe) If(cond runtime.PipelineCond, thenFn, elseFn func(myPipe)) *runtime.IfResult {
	var t, e func(*mdbq.Pipeline)
	if thenFn != nil {
		t = func(p *mdbq.Pipeline) { thenFn(mdbqPipe{p}) }
	}
	if elseFn != nil {
		e = func(p *mdbq.Pipeline) { elseFn(mdbqPipe{p}) }
	}
	return a.p.If(cond, t, e)
}

type mdbqQueries struct{ q *mdbq.Queries }

func (a mdbqQueries) PipelineTx(ctx context.Context, build func(myPipe) error) error {
	return a.q.PipelineTx(ctx, nil, func(p *mdbq.Pipeline) error { return build(mdbqPipe{p}) })
}
func (a mdbqQueries) Pipeline(ctx context.Context) (myPipe, func() error) {
	p := a.q.Pipeline(ctx)
	return mdbqPipe{p}, p.Flush
}
func (a mdbqQueries) WithTx(tx *sql.Tx) myQueries    { return mdbqQueries{a.q.WithTx(tx)} }
func (a mdbqQueries) CacheStats() runtime.CacheStats { return a.q.CacheStats() }
func (a mdbqQueries) Close() error                   { return a.q.Close() }

// myNewQueries returns the generated package that matches the engine: mdbq (a superset of myq that adds
// RETURNING) on MariaDB, myq on MySQL.
func myNewQueries(e engine, db *sql.DB) myQueries {
	if e.family == famMariaDB {
		return mdbqQueries{mdbq.New(db)}
	}
	return myqQueries{myq.New(db)}
}

// --- scenarios, run modes and the comparison --------------------------------------------------------

type myScn struct {
	name  string
	build func(p myPipe) (ifs []*runtime.IfResult, report func() string)
	// fallback: the single-round-trip path does not apply to this pipeline (documented: a column predicate
	// over a RETURNING operation), it must run sequentially, with the same outcome.
	fallback bool
}

type myMode struct {
	name string
	run  func(ctx context.Context, db *sql.DB, q myQueries, sc myScn) (ifs []*runtime.IfResult, report func() string, err error)
}

var myModes = []myMode{
	{"PipelineTx", func(ctx context.Context, db *sql.DB, q myQueries, sc myScn) (ifs []*runtime.IfResult, report func() string, err error) {
		err = q.PipelineTx(ctx, func(p myPipe) error {
			ifs, report = sc.build(p)
			return nil
		})
		return
	}},
	{"standalone Flush", func(ctx context.Context, db *sql.DB, q myQueries, sc myScn) (ifs []*runtime.IfResult, report func() string, err error) {
		p, flush := q.Pipeline(ctx)
		ifs, report = sc.build(p)
		err = flush()
		return
	}},
	{"user-managed tx", func(ctx context.Context, db *sql.DB, q myQueries, sc myScn) (ifs []*runtime.IfResult, report func() string, err error) {
		tx, berr := db.BeginTx(ctx, nil)
		if berr != nil {
			return nil, nil, berr
		}
		p, flush := q.WithTx(tx).Pipeline(ctx)
		ifs, report = sc.build(p)
		// the caller commits whatever the flush returned: a failed pipeline leaves the transaction open and
		// usable, and what it keeps is what the final state shows
		err = flush()
		if cerr := tx.Commit(); err == nil {
			err = cerr
		}
		return
	}},
}

// myPair is a sequential (plain go-sql-driver) and a single-round-trip (mysqlwire) pool on the same
// server, reused by every scenario of an engine so that connections are not re-established per case.
type myPair struct {
	admin     *sql.DB
	seq, wire *sql.DB
}

func openMyPair(t testing.TB, admin *sql.DB, dsn string) myPair {
	t.Helper()
	return myPair{admin: admin, seq: openMyWire(t, dsn, false), wire: openMyWire(t, dsn, true)}
}

const myFixture = "INSERT INTO sg_items VALUES (1, 1, 'n1', 10, 'new'), (2, 1, 'n2', 20, 'new'), (3, 1, 'n3', 30, 'new'), (4, 1, 'n4', 40, 'new')"

func myResetFixture(t testing.TB, db *sql.DB) {
	t.Helper()
	mustExec(t, db, "DELETE FROM sg_items")
	mustExec(t, db, myFixture)
}

// myRunOnce runs sc in mode on one pool from the fixture and records everything an observer can see.
func myRunOnce(t *testing.T, e engine, pr myPair, wire bool, mode myMode, sc myScn) myOutcome {
	t.Helper()
	myResetFixture(t, pr.admin)
	pool := pr.seq
	if wire {
		pool = pr.wire
	}
	q := myNewQueries(e, pool)
	defer q.Close()
	ifs, report, err := mode.run(ctxT(t), pool, q, sc)
	o := myOutcome{State: mySnapshot(t, pr.admin), Err: myErrKind(err)}
	for _, r := range ifs {
		if r != nil {
			o.Taken = append(o.Taken, r.Taken())
		}
	}
	if err == nil && report != nil {
		o.Report = report()
	}
	st := q.CacheStats()
	o.Stats = [2]uint64{st.WirePipelines, st.SequentialPipelines}
	return o
}

// myPrefixState is the fixture after the one statement every guard-matrix scenario runs first.
const myPrefixState = "1/1/n1/11/new;2/1/n2/20/new;3/1/n3/30/new;4/1/n4/40/new;"

// myCompare runs sc on both paths and fails unless they observe the same. In a user-managed transaction
// the failing operation of a FAILED ASSERTION is the one documented difference (DOCS.md): the sequential
// path has already run it when the assertion is checked in Go, so its effect stays; the single-round-trip
// path undoes it (savepoint). That difference is asserted instead of skipped: when strict is set (the
// scenario's first and only earlier statement is AddScoreOne(1, 1)), the wire state must be exactly the
// state before the failing operation.
func myCompare(t *testing.T, e engine, pr myPair, mode myMode, sc myScn, strict bool) {
	t.Helper()
	seq, wire := myRunOnce(t, e, pr, false, mode, sc), myRunOnce(t, e, pr, true, mode, sc)
	userTxAssertion := mode.name == "user-managed tx" && strings.HasPrefix(seq.Err, "ErrAssertionFailed")
	same := seq.Err == wire.Err && seq.Report == wire.Report && slices.Equal(seq.Taken, wire.Taken)
	if !userTxAssertion {
		same = same && seq.State == wire.State
	} else if strict && !sc.fallback && wire.State != myPrefixState {
		t.Fatalf("user-managed tx, failed assertion: the wire path must undo the failing operation\n  sequential %+v\n  wire       %+v\n  want state %s", seq, wire, myPrefixState)
	}
	if !same {
		t.Fatalf("wire pipeline diverges from sequential fallback:\n  sequential %+v\n  wire       %+v", seq, wire)
	}
	wantSeq, wantWire := [2]uint64{0, 1}, [2]uint64{1, 0}
	if sc.fallback {
		wantWire = [2]uint64{0, 1}
	}
	if seq.Stats != wantSeq || wire.Stats != wantWire {
		t.Fatalf("CacheStats{Wire,Sequential}: sequential run %v (want %v), wire run %v (want %v)", seq.Stats, wantSeq, wire.Stats, wantWire)
	}
}

// --- the guard matrix -------------------------------------------------------------------------------

type myGuardKind struct {
	name      string
	returning bool // MariaDB only
	// queue adds the operation under test, targeting row id (2 is present, 99 absent), and returns it.
	queue func(p myPipe, id int64) myFut
}

var myGuardKinds = []myGuardKind{
	{"update-one", false, func(p myPipe, id int64) myFut { return p.AddScoreOne(100, id) }},
	{"update-many", false, func(p myPipe, id int64) myFut { return p.AddScore(100, []int64{id, 3, 4}, 1) }},
	{"insert-one", false, func(p myPipe, id int64) myFut { return p.InsertItem(id, 9, "g", 5, "new") }},
	{"insert-multi", false, func(p myPipe, id int64) myFut {
		return p.InsertItems([]myNew{{ID: id, Org: 9, Name: "ga", Status: "x"}, {ID: id + 200, Org: 9, Name: "gb", Status: "x"}})
	}},
	{"delete-many", false, func(p myPipe, id int64) myFut { return p.DeleteItems([]int64{id, 3}) }},
	{"select-one", false, func(p myPipe, id int64) myFut { return p.GetItem(id) }},
	{"select-many", false, func(p myPipe, id int64) myFut { return p.ItemsByIDs([]int64{id, 3, 4}) }},
	{"insert-returning-one", true, func(p myPipe, id int64) myFut { return p.InsertItemReturning(id, 9, "g", 5, "new") }},
	{"insert-returning-multi", true, func(p myPipe, id int64) myFut {
		return p.InsertItemsReturning([]myNew{{ID: id, Org: 9, Name: "ga", Status: "x"}, {ID: id + 200, Org: 9, Name: "gb", Status: "x"}})
	}},
	{"delete-returning-many", true, func(p myPipe, id int64) myFut { return p.DeleteReturning([]int64{id, 3}) }},
	{"delete-returning-one", true, func(p myPipe, id int64) myFut { return p.DeleteOneReturning(id) }},
}

// myGuardCond returns the i-th (0..2) condition over op. colPred picks a column predicate for the first
// one where the operation is a read.
func myGuardCond(op myFut, i int, colPred bool) runtime.PipelineCond {
	switch {
	case i == 0 && colPred && op.colPred != nil:
		return op.colPred(20)
	case i == 0:
		return op.src.Found()
	case i == 1:
		return op.src.RowsGe(1)
	default:
		return op.src.NotFound().Not().And(op.src.RowsEq(1))
	}
}

type myGate int

const (
	gateNone    myGate = iota
	gateTaken          // the operation sits in a branch that is taken
	gateSkipped        // ... in a branch that is not: it never runs, its assertion and conditions are inert
)

func (g myGate) String() string { return [...]string{"ungated", "gated-taken", "gated-skipped"}[g] }

func myGuardScenario(k myGuardKind, id int64, assert int64, nconds int, colPred bool, gate myGate) myScn {
	return myScn{
		name: fmt.Sprintf("%s id=%d assert=%d conds=%d colpred=%v %s", k.name, id, assert, nconds, colPred, gate),
		// a column predicate over a RETURNING operation is the documented sequential fallback
		fallback: colPred && nconds > 0 && k.returning,
		build: func(p myPipe) ([]*runtime.IfResult, func() string) {
			p.AddScoreOne(1, 1)
			var op myFut
			queue := func(p myPipe) {
				op = k.queue(p, id)
				if assert >= 0 {
					op.assertRows(assert)
				}
			}
			switch gate {
			case gateNone:
				queue(p)
			case gateTaken:
				p.If(p.GetItem(1).src.Found(), queue, nil)
			case gateSkipped:
				p.If(p.GetItem(404).src.Found(), queue, nil)
			}
			var ifs []*runtime.IfResult
			for c := 0; c < nconds; c++ {
				ifs = append(ifs, p.If(myGuardCond(op, c, colPred),
					func(p myPipe) { p.AddScoreOne(10, int64(2+c)) },
					func(p myPipe) { p.AddScoreOne(1000, int64(2+c)) }))
			}
			// a statement after the guarded one: it must not run when the batch failed
			p.DeleteItems([]int64{4})
			return ifs, func() string { return op.desc() }
		},
	}
}

func myGuardScenarios(returning, full bool) []myScn {
	var out []myScn
	for _, k := range myGuardKinds {
		if k.returning && !returning {
			continue
		}
		for _, id := range []int64{2, 99} { // present / absent
			asserts := []int64{-1, 0, 1, 3}
			conds := []int{0, 1, 2, 3}
			gates := []myGate{gateNone, gateTaken, gateSkipped}
			if !full {
				asserts, conds, gates = []int64{-1, 1}, []int{0, 2}, []myGate{gateNone, gateTaken}
			}
			for _, a := range asserts {
				for _, n := range conds {
					for _, g := range gates {
						out = append(out, myGuardScenario(k, id, a, n, false, g))
						// column predicates: reads (wire) and RETURNING operations (sequential fallback)
						if n > 0 && (k.name == "select-one" || k.name == "select-many" || k.returning) {
							out = append(out, myGuardScenario(k, id, a, n, true, g))
						}
					}
				}
			}
		}
	}
	return out
}

func TestRealEngineMySQLPipelineGuardMatrix(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		pr := openMyPair(t, db, dsn)
		returning := e.family == famMariaDB
		n := 0
		for _, mode := range myModes {
			for _, sc := range myGuardScenarios(returning, mode.name == "PipelineTx") {
				n++
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					myCompare(t, e, pr, mode, sc, true)
				})
			}
		}
		t.Logf("%s: %d guard-matrix cases", e.name, n)
	})
}

// A genuine server error raised by the caller's own statement (an integer overflow, 1264/1690) must
// surface as the database error, never as ErrAssertionFailed, and a failed assertion must report the
// actual count.
var myOverflowScenarios = []myScn{
	{name: "overflowing update with an assertion", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 1)
		p.AddScoreOne(1<<63-1, 2).assertRows(1) // BIGINT overflow
		return nil, nil
	}},
	{name: "overflowing update without an assertion", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1<<63-1, 2)
		return nil, nil
	}},
	{name: "overflowing update with an assertion and a condition", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		u := p.AddScoreOne(1<<63-1, 2)
		u.assertRows(1)
		return []*runtime.IfResult{p.If(u.src.Found(), func(p myPipe) { p.AddScoreOne(1, 3) }, nil)}, nil
	}},
	{name: "overflowing update in a taken branch", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		g := p.GetItem(1)
		return []*runtime.IfResult{p.If(g.src.Found(), func(p myPipe) { p.AddScoreOne(1<<63-1, 2).assertRows(1) }, nil)}, nil
	}},
	{name: "overflowing update in a branch that is not taken", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		g := p.GetItem(404)
		return []*runtime.IfResult{p.If(g.src.Found(), func(p myPipe) { p.AddScoreOne(1<<63-1, 2).assertRows(1) },
			func(p myPipe) { p.AddScoreOne(1, 2).assertRows(1) })}, nil
	}},
	{name: "overflow after a failed assertion never runs", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1, 999).assertRows(1) // fails first
		p.AddScoreOne(1<<63-1, 2)
		return nil, nil
	}},
	{name: "abort after an overflow never runs", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		p.AddScoreOne(1<<63-1, 2).assertRows(1)
		p.Abort(myErrStop)
		return nil, nil
	}},
	{name: "abort in a taken branch keeps its own error", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		g := p.GetItem(1)
		return []*runtime.IfResult{p.If(g.src.Found(), func(p myPipe) { p.Abort(myErrStop) }, nil)}, nil
	}},
	{name: "failed assertion reports the actual count (many rows)", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		p.ItemsByIDs([]int64{1, 2, 3}).assertRows(2)
		return nil, nil
	}},
	{name: "failed assertion reports the actual count (zero rows)", build: func(p myPipe) ([]*runtime.IfResult, func() string) {
		p.DeleteItems([]int64{98, 99}).assertRows(2)
		return nil, nil
	}},
}

func TestRealEngineMySQLPipelineUserOverflowIsNotAnAssertion(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if !e.isMySQL() {
			t.Skip("MySQL-protocol engines only")
		}
		dsn := os.Getenv(e.env)
		setupItems(t, db)
		pr := openMyPair(t, db, dsn)
		for _, mode := range myModes {
			for _, sc := range myOverflowScenarios {
				t.Run(strings.ReplaceAll(mode.name+"/"+sc.name, " ", "_"), func(t *testing.T) {
					myCompare(t, e, pr, mode, sc, false)
					// a server error is a server error: never an assertion (the failed-assertion cases excepted)
					if o := myRunOnce(t, e, pr, true, mode, sc); strings.Contains(sc.name, "overflowing") &&
						strings.Contains(o.Err, "ErrAssertionFailed") {
						t.Fatalf("a server error was reported as an assertion: %+v", o)
					}
				})
			}
		}
	})
}
