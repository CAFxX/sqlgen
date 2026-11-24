package runtime

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func assertTestDescs() (upd, sel *QueryDescriptor) {
	upd = &QueryDescriptor{
		Name:     "BumpScores",
		StmtID:   21,
		StmtVerb: VerbUpdate,
		Segments: []string{"UPDATE items SET score = score + 1 WHERE category = ", ""},
		Slots:    []Slot{{Kind: SlotScalar}},
	}
	sel = &QueryDescriptor{
		Name:      "ItemByID",
		StmtID:    22,
		StmtVerb:  VerbSelect,
		StaticSQL: "SELECT score FROM items WHERE id = ?",
		Segments:  []string{"SELECT score FROM items WHERE id = ", ""},
		Slots:     []Slot{{Kind: SlotScalar}},
	}
	return upd, sel
}

func TestAssertionErrorNamesStatementAndCounts(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	cache := NewStmtCache()
	defer func() { _ = cache.Close() }()
	updDesc, selDesc := assertTestDescs()

	// cat_1 matches 8 of the 32 seeded rows; the assertion expects 5.
	p := NewPipeline(ctx, db, cache, nil, true, DialectQuestion)
	ab := AcquireArgsCap(1)
	*ab = append(*ab, "cat_1")
	fut := p.QueueExec(21, updDesc, RenderInput{}, MakeVariantKey(21, 0, 0, 0), nil, ab, nil).AssertRowsAffected(5)
	err := p.Flush()
	if !errors.Is(err, ErrAssertionFailed) {
		t.Fatalf("errors.Is(ErrAssertionFailed) = false for %v", err)
	}
	var ae *AssertionError
	if !errors.As(err, &ae) {
		t.Fatalf("error %v is not an *AssertionError", err)
	}
	if ae.Statement != "BumpScores" || ae.Want != 5 || ae.Got != 8 || !ae.GotKnown {
		t.Fatalf("unexpected AssertionError fields: %+v", ae)
	}
	want := `sqlgen: pipeline assertion failed on statement "BumpScores": expected 5 rows affected, got 8`
	if err.Error() != want {
		t.Fatalf("message = %q, want %q", err.Error(), want)
	}
	if _, ferr := fut.Result(); !errors.Is(ferr, ErrAssertionFailed) {
		t.Fatalf("future error %v does not match ErrAssertionFailed", ferr)
	}

	// A query assertion reports the statement too.
	p2 := NewPipeline(ctx, db, cache, nil, true, DialectQuestion)
	ab2 := AcquireArgsCap(1)
	*ab2 = append(*ab2, int64(999999))
	rf := QueueRow[int64](p2, 22, selDesc, RenderInput{}, MakeVariantKey(22, 0, 0, 0), nil, ab2, nil,
		func(r *sql.Row, dst *int64) error { return r.Scan(dst) },
		func(rs *sql.Rows, dst *int64) error { return rs.Scan(dst) },
	).AssertFound()
	err = p2.Flush()
	if !errors.Is(err, ErrAssertionFailed) || !strings.Contains(err.Error(), `statement "ItemByID": expected 1 rows affected, got 0`) {
		t.Fatalf("unexpected message %v", err)
	}
	if _, gerr := rf.Get(); !errors.Is(gerr, ErrAssertionFailed) {
		t.Fatalf("future error %v", gerr)
	}
}

func TestAssertionErrorFormatting(t *testing.T) {
	cases := []struct {
		e    *AssertionError
		want string
	}{
		{&AssertionError{Statement: "S", Want: 5}, `sqlgen: pipeline assertion failed on statement "S": expected 5 rows affected`},
		{&AssertionError{Want: 2, Got: 1, GotKnown: true}, `sqlgen: pipeline assertion failed: expected 2 rows affected, got 1`},
	}
	for _, c := range cases {
		if got := c.e.Error(); got != c.want {
			t.Errorf("got %q want %q", got, c.want)
		}
		if !errors.Is(c.e, ErrAssertionFailed) {
			t.Errorf("%v does not match ErrAssertionFailed", c.e)
		}
	}
}

func TestTranslateWireErrorAssertionNamesStatementAndCarriesTheActualCount(t *testing.T) {
	op := &PipelineOp{
		Desc:       &QueryDescriptor{Name: "InsertAudit"},
		Decorators: OpDecorators{AssertRows: 5},
	}
	got := TranslateWireError(op, sqlStateErr{"22P02", `ERROR: invalid input syntax for type integer: "sqlgen:assert:3" (SQLSTATE 22P02)`})
	want := `sqlgen: pipeline assertion failed on statement "InsertAudit": expected 5 rows affected, got 3`
	if got == nil || got.Error() != want {
		t.Fatalf("got %v want %q", got, want)
	}
	var ae *AssertionError
	if !errors.As(got, &ae) || !ae.GotKnown || ae.Got != 3 || ae.Want != 5 || !errors.Is(got, ErrAssertionFailed) {
		t.Fatalf("unexpected error %#v", got)
	}
	// CockroachDB words it differently but carries the same marker; without a readable count the
	// violation is still reported, just without the actual count
	crdb := TranslateWireError(&PipelineOp{Desc: op.Desc, Decorators: op.Decorators}, errors.New(`ERROR: could not parse "sqlgen:assert:12" as type int: strconv.ParseInt: parsing "sqlgen:assert:12": invalid syntax (SQLSTATE 22P02)`))
	if !errors.As(crdb, &ae) || !ae.GotKnown || ae.Got != 12 {
		t.Fatalf("cockroach wording: %#v", crdb)
	}
	noCount := TranslateWireError(&PipelineOp{Desc: op.Desc, Decorators: op.Decorators}, errors.New(`ERROR: invalid input syntax for type integer: "sqlgen:assert:x"`))
	if !errors.As(noCount, &ae) || ae.GotKnown {
		t.Fatalf("unreadable count: %#v", noCount)
	}
}

// A pipeline bound to a user-managed transaction reports a failed assertion but never rolls the
// transaction back: earlier writes stay in it and the caller decides (Rollback or Commit).
func TestUserManagedTxAssertionFailureDoesNotRollBack(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	cache := NewStmtCache()
	defer func() { _ = cache.Close() }()
	updDesc, _ := assertTestDescs()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tc := NewTxStmtCache(tx)
	defer func() { _ = tc.Close() }()
	p := NewPipeline(ctx, db, cache, tc, true, DialectQuestion)
	mk := func(cat string) *[]any {
		ab := AcquireArgsCap(1)
		*ab = append(*ab, cat)
		return ab
	}
	// First write succeeds (8 rows in cat_1), the second asserts a wrong count.
	p.QueueExec(21, updDesc, RenderInput{}, MakeVariantKey(21, 0, 0, 0), nil, mk("cat_1"), nil).AssertRowsAffected(8)
	p.QueueExec(21, updDesc, RenderInput{}, MakeVariantKey(21, 0, 0, 0), nil, mk("cat_2"), nil).AssertRowsAffected(1)
	if err := p.Flush(); !errors.Is(err, ErrAssertionFailed) {
		t.Fatalf("Flush = %v, want ErrAssertionFailed", err)
	}
	// The transaction is still open and still contains the first write.
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM items WHERE category = 'cat_1' AND score % 10 = 1").Scan(&n); err != nil {
		t.Fatalf("tx unusable after failed assertion: %v", err)
	}
	if n != 8 {
		t.Fatalf("earlier write not visible inside the still-open tx: %d", n)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("caller Rollback: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM items WHERE score % 10 = 1").Scan(&n); err != nil || n != 0 {
		t.Fatalf("after Rollback n=%d err=%v", n, err)
	}
}

// The question dialect (SQLite, which the example suites use to simulate a wire driver) cannot carry a
// marker in an error message: its guard is a constant integer overflow, and only operations rendered for
// that dialect recognise it. For the $N dialect the very same error is a genuine database error.
func TestQuestionDialectGuardIsRecognisedAsOverflow(t *testing.T) {
	upd := &QueryDescriptor{Name: "Upd", StmtVerb: VerbUpdate, Segments: []string{"UPDATE t SET v = ", ""}, Slots: []Slot{{Kind: SlotScalar}}}
	abortErr := errors.New("stop")
	mk := func() []PipelineOp {
		return []PipelineOp{
			{Desc: upd, Decorators: OpDecorators{AssertRows: 1}},
			{Decorators: OpDecorators{AssertRows: -1, IsAbort: true}, AbortErr: abortErr},
			{Desc: upd, Decorators: OpDecorators{AssertRows: -1}},
		}
	}
	overflow := errors.New("SQL logic error: integer overflow (1)")

	q := mk()
	renderPipelineBatch(DialectQuestion, nil, q)
	if got := TranslateWireError(&q[0], overflow); !errors.Is(got, ErrAssertionFailed) {
		t.Errorf("assertion: %v", got)
	}
	if got := TranslateWireError(&q[1], sqlStateErr{"22003", "x"}); !errors.Is(got, abortErr) {
		t.Errorf("abort: %v", got)
	}
	if got := TranslateWireError(&q[2], overflow); got != overflow {
		t.Errorf("an unguarded operation translated an overflow: %v", got)
	}
	if got := TranslateWireError(&q[0], errors.New("disk I/O error")); got.Error() != "disk I/O error" {
		t.Errorf("unrelated error translated: %v", got)
	}

	d := mk()
	renderPipelineBatch(DialectDollar, nil, d)
	if got := TranslateWireError(&d[0], overflow); got != overflow {
		t.Errorf("$N dialect: a genuine overflow was translated: %v", got)
	}
}
