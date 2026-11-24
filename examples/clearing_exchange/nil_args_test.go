package clearing_exchange

import (
	"context"
	"errors"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
)

// A nil iterator or callback is reported as runtime.ErrNilArgument before the database is touched.
func TestNilIteratorAndCallbackArgumentsAreErrors(t *testing.T) {
	q := New(nil)
	ctx := context.Background()
	_, stream := q.ApplyMarginSettlementsStream(ctx, nil)
	var seqErr error
	for _, err := range q.IngestOrdersStreamSeq(ctx, nil, 0) {
		seqErr = err
	}
	_, manyStream := q.IngestOrdersStream(ctx, nil, 0)
	for name, err := range map[string]error{
		"many StreamSeq":              seqErr,
		"many Stream":                 manyStream,
		"Stream":                      stream,
		"IngestOrdersEach":            runtime.Each(q.IngestOrdersSeq(ctx, nil, 0), nil),
		"CancelOrdersByIDsEach":       runtime.Each(q.CancelOrdersByIDsSeq(ctx, 0, 0, nil), nil),
		"ListOpenOrdersForSymbolEach": runtime.Each(q.ListOpenOrdersForSymbolSeq(ctx, "", "", 0), nil),
		"QueryRiskPositionsPageEach":  runtime.Each(q.QueryRiskPositionsPageSeq(ctx, QueryRiskPositionsPageParams{}), nil),
		"InTx":                        q.InTx(ctx, nil, nil),
		"PipelineTx":                  q.PipelineTx(ctx, nil, nil),
	} {
		if !errors.Is(err, runtime.ErrNilArgument) {
			t.Errorf("%s: err = %v, want runtime.ErrNilArgument", name, err)
		}
	}
}

// An Order that repeats a column makes argument expansion fail with ErrInvalidOrder.
func TestInvalidOrderFailsArgumentExpansion(t *testing.T) {
	bad := OrderQueryRiskPositionsPage(QueryRiskPositionsPageOrderByNotionalCents.Asc(), QueryRiskPositionsPageOrderByNotionalCents.Desc())
	var dst []any
	sub := runtime.Query{Desc: &runtime.QueryDescriptor{}, Expand: func(*[]any, runtime.Dialect, *runtime.Query) error { return nil }}
	err := expandQueryRiskPositionsPageParams(&dst, runtime.DialectQuestion, QueryRiskPositionsPageParams{MatchingPositions: sub, Order: bad})
	if !errors.Is(err, runtime.ErrInvalidOrder) {
		t.Fatalf("err = %v, want runtime.ErrInvalidOrder", err)
	}
}
