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
		"IngestOrdersEach":            q.IngestOrdersEach(ctx, nil, 0, nil),
		"IngestOrdersBatchEach":       q.IngestOrdersBatchEach(ctx, nil, 0, nil),
		"CancelOrdersByIDsEach":       q.CancelOrdersByIDsEach(ctx, 0, 0, nil, nil),
		"CancelOrdersByIDsBatchEach":  q.CancelOrdersByIDsBatchEach(ctx, 0, 0, nil, nil),
		"ListOpenOrdersForSymbolEach": q.ListOpenOrdersForSymbolEach(ctx, "", "", 0, nil),
		"QueryRiskPositionsPageEach":  q.QueryRiskPositionsPageEach(ctx, QueryRiskPositionsPageParams{}, nil),
		"InTx":                        q.InTx(ctx, nil, nil),
		"PipelineTx":                  q.PipelineTx(ctx, nil, nil),
	} {
		if !errors.Is(err, runtime.ErrNilArgument) {
			t.Errorf("%s: err = %v, want runtime.ErrNilArgument", name, err)
		}
	}
}
