package runtime

import (
	"errors"
	"testing"
)

type errResult struct{ rowsErr, idErr error }

func (r errResult) RowsAffected() (int64, error) { return 0, r.rowsErr }
func (r errResult) LastInsertId() (int64, error) { return 0, r.idErr }

// A chunk whose driver cannot report the affected rows must not make the aggregate look like a clean,
// smaller count: the error has to surface from RowsAffected.
func TestBatchExecResultReportsRowsAffectedErrors(t *testing.T) {
	unsupported := errors.New("rows affected unsupported")
	var br BatchExecResult
	br.Add(errResult{})
	br.Add(errResult{rowsErr: unsupported})
	br.Add(errResult{})
	if _, err := br.RowsAffected(); !errors.Is(err, unsupported) {
		t.Fatalf("RowsAffected error = %v, want %v", err, unsupported)
	}
}
