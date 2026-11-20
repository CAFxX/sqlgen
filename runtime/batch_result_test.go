package runtime

import (
	"database/sql"
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

func TestReleaseRowsIsIdempotent(t *testing.T) {
	ms := newManagedStmt(nil, 2)
	var rows *sql.Rows
	ReleaseRows(&rows, &ms)
	ReleaseRows(&rows, &ms) // already cleared: must not release again
	if ms != nil {
		t.Fatal("ReleaseRows must clear the statement pointer")
	}
	// ms was cleared, so the single release above took the count from 2 to 1 and no second one happened.
	probe := newManagedStmt(nil, 1)
	keep := probe
	ReleaseRows(&rows, &probe)
	if got := keep.refs.Load(); got != 0 {
		t.Fatalf("refs = %d, want 0 after one release", got)
	}
	var nilRows *sql.Rows
	var nilMS *ManagedStmt
	ReleaseRows(&nilRows, &nilMS)
}
