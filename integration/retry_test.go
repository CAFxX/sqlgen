package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen/runtime"
)

// provokeDeadlock makes two transactions lock two rows in opposite order and returns the error the
// engine raised for the victim (always exactly one of them fails on a correct engine).
func provokeDeadlock(t *testing.T, db *sql.DB, table string, ph func(int) string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	upd := func(tx *sql.Tx, id int) error {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET v = v + 1 WHERE id = %s", table, ph(id)), id)
		return err
	}
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := upd(tx1, 1); err != nil {
		t.Fatal(err)
	}
	if err := upd(tx2, 2); err != nil {
		t.Fatal(err)
	}
	res := make(chan error, 2)
	go func() { res <- upd(tx1, 2) }()
	time.Sleep(300 * time.Millisecond) // let tx1 block on row 2 first
	go func() { res <- upd(tx2, 1) }()
	var victim error
	for i := 0; i < 2; i++ {
		select {
		case err := <-res:
			if err != nil {
				victim = err
				// release the survivor
				_ = tx1.Rollback()
				_ = tx2.Rollback()
			}
		case <-ctx.Done():
			t.Fatal("deadlock was not detected within 30s")
		}
	}
	_ = tx1.Rollback()
	_ = tx2.Rollback()
	return victim
}

// TestRealEngineDeadlockIsRetryable: the retry policy is only useful if the *real* errors raised by
// real drivers are recognised as transient.
func TestRealEngineDeadlockIsRetryable(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if e.family == famCockroach {
			t.Skip("CockroachDB reports contention as 40001 retry errors; covered by TestRealEngineSerializationFailureIsRetryable")
		}
		table := makeTable(t, db, "dl", "CREATE TABLE %s (id INT PRIMARY KEY, v INT NOT NULL)")
		mustExec(t, db, fmt.Sprintf("INSERT INTO %s VALUES (1, 0), (2, 0)", table))
		ph := func(int) string {
			if e.isPG() {
				return "$1"
			}
			return "?"
		}
		err := provokeDeadlock(t, db, table, ph)
		if err == nil {
			t.Fatal("expected one transaction to be chosen as a deadlock victim")
		}
		t.Logf("%s deadlock error (%T): %v", e.name, err, err)
		if !runtime.IsRetryableTxError(err, false) {
			t.Fatalf("real %s deadlock error not classified as retryable: %T %v", e.name, err, err)
		}
	})
}

// TestRealEngineSerializationFailureIsRetryable: a write-write conflict under REPEATABLE READ /
// SERIALIZABLE must yield an error recognised as retryable (SQLSTATE 40001).
func TestRealEngineSerializationFailureIsRetryable(t *testing.T) {
	forEachEngine(t, func(t *testing.T, e engine, db *sql.DB) {
		if e.isMySQL() {
			t.Skip("InnoDB resolves this by locking, not by serialization failure; see the deadlock test")
		}
		table := makeTable(t, db, "ser", "CREATE TABLE %s (id INT PRIMARY KEY, v INT NOT NULL)")
		mustExec(t, db, fmt.Sprintf("INSERT INTO %s VALUES (1, 0)", table))
		ctx := context.Background()
		iso := sql.LevelSerializable
		if e.family == famPostgres {
			iso = sql.LevelRepeatableRead
		}
		tx1, _ := db.BeginTx(ctx, &sql.TxOptions{Isolation: iso})
		tx2, _ := db.BeginTx(ctx, &sql.TxOptions{Isolation: iso})
		defer tx1.Rollback()
		defer tx2.Rollback()
		for _, tx := range []*sql.Tx{tx1, tx2} {
			var v int
			if err := tx.QueryRowContext(ctx, fmt.Sprintf("SELECT v FROM %s WHERE id = 1", table)).Scan(&v); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx1.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET v = 1 WHERE id = 1", table)); err != nil {
			t.Fatal(err)
		}
		if err := tx1.Commit(); err != nil {
			t.Fatalf("first commit: %v", err)
		}
		var err error
		if _, err = tx2.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET v = 2 WHERE id = 1", table)); err == nil {
			err = tx2.Commit()
		}
		if err == nil {
			t.Fatal("expected a serialization failure")
		}
		t.Logf("%s serialization error (%T): %v", e.name, err, err)
		if !runtime.IsRetryableTxError(err, false) {
			t.Fatalf("real %s serialization failure not classified as retryable: %T %v", e.name, err, err)
		}
		var _ = errors.Is
	})
}
