package wire

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
)

// Without rows attached to its context the carrier connection refuses every entry point with
// driver.ErrSkip (database/sql then reports an error), and Close/Driver are inert.
func TestRowsCarrierWithoutRows(t *testing.T) {
	if _, err := RowsFromDriverRows(context.Background(), nil); err == nil {
		t.Fatal("nil driver.Rows: want an error")
	}
	c := rowsConnector{}
	if c.Driver() != nil {
		t.Fatal("carrier connector has no driver")
	}
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Prepare("SELECT 1"); !errors.Is(err, driver.ErrSkip) {
		t.Fatalf("Prepare = %v", err)
	}
	if _, err := conn.Begin(); !errors.Is(err, driver.ErrSkip) {
		t.Fatalf("Begin = %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}
