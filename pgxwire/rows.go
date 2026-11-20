package pgxwire

import (
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// converter turns one text-format column value into a database/sql driver value.
type converter func(src []byte) (driver.Value, error)

// rows is a fully buffered driver.Rows over one statement's result.
type rows struct {
	cols []string
	conv []converter
	data [][][]byte
	next int
}

func (r *rows) Columns() []string { return r.cols }
func (r *rows) Close() error      { return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.next >= len(r.data) {
		return io.EOF
	}
	row := r.data[r.next]
	r.next++
	for i, raw := range row {
		if raw == nil {
			dest[i] = nil
			continue
		}
		v, err := r.conv[i](raw)
		if err != nil {
			return fmt.Errorf("convert field %d (%s) failed: %w", i, r.cols[i], err)
		}
		dest[i] = v
	}
	return nil
}

// driverRows wraps a statement result as driver.Rows.
func (c *conn) driverRows(res *pgconn.Result) driver.Rows {
	r := &rows{
		cols: make([]string, len(res.FieldDescriptions)),
		conv: make([]converter, len(res.FieldDescriptions)),
		data: res.Rows,
	}
	for i, fd := range res.FieldDescriptions {
		r.cols[i] = fd.Name
		r.conv[i] = c.converterFor(fd.DataTypeOID)
	}
	return r
}

// converterFor mirrors the value types pgx's stdlib driver hands to database/sql, so a pipeline
// scans exactly what the sequential path scans: integers as int64, floats as float64, bytea/json/xml
// as []byte, date/time types as time.Time, booleans as bool and everything else as its text form.
func (c *conn) converterFor(oid uint32) converter {
	if f, ok := c.conv[oid]; ok {
		return f
	}
	m := c.Conn.Conn().TypeMap()
	const text = pgtype.TextFormatCode
	var f converter
	switch oid {
	case pgtype.BoolOID:
		plan := m.PlanScan(oid, text, new(bool))
		f = func(src []byte) (driver.Value, error) {
			var d bool
			err := plan.Scan(src, &d)
			return d, err
		}
	case pgtype.ByteaOID, pgtype.JSONOID, pgtype.JSONBOID, pgtype.XMLOID:
		plan := m.PlanScan(oid, text, new([]byte))
		f = func(src []byte) (driver.Value, error) {
			var d []byte
			err := plan.Scan(src, &d)
			return d, err
		}
	case pgtype.CIDOID, pgtype.OIDOID, pgtype.XIDOID:
		plan := m.PlanScan(oid, text, new(pgtype.Uint32))
		f = scanValue[pgtype.Uint32](plan)
	case pgtype.DateOID:
		plan := m.PlanScan(oid, text, new(pgtype.Date))
		f = scanValue[pgtype.Date](plan)
	case pgtype.TimestampOID:
		plan := m.PlanScan(oid, text, new(pgtype.Timestamp))
		f = scanValue[pgtype.Timestamp](plan)
	case pgtype.TimestamptzOID:
		plan := m.PlanScan(oid, text, new(pgtype.Timestamptz))
		f = scanValue[pgtype.Timestamptz](plan)
	case pgtype.Float4OID:
		plan := m.PlanScan(oid, text, new(float32))
		f = func(src []byte) (driver.Value, error) {
			var d float32
			err := plan.Scan(src, &d)
			return float64(d), err
		}
	case pgtype.Float8OID:
		plan := m.PlanScan(oid, text, new(float64))
		f = func(src []byte) (driver.Value, error) {
			var d float64
			err := plan.Scan(src, &d)
			return d, err
		}
	case pgtype.Int2OID:
		plan := m.PlanScan(oid, text, new(int16))
		f = func(src []byte) (driver.Value, error) {
			var d int16
			err := plan.Scan(src, &d)
			return int64(d), err
		}
	case pgtype.Int4OID:
		plan := m.PlanScan(oid, text, new(int32))
		f = func(src []byte) (driver.Value, error) {
			var d int32
			err := plan.Scan(src, &d)
			return int64(d), err
		}
	case pgtype.Int8OID:
		plan := m.PlanScan(oid, text, new(int64))
		f = func(src []byte) (driver.Value, error) {
			var d int64
			err := plan.Scan(src, &d)
			return d, err
		}
	default:
		plan := m.PlanScan(oid, text, new(string))
		f = func(src []byte) (driver.Value, error) {
			var d string
			err := plan.Scan(src, &d)
			return d, err
		}
	}
	if c.conv == nil {
		c.conv = make(map[uint32]converter)
	}
	c.conv[oid] = f
	return f
}

// scanValue scans into a fresh T and returns its driver.Value (pgtype's Date, Timestamp, ... types).
func scanValue[T interface{ Value() (driver.Value, error) }](plan pgtype.ScanPlan) converter {
	return func(src []byte) (driver.Value, error) {
		var d T
		if err := plan.Scan(src, &d); err != nil {
			return nil, err
		}
		return d.Value()
	}
}
