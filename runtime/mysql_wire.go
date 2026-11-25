package runtime

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"
)

// This file is the MySQL / MariaDB flavor of the single-round-trip pipeline (WireFlavorMySQL). Where the
// PostgreSQL flavor folds the guards of an operation into ONE statement built on CTEs and
// transaction-local set_config values, the MySQL protocol has neither, so an operation becomes a short
// statement SEQUENCE over session user variables, all sent in one multi-statement text:
//
//	SET @sqlgen_f=-1; SET @sqlgen_p=-1;                  -- f: first failed guard (-1 none); p: operation in progress
//	START TRANSACTION;                                   -- own transaction only
//	SET @sqlgen_p=0; <op 0>; SET @sqlgen_n0=ROW_COUNT(); -- n: affected / returned rows of the operation
//	SET @sqlgen_p=1; <op 1 gated>; SET @sqlgen_n1=...;
//	  SET @sqlgen_f=IF(@sqlgen_f<0 AND (<gates>) AND NOT (@sqlgen_n1<=>W), 1, @sqlgen_f);  -- assertion
//	  SET @sqlgen_c3=COALESCE(<condition>,0);                                                -- condition
//	SET @sqlgen_p=2; SET @sqlgen_q=IF(@sqlgen_f<0,'COMMIT','ROLLBACK'); PREPARE/EXECUTE/DEALLOCATE;
//	SELECT @sqlgen_f, @sqlgen_n0, ..., @sqlgen_c3;       -- the summary row
//
// A failed assertion or Abort therefore is NOT a server error: later operations are gated off by
// @sqlgen_f<0, the transaction rolls back, and the summary row names the failed operation and its row
// count, from which MySQLBatch.Apply builds exactly the error the sequential path returns. A database
// error of a statement is a real error; @sqlgen_p tells the recovery turn which operation it was.
//
// Every @sqlgen_ variable a statement reads is assigned earlier in the SAME batch (session variables
// persist across batches on a pooled connection, so nothing may depend on a previous value). The
// arguments are inlined as literals by the renderer itself (MySQLFlavor.WriteLiteral), which knows where
// the placeholders are; text is never searched for '?'.

// MySQLFlavor carries what rendering a MySQL-flavored batch needs to know about the server and the
// connection. A driver for another MySQL-protocol server fills it differently.
type MySQLFlavor struct {
	// Loc is the time zone time.Time arguments are rendered in (the driver's loc parameter); nil is UTC.
	Loc *time.Location
	// Charset is the connection character set (utf8mb4 when empty). A string is inlined as a plain quoted
	// literal only when that is unambiguous in this character set, see WriteLiteral.
	Charset string
	// Returning reports that INSERT / DELETE ... RETURNING exists (MariaDB 10.5+). Without it an
	// operation with result columns that is not a SELECT is not applicable to this flavor.
	Returning bool
	// HexLiteralIsNumber reports that the server reads an introduced hex string literal (_utf8mb4 0x...) as
	// a number in a numeric, temporal or JSON context (TiDB ignores the introducer there, where MySQL and
	// MariaDB convert the string). A string that would need that form (see WriteLiteral) is then not
	// inlined: normalize fails with ErrWireNotApplicable and the pipeline runs sequentially.
	HexLiteralIsNumber bool
	// Forwarding reports that the server runs what forwarded values (RowFuture.Arg,
	// ExecFuture.LastInsertIDArg) are made of on this flavor: a user-variable assignment inside an
	// expression (SELECT ..., (@v:=col); deprecated by MySQL 8.0 but still accepted) and DO
	// LAST_INSERT_ID(0). Without it a batch with forwarded values is not applicable (it runs sequentially).
	Forwarding bool
}

const unsafeBackslashCharsets = "big5 cp932 gb2312 gbk sjis gb18030"

func (fl *MySQLFlavor) charset() string {
	if fl.Charset == "" {
		return "utf8mb4"
	}
	return fl.Charset
}

// plainQuotable reports whether a string literal in the connection character set may be written between
// quotes with only the quote doubled: not when the string has a backslash (escape processing depends on
// sql_mode) and not in a character set whose multi-byte characters can end in a backslash byte or a quote.
func (fl *MySQLFlavor) plainQuotable(s string) bool {
	if strings.IndexByte(s, '\\') >= 0 {
		return false
	}
	return !strings.Contains(" "+unsafeBackslashCharsets+" ", " "+fl.charset()+" ")
}

const maxValuerDepth = 8

// resolveArg reduces arg to one of nil, int64, uint64, float64, bool, string, []byte, time.Time (changed
// reports whether that differs from arg itself). driver.Valuer values are resolved first and pointers
// followed, as database/sql does for a prepared statement.
func resolveMySQLArg(arg any, depth int) (v any, changed bool, err error) {
	switch x := arg.(type) {
	case nil, int64, uint64, float64, bool, string, []byte, time.Time:
		return arg, false, nil
	case int:
		return int64(x), true, nil
	case int8:
		return int64(x), true, nil
	case int16:
		return int64(x), true, nil
	case int32:
		return int64(x), true, nil
	case uint:
		return uint64(x), true, nil
	case uint8:
		return uint64(x), true, nil
	case uint16:
		return uint64(x), true, nil
	case uint32:
		return uint64(x), true, nil
	case float32:
		return float64(x), true, nil
	}
	if vr, ok := arg.(driver.Valuer); ok {
		if rv := reflect.ValueOf(arg); rv.Kind() == reflect.Pointer && rv.IsNil() {
			return nil, true, nil
		}
		if depth >= maxValuerDepth {
			return nil, false, errors.New("driver.Valuer chain too deep")
		}
		dv, err := vr.Value()
		if err != nil {
			return nil, false, err
		}
		dv, _, err = resolveMySQLArg(dv, depth+1)
		return dv, true, err
	}
	rv := reflect.ValueOf(arg)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return nil, true, nil
		}
		if depth >= maxValuerDepth {
			return nil, false, errors.New("pointer chain too deep")
		}
		v, _, err := resolveMySQLArg(rv.Elem().Interface(), depth+1)
		return v, true, err
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), true, nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true, nil
	case reflect.Bool:
		return rv.Bool(), true, nil
	case reflect.String:
		return rv.String(), true, nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			if rv.IsNil() {
				return nil, true, nil
			}
			return rv.Bytes(), true, nil
		}
	}
	return nil, false, fmt.Errorf("unsupported argument type %T", arg)
}

// normalize returns args reduced as resolveMySQLArg does (args itself when nothing needs to change, no
// allocation). An error wraps ErrWireNotApplicable.
func (fl *MySQLFlavor) normalize(args []any) ([]any, error) {
	out := args
	for i, a := range args {
		if _, isRef := a.(*argRef); isRef {
			continue // written as the producer's session variable (writeArg)
		}
		v, changed, err := resolveMySQLArg(a, 0)
		if err == nil {
			switch x := v.(type) {
			case float64:
				if math.IsNaN(x) || math.IsInf(x, 0) {
					err = errors.New("NaN and Inf cannot be written as a literal")
				}
			case time.Time:
				loc := fl.Loc
				if loc == nil {
					loc = time.UTC
				}
				if y := x.In(loc).Year(); !x.IsZero() && (y < 0 || y > 9999) {
					err = errors.New("year out of range")
				}
			case string:
				if fl.HexLiteralIsNumber && x != "" && !fl.plainQuotable(x) {
					err = errors.New("the string needs a hex literal, which this server reads as a number")
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("argument %d: %v: %w", i+1, err, ErrWireNotApplicable)
		}
		if changed {
			if len(out) > 0 && &out[0] == &args[0] {
				out = append([]any(nil), args...)
			}
			out[i] = v
		}
	}
	return out, nil
}

// WriteLiteral writes the SQL literal of arg, which must come from normalize (nil, int64, uint64,
// float64, bool, string, []byte, time.Time). The rules were established by differential tests against the
// prepared-statement path on MySQL 8.4/9.7 and MariaDB 10.11/12.3:
//
//   - integers decimal, bool 1/0, floats with the shortest round-tripping exponent form;
//   - time.Time in Loc with every fractional digit ('2006-01-02 15:04:05.999999999'), the zero time as
//     the zero date;
//   - []byte as 0x<hex> (the empty one as ”), NULL for a nil one;
//   - strings between quotes with the quote doubled, unless the string has a backslash or the connection
//     character set is one of big5 cp932 gb2312 gbk sjis gb18030: then as _<charset> 0x<hex>, which no
//     sql_mode or multi-byte character can misread (but which gives the literal the default collation of
//     the character set, so a comparison against a column of another collation of that set can fail with
//     error 1267; use a plain string without a backslash there).
func (fl *MySQLFlavor) WriteLiteral(b *strings.Builder, arg any) {
	switch x := arg.(type) {
	case nil:
		b.WriteString("NULL")
	case int64:
		var buf [24]byte
		b.Write(strconv.AppendInt(buf[:0], x, 10))
	case uint64:
		var buf [24]byte
		b.Write(strconv.AppendUint(buf[:0], x, 10))
	case bool:
		if x {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	case float64:
		var buf [40]byte
		b.Write(strconv.AppendFloat(buf[:0], x, 'e', -1, 64))
	case string:
		fl.writeString(b, x)
	case []byte:
		switch {
		case x == nil:
			b.WriteString("NULL")
		case len(x) == 0:
			b.WriteString("''")
		case fl.utf8() && utf8.Valid(x):
			// Text in the connection character set travels as a string, exactly as the prepared path sends
			// it (the server converts it for the column: 0x3132 would be the NUMBER 12594 in an integer
			// column, and a JSON column refuses a binary string).
			fl.writeString(b, unsafe.String(&x[0], len(x)))
		default:
			b.WriteString("_binary 0x")
			writeHexBytes(b, x)
		}
	case time.Time:
		if x.IsZero() {
			b.WriteString("'0000-00-00'")
			return
		}
		loc := fl.Loc
		if loc == nil {
			loc = time.UTC
		}
		var buf [40]byte
		b.WriteByte('\'')
		x = x.In(loc)
		if h, m, sec := x.Clock(); h|m|sec|x.Nanosecond() == 0 {
			// the prepared path (go-sql-driver) writes a time without a time of day as a bare date, which
			// is what a character or binary column stores
			b.Write(x.AppendFormat(buf[:0], "2006-01-02"))
		} else {
			b.Write(x.AppendFormat(buf[:0], "2006-01-02 15:04:05.999999999"))
		}
		b.WriteByte('\'')
	}
}

// utf8 reports whether the connection character set is a UTF-8 one.
func (fl *MySQLFlavor) utf8() bool {
	switch fl.charset() {
	case "utf8mb4", "utf8mb3", "utf8":
		return true
	}
	return false
}

// writeString writes a string literal (see WriteLiteral for the rules).
func (fl *MySQLFlavor) writeString(b *strings.Builder, x string) {
	switch {
	case x == "":
		b.WriteString("''")
	case fl.plainQuotable(x):
		b.WriteByte('\'')
		for {
			i := strings.IndexByte(x, '\'')
			if i < 0 {
				break
			}
			b.WriteString(x[:i+1])
			b.WriteByte('\'')
			x = x[i+1:]
		}
		b.WriteString(x)
		b.WriteByte('\'')
	default:
		b.WriteByte('_')
		b.WriteString(fl.charset())
		b.WriteString(" 0x")
		writeHex(b, x)
	}
}

func writeHex(b *strings.Builder, s string) {
	const digits = "0123456789abcdef"
	for i := 0; i < len(s); i++ {
		b.WriteByte(digits[s[i]>>4])
		b.WriteByte(digits[s[i]&15])
	}
}

func writeHexBytes(b *strings.Builder, p []byte) {
	const digits = "0123456789abcdef"
	for _, c := range p {
		b.WriteByte(digits[c>>4])
		b.WriteByte(digits[c&15])
	}
}

// literalBound bounds the bytes WriteLiteral writes for a normalized arg.
func (fl *MySQLFlavor) literalBound(arg any) int {
	switch x := arg.(type) {
	case nil:
		return 4
	case int64, uint64:
		return 20
	case bool:
		return 1
	case float64:
		return 32
	case string:
		return 2*len(x) + len(fl.charset()) + 8
	case []byte:
		return 2*len(x) + len(fl.charset()) + 12
	case time.Time:
		return 40
	case *argRef:
		return 32
	}
	return 0
}

// mysqlArgs is the state shared by the cursors that render the placeholders of one operation.
type mysqlArgs struct {
	fl   *MySQLFlavor
	args []any
	err  error
}

// mysqlCursor is the MySQL-flavor part of a streamCursor.
type mysqlCursor struct {
	st *mysqlArgs
	// failGate: an earlier operation of the batch may have failed an assertion or aborted, so every
	// statement of this operation must also be gated on @sqlgen_f<0.
	failGate bool
	// keepResultSet: the statement is a DELETE ... RETURNING. MariaDB answers one whose WHERE is constantly
	// false ("Impossible WHERE": a closed gate is a constant, @sqlgen_c1=1) with a bare OK packet and no
	// (empty) result set, which would shift every later result set of the batch. A gate that the
	// optimizer cannot fold keeps the result set.
	keepResultSet bool
}

// forSub returns the cursor for a nested query: same arguments, but only the operation's own statement
// carries the failure gate.
func (m *mysqlCursor) forSub() *mysqlCursor {
	if m == nil {
		return nil
	}
	return &mysqlCursor{st: m.st}
}

// writeArg writes the literal of the idx-th (1-based) argument.
func (m *mysqlCursor) writeArg(b *strings.Builder, idx int) {
	if idx < 1 || idx > len(m.st.args) {
		if m.st.err == nil {
			m.st.err = fmt.Errorf("placeholder %d has no argument (%d given): %w", idx, len(m.st.args), ErrWireNotApplicable)
		}
		b.WriteString("NULL")
		return
	}
	if r, ok := m.st.args[idx-1].(*argRef); ok {
		b.WriteString("@sqlgen_v")
		b.WriteString(strconv.Itoa(r.id))
		return
	}
	m.st.fl.WriteLiteral(b, m.st.args[idx-1])
}

func (m *mysqlCursor) writeGates(b *strings.Builder, gates []BranchGate, numBuf []byte) {
	if m.keepResultSet && (m.failGate || len(gates) > 0) {
		b.WriteByte('(')
		defer b.WriteString(" OR RAND()<0)")
	}
	first := true
	if m.failGate {
		b.WriteString("@sqlgen_f<0")
		first = false
	}
	for _, g := range gates {
		if !first {
			b.WriteString(" AND ")
		}
		first = false
		b.WriteString("@sqlgen_c")
		b.Write(strconv.AppendInt(numBuf, int64(g.CondID), 10))
		if g.WantTrue {
			b.WriteString("=1")
		} else {
			b.WriteString("=0")
		}
	}
}

// MySQLRenderOptions parameterises RenderMySQLBatch.
type MySQLRenderOptions struct {
	Flavor *MySQLFlavor
	// InTx: the connection is inside a transaction the caller began. The batch then has no START
	// TRANSACTION / COMMIT, and an operation with an assertion runs after a savepoint so that a failed
	// assertion can undo that operation (as the PostgreSQL flavor does) and leave the transaction usable.
	InTx bool
	// TxOpts are the isolation level and read-only mode of the transaction the batch begins (own
	// transaction only).
	TxOpts *sql.TxOptions
}

type sumKind uint8

const (
	sumFail  sumKind = iota // @sqlgen_f
	sumCount                // @sqlgen_n<op>
	sumCond                 // @sqlgen_c<CondID> of op.Decorators.ProduceCond
	sumExtra                // @sqlgen_c<CondID> of op.ExtraConds[extra]
	sumRef                  // @sqlgen_r: the ID of the unresolved forwarded value of a failed consumer
)

type sumCol struct {
	kind  sumKind
	op    int
	extra int
}

// MySQLBatch is a rendered MySQL-flavor batch.
type MySQLBatch struct {
	// SQL is the whole multi-statement text. PipelineOp.SQL of every operation is set to the part of it
	// that belongs to that operation.
	SQL string

	nOps     int
	own      bool // the batch begins and ends its own transaction
	sets     []int
	flags    []uint8 // per operation: opTrim, opSavepoint
	prepared bool
	sum      []sumCol
}

const (
	opTrim      uint8 = 1 << iota // the result set has the RETURNING counter as an extra column
	opSavepoint                   // a savepoint is taken right before the operation
	opTrimVal                     // the result set has a forwarded value's assignment as an extra column
)

// ResultSetOps returns the indices of the operations whose rows the server returns, in the order of the
// result sets that carry them. The summary row (if HasSummary) is the result set after the last of them.
func (m *MySQLBatch) ResultSetOps() []int { return m.sets }

// TrimColumns returns how many columns the result set of operation i has beyond the operation's own (the
// counter that tells how many rows a RETURNING statement returned, the assignment that records a
// forwarded value). They are the last columns; drop them.
func (m *MySQLBatch) TrimColumns(i int) int {
	n := 0
	if m.flags[i]&opTrim != 0 {
		n++
	}
	if m.flags[i]&opTrimVal != 0 {
		n++
	}
	return n
}

// HasSummary reports whether the batch ends with the summary row.
func (m *MySQLBatch) HasSummary() bool { return len(m.sum) > 0 }

// SummaryLen is the number of columns of the summary row.
func (m *MySQLBatch) SummaryLen() int { return len(m.sum) }

// CommitPos is the value @sqlgen_p has while the COMMIT of an own-transaction batch runs; -1 is the value
// before the first operation (BEGIN), and 0..n-1 are the operations.
func (m *MySQLBatch) CommitPos() int { return m.nOps }

// OwnsTx reports whether the batch begins and ends its transaction.
func (m *MySQLBatch) OwnsTx() bool { return m.own }

// Savepoint returns the name of the savepoint taken before operation i, if there is one.
func (m *MySQLBatch) Savepoint(i int) (string, bool) {
	if i < 0 || i >= len(m.flags) || m.flags[i]&opSavepoint == 0 {
		return "", false
	}
	return mysqlSavepointName(i), true
}

// UsesPrepare reports whether the batch ends its transaction through a server-side prepared statement
// (it has a guard that can fail), which a failed COMMIT leaves behind.
func (m *MySQLBatch) UsesPrepare() bool { return m.own && m.prepared }

func mysqlSavepointName(i int) string { return "sqlgen_sp" + strconv.Itoa(i) }

// Apply feeds the summary row to the operations: affected-row counts, recorded conditions, and the
// first failed guard. For a failed guard it returns the index of the operation and the error the
// sequential path returns (op.AbortErr, or an *AssertionError with the row count), which it also stores
// in op.Err; otherwise (-1, nil). row holds the columns of the summary row.
func (m *MySQLBatch) Apply(ops []PipelineOp, row []any) (failed int, err error) {
	if len(row) != len(m.sum) {
		return -1, fmt.Errorf("sqlgen: summary row has %d columns, want %d", len(row), len(m.sum))
	}
	failed = -1
	refID := 0
	for i, c := range m.sum {
		v, ok := summaryInt(row[i])
		if !ok {
			return -1, fmt.Errorf("sqlgen: summary column %d is %v, want an integer", i, row[i])
		}
		switch c.kind {
		case sumRef:
			refID = int(v)
		case sumFail:
			failed = int(v)
		case sumCount:
			if op := &ops[c.op]; !op.HasResultCols {
				op.RowCount = v
			}
		case sumCond:
			ops[c.op].CondVal, ops[c.op].CondSet = v != 0, true
		case sumExtra:
			ec := &ops[c.op].ExtraConds[c.extra]
			ec.Val, ec.Set = v != 0, true
		}
	}
	if failed < 0 {
		return -1, nil
	}
	if failed >= len(ops) && failed < 2*len(ops) {
		// A consumer whose forwarded value is unresolved (it did not run): failed is len(ops) + its index.
		failed -= len(ops)
		op := &ops[failed]
		r := op.refByID(refID)
		if r == nil {
			return -1, fmt.Errorf("sqlgen: summary names forwarded value %d, which operation %d does not use", refID, failed)
		}
		var got int64
		known := false
		if j := opIndex(ops, r.op); j >= 0 {
			for i, c := range m.sum {
				if c.kind == sumCount && c.op == j {
					got, known = summaryInt(row[i])
				}
			}
		}
		aerr := r.argError(op, got, known)
		op.Err = aerr
		return failed, aerr
	}
	if failed >= len(ops) {
		return -1, fmt.Errorf("sqlgen: summary names operation %d of %d as failed", failed, len(ops))
	}
	op := &ops[failed]
	if op.Decorators.IsAbort {
		op.Err = op.AbortErr
		return failed, op.AbortErr
	}
	aerr := &AssertionError{Statement: op.stmtName(), Op: op.opIdx, Label: op.label, Want: op.Decorators.AssertRows}
	for i, c := range m.sum {
		if c.kind == sumCount && c.op == failed {
			if n, ok := summaryInt(row[i]); ok {
				aerr.Got, aerr.GotKnown = n, true
			}
		}
	}
	op.Err = aerr
	return failed, aerr
}

// opIndex returns the index in ops of the operation op (a pointer to the pipeline's own copy), or -1.
func opIndex(ops []PipelineOp, op *PipelineOp) int {
	for j := range ops {
		if ops[j].opIdx == op.opIdx {
			return j
		}
	}
	return -1
}

func summaryInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case uint64:
		return int64(x), true
	case float64:
		return int64(x), true
	case []byte:
		n, err := strconv.ParseInt(string(x), 10, 64)
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	}
	return 0, false
}

// mysqlBeginSQL renders the statements that begin the transaction of an own-transaction batch.
func mysqlBeginSQL(b *strings.Builder, o *sql.TxOptions) error {
	if o != nil {
		switch o.Isolation {
		case sql.LevelDefault:
		case sql.LevelReadUncommitted:
			b.WriteString("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
		case sql.LevelReadCommitted:
			b.WriteString("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
		case sql.LevelRepeatableRead:
			b.WriteString("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		case sql.LevelSerializable:
			b.WriteString("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
		default:
			return fmt.Errorf("isolation level %v has no MySQL equivalent: %w", o.Isolation, ErrWireNotApplicable)
		}
	}
	b.WriteString("START TRANSACTION")
	if o != nil && o.ReadOnly {
		b.WriteString(" READ ONLY")
	}
	b.WriteByte(';')
	return nil
}

// condCounts reports, for an operation, whether its row count must be recorded server-side and how many
// ColPred leaves its conditions evaluate over a second copy of the statement.
func (op *PipelineOp) mysqlNeedsCount() bool {
	return !op.HasResultCols || op.Decorators.guarded()
}

type mysqlBatchRender struct {
	b      strings.Builder
	o      *MySQLRenderOptions
	ops    []PipelineOp
	args   [][]any
	batch  *MySQLBatch
	numBuf [24]byte
	spans  []int // start, end of every operation's SQL in the final text
	cur    streamCursor
	my     mysqlCursor
	st     mysqlArgs
}

func (r *mysqlBatchRender) num(n int) { r.b.Write(strconv.AppendInt(r.numBuf[:0], int64(n), 10)) }

// RenderMySQLBatch renders ops as one multi-statement text for WireFlavorMySQL and sets op.SQL of every
// operation to its part of it. Nothing is executed. An error wraps ErrWireNotApplicable: the batch cannot
// be expressed for this flavor (an argument that cannot be inlined, RETURNING without support, a
// transaction option without equivalent) and the caller must not send anything.
func RenderMySQLBatch(ops []PipelineOp, o MySQLRenderOptions) (*MySQLBatch, error) {
	if o.Flavor == nil {
		return nil, errors.New("sqlgen: RenderMySQLBatch needs a MySQLFlavor")
	}
	r := &mysqlBatchRender{
		o:   &o,
		ops: ops,
		batch: &MySQLBatch{
			nOps:  len(ops),
			own:   !o.InTx,
			flags: make([]uint8, len(ops)),
		},
	}
	// Reduce the arguments first: it validates them (nothing is rendered for a batch that cannot be
	// sent) and lets the estimate know the literal sizes.
	r.args = make([][]any, len(ops))
	r.spans = make([]int, 2*len(ops))
	est := 1024
	for i := range ops {
		a, err := o.Flavor.normalize(ops[i].Args)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", opLabel(&ops[i]), err)
		}
		r.args[i] = a
		est += estimateMySQLOpBytes(o.Flavor, &ops[i], a)
	}
	r.b.Grow(est)

	hasFail, hasRefs := false, false
	for i := range ops {
		if ops[i].Decorators.IsAbort || ops[i].Decorators.AssertRows >= 0 || ops[i].nRefs > 0 {
			hasFail = true
		}
		if ops[i].nRefs > 0 || len(ops[i].Decorators.ValRecs) > 0 {
			if !o.Flavor.Forwarding {
				return nil, fmt.Errorf("%s: the server does not run what forwarded values are made of: %w", opLabel(&ops[i]), ErrWireNotApplicable)
			}
			hasRefs = hasRefs || ops[i].nRefs > 0
		}
	}
	r.b.WriteString("SET @sqlgen_p=-1;")
	if hasFail {
		r.b.WriteString("SET @sqlgen_f=-1;")
		r.batch.sum = append(r.batch.sum, sumCol{kind: sumFail})
	}
	if hasRefs {
		r.b.WriteString("SET @sqlgen_r=0;")
		r.batch.sum = append(r.batch.sum, sumCol{kind: sumRef})
	}
	if r.batch.own {
		if err := mysqlBeginSQL(&r.b, o.TxOpts); err != nil {
			return nil, err
		}
	}

	failGate := false
	for i := range ops {
		r.spans[2*i] = r.b.Len()
		if ops[i].nRefs > 0 {
			failGate = true // the consumer itself is gated on its own check
		}
		if err := r.renderOp(i, &ops[i], failGate); err != nil {
			return nil, fmt.Errorf("%s: %w", opLabel(&ops[i]), err)
		}
		r.spans[2*i+1] = r.b.Len()
		if ops[i].Decorators.IsAbort || ops[i].Decorators.AssertRows >= 0 {
			failGate = true
		}
	}

	if r.batch.own {
		r.b.WriteString("SET @sqlgen_p=")
		r.num(len(ops))
		r.b.WriteByte(';')
		if hasFail {
			r.batch.prepared = true
			r.b.WriteString("SET @sqlgen_q=IF(@sqlgen_f<0,'COMMIT','ROLLBACK');PREPARE sqlgen_s FROM @sqlgen_q;EXECUTE sqlgen_s;DEALLOCATE PREPARE sqlgen_s;")
		} else {
			r.b.WriteString("COMMIT;")
		}
	}
	if len(r.batch.sum) > 0 {
		r.b.WriteString("SELECT ")
		for i, c := range r.batch.sum {
			if i > 0 {
				r.b.WriteByte(',')
			}
			switch c.kind {
			case sumFail:
				r.b.WriteString("@sqlgen_f")
			case sumCount:
				r.b.WriteString("@sqlgen_n")
				r.num(c.op)
			case sumCond:
				r.b.WriteString("@sqlgen_c")
				r.num(ops[c.op].Decorators.CondID)
			case sumExtra:
				r.b.WriteString("@sqlgen_c")
				r.num(ops[c.op].ExtraConds[c.extra].CondID)
			case sumRef:
				r.b.WriteString("@sqlgen_r")
			}
		}
		r.b.WriteByte(';')
	}
	r.batch.SQL = r.b.String()
	for i := range ops {
		ops[i].SQL = r.batch.SQL[r.spans[2*i]:r.spans[2*i+1]]
	}
	return r.batch, nil
}
