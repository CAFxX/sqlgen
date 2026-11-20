// Package pgxwire gives sqlgen's pipelines real single-round-trip execution on PostgreSQL-protocol
// servers (PostgreSQL, CockroachDB, ...) through the pgx driver.
//
// It returns an ordinary *sql.DB. Everything that is not a sqlgen pipeline behaves exactly as with
// pgx's stdlib driver; a sqlgen Pipeline, PipelineTx or Pipeline.Flush additionally sends all of its
// statements, and for PipelineTx and standalone flushes also BEGIN and COMMIT, to the server in ONE
// network round-trip (one write, one Sync). See the sqlgen DOCS.md, section 3.4, for when that path is taken and how to verify it.
//
//	db, err := pgxwire.OpenDB("postgres://user:pass@host/db")
//	q := mydb.New(db)
//	err = q.PipelineTx(ctx, nil, func(p *mydb.Pipeline) error { ...; return nil })
//
// # Statement typing
//
// A first-seen pipeline must not cost an extra describe round-trip, so statements are sent with
// pgx's QueryExecModeExec semantics: unnamed Parse/Bind/Execute, parameters in text format and without
// declared types (the server infers them from context), results in text format decoded here. sqlgen
// already casts the placeholders whose type the server could not otherwise infer. The consequences:
//
//   - pgx's statement cache is not used for pipelines, so the server parses every statement of every
//     flush. That costs server CPU, never a round-trip. Parse-heavy hot paths that gain nothing from
//     batching can stay on the sequential (prepared-statement) path by using a plain *sql.DB.
//   - A Go value is converted by pgx's default type mapping (pgtype.Map), like QueryExecModeExec.
//   - pgx query tracers do not observe pipeline batches (they run below pgx.Conn); use
//     runtime.CacheStats.WirePipelines to confirm the path.
package pgxwire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/CAFxX/sqlgen/runtime"
)

// OpenDB parses dsn (a URL or key/value connection string, as accepted by pgx.ParseConfig) and returns
// a *sql.DB whose connections run sqlgen pipelines in a single round-trip.
func OpenDB(dsn string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(Connector(cfg)), nil
}

// Connector returns a driver.Connector for sql.OpenDB. cfg is copied. Options are those of
// stdlib.GetConnector (for example stdlib.OptionAfterConnect). The connections implement
// runtime.WirePipeliner and runtime.WireTxPipeliner and are already wrapped with runtime.WrapConnector
// so that user-managed transactions (q.WithTx(tx).Pipeline(ctx)) also flush in one round-trip.
func Connector(cfg *pgx.ConnConfig, opts ...stdlib.OptionOpenDB) driver.Connector {
	return runtime.WrapConnector(&connector{base: stdlib.GetConnector(*cfg, opts...)})
}

type connector struct{ base driver.Connector }

func (c *connector) Driver() driver.Driver { return c.base.Driver() }

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	dc, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	sc, ok := dc.(*stdlib.Conn)
	if !ok {
		_ = dc.Close()
		return nil, fmt.Errorf("pgxwire: unexpected connection type %T", dc)
	}
	return &conn{Conn: sc}, nil
}

// conn is a pgx stdlib connection that can also run pipelines. Everything else (prepare, query, exec,
// begin, reset, validity) is promoted from the embedded *stdlib.Conn.
type conn struct {
	*stdlib.Conn
	conv map[uint32]converter // result decoders by type OID, built lazily (a conn is used by one goroutine at a time)
}

var (
	_ runtime.WirePipeliner   = (*conn)(nil)
	_ runtime.WireTxPipeliner = (*conn)(nil)
)

// ExecPipeline sends ops in one round-trip. Inside a transaction the caller began (a user-managed
// *sql.Tx) the ops simply run there and the caller owns commit and rollback. On an idle connection the
// batch is wrapped in BEGIN ... COMMIT, exactly like ExecTxPipeline with default options: atomicity
// then does not depend on the engine's handling of implicit transactions (CockroachDB, for one, rejects
// the transaction-local set_config the condition recording relies on outside an explicit transaction).
func (c *conn) ExecPipeline(ctx context.Context, ops []runtime.PipelineOp) error {
	return c.exec(ctx, false, nil, ops)
}

// ExecTxPipeline sends BEGIN, ops and COMMIT in one round-trip.
func (c *conn) ExecTxPipeline(ctx context.Context, txOpts *sql.TxOptions, ops []runtime.PipelineOp) error {
	return c.exec(ctx, true, txOpts, ops)
}

type stmtRole uint8

const (
	roleBegin stmtRole = iota
	roleOp
	roleCond      // reads back the condition recorded by roleOp's statement
	roleExtraCond // reads back op.ExtraConds[extra]
	roleCommit
)

type stmtRef struct {
	role  stmtRole
	op    *runtime.PipelineOp
	extra int // index into op.ExtraConds for roleExtraCond
}

func (c *conn) exec(ctx context.Context, tx bool, txOpts *sql.TxOptions, ops []runtime.PipelineOp) error {
	pc := c.Conn.Conn()
	if pc.IsClosed() {
		return driver.ErrBadConn
	}
	pg := pc.PgConn()
	if !tx && pg.TxStatus() == 'I' {
		tx = true
	}

	var (
		batch pgconn.Batch
		eqb   pgx.ExtendedQueryBuilder
		refs  = make([]stmtRef, 0, len(ops)+2)
	)
	if tx {
		begin, err := beginSQL(txOpts)
		if err != nil {
			return err
		}
		batch.ExecParams(begin, nil, nil, nil, nil)
		refs = append(refs, stmtRef{role: roleBegin})
	}
	for i := range ops {
		op := &ops[i]
		// Declared parameter types stay empty: the server infers them (QueryExecModeExec semantics), so
		// no describe round-trip is ever needed, even for a statement text pgx has never seen.
		if err := eqb.Build(pc.TypeMap(), nil, op.Args); err != nil {
			return fmt.Errorf("pgxwire: %s: %w", opName(op), err)
		}
		batch.ExecParams(op.SQL, eqb.ParamValues, nil, eqb.ParamFormats, nil)
		refs = append(refs, stmtRef{role: roleOp, op: op})
		if op.Decorators.ProduceCond != nil {
			// The condition was recorded server-side by set_config('sqlgen.c<ID>', ..., true). Read it back
			// inside the same batch: later operations are gated on it by the server, and the Go side needs
			// the value to resolve IfResult.Taken() and the skipped futures.
			batch.ExecParams(condSQL(op.Decorators.CondID), nil, nil, nil, nil)
			refs = append(refs, stmtRef{role: roleCond, op: op})
			for j := range op.ExtraConds {
				batch.ExecParams(condSQL(op.ExtraConds[j].CondID), nil, nil, nil, nil)
				refs = append(refs, stmtRef{role: roleExtraCond, op: op, extra: j})
			}
		}
	}
	if tx {
		batch.ExecParams("COMMIT", nil, nil, nil, nil)
		refs = append(refs, stmtRef{role: roleCommit})
	}

	results := make([]*pgconn.Result, 0, len(refs))
	mrr := pg.ExecBatch(ctx, &batch)
	for mrr.NextResult() {
		res := mrr.ResultReader().Read()
		results = append(results, res)
		if res.Err != nil {
			break
		}
	}
	err := mrr.Close()
	if err == nil && len(results) != len(refs) {
		err = fmt.Errorf("pgxwire: server answered %d of %d statements", len(results), len(refs))
	}
	if err != nil {
		return c.fail(ctx, pg, tx, refs, results, err)
	}
	return c.deliver(ctx, refs, results)
}

// fail leaves the connection clean after a failed batch and translates err for the runtime.
func (c *conn) fail(ctx context.Context, pg *pgconn.PgConn, tx bool, refs []stmtRef, results []*pgconn.Result, err error) error {
	var pgErr *pgconn.PgError
	isServerErr := errors.As(err, &pgErr)

	if tx && !pg.IsClosed() && pg.TxStatus() != 'I' {
		// A statement failed after BEGIN: the server is in an aborted transaction and skipped the rest of
		// the batch (including COMMIT). Roll back; this is the only extra round-trip and only on errors.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if _, rerr := pg.Exec(rctx, "ROLLBACK").ReadAll(); rerr != nil {
			_ = pg.Close(rctx) // database/sql discards it at the next checkout (ResetSession -> ErrBadConn)
		}
		cancel()
	}

	// Statements are answered in order, so the failed one is the first without a clean result.
	failed := len(results)
	if failed > 0 && results[failed-1].Err != nil {
		failed--
	}
	switch {
	case !isServerErr:
		// Transport, protocol or context error. In a transaction the COMMIT may already have been
		// processed, so the outcome is unknown: report it as a commit-time failure (never retried).
		if tx {
			return &runtime.CommitError{Err: err}
		}
		return err
	case failed >= len(refs):
		return err
	}
	switch ref := refs[failed]; ref.role {
	case roleOp:
		return runtime.TranslateWireError(ref.op, err)
	case roleCommit:
		return &runtime.CommitError{Err: err}
	default:
		return err
	}
}

// deliver hands the answers of a successful batch to the operations.
func (c *conn) deliver(ctx context.Context, refs []stmtRef, results []*pgconn.Result) error {
	for i, ref := range refs {
		res := results[i]
		switch ref.role {
		case roleCond:
			ref.op.CondVal, ref.op.CondSet = condValue(res)
		case roleExtraCond:
			ec := &ref.op.ExtraConds[ref.extra]
			ec.Val, ec.Set = condValue(res)
		case roleOp:
			if err := c.deliverOp(ctx, ref.op, res); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *conn) deliverOp(ctx context.Context, op *runtime.PipelineOp, res *pgconn.Result) error {
	switch op.WireResult() {
	case runtime.WireResultCommandTag:
		op.RowCount = res.CommandTag.RowsAffected()
	case runtime.WireResultGuarded:
		op.RowCount = 0
		if len(res.Rows) > 0 && len(res.Rows[0]) > 0 {
			n, err := strconv.ParseInt(string(res.Rows[0][0]), 10, 64)
			if err != nil {
				return fmt.Errorf("pgxwire: %s: affected-row count %q: %w", opName(op), res.Rows[0][0], err)
			}
			op.RowCount = n
		}
	case runtime.WireResultRows:
		sqlRows, err := runtime.RowsFromDriverRows(ctx, c.driverRows(res))
		if err != nil {
			return err
		}
		op.RowCount, err = op.ScanWireRows(sqlRows)
		_ = sqlRows.Close()
		if err != nil {
			return err
		}
	case runtime.WireResultDiscard:
	}
	return nil
}

func condSQL(id int) string {
	return "SELECT current_setting('sqlgen.c" + strconv.Itoa(id) + "', true)"
}

// condValue reads the answer of condSQL: set is false when the condition was never recorded (the
// operation was gated off, so the setting is NULL or empty).
func condValue(res *pgconn.Result) (val, set bool) {
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		return false, false
	}
	switch string(res.Rows[0][0]) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}

func opName(op *runtime.PipelineOp) string {
	if op.Desc != nil && op.Desc.Name != "" {
		return op.Desc.Name
	}
	return "pipeline statement"
}

// beginSQL renders the BEGIN for txOpts with the same isolation mapping as pgx's stdlib driver.
func beginSQL(o *sql.TxOptions) (string, error) {
	if o == nil {
		return "BEGIN", nil
	}
	var sb strings.Builder
	sb.WriteString("BEGIN")
	switch o.Isolation {
	case sql.LevelDefault:
	case sql.LevelReadUncommitted:
		sb.WriteString(" ISOLATION LEVEL READ UNCOMMITTED")
	case sql.LevelReadCommitted:
		sb.WriteString(" ISOLATION LEVEL READ COMMITTED")
	case sql.LevelRepeatableRead, sql.LevelSnapshot:
		sb.WriteString(" ISOLATION LEVEL REPEATABLE READ")
	case sql.LevelSerializable:
		sb.WriteString(" ISOLATION LEVEL SERIALIZABLE")
	default:
		return "", fmt.Errorf("unsupported isolation: %v", o.Isolation)
	}
	if o.ReadOnly {
		sb.WriteString(" READ ONLY")
	}
	return sb.String(), nil
}
