package runtime

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"
)

// --- literals -------------------------------------------------------------------------------------

type valuerOf struct{ v any }

func (v valuerOf) Value() (driver.Value, error) { return v.v, nil }

type namedInt int32
type namedStr string

func renderLit(t *testing.T, fl *MySQLFlavor, arg any) string {
	t.Helper()
	norm, err := fl.normalize([]any{arg})
	if err != nil {
		t.Fatalf("normalize(%#v): %v", arg, err)
	}
	var b strings.Builder
	fl.WriteLiteral(&b, norm[0])
	if bound := fl.literalBound(norm[0]); len(b.String()) > bound {
		t.Errorf("literalBound(%#v) = %d < %d (%s)", arg, bound, b.Len(), b.String())
	}
	return b.String()
}

func TestMySQLLiterals(t *testing.T) {
	rome, _ := time.LoadLocation("Europe/Rome")
	fl := &MySQLFlavor{}
	gbk := &MySQLFlavor{Charset: "gbk"}
	romeFl := &MySQLFlavor{Loc: rome}
	ptr := func(v int) *int { return &v }
	cases := []struct {
		name string
		fl   *MySQLFlavor
		in   any
		want string
	}{
		{"nil", fl, nil, "NULL"},
		{"int", fl, 42, "42"},
		{"int64 min", fl, int64(math.MinInt64), "-9223372036854775808"},
		{"uint64 max", fl, uint64(math.MaxUint64), "18446744073709551615"},
		{"uint8", fl, uint8(7), "7"},
		{"named int", fl, namedInt(-3), "-3"},
		{"bool true", fl, true, "1"},
		{"bool false", fl, false, "0"},
		{"float", fl, 0.1, "1e-01"},
		{"float32 is widened like the driver does", fl, float32(0.5), "5e-01"},
		{"negative zero", fl, math.Copysign(0, -1), "-0e+00"},
		{"string", fl, "plain", "'plain'"},
		{"named string", fl, namedStr("x"), "'x'"},
		{"empty string", fl, "", "''"},
		{"quote doubled", fl, "it's", "'it''s'"},
		{"only quotes", fl, "'''", "''''''''"},
		{"semicolon and comment stay inside the literal", fl, "a; -- /* ? */ b", "'a; -- /* ? */ b'"},
		{"double quote untouched", fl, `say "hi"`, `'say "hi"'`},
		{"backslash goes through hex", fl, `a\b`, "_utf8mb4 0x615c62"},
		{"backslash then quote", fl, `\'`, "_utf8mb4 0x5c27"},
		{"unsafe charset goes through hex", gbk, "abc", "_gbk 0x616263"},
		{"bytes", fl, []byte{0, 1, 0xff}, "0x0001ff"},
		{"empty bytes", fl, []byte{}, "''"},
		{"nil bytes", fl, []byte(nil), "NULL"},
		{"named bytes", fl, namedBytes("hi"), "0x6869"},
		{"nil pointer", fl, (*int)(nil), "NULL"},
		{"pointer", fl, ptr(5), "5"},
		{"valuer", fl, valuerOf{"x'y"}, "'x''y'"},
		{"valuer chain", fl, valuerOf{valuerOf{int64(9)}}, "9"},
		{"sql.NullString valid", fl, sql.NullString{String: "s", Valid: true}, "'s'"},
		{"sql.NullInt64 null", fl, sql.NullInt64{}, "NULL"},
		{"time UTC keeps every digit", fl, time.Date(2024, 2, 29, 23, 59, 59, 123456789, time.UTC), "'2024-02-29 23:59:59.123456789'"},
		{"time without fraction", fl, time.Date(2024, 2, 29, 23, 59, 59, 0, time.UTC), "'2024-02-29 23:59:59'"},
		{"time converted to Loc", romeFl, time.Date(2024, 7, 1, 10, 0, 0, 0, time.UTC), "'2024-07-01 12:00:00'"},
		{"zero time", fl, time.Time{}, "'0000-00-00 00:00:00.000000'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := renderLit(t, tc.fl, tc.in); got != tc.want {
				t.Fatalf("literal = %s, want %s", got, tc.want)
			}
		})
	}
}

type namedBytes []byte

type recursiveValuer struct{}

func (recursiveValuer) Value() (driver.Value, error) { return recursiveValuer{}, nil }

type failingValuer struct{}

func (failingValuer) Value() (driver.Value, error) { return nil, errors.New("boom") }

type selfPtr struct{ next *selfPtr }

func TestMySQLArgumentsThatCannotBeInlined(t *testing.T) {
	fl := &MySQLFlavor{}
	for name, arg := range map[string]any{
		"NaN":                 math.NaN(),
		"+Inf":                math.Inf(1),
		"-Inf":                math.Inf(-1),
		"struct":              struct{ A int }{1},
		"map":                 map[string]int{"a": 1},
		"slice of ints":       []int{1, 2},
		"chan":                make(chan int),
		"uuid array":          [16]byte{},
		"year 10000":          time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		"negative year":       time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
		"valuer error":        failingValuer{},
		"valuer chain":        recursiveValuer{},
		"pointer to struct":   &struct{ A int }{1},
		"pointer chain":       func() any { p := &selfPtr{}; p.next = p; return p }(),
		"valuer of a NaN":     valuerOf{math.NaN()},
		"valuer of a struct":  valuerOf{struct{}{}},
		"pointer to a valuer": &failingValuer{},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := fl.normalize([]any{1, arg})
			if !errors.Is(err, ErrWireNotApplicable) {
				t.Fatalf("normalize = %v, want ErrWireNotApplicable", err)
			}
			if !strings.Contains(err.Error(), "argument 2") {
				t.Fatalf("error does not name the argument: %v", err)
			}
		})
	}
}

func TestMySQLNormalizeAllocatesOnlyWhenNeeded(t *testing.T) {
	fl := &MySQLFlavor{}
	args := []any{int64(1), "x", []byte("b"), nil, true, 2.5, time.Time{}, uint64(3)}
	out, err := fl.normalize(args)
	if err != nil || &out[0] != &args[0] {
		t.Fatalf("plain arguments must be returned as they are: %v", err)
	}
	if n := testing.AllocsPerRun(100, func() { _, _ = fl.normalize(args) }); n != 0 {
		t.Fatalf("normalize of plain arguments allocates %v times", n)
	}
	mixed := []any{1, "x"}
	out, err = fl.normalize(mixed)
	if err != nil || out[0] != int64(1) || mixed[0] != 1 {
		t.Fatalf("normalize must copy, not modify, the caller's arguments: %v %v %v", out, mixed, err)
	}
}

// --- shape ----------------------------------------------------------------------------------------

type shapeDesc struct {
	name   string
	desc   *QueryDescriptor
	args   int // placeholders, with in.SliceLens / TupleLens as below
	in     RenderInput
	hasRes bool
}

func shapeDescs() []shapeDesc {
	return []shapeDesc{
		{name: "select", hasRes: true, args: 1,
			desc: &QueryDescriptor{Name: "sel", StmtVerb: VerbSelect, Segments: []string{"SELECT id FROM t WHERE id = ", ""}, Slots: []Slot{{Kind: SlotScalar}}}},
		{name: "update", args: 4, in: RenderInput{SliceLens: []int{2}},
			desc: &QueryDescriptor{Name: "upd", StmtVerb: VerbUpdate,
				Segments: []string{"UPDATE t SET score = score + ", "\nWHERE ", "id IN (", ") AND org_id = ", "", ""},
				Slots:    []Slot{{Kind: SlotScalar}, {Kind: SlotBranchGuardOpen}, {Kind: SlotSlice}, {Kind: SlotScalar}, {Kind: SlotBranchGuardClose}}, NumSlices: 1}},
		{name: "update-no-where", args: 1,
			desc: &QueryDescriptor{Name: "all", StmtVerb: VerbUpdate, Segments: []string{"UPDATE t SET status = ", "", ""}, Slots: []Slot{{Kind: SlotScalar}, {Kind: SlotBranchGuardWhere}}}},
		{name: "delete", args: 2, in: RenderInput{SliceLens: []int{2}},
			desc: &QueryDescriptor{Name: "del", StmtVerb: VerbDelete, Segments: []string{"DELETE FROM t WHERE ", "id IN (", ")", ""},
				Slots: []Slot{{Kind: SlotBranchGuardOpen}, {Kind: SlotSlice}, {Kind: SlotBranchGuardClose}}, NumSlices: 1}},
		{name: "insert-values", args: 3,
			desc: &QueryDescriptor{Name: "ins", StmtVerb: VerbInsert, Segments: []string{"INSERT INTO t (id, name, score)\n", "", ", ", ", ", "", ""},
				Slots: []Slot{{Kind: SlotInsertValuesOpen}, {Kind: SlotScalar}, {Kind: SlotScalar}, {Kind: SlotScalar}, {Kind: SlotInsertValuesClose}}}},
		{name: "insert-tuples", args: 6, in: RenderInput{TupleLens: []int{2}},
			desc: &QueryDescriptor{Name: "insn", StmtVerb: VerbInsert, Segments: []string{"INSERT INTO t (id, name, score)\n", "", " ON DUPLICATE KEY UPDATE score = VALUES(score)", ""},
				Slots: []Slot{{Kind: SlotInsertTupleValuesOpen}, {Kind: SlotTupleSlice, Tuple: &TupleDescriptor{Segments: []string{"", ", ", ", ", ""}, NumFields: 3}}, {Kind: SlotInsertTupleValuesClose}}, NumTuples: 1}},
	}
}

func shapeArgs(n int) []any {
	pool := []any{int64(7), "o'k", []byte{1, 2}, nil, 2.5, true, time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)}
	out := make([]any, n)
	for i := range out {
		out[i] = pool[i%len(pool)]
	}
	return out
}

func mkOp(sd shapeDesc, gates []BranchGate, dec OpDecorators) PipelineOp {
	in := sd.in
	in.BranchGates = gates
	return PipelineOp{Desc: sd.desc, Input: in, Decorators: dec, Args: shapeArgs(sd.args), HasResultCols: sd.hasRes}
}

var (
	sqlgenVarRe  = regexp.MustCompile(`@sqlgen_[a-z]+[0-9]*`)
	setVarRe     = regexp.MustCompile(`^SET (@sqlgen_[a-z]+[0-9]*)=(.*)$`)
	prepareRe    = regexp.MustCompile(`^PREPARE|^EXECUTE|^DEALLOCATE`)
	savepointRe  = regexp.MustCompile(`^SAVEPOINT sqlgen_sp(\d+)$`)
	summaryRe    = regexp.MustCompile(`^SELECT (@sqlgen_.*)$`)
	setCounterRe = regexp.MustCompile(`^SET @sqlgen_k=0$`)
)

// checkMySQLBatch pins the structural invariants every rendered batch must have, whatever the statement
// shapes: a variable is only read after the same batch assigned it (session variables outlive a batch on
// a pooled connection), one assignment per SET, the summary names variables the batch assigned, and the
// transaction brackets and savepoints are what the mode requires.
func checkMySQLBatch(t *testing.T, name string, b *MySQLBatch, ops []PipelineOp, fl *MySQLFlavor, inTx bool) {
	t.Helper()
	stmts := strings.Split(strings.TrimSuffix(b.SQL, ";"), ";")
	assigned := map[string]bool{}
	nAssign := map[string]int{}
	savepoints := map[int]bool{}
	begins, commits := 0, 0
	if stmts[0] != "SET @sqlgen_p=-1" {
		t.Errorf("%s: the batch must start by resetting @sqlgen_p:\n%s", name, b.SQL)
	}
	for _, s := range stmts {
		if m := setVarRe.FindStringSubmatch(s); m != nil && hasTopLevelComma(m[2]) {
			t.Errorf("%s: more than one assignment in one SET (MySQL evaluates them left to right with the old values): %s", name, s)
		}
		rhs := s
		if m := setVarRe.FindStringSubmatch(s); m != nil {
			rhs = m[2]
		}
		for _, v := range sqlgenVarRe.FindAllString(rhs, -1) {
			if !assigned[v] {
				t.Errorf("%s: %s is read before this batch assigned it: %s\n%s", name, v, s, b.SQL)
			}
		}
		if m := setVarRe.FindStringSubmatch(s); m != nil {
			assigned[m[1]] = true
			nAssign[m[1]]++
		}
		if m := savepointRe.FindStringSubmatch(s); m != nil {
			var i int
			fmt.Sscan(m[1], &i)
			savepoints[i] = true
		}
		if s == "START TRANSACTION" || strings.HasPrefix(s, "START TRANSACTION READ") {
			begins++
		}
		if s == "COMMIT" || strings.Contains(s, "'COMMIT'") {
			commits++
		}
		if m := summaryRe.FindStringSubmatch(s); m != nil {
			for _, v := range strings.Split(m[1], ",") {
				if !assigned[v] {
					t.Errorf("%s: the summary selects %s which the batch never assigned", name, v)
				}
			}
		}
		if inTx && (prepareRe.MatchString(s) || s == "COMMIT" || s == "ROLLBACK") {
			t.Errorf("%s: a batch inside the caller's transaction must not end it: %s", name, s)
		}
	}
	for v, n := range nAssign {
		if strings.HasPrefix(v, "@sqlgen_n") && n != 1 {
			t.Errorf("%s: %s assigned %d times", name, v, n)
		}
	}
	if inTx && begins != 0 || !inTx && (begins != 1 || commits != 1) {
		t.Errorf("%s: START TRANSACTION x%d, COMMIT x%d for inTx=%v:\n%s", name, begins, commits, inTx, b.SQL)
	}
	var wantSets int
	for i := range ops {
		op := &ops[i]
		if !assigned[fmt.Sprintf("@sqlgen_p")] {
			t.Errorf("%s: @sqlgen_p never assigned", name)
		}
		if op.HasResultCols && !op.Decorators.IsAbort {
			wantSets++
		}
		_, hasSP := b.Savepoint(i)
		if want := inTx && op.Decorators.AssertRows >= 0 && !op.Decorators.IsAbort; hasSP != want || savepoints[i] != want {
			t.Errorf("%s: op %d savepoint = %v/%v, want %v", name, i, hasSP, savepoints[i], want)
		}
		if op.SQL == "" || !strings.Contains(b.SQL, op.SQL) {
			t.Errorf("%s: op %d SQL is not a part of the batch", name, i)
		}
		if est := estimateMySQLOpBytes(fl, op, op.Args); len(op.SQL) > est {
			t.Errorf("%s: op %d estimate %d < rendered %d:\n%s", name, i, est, len(op.SQL), op.SQL)
		}
	}
	if len(b.ResultSetOps()) != wantSets {
		t.Errorf("%s: %d result sets, want %d", name, len(b.ResultSetOps()), wantSets)
	}
}

func TestMySQLShapeMatrix(t *testing.T) {
	fl := &MySQLFlavor{Returning: true}
	rows := func(k CondKind, n int64) *PipelineCond { return &PipelineCond{Kind: k, Threshold: n} }
	type condShape struct {
		build func(self *PipelineOp) (*PipelineCond, []ProducedCond)
		pred  bool
	}
	condShapes := map[string]condShape{
		"none": {build: func(*PipelineOp) (*PipelineCond, []ProducedCond) { return nil, nil }},
		"single": {build: func(self *PipelineOp) (*PipelineCond, []ProducedCond) {
			c := rows(CondRowsGt, 0)
			c.Op = self
			return c, nil
		}},
		"single+2x": {build: func(self *PipelineOp) (*PipelineCond, []ProducedCond) {
			a, b, c := rows(CondRowsGt, 0), rows(CondRowsEq, 1), rows(CondRowsLe, 5)
			a.Op, b.Op, c.Op = self, self, self
			return a, []ProducedCond{{CondID: 7, Cond: b}, {CondID: 8, Cond: c}}
		}},
		"colpred": {pred: true, build: func(self *PipelineOp) (*PipelineCond, []ProducedCond) {
			return &PipelineCond{Kind: CondColPred, Op: self, SQLPred: "score > 1"}, nil
		}},
		"recorded": {build: func(self *PipelineOp) (*PipelineCond, []ProducedCond) {
			return &PipelineCond{Kind: CondOr, Left: &PipelineCond{Kind: CondRowsEq, Recorded: 30, Op: &PipelineOp{}}, Right: &PipelineCond{Kind: CondRowsGt, Op: self}}, nil
		}},
		"not(and)+ge+lt": {build: func(self *PipelineOp) (*PipelineCond, []ProducedCond) {
			return &PipelineCond{Kind: CondNot, Left: &PipelineCond{Kind: CondAnd,
				Left: &PipelineCond{Kind: CondRowsGe, Op: self, Threshold: 2}, Right: &PipelineCond{Kind: CondRowsLt, Op: self, Threshold: 3}}}, nil
		}},
		"no op": {build: func(*PipelineOp) (*PipelineCond, []ProducedCond) {
			return &PipelineCond{Kind: CondRowsEq}, nil
		}},
	}
	for _, sd := range shapeDescs() {
		for cn, cs := range condShapes {
			for _, assert := range []int64{-1, 1} {
				for _, nLeaf := range []int{0, 2} {
					for _, nGates := range []int{0, 2} {
						for _, inTx := range []bool{false, true} {
							for _, prior := range []bool{false, true} {
								name := fmt.Sprintf("%s/%s/assert=%d/leafrecs=%d/gates=%d/inTx=%v/prior=%v", sd.name, cn, assert, nLeaf, nGates, inTx, prior)
								var gates []BranchGate
								for g := 0; g < nGates; g++ {
									gates = append(gates, BranchGate{CondID: 20 + g, WantTrue: g%2 == 0})
								}
								ops := make([]PipelineOp, 0, 4)
								// an earlier operation that records what the gates and the recorded leaf read (conditions
								// 20, 21 and leaf record 30)
								pre := mkOp(shapeDescs()[2], nil, OpDecorators{AssertRows: -1})
								pre.Decorators.ProduceCond, pre.Decorators.CondID = &PipelineCond{Kind: CondRowsGt, Op: &pre}, 20
								pre.Decorators.ExtraConds = []ProducedCond{{CondID: 21, Cond: &PipelineCond{Kind: CondRowsEq, Op: &pre}}}
								pre.ExtraConds = pre.Decorators.ExtraConds
								pre.Decorators.LeafRecs = []ProducedCond{{CondID: 30, Cond: &PipelineCond{Kind: CondRowsGt, Op: &pre}}}
								ops = append(ops, pre)
								if prior {
									ops = append(ops, PipelineOp{Decorators: OpDecorators{AssertRows: -1, IsAbort: true}, AbortErr: errors.New("stop")})
								}
								idx := len(ops)
								ops = append(ops, mkOp(sd, gates, OpDecorators{AssertRows: assert}))
								self := &ops[idx]
								cond, extra := cs.build(self)
								self.Decorators.ProduceCond, self.Decorators.CondID, self.Decorators.ExtraConds = cond, 3, extra
								self.ExtraConds = extra
								for l := 0; l < nLeaf; l++ {
									self.Decorators.LeafRecs = append(self.Decorators.LeafRecs, ProducedCond{CondID: 40 + l, Cond: &PipelineCond{Kind: CondRowsEq, Op: self, Threshold: int64(l)}})
								}
								// a trailing gated op, to see the conditions used as gates
								if cond != nil {
									ops = append(ops, mkOp(shapeDescs()[2], []BranchGate{{CondID: 3, WantTrue: true}}, OpDecorators{AssertRows: -1}))
								}
								self = &ops[idx] // ops was re-sliced by append: re-point the conditions' leaves
								if cond != nil {
									repoint(cond, self)
									for _, e := range extra {
										repoint(e.Cond, self)
									}
									for _, e := range self.Decorators.LeafRecs {
										repoint(e.Cond, self)
									}
								}
								b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: inTx})
								if cs.pred && sd.desc.StmtVerb != VerbSelect {
									if !errors.Is(err, ErrWireNotApplicable) {
										t.Errorf("%s: a column condition over a non-SELECT must not be applicable, got %v", name, err)
									}
									continue
								}
								if err != nil {
									t.Errorf("%s: %v", name, err)
									continue
								}
								checkMySQLBatch(t, name, b, ops, fl, inTx)
								if prior || assert >= 0 {
									// every statement of an operation after a possible failure is gated on it
									for i := range ops {
										if i > idx || prior && i == idx {
											if !strings.Contains(ops[i].SQL, "@sqlgen_f<0") {
												t.Errorf("%s: op %d is not gated on a failed guard:\n%s", name, i, ops[i].SQL)
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

// repoint makes every leaf of c that referred to some operation (other than the recorded ones) refer to op.
func repoint(c *PipelineCond, op *PipelineOp) {
	if c == nil {
		return
	}
	switch c.Kind {
	case CondAnd, CondOr, CondNot:
		repoint(c.Left, op)
		repoint(c.Right, op)
	default:
		if c.Op != nil && c.Recorded == 0 {
			c.Op = op
		}
	}
}

// hasTopLevelComma reports whether s has a comma outside parentheses and quotes: in a SET that would be a
// second assignment.
func hasTopLevelComma(s string) bool {
	depth, quote := 0, byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			return true
		}
	}
	return false
}

// --- golden shapes --------------------------------------------------------------------------------

func TestMySQLGoldenShapes(t *testing.T) {
	fl := &MySQLFlavor{Returning: true}
	descs := map[string]shapeDesc{}
	for _, sd := range shapeDescs() {
		descs[sd.name] = sd
	}
	t.Run("plain statements commit unconditionally", func(t *testing.T) {
		ops := []PipelineOp{mkOp(descs["update"], nil, OpDecorators{AssertRows: -1}), mkOp(descs["select"], nil, OpDecorators{AssertRows: -1})}
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl})
		if err != nil {
			t.Fatal(err)
		}
		want := "SET @sqlgen_p=-1;START TRANSACTION;" +
			"SET @sqlgen_p=0;UPDATE t SET score = score + 7\nWHERE id IN ('o''k', 0x0102) AND org_id = NULL;SET @sqlgen_n0=ROW_COUNT();" +
			"SET @sqlgen_p=1;SELECT id FROM t WHERE id = 7;" +
			"SET @sqlgen_p=2;COMMIT;SELECT @sqlgen_n0;"
		if b.SQL != want {
			t.Fatalf("got\n%s\nwant\n%s", b.SQL, want)
		}
		if got := b.ResultSetOps(); len(got) != 1 || got[0] != 1 || !b.HasSummary() || b.SummaryLen() != 1 || b.CommitPos() != 2 || !b.OwnsTx() || b.UsesPrepare() {
			t.Fatalf("layout: sets=%v summary=%d commit=%d own=%v prepare=%v", got, b.SummaryLen(), b.CommitPos(), b.OwnsTx(), b.UsesPrepare())
		}
	})

	t.Run("read-only batch has no summary", func(t *testing.T) {
		ops := []PipelineOp{mkOp(descs["select"], nil, OpDecorators{AssertRows: -1})}
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, TxOpts: &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelSerializable}})
		if err != nil {
			t.Fatal(err)
		}
		want := "SET @sqlgen_p=-1;SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;START TRANSACTION READ ONLY;SET @sqlgen_p=0;SELECT id FROM t WHERE id = 7;SET @sqlgen_p=1;COMMIT;"
		if b.SQL != want || b.HasSummary() {
			t.Fatalf("got %q summary=%v", b.SQL, b.HasSummary())
		}
	})

	t.Run("assertion, condition and a gated statement", func(t *testing.T) {
		ops := []PipelineOp{mkOp(descs["update"], nil, OpDecorators{AssertRows: 1}), mkOp(descs["update-no-where"], []BranchGate{{CondID: 1, WantTrue: true}}, OpDecorators{AssertRows: -1})}
		ops[0].Decorators.ProduceCond = &PipelineCond{Kind: CondRowsGt, Op: &ops[0]}
		ops[0].Decorators.CondID = 1
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl})
		if err != nil {
			t.Fatal(err)
		}
		want := "SET @sqlgen_p=-1;SET @sqlgen_f=-1;START TRANSACTION;" +
			"SET @sqlgen_p=0;UPDATE t SET score = score + 7\nWHERE id IN ('o''k', 0x0102) AND org_id = NULL;SET @sqlgen_n0=ROW_COUNT();" +
			"SET @sqlgen_f=IF(@sqlgen_f<0 AND (1) AND NOT (@sqlgen_n0<=>1),0,@sqlgen_f);SET @sqlgen_c1=COALESCE(@sqlgen_n0>0,0);" +
			"SET @sqlgen_p=1;UPDATE t SET status = 7 WHERE @sqlgen_f<0 AND @sqlgen_c1=1;SET @sqlgen_n1=ROW_COUNT();" +
			"SET @sqlgen_p=2;SET @sqlgen_q=IF(@sqlgen_f<0,'COMMIT','ROLLBACK');PREPARE sqlgen_s FROM @sqlgen_q;EXECUTE sqlgen_s;DEALLOCATE PREPARE sqlgen_s;" +
			"SELECT @sqlgen_f,@sqlgen_n0,@sqlgen_c1,@sqlgen_n1;"
		if b.SQL != want {
			t.Fatalf("got\n%s\nwant\n%s", b.SQL, want)
		}
		if !b.UsesPrepare() {
			t.Fatal("a batch with a guard ends its transaction through a prepared statement")
		}
	})

	t.Run("inside the caller's transaction", func(t *testing.T) {
		ops := []PipelineOp{mkOp(descs["update-no-where"], nil, OpDecorators{AssertRows: 2})}
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: true})
		if err != nil {
			t.Fatal(err)
		}
		want := "SET @sqlgen_p=-1;SET @sqlgen_f=-1;SET @sqlgen_p=0;SAVEPOINT sqlgen_sp0;UPDATE t SET status = 7;SET @sqlgen_n0=ROW_COUNT();" +
			"SET @sqlgen_f=IF(@sqlgen_f<0 AND (1) AND NOT (@sqlgen_n0<=>2),0,@sqlgen_f);SELECT @sqlgen_f,@sqlgen_n0;"
		if b.SQL != want {
			t.Fatalf("got\n%s\nwant\n%s", b.SQL, want)
		}
		if sp, ok := b.Savepoint(0); !ok || sp != "sqlgen_sp0" {
			t.Fatalf("savepoint = %q %v", sp, ok)
		}
		if _, ok := b.Savepoint(5); ok {
			t.Fatal("savepoint of an unknown operation")
		}
		if b.OwnsTx() || b.UsesPrepare() {
			t.Fatal("a batch in the caller's transaction owns nothing")
		}
	})

	t.Run("an Abort is only a gated failure record", func(t *testing.T) {
		ops := []PipelineOp{{Decorators: OpDecorators{AssertRows: -1, IsAbort: true}, AbortErr: errors.New("stop"), Input: RenderInput{BranchGates: []BranchGate{{CondID: 4, WantTrue: false}}}}}
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: true})
		if err != nil {
			t.Fatal(err)
		}
		want := "SET @sqlgen_p=-1;SET @sqlgen_f=-1;SET @sqlgen_p=0;SET @sqlgen_f=IF(@sqlgen_f<0 AND (@sqlgen_c4=0),0,@sqlgen_f);SELECT @sqlgen_f;"
		if b.SQL != want {
			t.Fatalf("got\n%s\nwant\n%s", b.SQL, want)
		}
	})

	t.Run("a gated INSERT of several rows is a UNION of gated SELECTs", func(t *testing.T) {
		ops := []PipelineOp{mkOp(descs["insert-tuples"], []BranchGate{{CondID: 2, WantTrue: true}}, OpDecorators{AssertRows: -1})}
		ops[0].Args = []any{int64(1), "a", int64(2), int64(3), "b", int64(4)}
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: true})
		if err != nil {
			t.Fatal(err)
		}
		want := "INSERT INTO t (id, name, score)\nSELECT 1, 'a', 2 FROM DUAL WHERE @sqlgen_c2=1 UNION ALL SELECT 3, 'b', 4 FROM DUAL WHERE @sqlgen_c2=1 ON DUPLICATE KEY UPDATE score = VALUES(score);"
		if !strings.Contains(b.SQL, want) {
			t.Fatalf("got\n%s\nwant it to contain\n%s", b.SQL, want)
		}
	})

	t.Run("a gated INSERT of one row selects from DUAL", func(t *testing.T) {
		ops := []PipelineOp{mkOp(descs["insert-values"], []BranchGate{{CondID: 2, WantTrue: false}}, OpDecorators{AssertRows: -1})}
		ops[0].Args = []any{int64(1), "a", int64(2)}
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: true})
		if err != nil {
			t.Fatal(err)
		}
		want := "INSERT INTO t (id, name, score)\nSELECT 1, 'a', 2 FROM (SELECT 1) AS _sqlgen_dual WHERE @sqlgen_c2=0;"
		if !strings.Contains(b.SQL, want) {
			t.Fatalf("got\n%s\nwant it to contain\n%s", b.SQL, want)
		}
	})

	t.Run("a column condition runs the select a second time", func(t *testing.T) {
		ops := []PipelineOp{mkOp(descs["select"], []BranchGate{{CondID: 9, WantTrue: true}}, OpDecorators{AssertRows: -1})}
		ops[0].Args = []any{int64(5)}
		ops[0].Decorators.ProduceCond = &PipelineCond{Kind: CondColPred, Op: &ops[0], SQLPred: "score > 1"}
		ops[0].Decorators.CondID = 2
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: true})
		if err != nil {
			t.Fatal(err)
		}
		want := "SELECT * FROM (SELECT id FROM t WHERE id = 5) AS _sqlgen_if WHERE @sqlgen_c9=1;SET @sqlgen_n0=FOUND_ROWS();" +
			"SET @sqlgen_c2=COALESCE(IF(@sqlgen_c9=1,(SELECT COALESCE(COUNT(*)>0 AND MIN(CASE WHEN (score > 1) THEN 1 ELSE 0 END)=1,0) FROM (SELECT id FROM t WHERE id = 5) AS _sqlgen_op),0),0);"
		if !strings.Contains(b.SQL, want) {
			t.Fatalf("got\n%s\nwant it to contain\n%s", b.SQL, want)
		}
	})
}

// --- MariaDB RETURNING ------------------------------------------------------------------------------

func TestMySQLReturning(t *testing.T) {
	ret := &QueryDescriptor{Name: "ret", StmtVerb: VerbInsert, HasReturning: true, Segments: []string{"INSERT INTO t (id) VALUES (", ") RETURNING id"}, Slots: []Slot{{Kind: SlotScalar}}}
	mk := func(dec OpDecorators) []PipelineOp {
		return []PipelineOp{{Desc: ret, HasResultCols: true, Decorators: dec, Args: []any{int64(3)}}}
	}
	t.Run("without support the batch is not applicable", func(t *testing.T) {
		_, err := RenderMySQLBatch(mk(OpDecorators{AssertRows: -1}), MySQLRenderOptions{Flavor: &MySQLFlavor{}})
		if !errors.Is(err, ErrWireNotApplicable) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unguarded", func(t *testing.T) {
		ops := mk(OpDecorators{AssertRows: -1})
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: &MySQLFlavor{Returning: true}, InTx: true})
		if err != nil {
			t.Fatal(err)
		}
		if b.SQL != "SET @sqlgen_p=-1;SET @sqlgen_p=0;INSERT INTO t (id) VALUES (3) RETURNING id;" || b.TrimsLastColumn(0) {
			t.Fatalf("got %q trim=%v", b.SQL, b.TrimsLastColumn(0))
		}
	})
	t.Run("a guard counts the returned rows in a counter column", func(t *testing.T) {
		ops := mk(OpDecorators{AssertRows: 1})
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: &MySQLFlavor{Returning: true}, InTx: true})
		if err != nil {
			t.Fatal(err)
		}
		want := "SET @sqlgen_p=0;SAVEPOINT sqlgen_sp0;SET @sqlgen_k=0;INSERT INTO t (id) VALUES (3) RETURNING id, (@sqlgen_k:=@sqlgen_k+1);SET @sqlgen_n0=@sqlgen_k;"
		if !strings.Contains(b.SQL, want) || !b.TrimsLastColumn(0) {
			t.Fatalf("got %q trim=%v", b.SQL, b.TrimsLastColumn(0))
		}
	})
	t.Run("a column condition over it is not applicable", func(t *testing.T) {
		ops := mk(OpDecorators{AssertRows: -1})
		ops[0].Decorators.ProduceCond = &PipelineCond{Kind: CondColPred, Op: &ops[0], SQLPred: "id > 1"}
		_, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: &MySQLFlavor{Returning: true}, InTx: true})
		if !errors.Is(err, ErrWireNotApplicable) {
			t.Fatalf("err = %v", err)
		}
	})
}

// --- not applicable -------------------------------------------------------------------------------

func TestMySQLRenderNotApplicable(t *testing.T) {
	sds := shapeDescs()
	fl := &MySQLFlavor{}
	t.Run("argument that cannot be inlined", func(t *testing.T) {
		ops := []PipelineOp{mkOp(sds[0], nil, OpDecorators{AssertRows: -1})}
		ops[0].Args = []any{math.NaN()}
		_, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl})
		if !errors.Is(err, ErrWireNotApplicable) || !strings.Contains(err.Error(), "sel") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing argument", func(t *testing.T) {
		ops := []PipelineOp{mkOp(sds[0], nil, OpDecorators{AssertRows: -1})}
		ops[0].Args = nil
		_, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl})
		if !errors.Is(err, ErrWireNotApplicable) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("isolation without a MySQL equivalent", func(t *testing.T) {
		ops := []PipelineOp{mkOp(sds[0], nil, OpDecorators{AssertRows: -1})}
		_, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, TxOpts: &sql.TxOptions{Isolation: sql.LevelSnapshot}})
		if !errors.Is(err, ErrWireNotApplicable) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("isolation levels", func(t *testing.T) {
		for lvl, want := range map[sql.IsolationLevel]string{
			sql.LevelReadUncommitted: "READ UNCOMMITTED", sql.LevelReadCommitted: "READ COMMITTED",
			sql.LevelRepeatableRead: "REPEATABLE READ", sql.LevelSerializable: "SERIALIZABLE",
		} {
			ops := []PipelineOp{mkOp(sds[0], nil, OpDecorators{AssertRows: -1})}
			b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, TxOpts: &sql.TxOptions{Isolation: lvl}})
			if err != nil || !strings.Contains(b.SQL, "SET TRANSACTION ISOLATION LEVEL "+want+";START TRANSACTION;") {
				t.Fatalf("%v: %v %q", lvl, err, b.SQL)
			}
		}
	})
	t.Run("no flavor", func(t *testing.T) {
		if _, err := RenderMySQLBatch(nil, MySQLRenderOptions{}); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("operation without a descriptor", func(t *testing.T) {
		if _, err := RenderMySQLBatch([]PipelineOp{{}}, MySQLRenderOptions{Flavor: fl}); err == nil {
			t.Fatal("expected an error")
		}
	})
}

// --- the summary row --------------------------------------------------------------------------------

func TestMySQLApply(t *testing.T) {
	sds := shapeDescs()
	fl := &MySQLFlavor{}
	build := func(abortFirst bool) ([]PipelineOp, *MySQLBatch) {
		ops := []PipelineOp{
			mkOp(sds[2], nil, OpDecorators{AssertRows: -1}),
			mkOp(sds[2], nil, OpDecorators{AssertRows: 1}),
			mkOp(sds[0], nil, OpDecorators{AssertRows: -1}),
		}
		ops[0].Decorators.ProduceCond, ops[0].Decorators.CondID = &PipelineCond{Kind: CondRowsGt, Op: &ops[0]}, 5
		ops[0].Decorators.ExtraConds = []ProducedCond{{CondID: 6, Cond: &PipelineCond{Kind: CondRowsEq, Op: &ops[0]}}}
		ops[0].ExtraConds = ops[0].Decorators.ExtraConds
		ops[1].Desc = &QueryDescriptor{Name: "named", StmtVerb: VerbUpdate, Segments: []string{"UPDATE t SET a = 1", ""}}
		ops[1].Args = nil
		ops[2].Decorators.AssertRows = 4 // a row-returning operation with an assertion
		if abortFirst {
			ops = append(ops, PipelineOp{Decorators: OpDecorators{AssertRows: -1, IsAbort: true}, AbortErr: errStopTest})
		}
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl})
		if err != nil {
			t.Fatal(err)
		}
		return ops, b
	}
	// columns: f, n0, c5, c6, n1, n2 (assertion on a row-returning op records its count)
	ops, b := build(true)
	if b.SummaryLen() != 6 {
		t.Fatalf("summary has %d columns: %s", b.SummaryLen(), b.SQL)
	}
	t.Run("success", func(t *testing.T) {
		ops, b := build(true)
		f, err := b.Apply(ops, []any{int64(-1), []byte("2"), uint64(1), 0.0, "1", []byte("4")})
		if f != -1 || err != nil {
			t.Fatalf("Apply = %d, %v", f, err)
		}
		if ops[0].RowCount != 2 || !ops[0].CondSet || !ops[0].CondVal || !ops[0].ExtraConds[0].Set || ops[0].ExtraConds[0].Val || ops[1].RowCount != 1 {
			t.Fatalf("ops = %+v / %+v", ops[0], ops[1])
		}
		if ops[2].RowCount != 0 {
			t.Fatalf("the rows of a row-returning operation are counted by the scan, not the summary: %d", ops[2].RowCount)
		}
	})
	t.Run("assertion failure reports the statement and the counts", func(t *testing.T) {
		f, err := b.Apply(ops, []any{int64(1), int64(0), int64(0), int64(0), int64(3), int64(4)})
		var ae *AssertionError
		if f != 1 || !errors.As(err, &ae) || ae.Statement != "named" || ae.Want != 1 || ae.Got != 3 || !ae.GotKnown || !errors.Is(err, ErrAssertionFailed) || ops[1].Err != err {
			t.Fatalf("Apply = %d, %v (%+v)", f, err, ae)
		}
	})
	t.Run("abort failure returns the caller's error", func(t *testing.T) {
		f, err := b.Apply(ops, []any{int64(3), int64(0), int64(0), int64(0), int64(1), int64(4)})
		if f != 3 || err != errStopTest || ops[3].Err != errStopTest {
			t.Fatalf("Apply = %d, %v", f, err)
		}
	})
	t.Run("bad summaries are errors, never silent", func(t *testing.T) {
		for name, row := range map[string][]any{
			"short":         {int64(-1)},
			"NULL":          {int64(-1), nil, int64(0), int64(0), int64(0), int64(0)},
			"not a number":  {int64(-1), []byte("x"), int64(0), int64(0), int64(0), int64(0)},
			"out of range":  {int64(17), int64(0), int64(0), int64(0), int64(0), int64(0)},
			"unknown value": {int64(-1), struct{}{}, int64(0), int64(0), int64(0), int64(0)},
		} {
			if f, err := b.Apply(ops, row); err == nil || f >= 0 {
				t.Errorf("%s: Apply = %d, %v", name, f, err)
			}
		}
	})
}

var errStopTest = errors.New("stop")

// --- the estimate ---------------------------------------------------------------------------------

// The estimate is what the renderer pre-sizes its builder with; it must never be below what is written.
func FuzzEstimateMySQLOpBytesUpperBound(f *testing.F) {
	f.Add(uint8(0), uint8(2), uint8(1), true, int64(1), uint8(3), uint8(0), "it's")
	f.Add(uint8(3), uint8(5), uint8(9), false, int64(-1), uint8(0), uint8(2), `back\slash`)
	f.Fuzz(func(t *testing.T, descRaw, nGatesRaw, nArgsRaw uint8, inTx bool, assertRows int64, condRaw, charsetRaw uint8, text string) {
		sds := shapeDescs()
		sd := sds[int(descRaw)%len(sds)]
		var gates []BranchGate
		for g := 0; g < int(nGatesRaw%6); g++ {
			gates = append(gates, BranchGate{CondID: 1 + g*1000003, WantTrue: g%2 == 0})
		}
		if assertRows < -1 {
			assertRows = -1
		}
		op := mkOp(sd, gates, OpDecorators{AssertRows: assertRows % 10})
		pool := []any{text, []byte(text), time.Unix(int64(nArgsRaw)*1e6, int64(nArgsRaw)*1e7).UTC(), math.MaxInt64, uint64(math.MaxUint64), 1.5e300, true, nil, "", fmt.Sprint(text, text)}
		for i := range op.Args {
			op.Args[i] = pool[(i+int(nArgsRaw))%len(pool)]
		}
		leaf := &PipelineCond{Kind: CondRowsEq + CondKind(condRaw%5), Op: &op, Threshold: int64(nArgsRaw)}
		switch condRaw % 4 {
		case 1:
			op.Decorators.ProduceCond, op.Decorators.CondID = leaf, math.MaxInt32
		case 2:
			op.Decorators.ProduceCond, op.Decorators.CondID = &PipelineCond{Kind: CondNot, Left: leaf}, 3
			op.Decorators.LeafRecs = []ProducedCond{{CondID: 9, Cond: leaf}}
		case 3:
			if sd.desc.StmtVerb == VerbSelect {
				op.Decorators.ProduceCond, op.Decorators.CondID = &PipelineCond{Kind: CondOr, Left: &PipelineCond{Kind: CondColPred, Op: &op, SQLPred: text}, Right: leaf}, 4
			}
		}
		fl := &MySQLFlavor{Charset: []string{"", "gbk", "binary", "utf8mb4"}[charsetRaw%4]}
		ops := []PipelineOp{op}
		b, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl, InTx: inTx})
		if err != nil {
			if errors.Is(err, ErrWireNotApplicable) {
				return
			}
			t.Fatal(err)
		}
		if est := estimateMySQLOpBytes(fl, &ops[0], ops[0].Args); len(ops[0].SQL) > est {
			t.Fatalf("estimate %d < rendered %d:\n%s", est, len(ops[0].SQL), b.SQL)
		}
	})
}

// Rendering a batch must stay allocation-lean: the text is built in one pre-sized builder, the cursor is
// reused, and plain arguments are inlined without copying.
func TestMySQLRenderAllocations(t *testing.T) {
	sds := shapeDescs()
	fl := &MySQLFlavor{}
	ops := make([]PipelineOp, 0, 8)
	for i := 0; i < 8; i++ {
		ops = append(ops, mkOp(sds[1+i%2], nil, OpDecorators{AssertRows: -1}))
		ops[i].Args = shapeArgs(sds[1+i%2].args)
	}
	allocs := testing.AllocsPerRun(50, func() {
		if _, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl}); err != nil {
			t.Fatal(err)
		}
	})
	// batch struct, flags, args table, spans, the builder, its string, the summary slice growth
	if allocs > 16 {
		t.Fatalf("RenderMySQLBatch of 8 operations allocates %v times", allocs)
	}
	t.Logf("allocations for 8 operations: %v", allocs)
}

func BenchmarkRenderMySQLBatch(b *testing.B) {
	sds := shapeDescs()
	fl := &MySQLFlavor{}
	ops := make([]PipelineOp, 0, 8)
	for i := 0; i < 8; i++ {
		ops = append(ops, mkOp(sds[1+i%2], nil, OpDecorators{AssertRows: 1}))
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := RenderMySQLBatch(ops, MySQLRenderOptions{Flavor: fl}); err != nil {
			b.Fatal(err)
		}
	}
}
