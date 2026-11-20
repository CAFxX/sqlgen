package runtime

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// The assertion and condition guards are CTEs with side effects (an error, set_config). PostgreSQL
// removes a CTE whose output the statement never uses, so EVERY guard must be referenced by the final
// SELECT of EVERY statement shape: verb x result columns x assertion x condition(s) x branch gates.
// This test pins that structurally; the real-engine matrix (integration/wire_guard_test.go) proves the
// planner honours it.

var guardCTERe = regexp.MustCompile(`, (_sqlgen_(?:chk|cond|x\d+|l\d+)) AS \(`)

func guardTail(sql string) string {
	const endOfGuardCTE = "AS _ok FROM _sqlgen_op)"
	i := strings.LastIndex(sql, endOfGuardCTE)
	if i < 0 {
		return ""
	}
	return sql[i+len(endOfGuardCTE):]
}

func TestEveryGuardCTEIsReferencedByTheFinalSelect(t *testing.T) {
	descs := map[string]*QueryDescriptor{
		"select":           {StmtVerb: VerbSelect, Segments: []string{"SELECT id FROM t WHERE id = ", ""}, Slots: []Slot{{Kind: SlotScalar}}},
		"update":           {StmtVerb: VerbUpdate, Segments: []string{"UPDATE t SET v = ", ""}, Slots: []Slot{{Kind: SlotScalar}}},
		"update-returning": {StmtVerb: VerbUpdate, HasReturning: true, Segments: []string{"UPDATE t SET v = ", " RETURNING id"}, Slots: []Slot{{Kind: SlotScalar}}},
		"delete":           {StmtVerb: VerbDelete, Segments: []string{"DELETE FROM t WHERE id = ", ""}, Slots: []Slot{{Kind: SlotScalar}}},
		"insert":           {StmtVerb: VerbInsert, Segments: []string{"INSERT INTO t (id) VALUES (", ")"}, Slots: []Slot{{Kind: SlotScalar}}},
	}
	rows := func(k CondKind, n int64) *PipelineCond { return &PipelineCond{Kind: k, Threshold: n} }
	condShapes := map[string]func() (*PipelineCond, []ProducedCond){
		"none":   func() (*PipelineCond, []ProducedCond) { return nil, nil },
		"single": func() (*PipelineCond, []ProducedCond) { return rows(CondRowsGt, 0), nil },
		"single+x": func() (*PipelineCond, []ProducedCond) {
			return rows(CondRowsGt, 0), []ProducedCond{{CondID: 7, Cond: rows(CondRowsEq, 1)}}
		},
		"single+2x": func() (*PipelineCond, []ProducedCond) {
			return rows(CondRowsGt, 0), []ProducedCond{{CondID: 7, Cond: rows(CondRowsEq, 1)}, {CondID: 8, Cond: rows(CondRowsLt, 5)}}
		},
		"colpred": func() (*PipelineCond, []ProducedCond) {
			return &PipelineCond{Kind: CondColPred, SQLPred: "score > 1"}, nil
		},
		"recorded": func() (*PipelineCond, []ProducedCond) {
			return &PipelineCond{Kind: CondOr, Left: &PipelineCond{Kind: CondRowsEq, Recorded: 30, Op: &PipelineOp{}}, Right: rows(CondRowsGt, 0)}, nil
		},
		"not(and)": func() (*PipelineCond, []ProducedCond) {
			return &PipelineCond{Kind: CondNot, Left: &PipelineCond{Kind: CondAnd, Left: rows(CondRowsGt, 0), Right: rows(CondRowsLe, 3)}}, nil
		},
	}
	for dn, desc := range descs {
		for cn, mk := range condShapes {
			for _, assert := range []int64{-1, 0, 1} {
				for _, nLeaf := range []int{0, 1, 2} {
					for _, nGates := range []int{0, 2} {
						name := fmt.Sprintf("%s/%s/assert=%d/leafrecs=%d/gates=%d", dn, cn, assert, nLeaf, nGates)
						cond, extra := mk()
						dec := OpDecorators{AssertRows: assert, ProduceCond: cond, CondID: 3, ExtraConds: extra}
						for l := 0; l < nLeaf; l++ { // records of leaves of conditions rooted in later operations
							dec.LeafRecs = append(dec.LeafRecs, ProducedCond{CondID: 40 + l, Cond: &PipelineCond{Kind: CondRowsEq, Op: &PipelineOp{}, Threshold: int64(l)}})
						}
						var gates []BranchGate
						for g := 0; g < nGates; g++ {
							gates = append(gates, BranchGate{CondID: 20 + g, WantTrue: g%2 == 0})
						}
						sql := renderPipelineOp(t, DialectDollar, desc, RenderInput{BranchGates: gates}, dec)
						guards := guardCTERe.FindAllStringSubmatch(sql, -1)
						want := nLeaf
						if assert >= 0 {
							want++
						}
						if cond != nil {
							want += 1 + len(extra)
						}
						if len(guards) != want {
							t.Errorf("%s: %d guard CTEs rendered, want %d:\n%s", name, len(guards), want, sql)
							continue
						}
						tail := guardTail(sql)
						for _, g := range guards {
							if !strings.Contains(tail, "(SELECT _ok FROM "+g[1]+")") {
								t.Errorf("%s: guard %s is not referenced by the final SELECT (the planner may prune it):\n%s", name, g[1], sql)
							}
						}
					}
				}
			}
		}
	}
}
