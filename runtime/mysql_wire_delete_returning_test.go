package runtime

import (
	"strings"
	"testing"
)

// MariaDB answers a DELETE ... RETURNING whose WHERE is constantly false (a closed gate over a session
// variable is a constant) with a bare OK packet instead of an empty result set, which would shift every
// later result set of the batch. A gated DELETE ... RETURNING therefore carries a gate the optimizer
// cannot fold; other statements keep the plain gate.
func TestMySQLGatedDeleteReturningKeepsItsResultSet(t *testing.T) {
	del := &QueryDescriptor{Name: "del", StmtVerb: VerbDelete, HasReturning: true,
		Segments: []string{"DELETE FROM t WHERE ", "id = ", "", " RETURNING id"},
		Slots:    []Slot{{Kind: SlotBranchGuardOpen}, {Kind: SlotScalar}, {Kind: SlotBranchGuardClose}}}
	upd := &QueryDescriptor{Name: "upd", StmtVerb: VerbUpdate,
		Segments: []string{"UPDATE t SET x = 1 WHERE ", "id = ", ""},
		Slots:    []Slot{{Kind: SlotBranchGuardOpen}, {Kind: SlotScalar}, {Kind: SlotBranchGuardClose}}}
	gates := RenderInput{BranchGates: []BranchGate{{CondID: 1, WantTrue: true}}}
	render := func(op PipelineOp) string {
		t.Helper()
		b, err := RenderMySQLBatch([]PipelineOp{op}, MySQLRenderOptions{Flavor: &MySQLFlavor{Returning: true}, InTx: true})
		if err != nil {
			t.Fatal(err)
		}
		return b.SQL
	}
	got := render(PipelineOp{Desc: del, HasResultCols: true, Input: gates, Decorators: OpDecorators{AssertRows: -1}, Args: []any{int64(3)}})
	if !strings.Contains(got, "(@sqlgen_c1=1 OR RAND()<0)") {
		t.Fatalf("gated DELETE ... RETURNING: %s", got)
	}
	got = render(PipelineOp{Desc: del, HasResultCols: true, Decorators: OpDecorators{AssertRows: -1}, Args: []any{int64(3)}})
	if strings.Contains(got, "RAND") {
		t.Fatalf("an ungated DELETE ... RETURNING needs no unfoldable gate: %s", got)
	}
	got = render(PipelineOp{Desc: upd, Input: gates, Decorators: OpDecorators{AssertRows: -1}, Args: []any{int64(3)}})
	if strings.Contains(got, "RAND") {
		t.Fatalf("only DELETE ... RETURNING is affected: %s", got)
	}
}
