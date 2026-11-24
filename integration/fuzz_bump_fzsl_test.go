package integration

import (
	"context"

	"github.com/CAFxX/sqlgen/integration/fzsl"
)

// BumpRet is UPDATE ... RETURNING, which only the PostgreSQL protocol engines support; the fzmy
// counterpart (fuzz_bump_fzmy_test.go) reports it as unsupported.
func (a *fzslAPI) BumpRet(ctx context.Context, delta int64, ids []int64, grp *int64) ([]fzOut, error) {
	p := fzsl.FzBumpRetParams{Delta: delta, IDs: ids}
	if grp != nil {
		p.OnlyGrp, p.Grp = true, *grp
	}
	return outsFzsl(a.q.FzBumpRet(ctx, p))
}
