package integration

import (
	"context"

	"github.com/CAFxX/sqlgen/integration/fzpg"
)

// BumpRet is UPDATE ... RETURNING, which only the PostgreSQL protocol engines support; the fzmy
// counterpart (fuzz_bump_fzmy_test.go) reports it as unsupported.
func (a *fzpgAPI) BumpRet(ctx context.Context, delta int64, ids []int64, grp *int64) ([]fzOut, error) {
	p := fzpg.FzBumpRetParams{Delta: delta, IDs: ids}
	if grp != nil {
		p.OnlyGrp, p.Grp = true, *grp
	}
	return outsFzpg(a.q.FzBumpRet(ctx, p))
}
