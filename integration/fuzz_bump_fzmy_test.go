package integration

import (
	"context"
	"errors"
)

var errNoUpdateReturning = errors.New("UPDATE ... RETURNING is not supported by MySQL/MariaDB")

// BumpRet: no UPDATE ... RETURNING on MySQL or MariaDB, so the fzmy package does not have the statement.
func (a *fzmyAPI) BumpRet(context.Context, int64, []int64, *int64) ([]fzOut, error) {
	return nil, errNoUpdateReturning
}
