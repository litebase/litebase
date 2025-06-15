package database

import (
	"context"

	"github.com/litebase/litebase/pkg/sqlite3"
)

type Statement struct {
	context          context.Context
	Sqlite3Statement *sqlite3.Statement
}
