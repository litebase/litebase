package database

import (
	"context"
	"litebase/server/sqlite3"
)

type Statement struct {
	context          context.Context
	Sqlite3Statement *sqlite3.Statement
}
