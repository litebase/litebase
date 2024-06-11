package database

import "litebase/server/sqlite3"

type Statement struct {
	// queryPlan []
	Sqlite3Statement *sqlite3.Statement
}
