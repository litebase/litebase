package database

import "litebasedb/server/sqlite3"

type Statement struct {
	// queryPlan []
	Sqlite3Statement *sqlite3.Statement
}
