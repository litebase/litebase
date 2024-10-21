package database

import "litebase/server/sqlite3"

type QueryInput struct {
	Id         string                       `json:"id"`
	Statement  string                       `json:"statement"`
	Parameters []sqlite3.StatementParameter `json:"parameters"`
}
