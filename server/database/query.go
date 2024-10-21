package database

import (
	"litebase/server/auth"
	"litebase/server/cluster"
	"strings"
)

type Query struct {
	AccessKey       *auth.AccessKey
	cluster         *cluster.Cluster
	databaseManager *DatabaseManager
	DatabaseKey     *DatabaseKey
	Input           *QueryInput
	invalid         bool
}

func NewQuery(
	cluster *cluster.Cluster,
	databaseManager *DatabaseManager,
	databaseKey *DatabaseKey,
	accessKey *auth.AccessKey,
	input *QueryInput,
) (*Query, error) {
	return &Query{
		AccessKey:       accessKey,
		cluster:         cluster,
		DatabaseKey:     databaseKey,
		databaseManager: databaseManager,
		Input:           input,
	}, nil
}

func (query *Query) Resolve(response cluster.NodeQueryResponse) error {
	return ResolveQuery(query, response.(*QueryResponse))
}

func (q *Query) Validate(statement Statement) error {
	// if q.IsPragma() {
	// 	// TODO: Validate the types of pragma that are allowed
	// 	return nil
	// }

	return ValidateQuery(statement.Sqlite3Statement, q.Input.Parameters...)
}

func (query *Query) IsDDL() bool {
	return (len(query.Input.Statement) >= 6 &&
		(strings.HasPrefix(query.Input.Statement, "create") || strings.HasPrefix(query.Input.Statement, "CREATE") ||
			strings.HasPrefix(query.Input.Statement, "alter") || strings.HasPrefix(query.Input.Statement, "ALTER") ||
			strings.HasPrefix(query.Input.Statement, "drop") || strings.HasPrefix(query.Input.Statement, "DROP")))
}

func (query *Query) IsDML() bool {
	return (len(query.Input.Statement) >= 6 &&
		(strings.HasPrefix(query.Input.Statement, "insert") || strings.HasPrefix(query.Input.Statement, "INSERT") ||
			strings.HasPrefix(query.Input.Statement, "update") || strings.HasPrefix(query.Input.Statement, "UPDATE") ||
			strings.HasPrefix(query.Input.Statement, "delete") || strings.HasPrefix(query.Input.Statement, "DELETE")))
}

func (query *Query) IsDQL() bool {
	return len(query.Input.Statement) >= 6 && (strings.HasPrefix(query.Input.Statement, "select") || strings.HasPrefix(query.Input.Statement, "SELECT"))
}

func (query *Query) IsPragma() bool {
	return len(query.Input.Statement) >= 6 && (strings.HasPrefix(query.Input.Statement, "pragma") || strings.HasPrefix(query.Input.Statement, "PRAGMA"))
}

func (query *Query) IsRead() bool {
	return query.IsDQL()
}

func (query *Query) IsWrite() bool {
	return query.IsDDL() || query.IsDML() || query.IsPragma()
}

func (query *Query) Reset() {
	query.AccessKey = nil
	query.DatabaseKey = nil
	query.Input = nil
	query.invalid = false
}
