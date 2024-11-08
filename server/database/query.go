package database

import (
	"bytes"
	"litebase/server/auth"
	"litebase/server/cluster"
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

func (query *Query) Resolve(response cluster.NodeQueryResponse) (cluster.NodeQueryResponse, error) {
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
		(bytes.HasPrefix(query.Input.Statement, []byte("create")) || bytes.HasPrefix(query.Input.Statement, []byte("CREATE")) ||
			bytes.HasPrefix(query.Input.Statement, []byte("alter")) || bytes.HasPrefix(query.Input.Statement, []byte("ALTER")) ||
			bytes.HasPrefix(query.Input.Statement, []byte("drop")) || bytes.HasPrefix(query.Input.Statement, []byte("DROP"))))
}

func (query *Query) IsDML() bool {
	return (len(query.Input.Statement) >= 6 &&
		(bytes.HasPrefix(query.Input.Statement, []byte("insert")) || bytes.HasPrefix(query.Input.Statement, []byte("INSERT")) ||
			bytes.HasPrefix(query.Input.Statement, []byte("update")) || bytes.HasPrefix(query.Input.Statement, []byte("UPDATE")) ||
			bytes.HasPrefix(query.Input.Statement, []byte("delete")) || bytes.HasPrefix(query.Input.Statement, []byte("DELETE"))))
}

func (query *Query) IsDQL() bool {
	return len(query.Input.Statement) >= 6 && (bytes.HasPrefix(query.Input.Statement, []byte("select")) || bytes.HasPrefix(query.Input.Statement, []byte("SELECT")))
}

func (query *Query) IsPragma() bool {
	return len(query.Input.Statement) >= 6 && (bytes.HasPrefix(query.Input.Statement, []byte("pragma")) || bytes.HasPrefix(query.Input.Statement, []byte("PRAGMA")))
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
