package database

import (
	"bytes"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/server/logs"
)

type Query struct {
	AccessKey       *auth.AccessKey
	cluster         *cluster.Cluster
	databaseManager *DatabaseManager
	DatabaseKey     *auth.DatabaseKey
	Input           *QueryInput
	invalid         bool
	logManager      *logs.LogManager
	transaction     *Transaction
}

func NewQuery(
	cluster *cluster.Cluster,
	databaseManager *DatabaseManager,
	logManager *logs.LogManager,
	databaseKey *auth.DatabaseKey,
	accessKey *auth.AccessKey,
	input *QueryInput,
) (*Query, error) {
	return &Query{
		AccessKey:       accessKey,
		cluster:         cluster,
		DatabaseKey:     databaseKey,
		databaseManager: databaseManager,
		Input:           input,
		logManager:      logManager,
	}, nil
}

func (query *Query) ForTransaction(transaction *Transaction) *Query {
	query.transaction = transaction

	return query
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

func (query *Query) IsTransactional() bool {
	return query.transaction != nil
}

func (query *Query) IsTransactionEnd() bool {
	return len(query.Input.Statement) >= 3 && (bytes.HasPrefix(query.Input.Statement, []byte("commit")) || bytes.HasPrefix(query.Input.Statement, []byte("COMMIT")) || bytes.HasPrefix(query.Input.Statement, []byte("end")) || bytes.HasPrefix(query.Input.Statement, []byte("END")))
}

func (query *Query) IsTransactionRollback() bool {
	return len(query.Input.Statement) >= 6 && (bytes.HasPrefix(query.Input.Statement, []byte("rollback")) || bytes.HasPrefix(query.Input.Statement, []byte("ROLLBACK")))
}

func (query *Query) IsTransactionStart() bool {
	return len(query.Input.Statement) >= 5 && (bytes.HasPrefix(query.Input.Statement, []byte("begin")) || bytes.HasPrefix(query.Input.Statement, []byte("BEGIN")))
}

func (query *Query) IsWrite() bool {
	return query.IsDDL() || query.IsDML() || query.IsPragma()
}

func (query *Query) Reset() {
	query.AccessKey = nil
	query.DatabaseKey = nil
	query.Input = nil
	query.invalid = false
	query.transaction = nil
}

func (query *Query) Resolve(response cluster.NodeQueryResponse) (cluster.NodeQueryResponse, error) {
	return ResolveQuery(query.logManager, query, response.(*QueryResponse))
}

func (q *Query) Validate(statement Statement) error {
	return ValidateQuery(statement.Sqlite3Statement, q.Input.Parameters...)
}
