package database

import (
	"strings"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/logs"
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

func (query *Query) IsTransactional() bool {
	return query.transaction != nil
}

func (query *Query) IsTransactionEnd() bool {
	return len(query.Input.Statement) >= 3 && (strings.HasPrefix(query.Input.Statement, "commit") || strings.HasPrefix(query.Input.Statement, "COMMIT") || strings.HasPrefix(query.Input.Statement, "end") || strings.HasPrefix(query.Input.Statement, "END"))
}

func (query *Query) IsTransactionRollback() bool {
	return len(query.Input.Statement) >= 6 && (strings.HasPrefix(query.Input.Statement, "rollback") || strings.HasPrefix(query.Input.Statement, "ROLLBACK"))
}

func (query *Query) IsTransactionStart() bool {
	return len(query.Input.Statement) >= 5 && (strings.HasPrefix(query.Input.Statement, "begin") || strings.HasPrefix(query.Input.Statement, "BEGIN"))
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
