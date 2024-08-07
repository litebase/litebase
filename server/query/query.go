package query

import (
	"litebase/server/auth"
	"litebase/server/database"
	"litebase/server/node"
)

type Query struct {
	AccessKey    auth.AccessKey
	BranchUuid   string
	DatabaseUuid string
	Id           string `json:"id"`
	invalid      bool
	Parameters   []interface{} `json:"parameters"`
	Statement    string        `json:"statement"`
}

func NewQuery(
	databaseUuid string,
	branchUuid string,
	accessKey auth.AccessKey,
	statement string,
	parameters []interface{},
	id string,
) (*Query, error) {
	var query = &Query{
		AccessKey:    accessKey,
		BranchUuid:   branchUuid,
		DatabaseUuid: databaseUuid,
		Statement:    statement,
		Parameters:   parameters,
	}

	query.Id = id

	return query, nil
}

func (query Query) Resolve(databaseHash string) (node.NodeQueryResponse, error) {
	return ResolveQuery(databaseHash, &query)
}

func (q *Query) Validate(statement database.Statement) error {
	// if q.IsPragma() {
	// 	// TODO: Validate the types of pragma that are allowed
	// 	return nil
	// }

	return ValidateQuery(statement.Sqlite3Statement, q.Parameters...)
}

func (query Query) IsDDL() bool {
	return (len(query.Statement) >= 6 && (query.Statement[:6] == "CREATE" ||
		query.Statement[:6] == "create" || query.Statement[:6] == "ALTER" ||
		query.Statement[:6] == "alter" || query.Statement[:6] == "DROP" ||
		query.Statement[:6] == "drop"))
}

func (query Query) IsDML() bool {
	return (len(query.Statement) >= 6 && (query.Statement[:6] == "INSERT" || query.Statement[:6] == "insert" ||
		query.Statement[:6] == "UPDATE" || query.Statement[:6] == "update" ||
		query.Statement[:6] == "DELETE" || query.Statement[:6] == "delete"))
}

func (query Query) IsDQL() bool {
	return (len(query.Statement) >= 6 && (query.Statement[:6] == "SELECT" ||
		query.Statement[:6] == "select"))
}

func (query Query) IsPragma() bool {
	return (len(query.Statement) >= 6 && (query.Statement[:6] == "PRAGMA" ||
		query.Statement[:6] == "pragma"))
}

func (query Query) IsRead() bool {
	return query.IsDQL()
}

func (query Query) IsWrite() bool {
	return query.IsDDL() || query.IsDML() || query.IsPragma()
}
