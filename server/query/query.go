package query

import (
	"encoding/json"
	"litebasedb/server/database"
	"litebasedb/server/sqlite3"

	"github.com/google/uuid"
)

type Query struct {
	AccessKeyId        string
	ClientConnection   *database.ClientConnection
	Id                 string `json:"id"`
	invalid            bool
	OriginalParameters []interface{} `json:"parameters"`
	OriginalStatement  string        `json:"statement"`
	parameters         []interface{}
	statement          *sqlite3.Statement
}

func NewQuery(
	clientConnection *database.ClientConnection,
	accessKeyId string,
	data map[string]interface{},
	id string,
) (*Query, error) {
	var statement string
	var parameters []interface{}

	statement = data["statement"].(string)
	parameters = data["parameters"].([]interface{})

	var query = &Query{
		AccessKeyId:        accessKeyId,
		ClientConnection:   clientConnection,
		OriginalStatement:  statement,
		OriginalParameters: parameters,
	}

	if id == "" {
		uuid, _ := uuid.NewUUID()
		query.Id = uuid.String()
	} else {
		query.Id = id
	}

	return query, nil
}

func (query *Query) Parameters() []interface{} {
	if query.parameters == nil {
		var bytes []byte

		if query.OriginalParameters == nil {
			bytes = []byte("[]")
		} else {
			bytes, _ = json.Marshal(query.OriginalParameters)

			if bytes == nil {
				bytes = []byte("[]")
			}
		}

		json.Unmarshal(bytes, &query.parameters)
	}

	return query.parameters
}

func (query *Query) Resolve() map[string]interface{} {
	resolver := NewResolver()

	return resolver.Handle(query.ClientConnection, query)
}

func (q *Query) Statement() (*sqlite3.Statement, error) {
	var err error

	if q.statement == nil {
		q.statement, err = q.ClientConnection.
			GetConnection().
			Statement(q.OriginalStatement)

		if err != nil {
			return nil, err
		}
	}

	return q.statement, nil
}

func (q *Query) Validate() error {
	statement, err := q.Statement()

	if err != nil {
		return err
	}

	return ValidateQuery(statement, q.Parameters()...)
}

func (query Query) isDDL() bool {
	return query.OriginalStatement[0:6] == "CREATE" || query.OriginalStatement[0:6] == "ALTER" || query.OriginalStatement[0:6] == "DROP"
}

func (query Query) isDML() bool {
	return query.OriginalStatement[0:6] == "INSERT" || query.OriginalStatement[0:6] == "UPDATE" || query.OriginalStatement[0:6] == "DELETE"
}

func (query Query) isDQL() bool {
	return query.OriginalStatement[0:6] == "SELECT"
}
