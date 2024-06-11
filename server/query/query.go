package query

import (
	"litebase/server/database"
	"strings"

	"github.com/goccy/go-json"

	"github.com/google/uuid"
)

type Query struct {
	AccessKeyId        string
	ClientConnection   *database.ClientConnection
	Id                 string `json:"id"`
	IsPragma           bool
	invalid            bool
	OriginalParameters []interface{} `json:"parameters"`
	OriginalStatement  string        `json:"statement"`
	parameters         []interface{}
	statement          database.Statement
}

func NewQuery(
	clientConnection *database.ClientConnection,
	accessKeyId string,
	data map[string]interface{},
	id string,
) (Query, error) {
	statement := data["statement"].(string)
	parameters := data["parameters"].([]interface{})

	isPragma := false

	if strings.HasPrefix(statement, "PRAGMA") {
		isPragma = true
	}

	var query = Query{
		AccessKeyId:        accessKeyId,
		ClientConnection:   clientConnection,
		IsPragma:           isPragma,
		OriginalStatement:  statement,
		OriginalParameters: parameters,
	}

	if id == "" {
		uuid, err := uuid.NewUUID()

		if err != nil {
			return Query{}, err
		}

		query.Id = uuid.String()
	} else {
		query.Id = id
	}

	return query, nil
}

func (query Query) Parameters() []interface{} {
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

func (query Query) Resolve() (map[string]interface{}, error) {
	return ResolveQuery(query.ClientConnection, query)
}

func (q Query) Statement() (database.Statement, error) {
	var err error

	if q.statement == (database.Statement{}) {
		q.statement, err = q.ClientConnection.
			GetConnection().
			Statement(q.OriginalStatement)

		if err != nil {
			return database.Statement{}, err
		}
	}

	return q.statement, nil
}

func (q *Query) Validate(statement database.Statement) error {
	// if q.IsPragma {
	// 	// TODO: Validate the types of pragma that are allowed
	// 	return nil
	// }

	return ValidateQuery(statement.Sqlite3Statement, q.Parameters()...)
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
