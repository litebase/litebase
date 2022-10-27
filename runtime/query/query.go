package query

import (
	"encoding/json"
	"fmt"
	"litebasedb/runtime/auth"
	"litebasedb/runtime/database"
	"litebasedb/runtime/sqlite3"

	"github.com/google/uuid"
)

type Query struct {
	AccessKeyId    string
	Batch          []*Query `json:"batch"`
	Database       *database.Database
	Id             string `json:"id"`
	Invalid        bool
	JsonParameters string `json:"parameters"`
	JsonStatement  string `json:"statement"`
	parameters     []interface{}
	statement      *sqlite3.Statement
}

func NewQuery(database *database.Database, accessKeyId string, data map[string]interface{}, id string) (*Query, error) {
	var batchedQueries []*Query
	var statement string
	var parameters string
	var err error

	if data["statement"] != nil {
		statement, err = auth.SecretsManager().DecryptFor(accessKeyId, data["statement"].(string), accessKeyId)

		if err != nil {
			return nil, fmt.Errorf("failed to decrypt query statement")
		}
	}

	if data["parameters"] != nil {
		parameters, err = auth.SecretsManager().DecryptFor(accessKeyId, data["parameters"].(string), "")

		if err != nil {
			return nil, fmt.Errorf("failed to decrypt query parameters")
		}
	}

	if data["batch"] != nil {
		batch := data["batch"].([]map[string]interface{})
		batchedQueries = make([]*Query, 0)

		for _, b := range batch {
			query, err := NewQuery(database, accessKeyId, b, "")

			if err != nil {
				query.Invalid = true
			}

			batchedQueries = append(batchedQueries, query)
		}
	}

	var query = &Query{
		AccessKeyId:    accessKeyId,
		Batch:          batchedQueries,
		Database:       database,
		JsonStatement:  statement,
		JsonParameters: parameters,
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
		json.Unmarshal([]byte(query.JsonParameters), &query.parameters)
	}

	return query.parameters
}

func (query *Query) Resolve() map[string]interface{} {
	resolver := NewResolver()

	return resolver.Handle(query.Database, query, false)
}

func (q *Query) Statement() (*sqlite3.Statement, error) {
	var err error

	if len(q.Batch) > 0 {
		return nil, nil
	}

	if q.statement == nil {
		q.statement, err = q.Database.GetConnection().Prepare(q.JsonStatement)

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

	return ValidateQuery(q.Batch, statement, q.Parameters()...)
}
