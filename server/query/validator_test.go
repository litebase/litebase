package query_test

import (
	"litebase/internal/test"
	"litebase/server/database"
	"litebase/server/query"
	"reflect"
	"testing"
)

func TestValidateQuery(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, _ := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		cases := []struct {
			statement  string
			parameters []interface{}
			error      *query.QueryValidationError
		}{
			{
				"SELECT * FROM users LIMIT ?",
				[]interface{}{1},
				nil,
			},
			{
				"",
				[]interface{}{},
				&query.QueryValidationError{Errors: map[string][]string{"statement": {"A query statement is required"}}},
			},
			{
				"SELECT * FROM users LIMIT ? OFFSET ?",
				[]interface{}{1},
				&query.QueryValidationError{Errors: map[string][]string{"parameters": {"Query parameters must match the number of placeholders"}}},
			},
		}

		for _, c := range cases {
			q := &query.Query{
				ClientConnection:   db,
				OriginalStatement:  c.statement,
				OriginalParameters: c.parameters,
			}

			statement, err := q.Statement()

			if err != nil {
				t.Fatal(err)
			}

			err = query.ValidateQuery(statement.Sqlite3Statement, q.Parameters()...)

			if err != nil && !reflect.DeepEqual(err, c.error) {
				t.Fatalf(" Expected error to be %v, got %v", c.error, err)
			}
		}
	})
}
