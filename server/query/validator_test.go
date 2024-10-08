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
		db, _ := database.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

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
				Input: &query.QueryInput{
					Statement:  c.statement,
					Parameters: c.parameters,
				},
			}

			statement, err := db.GetConnection().Statement(c.statement)

			if err != nil {
				t.Fatal(err)
			}

			err = query.ValidateQuery(statement.Sqlite3Statement, q.Input.Parameters...)

			if err != nil && !reflect.DeepEqual(err, c.error) {
				t.Fatalf(" Expected error to be %v, got %v", c.error, err)
			}
		}
	})
}
