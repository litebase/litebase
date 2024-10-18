package query_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/query"
	"litebase/server/sqlite3"
	"reflect"
	"testing"
)

func TestValidateQuery(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []sqlite3.StatementParameter{})

		db, _ = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		cases := []struct {
			statement  string
			parameters []sqlite3.StatementParameter
			error      *query.QueryValidationError
		}{
			{
				"SELECT * FROM users LIMIT ?",
				[]sqlite3.StatementParameter{{
					Type:  "INTEGER",
					Value: 1,
				}},
				nil,
			},
			{
				"",
				[]sqlite3.StatementParameter{{}},
				&query.QueryValidationError{Errors: map[string][]string{"statement": {"A query statement is required"}}},
			},
			{
				"SELECT * FROM users LIMIT ? OFFSET ?",
				[]sqlite3.StatementParameter{{
					Type:  "INTEGER",
					Value: 1,
				}},
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
