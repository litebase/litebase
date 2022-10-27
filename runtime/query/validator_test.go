package query_test

import (
	"litebasedb/internal/test"
	"litebasedb/runtime/database"
	"litebasedb/runtime/query"
	"reflect"
	"testing"
)

func TestValidateQuery(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()
		db, _ := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		cases := []struct {
			statement, parameters string
			error                 *query.QueryValidationError
		}{
			{
				"SELECT * FROM users LIMIT ?",
				"[1]",
				nil,
			},
			{
				"",
				"",
				&query.QueryValidationError{Errors: map[string][]string{"statement": {"A query statement is required"}}},
			},
			{
				"SELECT * FROM users LIMIT ? OFFSET ?",
				"[1]",
				&query.QueryValidationError{Errors: map[string][]string{"parameters": {"Query parameters must match the number of placeholders"}}},
			},
		}

		for _, c := range cases {
			q := &query.Query{
				Database:       db,
				JsonStatement:  c.statement,
				JsonParameters: c.parameters,
			}

			statement, err := q.Statement()

			if err != nil {
				t.Fatal(err)
			}

			err = query.ValidateQuery(q.Batch, statement, q.Parameters()...)

			if err != nil && !reflect.DeepEqual(err, c.error) {
				t.Fatalf(" Expected error to be %v, got %v", c.error, err)
			}
		}
	})
}
