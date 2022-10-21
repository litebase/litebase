package query

import (
	"litebasedb/runtime/app/database"
	"litebasedb/runtime/test"
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
			error                 *QueryValidationError
		}{
			{
				"SELECT * FROM users LIMIT ?",
				"[1]",
				nil,
			},
			{
				"",
				"",
				&QueryValidationError{Errors: map[string][]string{"statement": {"A query statement is required"}}},
			},
			{
				"SELECT * FROM users LIMIT ? OFFSET ?",
				"[1]",
				&QueryValidationError{Errors: map[string][]string{"parameters": {"Query parameters must match the number of placeholders"}}},
			},
		}

		for _, c := range cases {
			query := &Query{
				Database:       db,
				JsonStatement:  c.statement,
				JsonParameters: c.parameters,
			}

			statement, err := query.Statement()

			if err != nil {
				t.Fatal(err)
			}

			err = ValidateQuery(query.Batch, statement, query.Parameters()...)

			if err != nil && !reflect.DeepEqual(err, c.error) {
				t.Fatalf(" Expected error to be %v, got %v", c.error, err)
			}
		}
	})
}
