package query_test

import (
	"litebase/internal/test"
	"litebase/server/database"
	"litebase/server/query"
	"testing"
)

func TestHandle(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatal(err)
		}

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		cases := []struct {
			statement  string
			parameters []interface{}
			expected   string
		}{
			{
				"SELECT * FROM users",
				[]interface{}{},
				`success`,
			},
			{
				"SELECT * FROM users LIMIT ?",
				[]interface{}{1},
				`success`,
			},
			{
				"?SELECT * FROM users",
				[]interface{}{},
				`error`,
			},
		}

		for _, c := range cases {
			db, err = database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

			if err != nil {
				t.Fatal(err)
			}

			q, err := query.NewQuery(
				db,
				mock.AccessKeyId,
				map[string]interface{}{
					"statement":  c.statement,
					"parameters": c.parameters,
				},
				"",
			)

			if err != nil {
				t.Fatal(err)
			}

			response, err := query.ResolveQuery(db, q)

			if response["status"] != c.expected {
				t.Fatalf("Query was not successful: %s", response["message"])
			}

			if err != nil && c.expected == `success` {
				t.Fatal(err)
			}
		}

		// batchCases := []struct {
		// 	batch []struct {
		// 		statement  string
		// 		parameters string
		// 		expected   string
		// 	}
		// 	expected string
		// }{
		// 	{
		// 		[]struct {
		// 			statement  string
		// 			parameters string
		// 			expected   string
		// 		}{
		// 			{
		// 				"SELECT * FROM users",
		// 				"[]",
		// 				`success`,
		// 			},
		// 			{
		// 				"SELECT * FROM users LIMIT ?",
		// 				"[]",
		// 				`error`,
		// 			},
		// 			{
		// 				"?SELECT * FROM users",
		// 				"[]",
		// 				`error`,
		// 			},
		// 		},
		// 		`success`,
		// 	},
		// }

		// for batchIndex, batch := range batchCases {
		// 	for i, c := range batch.batch {
		// 		encrytpedQuery := test.EncryptQuery(
		// 			c.statement,
		// 			c.parameters,
		// 			mock.AccessKeyId,
		// 			mock.AccessKeySecret,
		// 		)

		// 		batchCases[batchIndex].batch[i].statement = encrytpedQuery["statement"]
		// 		batchCases[batchIndex].batch[i].parameters = encrytpedQuery["parameters"]
		// 	}
		// }

		// for _, batchCase := range batchCases {
		// 	db, err = database.Get(mock.DatabaseUuid, mock.BranchUuid, nil, false)

		// 	if err != nil {
		// 		t.Fatal(err)
		// 	}
		// 	batchQueries := []map[string]interface{}{}

		// 	for _, c := range batchCase.batch {
		// 		batchQueries = append(batchQueries, map[string]interface{}{
		// 			"statement":  c.statement,
		// 			"parameters": c.parameters,
		// 		})
		// 	}

		// 	query, err := query.NewQuery(
		// 		db,
		// 		mock.AccessKeyId,
		// 		map[string]interface{}{
		// 			"batch": batchQueries,
		// 		},
		// 		"",
		// 	)

		// 	if err != nil {
		// 		t.Fatal(err)
		// 	}

		// 	response := resolver.Handle(db, query)

		// 	if response["status"] != batchCase.expected {
		// 		t.Fatalf("Query was not successful: %s", response["message"])
		// 	}
		// }
	})
}
