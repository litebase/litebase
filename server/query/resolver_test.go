package query_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/database"
	"litebase/server/query"
	"testing"
)

func TestHandle(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

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

		queryResponse := &query.QueryResponse{}

		for _, c := range cases {
			q, err := query.NewQuery(
				app.Cluster,
				app.DatabaseManager,
				database.NewDatabaseKey(mock.DatabaseId, mock.BranchId),
				mock.AccessKey,
				&query.QueryInput{
					Statement:  c.statement,
					Parameters: c.parameters,
					Id:         "",
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			queryResponse.Reset()

			err = query.ResolveQuery(q, queryResponse)

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
		// 	db, err = database.Get(mock.DatabaseId, mock.BranchId, nil, false)

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
