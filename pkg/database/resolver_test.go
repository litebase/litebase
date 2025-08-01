package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestQueryResolver_Handle(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(db)

		db.GetConnection().Exec("CREATE TABLE users (id INT, name TEXT)", []sqlite3.StatementParameter{})

		cases := []struct {
			statement  string
			parameters []sqlite3.StatementParameter
			expected   string
		}{
			{
				"SELECT * FROM users",
				[]sqlite3.StatementParameter{},
				"success",
			},
			{
				"SELECT * FROM users LIMIT ?",
				[]sqlite3.StatementParameter{
					{
						Type:  "INTEGER",
						Value: int64(1),
					},
				},
				"success",
			},
			{
				"?SELECT * FROM users",
				[]sqlite3.StatementParameter{},
				"error",
			},
		}

		queryResponse := &database.QueryResponse{}

		for _, c := range cases {
			q, err := database.NewQuery(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				auth.NewDatabaseKey(mock.DatabaseID, mock.DatabaseName, mock.DatabaseBranchID, mock.BranchName),
				mock.AccessKey,
				&database.QueryInput{
					Statement:  c.statement,
					Parameters: c.parameters,
					Id:         "",
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			queryResponse.Reset()

			_, err = q.Resolve(queryResponse)

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
		// 			mock.AccessKeyID,
		// 			mock.AccessKeySecret,
		// 		)

		// 		batchCases[batchIndex].batch[i].statement = encrytpedQuery["statement"]
		// 		batchCases[batchIndex].batch[i].parameters = encrytpedQuery["parameters"]
		// 	}
		// }

		// for _, batchCase := range batchCases {
		// 	db, err = database.Get(mock.DatabaseID, mock.DatabaseBranchID, nil, false)

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
		// 		mock.AccessKeyID,
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
