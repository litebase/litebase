package query_test

import (
	"litebasedb/internal/test"
	"litebasedb/server/database"
	"litebasedb/server/query"
	"testing"
)

func TestNewResolver(t *testing.T) {
	resolver := query.NewResolver()

	if resolver == nil {
		t.Fatal("Resolver was not created")
	}
}

func TestHandle(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		resolver := query.NewResolver()

		db, err := database.ConnectionManager().Get(mock["databaseUuid"], mock["branchUuid"])

		if err != nil {
			t.Fatal(err)
		}

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		cases := []struct {
			statement  string
			parameters string
			expected   string
		}{
			{
				"SELECT * FROM users",
				"[]",
				`success`,
			},
			{
				"SELECT * FROM users LIMIT ?",
				"[]",
				`error`,
			},
			{
				"?SELECT * FROM users",
				"[]",
				`error`,
			},
		}

		for _, c := range cases {
			encrytpedQuery := test.EncryptQuery(
				c.statement,
				c.parameters,
				mock["accessKeyId"],
				mock["accessKeySecret"],
			)

			db, err = database.ConnectionManager().Get(mock["databaseUuid"], mock["branchUuid"])

			if err != nil {
				t.Fatal(err)
			}

			query, err := query.NewQuery(
				db,
				mock["accessKeyId"],
				map[string]interface{}{
					"statement":  encrytpedQuery["statement"],
					"parameters": encrytpedQuery["parameters"],
				},
				"",
			)

			if err != nil {
				t.Fatal(err)
			}

			response := resolver.Handle(db, query)

			if response["status"] != c.expected {
				t.Fatalf("Query was not successful: %s", response["message"])
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
		// 			mock["accessKeyId"],
		// 			mock["accessKeySecret"],
		// 		)

		// 		batchCases[batchIndex].batch[i].statement = encrytpedQuery["statement"]
		// 		batchCases[batchIndex].batch[i].parameters = encrytpedQuery["parameters"]
		// 	}
		// }

		// for _, batchCase := range batchCases {
		// 	db, err = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

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
		// 		mock["accessKeyId"],
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
