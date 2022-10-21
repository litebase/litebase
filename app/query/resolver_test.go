package query

import (
	"litebasedb/runtime/app/database"
	"litebasedb/runtime/test"
	"log"
	"testing"
)

func TestNewResolver(t *testing.T) {
	resolver := NewResolver()

	if resolver == nil {
		t.Fatal("Resolver was not created")
	}
}

func TestHandle(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()

		resolver := NewResolver()

		db, err := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

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

			db, err = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

			if err != nil {
				t.Fatal(err)
			}

			query, err := NewQuery(db, mock["accessKeyId"], map[string]interface{}{
				"statement":  encrytpedQuery["statement"],
				"parameters": encrytpedQuery["parameters"],
			}, "")

			if err != nil {
				t.Fatal(err)
			}

			response := resolver.Handle(db, query, false)

			if response["status"] != c.expected {
				t.Fatalf("Query was not successful: %s", response["message"])
			}
		}

		batchCases := []struct {
			batch []struct {
				statement  string
				parameters string
				expected   string
			}
			expected string
		}{
			{
				[]struct {
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
				},
				`success`,
			},
		}

		for batchIndex, batch := range batchCases {
			for i, c := range batch.batch {
				encrytpedQuery := test.EncryptQuery(
					c.statement,
					c.parameters,
					mock["accessKeyId"],
					mock["accessKeySecret"],
				)

				batchCases[batchIndex].batch[i].statement = encrytpedQuery["statement"]
				batchCases[batchIndex].batch[i].parameters = encrytpedQuery["parameters"]
			}
		}

		for _, batchCase := range batchCases {
			db, err = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

			if err != nil {
				t.Fatal(err)
			}
			batchQueries := []map[string]interface{}{}

			for _, c := range batchCase.batch {
				batchQueries = append(batchQueries, map[string]interface{}{
					"statement":  c.statement,
					"parameters": c.parameters,
				})
			}

			query, err := NewQuery(db, mock["accessKeyId"], map[string]interface{}{
				"batch": batchQueries,
			}, "")

			if err != nil {
				t.Fatal(err)
			}

			response := resolver.Handle(db, query, false)
			log.Println(response)
			if response["status"] != batchCase.expected {
				t.Fatalf("Query was not successful: %s", response["message"])
			}
		}
	})
}
