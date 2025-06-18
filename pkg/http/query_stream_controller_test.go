package http_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/litebase/litebase-go/sql"

	"github.com/litebase/litebase/internal/test"
)

func TestQueryStreamController(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		testCases := []*sql.Query{
			{
				Id:         []byte(uuid.NewString()),
				Statement:  []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"),
				Parameters: nil,
			},
			{
				Id:        []byte(uuid.NewString()),
				Statement: []byte("INSERT INTO test (name) VALUES (?)"),
				Parameters: []sql.Parameter{
					{
						Type:  "TEXT",
						Value: "name1",
					},
				},
			},
			{
				Id:         []byte(uuid.NewString()),
				Statement:  []byte("SELECT * FROM test"),
				Parameters: nil,
			},
		}

		url := fmt.Sprintf(
			"%s/%s/query/stream",
			testServer.Server.URL,
			testDatabase.DatabaseKey.Key,
		)

		connectionPool := sql.NewConnectionPool(
			testDatabase.AccessKey.AccessKeyId,
			testDatabase.AccessKey.AccessKeySecret,
			url,
			5,
		)

		defer connectionPool.Close()

		connection, err := connectionPool.Get()

		if err != nil {
			t.Fatal(err)
		}

		for _, testCase := range testCases {
			testCaseJson, err := json.Marshal(testCase)

			if err != nil {
				t.Fatal(err)
			}

			testCaseMap := map[string]any{}

			err = json.Unmarshal(testCaseJson, &testCaseMap)

			if err != nil {
				t.Fatal(err)
			}

			result, err := connection.Send(testCaseMap)

			if err != nil {
				t.Fatal(err)
			}

			t.Log(result)
		}

		connectionPool.Put(connection)
	})
}
