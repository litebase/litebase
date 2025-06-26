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
				ID:         uuid.NewString(),
				Statement:  "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
				Parameters: nil,
			},
			{
				ID:        uuid.NewString(),
				Statement: "INSERT INTO test (name) VALUES (?)",
				Parameters: []sql.Parameter{
					{
						Type:  "TEXT",
						Value: "name1",
					},
				},
			},
			{
				ID:         uuid.NewString(),
				Statement:  "SELECT * FROM test",
				Parameters: nil,
			},
		}

		url := fmt.Sprintf(
			"%s/%s/query/stream",
			testServer.Server.URL,
			testDatabase.DatabaseKey.Key,
		)

		connectionPool := sql.NewConnectionPool(
			testDatabase.AccessKey.AccessKeyID,
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

			testCase := sql.Query{}

			err = json.Unmarshal(testCaseJson, &testCase)

			if err != nil {
				t.Fatal(err)
			}

			result, err := connection.Send(testCase)

			if err != nil {
				t.Fatal(err)
			}

			if result.Error != nil {
				t.Fatal(string(result.Error))
			}

			if string(result.Data.ID) != testCase.ID {
				t.Fatalf("expected id %s, got %s", testCase.ID, string(result.Data.ID))
			}
		}

		connectionPool.Put(connection)
	})
}

func TestQueryStreamController_WithErrors(t *testing.T) {

	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		// Use invalid database key
		url := fmt.Sprintf(
			"%s/%s/query/stream",
			testServer.Server.URL,
			"invalid_database_key",
		)

		connectionPool := sql.NewConnectionPool(
			testDatabase.AccessKey.AccessKeyID,
			testDatabase.AccessKey.AccessKeySecret,
			url,
			5,
		)

		connection, err := connectionPool.Get()

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection.Send(sql.Query{
			ID:         uuid.NewString(),
			Statement:  "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
			Parameters: nil,
		})

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if err.Error() != "request failed: 400 Bad Request" {
			t.Fatalf("expected error 'request failed: 400 Bad Request', got %s", err.Error())
		}

		connectionPool.Put(connection)
		connectionPool.Close()

		// Use invalid access key
		url = fmt.Sprintf(
			"%s/%s/query/stream",
			testServer.Server.URL,
			testDatabase.DatabaseKey.Key,
		)

		connectionPool = sql.NewConnectionPool(
			"invalid_access_key_id",
			"invalid_access_key_secret",
			url,
			5,
		)

		connection, err = connectionPool.Get()

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection.Send(sql.Query{
			ID:         uuid.NewString(),
			Statement:  "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
			Parameters: nil,
		})

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if err.Error() != "request failed: 401 Unauthorized" {
			t.Fatalf("expected error 'request failed: 401 Unauthorized', got %s", err.Error())
		}

		connectionPool.Put(connection)
		connectionPool.Close()
	})
}

func TestQueryStreamController_WithValidationErrors(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		testCases := []*sql.Query{
			{
				ID:         uuid.NewString(),
				Statement:  "",
				Parameters: nil,
			},
			{
				ID:        uuid.NewString(),
				Statement: "INSERT INTO test (name) VALUES (?)",
				Parameters: []sql.Parameter{
					{
						Type:  "TEXT123",
						Value: "123",
					},
				},
			},
		}

		url := fmt.Sprintf(
			"%s/%s/query/stream",
			testServer.Server.URL,
			testDatabase.DatabaseKey.Key,
		)

		connectionPool := sql.NewConnectionPool(
			testDatabase.AccessKey.AccessKeyID,
			testDatabase.AccessKey.AccessKeySecret,
			url,
			5,
		)

		defer connectionPool.Close()

		connection, err := connectionPool.Get()

		if err != nil {
			t.Fatal(err)
		}

		// Send create table query
		result, err := connection.Send(sql.Query{
			ID:         uuid.NewString(),
			Statement:  "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
			Parameters: []sql.Parameter{},
		})

		if err != nil {
			t.Fatal(err)
		}

		if result.Error != nil {
			t.Fatal(string(result.Error))
		}

		for _, testCase := range testCases {
			testCaseJson, err := json.Marshal(testCase)

			if err != nil {
				t.Fatal(err)
			}

			testCase := sql.Query{}

			err = json.Unmarshal(testCaseJson, &testCase)

			if err != nil {
				t.Fatal(err)
			}

			result, err := connection.Send(testCase)

			if err != nil {
				t.Fatal(err)
			}

			if result.Error == nil {
				t.Error("expected error, got nil")
			}

			if string(result.Data.ID) != testCase.ID {
				t.Fatalf("expected id %s, got %s", testCase.ID, string(result.Data.ID))
			}
		}

		connectionPool.Put(connection)
	})
}
