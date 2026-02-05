package http_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/litebase/litebase-go/sql"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
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
			"%s/v1/databases/%s/branches/%s",
			testServer.Server.URL,
			testDatabase.DatabaseName,
			testDatabase.BranchName,
		)

		connectionPool := sql.NewConnectionPool(
			testDatabase.Credential.AccessKey().AccessKeyID,
			testDatabase.Credential.AccessKey().AccessKeySecret,
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
			"%s/v1/databases/%s/branches/%s",
			testServer.Server.URL,
			"invalid_database_key",
			testDatabase.BranchName,
		)

		connectionPool := sql.NewConnectionPool(
			testDatabase.Credential.AccessKey().AccessKeyID,
			testDatabase.Credential.AccessKey().AccessKeySecret,
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

		if err.Error() != "request failed: 404 Not Found" {
			t.Fatalf("expected error 'request failed: 404 Not Found', got %s", err.Error())
		}

		connectionPool.Put(connection)
		connectionPool.Close()

		// Use invalid access key
		url = fmt.Sprintf(
			"%s/v1/databases/%s/branches/%s",
			testServer.Server.URL,
			testDatabase.DatabaseName,
			testDatabase.BranchName,
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
			"%s/v1/databases/%s/branches/%s",
			testServer.Server.URL,
			testDatabase.DatabaseName,
			testDatabase.BranchName,
		)

		connectionPool := sql.NewConnectionPool(
			testDatabase.Credential.AccessKey().AccessKeyID,
			testDatabase.Credential.AccessKey().AccessKeySecret,
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

func TestQueryStreamController_RequiresAccessKeyAuth(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		// Create a token credential instead of access key
		token, err := testServer.App.Cluster.Auth.TokenManager.Create(
			"Test token",
			[]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: auth.Resource(fmt.Sprintf("database:%s:branch:%s", testDatabase.DatabaseID, testDatabase.DatabaseBranchID)),
					Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery},
				},
			},
		)

		if err != nil {
			t.Fatal(err)
		}

		tokenValue, err := token.Value()

		if err != nil {
			t.Fatal(err)
		}

		url := fmt.Sprintf(
			"%s/v1/databases/%s/branches/%s/query/stream",
			testServer.Server.URL,
			testDatabase.DatabaseName,
			testDatabase.BranchName,
		)

		// Try to connect with token authentication
		client := &http.Client{}

		req, err := http.NewRequest("POST", url, nil)

		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tokenValue))
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Upgrade", "lqtp")
		req.Header.Set("Connection", "Upgrade")

		resp, err := client.Do(req)

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			err := resp.Body.Close()

			if err != nil {
				t.Fatal(err)
			}
		}()

		// Should return 400 Bad Request
		if resp.StatusCode != 400 {
			t.Fatalf("expected status code 400, got %d", resp.StatusCode)
		}

		// Check error message
		body, err := io.ReadAll(resp.Body)

		if err != nil {
			t.Fatal(err)
		}

		var errorResponse map[string]any

		err = json.Unmarshal(body, &errorResponse)

		if err != nil {
			t.Fatal(err)
		}

		errorMsg, ok := errorResponse["error"].(string)

		if !ok {
			t.Fatal("expected error field in response")
		}

		expectedError := "Query stream connections require access key authentication. Token and basic auth are not supported for LQTP protocol."

		if errorMsg != expectedError {
			t.Fatalf("expected error message '%s', got '%s'", expectedError, errorMsg)
		}
	})
}

func TestQueryStreamControllerBlobHandling(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		url := fmt.Sprintf(
			"%s/v1/databases/%s/branches/%s",
			testServer.Server.URL,
			testDatabase.DatabaseName,
			testDatabase.BranchName,
		)

		connectionPool := sql.NewConnectionPool(
			testDatabase.Credential.AccessKey().AccessKeyID,
			testDatabase.Credential.AccessKey().AccessKeySecret,
			url,
			5,
		)

		defer connectionPool.Close()

		connection, err := connectionPool.Get()

		if err != nil {
			t.Fatal(err)
		}

		defer connectionPool.Put(connection)

		// Create a table with a BLOB column
		createTableQuery := sql.Query{
			ID:         uuid.NewString(),
			Statement:  "CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, content BLOB)",
			Parameters: nil,
		}

		result, err := connection.Send(createTableQuery)

		if err != nil {
			t.Fatal(err)
		}

		if result.Error != nil {
			t.Fatal(string(result.Error))
		}

		// Test 1: Insert a blob with binary data (client library will base64 encode it)
		binaryData := []byte("Hello World")

		insertQuery := sql.Query{
			ID:        uuid.NewString(),
			Statement: "INSERT INTO files (name, content) VALUES (?, ?)",
			Parameters: []sql.Parameter{
				{
					Type:  "TEXT",
					Value: "test.bin",
				},
				{
					Type:  "BLOB",
					Value: binaryData,
				},
			},
		}

		result, err = connection.Send(insertQuery)

		if err != nil {
			t.Fatal(err)
		}

		if result.Error != nil {
			t.Fatalf("Failed to insert blob: %s", string(result.Error))
		}

		if result.Data.LastInsertRowID != 1 {
			t.Fatalf("Expected lastInsertRowId to be 1, got %d", result.Data.LastInsertRowID)
		}

		// Test 2: Select the blob and verify it was stored correctly
		selectQuery := sql.Query{
			ID:        uuid.NewString(),
			Statement: "SELECT name, content FROM files WHERE id = ?",
			Parameters: []sql.Parameter{
				{
					Type:  "INTEGER",
					Value: 1,
				},
			},
		}

		result, err = connection.Send(selectQuery)

		if err != nil {
			t.Fatal(err)
		}

		if result.Error != nil {
			t.Fatalf("Failed to select blob: %s", string(result.Error))
		}

		rows := result.Data.Rows
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}

		// Test 3: Insert empty blob
		emptyBlobQuery := sql.Query{
			ID:        uuid.NewString(),
			Statement: "INSERT INTO files (name, content) VALUES (?, ?)",
			Parameters: []sql.Parameter{
				{
					Type:  "TEXT",
					Value: "empty.bin",
				},
				{
					Type:  "BLOB",
					Value: []byte{},
				},
			},
		}

		result, err = connection.Send(emptyBlobQuery)

		if err != nil {
			t.Fatal(err)
		}

		if result.Error != nil {
			t.Fatalf("Failed to insert empty blob: %s", string(result.Error))
		}

		// Test 4: Insert large blob (1KB of data)
		largeData := make([]byte, 1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		largeBlobQuery := sql.Query{
			ID:        uuid.NewString(),
			Statement: "INSERT INTO files (name, content) VALUES (?, ?)",
			Parameters: []sql.Parameter{
				{
					Type:  "TEXT",
					Value: "large.bin",
				},
				{
					Type:  "BLOB",
					Value: largeData,
				},
			},
		}

		result, err = connection.Send(largeBlobQuery)

		if err != nil {
			t.Fatal(err)
		}

		if result.Error != nil {
			t.Fatalf("Failed to insert large blob: %s", string(result.Error))
		}

		// Verify the large blob was stored correctly by checking its length
		lengthQuery := sql.Query{
			ID:        uuid.NewString(),
			Statement: "SELECT LENGTH(content) FROM files WHERE name = ?",
			Parameters: []sql.Parameter{
				{
					Type:  "TEXT",
					Value: "large.bin",
				},
			},
		}

		result, err = connection.Send(lengthQuery)

		if err != nil {
			t.Fatal(err)
		}

		if result.Error != nil {
			t.Fatalf("Failed to get blob length: %s", string(result.Error))
		}

		lengthRows := result.Data.Rows
		if len(lengthRows) != 1 {
			t.Fatalf("Expected 1 row for length query, got %d", len(lengthRows))
		}
	})
}
