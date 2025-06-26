package http_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestTransactionCommitController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		database := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(
			database.DatabaseKey.DatabaseID,
			database.DatabaseKey.BranchID,
		)

		defer server.App.DatabaseManager.ConnectionManager().Release(
			database.DatabaseKey.DatabaseID,
			database.DatabaseKey.BranchID,
			con,
		)

		if err != nil {
			t.Fatalf("Failed to get database connection: %v", err)
		}

		// Create a test table
		_, err = con.GetConnection().Exec("CREATE TABLE test (id TEXT PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Failed to create test table: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"*"},
			},
		})

		var transactionId string

		t.Run("Store", func(t *testing.T) {
			response, statusCode, err := client.Send(
				fmt.Sprintf("/%s/transactions", database.DatabaseKey.Key),
				"POST", map[string]any{},
			)

			if err != nil {
				t.Fatalf("Failed to create transaction: %v", err)
			}

			if statusCode != 200 {
				t.Log("Response:", response["message"])
				t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["message"] != "Transaction created successfully" {
				t.Errorf("Unexpected message: %s, expected 'Transaction created successfully'", response["message"])
			}

			data, ok := response["data"].(map[string]any)

			if !ok {
				t.Fatal("Response data is not a map")
			}

			transactionId, ok = data["id"].(string)

			if !ok || transactionId == "" {
				t.Fatal("Transaction ID is empty or not a string")
			}
		})

		t.Run("Query", func(t *testing.T) {
			response, statusCode, err := client.Send(
				fmt.Sprintf("/%s/query", database.DatabaseKey.Key),
				"POST",
				map[string]any{
					"id":             uuid.NewString(),
					"transaction_id": transactionId,
					"statement":      "INSERT INTO test (id, value) VALUES (?, ?)",
					"parameters": []map[string]any{
						{
							"type":  "TEXT",
							"value": "test",
						},
					},
				},
			)

			if err != nil {
				t.Fatalf("Failed to query database: %v", err)
			}

			if statusCode != 200 {
				t.Log("Response:", response["message"])
				t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}

			// Query the count of rows in the test table
			countResponse, countStatusCode, err := client.Send(
				fmt.Sprintf("/%s/query", database.DatabaseKey.Key),
				"POST",
				map[string]any{
					"id":             uuid.NewString(),
					"transaction_id": transactionId,
					"statement":      "SELECT COUNT(*) FROM test",
					"parameters":     []map[string]any{},
				},
			)

			if err != nil {
				t.Fatalf("Failed to count rows in test table: %v", err)
			}

			if countStatusCode != 200 {
				t.Log("Response:", countResponse["message"])
				t.Fatalf("Unexpected status code: %d, expected 200", countStatusCode)
			}

			if countResponse["status"] != "success" {
				t.Errorf("Unexpected response: %v", countResponse)
			}

			data, ok := countResponse["data"].(map[string]any)

			if !ok {
				t.Fatal("Count response data is not a map")
			}

			rows, ok := data["rows"].([]any)

			if !ok || len(rows) == 0 {
				t.Fatal("Count response rows is not a slice or is empty")
			}

			if len(rows) != 1 || rows[0] == nil {
				t.Fatal("Count response does not contain a valid count")
			}
			row, ok := rows[0].([]any)
			if !ok {
				t.Fatal("Count response row is not a map")
			}

			if count := row[0].(float64); count != 1 {
				t.Errorf("Expected 1 row in test table, got %d", int(count))
			}
		})

		t.Run("Commit", func(t *testing.T) {
			if transactionId == "" {
				t.Fatal("Transaction ID is empty, cannot commit transaction")
			}

			response, statusCode, err := client.Send(
				fmt.Sprintf("/%s/transactions/%s/commit", database.DatabaseKey.Key, transactionId),
				"POST", nil,
			)

			if err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}

			if statusCode != 200 {
				t.Log("Response:", response["message"])
				t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["message"] != "Transaction committed successfully" {
				t.Errorf("Unexpected message: %s, expected 'Transaction committed successfully'", response["message"])
			}
		})

		t.Run("Query after commit", func(t *testing.T) {
			result, err := con.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Fatalf("Failed to query after transaction commit: %v", err)
			}

			rows := result.Rows

			if len(rows) == 0 {
				t.Fatal("Expected at least one row in result after transaction commit")
			}
			row := rows[0]
			if len(row) == 0 {
				t.Fatal("Expected at least one column in row after transaction commit")
			}

			count := row[0].Int64()

			if count != 1 {
				t.Errorf("Expected 1 row in test table after transaction commit, got %d", count)
			}
		})
	})
}

func TestTransactionCommitController_WithInvalidTransaction(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		database := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"*"},
			},
		})

		transactionId := uuid.NewString()

		t.Run("Commit with invalid transaction", func(t *testing.T) {
			response, statusCode, err := client.Send(
				fmt.Sprintf("/%s/transactions/%s/commit", database.DatabaseKey.Key, transactionId),
				"POST", nil,
			)

			if err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}

			if statusCode != 404 {
				t.Log("Response:", response["message"])
				t.Fatalf("Unexpected status code: %d, expected 404", statusCode)
			}

			if response["status"] != "error" {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["message"] != "Error: transaction not found" {
				t.Errorf("Unexpected message: %s, expected 'Error: transaction not found'", response["message"])
			}
		})
	})
}
