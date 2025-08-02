package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestQueryController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		// Get a new connection for the HTTP request verification
		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		// Create a table
		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"id":         "1",
				"statement":  "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
				"parameters": []map[string]any{},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// Insert a row
		resp, responseCode, err = client.Send(fmt.Sprintf(
			"/v1/databases/%s/%s/query",
			mock.DatabaseName,
			mock.BranchName,
		),
			"POST",
			map[string]any{
				"id":        "1",
				"statement": "INSERT INTO test (value) VALUES (?);",
				"parameters": []map[string]any{{
					"type":  "TEXT",
					"value": "John Doe",
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		if resp["data"].(map[string]any)["last_insert_row_id"].(float64) != 1 {
			t.Fatalf("Expected last_insert_row_id to be 1, got %v", resp["data"].(map[string]any)["last_insert_row_id"])
		}

		// Select the row
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"id":        "1",
				"statement": "SELECT * FROM test WHERE id = ?;",
				"parameters": []map[string]any{{
					"type":  "INTEGER",
					"value": 1,
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		if int(resp["data"].(map[string]any)["rows"].([]any)[0].([]any)[0].(float64)) != 1 {
			t.Fatalf("Expected id to be 1, got %v", resp["data"].(map[string]any)["rows"].([]any)[0].([]any)[0])
		}

		if resp["data"].(map[string]any)["rows"].([]any)[0].([]any)[1] != "John Doe" {
			t.Fatalf("Expected value to be 'John Doe', got %v", resp["data"].(map[string]any)["rows"].([]any)[0].([]any)[1])
		}
	})
}

func TestQueryController_Errors(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Get a new connection for the HTTP request verification
		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		// Test invalid database key
		resp, responseCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/%s/query", "invalidDatabase", "invalidBranch"), "POST", map[string]any{
			"id":         "1",
			"statement":  "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
			"parameters": []map[string]any{},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 404 {
			t.Fatalf("Expected response code 404, got %d: %s", responseCode, resp)
		}

		db, err := server.App.DatabaseManager.Create("test", "main")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = server.App.DatabaseManager.Delete(db)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		primaryBranch := db.PrimaryBranch()

		if primaryBranch == nil {
			t.Fatal("Expected primary branch to be found, but got nil")
		}

		resp, responseCode, err = client.Send(fmt.Sprintf("/v1/databases/%s/%s/query", "test", "main"), "POST", map[string]any{
			"id":         "1",
			"statement":  "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
			"parameters": []map[string]any{},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 404 {
			t.Fatalf("Expected response code 400, got %d: %s", responseCode, resp)
		}

		// Test bad input
		mock := test.MockDatabase(server.App)

		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"id":         "1",
				"statements": "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
				"parameters": "test",
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 400 {
			t.Fatalf("Expected response code 400, got %d: %s", responseCode, resp)
		}

		// Test invalid input
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"id":         "1",
				"statements": "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
				"parameters": []map[string]any{{
					"type": "STRING",
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 422 {
			t.Fatalf("Expected response code 422, got %d: %s", responseCode, resp)
		}
	})
}
