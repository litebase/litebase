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
				"queries": []map[string]any{{
					"id":         "1",
					"statement":  "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
					"parameters": []map[string]any{},
				}},
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
				"queries": []map[string]any{{
					"id":        "1",
					"statement": "INSERT INTO test (value) VALUES (?);",
					"parameters": []map[string]any{{
						"type":  "TEXT",
						"value": "John Doe",
					}},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		responseData := resp["data"].([]any)[0].(map[string]any)
		if responseData["last_insert_row_id"].(float64) != 1 {
			t.Fatalf("Expected last_insert_row_id to be 1, got %v", responseData["last_insert_row_id"])
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
				"queries": []map[string]any{{
					"id":        "1",
					"statement": "SELECT * FROM test WHERE id = ?;",
					"parameters": []map[string]any{{
						"type":  "INTEGER",
						"value": 1,
					}},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		responseData = resp["data"].([]interface{})[0].(map[string]interface{})
		if int(responseData["rows"].([]any)[0].([]any)[0].(float64)) != 1 {
			t.Fatalf("Expected id to be 1, got %v", responseData["rows"].([]any)[0].([]any)[0])
		}

		if responseData["rows"].([]any)[0].([]any)[1] != "John Doe" {
			t.Fatalf("Expected value to be 'John Doe', got %v", responseData["rows"].([]any)[0].([]any)[1])
		}
	})
}

func TestQueryControllerMultipleQueries(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{
					{
						"id":         "1",
						"statement":  "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
						"parameters": []map[string]any{},
					},
					{
						"id":        "2",
						"statement": "INSERT INTO test (value) VALUES (?);",
						"parameters": []map[string]any{
							{
								"type":  "TEXT",
								"value": "Jane Doe",
							},
						},
					},
					{
						"id":        "3",
						"statement": "SELECT * FROM test WHERE id = ?;",
						"parameters": []map[string]any{
							{
								"type":  "INTEGER",
								"value": 1,
							},
						},
					},
				},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		responseData := resp["data"].([]any)

		if len(responseData) != 3 {
			t.Fatalf("Expected 3 responses, got %d", len(responseData))
		}

		if responseData[0].(map[string]any)["rows"] != nil {
			t.Fatalf("Expected no rows for CREATE statement, got %v", responseData[0])
		}

		if responseData[1].(map[string]any)["last_insert_row_id"].(float64) != 1 {
			t.Fatalf("Expected last_insert_row_id to be 1, got %v", responseData[1])
		}

		rows := responseData[2].(map[string]any)["rows"].([]any)

		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}

		row := rows[0].([]any)

		if int(row[0].(float64)) != 1 {
			t.Fatalf("Expected id to be 1, got %v", row[0])
		}

		if row[1] != "Jane Doe" {
			t.Fatalf("Expected value to be 'Jane Doe', got %v", row[1])
		}
	})
}

func TestQueryController_Errors(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		// Test invalid database key
		resp, responseCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/%s/query", "invalidDatabase", "invalidBranch"), "POST", map[string]any{
			"queries": []map[string]any{{
				"id":         "1",
				"statement":  "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
				"parameters": []map[string]any{},
			}},
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
			"queries": []map[string]any{{
				"id":         "1",
				"statement":  "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
				"parameters": []map[string]any{},
			}},
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
				"queries": []map[string]any{{
					"id":         "1",
					"statement":  "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
					"parameters": "test",
				}},
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
				"queries": []map[string]any{
					{
						"id": "1",
						"parameters": []map[string]any{{
							"type":  "STRING",
							"value": "",
						}},
					},
					{
						"id": "1",
						"parameters": []map[string]any{{
							"type":  "TEXT",
							"value": "",
						}},
					},
					{
						"id":        "2",
						"statement": "CREATE table test (id INTEGER PRIMARY KEY, value TEXT);",
						"parameters": []map[string]any{{
							"type":  "TEXT",
							"value": nil,
						}},
					},
					{
						"id":        "3",
						"statement": "INSERT INTO test (value) VALUES (?);",
						"parameters": []map[string]any{{
							"type":  "NULL",
							"value": nil,
						}},
					},
				},
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
