package http_test

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestQueryController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		// Get a new connection for the HTTP request verification
		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		// Create a table
		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
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
			"/v1/databases/%s/branches/%s/query",
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
		if responseData["lastInsertRowId"].(float64) != 1 {
			t.Fatalf("Expected lastInsertRowId to be 1, got %v", responseData["lastInsertRowId"])
		}

		// Select the row
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
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

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
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

		if len(responseData[0].(map[string]any)["rows"].([]any)) != 0 {
			t.Fatalf("Expected no rows for CREATE statement, got %v", responseData[0])
		}

		if responseData[1].(map[string]any)["lastInsertRowId"].(float64) != 1 {
			t.Fatalf("Expected lastInsertRowId to be 1, got %v", responseData[1])
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

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		// Test invalid database key
		resp, responseCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches/%s/query", "invalidDatabase", "invalidBranch"), "POST", map[string]any{
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

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if primaryBranch == nil {
			t.Fatal("Expected primary branch to be found, but got nil")
		}

		resp, responseCode, err = client.Send(fmt.Sprintf("/v1/databases/%s/branches/%s/query", "test", "main"), "POST", map[string]any{
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
				"/v1/databases/%s/branches/%s/query",
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
				"/v1/databases/%s/branches/%s/query",
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

func TestQueryControllerColumnsInResponse(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		// Get a new connection for the HTTP request verification
		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		// Create a table with specific column names
		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":         "1",
					"statement":  "CREATE table users (user_id INTEGER PRIMARY KEY, username TEXT, email TEXT);",
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

		// Insert some test data
		resp, responseCode, err = client.Send(fmt.Sprintf(
			"/v1/databases/%s/branches/%s/query",
			mock.DatabaseName,
			mock.BranchName,
		),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "2",
					"statement": "INSERT INTO users (username, email) VALUES (?, ?);",
					"parameters": []map[string]any{
						{
							"type":  "TEXT",
							"value": "john_doe",
						},
						{
							"type":  "TEXT",
							"value": "john@example.com",
						},
					},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// Select with explicit column names to verify columns are returned
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "3",
					"statement": "SELECT user_id, username, email FROM users WHERE user_id = ?;",
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

		responseData := resp["data"].([]any)[0].(map[string]any)

		// Verify columns field exists and contains expected column names
		columns, exists := responseData["columns"]

		if !exists {
			t.Fatal("Expected 'columns' field to be present in response")
		}

		columnsList, ok := columns.([]any)

		if !ok {
			t.Fatalf("Expected columns to be a slice, got %T", columns)
		}

		expectedColumns := []sqlite3.ColumnDefinition{
			{ColumnName: "user_id", ColumnType: sqlite3.ColumnTypeInteger},
			{ColumnName: "username", ColumnType: sqlite3.ColumnTypeText},
			{ColumnName: "email", ColumnType: sqlite3.ColumnTypeText},
		}

		if len(columnsList) != len(expectedColumns) {
			t.Fatalf("Expected %d columns, got %d", len(expectedColumns), len(columnsList))
		}

		for i, expectedCol := range expectedColumns {
			colDef, ok := columnsList[i].(map[string]any)
			if !ok {
				t.Fatalf("Expected column definition to be a map, got %T", columnsList[i])
			}

			if colDef["name"] != expectedCol.ColumnName {
				t.Fatalf("Expected column %d name to be '%s', got '%v'", i, expectedCol.ColumnName, colDef["name"])
			}

			if int(colDef["type"].(float64)) != int(expectedCol.ColumnType) {
				t.Fatalf("Expected column %d type to be '%d', got '%v'", i, expectedCol.ColumnType, colDef["type"])
			}
		}

		// Verify rows field exists and contains expected data
		rows, exists := responseData["rows"]

		if !exists {
			t.Fatal("Expected 'rows' field to be present in response")
		}

		rowsList, ok := rows.([]any)

		if !ok {
			t.Fatalf("Expected rows to be a slice, got %T", rows)
		}

		if len(rowsList) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rowsList))
		}

		row := rowsList[0].([]any)

		if len(row) != 3 {
			t.Fatalf("Expected 3 columns in row, got %d", len(row))
		}

		// Verify the data matches what we inserted
		if int(row[0].(float64)) != 1 {
			t.Fatalf("Expected user_id to be 1, got %v", row[0])
		}

		if row[1] != "john_doe" {
			t.Fatalf("Expected username to be 'john_doe', got %v", row[1])
		}

		if row[2] != "john@example.com" {
			t.Fatalf("Expected email to be 'john@example.com', got %v", row[2])
		}

		// Verify row_count field
		rowCount, exists := responseData["rowCount"]

		if !exists {
			t.Fatal("Expected 'rowCount' field to be present in response")
		}

		if int(rowCount.(float64)) != 1 {
			t.Fatalf("Expected row_count to be 1, got %v", rowCount)
		}
	})
}

func TestQueryControllerResponseStructureConsistency(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		// Test multiple types of queries to ensure consistent response structure
		queries := []struct {
			name          string
			statement     string
			params        []map[string]any
			expectResults bool
		}{
			{
				name:          "CREATE table",
				statement:     "CREATE table test_table (id INTEGER PRIMARY KEY, name TEXT, score FLOAT);",
				params:        []map[string]any{},
				expectResults: false,
			},
			{
				name:      "INSERT query",
				statement: "INSERT INTO test_table (name, score) VALUES (?, ?);",
				params: []map[string]any{
					{"type": "TEXT", "value": "Alice"},
					{"type": "FLOAT", "value": 95.5},
				},
				expectResults: false,
			},
			{
				name:      "SELECT query",
				statement: "SELECT id, name, score FROM test_table WHERE id = ?;",
				params: []map[string]any{
					{"type": "INTEGER", "value": 1},
				},
				expectResults: true,
			},
		}

		for _, query := range queries {
			t.Run(query.name, func(t *testing.T) {
				resp, responseCode, err := client.Send(
					fmt.Sprintf(
						"/v1/databases/%s/branches/%s/query",
						mock.DatabaseName,
						mock.BranchName,
					),
					"POST",
					map[string]any{
						"queries": []map[string]any{{
							"id":         "test-query",
							"statement":  query.statement,
							"parameters": query.params,
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

				// Verify all required fields are present
				requiredFields := []string{"changes", "id", "latency", "lastInsertRowId", "rowCount", "rows", "columns"}

				for _, field := range requiredFields {
					if _, exists := responseData[field]; !exists {
						t.Errorf("Expected field '%s' to be present in response for %s", field, query.name)
					}
				}

				// Verify field types
				if _, ok := responseData["changes"].(float64); !ok {
					t.Errorf("Expected 'changes' to be a number for %s", query.name)
				}

				if _, ok := responseData["id"].(string); !ok {
					t.Errorf("Expected 'id' to be a string for %s", query.name)
				}

				if _, ok := responseData["latency"].(float64); !ok {
					t.Errorf("Expected 'latency' to be a number for %s", query.name)
				}

				if _, ok := responseData["lastInsertRowId"].(float64); !ok {
					t.Errorf("Expected 'lastInsertRowId' to be a number for %s", query.name)
				}

				if _, ok := responseData["rowCount"].(float64); !ok {
					t.Errorf("Expected 'rowCount' to be a number for %s", query.name)
				}

				// For rows and columns, they might be null for non-SELECT queries
				if rows := responseData["rows"]; rows != nil {
					if _, ok := rows.([]any); !ok {
						t.Errorf("Expected 'rows' to be an array or null for %s, got %T", query.name, rows)
					}
				}

				if columns := responseData["columns"]; columns != nil {
					if _, ok := columns.([]any); !ok {
						t.Errorf("Expected 'columns' to be an array or null for %s, got %T", query.name, columns)
					}
				}

				// For SELECT queries, verify we have results
				if query.expectResults {
					rows := responseData["rows"]
					columns := responseData["columns"]

					if rows == nil {
						t.Errorf("Expected rows for SELECT query, but got nil")
					} else {
						rowsArray := rows.([]any)

						if len(rowsArray) == 0 {
							t.Errorf("Expected rows for SELECT query, but got empty array")
						}
					}

					if columns == nil {
						t.Errorf("Expected columns for SELECT query, but got nil")
					} else {
						columnsArray := columns.([]any)

						if len(columnsArray) == 0 {
							t.Errorf("Expected columns for SELECT query, but got empty array")
						}

						// Verify columns match expected structure
						expectedColumns := []sqlite3.ColumnDefinition{
							{ColumnName: "id", ColumnType: sqlite3.ColumnTypeInteger},
							{ColumnName: "name", ColumnType: sqlite3.ColumnTypeText},
							{ColumnName: "score", ColumnType: sqlite3.ColumnTypeFloat},
						}

						if len(columnsArray) != len(expectedColumns) {
							t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columnsArray))
						}

						for i, expectedCol := range expectedColumns {
							colDef, ok := columnsArray[i].(map[string]any)

							if !ok {
								t.Fatalf("Expected column definition to be a map, got %T", columnsArray[i])
							}

							if colDef["name"] != expectedCol.ColumnName {
								t.Fatalf("Expected column %d name to be '%s', got '%v'", i, expectedCol.ColumnName, colDef["name"])
							}

							if int(colDef["type"].(float64)) != int(expectedCol.ColumnType) {
								t.Fatalf("Expected column %d type to be '%d', got '%v'", i, expectedCol.ColumnType, colDef["type"])
							}
						}
					}
				}
			})
		}
	})
}

func TestQueryControllerVsStreamControllerConsistency(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery, auth.DatabasePrivilegeCreateTable, auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeTransaction, auth.DatabasePrivilegeUpdate},
			},
		})

		// Setup test data
		_, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":         "setup-table",
					"statement":  "CREATE table consistency_test (id INTEGER PRIMARY KEY, name TEXT, active INTEGER);",
					"parameters": []map[string]any{},
				}},
			},
		)

		if err != nil || responseCode != 200 {
			t.Fatalf("Setup failed: %v, code: %d", err, responseCode)
		}

		_, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "setup-data",
					"statement": "INSERT INTO consistency_test (name, active) VALUES (?, ?);",
					"parameters": []map[string]any{
						{"type": "TEXT", "value": "Test User"},
						{"type": "INTEGER", "value": 1},
					},
				}},
			},
		)

		if err != nil || responseCode != 200 {
			t.Fatalf("Data setup failed: %v, code: %d", err, responseCode)
		}

		// Test query controller response structure
		queryControllerResp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "test-select",
					"statement": "SELECT id, name, active FROM consistency_test WHERE id = ?;",
					"parameters": []map[string]any{
						{"type": "INTEGER", "value": 1},
					},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Query controller request failed: %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, queryControllerResp)
		}

		queryResponseData := queryControllerResp["data"].([]any)[0].(map[string]any)

		// Verify that the query controller response includes all expected fields
		expectedFields := []string{"changes", "columns", "id", "latency", "lastInsertRowId", "rowCount", "rows"}

		for _, field := range expectedFields {
			if _, exists := queryResponseData[field]; !exists {
				t.Errorf("Query controller response missing field: %s", field)
			}
		}

		// Verify columns structure
		columns, exists := queryResponseData["columns"]

		if !exists {
			t.Fatal("Query controller response missing 'columns' field")
		}

		columnsArray, ok := columns.([]any)

		if !ok {
			t.Fatalf("Expected columns to be an array, got %T", columns)
		}

		expectedColumns := []sqlite3.ColumnDefinition{
			{ColumnName: "id", ColumnType: sqlite3.ColumnTypeInteger},
			{ColumnName: "name", ColumnType: sqlite3.ColumnTypeText},
			{ColumnName: "active", ColumnType: sqlite3.ColumnTypeInteger},
		}

		if len(columnsArray) != len(expectedColumns) {
			t.Fatalf("Expected %d columns, got %d", len(expectedColumns), len(columnsArray))
		}

		for i, expectedCol := range expectedColumns {
			colDef, ok := columnsArray[i].(map[string]any)

			if !ok {
				t.Fatalf("Expected column definition to be a map, got %T", columnsArray[i])
			}

			if colDef["name"] != expectedCol.ColumnName {
				t.Fatalf("Expected column %d name to be '%s', got '%v'", i, expectedCol.ColumnName, colDef["name"])
			}

			if int(colDef["type"].(float64)) != int(expectedCol.ColumnType) {
				t.Fatalf("Expected column %d type to be '%d', got '%v'", i, expectedCol.ColumnType, colDef["type"])
			}
		}

		// Verify rows structure
		rows, exists := queryResponseData["rows"]

		if !exists {
			t.Fatal("Query controller response missing 'rows' field")
		}

		rowsArray, ok := rows.([]any)

		if !ok {
			t.Fatalf("Expected rows to be an array, got %T", rows)
		}

		if len(rowsArray) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rowsArray))
		}

		row := rowsArray[0].([]any)

		if len(row) != 3 {
			t.Fatalf("Expected 3 columns in row, got %d", len(row))
		}

		// Verify actual data matches expectations
		if int(row[0].(float64)) != 1 {
			t.Errorf("Expected id to be 1, got %v", row[0])
		}

		if row[1] != "Test User" {
			t.Errorf("Expected name to be 'Test User', got %v", row[1])
		}

		if int(row[2].(float64)) != 1 {
			t.Errorf("Expected active to be 1, got %v", row[2])
		}

		// Verify row_count matches actual data
		rowCount, exists := queryResponseData["rowCount"]

		if !exists {
			t.Fatal("Query controller response missing 'rowCount' field")
		}

		if int(rowCount.(float64)) != 1 {
			t.Errorf("Expected row_count to be 1, got %v", rowCount)
		}
	})
}

func TestQueryControllerBlobHandling(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{"*"},
			},
		}) // Create a table with a BLOB column
		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":         "1",
					"statement":  "CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, content BLOB);",
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

		// Test 1: Insert a blob with base64-encoded data
		base64Data := "SGVsbG8gV29ybGQ=" // base64 of "Hello World"

		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "2",
					"statement": "INSERT INTO files (name, content) VALUES (?, ?);",
					"parameters": []map[string]any{
						{
							"type":  "TEXT",
							"value": "test.bin",
						},
						{
							"type":  "BLOB",
							"value": base64Data,
						},
					},
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
		if responseData["lastInsertRowId"].(float64) != 1 {
			t.Fatalf("Expected lastInsertRowId to be 1, got %v", responseData["lastInsertRowId"])
		}

		// Test 2: Select the blob and verify it was stored correctly
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "3",
					"statement": "SELECT name, content FROM files WHERE id = ?;",
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

		responseData = resp["data"].([]any)[0].(map[string]any)
		rows := responseData["rows"].([]any)

		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}

		row := rows[0].([]any)
		if row[0] != "test.bin" {
			t.Fatalf("Expected name to be 'test.bin', got %v", row[0])
		}

		// The blob should be returned as base64-encoded string in JSON
		if row[1] != base64Data {
			t.Fatalf("Expected blob content to be '%s', got %v", base64Data, row[1])
		}

		// Test 3: Insert empty blob
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "4",
					"statement": "INSERT INTO files (name, content) VALUES (?, ?);",
					"parameters": []map[string]any{
						{
							"type":  "TEXT",
							"value": "empty.bin",
						},
						{
							"type":  "BLOB",
							"value": "",
						},
					},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// Test 4: Insert large blob (1KB of data)
		largeData := make([]byte, 1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}
		// Base64 encode for transmission
		largeBase64 := base64.StdEncoding.EncodeToString(largeData)

		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "5",
					"statement": "INSERT INTO files (name, content) VALUES (?, ?);",
					"parameters": []map[string]any{
						{
							"type":  "TEXT",
							"value": "large.bin",
						},
						{
							"type":  "BLOB",
							"value": largeBase64,
						},
					},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// Verify the large blob was stored correctly
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "6",
					"statement": "SELECT LENGTH(content) FROM files WHERE name = ?;",
					"parameters": []map[string]any{{
						"type":  "TEXT",
						"value": "large.bin",
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

		responseData = resp["data"].([]any)[0].(map[string]any)
		rows = responseData["rows"].([]any)

		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}

		blobLength := int(rows[0].([]any)[0].(float64))
		if blobLength != 1024 {
			t.Fatalf("Expected blob length to be 1024, got %d", blobLength)
		}
	})
}

func TestQueryControllerIntegerAsString(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions: []auth.Privilege{
					auth.DatabasePrivilegeQuery,
					auth.DatabasePrivilegeCreateTable,
					auth.DatabasePrivilegeInsert,
					auth.DatabasePrivilegeRead,
					auth.DatabasePrivilegeSelect,
					auth.DatabasePrivilegeTransaction,
					auth.DatabasePrivilegeUpdate,
					auth.DatabasePrivilegeDelete,
				},
			},
		})

		// Create a table with a BIGINT column
		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":         "1",
					"statement":  "CREATE TABLE large_ids (id INTEGER PRIMARY KEY, name TEXT);",
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

		// Test 1: Insert a large integer beyond JavaScript's safe range (as string)
		largeIntStr := "9007199254740993" // 2^53 + 1 - beyond Number.MAX_SAFE_INTEGER
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "2",
					"statement": "INSERT INTO large_ids (id, name) VALUES (?, ?);",
					"parameters": []map[string]any{
						{
							"type":  "INTEGER",
							"value": largeIntStr, // String format
						},
						{
							"type":  "TEXT",
							"value": "Large ID",
						},
					},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// Test 2: Query using the large integer (as string)
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "3",
					"statement": "SELECT * FROM large_ids WHERE id = ?;",
					"parameters": []map[string]any{{
						"type":  "INTEGER",
						"value": largeIntStr, // String format
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
		rows := responseData["rows"].([]any)

		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}

		row := rows[0].([]any)
		// Large integers are now returned as strings to preserve precision
		// beyond JavaScript's safe integer range
		retrievedIDStr, ok := row[0].(string)

		if !ok {
			t.Fatalf("Expected large integer to be returned as string, got %T: %v", row[0], row[0])
		}

		if retrievedIDStr != largeIntStr {
			t.Fatalf("Expected ID '%s', got '%s'", largeIntStr, retrievedIDStr)
		}

		// Verify the column type is INTEGER_LARGE (101)
		columns := responseData["columns"].([]any)

		if len(columns) == 0 {
			t.Fatalf("Expected columns metadata, got none")
		}

		idColumn := columns[0].(map[string]any)
		columnType := int(idColumn["type"].(float64))

		if columnType != int(sqlite3.ColumnTypeIntegerLarge) { // INTEGER_LARGE
			t.Fatalf("Expected column type %d (INTEGER_LARGE), got %d", int(sqlite3.ColumnTypeIntegerLarge), columnType)
		}

		retrievedName := row[1].(string)
		if retrievedName != "Large ID" {
			t.Fatalf("Expected name 'Large ID', got %s", retrievedName)
		}

		// Test 3: Insert negative large integer (as string)
		negativeIntStr := "-9223372036854775808" // Minimum int64 value
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "4",
					"statement": "INSERT INTO large_ids (id, name) VALUES (?, ?);",
					"parameters": []map[string]any{
						{
							"type":  "INTEGER",
							"value": negativeIntStr,
						},
						{
							"type":  "TEXT",
							"value": "Min Int64",
						},
					},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// Test 4: Verify traditional numeric format still works
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "5",
					"statement": "INSERT INTO large_ids (id, name) VALUES (?, ?);",
					"parameters": []map[string]any{
						{
							"type":  "INTEGER",
							"value": 42, // Traditional number format
						},
						{
							"type":  "TEXT",
							"value": "Normal ID",
						},
					},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// Verify small integers are still returned as numbers (backward compatibility)
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "6",
					"statement": "SELECT * FROM large_ids WHERE id = ?;",
					"parameters": []map[string]any{{
						"type":  "INTEGER",
						"value": 42,
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

		responseData = resp["data"].([]any)[0].(map[string]any)
		rows = responseData["rows"].([]any)

		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}

		row = rows[0].([]any)

		// Small integers should be returned as numbers for backward compatibility
		smallInt, ok := row[0].(float64)

		if !ok {
			t.Fatalf("Expected small integer to be returned as number, got %T: %v", row[0], row[0])
		}

		if int(smallInt) != 42 {
			t.Fatalf("Expected small integer 42, got %v", smallInt)
		}

		// Verify the column type is still INTEGER (1) for small integers
		columns = responseData["columns"].([]any)
		idColumn = columns[0].(map[string]any)
		columnType = int(idColumn["type"].(float64))

		if columnType != int(sqlite3.ColumnTypeInteger) { // INTEGER
			t.Fatalf("Expected column type %d (INTEGER), got %d", int(sqlite3.ColumnTypeInteger), columnType)
		}

		// Test 5: Invalid integer string should fail
		resp, responseCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/query",
				mock.DatabaseName,
				mock.BranchName,
			),
			"POST",
			map[string]any{
				"queries": []map[string]any{{
					"id":        "7",
					"statement": "INSERT INTO large_ids (id, name) VALUES (?, ?);",
					"parameters": []map[string]any{
						{
							"type":  "INTEGER",
							"value": "not-a-number",
						},
						{
							"type":  "TEXT",
							"value": "Invalid",
						},
					},
				}},
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode == 200 {
			t.Fatalf("Expected error for invalid integer string, got success: %s", resp)
		}
	})
}
