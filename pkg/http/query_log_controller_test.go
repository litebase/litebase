package http_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestQueryLogControllerSuccess(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		// Create a test table and insert some data to generate query metrics
		err = db.GetConnection().Transaction(false, func(dbConn *database.DatabaseConnection) error {
			_, err = dbConn.Exec("CREATE TABLE test_metrics (id INTEGER PRIMARY KEY, value TEXT)", nil)
			if err != nil {
				return err
			}

			for i := range 10 {
				_, err = dbConn.Exec("INSERT INTO test_metrics (value) VALUES (?)", []sqlite3.StatementParameter{
					{
						Type:  sqlite3.ParameterTypeText,
						Value: fmt.Appendf(nil, "test value %d", i),
					},
				})

				if err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Failed to set up test data: %v", err)
		}

		// Give some time for metrics to be recorded
		time.Sleep(100 * time.Millisecond)

		// Set up test time range
		now := time.Now()
		start := now.Add(-1 * time.Hour).Unix()
		end := now.Unix()

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/metrics/query?start=%d&end=%d&step=1",
				mock.DatabaseName,
				mock.BranchName,
				start,
				end,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d. Response: %v", responseCode, resp)
		}

		// Verify response structure
		if resp["status"] != "success" {
			t.Errorf("Expected status 'success', got %v", resp["status"])
		}

		// Check for meta section with keys
		meta, ok := resp["meta"].(map[string]any)

		if !ok {
			t.Fatalf("Expected meta to be an object, got %T", resp["meta"])
		}

		keys, ok := meta["keys"].([]any)

		if !ok {
			t.Fatalf("Expected meta.keys to be an array, got %T", meta["keys"])
		}

		if len(keys) == 0 {
			t.Error("Expected meta.keys to contain metric field names")
		}

		// Check for data section
		data, ok := resp["data"].([]any)

		if !ok {
			t.Fatalf("Expected data to be an array, got %T", resp["data"])
		}

		// Data might be empty if no query metrics were recorded yet, which is fine
		t.Logf("Retrieved %d query metrics", len(data))
	})
}

func TestQueryLogControllerInvalidParameters(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		testCases := []struct {
			name          string
			queryParams   string
			expectedError string
		}{
			{
				name:          "Invalid step value",
				queryParams:   "?start=1000&end=2000&step=0",
				expectedError: "Error: invalid step value",
			},
			{
				name:          "Non-numeric start",
				queryParams:   "?start=invalid&end=2000&step=1",
				expectedError: "Error: the request query parameters are invalid",
			},
			{
				name:          "Non-numeric end",
				queryParams:   "?start=1000&end=invalid&step=1",
				expectedError: "Error: the request query parameters are invalid",
			},
			{
				name:          "Negative step",
				queryParams:   "?start=1000&end=2000&step=-1",
				expectedError: "Error: invalid step value",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				resp, responseCode, err := client.Send(
					fmt.Sprintf(
						"/v1/databases/%s/branches/%s/metrics/query%s",
						mock.DatabaseName,
						mock.BranchName,
						tc.queryParams,
					),
					"GET",
					nil,
				)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if responseCode != 400 {
					t.Fatalf("Expected response code 400, got %d. Response: %v", responseCode, resp)
				}

				if resp["status"] != "error" {
					t.Errorf("Expected status 'error', got %v", resp["status"])
				}

				if resp["message"] != tc.expectedError {
					t.Errorf("Expected message '%s', got %v", tc.expectedError, resp["message"])
				}
			})
		}
	})
}

func TestQueryLogControllerWithStepCombining(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		// Test with a step value greater than 1 to verify metric combining works
		now := time.Now()
		start := now.Add(-1 * time.Hour).Unix()
		end := now.Unix()
		step := 60 // 60 seconds

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/metrics/query?start=%d&end=%d&step=%d",
				mock.DatabaseName,
				mock.BranchName,
				start,
				end,
				step,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d. Response: %v", responseCode, resp)
		}

		if resp["status"] != "success" {
			t.Errorf("Expected status 'success', got %v", resp["status"])
		}

		// Verify the response structure is valid even with step combining
		meta, ok := resp["meta"].(map[string]any)

		if !ok {
			t.Fatalf("Expected meta to be an object, got %T", resp["meta"])
		}

		if _, ok := meta["keys"].([]any); !ok {
			t.Fatalf("Expected meta.keys to be an array, got %T", meta["keys"])
		}

		if _, ok := resp["data"].([]any); !ok {
			t.Fatalf("Expected data to be an array, got %T", resp["data"])
		}
	})
}

func TestQueryLogControllerForbidden(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		// Create client without proper privileges
		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeQuery}, // Wrong privilege
			},
		})

		now := time.Now()
		start := now.Add(-1 * time.Hour).Unix()
		end := now.Unix()

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/metrics/query?start=%d&end=%d&step=1",
				mock.DatabaseName,
				mock.BranchName,
				start,
				end,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 403 {
			t.Fatalf("Expected response code 403, got %d. Response: %v", responseCode, resp)
		}
	})
}

func TestQueryLogControllerNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		now := time.Now()
		start := now.Add(-1 * time.Hour).Unix()
		end := now.Unix()

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/nonexistent/branches/main/metrics/query?start=%d&end=%d&step=1",
				start,
				end,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Should return 404 for non-existent database
		if responseCode != 404 {
			t.Fatalf("Expected response code 404, got %d. Response: %v", responseCode, resp)
		}
	})
}
